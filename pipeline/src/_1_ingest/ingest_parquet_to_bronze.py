"""Bronze ingestion: Converter Parquet output -> Delta tables.

Reads Parquet files produced by the converter stage (_0_convert) and writes
them into Unity Catalog Delta tables. Uses the converter's tracking table
as the source of truth — only ingests files with status='SUCCESS' that
haven't been ingested into bronze yet.

Architecture:
- Reads Parquet from ADLS via abfss:// (External Location + UC credential vending)
- Spark reads Parquet natively (no Polars needed at this stage)
- Writes to 4 Delta tables: filemeta, channel, timeseries, statistics
- Tracks its own bronze watermark to avoid re-ingesting
- No Azure SDK credentials needed — UC External Location handles auth

Tables written:
  {catalog}.bronze.co2_filemeta
  {catalog}.bronze.co2_channel
  {catalog}.bronze.co2_timeseries
  {catalog}.bronze.co2_statistics

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_1_ingest/ingest_parquet_to_bronze.py
        parameters:
          - "--is_integration_test" / "--env"
"""
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

# Add sibling package paths for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "_0_convert"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from common import (
    get_env_variables, get_adls_config, TABLE_TYPES, logger,
)


# =============================================================================
# CONFIG
# =============================================================================

BRONZE_SCHEMA = "bronze"
TABLE_PREFIX = "co2"

# Bronze tracking table name (tracks which converter outputs have been ingested)
BRONZE_TRACKING_TABLE_SUFFIX = "bronze_ingest_tracking"


# =============================================================================
# HELPERS
# =============================================================================

def get_bronze_table_name(catalog: str, table_type: str, is_integration_test: bool) -> str:
    """Build fully qualified bronze table name.

    Pattern: {catalog}.bronze.co2_{table_type}[_int_test]
    Example: co2elyd_dev.bronze.co2_timeseries
    """
    suffix = "_int_test" if is_integration_test else ""
    return f"{catalog}.{BRONZE_SCHEMA}.{TABLE_PREFIX}_{table_type}{suffix}"


def get_bronze_tracking_table(catalog: str, is_integration_test: bool) -> str:
    """Build fully qualified bronze tracking table name."""
    suffix = "_int_test" if is_integration_test else ""
    return f"{catalog}.{BRONZE_SCHEMA}.{BRONZE_TRACKING_TABLE_SUFFIX}{suffix}"


def build_abfss_path(storage_account: str, container: str, blob_path: str) -> str:
    """Build abfss:// URI for reading Parquet from ADLS.

    Uses UC External Location for auth (credential vending, zero secrets).
    Example: abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/parquet_raw/timeseries/file.parquet
    """
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{blob_path}"


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ELY Bronze Ingestion")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    t_start = time.perf_counter()

    is_integration_test = args.is_integration_test.lower() == "true"

    # Auto-detect environment
    env_vars = get_env_variables(spark)
    catalog = env_vars["unity_catalog"]
    adls_config = get_adls_config(env_vars)
    storage_account = adls_config["storage_account"]
    container = adls_config["container"]
    output_prefix = adls_config["output_prefix"]
    converter_tracking = adls_config["tracking_table"]

    if is_integration_test:
        converter_tracking = converter_tracking.replace("file_tracking", "file_tracking_int_test")
        output_prefix = f"{output_prefix}/_int_test"

    bronze_tracking = get_bronze_tracking_table(catalog, is_integration_test)

    logger.info(f"{'='*60}")
    logger.info(f"ELY Bronze Ingestion")
    logger.info(f"  Environment: {env_vars['environment']}, Catalog: {catalog}")
    logger.info(f"  Storage: {storage_account}/{container}/{output_prefix}")
    logger.info(f"  Converter tracking: {converter_tracking}")
    logger.info(f"  Bronze tracking: {bronze_tracking}")
    logger.info(f"{'='*60}")

    # =========================================================================
    # STEP 1: Ensure bronze schema + tracking table exist
    # =========================================================================
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{BRONZE_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_tracking} (
            file_uuid STRING,
            blob_path STRING,
            table_type STRING,
            bronze_table STRING,
            rows_written BIGINT,
            ingested_at TIMESTAMP
        ) USING DELTA
    """)

    # =========================================================================
    # STEP 2: Find converter outputs not yet ingested into bronze
    # =========================================================================
    # Get all SUCCESS records from converter tracking
    try:
        converter_success = spark.sql(f"""
            SELECT file_uuid, blob_path, output_paths
            FROM {converter_tracking}
            WHERE status = 'SUCCESS' AND output_paths IS NOT NULL
        """)
    except Exception as e:
        logger.warning(f"Converter tracking table not ready: {e}")
        print("Pipeline Summary: converter tracking table not available")
        return

    # Get already-ingested file_uuids
    try:
        already_ingested = spark.sql(f"""
            SELECT DISTINCT file_uuid FROM {bronze_tracking}
        """)
        # Anti-join: only new records
        new_records = converter_success.join(
            already_ingested, on="file_uuid", how="left_anti"
        )
    except Exception:
        # Bronze tracking empty or doesn't exist yet — ingest everything
        new_records = converter_success

    new_records_list = new_records.collect()

    if not new_records_list:
        logger.info("No new converter outputs to ingest.")
        total_time = time.perf_counter() - t_start
        print(f"\n{'='*60}")
        print(f"Bronze Ingest Summary: no_new_data ({total_time:.1f}s)")
        return

    logger.info(f"New files to ingest into bronze: {len(new_records_list)}")

    # =========================================================================
    # STEP 3: Read Parquet from ADLS (abfss://) and write to Delta
    # =========================================================================
    total_rows = {}
    tracking_records = []

    for table_type in TABLE_TYPES:
        bronze_table = get_bronze_table_name(catalog, table_type, is_integration_test)

        # Collect all Parquet paths for this table_type
        parquet_paths = []
        for row in new_records_list:
            output_paths = json.loads(row.output_paths) if row.output_paths else {}
            if table_type in output_paths:
                # output_paths stores relative: "timeseries/filename.parquet"
                # Reconstruct full abfss:// path using External Location
                blob_path = f"{output_prefix}/{output_paths[table_type]}"
                abfss_path = build_abfss_path(storage_account, container, blob_path)
                parquet_paths.append(abfss_path)

        if not parquet_paths:
            logger.info(f"  {table_type}: no Parquet files to ingest")
            total_rows[table_type] = 0
            continue

        # Read all Parquet files for this table_type in one shot
        # Auth: UC External Location + credential vending (zero secrets needed)
        logger.info(f"  {table_type}: reading {len(parquet_paths)} Parquet file(s)")
        df = spark.read.parquet(*parquet_paths)

        # Add ingestion metadata
        df = df.withColumn("_bronze_ingested_at", current_timestamp())

        # Write to Delta (append mode — incremental)
        # saveAsTable auto-creates on first run, appends on subsequent
        df.write.format("delta").mode("append").saveAsTable(bronze_table)

        # Get row count from Delta table history (no extra scan)
        try:
            last_op = spark.sql(
                f"DESCRIBE HISTORY {bronze_table} LIMIT 1"
            ).select("operationMetrics").collect()[0][0]
            row_count = int(last_op.get("numOutputRows", 0)) if last_op else 0
        except Exception:
            row_count = 0
        total_rows[table_type] = row_count
        logger.info(f"  {table_type}: wrote {row_count:,} rows to {bronze_table}")

        # Track each file's contribution
        for row in new_records_list:
            output_paths = json.loads(row.output_paths) if row.output_paths else {}
            if table_type in output_paths:
                tracking_records.append((
                    row.file_uuid, row.blob_path, table_type, bronze_table, row_count,
                ))

    # =========================================================================
    # STEP 4: Update bronze tracking table
    # =========================================================================
    if tracking_records:
        from pyspark.sql import Row as SparkRow
        tracking_rows = [
            SparkRow(file_uuid=r[0], blob_path=r[1], table_type=r[2],
                     bronze_table=r[3], rows_written=r[4],
                     ingested_at=datetime.now(tz=timezone.utc))
            for r in tracking_records
        ]
        tracking_df = spark.createDataFrame(tracking_rows)
        tracking_df.write.format("delta").mode("append").saveAsTable(bronze_tracking)

    # Summary
    total_time = time.perf_counter() - t_start
    print(f"\n{'='*60}")
    print("Bronze Ingest Summary:")
    print(f"  Environment: {env_vars['environment']}")
    print(f"  Integration test: {is_integration_test}")
    print(f"  Files ingested: {len(new_records_list)}")
    for tt, count in total_rows.items():
        print(f"  {tt}: {count:,} rows")
    print(f"  Total time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
