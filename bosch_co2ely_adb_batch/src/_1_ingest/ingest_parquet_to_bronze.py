"""Bronze ingestion: Converter Parquet output -> Delta tables (Auto Loader).

Uses Structured Streaming with cloudFiles (Auto Loader) + Trigger.AvailableNow
for incremental, exactly-once ingestion. No custom tracking table needed —
Auto Loader handles deduplication via checkpoints.

Architecture:
- Auto Loader discovers new Parquet files in parquet_raw/{table_type}/
- Trigger.AvailableNow processes all new files then stops (batch-like)
- One stream per table_type (filemeta, channel, timeseries, statistics)
- Checkpoints stored in ADLS: parquet_raw/_checkpoints/bronze_{table_type}/
- Writes to 4 Delta tables with append mode + schema evolution
- No Azure SDK credentials needed — UC External Location handles auth

Tables written:
  {catalog}.{schema}.bronze_filemeta
  {catalog}.{schema}.bronze_channel
  {catalog}.{schema}.bronze_timeseries    (liquid clustered by uuid, group)
  {catalog}.{schema}.bronze_statistics

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_1_ingest/ingest_parquet_to_bronze.py
        parameters:
          - "--is_integration_test" / "--env"
"""
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Add shared config to path.
# Databricks spark_python_task runs via exec() where __file__ is not defined.
# Fallback: co_filename from the code object IS set by compile().
try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent / "_common"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from common_config import (
    env_variables, medallion_variables, layer_variables,
    build_table_name, CONVERTER_CONFIG, TABLE_TYPES, logger,
)


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ELY Bronze Ingestion (Auto Loader)")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    t_start = time.perf_counter()

    is_integration_test = args.is_integration_test.lower() == "true"

    # --- Resolve environment ---
    env = env_variables(spark)
    environment = env["environment"]
    catalog = env["unity_catalog"]

    # Resolve layers: _1_ingest reads from "raw" (parquet), writes to "bronze" (Delta)
    layers = layer_variables("_1_ingest")
    read_medal = medallion_variables(layers["read_layer"], environment)
    write_medal = medallion_variables(layers["write_layer"], environment)

    schema = write_medal["uc_schema"]
    container = read_medal["adls_container"]
    output_prefix = CONVERTER_CONFIG["output_prefix"]

    # Extract storage_account from adls_domain
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    if is_integration_test:
        output_prefix = f"{output_prefix}/_int_test"

    # Base ADLS path for converter output
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{output_prefix}"
    checkpoint_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{output_prefix}/_checkpoints"

    logger.info(f"{'='*60}")
    logger.info("ELY Bronze Ingestion (Auto Loader)")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Catalog.Schema: {catalog}.{schema}")
    logger.info(f"  Source: {base_path}/{{table_type}}/")
    logger.info(f"  Checkpoints: {checkpoint_base}/")
    logger.info(f"  Integration test: {is_integration_test}")
    logger.info(f"{'='*60}")

    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # =========================================================================
    # Auto Loader: one stream per table_type
    # =========================================================================
    total_rows = {}

    for table_type in TABLE_TYPES:
        bronze_table = build_table_name(
            catalog, schema, write_medal["table_prefix"], table_type, is_integration_test
        )
        source_path = f"{base_path}/{table_type}/"
        checkpoint_path = f"{checkpoint_base}/bronze_{table_type}"

        logger.info(f"  {table_type}: Auto Loader from {source_path}")

        # Read with Auto Loader (cloudFiles)
        stream_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", checkpoint_path)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(source_path)
            .withColumns({"_bronze_ingested_at": current_timestamp()})
        )

        # Write with Trigger.AvailableNow (process all new files, then stop)
        query = (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(bronze_table)
        )

        # Wait for this stream to finish
        query.awaitTermination()

        # Get rows written from stream progress
        rows_written = 0
        if query.lastProgress and query.lastProgress.get("numInputRows"):
            rows_written = query.lastProgress["numInputRows"]
        else:
            # Fallback: sum from all progress updates
            for p in (query.recentProgress or []):
                rows_written += p.get("numInputRows", 0)

        total_rows[table_type] = rows_written
        logger.info(f"  {table_type}: ingested {rows_written:,} rows -> {bronze_table}")

        # Enable liquid clustering on timeseries (largest table)
        if table_type == "timeseries" and rows_written > 0:
            try:
                spark.sql(f"ALTER TABLE {bronze_table} CLUSTER BY (uuid, `group`)")
            except Exception:
                pass  # Already clustered or not supported

    # =========================================================================
    # Summary
    # =========================================================================
    total_time = time.perf_counter() - t_start
    total_ingested = sum(total_rows.values())
    print(f"\n{'='*60}")
    print("Bronze Ingest Summary (Auto Loader):")
    print(f"  Environment: {environment}")
    print(f"  Integration test: {is_integration_test}")
    for tt, count in total_rows.items():
        print(f"  {tt}: {count:,} rows")
    print(f"  Total rows: {total_ingested:,}")
    print(f"  Total time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
