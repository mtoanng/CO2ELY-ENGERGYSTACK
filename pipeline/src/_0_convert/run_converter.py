"""ELY Data Converter — Spark distributed orchestrator.

Architecture: Azure SDK + Spark mapPartitions.
┌─────────────────────────────────────────────────────────────────────────┐
│ DRIVER                                                                  │
│  1. get_env_variables(spark) → storage_account, container, catalog      │
│  2. IncrementalTracker.get_new_files() → List[BlobInfo] (SDK listing)   │
│  3. spark.createDataFrame(blobs).repartition(N)                         │
│  4. .rdd.mapPartitions(_process_partition) → results                    │
│  5. tracker.batch_merge_results(results_df)                             │
└─────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────┐
│ WORKER (per partition)                                                   │
│  1. os.environ["AZURE_*"] → BlobServiceClient (per-worker instance)     │
│  2. download_blob().readall() → bytes (parallel HTTP chunks)            │
│  3. Polars + calamine parse (Rust, releases GIL)                        │
│  4. PyArrow → Parquet bytes in memory                                   │
│  5. upload_blob(buf, max_concurrency=4) → ADLS                          │
│  6. yield Row(blob_path, status, output_paths, ...)                     │
└─────────────────────────────────────────────────────────────────────────┘

Scalability:
- num_workers=0 (dev): all partitions on driver, local[*] parallelism
- num_workers=N (prod): Spark distributes partitions across N executors
- Linear scaling: 4 workers × 16 cores = process 64 files concurrently

Auth: SP secret via spark_env_vars ({{secrets/...}} resolved at cluster start).
Workers read os.environ["AZURE_*"] — set at cluster init, available on ALL nodes.
ADLS config (storage_account, container, output_prefix) passed via DataFrame columns
to ensure propagation to remote executors.
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterator

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

# Add source directory to path for imports
_SRC_DIR = str(Path(__file__).resolve().parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


# =============================================================================
# RESULT SCHEMA (collected from workers → batch MERGE)
# =============================================================================

RESULT_SCHEMA = StructType([
    StructField("blob_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("file_uuid", StringType(), True),
    StructField("last_modified", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("output_paths", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
])


# =============================================================================
# WORKER FUNCTION (runs inside mapPartitions on executors)
# =============================================================================

def _process_partition(rows: Iterator[Row]) -> Iterator[Row]:
    """Process a partition of blob paths on a Spark worker.

    Each worker:
    1. Creates its own BlobServiceClient from env vars (connection pooling)
    2. Downloads file bytes via SDK (parallel HTTP chunks)
    3. Parses with Polars (Rust, zero GIL contention)
    4. Writes Parquet via PyArrow → bytes → SDK upload

    ADLS config (storage_account, container, output_prefix) is passed as
    columns in each Row — NOT via os.environ (which isn't propagated to
    remote executors in multi-worker mode).

    Lazy imports inside worker (addPyFile compatibility + clean isolation).
    """
    # Lazy imports — only loaded on workers that actually process data
    from common import (
        get_blob_service_client, generate_file_uuid, sanitize_name,
        ParquetWriter, logger,
    )
    from xlsx_converter import convert as convert_xlsx
    from csv_converter import convert as convert_csv

    # Per-partition state (lazy init on first row)
    sdk_client = None
    container_client = None
    writer = None

    for row in rows:
        t0 = time.time()
        blob_path = row.blob_path        # e.g. "raw_data/subdir/file.xlsx"
        relative_path = row.relative_path # e.g. "subdir/file.xlsx"
        file_name = row.file_name
        file_size = row.file_size
        last_modified = row.last_modified
        extension = row.extension
        # ADLS config carried per-row (propagated to remote workers)
        storage_account = row.storage_account
        container = row.container
        output_prefix = row.output_prefix

        # Lazy init SDK client (once per partition, reused across rows)
        if sdk_client is None:
            sdk_client = get_blob_service_client(storage_account)
            container_client = sdk_client.get_container_client(container)
            writer = ParquetWriter(storage_account, container, output_prefix)

        try:
            # --- DOWNLOAD (Azure SDK — parallel HTTP chunks, zero JVM) ---
            blob = container_client.get_blob_client(blob_path)
            file_bytes = blob.download_blob(max_concurrency=4).readall()

            # --- PARSE (Polars + calamine — Rust, releases GIL) ---
            if extension in (".xlsx", ".xls"):
                results = convert_xlsx(file_bytes, relative_path, file_size, last_modified)
            elif extension == ".csv":
                results = convert_csv(file_bytes, relative_path, file_size, last_modified)
            else:
                raise ValueError(f"Unsupported extension: {extension}")

            if not results:
                raise ValueError("No data extracted (empty or unparseable)")

            # --- WRITE (PyArrow → bytes → SDK upload, parallel PUT) ---
            file_uuid = generate_file_uuid(relative_path)
            base_filename = sanitize_name(Path(file_name).stem)
            all_output_paths = {}

            for r in results:
                paths = writer.write_result(r, base_filename)
                all_output_paths.update(paths)

            duration = time.time() - t0
            logger.info(f"  OK: {relative_path} ({file_size/1024:.0f}KB, "
                        f"{len(results)} group(s), {duration:.1f}s)")

            yield Row(
                blob_path=relative_path,
                file_name=file_name,
                file_size=file_size,
                file_uuid=file_uuid,
                last_modified=last_modified,
                status="SUCCESS",
                output_paths=json.dumps(all_output_paths),
                error_message=None,
                duration_seconds=round(duration, 2),
            )

        except Exception as e:
            duration = time.time() - t0
            logger.error(f"  FAIL: {relative_path} — {e}")
            yield Row(
                blob_path=relative_path,
                file_name=file_name,
                file_size=file_size,
                file_uuid=None,
                last_modified=last_modified,
                status="FAILED",
                output_paths=None,
                error_message=str(e)[:500],
                duration_seconds=round(duration, 2),
            )


# =============================================================================
# DRIVER MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ELY Converter (Azure SDK + mapPartitions)")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    parser.add_argument("--extensions", type=str, default=".xlsx,.xls,.csv",
                        help="Comma-separated extensions to process")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    target_extensions = set(args.extensions.split(","))

    # Import common utilities (driver-side)
    from common import (
        get_env_variables, get_adls_config, IncrementalTracker, logger,
    )

    # --- 1. Environment detection ---
    env_vars = get_env_variables(spark)
    adls_config = get_adls_config(env_vars)
    storage_account = adls_config["storage_account"]
    container = adls_config["container"]
    source_prefix = adls_config["source_prefix"]
    output_prefix = adls_config["output_prefix"]
    tracking_table = adls_config["tracking_table"]

    if is_int_test:
        tracking_table = tracking_table.replace("file_tracking", "file_tracking_int_test")
        source_prefix = f"{source_prefix}/_int_test"
        output_prefix = f"{output_prefix}/_int_test"

    logger.info(f"{'='*60}")
    logger.info(f"ELY Converter — Azure SDK + mapPartitions")
    logger.info(f"  Environment: {env_vars['environment']}")
    logger.info(f"  Storage: {storage_account}/{container}")
    logger.info(f"  Source prefix: {source_prefix}")
    logger.info(f"  Output prefix: {output_prefix}")
    logger.info(f"  Tracking table: {tracking_table}")
    logger.info(f"  Extensions: {target_extensions}")
    logger.info(f"  Integration test: {is_int_test}")
    logger.info(f"{'='*60}")

    # --- 2. Incremental file discovery (SDK listing + watermark) ---
    tracker = IncrementalTracker(tracking_table, spark)
    new_blobs = tracker.get_new_files(storage_account, container, source_prefix)

    # Filter by target extensions
    new_blobs = [b for b in new_blobs if b.extension in target_extensions]
    logger.info(f"Files to process (after extension filter): {len(new_blobs)}")

    if not new_blobs:
        logger.info("Nothing to process. Exiting.")
        return

    # --- 3. Distribute to workers via Spark ---
    # ADLS config is passed as columns in the DataFrame (NOT via os.environ).
    # os.environ on driver is NOT propagated to remote executors in multi-worker.
    # AZURE_* creds are set via spark_env_vars at cluster init → available everywhere.
    file_rows = [
        Row(
            blob_path=b.blob_path,
            relative_path=b.relative_path,
            file_name=b.file_name,
            file_size=b.file_size,
            last_modified=b.last_modified,
            extension=b.extension,
            # ADLS config carried per-row for worker access
            storage_account=storage_account,
            container=container,
            output_prefix=output_prefix,
        )
        for b in new_blobs
    ]

    # Determine partition count (scale with cluster size)
    num_partitions = min(len(new_blobs), spark.sparkContext.defaultParallelism)
    logger.info(f"Partitions: {num_partitions} (for {len(new_blobs)} files)")

    # Distribute source modules for worker imports
    src_dir = Path(__file__).resolve().parent
    for module_file in ["common.py", "xlsx_converter.py", "csv_converter.py"]:
        module_path = str(src_dir / module_file)
        spark.sparkContext.addPyFile(module_path)

    # Create distributed DataFrame and process via mapPartitions
    input_df = spark.createDataFrame(file_rows)
    distributed_df = input_df.repartition(num_partitions)

    # --- 4. Execute distributed conversion ---
    results_rdd = distributed_df.rdd.mapPartitions(_process_partition)
    results_df = spark.createDataFrame(results_rdd, schema=RESULT_SCHEMA)

    # Force execution and cache for reporting
    results_df.cache()
    total = results_df.count()
    success = results_df.filter("status = 'SUCCESS'").count()
    failed = total - success

    logger.info(f"\n{'='*60}")
    logger.info(f"Results: {total} total, {success} success, {failed} failed")

    if failed > 0:
        logger.warning("Failed files:")
        for row in results_df.filter("status = 'FAILED'").select("blob_path", "error_message").collect():
            logger.warning(f"  {row.blob_path}: {row.error_message[:100]}")

    # --- 5. Batch MERGE results into tracking table ---
    tracker.batch_merge_results(results_df)
    logger.info(f"Tracking table updated: {tracking_table}")
    logger.info(f"{'='*60}")

    results_df.unpersist()


if __name__ == "__main__":
    main()
