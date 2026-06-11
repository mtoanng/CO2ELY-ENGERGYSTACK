"""ELY Data Converter - production entry point.

Architecture: Distributed Polars on Spark Workers + UC Volume FUSE.

[Driver Node]
  ├── Lists XLSX/CSV file paths from UC Volume FUSE mount (os.walk)
  ├── Queries tracking table for watermark → filters to new files
  ├── Creates Spark DataFrame of file paths → repartitions across cores
  └── Collects results from workers → batch MERGE INTO tracking table

[Worker Nodes]  (or local[*] threads on single node)
  ├── Polars (Rust) reads file directly from FUSE (zero JVM)
  ├── Transforms: 3-row header → unpivot → 4 Arrow tables
  └── PyArrow writes Parquet directly to FUSE output (zero JVM)

Auth: UC Managed Identity via FUSE mount. Zero credentials in code.
Incremental: Delta tracking table with watermark strategy.
Linear scaling: N executor cores = N files processed in parallel.

Output tables (4):
  - filemeta: UUID, file_path, raw_file_name, file_size, last_modified, ingested_timestamp
  - channel: UUID, group, channel, channel_name, unit, column_index
  - timeseries: UUID, group, sample_offset, channel, value, value_str
  - statistics: UUID, group, n_channels, n_rows, n_timeseries_rows

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_0_convert/run_converter.py
        parameters:
          - "--is_integration_test" / "--env"
          - "--extensions" (optional, e.g. ".xlsx,.xls")
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

# Add current dir to path for sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

from common import (
    FileInfo, ParquetWriter, IncrementalTracker,
    sanitize_name, generate_file_uuid, logger,
    get_env_variables, get_volume_paths, CONVERTER_CONFIG,
)


# =============================================================================
# SPARK DISTRIBUTION: WORKER FUNCTION
# =============================================================================

# Schema for results returned by mapPartitions
RESULT_SCHEMA = StructType([
    StructField("blob_path", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("file_size", LongType(), False),
    StructField("file_uuid", StringType(), False),
    StructField("last_modified", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("output_paths", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("duration_seconds", DoubleType(), False),
])


def _process_partition(rows):
    """Execute on Spark workers. Reads/writes via FUSE. No SparkSession needed.

    Each row contains file metadata. Worker:
    1. Reads XLSX/CSV from FUSE path (Polars Rust I/O, zero JVM)
    2. Transforms (3-row header scan → unpivot → 4 Arrow tables)
    3. Writes Parquet to FUSE output path (PyArrow, zero JVM)
    4. Yields result Row for driver to collect

    All file I/O bypasses the JVM entirely. Only path strings cross Spark boundary.
    """
    # Lazy imports — these run on worker nodes
    from common import (
        generate_file_uuid, sanitize_name, ParquetWriter, logger as worker_logger,
    )
    from xlsx_converter import convert as xlsx_convert
    from csv_converter import convert as csv_convert

    for row in rows:
        t0 = time.perf_counter()
        fuse_input = row.fuse_path
        output_dir = row.output_dir
        ext = row.extension

        try:
            # Dispatch converter by extension
            converter = xlsx_convert if ext in (".xlsx", ".xls") else csv_convert
            results = converter(fuse_input, row.file_size, row.last_modified)

            if not results:
                # File had no valid data (empty sheets, etc.)
                duration = time.perf_counter() - t0
                yield Row(
                    blob_path=row.relative_path, file_name=row.file_name,
                    file_size=row.file_size,
                    file_uuid=generate_file_uuid(row.relative_path),
                    last_modified=row.last_modified, status="SKIPPED",
                    output_paths=None, error_message="No valid data in file",
                    duration_seconds=round(duration, 2),
                )
                continue

            # Write all results (4 Parquet files per sheet/group)
            writer = ParquetWriter(output_dir)
            base_filename = sanitize_name(Path(fuse_input).stem)
            all_paths = {}
            total_rows = 0

            for r in results:
                paths = writer.write_result(r, base_filename)
                all_paths.update(paths)
                total_rows += r.n_rows

            file_uuid = generate_file_uuid(row.relative_path)
            duration = time.perf_counter() - t0
            worker_logger.info(
                f"  \u2713 {row.file_name}: {total_rows:,} rows, {duration:.1f}s"
            )

            yield Row(
                blob_path=row.relative_path, file_name=row.file_name,
                file_size=row.file_size, file_uuid=file_uuid,
                last_modified=row.last_modified, status="SUCCESS",
                output_paths=json.dumps(all_paths), error_message=None,
                duration_seconds=round(duration, 2),
            )

        except Exception as e:
            duration = time.perf_counter() - t0
            file_uuid = generate_file_uuid(row.relative_path)
            worker_logger.error(f"  \u2717 {row.file_name}: {e}")

            yield Row(
                blob_path=row.relative_path, file_name=row.file_name,
                file_size=row.file_size, file_uuid=file_uuid,
                last_modified=row.last_modified, status="FAILED",
                output_paths=None, error_message=str(e)[:500],
                duration_seconds=round(duration, 2),
            )


# =============================================================================
# MAIN (same arg pattern as TBP task_runner.py)
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ELY Data Converter")
    # TBP-standard params (passed by all tasks)
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    # Converter-specific params
    parser.add_argument("--extensions", default=None,
                        help="Comma-separated extensions filter, e.g. '.xlsx,.xls' or '.csv'")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    t_start = time.perf_counter()

    is_integration_test = args.is_integration_test.lower() == "true"

    # Auto-detect environment from workspace URL (same as TBP common_utils.py)
    env_vars = get_env_variables(spark)
    unity_catalog = env_vars["unity_catalog"]
    logger.info(f"Environment: {env_vars['environment']}, Catalog: {unity_catalog}")

    # Resolve UC Volume FUSE paths
    vol_paths = get_volume_paths(unity_catalog)
    source_dir = vol_paths["source_dir"]
    output_dir = vol_paths["output_dir"]
    tracking_table = vol_paths["tracking_table"]

    # Integration test uses separate tracking table
    if is_integration_test:
        tracking_table = tracking_table.replace("file_tracking", "tmp_int_test_file_tracking")

    logger.info(f"Source: {source_dir}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Tracking: {tracking_table}")

    # Parse extensions filter
    extensions = None
    if args.extensions:
        extensions = {e.strip() for e in args.extensions.split(",")}
        logger.info(f"Extensions filter: {extensions}")

    # =========================================================================
    # STEP 1: Discover new files (driver-side, FUSE listing + watermark)
    # =========================================================================
    tracker = IncrementalTracker(tracking_table, spark)
    new_files = tracker.get_new_files(source_dir)

    # Apply extensions filter
    if extensions and new_files:
        new_files = [f for f in new_files if f.extension in extensions]
        logger.info(f"After extensions filter: {len(new_files)} file(s)")

    if not new_files:
        logger.info("No new files to process.")
        print(f"\n{'='*60}")
        print(f"Pipeline Summary: no_new_files (0.0s)")
        return

    logger.info(f"Files to process: {len(new_files)}")

    # =========================================================================
    # STEP 2: Distribute file paths across Spark workers
    # =========================================================================

    # Distribute sibling modules to workers (needed for mapPartitions imports)
    src_dir = str(Path(__file__).resolve().parent)
    spark.sparkContext.addPyFile(os.path.join(src_dir, "common.py"))
    spark.sparkContext.addPyFile(os.path.join(src_dir, "xlsx_converter.py"))
    spark.sparkContext.addPyFile(os.path.join(src_dir, "csv_converter.py"))

    # Build rows for Spark DataFrame
    file_rows = [
        Row(
            fuse_path=f.fuse_path,
            relative_path=f.relative_path,
            file_name=f.file_name,
            file_size=f.file_size,
            last_modified=f.last_modified,
            extension=f.extension,
            output_dir=output_dir,
        )
        for f in new_files
    ]

    # Repartition evenly across all available cores
    num_cores = spark.sparkContext.defaultParallelism
    num_partitions = min(len(file_rows), num_cores)
    files_df = spark.createDataFrame(file_rows)
    distributed_df = files_df.repartition(num_partitions)

    logger.info(f"Distributing {len(file_rows)} files across {num_partitions} partitions "
                f"({num_cores} cores available)")

    # =========================================================================
    # STEP 3: Execute on workers (Polars + FUSE, zero JVM for file I/O)
    # =========================================================================
    results_rdd = distributed_df.rdd.mapPartitions(_process_partition)
    results_df = spark.createDataFrame(results_rdd, schema=RESULT_SCHEMA)

    # Force execution and cache results
    results_df.cache()
    results_df.count()

    # =========================================================================
    # STEP 4: Batch MERGE results into tracking table
    # =========================================================================
    tracker.batch_merge_results(results_df)

    # Summary
    stats = results_df.groupBy("status").count().collect()
    stats_dict = {r.status: r["count"] for r in stats}
    total_time = time.perf_counter() - t_start

    results_df.unpersist()

    print(f"\n{'='*60}")
    print(f"Pipeline Summary:")
    print(f"  Environment: {env_vars['environment']}")
    print(f"  Integration test: {is_integration_test}")
    print(f"  Succeeded: {stats_dict.get('SUCCESS', 0)}")
    print(f"  Failed: {stats_dict.get('FAILED', 0)}")
    print(f"  Skipped: {stats_dict.get('SKIPPED', 0)}")
    print(f"  Partitions: {num_partitions} (of {num_cores} cores)")
    print(f"  Total time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
