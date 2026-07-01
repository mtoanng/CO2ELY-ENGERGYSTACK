"""ELY Data Converter — Spark distributed orchestrator.

Architecture: Azure SDK + Spark mapPartitions + ThreadPoolExecutor.
┌─────────────────────────────────────────────────────────────────────────┐
│ DRIVER                                                                  │
│  1. get_env_variables(spark) → storage_account, container, catalog      │
│  2. IncrementalTracker.get_new_files() → List[BlobInfo] (SDK listing)   │
│  3. spark.createDataFrame(blobs).repartition(num_partitions)            │
│  4. .rdd.mapPartitions(_process_partition) → results                    │
│  5. tracker.batch_merge_results(results_df)                             │
└─────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────┐
│ WORKER (per partition) — processes MULTIPLE files via ThreadPool        │
│  1. os.environ["AZURE_*"] → BlobServiceClient (per-partition instance)  │
│  2. ThreadPoolExecutor(max_workers=THREADS_PER_PARTITION)               │
│  3. Each thread: download → Polars parse → PyArrow → upload             │
│  4. Retry with backoff for transient errors (network, throttle)         │
│  5. Collect results → yield Row(...)                                    │
└─────────────────────────────────────────────────────────────────────────┘

Reliability:
  - Transient errors (timeout, throttle, connection reset): retry up to MAX_RETRIES
    with exponential backoff (1s, 2s, 4s)
  - Permanent errors (parse failure, corrupt file): fail immediately, no retry
  - Retry cap in tracking table: files that failed MAX_RETRIES across runs
    are skipped (avoids infinite retry of corrupt files)

Deduplication:
  - Spark repartition guarantees each Row in exactly ONE partition
  - xlsx tasks filter by extension → no overlap
  - Tracking table MERGE on blob_path → idempotent updates

Scalability (two-level parallelism):
  Level 1 (inter-node): Spark distributes partitions across workers
  Level 2 (intra-node): ThreadPoolExecutor within each partition
  spark.task.cpus=4 → 4 slots per E16 worker → 16 concurrent files max
  Benchmark: 4T*4thr fastest (fewer SDK pools = better HTTP reuse)
  Partition strategy: fill all task slots first, then batch remaining
"""
import os
import sys
import json
import time
import argparse
import functools
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterator, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

# Add source directory to path for imports.
# Databricks spark_python_task runs files via exec(compile(...)) which does NOT
# define __file__. Fallback: co_filename from the code object IS set by compile().
try:
    _SRC_DIR = str(Path(__file__).resolve().parent)
except NameError:
    _SRC_DIR = str(Path(sys._getframe().f_code.co_filename).resolve().parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

# Module-level imports required for _process_single_file (called via addPyFile workers)
from converter_utils import build_abfss_path
from channel_mapping import load_series_mapping, resolve_mapping_for_path


# =============================================================================
# TUNING CONSTANTS
# =============================================================================

# Threads per partition (concurrent files within a single Spark task)
THREADS_PER_PARTITION = 4

# Cluster topology
CORES_PER_WORKER = 16   # Standard_E16as_v4
MAX_WORKERS = 3         # autoscale max

# Retry config for transient errors
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # seconds (1s, 2s, 4s)

# Max retry count across runs (skip permanently broken files)
MAX_TOTAL_RETRIES = 5

# Transient error types (retry these)
TRANSIENT_ERRORS = (
    "ConnectionError",
    "ConnectionResetError",
    "TimeoutError",
    "ServiceRequestError",
    "ServiceResponseError",
    "HttpResponseError",  # Azure SDK throttling (429)
    "ClientAuthenticationError",  # token refresh transient
)


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
# RETRY HELPER
# =============================================================================

def _is_transient_error(error: Exception) -> bool:
    """Check if error is transient (network/throttle) and worth retrying.

    Inspects error type name, cause chain, and message content for known
    transient patterns (Azure SDK errors, HTTP 429/5xx).

    Args:
        error: The exception to classify.

    Returns:
        True if error is transient and should be retried with backoff.
        False if error is permanent (parse failure, corrupt file, etc.).
    """
    error_type = type(error).__name__
    # Check against known transient error types
    if error_type in TRANSIENT_ERRORS:
        return True
    # Azure SDK wraps errors — check cause chain
    if hasattr(error, "__cause__") and error.__cause__:
        cause_type = type(error.__cause__).__name__
        if cause_type in TRANSIENT_ERRORS:
            return True
    # HTTP 429 (throttled) or 5xx (server error) in message
    error_msg = str(error).lower()
    if "429" in error_msg or "throttl" in error_msg:
        return True
    if any(f"{code}" in error_msg for code in range(500, 504)):
        return True
    return False


# =============================================================================
# SINGLE FILE PROCESSOR (called by ThreadPoolExecutor, with retry)
# =============================================================================

def _process_single_file(
    row,
    container_client,
    writer,
    convert_xlsx,
    generate_file_uuid,
    sanitize_name,
    logger,
    series_mapping: dict = None,
) -> Row:
    """Process a single file: download → parse → write Parquet → return result Row.

    Implements retry with exponential backoff for transient errors.
    Thread-safe (no shared mutable state). Called by ThreadPoolExecutor
    inside _process_partition.

    Args:
        row: Spark Row with fields: blob_path, relative_path, file_name,
            file_size, last_modified, extension, storage_account, container,
            output_prefix.
        container_client: Azure ContainerClient (shared, thread-safe).
        writer: ParquetWriter instance for uploading output Parquet files.
        convert_xlsx: xlsx_converter.convert function reference.
        generate_file_uuid: common.generate_file_uuid function reference.
        sanitize_name: common.sanitize_name function reference.
        logger: Logger instance for structured output.

    Returns:
        Spark Row with RESULT_SCHEMA fields: blob_path, file_name, file_size,
        file_uuid, last_modified, status ("SUCCESS" or "FAILED"),
        output_paths (JSON), error_message, duration_seconds.
    """
    t0 = time.time()
    blob_path = row.blob_path
    relative_path = row.relative_path
    file_name = row.file_name
    file_size = row.file_size
    last_modified = row.last_modified
    extension = row.extension

    last_error = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            # --- DOWNLOAD (Azure SDK — parallel HTTP chunks) ---
            blob = container_client.get_blob_client(blob_path)
            file_bytes = blob.download_blob(max_concurrency=4).readall()

            # --- PARSE (Polars + calamine) ---
            # Full abfss:// path for filemeta.file_path (downstream traceability)
            abfss_path = build_abfss_path(row.storage_account, row.container, blob_path)

            # Resolve channel mapping + series for this file. Integration-test paths
            # can include extra prefixes, so inspect all relative path components.
            mapping, series = resolve_mapping_for_path(relative_path, series_mapping or {})

            if extension in (".xlsx", ".xls"):
                results = convert_xlsx(file_bytes, relative_path, file_size, last_modified,
                                       abfss_file_path=abfss_path, mapping=mapping, series=series)
            # elif extension == ".csv":
            #     results = convert_csv(file_bytes, relative_path, file_size, last_modified, abfss_file_path=abfss_path)
            else:
                raise ValueError(f"Unsupported extension: {extension}")

            if not results:
                raise ValueError("No data extracted (empty or unparseable)")

            # --- WRITE (PyArrow → bytes → SDK upload) ---
            file_uuid = generate_file_uuid(relative_path)
            base_filename = sanitize_name(Path(file_name).stem)
            all_output_paths = {}

            for r in results:
                paths = writer.write_result(r, base_filename)
                all_output_paths.update(paths)

            duration = time.time() - t0
            retry_info = f", retries={attempt}" if attempt > 0 else ""
            logger.info(f"  OK: {relative_path} ({file_size/1024:.0f}KB, "
                        f"{len(results)} group(s), {duration:.1f}s{retry_info})")

            return Row(
                blob_path=blob_path,
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
            last_error = e

            # Check if error is transient and retryable
            if _is_transient_error(e) and attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE * (2 ** attempt)  # 1s, 2s, 4s
                logger.warning(f"  RETRY {attempt+1}/{MAX_RETRIES}: {relative_path} "
                               f"— {type(e).__name__}: {str(e)[:100]} (backoff {backoff}s)")
                time.sleep(backoff)
                continue
            else:
                # Permanent error or max retries exhausted
                break

    # All retries exhausted or permanent error
    duration = time.time() - t0
    error_prefix = f"[after {MAX_RETRIES} retries] " if _is_transient_error(last_error) else ""
    logger.error(f"  FAIL: {relative_path} — {error_prefix}{last_error}")
    return Row(
        blob_path=blob_path,
        file_name=file_name,
        file_size=file_size,
        file_uuid=None,
        last_modified=last_modified,
        status="FAILED",
        output_paths=None,
        error_message=f"{error_prefix}{str(last_error)[:500]}",
        duration_seconds=round(duration, 2),
    )


# =============================================================================
# WORKER FUNCTION (runs inside mapPartitions on executors)
# =============================================================================

def _process_partition(rows: Iterator[Row], series_mapping: dict = None) -> Iterator[Row]:
    """Process a partition of files on a Spark worker.

    Creates a single SDK client and ParquetWriter per partition, then processes
    files concurrently via ThreadPoolExecutor. Polars releases GIL, so true
    parallelism is achieved across threads.

    Lazy imports inside worker (addPyFile compatibility + clean isolation).

    Args:
        rows: Iterator of Spark Rows from one partition. Each Row contains
            file metadata + ADLS config (blob_path, relative_path, file_name,
            file_size, last_modified, extension, storage_account, container,
            output_prefix).

    Returns:
        Iterator of result Rows with RESULT_SCHEMA fields (blob_path, status,
        output_paths, error_message, duration_seconds, etc.).
    """
    # Materialize iterator to list (needed for ThreadPoolExecutor)
    rows_list = list(rows)
    if not rows_list:
        return

    # Lazy imports — only loaded on workers that actually process data
    from converter_utils import (
        get_blob_service_client, generate_file_uuid, sanitize_name,
        build_abfss_path, ParquetWriter, logger,
    )
    from xlsx_converter import convert as convert_xlsx

    # Get ADLS config from first row (all rows in partition have same config)
    first_row = rows_list[0]
    storage_account = first_row.storage_account
    container = first_row.container
    output_prefix = first_row.output_prefix

    # Create per-partition SDK client (thread-safe, connection pooled)
    sdk_client = get_blob_service_client(storage_account)
    container_client = sdk_client.get_container_client(container)

    # Create per-partition Parquet writer (shares SDK client — single auth)
    writer = ParquetWriter(storage_account, container, output_prefix)

    logger.info(f"Partition received {len(rows_list)} file(s), "
                f"processing with {THREADS_PER_PARTITION} threads")

    # Process files in parallel using ThreadPoolExecutor
    if len(rows_list) == 1:
        # Single file — no thread overhead
        yield _process_single_file(
            rows_list[0], container_client, writer,
            convert_xlsx, generate_file_uuid, sanitize_name, logger,
            series_mapping,
        )
    else:
        # Multiple files — use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=THREADS_PER_PARTITION) as executor:
            futures = {
                executor.submit(
                    _process_single_file,
                    row, container_client, writer,
                    convert_xlsx, generate_file_uuid, sanitize_name, logger,
                    series_mapping,
                ): row
                for row in rows_list
            }
            for future in as_completed(futures):
                yield future.result()


# =============================================================================
# DRIVER MAIN
# =============================================================================

def parse_args():
    """Parse CLI arguments passed by Databricks spark_python_task.

    Args:
        None (reads from sys.argv).

    Returns:
        argparse.Namespace with fields: is_integration_test (str),
        env (str), extensions (str), threads_per_partition (int).
    """
    parser = argparse.ArgumentParser(description="ELY Converter (Azure SDK + mapPartitions)")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    parser.add_argument("--extensions", type=str, default=".xlsx,.xls",
                        help="Comma-separated extensions to process")
    parser.add_argument("--threads_per_partition", type=int, default=THREADS_PER_PARTITION,
                        help="Threads per partition (concurrent files)")
    args, _ = parser.parse_known_args()
    return args


def main():  
    """Driver entry point: discover files, distribute to workers, merge results.

    Orchestrates the full conversion pipeline:
    1. Auto-detect environment from workspace URL
    2. Discover new/modified blobs via Azure SDK + watermark
    3. Filter by extension and skip exhausted retries
    4. Distribute files to Spark workers via mapPartitions
    5. Collect results and MERGE into tracking table

    Returns:
        None. Side effects: Parquet files written to ADLS, tracking table updated.
    """
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    target_extensions = set(args.extensions.split(","))
    threads_per_partition = args.threads_per_partition

    # Update global for workers
    global THREADS_PER_PARTITION
    THREADS_PER_PARTITION = threads_per_partition

    # Import driver-side helpers from shared config and converter utilities.
    from common_config import get_adls_config, get_env_variables
    from converter_utils import IncrementalTracker, logger

    # --- 1. Environment detection ---
    env_vars = get_env_variables(spark, env_override=args.env)
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
    logger.info(f"ELY Converter — Azure SDK + mapPartitions + ThreadPool")
    logger.info(f"  Environment: {env_vars['environment']}")
    logger.info(f"  Storage: {storage_account}/{container}")
    logger.info(f"  Source prefix: {source_prefix}")
    logger.info(f"  Output prefix: {output_prefix}")
    logger.info(f"  Tracking table: {tracking_table}")
    logger.info(f"  Extensions: {target_extensions}")
    logger.info(f"  Integration test: {is_int_test}")
    logger.info(f"  Threads/partition: {threads_per_partition}")
    logger.info(f"  Max retries (transient): {MAX_RETRIES}")
    logger.info(f"  Max total retries (across runs): {MAX_TOTAL_RETRIES}")
    logger.info(f"{'='*60}")

    # --- 2. Incremental file discovery (SDK listing + watermark) ---
    tracker = IncrementalTracker(tracking_table, spark)
    new_blobs = tracker.get_new_files(storage_account, container, source_prefix)

    # Filter by target extensions
    new_blobs = [b for b in new_blobs if b.extension in target_extensions]

    # Skip files that have exceeded max total retries (permanently broken)
    if new_blobs:
        try:
            candidate_paths_df = spark.createDataFrame(
                [Row(blob_path=b.blob_path) for b in new_blobs]
            ).dropDuplicates(["blob_path"])
            candidate_paths_df.createOrReplaceTempView("_converter_candidate_paths")
            exhausted = spark.sql(f"""
                SELECT t.blob_path
                FROM {tracking_table} t
                INNER JOIN _converter_candidate_paths c
                    ON t.blob_path = c.blob_path
                WHERE t.status = 'FAILED'
                  AND t.retry_count >= {MAX_TOTAL_RETRIES}
            """).collect()
            spark.catalog.dropTempView("_converter_candidate_paths")
            exhausted_paths = {r.blob_path for r in exhausted}
            if exhausted_paths:
                logger.warning(f"Skipping {len(exhausted_paths)} file(s) that exceeded "
                               f"max retries ({MAX_TOTAL_RETRIES}):")
                for p in exhausted_paths:
                    logger.warning(f"  SKIP: {p}")
                new_blobs = [b for b in new_blobs if b.blob_path not in exhausted_paths]
        except Exception:
            # retry_count column might not exist yet (first run) — proceed anyway
            pass

    logger.info(f"Files to process (after extension + retry filter): {len(new_blobs)}")

    if not new_blobs:
        logger.info("Nothing to process. Exiting.")
        return

    # --- 3. Distribute to workers via Spark ---
    # Strategy: MAXIMIZE parallelism by filling all available task slots.
    # Each file gets its own partition (= own task) when possible.
    # Only batch files into a partition when num_files exceeds total slots.
    task_cpus = int(spark.conf.get("spark.task.cpus", "4"))
    slots_per_worker = CORES_PER_WORKER // task_cpus
    total_slots = slots_per_worker * MAX_WORKERS
    num_partitions = min(len(new_blobs), total_slots)
    files_per_partition = (len(new_blobs) + num_partitions - 1) // num_partitions
    logger.info(f"Partitions: {num_partitions} (for {len(new_blobs)} files, "
                f"~{files_per_partition} files/partition, "
                f"{total_slots} total slots across {MAX_WORKERS} workers)")

    # Build input DataFrame with ADLS config carried per-row
    file_rows = [
        Row(
            blob_path=b.blob_path,
            relative_path=b.relative_path,
            file_name=b.file_name,
            file_size=b.file_size,
            last_modified=b.last_modified,
            extension=b.extension,
            storage_account=storage_account,
            container=container,
            output_prefix=output_prefix,
        )
        for b in new_blobs
    ]

    # Distribute source modules for worker imports (uses _SRC_DIR computed at top)
    src_dir = Path(_SRC_DIR)
    for module_file in [
        "converter_utils.py",
        "channel_mapping.py",
        "derived_metrics.py",
        "xlsx_converter.py",
    ]:
        module_path = str(src_dir / module_file)
        spark.sparkContext.addPyFile(module_path)

    # Create distributed DataFrame and process via mapPartitions
    input_df = spark.createDataFrame(file_rows)
    distributed_df = input_df.repartition(num_partitions)

    # --- 4. Execute distributed conversion ---
    # Load series->mapping config on driver; bake into worker closure via partial.
    # The mapping dict is small (KB) and serialized with the closure automatically.
    repo_root = src_dir.parent.parent
    series_mapping = load_series_mapping(repo_root)
    logger.info(f"Series mappings loaded: {len(series_mapping)} series configured")
    process_partition = functools.partial(_process_partition, series_mapping=series_mapping)
    results_rdd = distributed_df.rdd.mapPartitions(process_partition)
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
