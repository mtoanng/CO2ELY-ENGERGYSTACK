"""ELY Data Converter - production entry point.

Converts raw XLSX/CSV measurement files from ADLS to normalized Parquet.
Uses ThreadPoolExecutor for file-level parallelism.
Tracks processed files in Delta table for incremental loads.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_0_convert/run_converter.py
        parameters:
          - "--tenant_id" / "--client_id" / "--client_secret"
          - "--storage_account" / "--container"
          - "--source_prefix" / "--output_prefix"
          - "--tracking_table" / "--max_workers"
"""
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current dir to path for sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, ContainerClient
from pyspark.sql import SparkSession

from common import (
    ConversionResult, FileInfo, ParquetWriter, IncrementalTracker,
    sanitize_name, logger,
)
import csv_converter
import xlsx_converter


# =============================================================================
# CONVERTER DISPATCH
# =============================================================================

def get_converter(extension: str):
    """Return the convert() function for a file extension."""
    return {
        ".csv": csv_converter.convert,
        ".xlsx": xlsx_converter.convert,
        ".xls": xlsx_converter.convert,
    }.get(extension.lower())


# =============================================================================
# PIPELINE
# =============================================================================

class ConverterPipeline:
    """Orchestrates end-to-end conversion.

    1. Discover new files (via IncrementalTracker + Delta)
    2. For each file: download -> convert -> write Parquet -> track
    3. Parallel execution with ThreadPoolExecutor
    """

    def __init__(self, storage_account, container, source_prefix, output_prefix,
                 credential, tracker, writer, max_workers=4):
        self.storage_account = storage_account
        self.container = container
        self.source_prefix = source_prefix
        self.output_prefix = output_prefix
        self.credential = credential
        self.tracker = tracker
        self.writer = writer
        self.max_workers = max_workers
        self._url = f"https://{storage_account}.blob.core.windows.net"

    def run(self) -> Dict:
        """Execute pipeline. Returns summary."""
        t_start = time.perf_counter()

        container_client = ContainerClient(
            account_url=self._url,
            container_name=self.container,
            credential=self.credential,
        )
        new_files = self.tracker.get_new_files(container_client, self.source_prefix)

        if not new_files:
            logger.info("No new files to process.")
            return {"status": "no_new_files", "total_time": 0}

        logger.info(f"Processing {len(new_files)} file(s) with {self.max_workers} workers")
        results = {"success": 0, "failed": 0, "total_rows": 0, "files": []}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._process_file, f): f for f in new_files}
            for future in as_completed(futures):
                r = future.result()
                if r["status"] == "success":
                    results["success"] += 1
                    results["total_rows"] += r.get("rows", 0)
                else:
                    results["failed"] += 1
                results["files"].append(r)

        results["total_time"] = time.perf_counter() - t_start
        results["status"] = "completed"
        logger.info(
            f"Pipeline complete: {results['success']} ok, {results['failed']} failed, "
            f"{results['total_rows']:,} rows, {results['total_time']:.1f}s"
        )
        return results

    def _process_file(self, file_info: FileInfo) -> Dict:
        """Download -> convert -> write -> track."""
        t0 = time.perf_counter()
        logger.info(f"  Processing: {file_info.file_name} ({file_info.file_size / 1_048_576:.1f} MB)")

        try:
            blob_client = BlobClient(
                account_url=self._url,
                container_name=self.container,
                blob_name=file_info.blob_path,
                credential=self.credential,
            )
            download = blob_client.download_blob()
            file_bytes = download.readall()

            converter = get_converter(file_info.extension)
            if not converter:
                raise ValueError(f"No converter for: {file_info.extension}")

            conversion_results = converter(
                file_bytes, file_info.blob_path, file_info.file_size,
                last_modified=file_info.last_modified,
            )
            del file_bytes

            base_filename = sanitize_name(file_info.file_name.rsplit(".", 1)[0])
            all_paths = {}
            total_rows = 0

            for cr in conversion_results:
                paths = self.writer.write_result(cr, base_filename)
                all_paths.update(paths)
                total_rows += cr.n_rows

            duration = time.perf_counter() - t0
            self.tracker.mark_success(file_info, all_paths, duration)
            logger.info(f"  \u2713 {file_info.file_name}: {total_rows:,} rows, {duration:.1f}s")
            return {"status": "success", "file": file_info.file_name, "rows": total_rows, "duration": duration}

        except Exception as e:
            duration = time.perf_counter() - t0
            self.tracker.mark_failed(file_info, str(e), duration)
            logger.error(f"  \u2717 {file_info.file_name}: {e}")
            return {"status": "failed", "file": file_info.file_name, "error": str(e), "duration": duration}


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ELY Data Converter")
    parser.add_argument("--tenant_id", required=True)
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--client_secret", required=True)
    parser.add_argument("--storage_account", default="stpsbdodxdev2datalake")
    parser.add_argument("--container", default="co2elyd-data")
    parser.add_argument("--source_prefix", default="test/")
    parser.add_argument("--output_prefix", default="parquet_raw")
    parser.add_argument("--tracking_table", default="co2elyd_dev.converter.file_tracking")
    parser.add_argument("--max_workers", type=int, default=4)
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    credential = ClientSecretCredential(args.tenant_id, args.client_id, args.client_secret)

    tracker = IncrementalTracker(args.tracking_table, spark)
    writer = ParquetWriter(
        storage_account=args.storage_account,
        container=args.container,
        output_prefix=args.output_prefix,
        credential=credential,
    )

    pipeline = ConverterPipeline(
        storage_account=args.storage_account,
        container=args.container,
        source_prefix=args.source_prefix,
        output_prefix=args.output_prefix,
        credential=credential,
        tracker=tracker,
        writer=writer,
        max_workers=args.max_workers,
    )

    results = pipeline.run()
    print(f"\n{'='*60}")
    print(f"Pipeline Summary:")
    print(f"  Status: {results['status']}")
    print(f"  Succeeded: {results.get('success', 0)}")
    print(f"  Failed: {results.get('failed', 0)}")
    print(f"  Total rows: {results.get('total_rows', 0):,}")
    print(f"  Total time: {results.get('total_time', 0):.1f}s")


if __name__ == "__main__":
    main()
