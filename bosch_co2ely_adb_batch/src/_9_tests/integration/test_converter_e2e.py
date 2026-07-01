"""Integration tests for the converter pipeline (end-to-end).

REQUIRES: Live Databricks cluster with polars, azure-sdk, pyarrow.
Run via: pytest src/_9_tests/integration/ -v --timeout=300

These tests use REAL Azure Blob Storage (integration test prefix)
and REAL Unity Catalog tracking table (with _int_test suffix).
"""
import pytest
import os

# Skip entire module if not on a Databricks cluster
pytestmark = pytest.mark.skipif(
    "DATABRICKS_RUNTIME_VERSION" not in os.environ,
    reason="Integration tests require a live Databricks cluster",
)


class TestConverterE2E:
    """End-to-end converter: raw XLSX -> Parquet on ADLS + tracking table."""

    def test_single_file_conversion(self, spark):
        """Upload a test XLSX to int_test prefix, run converter, verify output."""
        # TODO: Implement when test infrastructure (int_test blob container) is ready
        # Steps:
        # 1. Upload sample_xlsx_bytes to raw_data/_int_test/test_file.xlsx
        # 2. Run converter with --is_integration_test true
        # 3. Verify tracking table has SUCCESS row
        # 4. Verify Parquet files exist at parquet_raw/_int_test/{table_type}/
        # 5. Read back Parquet and validate schemas
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_incremental_no_reprocess(self, spark):
        """Running converter twice on same file should not reprocess."""
        # Steps:
        # 1. First run: file processed, tracking = SUCCESS
        # 2. Second run: IncrementalTracker skips it (already tracked)
        # 3. Verify no duplicate output Parquet files
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_corrupt_file_tracked_as_failed(self, spark):
        """Corrupt file gets FAILED status in tracking table."""
        # Steps:
        # 1. Upload invalid bytes to raw_data/_int_test/corrupt.xlsx
        # 2. Run converter
        # 3. Verify tracking table has FAILED row with error_message
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_retry_exhausted_skipped(self, spark):
        """Files exceeding MAX_TOTAL_RETRIES are skipped."""
        # Steps:
        # 1. Manually insert FAILED row with retry_count=5
        # 2. Run converter with the same file still in ADLS
        # 3. Verify it's skipped (not reprocessed)
        pytest.skip("Awaiting int_test infrastructure setup")


class TestBronzeE2E:
    """End-to-end bronze ingestion: Parquet from converter -> Delta tables."""

    def test_auto_loader_ingests_new_files(self, spark):
        """New Parquet files are picked up by Auto Loader."""
        # Steps:
        # 1. Write sample Parquet to parquet_raw/_int_test/{table_type}/
        # 2. Run bronze_ingest_parquet with --is_integration_test true
        # 3. Verify Delta table has data
        # 4. Run again: no new rows added (checkpoint remembers)
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_schema_evolution(self, spark):
        """New columns in Parquet are auto-added to Delta schema."""
        # Steps:
        # 1. Write Parquet with columns [uuid, group, value]
        # 2. Run bronze ingest
        # 3. Write Parquet with columns [uuid, group, value, new_col]
        # 4. Run bronze ingest again
        # 5. Verify Delta table now has new_col
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_liquid_clustering_applied(self, spark):
        """Timeseries table has liquid clustering on (uuid, group)."""
        # Steps:
        # 1. Run bronze ingest
        # 2. DESCRIBE DETAIL on timeseries table
        # 3. Verify clusteringColumns contains uuid, group
        pytest.skip("Awaiting int_test infrastructure setup")

    def test_idempotent_rerun(self, spark):
        """Re-running bronze ingest doesn't duplicate rows."""
        # Steps:
        # 1. Run bronze ingest (ingests N rows)
        # 2. Run again (no new Parquet files)
        # 3. Verify row count unchanged
        pytest.skip("Awaiting int_test infrastructure setup")
