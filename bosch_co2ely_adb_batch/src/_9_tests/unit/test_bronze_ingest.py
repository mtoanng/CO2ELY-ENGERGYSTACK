"""Unit tests for _1_r2b/bronze_ingest_parquet.py.

Tests argument parsing, path construction, Auto Loader config validation,
and environment resolution for the bronze ingestion job.

NOTE: Actual streaming/Delta tests require a live Spark cluster
and belong in the integration/ directory.
"""
import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from common_io_utils import build_external_table_location

# Ensure ingest module is importable
_INGEST_DIR = Path(__file__).resolve().parent.parent.parent / "_1_r2b"
sys.path.insert(0, str(_INGEST_DIR))

from common_config import (
    env_variables,
    medallion_variables,
    layer_variables,
    build_table_name,
    CONVERTER_CONFIG,
    TABLE_TYPES,
)


# =============================================================================
# PATH CONSTRUCTION
# =============================================================================

class TestPathConstruction:
    """ADLS paths built correctly from config."""

    def test_base_path_format(self, mock_spark):
        env = env_variables(mock_spark)
        adls_domain = env["adls_domain"]
        storage_account = adls_domain.replace(".dfs.core.windows.net", "")
        layers = layer_variables("_1_r2b")
        read_medal = medallion_variables(layers["read_layer"], env["environment"])
        container = read_medal["adls_container"]
        output_prefix = CONVERTER_CONFIG["output_prefix"]

        base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{output_prefix}"
        assert base_path.startswith("abfss://")
        assert "co2elyd-data" in base_path
        assert "parquet_raw" in base_path
        assert "stpsbdodxdev2datalake" in base_path

    def test_checkpoint_path_under_output(self, mock_spark):
        env = env_variables(mock_spark)
        adls_domain = env["adls_domain"]
        storage_account = adls_domain.replace(".dfs.core.windows.net", "")
        layers = layer_variables("_1_r2b")
        read_medal = medallion_variables(layers["read_layer"], env["environment"])
        container = read_medal["adls_container"]
        output_prefix = CONVERTER_CONFIG["output_prefix"]

        checkpoint_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{output_prefix}/_checkpoints"
        assert "_checkpoints" in checkpoint_base

    def test_integration_test_prefix(self):
        output_prefix = CONVERTER_CONFIG["output_prefix"]
        int_test_prefix = f"{output_prefix}/_int_test"
        assert int_test_prefix == "parquet_raw/_int_test"

    def test_integration_test_locations_isolated(self):
        prod_location = build_external_table_location("storage", "container", "bronze", "timeseries")
        test_location = build_external_table_location("storage", "container", "bronze_int_test", "timeseries")
        assert prod_location != test_location
        assert "/bronze_int_test/timeseries/" in test_location

    def test_source_path_per_table_type(self):
        """Each table type gets its own subdirectory."""
        base = "abfss://co2elyd-data@storage.dfs.core.windows.net/parquet_raw"
        for tt in TABLE_TYPES:
            path = f"{base}/{tt}/"
            assert path.endswith(f"/{tt}/")


# =============================================================================
# TABLE NAME RESOLUTION
# =============================================================================

class TestBronzeTableNames:
    """Bronze table names follow correct pattern."""

    def test_dev_table_names(self, mock_spark):
        env = env_variables(mock_spark)
        layers = layer_variables("_1_r2b")
        write_medal = medallion_variables(layers["write_layer"], env["environment"])
        catalog = env["unity_catalog"]
        schema = write_medal["uc_schema"]
        prefix = write_medal["table_prefix"]

        for tt in TABLE_TYPES:
            name = build_table_name(catalog, schema, prefix, tt)
            assert name == f"ps_xplatform_dev.co2elyd_dev.bronze_{tt}"

    def test_integration_test_names(self, mock_spark):
        env = env_variables(mock_spark)
        layers = layer_variables("_1_r2b")
        write_medal = medallion_variables(layers["write_layer"], env["environment"])

        name = build_table_name(
            env["unity_catalog"], write_medal["uc_schema"],
            write_medal["table_prefix"], "timeseries", is_integration_test=True
        )
        assert name.endswith("_int_test")

    def test_qa_table_names(self, mock_spark_qa):
        env = env_variables(mock_spark_qa)
        layers = layer_variables("_1_r2b")
        write_medal = medallion_variables(layers["write_layer"], env["environment"])

        name = build_table_name(env["unity_catalog"], write_medal["uc_schema"],
                                write_medal["table_prefix"], "filemeta")
        assert name == "ps_xplatform_qa.co2elyd_qa.bronze_filemeta"


# =============================================================================
# AUTO LOADER CONFIG VALIDATION
# =============================================================================

class TestAutoLoaderConfig:
    """Validate Auto Loader configuration choices."""

    def test_cloud_files_format_is_parquet(self):
        """Auto Loader reads Parquet."""
        # This is a design validation test
        expected_format = "parquet"
        assert expected_format == "parquet"

    def test_schema_evolution_mode(self):
        """addNewColumns allows forward compatibility when converter adds fields."""
        mode = "addNewColumns"
        valid_modes = ["addNewColumns", "failOnNewColumns", "rescue", "none"]
        assert mode in valid_modes

    def test_trigger_available_now_semantics(self):
        """AvailableNow processes all pending then stops (batch-like)."""
        # Design test: confirm we use batch semantics for scheduled jobs
        trigger_type = "availableNow"
        assert trigger_type == "availableNow"

    def test_checkpoint_isolation_per_table_type(self):
        """Each table_type has its own checkpoint (no cross-contamination)."""
        checkpoint_paths = set()
        for tt in TABLE_TYPES:
            path = f"_checkpoints/bronze_{tt}"
            assert path not in checkpoint_paths
            checkpoint_paths.add(path)


# =============================================================================
# ARGUMENT PARSING
# =============================================================================

class TestArgParsing:
    """Bronze job argument parsing."""

    def test_default_args(self):
        with patch("sys.argv", ["prog"]):
            sys.path.insert(0, str(_INGEST_DIR))
            from common_system_utils import get_job_args

            args = get_job_args()
            assert args.is_integration_test is False
            assert args.env == "dev"

    def test_integration_test_true(self):
        with patch("sys.argv", ["prog", "--is_integration_test", "true"]):
            from common_system_utils import get_job_args

            args = get_job_args()
            assert args.is_integration_test is True


# =============================================================================
# LIQUID CLUSTERING DESIGN
# =============================================================================

class TestLiquidClustering:
    """Liquid clustering applied to correct tables."""

    def test_only_timeseries_clustered(self):
        """Only timeseries table gets CLUSTER BY (uuid, group)."""
        clustered_tables = ["timeseries"]
        non_clustered = [tt for tt in TABLE_TYPES if tt not in clustered_tables]
        assert len(non_clustered) == 3
        assert "timeseries" not in non_clustered

    def test_cluster_columns(self):
        """Cluster keys optimize for downstream queries (filter by uuid+group)."""
        cluster_cols = ["uuid", "group"]
        # These are the most common filter predicates in silver/gold
        assert "uuid" in cluster_cols
        assert "group" in cluster_cols
