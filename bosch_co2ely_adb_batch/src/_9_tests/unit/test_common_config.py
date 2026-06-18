"""Unit tests for _5_common/common_config.py.

Tests environment resolution, medallion layer config, table name building,
and backward-compat wrappers. No Spark cluster needed (uses mock SparkSession).
"""
import pytest
from common_config import (
    ENVIRONMENT_VARIABLES,
    MEDALLION_VARIABLES,
    LAYER_VARIABLES,
    CONVERTER_CONFIG,
    TABLE_TYPES,
    env_variables,
    medallion_variables,
    layer_variables,
    build_table_name,
    get_env_variables,
    get_adls_config,
)


# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================

class TestEnvVariables:
    """Environment detection from workspace URL."""

    def test_dev_workspace_detected(self, mock_spark):
        env = env_variables(mock_spark)
        assert env["environment"] == "dev"
        assert env["unity_catalog"] == "ps_xplatform_dev"
        assert "dev2datalake" in env["adls_domain"]

    def test_qa_workspace_detected(self, mock_spark_qa):
        env = env_variables(mock_spark_qa)
        assert env["environment"] == "qa"
        assert env["unity_catalog"] == "ps_xplatform_qa"

    def test_prod_workspace_detected(self, mock_spark_prod):
        env = env_variables(mock_spark_prod)
        assert env["environment"] == "prod"
        assert env["unity_catalog"] == "ps_xplatform_prod"

    def test_local_fallback_on_exception(self, mock_spark_local):
        env = env_variables(mock_spark_local)
        assert env["environment"] == "local"
        assert env["adls_domain"] is None

    def test_local_override(self, mock_spark):
        env = env_variables(mock_spark, env_override="local")
        assert env["environment"] == "local"

    def test_unknown_workspace_falls_back_to_dev(self, mock_spark):
        mock_spark.conf.get.return_value = "adb-unknown.azuredatabricks.net"
        env = env_variables(mock_spark)
        assert env["environment"] == "dev"  # fallback

    def test_all_envs_have_required_keys(self):
        required_keys = {"environment", "adls_domain", "unity_catalog"}
        for host, config in ENVIRONMENT_VARIABLES.items():
            assert required_keys.issubset(config.keys()), f"Missing keys in {host}"


# =============================================================================
# MEDALLION VARIABLES
# =============================================================================

class TestMedallionVariables:
    """Medallion layer configuration and schema resolution."""

    def test_all_layers_exist(self):
        for layer in ["raw", "bronze", "silver", "gold"]:
            result = medallion_variables(layer, "dev")
            assert "uc_schema" in result
            assert "adls_container" in result
            assert "table_prefix" in result

    def test_schema_suffix_appended(self):
        result = medallion_variables("bronze", "dev")
        assert result["uc_schema"] == "co2elyd_dev"

    def test_schema_suffix_qa(self):
        result = medallion_variables("silver", "qa")
        assert result["uc_schema"] == "co2elyd_qa"

    def test_schema_suffix_prod(self):
        result = medallion_variables("gold", "prod")
        assert result["uc_schema"] == "co2elyd_prod"

    def test_no_env_returns_base_schema(self):
        result = medallion_variables("bronze")
        # When env=None, uc_schema is not suffixed
        assert result["uc_schema"] == "co2elyd"

    def test_container_consistent_across_layers(self):
        """All layers use same ADLS container."""
        containers = {medallion_variables(l, "dev")["adls_container"]
                      for l in ["raw", "bronze", "silver", "gold"]}
        assert len(containers) == 1
        assert "co2elyd-data" in containers

    def test_invalid_layer_raises(self):
        with pytest.raises(KeyError):
            medallion_variables("invalid_layer", "dev")


# =============================================================================
# LAYER VARIABLES
# =============================================================================

class TestLayerVariables:
    """Pipeline step -> read/write layer mapping."""

    def test_convert_reads_raw_writes_raw(self):
        result = layer_variables("_0_convert")
        assert result == {"read_layer": "raw", "write_layer": "raw"}

    def test_ingest_reads_raw_writes_bronze(self):
        result = layer_variables("_1_ingest")
        assert result == {"read_layer": "raw", "write_layer": "bronze"}

    def test_enrich_reads_bronze_writes_silver(self):
        result = layer_variables("_2_enrich")
        assert result == {"read_layer": "bronze", "write_layer": "silver"}

    def test_gold_reads_silver_writes_gold(self):
        result = layer_variables("_3_gold")
        assert result == {"read_layer": "silver", "write_layer": "gold"}

    def test_invalid_step_raises(self):
        with pytest.raises(KeyError):
            layer_variables("_99_invalid")


# =============================================================================
# BUILD TABLE NAME
# =============================================================================

class TestBuildTableName:
    """Fully qualified table name construction."""

    def test_standard_name(self):
        result = build_table_name("ps_xplatform_dev", "co2elyd_dev", "bronze", "timeseries")
        assert result == "ps_xplatform_dev.co2elyd_dev.bronze_timeseries"

    def test_integration_test_suffix(self):
        result = build_table_name("ps_xplatform_dev", "co2elyd_dev", "bronze", "filemeta", True)
        assert result == "ps_xplatform_dev.co2elyd_dev.bronze_filemeta_int_test"

    def test_no_integration_test_suffix(self):
        result = build_table_name("catalog", "schema", "silver", "channel", False)
        assert result == "catalog.schema.silver_channel"
        assert "_int_test" not in result

    def test_all_table_types_produce_valid_names(self):
        for tt in TABLE_TYPES:
            name = build_table_name("cat", "sch", "bronze", tt)
            parts = name.split(".")
            assert len(parts) == 3
            assert parts[2].startswith("bronze_")


# =============================================================================
# CONVERTER CONFIG
# =============================================================================

class TestConverterConfig:
    """Converter-specific config constants."""

    def test_required_keys(self):
        assert "source_prefix" in CONVERTER_CONFIG
        assert "output_prefix" in CONVERTER_CONFIG
        assert "tracking_table_name" in CONVERTER_CONFIG

    def test_output_prefix_is_parquet_raw(self):
        assert CONVERTER_CONFIG["output_prefix"] == "parquet_raw"

    def test_tracking_table_name(self):
        assert CONVERTER_CONFIG["tracking_table_name"] == "file_tracking"


# =============================================================================
# TABLE TYPES
# =============================================================================

class TestTableTypes:
    """TABLE_TYPES constant consistency."""

    def test_four_types(self):
        assert len(TABLE_TYPES) == 4

    def test_expected_types(self):
        assert set(TABLE_TYPES) == {"filemeta", "channel", "timeseries", "statistics"}


# =============================================================================
# BACKWARD-COMPAT WRAPPERS
# =============================================================================

class TestBackwardCompatWrappers:
    """get_env_variables() and get_adls_config() wrappers."""

    def test_get_env_variables_returns_flat_dict(self, mock_spark):
        result = get_env_variables(mock_spark)
        assert "environment" in result
        assert "storage_account" in result
        assert "container" in result
        assert "unity_catalog" in result
        assert "unity_schema" in result

    def test_get_env_variables_storage_account_extracted(self, mock_spark):
        result = get_env_variables(mock_spark)
        assert result["storage_account"] == "stpsbdodxdev2datalake"

    def test_get_adls_config_returns_paths(self, mock_spark):
        env = get_env_variables(mock_spark)
        config = get_adls_config(env)
        assert "storage_account" in config
        assert "container" in config
        assert "source_prefix" in config
        assert "output_prefix" in config
        assert "tracking_table" in config

    def test_get_adls_config_tracking_table_fully_qualified(self, mock_spark):
        env = get_env_variables(mock_spark)
        config = get_adls_config(env)
        parts = config["tracking_table"].split(".")
        assert len(parts) == 3  # catalog.schema.table
