"""ELY project environment & medallion config (aligned with TBP-ADB pemely convention).

Three-tier config structure:
  ENVIRONMENT_VARIABLES  -> workspace URL -> {env, adls_domain, catalog}
  MEDALLION_VARIABLES    -> layer         -> {container, schema, auth_group, table_prefix}
  LAYER_VARIABLES        -> pipeline step -> {read_layer, write_layer}

Usage:
    from common_config import (
        env_variables, medallion_variables, layer_variables, build_table_name,
    )
    env = env_variables(spark)
    medal = medallion_variables("bronze", env["environment"])
    table = build_table_name(env["unity_catalog"], medal["uc_schema"],
                             medal["table_prefix"], "timeseries")
    # -> ps_xplatform_dev.co2elyd_dev.bronze_timeseries
"""
import logging

logger = logging.getLogger("ely_converter")


# =============================================================================
# ENVIRONMENT VARIABLES (workspace URL -> environment config)
# =============================================================================

ENVIRONMENT_VARIABLES = {
    "adb-1032635496032522.2.azuredatabricks.net": {
        "environment": "dev",
        "adls_domain": "stpsbdodxdev2datalake.dfs.core.windows.net",
        "unity_catalog": "ps_xplatform_dev",
    },
    "adb-7376334951991000.0.azuredatabricks.net": {
        "environment": "qa",
        "adls_domain": "stpsbdodxqadatalake.dfs.core.windows.net",
        "unity_catalog": "ps_xplatform_qa",
    },
    "adb-5407587042408609.9.azuredatabricks.net": {
        "environment": "prod",
        "adls_domain": "stpsbdodxproddatalake.dfs.core.windows.net",
        "unity_catalog": "ps_xplatform_prod",
    },
    "local": {
        "environment": "local",
        "adls_domain": None,
        "unity_catalog": None,
    },
}


# =============================================================================
# MEDALLION VARIABLES (layer -> ADLS + UC + auth config)
# =============================================================================

MEDALLION_VARIABLES = {
    "raw": {
        "adls_container": "co2elyd-data",
        "uc_schema": "co2elyd",
        "authorization_group": "idm2bcd_dssi03prod_co2elyd_data",
        "table_prefix": "raw",
    },
    "bronze": {
        "adls_container": "co2elyd-data",
        "uc_schema": "co2elyd",
        "authorization_group": "idm2bcd_dssi03prod_co2elyd_data",
        "table_prefix": "bronze",
    },
    "silver": {
        "adls_container": "co2elyd-data",
        "uc_schema": "co2elyd",
        "authorization_group": "idm2bcd_dssi03prod_co2elyd_data",
        "table_prefix": "silver",
    },
    "gold": {
        "adls_container": "co2elyd-data",
        "uc_schema": "co2elyd",
        "authorization_group": "idm2bcd_dssi03prod_co2elyd_data",
        "table_prefix": "gold",
    },
}


# =============================================================================
# LAYER VARIABLES (pipeline step -> read/write medallion layers)
# =============================================================================

LAYER_VARIABLES = {
    "_0_convert": {"read_layer": "raw", "write_layer": "raw"},
    "_1_ingest": {"read_layer": "raw", "write_layer": "bronze"},
    "_2_enrich": {"read_layer": "bronze", "write_layer": "silver"},
    "_3_gold": {"read_layer": "silver", "write_layer": "gold"},
}


# =============================================================================
# CONVERTER-SPECIFIC CONFIG (ADLS prefixes within raw layer)
# =============================================================================

CONVERTER_CONFIG = {
    "source_prefix": "raw_data",
    "output_prefix": "parquet_raw",
    "tracking_table_name": "file_tracking",
}

# Output table types produced by the converter (4 Parquet table types).
# Defined here (not in common.py) so downstream stages can import without
# pulling in heavy dependencies (polars, pyarrow) that converter needs.
TABLE_TYPES = ["filemeta", "channel", "timeseries", "statistics"]


# =============================================================================
# HELPER FUNCTIONS (pemely-compatible API)
# =============================================================================

def env_variables(spark, env_override: str = None) -> dict:
    """Retrieve environment config from workspace URL.

    Args:
        spark: Active SparkSession instance.
        env_override: Force a specific environment (e.g. "local").

    Returns:
        dict with keys: environment, adls_domain, unity_catalog.
    """
    if env_override == "local":
        return ENVIRONMENT_VARIABLES["local"]

    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        logger.warning("Workspace URL not found (local mode?)")
        return ENVIRONMENT_VARIABLES["local"]

    config = ENVIRONMENT_VARIABLES.get(workspace_url)
    if config is None:
        logger.warning(f"Unrecognized workspace: {workspace_url}, using dev defaults")
        return ENVIRONMENT_VARIABLES["adb-1032635496032522.2.azuredatabricks.net"]

    return config


def medallion_variables(layer: str, env: str = None) -> dict:
    """Get medallion config for a layer, resolving uc_schema with env suffix.

    Args:
        layer: Medallion layer name ("raw", "bronze", "silver", "gold").
        env: Environment name ("dev", "qa", "prod") for schema resolution.

    Returns:
        dict with keys: adls_container, uc_schema (resolved, e.g. "co2elyd_dev"),
        authorization_group, table_prefix.
    """
    medal = MEDALLION_VARIABLES[layer].copy()
    if env and medal.get("uc_schema"):
        medal["uc_schema"] = f"{medal['uc_schema']}_{env}"
    return medal


def layer_variables(step: str) -> dict:
    """Get read/write layer mapping for a pipeline step.

    Args:
        step: Pipeline step name (e.g. "_1_ingest", "_2_enrich").

    Returns:
        dict with keys: read_layer, write_layer (medallion layer names).
    """
    return LAYER_VARIABLES[step]


def build_table_name(
    unity_catalog: str, schema: str, prefix: str, table: str,
    is_integration_test: bool = False,
) -> str:
    """Build fully qualified table name.

    Pattern: {catalog}.{schema}.{prefix}_{table}[_int_test]
    Example: ps_xplatform_dev.co2elyd_dev.bronze_timeseries

    Args:
        unity_catalog: UC catalog name (e.g. "ps_xplatform_dev").
        schema: UC schema name (e.g. "co2elyd_dev").
        prefix: Medallion layer prefix (e.g. "bronze", "silver").
        table: Base table name (e.g. "timeseries", "filemeta").
        is_integration_test: If True, appends "_int_test" suffix.

    Returns:
        Fully qualified table name string.
    """
    suffix = "_int_test" if is_integration_test else ""
    return f"{unity_catalog}.{schema}.{prefix}_{table}{suffix}"


# =============================================================================
# BACKWARD-COMPATIBLE WRAPPERS (used by _0_convert/common.py)
# =============================================================================

def get_env_variables(spark) -> dict:
    """Backward-compatible: returns flat dict matching old ENVIRONMENT_CONFIG style.

    Returns dict with keys: environment, storage_account, container,
    unity_catalog, unity_schema.
    """
    env = env_variables(spark)
    environment = env["environment"]
    medal = medallion_variables("raw", environment)

    # Extract storage_account from adls_domain (strip .dfs.core.windows.net)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "") if adls_domain else None

    return {
        "environment": environment,
        "storage_account": storage_account,
        "container": medal["adls_container"],
        "unity_catalog": env["unity_catalog"],
        "unity_schema": medal["uc_schema"],
    }


def get_adls_config(env_vars: dict) -> dict:
    """Backward-compatible: resolve ADLS paths for converter.

    Returns dict with: storage_account, container, source_prefix,
    output_prefix, tracking_table.
    """
    catalog = env_vars["unity_catalog"]
    schema = env_vars["unity_schema"]
    return {
        "storage_account": env_vars["storage_account"],
        "container": env_vars["container"],
        "source_prefix": CONVERTER_CONFIG["source_prefix"],
        "output_prefix": CONVERTER_CONFIG["output_prefix"],
        "tracking_table": f"{catalog}.{schema}.{CONVERTER_CONFIG['tracking_table_name']}",
    }
