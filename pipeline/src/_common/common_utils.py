"""Shared utilities for the CO₂ energystack pipeline.

Migrated from CO_energystacck Dash app patterns + bosch_ely_adb_batch conventions:
- Environment detection (dev/qa/prod) via workspace host
- Medallion layer variables (catalog, schema, ADLS paths)
- Argument parsing for job parameters
- Structured logging
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Environment configuration (mirrors bosch_ely_adb_batch pattern)
# ---------------------------------------------------------------------------

ENVIRONMENT_VARIABLES = {
    "adb-1032635496032522.2.azuredatabricks.net": {
        "environment": "dev",
        "adls_domain": "stpsbdodxdevdatalake.dfs.core.windows.net",
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

# Medallion layer config
# NOTE: Update uc_schema values when CO₂ gets its own schema
MEDALLION_VARIABLES = {
    "raw": {
        "adls_container": "pemely-data",
        "uc_schema": None,
        "table_prefix": "raw",
    },
    "bronze": {
        "adls_container": "pemely-dev",
        "uc_schema": "pemely_dev",
        "table_prefix": "bronze",
    },
    "silver": {
        "adls_container": "pemely-dev",
        "uc_schema": "pemely_dev",
        "table_prefix": "silver",
    },
    "gold": {
        "adls_container": "pemely-ops",
        "uc_schema": "pemely_ops",
        "table_prefix": "gold",
    },
}

LAYER_VARIABLES = {
    "_1_ingest": {"read_layer": "raw", "write_layer": "bronze"},
    "_2_enrich": {"read_layer": "bronze", "write_layer": "silver"},
    "_3_gold": {"read_layer": "silver", "write_layer": "gold"},
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure a structured logger for pipeline tasks.

    Args:
        name: Logger name (typically module name, e.g. 'ingest_excel').
        level: Logging level. Default: INFO.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(f"co_energystack.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# Environment & argument parsing
# ---------------------------------------------------------------------------

def get_job_args() -> argparse.Namespace:
    """Parse standard job arguments (--env, --is_integration_test).

    Returns:
        Namespace with .env (str) and .is_integration_test (bool).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, default="dev")
    parser.add_argument("--is_integration_test", type=str, default="false")
    args, _ = parser.parse_known_args()
    args.is_integration_test = args.is_integration_test.lower() == "true"
    return args


def env_variables(spark: SparkSession, env_override: str = None) -> dict:
    """Resolve environment variables from workspace host or override.

    Args:
        spark: Active SparkSession.
        env_override: Optional explicit environment name (from --env arg).

    Returns:
        Dict with keys: environment, adls_domain, unity_catalog.
    """
    if env_override and env_override != "dev":
        for _host, config in ENVIRONMENT_VARIABLES.items():
            if config["environment"] == env_override:
                return config

    # Auto-detect from Spark conf
    try:
        host = spark.conf.get("spark.databricks.workspaceUrl", "local")
    except Exception:
        host = "local"

    return ENVIRONMENT_VARIABLES.get(host, ENVIRONMENT_VARIABLES["local"])


def medallion_variables(layer: str) -> dict:
    """Get medallion-layer configuration.

    Args:
        layer: One of 'raw', 'bronze', 'silver', 'gold'.

    Returns:
        Dict with adls_container, uc_schema, table_prefix.
    """
    return MEDALLION_VARIABLES[layer]


def layer_variables(layer_module: str) -> dict:
    """Get read/write layer mapping for a source module.

    Args:
        layer_module: Module name like '_1_ingest', '_2_enrich', '_3_gold'.

    Returns:
        Dict with read_layer and write_layer.
    """
    return LAYER_VARIABLES[layer_module]


def build_table_name(
    unity_catalog: str,
    schema: str,
    prefix: str,
    table: str,
    is_integration_test: bool = False,
) -> str:
    """Build a fully qualified Unity Catalog table name.

    Pattern: {catalog}.{schema}.{prefix}_{table}[_int_test]
    Example: ps_xplatform_dev.pemely_dev.bronze_co2_timeseries

    Args:
        unity_catalog: Catalog name.
        schema: Schema name.
        prefix: Medallion prefix (bronze/silver/gold).
        table: Table base name.
        is_integration_test: If True, appends '_int_test' suffix.

    Returns:
        Fully qualified three-part table name.
    """
    suffix = "_int_test" if is_integration_test else ""
    return f"{unity_catalog}.{schema}.{prefix}_{table}{suffix}"
