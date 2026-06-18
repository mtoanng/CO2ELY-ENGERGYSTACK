"""Generic Spark utilities for the CO2 ELY pipeline.

This module contains ONLY Spark-native helpers with ZERO heavy dependencies
(no polars, no pyarrow, no azure-sdk). Safe to import from ANY job layer.

Functions:
    build_abfss_path   - Build abfss:// URI from components
    configure_logger   - Structured logger for pipeline tasks
    get_job_args       - Standard --env / --is_integration_test arg parsing

For Delta writes, use Spark's native API directly:
    df.write.format("delta").mode("append").saveAsTable(table_name)
"""

import sys
import logging
import argparse

from pyspark.sql import SparkSession


# =============================================================================
# LOGGING
# =============================================================================

def configure_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure a structured logger for pipeline tasks.

    Args:
        name: Logger name (typically module name, e.g. 'enrich_timeseries').
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


# =============================================================================
# ARGUMENT PARSING
# =============================================================================

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


# =============================================================================
# ADLS PATH BUILDER
# =============================================================================

def build_abfss_path(storage_account: str, container: str, path: str) -> str:
    """Build abfss:// URI for ADLS access via UC External Location.

    Args:
        storage_account: e.g. "stpsbdodxdev2datalake"
        container: e.g. "co2elyd-data"
        path: blob path within container

    Returns:
        Full abfss:// URI.
    """
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
