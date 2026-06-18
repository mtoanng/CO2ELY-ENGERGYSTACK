"""Shared utilities for the CO2 energystack pipeline.

THIS IS A BACKWARD-COMPATIBILITY SHIM. All config is canonical in common_config.py.
Downstream layers (_2_enrich, _3_gold) import from here for historical reasons.
New code should import directly from common_config and common_spark_utils.

Re-exports from common_config:
    env_variables, medallion_variables, layer_variables, build_table_name

Re-exports from common_spark_utils:
    configure_logger, get_job_args, build_abfss_path
"""

# Re-export config functions (canonical source: common_config.py)
from _5_common.common_config import (
    env_variables,
    medallion_variables,
    layer_variables,
    build_table_name,
)

# Re-export generic utilities (canonical source: common_spark_utils.py)
from _5_common.common_spark_utils import (
    configure_logger,
    get_job_args,
    build_abfss_path,
)

__all__ = [
    "env_variables",
    "medallion_variables",
    "layer_variables",
    "build_table_name",
    "configure_logger",
    "get_job_args",
    "build_abfss_path",
]
