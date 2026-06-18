"""Shared utilities for the CO2 energystack pipeline.

THIS IS A BACKWARD-COMPATIBILITY SHIM. All config is canonical in common_config.py.
Downstream layers (_2_enrich, _3_gold) import from here for historical reasons.
New code should import directly from common_config and spark_utils.

Re-exports from common_config:
    env_variables, medallion_variables, layer_variables, build_table_name

Re-exports from spark_utils:
    configure_logger, get_job_args, build_abfss_path
"""

# Re-export config functions (canonical source: common_config.py)
from common_config import (
    env_variables,
    medallion_variables,
    layer_variables,
    build_table_name,
)

# Re-export generic utilities (canonical source: spark_utils.py)
from spark_utils import (
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
