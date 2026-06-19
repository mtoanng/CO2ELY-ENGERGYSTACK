"""Shared utilities for the CO2 energystack pipeline.

New code should import directly from the canonical modules:

    common_config.py        -> env_variables, medallion_variables, layer_variables, build_table_name
    common_system_utils.py  -> configure_logger, get_job_args
    common_io_utils.py      -> build_abfss_path, build_external_table_location, write_to_delta
"""

# Re-export config functions (canonical source: common_config.py)
try:
    from _5_common.common_config import (
        env_variables,
        medallion_variables,
        layer_variables,
        build_table_name,
    )
except ImportError:
    from .common_config import (
        env_variables,
        medallion_variables,
        layer_variables,
        build_table_name,
    )

# Re-export system utilities (canonical source: common_system_utils.py)
try:
    from _5_common.common_system_utils import configure_logger, get_job_args
except ImportError:
    from .common_system_utils import configure_logger, get_job_args

# Re-export ADLS/IO utilities (canonical source: common_io_utils.py)
try:
    from _5_common.common_io_utils import build_abfss_path, build_external_table_location
except ImportError:
    from .common_io_utils import build_abfss_path, build_external_table_location

__all__ = [
    "env_variables",
    "medallion_variables",
    "layer_variables",
    "build_table_name",
    "configure_logger",
    "get_job_args",
    "build_abfss_path",
    "build_external_table_location",
]
