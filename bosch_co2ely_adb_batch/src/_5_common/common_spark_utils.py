"""BACKWARD-COMPATIBILITY SHIM.

Functions have been moved to purpose-specific modules:
    configure_logger, get_job_args        -> common_system_utils.py
    build_abfss_path, build_external_table_location -> common_io_utils.py

New code should import from the canonical modules directly.
"""

from common_system_utils import configure_logger, get_job_args  # noqa: F401
from common_io_utils import build_abfss_path, build_external_table_location  # noqa: F401

__all__ = [
    "configure_logger",
    "get_job_args",
    "build_abfss_path",
    "build_external_table_location",
]
