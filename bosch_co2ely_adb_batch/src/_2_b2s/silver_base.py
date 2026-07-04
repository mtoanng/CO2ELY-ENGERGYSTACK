"""Compatibility entry point for the silver layer.

This module does not publish silver tables directly. The active silver outputs
are produced by the dedicated dimension jobs.
"""

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession

from _5_common.common_utils import (
    configure_logger,
    env_variables,
    get_job_args,
)

logger = configure_logger("silver_base")


def main():
    """Log the active silver entry points and exit."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]

    logger.info(f"{'='*60}")
    logger.info("Silver Base — DEPRECATED (no-op)")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Silver dims are now built by:")
    logger.info(f"    silver_dim_experiment.py")
    logger.info(f"    silver_dim_signal.py")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
