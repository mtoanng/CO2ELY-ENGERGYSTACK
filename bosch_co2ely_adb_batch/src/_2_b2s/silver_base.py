"""Silver base layer — DEPRECATED.

This module previously published bronze tables as silver dim/fact copies.
Silver dimensions are now handled by dedicated scripts:
  - silver_dim_experiment.py  (assigns experiment_id)
  - silver_dim_signal.py      (assigns signal_id + std_channel mapping)

This file is retained as a no-op entry point so existing job references
don't break during the migration. It can be removed once all job YAMLs
are updated to point to the new scripts directly.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/silver_base.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
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
    """No-op — silver dims are now built by dedicated scripts."""
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
