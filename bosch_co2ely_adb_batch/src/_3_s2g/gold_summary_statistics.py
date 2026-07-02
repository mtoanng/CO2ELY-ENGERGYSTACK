"""DEPRECATED — Summary statistics merged into gold_serving_tables.py.

KPIs are now computed inside gold_serving_tables.build_experiment_index()
using the 1-min aggregate (weighted mean, metric-specific logic).

This file is kept as a no-op so any stale job references don't crash.
Remove after all job YAMLs are confirmed updated.
"""

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from _5_common.common_utils import configure_logger

logger = configure_logger("gold_summary_statistics")


def main():
    logger.info("gold_summary_statistics — DEPRECATED (no-op).")
    logger.info("KPIs are now computed in gold_serving_tables.py -> gold_experiment_index.")


if __name__ == "__main__":
    main()
