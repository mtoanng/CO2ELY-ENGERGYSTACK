"""System: OPTIMIZE and cluster Delta tables for query performance.

Runs OPTIMIZE (bin-packing) and ensures liquid clustering is configured
on all co2ely Delta tables.  Targeted at Plotly.js / Databricks SQL Warehouse
query patterns:

  App filters:  WHERE series = ? AND std_channel IN (?)
  Drill-down:   WHERE series = ? AND std_channel = ? AND experiment_id = ?

Liquid clustering on (series, std_channel, experiment_id) gives the best
I/O skip for the interactive chart query patterns.

Scheduled weekly or on-demand.
"""
import sys
import argparse
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent / "_5_common"))

from pyspark.sql import SparkSession
from common_config import env_variables, medallion_variables, build_table_name
from common_system_utils import configure_logger

logger = configure_logger("sys_optimize_tables")


# Tables and their liquid-cluster keys, ordered Bronze → Silver → Gold.
_TABLE_SPECS = [
    # (table_suffix, prefix, cluster_cols)
    # Bronze (raw staging — clustered for Gold reads)
    ("timeseries",               "bronze", ["uuid", "group"]),
    ("channel",                  "bronze", ["uuid", "group"]),
    ("filemeta",                 "bronze", ["uuid"]),
    ("statistics",               "bronze", ["uuid", "group"]),
    # Silver dims (small — clustered for broadcast-join reads)
    ("dim_experiment",           "silver", ["series", "experiment_id"]),
    ("dim_signal",               "silver", ["experiment_id"]),
    # Gold facts (clustered for app query pattern: series → std_channel → experiment)
    ("timeseries",               "gold",   ["series", "std_channel", "experiment_id"]),
    ("timeseries_agg_1min",      "gold",   ["series", "std_channel", "experiment_id"]),
    ("timeseries_agg_15min",     "gold",   ["series", "std_channel", "experiment_id"]),
    ("timeseries_agg_60min",     "gold",   ["series", "std_channel", "experiment_id"]),
    # Gold serving/lookup (small — clustered on natural lookup keys)
    ("experiment_index",         "gold",   ["series", "experiment_id"]),
    ("channel_catalog_series",   "gold",   ["series"]),
    ("channel_catalog_experiment", "gold", ["experiment_id"]),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Optimize Delta tables")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev")
    args, _ = parser.parse_known_args()
    return args


def _cluster_cols_str(cols: list[str]) -> str:
    return ", ".join(f"`{c}`" for c in cols)


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]

    logger.info("=" * 60)
    logger.info(f"Table optimization — {environment}")
    logger.info(f"  Integration test: {is_int_test}")
    logger.info("=" * 60)

    optimized = 0
    skipped = 0

    for table_suffix, layer, cluster_cols in _TABLE_SPECS:
        medal = medallion_variables(layer, environment)
        fqn = build_table_name(
            catalog, medal["uc_schema"], medal["table_prefix"],
            table_suffix, is_int_test,
        )

        # 1. Ensure liquid clustering is configured (idempotent ALTER TABLE)
        try:
            spark.sql(f"ALTER TABLE {fqn} CLUSTER BY ({_cluster_cols_str(cluster_cols)})")
            logger.info(f"  Cluster set: {fqn} -> ({', '.join(cluster_cols)})")
        except Exception as e:
            logger.warning(f"  Cluster skip {fqn}: {str(e)[:120]}")

        # 2. OPTIMIZE (bin-pack + honour liquid cluster layout)
        try:
            spark.sql(f"OPTIMIZE {fqn}")
            logger.info(f"  Optimized:   {fqn}")
            optimized += 1
        except Exception as e:
            logger.warning(f"  Optimize skip {fqn}: {str(e)[:120]}")
            skipped += 1

    logger.info("=" * 60)
    logger.info(f"Done. {optimized} optimized, {skipped} skipped.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
