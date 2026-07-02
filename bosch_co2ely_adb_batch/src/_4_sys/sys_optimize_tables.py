"""System: OPTIMIZE and cluster Delta tables for query performance.

Runs OPTIMIZE (bin-packing) and ensures liquid clustering is configured
on all co2ely Delta tables. Targeted at Plotly.js / Databricks SQL Warehouse
query patterns:

  gold_timeseries        — queried by (uuid, group, std_channel) for chart data
  gold_timeseries_agg    — queried by (uuid, group, std_channel) for fast overview
  gold_*_agg_*min        — queried by (uuid, group, std_channel, elapsed_bin_s)
  gold_experiment_index  — queried by (series, uuid) for selector dropdowns
  gold_channel_catalog   — queried by (uuid, group) for channel list
  gold_summary_statistics — queried by (series) for KPI tables
  silver_fact_timeseries_enriched — queried by (uuid, group) from Gold layer
  bronze_timeseries      — queried by (uuid, group) from Silver layer

Liquid clustering on (uuid, group) gives the best I/O skip for the
multi-experiment time-series query patterns.

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
# Cluster key choice rationale: Plotly.js queries always filter on uuid+group
# (one experiment), then optionally std_channel. Elapsed-time range scans
# benefit from elapsed_time_s / elapsed_bin_s in the cluster key.
_TABLE_SPECS = [
    # (table_suffix, prefix, cluster_cols)
    # Bronze
    ("timeseries",               "bronze", ["uuid", "group"]),
    ("channel",                  "bronze", ["uuid", "group"]),
    ("filemeta",                 "bronze", ["uuid"]),
    ("statistics",               "bronze", ["uuid", "group"]),
    # Silver
    ("fact_timeseries_enriched", "silver", ["uuid", "group"]),
    # Gold — timeseries (largest; cluster on channel too for channel-specific queries)
    ("timeseries",               "gold",   ["uuid", "group", "std_channel"]),
    ("timeseries_agg",           "gold",   ["uuid", "group", "std_channel"]),
    ("timeseries_agg_15min",     "gold",   ["uuid", "group", "std_channel"]),
    ("timeseries_agg_60min",     "gold",   ["uuid", "group", "std_channel"]),
    # Gold — serving/lookup (small; cluster on natural lookup keys)
    ("experiment_index",         "gold",   ["series", "uuid"]),
    ("channel_catalog",          "gold",   ["uuid", "group"]),
    ("summary_statistics",       "gold",   ["series", "uuid"]),
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
