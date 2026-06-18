"""Gold layer: Build dashboard-ready timeseries table.

Constructs a gold-layer table that selects the key columns
consumed by the Dash frontend for chart rendering.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_3_gold/gold_timeseries_view.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.io_utils import write_to_delta

logger = configure_logger("gold_timeseries_view")

# Columns the Dash frontend renders in charts
VIEW_COLUMNS = [
    "Elapsed time",
    "Stack Voltage",
    "Current",
    "Current density",
    "Energy Efficiency",
    "Faradaic Efficiency of CO",
    "Faradaic Efficiency of H2",
    "Faradaic Efficiency of O2",
    "Faradaic Efficiency of CO and H2",
    "Single Pass Conversion Efficiency",
    "CO2:O2 ratio in anode product gas",
]


def main():
    """Main entry point for gold timeseries table creation."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve environment
    env = env_variables(spark, env_override=args.env)
    read_layer = layer_variables("_3_gold")
    read_medal = medallion_variables(read_layer["read_layer"])
    write_medal = medallion_variables(read_layer["write_layer"])

    # Source table
    source_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="co2_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    # Target gold table
    target_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_timeseries_dashboard",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Building gold table: {target_table}")
    logger.info(f"Source: {source_table}")

    # Select only the dashboard-relevant columns
    df = spark.read.table(source_table)
    available_cols = [c for c in VIEW_COLUMNS if c in df.columns]

    if not available_cols:
        logger.warning("No matching columns found in source table!")
        return

    df_view = df.select(available_cols)
    row_count = df_view.count()

    # Write gold table
    write_to_delta(df_view, target_table)
    logger.info(f"Gold table written: {target_table} ({row_count} rows)")


if __name__ == "__main__":
    main()
