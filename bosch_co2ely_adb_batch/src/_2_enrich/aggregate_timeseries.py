"""Silver aggregation: Compute time-binned statistics from enriched data.

Reads the silver enriched table, applies time-windowed aggregation
(min/max/mean per configurable interval), writes to silver aggregated table.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_enrich/aggregate_timeseries.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from pyspark.sql import SparkSession

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.io_utils import polars_to_spark, write_to_delta

logger = configure_logger("aggregate_timeseries")

# Default aggregation interval (seconds) — matches Dash app default
DEFAULT_INTERVAL_SECONDS = 900  # 15 minutes


def aggregate_timeseries(
    pl_df: pl.DataFrame,
    time_col: str = "Elapsed time",
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
) -> pl.DataFrame:
    """Aggregate timeseries data into fixed time bins.

    For each numeric column, computes min/max/mean per bin.

    Args:
        pl_df: Enriched Polars DataFrame.
        time_col: Column with elapsed time in seconds.
        interval_seconds: Bin width in seconds.

    Returns:
        Aggregated DataFrame with one row per time bin.
    """
    if time_col not in pl_df.columns:
        logger.warning(f"Time column '{time_col}' not found. Returning as-is.")
        return pl_df

    # Create time bin column
    pl_df = pl_df.with_columns(
        (pl.col(time_col) / interval_seconds).floor().cast(pl.Int64).alias("_time_bin")
    )

    # Aggregate numeric columns
    numeric_cols = [
        c for c in pl_df.columns
        if pl_df[c].dtype.is_numeric() and c not in (time_col, "_time_bin")
    ]

    agg_exprs = []
    for col_name in numeric_cols:
        agg_exprs.extend([
            pl.col(col_name).mean().alias(f"{col_name}_mean"),
            pl.col(col_name).min().alias(f"{col_name}_min"),
            pl.col(col_name).max().alias(f"{col_name}_max"),
        ])

    # Add bin start time
    agg_exprs.append(
        (pl.col("_time_bin") * interval_seconds).first().alias(f"{time_col}_bin_start")
    )

    pl_agg = pl_df.group_by("_time_bin").agg(agg_exprs).sort("_time_bin")
    pl_agg = pl_agg.drop("_time_bin")

    return pl_agg


def main():
    """Main entry point for silver aggregation."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve environment
    env = env_variables(spark, env_override=args.env)
    layer = layer_variables("_2_enrich")
    write_medal = medallion_variables(layer["write_layer"])

    logger.info(f"Environment: {env['environment']}")

    # Source: silver enriched table
    source_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    # Target: silver aggregated table
    target_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_timeseries_aggregated",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Reading from: {source_table}")
    logger.info(f"Aggregating with interval: {DEFAULT_INTERVAL_SECONDS}s")

    # Read enriched data → Polars
    df_spark = spark.read.table(source_table)
    df_pd = df_spark.toPandas()
    pl_df = pl.from_pandas(df_pd)

    # Aggregate
    pl_agg = aggregate_timeseries(pl_df, interval_seconds=DEFAULT_INTERVAL_SECONDS)
    logger.info(f"Aggregated: {pl_agg.shape[0]} bins * {pl_agg.shape[1]} cols")

    # Write to Delta
    df_agg = polars_to_spark(spark, pl_agg)
    write_to_delta(df_agg, target_table, mode="overwrite")
    logger.info(f"Successfully wrote aggregated data to {target_table}")


if __name__ == "__main__":
    main()
