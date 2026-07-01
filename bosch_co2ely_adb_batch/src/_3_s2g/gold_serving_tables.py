"""Gold serving tables: app-oriented surfaces for fast dashboard queries.

Produces four tables optimized for the reporting frontend:

  gold_experiment_index   — one row per (series, uuid, group) with timestamps,
                           channel count, sample count, and file metadata.
                           Used for selector dropdowns and quick-view panels.

  gold_channel_catalog    — one row per (series, uuid, group, channel_id) with
                           raw_channel, std_channel, and unit. Used for axis
                           labels and channel selector population.

  gold_timeseries_agg_15min — 15-minute elapsed-time bins with mean/min/max/count.
                              Coarse tier for overview zoom and initial page load.

  gold_timeseries_agg_60min — 60-minute elapsed-time bins. Ultra-coarse tier for
                              long experiments (days/weeks) overview.

All tables are append-only. Incrementality is by (uuid, group) pairs: only
new experiments not yet present in the target are processed.

Source: gold_timeseries and gold_timeseries_agg (both produced by
gold_timeseries_view.py). Must run AFTER gold_timeseries_view completes.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_3_s2g/gold_serving_tables.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("gold_serving_tables")


# =============================================================================
# HELPERS
# =============================================================================

def _existing_pairs(spark: SparkSession, table: str) -> DataFrame:
    """Return distinct (uuid, group) pairs already present in a Delta table."""
    try:
        return spark.read.table(table).select("uuid", "group").distinct()
    except AnalysisException:
        return spark.createDataFrame([], "uuid string, group string")


def _new_pairs(source_df: DataFrame, target_table: str, spark: SparkSession) -> DataFrame:
    """Return (uuid, group) pairs in source not yet in target."""
    existing = _existing_pairs(spark, target_table)
    return (
        source_df.select("uuid", "group").distinct()
        .join(existing, on=["uuid", "group"], how="left_anti")
    )


# =============================================================================
# TABLE BUILDERS
# =============================================================================

def build_experiment_index(gold_ts: DataFrame, filemeta: DataFrame) -> DataFrame:
    """One row per (series, uuid, group) with summary metadata.

    Enables: selector dropdowns, experiment cards, quick-view panels.
    """
    ts_summary = gold_ts.groupBy("series", "uuid", "group").agg(
        F.min("elapsed_time_s").alias("start_time_s"),
        F.max("elapsed_time_s").alias("end_time_s"),
        F.min("timestamp").alias("start_timestamp"),
        F.max("timestamp").alias("end_timestamp"),
        F.countDistinct("channel_id").alias("channel_count"),
        F.max("sample_offset").alias("max_sample_offset"),
        F.count("*").alias("total_data_points"),
    ).withColumn(
        "duration_s",
        F.col("end_time_s") - F.col("start_time_s"),
    )

    # Join file metadata for context
    fm = filemeta.select(
        "uuid",
        F.col("file_path").alias("source_file_path"),
        F.col("raw_file_name").alias("source_file_name"),
        F.col("file_size").alias("source_file_size"),
        F.col("last_modified").alias("source_last_modified"),
        F.col("_ingestion_timestamp").alias("ingested_at"),
    )

    return ts_summary.join(fm, on="uuid", how="left")


def build_channel_catalog(gold_ts: DataFrame) -> DataFrame:
    """One row per (series, uuid, group, channel_id) with display metadata.

    Enables: channel selectors, axis labels, unit lookups.
    """
    return (
        gold_ts
        .select("series", "uuid", "group", "channel_id", "raw_channel", "std_channel", "unit")
        .distinct()
    )


def build_agg_from_1min(agg_1min: DataFrame, bin_seconds: int) -> DataFrame:
    """Re-aggregate 1-minute bins into coarser resolution.

    Uses weighted mean (by value_count) for correctness when 1-min bins
    have different sample counts.
    """
    return (
        agg_1min
        .withColumn(
            "coarse_bin_s",
            (F.floor(F.col("elapsed_bin_s") / bin_seconds) * bin_seconds),
        )
        .groupBy("series", "uuid", "group", "coarse_bin_s", "channel_id", "raw_channel", "std_channel", "unit")
        .agg(
            F.first("elapsed_time_s", ignorenulls=True).alias("elapsed_time_s"),
            F.first("timestamp", ignorenulls=True).alias("timestamp"),
            # Weighted mean: sum(mean * count) / sum(count)
            (F.sum(F.col("value_mean") * F.col("value_count")) / F.sum("value_count")).alias("value_mean"),
            F.min("value_min").alias("value_min"),
            F.max("value_max").alias("value_max"),
            F.sum("value_count").alias("value_count"),
        )
        .withColumnRenamed("coarse_bin_s", "elapsed_bin_s")
    )


# =============================================================================
# MAIN
# =============================================================================

def main():
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    gold_medal = medallion_variables("gold", environment)
    bronze_medal = medallion_variables("bronze", environment)
    schema_g = gold_medal["uc_schema"]
    schema_b = bronze_medal["uc_schema"]

    # Source tables (produced by gold_timeseries_view.py)
    gold_ts_table = build_table_name(catalog, schema_g, "gold", "timeseries", args.is_integration_test)
    gold_agg_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg", args.is_integration_test)
    bronze_fm_table = build_table_name(catalog, schema_b, "bronze", "filemeta", args.is_integration_test)

    # Target serving tables
    index_table = build_table_name(catalog, schema_g, "gold", "experiment_index", args.is_integration_test)
    catalog_table = build_table_name(catalog, schema_g, "gold", "channel_catalog", args.is_integration_test)
    agg_15_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_15min", args.is_integration_test)
    agg_60_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_60min", args.is_integration_test)

    gold_layer = "gold_int_test" if args.is_integration_test else gold_medal["table_prefix"]

    logger.info("=" * 60)
    logger.info("Gold Serving Tables")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Source: {gold_ts_table}")
    logger.info(f"  Source: {gold_agg_table}")
    logger.info(f"  -> {index_table}")
    logger.info(f"  -> {catalog_table}")
    logger.info(f"  -> {agg_15_table}")
    logger.info(f"  -> {agg_60_table}")
    logger.info("=" * 60)

    # --- Find new (uuid, group) pairs not yet in serving tables ---
    new_index_pairs = _new_pairs(
        spark.read.table(gold_ts_table), index_table, spark
    )
    new_count = new_index_pairs.count()
    logger.info(f"  New (uuid, group) pairs to process: {new_count}")

    if new_count == 0:
        logger.info("  Nothing new. Exiting.")
        return

    new_index_pairs = F.broadcast(new_index_pairs)

    # =========================================================================
    # 1. EXPERIMENT INDEX
    # =========================================================================
    gold_ts = spark.read.table(gold_ts_table).join(
        new_index_pairs, on=["uuid", "group"], how="inner"
    )
    filemeta = spark.read.table(bronze_fm_table)

    index_df = build_experiment_index(gold_ts, filemeta)
    index_count = index_df.count()
    logger.info(f"  gold_experiment_index: {index_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "experiment_index")
    write_to_delta(index_df, index_table, mode="append", location=loc)

    # =========================================================================
    # 2. CHANNEL CATALOG
    # =========================================================================
    catalog_df = build_channel_catalog(gold_ts)
    catalog_count = catalog_df.count()
    logger.info(f"  gold_channel_catalog: {catalog_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "channel_catalog")
    write_to_delta(catalog_df, catalog_table, mode="append", location=loc)

    # =========================================================================
    # 3. 15-MINUTE AGGREGATION
    # =========================================================================
    agg_1min = spark.read.table(gold_agg_table).join(
        new_index_pairs, on=["uuid", "group"], how="inner"
    )

    agg_15_df = build_agg_from_1min(agg_1min, bin_seconds=900)
    agg_15_count = agg_15_df.count()
    logger.info(f"  gold_timeseries_agg_15min: {agg_15_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "timeseries_agg_15min")
    write_to_delta(agg_15_df, agg_15_table, mode="append", location=loc)

    # =========================================================================
    # 4. 60-MINUTE AGGREGATION
    # =========================================================================
    agg_60_df = build_agg_from_1min(agg_1min, bin_seconds=3600)
    agg_60_count = agg_60_df.count()
    logger.info(f"  gold_timeseries_agg_60min: {agg_60_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "timeseries_agg_60min")
    write_to_delta(agg_60_df, agg_60_table, mode="append", location=loc)

    logger.info("=" * 60)
    logger.info("Gold serving tables complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
