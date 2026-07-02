"""Gold serving tables: app-oriented surfaces for fast dashboard queries.

Produces tables optimized for the Plotly.js reporting frontend:

  gold_timeseries_agg_15min  — 15-minute elapsed-time bins (overview).
  gold_timeseries_agg_60min  — 60-minute elapsed-time bins (long experiments).
  gold_experiment_index      — merged metadata + KPI summary per experiment.
  gold_channel_catalog_series     — unique (series, std_channel, unit) for pickers.
  gold_channel_catalog_experiment — per-experiment channel availability.

All tables are append-only.  Incrementality is by experiment_id: only new
experiments not yet present in the target are processed.

Idempotency contract:
  - experiment_index is written LAST (commit marker). If the script is
    interrupted, the next run reprocesses the same experiments.
  - channel_catalog_series uses anti-join (safe for retry).
  - channel_catalog_experiment and agg tiers may see duplicates on retry
    (extremely rare — requires job failure between writes within a single run).
  - Only experiments that EXIST in gold_timeseries_agg_1min are processed,
    making it safe to run even if gold_timeseries_view hasn't completed.

Sources: gold_timeseries_agg_1min (base aggregate) + silver_dim_signal +
         bronze_filemeta (for file metadata in experiment_index).
Must run AFTER gold_timeseries_view completes.

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
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("gold_serving_tables")


# =============================================================================
# HELPERS
# =============================================================================

def _existing_experiment_ids(spark: SparkSession, table: str) -> DataFrame:
    """Return distinct experiment_id values already present in a Delta table."""
    if not spark.catalog.tableExists(table):
        return spark.createDataFrame([], "experiment_id long")
    return spark.read.table(table).select("experiment_id").distinct()


def _existing_series_channels(spark: SparkSession, table: str) -> DataFrame:
    """Return existing (series, std_channel) pairs in the series catalog."""
    if not spark.catalog.tableExists(table):
        return spark.createDataFrame([], "series string, std_channel string")
    return spark.read.table(table).select("series", "std_channel")


# =============================================================================
# TABLE BUILDERS
# =============================================================================

def build_agg_from_1min(agg_1min: DataFrame, bin_seconds: int) -> DataFrame:
    """Re-aggregate 1-minute bins into coarser resolution.

    Uses weighted mean (by value_count) for correctness.
    elapsed_bin_s is BIGINT (exact integer seconds).
    """
    return (
        agg_1min
        .withColumn(
            "coarse_bin_s",
            (F.floor(F.col("elapsed_bin_s") / bin_seconds) * bin_seconds).cast("long"),
        )
        .groupBy("series", "std_channel", "experiment_id", "signal_id", "coarse_bin_s")
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


def build_experiment_index(
    agg_1min: DataFrame,
    signal_df: DataFrame,
    filemeta_df: DataFrame,
) -> DataFrame:
    """One row per experiment: metadata + KPIs (computed from 1-min aggregate).

    KPI logic is metric-specific:
      - avg_*  → weighted mean: sum(value_mean * value_count) / sum(value_count)
      - peak_* → max(value_max)
    """
    # Join agg with signal dim to get std_channel for KPI filtering
    agg_with_channel = agg_1min.join(
        F.broadcast(signal_df.select("signal_id", "std_channel").distinct()),
        on="signal_id",
        how="inner",
    )

    # Per-experiment aggregation
    experiment_stats = agg_with_channel.groupBy("experiment_id").agg(
        # Metadata
        F.min("elapsed_time_s").alias("start_time_s"),
        F.max(F.col("elapsed_bin_s").cast("double") + 60.0).alias("end_time_s"),
        F.min("timestamp").alias("start_timestamp"),
        F.max("timestamp").alias("end_timestamp"),
        F.countDistinct("signal_id").alias("channel_count"),
        F.sum("value_count").alias("total_data_points"),
        # KPIs — metric-specific aggregation
        # avg: weighted mean over all bins for that channel
        (
            F.sum(F.when(F.col("std_channel") == "Stack Voltage", F.col("value_mean") * F.col("value_count")))
            / F.sum(F.when(F.col("std_channel") == "Stack Voltage", F.col("value_count")))
        ).alias("avg_stack_voltage_v"),
        # peak: max of all bin maxes
        F.max(F.when(F.col("std_channel") == "Current density", F.col("value_max"))).alias("peak_current_density_ma_cm2"),
        # avg: weighted mean
        (
            F.sum(F.when(F.col("std_channel") == "Energy Efficiency", F.col("value_mean") * F.col("value_count")))
            / F.sum(F.when(F.col("std_channel") == "Energy Efficiency", F.col("value_count")))
        ).alias("avg_energy_efficiency_pct"),
        (
            F.sum(F.when(F.col("std_channel") == "Faradaic Efficiency of CO", F.col("value_mean") * F.col("value_count")))
            / F.sum(F.when(F.col("std_channel") == "Faradaic Efficiency of CO", F.col("value_count")))
        ).alias("avg_fe_co_pct"),
        (
            F.sum(F.when(F.col("std_channel") == "Faradaic Efficiency of H2", F.col("value_mean") * F.col("value_count")))
            / F.sum(F.when(F.col("std_channel") == "Faradaic Efficiency of H2", F.col("value_count")))
        ).alias("avg_fe_h2_pct"),
        (
            F.sum(F.when(F.col("std_channel") == "Single Pass Conversion Efficiency", F.col("value_mean") * F.col("value_count")))
            / F.sum(F.when(F.col("std_channel") == "Single Pass Conversion Efficiency", F.col("value_count")))
        ).alias("avg_spce_pct"),
    ).withColumn(
        "duration_s", F.col("end_time_s") - F.col("start_time_s"),
    )

    # Join to get series, uuid, group from signal dimension
    exp_identity = signal_df.select("experiment_id", "series", "uuid", "group").distinct()
    experiment_stats = experiment_stats.join(exp_identity, on="experiment_id", how="left")

    # File metadata enrichment (join on uuid)
    fm = filemeta_df.select(
        "uuid",
        F.col("raw_file_name").alias("source_file_name"),
        F.col("file_size").alias("source_file_size"),
        F.col("last_modified").alias("source_last_modified"),
        F.col("ingested_timestamp").alias("ingested_at"),
    )
    experiment_stats = experiment_stats.join(fm, on="uuid", how="left")

    return experiment_stats.select(
        "experiment_id", "series", "uuid", "group",
        "start_time_s", "end_time_s", "start_timestamp", "end_timestamp",
        "duration_s", "channel_count", "total_data_points",
        "source_file_name", "source_file_size", "source_last_modified", "ingested_at",
        "avg_stack_voltage_v", "peak_current_density_ma_cm2",
        "avg_energy_efficiency_pct", "avg_fe_co_pct", "avg_fe_h2_pct", "avg_spce_pct",
    )


def build_channel_catalog_series(signal_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    """New unique (series, std_channel, unit) not yet in the catalog."""
    all_channels = signal_df.select("series", "std_channel", "unit").distinct()
    return all_channels.join(existing_df, on=["series", "std_channel"], how="left_anti")


def build_channel_catalog_experiment(
    signal_df: DataFrame, agg_1min: DataFrame,
) -> DataFrame:
    """Per-experiment channel availability with has_data flag.

    has_data = True if the signal has at least one non-NULL value in the agg.
    """
    has_data = (
        agg_1min
        .groupBy("experiment_id", "signal_id")
        .agg((F.max("value_count") > 0).alias("has_data"))
    )

    return (
        signal_df
        .select("experiment_id", "signal_id", "series", "std_channel", "unit")
        .join(has_data, on=["experiment_id", "signal_id"], how="left")
        .withColumn("has_data", F.coalesce(F.col("has_data"), F.lit(False)))
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
    silver_medal = medallion_variables("silver", environment)
    bronze_medal = medallion_variables("bronze", environment)
    schema_g = gold_medal["uc_schema"]
    schema_s = silver_medal["uc_schema"]
    schema_b = bronze_medal["uc_schema"]

    # Source tables
    gold_agg_1min_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_1min", args.is_integration_test)
    silver_signal_table = build_table_name(catalog, schema_s, "silver", "dim_signal", args.is_integration_test)
    bronze_fm_table = build_table_name(catalog, schema_b, "bronze", "filemeta", args.is_integration_test)

    # Target serving tables
    index_table = build_table_name(catalog, schema_g, "gold", "experiment_index", args.is_integration_test)
    catalog_series_table = build_table_name(catalog, schema_g, "gold", "channel_catalog_series", args.is_integration_test)
    catalog_exp_table = build_table_name(catalog, schema_g, "gold", "channel_catalog_experiment", args.is_integration_test)
    agg_15_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_15min", args.is_integration_test)
    agg_60_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_60min", args.is_integration_test)

    gold_layer = "gold_int_test" if args.is_integration_test else gold_medal["table_prefix"]

    logger.info("=" * 60)
    logger.info("Gold Serving Tables")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Source: {gold_agg_1min_table}")
    logger.info(f"  -> {index_table}")
    logger.info(f"  -> {catalog_series_table}")
    logger.info(f"  -> {catalog_exp_table}")
    logger.info(f"  -> {agg_15_table}")
    logger.info(f"  -> {agg_60_table}")
    logger.info("=" * 60)

    # --- Load silver_dim_signal (tiny, needed for KPI channel lookups) ---
    signal_df = spark.read.table(silver_signal_table)

    # --- Find new experiment_ids not yet in experiment_index (commit marker) ---
    existing_index_ids = _existing_experiment_ids(spark, index_table)
    all_experiment_ids = signal_df.select("experiment_id").distinct()
    candidate_ids = all_experiment_ids.join(existing_index_ids, on="experiment_id", how="left_anti")

    candidate_count = candidate_ids.count()
    if candidate_count == 0:
        logger.info("  Nothing new. Exiting.")
        return
    logger.info(f"  Candidate experiment_ids (not in index): {candidate_count}")

    # --- Load 1-min aggregate for candidates, cache once ---
    agg_1min = (
        spark.read.table(gold_agg_1min_table)
        .join(F.broadcast(candidate_ids), on="experiment_id", how="inner")
        .cache()
    )
    agg_row_count = agg_1min.count()  # materialize cache

    # Narrow to experiments that ACTUALLY HAVE data in gold_agg_1min.
    # Safety: if serving runs before gold_timeseries_view, experiments without
    # agg data are skipped — they will be picked up on the next run after gold.
    new_experiment_ids = agg_1min.select("experiment_id").distinct()
    new_count = new_experiment_ids.count()

    if new_count == 0:
        logger.info("  No experiments with agg data ready. Exiting (gold may not have run yet).")
        agg_1min.unpersist()
        return

    if new_count < candidate_count:
        logger.warning(
            f"  {candidate_count - new_count} experiment(s) skipped (no agg_1min data yet — "
            "gold_timeseries_view may not have processed them)."
        )
    logger.info(f"  Processing {new_count} experiment(s) ({agg_row_count:,} agg rows)")

    # Filter signal_df to new experiments (only those with agg data)
    signal_new = signal_df.join(F.broadcast(new_experiment_ids), on="experiment_id", how="inner")

    # =========================================================================
    # 1. CHANNEL CATALOGS (written before experiment_index for idempotency)
    # =========================================================================
    # Series-level catalog (append only new series+channel combinations)
    existing_series_channels = _existing_series_channels(spark, catalog_series_table)
    catalog_series_df = build_channel_catalog_series(signal_df, existing_series_channels)
    series_count = catalog_series_df.count()
    if series_count > 0:
        loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "channel_catalog_series")
        write_to_delta(catalog_series_df, catalog_series_table, mode="append", location=loc)
    logger.info(f"  gold_channel_catalog_series: {series_count} new row(s)")

    # Experiment-level catalog (per-experiment channel availability)
    catalog_exp_df = build_channel_catalog_experiment(signal_new, agg_1min)
    catalog_exp_count = catalog_exp_df.count()
    logger.info(f"  gold_channel_catalog_experiment: {catalog_exp_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "channel_catalog_experiment")
    write_to_delta(catalog_exp_df, catalog_exp_table, mode="append", location=loc)

    # =========================================================================
    # 2. COARSE AGGREGATION TIERS
    # =========================================================================
    agg_15_df = build_agg_from_1min(agg_1min, bin_seconds=900)
    agg_15_count = agg_15_df.count()
    logger.info(f"  gold_timeseries_agg_15min: {agg_15_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "timeseries_agg_15min")
    write_to_delta(agg_15_df, agg_15_table, mode="append", location=loc)

    agg_60_df = build_agg_from_1min(agg_1min, bin_seconds=3600)
    agg_60_count = agg_60_df.count()
    logger.info(f"  gold_timeseries_agg_60min: {agg_60_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "timeseries_agg_60min")
    write_to_delta(agg_60_df, agg_60_table, mode="append", location=loc)

    # =========================================================================
    # 3. EXPERIMENT INDEX (commit marker — written LAST for idempotency)
    # =========================================================================
    # If the script is interrupted before this point, the next run will
    # reprocess the same experiments (safe: catalogs use anti-join or are
    # append-only; agg tiers may see minor duplicates on rare retry).
    filemeta = spark.read.table(bronze_fm_table)

    index_df = build_experiment_index(agg_1min, signal_new, filemeta)
    index_count = index_df.count()
    logger.info(f"  gold_experiment_index: {index_count} new row(s)")

    loc = build_external_table_location(storage_account, gold_medal["adls_container"], gold_layer, "experiment_index")
    write_to_delta(index_df, index_table, mode="append", location=loc)

    agg_1min.unpersist()

    logger.info("=" * 60)
    logger.info("Gold serving tables complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
