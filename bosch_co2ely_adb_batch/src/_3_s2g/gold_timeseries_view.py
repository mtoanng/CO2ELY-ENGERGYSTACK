"""Gold layer: Lean timeseries facts (Bronze + Silver dim_signal -> Gold).

Reads Bronze timeseries and joins the small Silver signal dimension at
Gold-build time via a single broadcast join.  Incremental append-only:
only processes experiment_id values not already present in the gold tables.

Produces two tables:

gold_timeseries — raw drill-down, one row per measurement point
    series          string       -- app filter #1
    std_channel     string       -- app filter #2
    experiment_id   bigint       -- trace identity (replaces uuid+group)
    signal_id       bigint       -- technical lineage key
    sample_offset   bigint       -- row ordering within sheet
    elapsed_time_s  double       -- X-axis (seconds from start)
    timestamp       timestamp    -- parsed event timestamp
    value           double       -- Y-axis measurement
    value_str       string       -- non-numeric fallback

gold_timeseries_agg_1min — 1-minute base aggregate
    series          string
    std_channel     string
    experiment_id   bigint
    signal_id       bigint
    elapsed_bin_s   bigint       -- floor(elapsed_time_s / 60) * 60
    elapsed_time_s  double       -- first value in bin
    timestamp       timestamp    -- first timestamp in bin
    value_mean      double
    value_min       double
    value_max       double
    value_count     bigint

Both tables are append-only.  Re-running skips already-written
experiment_id values.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_3_s2g/gold_timeseries_view.py
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

logger = configure_logger("gold_timeseries")

# Aggregation bin size in seconds.
_AGG_BIN_S = 60

_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy/MM/dd HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
]


def _parse_timestamp(raw_col) -> F.Column:
    """Try multiple timestamp formats, return first successful parse."""
    parsed = None
    for fmt in _TIMESTAMP_FORMATS:
        candidate = F.to_timestamp(raw_col, fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
    return F.coalesce(parsed, F.to_timestamp(raw_col))


def _existing_experiment_ids(spark: SparkSession, table: str) -> DataFrame:
    """Return distinct experiment_id values already present in a Delta table."""
    if not spark.catalog.tableExists(table):
        return spark.createDataFrame([], "experiment_id long")
    return spark.read.table(table).select("experiment_id").distinct()


def _build_gold_timeseries(
    timeseries_df: DataFrame, signal_df: DataFrame,
) -> DataFrame:
    """Join Bronze fact rows with Silver signal dimension → lean Gold fact.

    Single broadcast join: bronze_timeseries × silver_dim_signal
    Join key: (uuid, group, channel_id) — natural keys from bronze.
    Output: lean fact with surrogate keys + app predicates.
    """
    # Select only the columns needed from dim_signal for the join
    signal_meta = signal_df.select(
        "uuid", "group", "channel_id",
        "experiment_id", "signal_id", "series", "std_channel",
    )

    joined = timeseries_df.join(
        F.broadcast(signal_meta),
        on=["uuid", "group", "channel_id"],
        how="inner",
    )

    return joined.select(
        "series",
        "std_channel",
        "experiment_id",
        "signal_id",
        "sample_offset",
        "elapsed_time_s",
        _parse_timestamp(F.col("timestamp")).alias("timestamp"),
        "value",
        "value_str",
    )


def _build_gold_timeseries_agg(gold_ts: DataFrame) -> DataFrame:
    """Aggregate gold_timeseries into 1-minute elapsed-time bins.

    elapsed_bin_s is BIGINT (exact integer seconds).
    elapsed_time_s and timestamp are the FIRST value per bin (bin start).
    """
    return (
        gold_ts
        .filter(F.col("elapsed_time_s").isNotNull() & F.col("value").isNotNull())
        .withColumn(
            "elapsed_bin_s",
            (F.floor(F.col("elapsed_time_s") / _AGG_BIN_S) * _AGG_BIN_S).cast("long"),
        )
        .groupBy("series", "std_channel", "experiment_id", "signal_id", "elapsed_bin_s")
        .agg(
            F.first("elapsed_time_s", ignorenulls=True).alias("elapsed_time_s"),
            F.first("timestamp", ignorenulls=True).alias("timestamp"),
            F.mean("value").alias("value_mean"),
            F.min("value").alias("value_min"),
            F.max("value").alias("value_max"),
            F.count("value").alias("value_count"),
        )
    )


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

    layers = layer_variables("_3_s2g")
    silver_medal = medallion_variables(layers["read_layer"], environment)
    bronze_medal = medallion_variables("bronze", environment)
    gold_medal = medallion_variables(layers["write_layer"], environment)

    schema_s = silver_medal["uc_schema"]
    schema_b = bronze_medal["uc_schema"]
    schema_g = gold_medal["uc_schema"]

    # Integration-test-aware layer path (used for all ADLS writes)
    gold_layer = "gold_int_test" if args.is_integration_test else gold_medal["table_prefix"]

    # Source tables
    bronze_ts_table = build_table_name(
        catalog, schema_b, bronze_medal["table_prefix"], "timeseries", args.is_integration_test
    )
    silver_signal_table = build_table_name(
        catalog, schema_s, silver_medal["table_prefix"], "dim_signal", args.is_integration_test
    )

    # Target tables
    gold_ts_table = build_table_name(catalog, schema_g, "gold", "timeseries", args.is_integration_test)
    gold_agg_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg_1min", args.is_integration_test)

    logger.info("=" * 60)
    logger.info("Gold timeseries (Bronze + Silver dim_signal -> Gold, append-only)")
    logger.info(f"  Catalog.Schema: {catalog}.{schema_g}")
    logger.info(f"  bronze_timeseries  : {bronze_ts_table}")
    logger.info(f"  silver_dim_signal  : {silver_signal_table}")
    logger.info(f"  gold_timeseries    : {gold_ts_table}")
    logger.info(f"  gold_timeseries_agg_1min: {gold_agg_table}")
    logger.info(f"  ADLS layer: {gold_layer}")
    logger.info("=" * 60)

    # --- Incremental: find experiment_ids already in gold ---
    done_ts_ids = _existing_experiment_ids(spark, gold_ts_table)
    done_agg_ids = _existing_experiment_ids(spark, gold_agg_table)

    # If raw gold exists but agg is missing, backfill agg from raw gold
    missing_agg_ids = done_ts_ids.join(done_agg_ids, on="experiment_id", how="left_anti")
    missing_agg_count = missing_agg_ids.count()

    if missing_agg_count:
        logger.info(f"  Backfilling agg for {missing_agg_count} existing experiment(s)")
        backfill_gold_ts = spark.read.table(gold_ts_table).join(
            F.broadcast(missing_agg_ids), on="experiment_id", how="inner"
        )
        backfill_agg_df = _build_gold_timeseries_agg(backfill_gold_ts)
        backfill_count = backfill_agg_df.count()
        if backfill_count:
            loc_agg = build_external_table_location(
                storage_account=storage_account,
                container=gold_medal["adls_container"],
                layer=gold_layer,
                table_name="timeseries_agg_1min",
            )
            write_to_delta(backfill_agg_df, gold_agg_table, mode="append", location=loc_agg)
            logger.info(f"  Backfilled {backfill_count:,} agg rows -> {gold_agg_table}")

        # Refresh done_agg_ids after backfill
        done_agg_ids = _existing_experiment_ids(spark, gold_agg_table)

    # --- Load silver_dim_signal (tiny, broadcast) ---
    signal_df = spark.read.table(silver_signal_table)

    # --- Determine new experiment_ids from signal dim (not yet in gold_timeseries) ---
    all_experiment_ids = signal_df.select("experiment_id").distinct()
    new_experiment_ids = all_experiment_ids.join(done_ts_ids, on="experiment_id", how="left_anti")
    new_count = new_experiment_ids.count()
    logger.info(f"  New experiment_ids to process: {new_count}")

    if new_count == 0:
        logger.info("  Nothing new to write. Exiting.")
        return

    # --- Load Bronze fact, filter to new experiments only ---
    # Filter signal_df to only new experiments (for the join)
    signal_new = signal_df.join(
        F.broadcast(new_experiment_ids), on="experiment_id", how="inner"
    )
    # Get the (uuid, group) pairs for new experiments to filter bronze
    new_pairs = signal_new.select("uuid", "group").distinct()
    bronze_ts = (
        spark.read.table(bronze_ts_table)
        .join(F.broadcast(new_pairs), on=["uuid", "group"], how="inner")
    )

    # --- Build gold_timeseries (lean fact) ---
    gold_ts_df = _build_gold_timeseries(bronze_ts, signal_new).cache()

    ts_count = gold_ts_df.count()
    logger.info(f"  gold_timeseries rows (new): {ts_count:,}")

    if ts_count == 0:
        logger.warning("  No timeseries rows produced (bronze may have no matching data). Skipping writes.")
        gold_ts_df.unpersist()
        return

    loc_ts = build_external_table_location(
        storage_account=storage_account,
        container=gold_medal["adls_container"],
        layer=gold_layer,
        table_name="timeseries",
    )
    write_to_delta(gold_ts_df, gold_ts_table, mode="append", location=loc_ts)

    # --- Build gold_timeseries_agg_1min ---
    gold_agg_df = _build_gold_timeseries_agg(gold_ts_df)
    agg_count = gold_agg_df.count()
    logger.info(f"  gold_timeseries_agg_1min rows (new): {agg_count:,}")

    loc_agg = build_external_table_location(
        storage_account=storage_account,
        container=gold_medal["adls_container"],
        layer=gold_layer,
        table_name="timeseries_agg_1min",
    )
    write_to_delta(gold_agg_df, gold_agg_table, mode="append", location=loc_agg)

    gold_ts_df.unpersist()

    logger.info("=" * 60)
    logger.info("Gold timeseries complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
