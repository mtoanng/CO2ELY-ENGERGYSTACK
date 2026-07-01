"""Gold layer: Demo-ready timeseries tables (Bronze -> Gold, no Silver).

Reads directly from bronze tables. Incremental append-only: only processes
(uuid, group) pairs not already present in the gold tables.

Produces two tables:

gold_timeseries  — long format, one row per sample_offset per signal channel
    series          string
    uuid            string
    group           string
    sample_offset   bigint
    timestamp       timestamp  -- promoted from channel="timestamp" row
    elapsed_time_s  double     -- promoted from elapsed-time channel row
    channel         string     -- signal channels only (timestamp + elapsed excluded)
    channel_name    string     -- from bronze_channel
    unit            string     -- from bronze_channel
    value           double
    value_str       string

gold_timeseries_agg  — pre-aggregated, 1-minute elapsed-time bins
    series          string
    uuid            string
    group           string
    elapsed_bin_s   double     -- FLOOR(elapsed_time_s / 60) * 60
    channel         string
    channel_name    string
    unit            string
    value_mean      double
    value_min       double
    value_max       double
    value_count     bigint

Both tables are append-only. Re-running the job skips already-written
(uuid, group) pairs, so new experiment files are added without rewriting
existing data.

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
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("gold_timeseries")

# Channel name stored as a row in bronze_timeseries for the wall-clock timestamp.
_TIMESTAMP_CHANNEL = "timestamp"

# Regex matched against LOWER(channel) and LOWER(channel_name).
# Covers: "elapsed time", "elapsed_time", "test time", "testtime", etc.
_ELAPSED_PATTERN = r"(elapsed[\s_]?time|test[\s_]?time)"

# Aggregation bin size in seconds.
_AGG_BIN_S = 60.0


def _existing_pairs(spark: SparkSession, table: str) -> DataFrame:
    """Return distinct (uuid, group) pairs already present in a Delta table.

    Returns an empty DataFrame if the table does not exist yet. Keeping this as
    a DataFrame avoids collecting all processed pairs to the driver.
    """
    try:
        return spark.read.table(table).select("uuid", "group").distinct()
    except AnalysisException:
        return spark.createDataFrame([], "uuid string, group string")


def _build_gold_timeseries(
    ts_with_meta: DataFrame,
    fm_series: DataFrame,
) -> DataFrame:
    """Promote timestamp + elapsed-time rows; keep signal rows in long format."""

    elapsed_cond = (
        F.lower(F.col("channel")).rlike(_ELAPSED_PATTERN) |
        F.lower(F.col("channel_name")).rlike(_ELAPSED_PATTERN)
    )

    ts_timestamp = (
        ts_with_meta
        .filter(F.col("channel") == _TIMESTAMP_CHANNEL)
        .select(
            "uuid", "group", "sample_offset",
            F.to_timestamp(F.col("value_str"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        )
    )

    ts_elapsed = (
        ts_with_meta
        .filter(elapsed_cond)
        .select(
            "uuid", "group", "sample_offset",
            F.col("value").alias("elapsed_time_s"),
        )
    )

    ts_signals = ts_with_meta.filter(
        (F.col("channel") != _TIMESTAMP_CHANNEL) & ~elapsed_cond
    )

    return (
        ts_signals
        .join(ts_timestamp, on=["uuid", "group", "sample_offset"], how="left")
        .join(ts_elapsed,   on=["uuid", "group", "sample_offset"], how="left")
        .join(F.broadcast(fm_series), on="uuid", how="left")
        .select(
            "series", "uuid", "group", "sample_offset",
            "timestamp", "elapsed_time_s",
            "channel", "channel_name", "unit",
            "value", "value_str",
        )
    )


def _build_gold_timeseries_agg(gold_ts: DataFrame) -> DataFrame:
    """Aggregate gold_timeseries into 1-minute elapsed-time bins.

    elapsed_time_s and timestamp are the FIRST value per bin (bin start),
    matching the behaviour of aggregate_timeseries() in the Dash app.
    """
    return (
        gold_ts
        .filter(F.col("elapsed_time_s").isNotNull() & F.col("value").isNotNull())
        .withColumn(
            "elapsed_bin_s",
            (F.floor(F.col("elapsed_time_s") / _AGG_BIN_S) * _AGG_BIN_S),
        )
        .groupBy("series", "uuid", "group", "elapsed_bin_s", "channel", "channel_name", "unit")
        .agg(
            F.first("elapsed_time_s",  ignorenulls=True).alias("elapsed_time_s"),
            F.first("timestamp",       ignorenulls=True).alias("timestamp"),
            F.mean("value").alias("value_mean"),
            F.min("value").alias("value_min"),
            F.max("value").alias("value_max"),
            F.count("value").alias("value_count"),
        )
    )


def main():
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]

    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    bronze_medal = medallion_variables("bronze", environment)
    gold_medal   = medallion_variables("gold",   environment)

    schema_b = bronze_medal["uc_schema"]
    schema_g = gold_medal["uc_schema"]

    bronze_ts_table  = build_table_name(catalog, schema_b, "bronze", "timeseries",  args.is_integration_test)
    bronze_ch_table  = build_table_name(catalog, schema_b, "bronze", "channel",     args.is_integration_test)
    bronze_fm_table  = build_table_name(catalog, schema_b, "bronze", "filemeta",    args.is_integration_test)
    gold_ts_table    = build_table_name(catalog, schema_g, "gold",   "timeseries",  args.is_integration_test)
    gold_agg_table   = build_table_name(catalog, schema_g, "gold",   "timeseries_agg", args.is_integration_test)

    logger.info("=" * 60)
    logger.info("Gold timeseries (Bronze -> Gold, demo mode, append-only)")
    logger.info(f"  Catalog.Schema: {catalog}.{schema_g}")
    logger.info(f"  bronze_timeseries : {bronze_ts_table}")
    logger.info(f"  gold_timeseries   : {gold_ts_table}")
    logger.info(f"  gold_timeseries_agg: {gold_agg_table}")
    logger.info("=" * 60)

    # --- Incremental: find (uuid, group) pairs already in gold ---
    done_ts_df = _existing_pairs(spark, gold_ts_table)
    done_agg_df = _existing_pairs(spark, gold_agg_table)

    done_ts_count = done_ts_df.count()
    done_agg_count = done_agg_df.count()
    logger.info(f"  Existing gold_timeseries pairs: {done_ts_count}")
    logger.info(f"  Existing gold_timeseries_agg pairs: {done_agg_count}")

    missing_agg_df = done_ts_df.join(done_agg_df, on=["uuid", "group"], how="left_anti")
    missing_agg_count = missing_agg_df.count()

    # If raw gold exists but aggregate is missing, backfill aggregate from raw
    # gold instead of reprocessing bronze and duplicating raw rows.
    if missing_agg_count:
        logger.info(f"  Backfilling aggregate for {missing_agg_count} existing pair(s)")
        backfill_gold_ts = spark.read.table(gold_ts_table).join(
            missing_agg_df, on=["uuid", "group"], how="inner"
        )
        backfill_agg_df = _build_gold_timeseries_agg(backfill_gold_ts)
        backfill_count = backfill_agg_df.count()
        if backfill_count:
            loc_agg = build_external_table_location(
                storage_account=storage_account,
                container=gold_medal["adls_container"],
                layer=gold_medal["table_prefix"],
                table_name="timeseries_agg",
            )
            write_to_delta(backfill_agg_df, gold_agg_table, mode="append", location=loc_agg)
            logger.info(f"  Backfilled {backfill_count:,} aggregate rows -> {gold_agg_table}")

    # --- Load bronze ---
    ts = spark.read.table(bronze_ts_table)
    ch = spark.read.table(bronze_ch_table)
    fm = spark.read.table(bronze_fm_table)

    # --- Filter to raw gold pairs not written yet ---
    new_pairs = (
        ts.select("uuid", "group").distinct()
        .join(done_ts_df, on=["uuid", "group"], how="left_anti")
    )
    new_count = new_pairs.count()
    logger.info(f"  New (uuid, group) pairs to process: {new_count}")
    if new_count == 0:
        logger.info("  Nothing new to write. Exiting.")
        return

    new_pairs = F.broadcast(new_pairs)
    ts = ts.join(new_pairs, on=["uuid", "group"], how="inner")
    ch = ch.join(new_pairs, on=["uuid", "group"], how="inner")

    # --- Series is captured at convert time (see xlsx_converter.build_filemeta) ---
    # using the same governed series_config.json lookup that selects the channel
    # mapping, instead of re-deriving it here via regex on file_path.
    fm_series = fm.select("uuid", "series")

    # --- Join channel metadata onto timeseries rows ---
    ts_with_meta = ts.join(
        F.broadcast(ch.select("uuid", "group", "channel", "channel_name", "unit")),
        on=["uuid", "group", "channel"],
        how="left",
    )

    # --- Build gold_timeseries (long format, signals only) ---
    gold_ts_df = _build_gold_timeseries(ts_with_meta, fm_series).cache()

    ts_count = gold_ts_df.count()
    logger.info(f"  gold_timeseries rows (new): {ts_count:,}")

    loc_ts = build_external_table_location(
        storage_account=storage_account,
        container=gold_medal["adls_container"],
        layer=gold_medal["table_prefix"],
        table_name="timeseries",
    )
    write_to_delta(gold_ts_df, gold_ts_table, mode="append", location=loc_ts)
    logger.info(f"  Appended -> {gold_ts_table}")

    # --- Build gold_timeseries_agg (1-min bins) ---
    gold_agg_df = (
        _build_gold_timeseries_agg(gold_ts_df)
        .join(F.broadcast(done_agg_df), on=["uuid", "group"], how="left_anti")
    )

    agg_count = gold_agg_df.count()
    logger.info(f"  gold_timeseries_agg rows (new): {agg_count:,}")

    loc_agg = build_external_table_location(
        storage_account=storage_account,
        container=gold_medal["adls_container"],
        layer=gold_medal["table_prefix"],
        table_name="timeseries_agg",
    )
    write_to_delta(gold_agg_df, gold_agg_table, mode="append", location=loc_agg)
    gold_ts_df.unpersist()
    logger.info(f"  Appended -> {gold_agg_table}")


if __name__ == "__main__":
    main()
