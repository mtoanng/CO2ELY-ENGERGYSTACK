"""Gold layer: Demo-ready timeseries tables (Bronze + Silver dims -> Gold).

Reads Bronze timeseries and joins the small conformed Silver dimensions at
Gold-build time. Incremental append-only: only processes (uuid, group) pairs
not already present in the gold tables.

Produces two tables:

gold_timeseries  — long format, one row per sample_offset per signal channel
    series          string
    uuid            string
    group           string
    sample_offset   bigint
    timestamp       timestamp  -- standardized event timestamp from Silver
    elapsed_time_s  double     -- standardized elapsed time from Silver
    channel_id      string     -- signal channel join key from row-1 header
    raw_channel     string     -- row-2 display name carried from Bronze
    std_channel     string     -- canonical mapped channel name from Silver
    unit            string     -- carried from channel metadata
    value           double
    value_str       string

gold_timeseries_agg  — pre-aggregated, 1-minute elapsed-time bins
    series          string
    uuid            string
    group           string
    elapsed_bin_s   double     -- FLOOR(elapsed_time_s / 60) * 60
    channel_id      string
    raw_channel     string
    std_channel     string
    unit            string
    elapsed_time_s  double     -- first elapsed_time_s in the bin (bin start)
    timestamp       timestamp  -- first timestamp in the bin
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
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location
from _2_b2s.silver_channel_mapping import build_channel_mapping_df

logger = configure_logger("gold_timeseries")

# Aggregation bin size in seconds.
_AGG_BIN_S = 60.0

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
    parsed = None
    for fmt in _TIMESTAMP_FORMATS:
        candidate = F.to_timestamp(raw_col, fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
    return F.coalesce(parsed, F.to_timestamp(raw_col))


def build_enriched_timeseries_df(
    timeseries_df: DataFrame, channel_df: DataFrame, filemeta_df: DataFrame, mapping_df: DataFrame
) -> DataFrame:
    channel_meta = channel_df.select(
        "uuid",
        "group",
        "channel_id",
        "raw_channel",
        "unit",
    )
    file_series = filemeta_df.select("uuid", "series")

    joined = (
        timeseries_df
        .join(F.broadcast(channel_meta), on=["uuid", "group", "channel_id"], how="left")
        .join(F.broadcast(file_series), on="uuid", how="left")
        .join(F.broadcast(mapping_df), on=["series", "raw_channel"], how="left")
    )

    enriched = (
        joined
        .withColumn("std_channel", F.coalesce(F.col("mapped_std_channel"), F.col("raw_channel")))
        .withColumn("event_ts", _parse_timestamp(F.col("timestamp")))
        .withColumn(
            "is_valid_timestamp",
            F.when(F.col("timestamp").isNull(), F.lit(False))
            .when(F.col("event_ts").isNotNull(), F.lit(True))
            .otherwise(F.lit(False)),
        )
    )

    return enriched.select(
        "series",
        "uuid",
        "group",
        "sample_offset",
        "event_ts",
        "is_valid_timestamp",
        "elapsed_time_s",
        "channel_id",
        "raw_channel",
        "std_channel",
        "unit",
        "value",
        "value_str",
    )


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
    timeseries_df: DataFrame, channel_df: DataFrame, filemeta_df: DataFrame, mapping_df: DataFrame
) -> DataFrame:
    """Join Bronze fact rows with Silver dimensions and shape the Gold contract."""
    enriched_ts = build_enriched_timeseries_df(timeseries_df, channel_df, filemeta_df, mapping_df)
    return enriched_ts.select(
        "series", "uuid", "group", "sample_offset",
        F.col("event_ts").alias("timestamp"),
        "elapsed_time_s",
        "channel_id", "raw_channel", "std_channel", "unit",
        "value", "value_str",
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
        .groupBy("series", "uuid", "group", "elapsed_bin_s", "channel_id", "raw_channel", "std_channel", "unit")
        .agg(
            F.first("elapsed_time_s", ignorenulls=True).alias("elapsed_time_s"),
            F.first("timestamp",      ignorenulls=True).alias("timestamp"),
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

    bronze_ts_table = build_table_name(
        catalog, schema_b, bronze_medal["table_prefix"], "timeseries", args.is_integration_test
    )
    silver_channel_table = build_table_name(
        catalog, schema_s, silver_medal["table_prefix"], "dim_channel", args.is_integration_test
    )
    silver_filemeta_table = build_table_name(
        catalog, schema_s, silver_medal["table_prefix"], "dim_filemeta", args.is_integration_test
    )
    gold_ts_table = build_table_name(catalog, schema_g, "gold", "timeseries", args.is_integration_test)
    gold_agg_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg", args.is_integration_test)

    logger.info("=" * 60)
    logger.info("Gold timeseries (Bronze + Silver dims -> Gold, append-only)")
    logger.info(f"  Catalog.Schema: {catalog}.{schema_g}")
    logger.info(f"  bronze_timeseries : {bronze_ts_table}")
    logger.info(f"  silver_channel    : {silver_channel_table}")
    logger.info(f"  silver_filemeta   : {silver_filemeta_table}")
    logger.info(f"  gold_timeseries   : {gold_ts_table}")
    logger.info(f"  gold_timeseries_agg: {gold_agg_table}")
    logger.info("=" * 60)

    # --- Incremental: find (uuid, group) pairs already in gold ---
    done_ts_df = _existing_pairs(spark, gold_ts_table)
    done_agg_df = _existing_pairs(spark, gold_agg_table)

    # Only count missing_agg (used for branching); skip full-count log scans
    missing_agg_df = done_ts_df.join(done_agg_df, on=["uuid", "group"], how="left_anti")
    missing_agg_count = missing_agg_df.count()

    # If raw gold exists but aggregate is missing, backfill aggregate from raw
    # gold instead of reprocessing silver and duplicating raw rows.
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

        # Refresh done_agg_df after backfill so the left-anti join below
        # correctly excludes the pairs we just wrote.
        done_agg_df = _existing_pairs(spark, gold_agg_table)

    # --- Load Bronze fact and filter to (uuid, group) pairs not yet in gold_timeseries ---
    bronze_ts = spark.read.table(bronze_ts_table)
    new_pairs = (
        bronze_ts.select("uuid", "group").distinct()
        .join(done_ts_df, on=["uuid", "group"], how="left_anti")
    )
    new_count = new_pairs.count()
    logger.info(f"  New (uuid, group) pairs to process: {new_count}")
    if new_count == 0:
        logger.info("  Nothing new to write. Exiting.")
        return

    bronze_ts = bronze_ts.join(F.broadcast(new_pairs), on=["uuid", "group"], how="inner")
    channel_df = spark.read.table(silver_channel_table)
    filemeta_df = spark.read.table(silver_filemeta_table)
    mapping_df = build_channel_mapping_df(spark)

    # --- Build gold_timeseries (long format, signals only) ---
    gold_ts_df = _build_gold_timeseries(bronze_ts, channel_df, filemeta_df, mapping_df).cache()

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
    # Exclude pairs already in done_agg_df (covers backfilled pairs too).
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
    logger.info(f"  Appended -> {gold_agg_table}")

    gold_ts_df.unpersist()


if __name__ == "__main__":
    main()
