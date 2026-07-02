"""Gold layer: Demo-ready timeseries tables (Silver -> Gold).

Reads curated silver enriched timeseries. Incremental append-only: only processes
(uuid, group) pairs not already present in the gold tables.

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

logger = configure_logger("gold_timeseries")

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


def _build_gold_timeseries(silver_ts: DataFrame) -> DataFrame:
    """Shape curated Silver rows into the app-serving Gold contract."""
    return silver_ts.select(
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
    read_medal = medallion_variables(layers["read_layer"], environment)
    gold_medal = medallion_variables(layers["write_layer"], environment)

    schema_r = read_medal["uc_schema"]
    schema_g = gold_medal["uc_schema"]

    silver_ts_table = build_table_name(
        catalog, schema_r, read_medal["table_prefix"], "fact_timeseries_enriched", args.is_integration_test
    )
    gold_ts_table = build_table_name(catalog, schema_g, "gold", "timeseries", args.is_integration_test)
    gold_agg_table = build_table_name(catalog, schema_g, "gold", "timeseries_agg", args.is_integration_test)

    logger.info("=" * 60)
    logger.info("Gold timeseries (Silver -> Gold, append-only)")
    logger.info(f"  Catalog.Schema: {catalog}.{schema_g}")
    logger.info(f"  silver_timeseries : {silver_ts_table}")
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

    # --- Load curated silver ---
    silver_ts = spark.read.table(silver_ts_table)

    # --- Filter to (uuid, group) pairs not yet in gold_timeseries ---
    new_pairs = (
        silver_ts.select("uuid", "group").distinct()
        .join(done_ts_df, on=["uuid", "group"], how="left_anti")
    )
    new_count = new_pairs.count()
    logger.info(f"  New (uuid, group) pairs to process: {new_count}")
    if new_count == 0:
        logger.info("  Nothing new to write. Exiting.")
        return

    silver_ts = silver_ts.join(F.broadcast(new_pairs), on=["uuid", "group"], how="inner")

    # --- Build gold_timeseries (long format, signals only) ---
    gold_ts_df = _build_gold_timeseries(silver_ts).cache()

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
