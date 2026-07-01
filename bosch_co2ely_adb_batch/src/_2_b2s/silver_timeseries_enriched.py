"""Silver refinement for timeseries facts.

Promotes timestamp-like channels into dedicated semantic columns while keeping
measurement signals in long format. Bronze stays generic; silver adds meaning.

Outputs:
    silver_fact_timeseries_enriched

Behavior:
- Detect event-time channels from channel metadata (e.g. real time, timestamp)
- Detect elapsed-time channels from channel metadata (e.g. Time)
- Parse event timestamps into event_ts and annotate is_valid_timestamp
- Parse elapsed time into elapsed_time where possible
- Exclude detected structural time channels from measurement rows
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession, DataFrame, functions as F

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import build_external_table_location

logger = configure_logger("silver_timeseries_enriched")

_EVENT_TIME_PATTERNS = [
    r"(?i)^timestamp$",
    r"(?i)^real\s*time$",
    r"(?i)^date\s*time$",
    r"(?i)^datetime$",
    r"(?i)^date/time$",
    r"(?i)^measurement\s*time$",
    r"(?i)^recorded\s*time$",
]

_ELAPSED_TIME_PATTERNS = [
    r"(?i)^time$",
    r"(?i)^elapsed\s*time$",
    r"(?i)^run\s*time$",
    r"(?i)^duration$",
]

_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy/MM/dd HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
]


def _matches_any(column, patterns: list[str]):
    expr = F.lit(False)
    for pattern in patterns:
        expr = expr | column.rlike(pattern)
    return expr


def _detect_channel_roles(channel_df: DataFrame) -> DataFrame:
    name_expr = F.coalesce(F.col("channel_name"), F.col("channel"), F.lit(""))
    id_expr = F.coalesce(F.col("channel"), F.lit(""))

    is_event_time = _matches_any(name_expr, _EVENT_TIME_PATTERNS) | _matches_any(id_expr, _EVENT_TIME_PATTERNS)
    is_elapsed_time = _matches_any(name_expr, _ELAPSED_TIME_PATTERNS) | _matches_any(id_expr, _ELAPSED_TIME_PATTERNS)

    return (
        channel_df.withColumn("is_event_time_channel", is_event_time)
        .withColumn("is_elapsed_time_channel", is_elapsed_time & ~is_event_time)
    )


def _parse_timestamp(raw_col) -> F.Column:
    parsed = None
    for fmt in _TIMESTAMP_FORMATS:
        candidate = F.to_timestamp(raw_col, fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
    return F.coalesce(parsed, F.to_timestamp(raw_col))


def build_enriched_timeseries_df(timeseries_df: DataFrame, channel_df: DataFrame) -> DataFrame:
    channel_roles = _detect_channel_roles(channel_df)

    joined = timeseries_df.join(
        channel_roles.select(
            "uuid",
            "group",
            "channel",
            "channel_name",
            "unit",
            "is_event_time_channel",
            "is_elapsed_time_channel",
        ),
        on=["uuid", "group", "channel"],
        how="left",
    )

    raw_value = F.coalesce(F.col("value_str"), F.col("value").cast("string"))

    event_time_rows = joined.filter(F.col("is_event_time_channel") == True)
    event_time_per_row = (
        event_time_rows.select(
            "uuid",
            "group",
            "sample_offset",
            raw_value.alias("event_ts_raw"),
        )
        .groupBy("uuid", "group", "sample_offset")
        .agg(F.first("event_ts_raw", ignorenulls=True).alias("event_ts_raw"))
        .withColumn("event_ts", _parse_timestamp(F.col("event_ts_raw")))
        .withColumn(
            "is_valid_timestamp",
            F.when(F.col("event_ts_raw").isNull(), F.lit(False))
            .when(F.col("event_ts").isNotNull(), F.lit(True))
            .otherwise(F.lit(False)),
        )
    )

    elapsed_time_rows = joined.filter(F.col("is_elapsed_time_channel") == True)
    elapsed_time_per_row = (
        elapsed_time_rows.select(
            "uuid",
            "group",
            "sample_offset",
            F.col("value").alias("elapsed_time_numeric"),
            raw_value.alias("elapsed_time_raw"),
        )
        .groupBy("uuid", "group", "sample_offset")
        .agg(
            F.first("elapsed_time_numeric", ignorenulls=True).alias("elapsed_time"),
            F.first("elapsed_time_raw", ignorenulls=True).alias("elapsed_time_raw"),
        )
    )

    measurements = joined.filter(
        ~F.coalesce(F.col("is_event_time_channel"), F.lit(False))
        & ~F.coalesce(F.col("is_elapsed_time_channel"), F.lit(False))
    )

    return (
        measurements.join(event_time_per_row, on=["uuid", "group", "sample_offset"], how="left")
        .join(elapsed_time_per_row, on=["uuid", "group", "sample_offset"], how="left")
        .withColumn(
            "is_valid_timestamp",
            F.coalesce(F.col("is_valid_timestamp"), F.lit(False)),
        )
        .select(
            "uuid",
            "group",
            "sample_offset",
            "event_ts",
            "event_ts_raw",
            "is_valid_timestamp",
            "elapsed_time",
            "elapsed_time_raw",
            "channel",
            "channel_name",
            "unit",
            "value",
            "value_str",
        )
    )


def main():
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    layers = layer_variables("_2_b2s")
    medal = medallion_variables(layers["write_layer"], environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    source_timeseries = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries",
        is_integration_test=args.is_integration_test,
    )
    source_channel = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="dim_channel",
        is_integration_test=args.is_integration_test,
    )
    target_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Streaming from: {source_timeseries}")
    logger.info(f"Joining channel metadata from: {source_channel}")
    logger.info(f"Appending to: {target_table}")

    silver_layer = "silver_int_test" if args.is_integration_test else medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="fact_timeseries_enriched",
    )
    checkpoint_path = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="_checkpoints/silver_fact_timeseries_enriched",
    )

    def append_batch(batch_df: DataFrame, batch_id: int) -> None:
        channel_df = spark.read.table(source_channel)
        enriched_df = build_enriched_timeseries_df(batch_df, channel_df).withColumn(
            "_silver_enriched_at",
            F.current_timestamp(),
        )

        (
            enriched_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("path", location)
            .saveAsTable(target_table)
        )
        logger.info(f"Batch {batch_id}: append completed -> {target_table}")

    query = (
        spark.readStream
        .table(source_timeseries)
        .writeStream
        .foreachBatch(append_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    rows_read = 0
    if query.lastProgress and query.lastProgress.get("numInputRows"):
        rows_read = query.lastProgress["numInputRows"]
    else:
        for progress in query.recentProgress or []:
            rows_read += progress.get("numInputRows", 0)
    logger.info(f"Processed {rows_read:,} new source row(s) -> {target_table}")


if __name__ == "__main__":
    main()
