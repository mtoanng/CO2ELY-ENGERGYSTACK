"""Silver refinement for timeseries facts.

Reads Bronze converter tables directly. Bronze timeseries contains signal rows
only, with raw channel metadata and structural timestamp/elapsed columns.
Silver applies canonical channel mapping, joins file/channel metadata, and
standardizes time columns.

Outputs:
    silver_fact_timeseries_enriched
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
from _2_b2s.silver_channel_mapping import build_channel_mapping_df

logger = configure_logger("silver_timeseries_enriched")

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
        .withColumn("elapsed_time", F.col("elapsed_time_s"))
    )

    return enriched.select(
        "series",
        "uuid",
        "group",
        "sample_offset",
        "event_ts",
        "is_valid_timestamp",
        "elapsed_time",
        "channel_id",
        "raw_channel",
        "std_channel",
        "unit",
        "value",
        "value_str",
    )


def main():
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    layers = layer_variables("_2_b2s")
    read_medal = medallion_variables(layers["read_layer"], environment)
    write_medal = medallion_variables(layers["write_layer"], environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    source_timeseries = build_table_name(
        unity_catalog=catalog,
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="timeseries",
        is_integration_test=args.is_integration_test,
    )
    source_channel = build_table_name(
        unity_catalog=catalog,
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="channel",
        is_integration_test=args.is_integration_test,
    )
    source_filemeta = build_table_name(
        unity_catalog=catalog,
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="filemeta",
        is_integration_test=args.is_integration_test,
    )
    target_table = build_table_name(
        unity_catalog=catalog,
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="fact_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Streaming from: {source_timeseries}")
    logger.info(f"Joining channel metadata from: {source_channel}")
    logger.info(f"Joining file metadata from: {source_filemeta}")
    logger.info(f"Appending to: {target_table}")

    silver_layer = "silver_int_test" if args.is_integration_test else write_medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=write_medal["adls_container"],
        layer=silver_layer,
        table_name="fact_timeseries_enriched",
    )
    checkpoint_path = build_external_table_location(
        storage_account=storage_account,
        container=write_medal["adls_container"],
        layer=silver_layer,
        table_name="_checkpoints/silver_fact_timeseries_enriched",
    )

    mapping_df = build_channel_mapping_df(spark)

    def append_batch(batch_df: DataFrame, batch_id: int) -> None:
        channel_df = spark.read.table(source_channel)
        filemeta_df = spark.read.table(source_filemeta)
        enriched_df = build_enriched_timeseries_df(batch_df, channel_df, filemeta_df, mapping_df)

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
