"""Silver base layer: publish bronze outputs as silver dim/fact tables.

Reads the bronze converter tables and republishes them as external silver
tables using semantic warehouse-style names.

Outputs:
    silver_dim_filemeta
    silver_dim_channel
    silver_fact_statistics

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/silver_base.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import build_external_table_location

logger = configure_logger("silver_base")

SILVER_ENTITY_MAP = {
    "filemeta": "dim_filemeta",
    "channel": "dim_channel",
    "statistics": "fact_statistics",
}


def _append_table_stream(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    location: str,
    checkpoint_path: str,
) -> int:
    """Append only new Bronze Delta commits to the Silver table."""
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    stream_df = (
        spark.readStream
        .table(source_table)
        .withColumn("_silver_published_at", current_timestamp())
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .option("path", location)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    rows_written = 0
    if query.lastProgress and query.lastProgress.get("numInputRows"):
        rows_written = query.lastProgress["numInputRows"]
    else:
        for progress in query.recentProgress or []:
            rows_written += progress.get("numInputRows", 0)

    logger.info(f"Published {rows_written:,} new row(s): {source_table} -> {target_table}")
    return rows_written


def main():
    """Main entry point for silver base publication."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    layers = layer_variables("_2_b2s")
    read_medal = medallion_variables(layers["read_layer"], environment)
    write_medal = medallion_variables(layers["write_layer"], environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    logger.info(f"{'='*60}")
    logger.info("Silver Base Publication")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Catalog.Schema: {catalog}.{write_medal['uc_schema']}")
    logger.info(f"  Integration test: {args.is_integration_test}")
    logger.info(f"{'='*60}")

    silver_layer = "silver_int_test" if args.is_integration_test else write_medal["table_prefix"]
    checkpoint_base = build_external_table_location(
        storage_account=storage_account,
        container=write_medal["adls_container"],
        layer=silver_layer,
        table_name="_checkpoints",
    ).rstrip("/")

    total_rows = {}
    for bronze_table, silver_table in SILVER_ENTITY_MAP.items():
        source_table = build_table_name(
            unity_catalog=catalog,
            schema=read_medal["uc_schema"],
            prefix=read_medal["table_prefix"],
            table=bronze_table,
            is_integration_test=args.is_integration_test,
        )
        target_table = build_table_name(
            unity_catalog=catalog,
            schema=write_medal["uc_schema"],
            prefix=write_medal["table_prefix"],
            table=silver_table,
            is_integration_test=args.is_integration_test,
        )
        logger.info(f"Streaming from: {source_table}")
        logger.info(f"Appending to: {target_table}")
        location = build_external_table_location(
            storage_account=storage_account,
            container=write_medal["adls_container"],
            layer=silver_layer,
            table_name=silver_table,
        )
        checkpoint_path = f"{checkpoint_base}/silver_base_{silver_table}"
        total_rows[silver_table] = _append_table_stream(
            spark,
            source_table,
            target_table,
            location,
            checkpoint_path,
        )

    total_written = sum(total_rows.values())
    logger.info(f"Silver Base append summary: {total_written:,} new row(s)")


if __name__ == "__main__":
    main()
