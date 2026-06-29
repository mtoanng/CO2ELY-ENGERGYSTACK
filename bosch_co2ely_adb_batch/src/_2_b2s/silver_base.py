"""Silver base layer: publish bronze outputs as silver dim/fact tables.

Reads the bronze converter tables and republishes them as external silver
tables using semantic warehouse-style names.

Outputs:
    silver_dim_filemeta
    silver_dim_channel
    silver_fact_timeseries
    silver_fact_statistics

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/silver_base.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("silver_base")

SILVER_ENTITY_MAP = {
    "filemeta": "dim_filemeta",
    "channel": "dim_channel",
    "timeseries": "fact_timeseries",
    "statistics": "fact_statistics",
}


def _copy_table(spark, source_table: str, target_table: str, location: str):
    """Copy a bronze table to its external silver dim/fact counterpart."""
    df = spark.read.table(source_table).withColumn("_silver_published_at", current_timestamp())
    write_to_delta(df, target_table, mode="overwrite", location=location)
    logger.info(f"Published {source_table} -> {target_table}")


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
        logger.info(f"Reading from: {source_table}")
        logger.info(f"Writing to: {target_table}")
        location = build_external_table_location(
            storage_account=storage_account,
            container=write_medal["adls_container"],
            layer=write_medal["table_prefix"],
            table_name=silver_table,
        )
        _copy_table(spark, source_table, target_table, location)


if __name__ == "__main__":
    main()
