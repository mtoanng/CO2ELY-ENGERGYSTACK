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

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
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

logger = configure_logger("silver_base")

SILVER_ENTITY_MAP = {
    "filemeta": "dim_filemeta",
    "channel": "dim_channel",
    "timeseries": "fact_timeseries",
    "statistics": "fact_statistics",
}

SILVER_ENTITY_KEYS = {
    "filemeta": ["uuid"],
    "channel": ["uuid", "group", "channel"],
    "timeseries": ["uuid", "group", "sample_offset", "channel"],
    "statistics": ["uuid", "group"],
}


def _copy_table(spark, source_table: str, target_table: str, location: str, key_columns: list[str]):
    """Append bronze rows not yet present in the silver dim/fact table."""
    df = spark.read.table(source_table).withColumn("_silver_published_at", current_timestamp())

    try:
        existing_keys = spark.read.table(target_table).select(*key_columns).distinct()
        df = df.join(existing_keys, on=key_columns, how="left_anti")
    except AnalysisException:
        pass

    rows_to_write = df.count()
    if rows_to_write == 0:
        logger.info(f"No new rows for {target_table}")
        return

    write_to_delta(df, target_table, mode="append", location=location)
    logger.info(f"Published {rows_to_write:,} new row(s): {source_table} -> {target_table}")


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
        _copy_table(spark, source_table, target_table, location, SILVER_ENTITY_KEYS[bronze_table])


if __name__ == "__main__":
    main()
