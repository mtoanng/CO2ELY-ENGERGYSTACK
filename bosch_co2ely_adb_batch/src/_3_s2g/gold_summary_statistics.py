"""Gold layer: Compute summary statistics for dashboard consumption.

Reads silver enriched table, computes per-experiment summary KPIs
(mean voltage, total FE, peak current density, SPCE) and writes to
gold summary table consumed by the Dash reporting dashboard.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_3_s2g/gold_summary_statistics.py
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
from pyspark.sql import functions as F

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("gold_summary_statistics")


def main():
    """Main entry point for gold summary statistics."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    write_medal = medallion_variables("gold", environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    logger.info(f"Environment: {environment}")

    # Read from gold_timeseries (has series column + derived metric channels)
    source_table = build_table_name(
        unity_catalog=catalog,
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="timeseries",
        is_integration_test=args.is_integration_test,
    )
    target_table = build_table_name(
        unity_catalog=catalog,
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="summary_statistics",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Reading from: {source_table}")
    df = spark.read.table(source_table)

    # Per-experiment summary: one row per (series, uuid, group)
    df_summary = df.groupBy("series", "uuid", "group").agg(
        F.count("*").alias("total_data_points"),
        F.avg(F.when(F.col("channel_name") == "Stack Voltage", F.col("value"))).alias("avg_stack_voltage_v"),
        F.max(F.when(F.col("channel_name") == "Current density", F.col("value"))).alias("peak_current_density_ma_cm2"),
        F.avg(F.when(F.col("channel_name") == "Energy Efficiency", F.col("value"))).alias("avg_energy_efficiency_pct"),
        F.avg(F.when(F.col("channel_name") == "Faradaic Efficiency of CO", F.col("value"))).alias("avg_fe_co_pct"),
        F.avg(F.when(F.col("channel_name") == "Faradaic Efficiency of H2", F.col("value"))).alias("avg_fe_h2_pct"),
        F.avg(F.when(F.col("channel_name") == "Single Pass Conversion Efficiency", F.col("value"))).alias("avg_spce_pct"),
        F.min("elapsed_time_s").alias("start_time_s"),
        F.max("elapsed_time_s").alias("end_time_s"),
        F.countDistinct("channel").alias("channel_count"),
    ).withColumn(
        "duration_s", F.col("end_time_s") - F.col("start_time_s")
    )

    logger.info("Computed summary statistics")

    gold_layer = "gold_int_test" if args.is_integration_test else write_medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=write_medal["adls_container"],
        layer=gold_layer,
        table_name="summary_statistics",
    )
    write_to_delta(df_summary, target_table, mode="overwrite", location=location)
    logger.info(f"Successfully wrote to {target_table}")


if __name__ == "__main__":
    main()
