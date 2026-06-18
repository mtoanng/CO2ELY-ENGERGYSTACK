"""Gold layer: Compute summary statistics for dashboard consumption.

Reads silver enriched table, computes per-experiment summary KPIs
(mean voltage, total FE, peak current density, SPCE) and writes to
gold summary table consumed by the Dash reporting dashboard.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_3_gold/gold_summary_statistics.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max as spark_max, min as spark_min, count

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.io_utils import write_to_delta

logger = configure_logger("gold_summary_statistics")


def main():
    """Main entry point for gold summary statistics."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve environment
    env = env_variables(spark, env_override=args.env)
    read_layer = layer_variables("_3_gold")
    read_medal = medallion_variables(read_layer["read_layer"])
    write_medal = medallion_variables(read_layer["write_layer"])

    logger.info(f"Environment: {env['environment']}")

    # Source: silver enriched
    source_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="co2_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    # Target: gold summary
    target_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_summary_statistics",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Reading from: {source_table}")
    df = spark.read.table(source_table)

    # Compute summary KPIs — mirrors Dash app's summary panel
    df_summary = df.agg(
        count("*").alias("total_data_points"),
        avg("Stack Voltage").alias("avg_stack_voltage_v"),
        spark_max("Current density").alias("peak_current_density_ma_cm2"),
        avg("Energy Efficiency").alias("avg_energy_efficiency_pct"),
        avg("Faradaic Efficiency of CO").alias("avg_fe_co_pct"),
        avg("Faradaic Efficiency of H2").alias("avg_fe_h2_pct"),
        avg("Single Pass Conversion Efficiency").alias("avg_spce_pct"),
        spark_min("Elapsed time").alias("start_time_s"),
        spark_max("Elapsed time").alias("end_time_s"),
    )

    logger.info("Computed summary statistics")

    write_to_delta(df_summary, target_table, mode="overwrite")
    logger.info(f"Successfully wrote to {target_table}")


if __name__ == "__main__":
    main()
