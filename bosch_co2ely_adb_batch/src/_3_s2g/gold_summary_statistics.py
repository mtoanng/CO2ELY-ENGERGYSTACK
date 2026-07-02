"""Gold layer: Compute summary statistics for dashboard consumption.

Reads gold_timeseries, computes per-experiment summary KPIs
(mean voltage, total FE, peak current density, SPCE) and merges into
gold_summary_statistics consumed by the Dash reporting dashboard.

Incremental: only processes (uuid, group) pairs not yet present in the
target, using a MERGE to upsert results. Full-overwrite avoided to keep
cost O(new experiments) rather than O(all experiments).

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
from pyspark.sql.utils import AnalysisException

from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("gold_summary_statistics")


def _existing_pairs(spark: SparkSession, table: str):
    """Return distinct (uuid, group) pairs already present in a Delta table."""
    try:
        return spark.read.table(table).select("uuid", "group").distinct()
    except AnalysisException:
        return spark.createDataFrame([], "uuid string, group string")


def main():
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    write_medal = medallion_variables("gold", environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    logger.info(f"Environment: {environment}")

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
    logger.info(f"Writing to:   {target_table}")

    # --- Incremental: only process new (uuid, group) pairs ---
    done_pairs = _existing_pairs(spark, target_table)
    gold_ts = spark.read.table(source_table)

    new_pairs = (
        gold_ts.select("uuid", "group").distinct()
        .join(done_pairs, on=["uuid", "group"], how="left_anti")
    )
    new_count = new_pairs.count()
    logger.info(f"New (uuid, group) pairs to compute: {new_count}")

    if new_count == 0:
        logger.info("Nothing new. Exiting.")
        return

    # Filter gold_timeseries to only new pairs
    gold_ts_new = gold_ts.join(F.broadcast(new_pairs), on=["uuid", "group"], how="inner")

    # Per-experiment summary: one row per (series, uuid, group)
    df_summary = gold_ts_new.groupBy("series", "uuid", "group").agg(
        F.count("*").alias("total_data_points"),
        F.avg(F.when(F.col("std_channel") == "Stack Voltage", F.col("value"))).alias("avg_stack_voltage_v"),
        F.max(F.when(F.col("std_channel") == "Current density", F.col("value"))).alias("peak_current_density_ma_cm2"),
        F.avg(F.when(F.col("std_channel") == "Energy Efficiency", F.col("value"))).alias("avg_energy_efficiency_pct"),
        F.avg(F.when(F.col("std_channel") == "Faradaic Efficiency of CO", F.col("value"))).alias("avg_fe_co_pct"),
        F.avg(F.when(F.col("std_channel") == "Faradaic Efficiency of H2", F.col("value"))).alias("avg_fe_h2_pct"),
        F.avg(F.when(F.col("std_channel") == "Single Pass Conversion Efficiency", F.col("value"))).alias("avg_spce_pct"),
        F.min("elapsed_time_s").alias("start_time_s"),
        F.max("elapsed_time_s").alias("end_time_s"),
        F.countDistinct("channel_id").alias("channel_count"),
    ).withColumn(
        "duration_s", F.col("end_time_s") - F.col("start_time_s")
    )

    summary_count = df_summary.count()
    logger.info(f"Computed {summary_count} new summary row(s)")

    gold_layer = "gold_int_test" if args.is_integration_test else write_medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=write_medal["adls_container"],
        layer=gold_layer,
        table_name="summary_statistics",
    )
    write_to_delta(df_summary, target_table, mode="append", location=location)
    logger.info(f"Successfully appended to {target_table}")


if __name__ == "__main__":
    main()
