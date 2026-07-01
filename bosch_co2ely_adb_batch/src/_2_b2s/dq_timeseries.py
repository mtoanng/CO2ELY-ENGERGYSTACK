"""Silver DQ task for the timeseries fact table.

Evaluates a composable rule matrix against silver_fact_timeseries and writes:
    silver_dim_dq_rule
    silver_fact_timeseries_dq_result
    silver_fact_timeseries_dq_summary

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/dq_timeseries.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
import json
import argparse
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession, DataFrame, functions as F

from _2_b2s.dq_engine import DEFAULT_TIMESERIES_RULES, evaluate_rules, summarize_failures
from _5_common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _5_common.common_io_utils import write_to_delta, build_external_table_location

logger = configure_logger("dq_timeseries")

_TIMESERIES_KEY_COLUMNS = ["uuid", "group", "sample_offset", "channel"]


def get_dq_job_args() -> argparse.Namespace:
    """Parse base job args plus DQ-specific cleanup controls."""
    args = get_job_args()
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cleanup_mode", type=str, default="delete_invalid")
    parser.add_argument("--apply_to_blocking_only", type=str, default="true")
    dq_args, _ = parser.parse_known_args()
    args.cleanup_mode = dq_args.cleanup_mode.lower()
    args.apply_to_blocking_only = dq_args.apply_to_blocking_only.lower() == "true"
    return args


def _select_invalid_keys(failures, apply_to_blocking_only: bool):
    """Return the distinct invalid row keys targeted by cleanup behavior."""
    selected = failures.filter(F.col("is_blocking") == True) if apply_to_blocking_only else failures
    return selected.select(*_TIMESERIES_KEY_COLUMNS).distinct()


def apply_is_valid_flags(spark, source_table: str, invalid_keys):
    """Mark invalid rows from the current DQ batch without rewriting history."""
    invalid_keys.createOrReplaceTempView("_silver_invalid_timeseries_keys")
    spark.sql(f"""
        MERGE INTO {source_table} t
        USING _silver_invalid_timeseries_keys s
        ON t.uuid = s.uuid
        AND t.`group` = s.`group`
        AND t.sample_offset = s.sample_offset
        AND t.channel = s.channel
        WHEN MATCHED THEN UPDATE SET
            is_valid = false
    """)
    spark.catalog.dropTempView("_silver_invalid_timeseries_keys")


def delete_invalid_rows(spark, source_table: str, invalid_keys):
    """Delete invalid fact rows from the current DQ batch in place."""
    invalid_keys.createOrReplaceTempView("_silver_invalid_timeseries_keys")
    spark.sql(f"""
        MERGE INTO {source_table} t
        USING _silver_invalid_timeseries_keys s
        ON t.uuid = s.uuid
        AND t.`group` = s.`group`
        AND t.sample_offset = s.sample_offset
        AND t.channel = s.channel
        WHEN MATCHED THEN DELETE
    """)
    spark.catalog.dropTempView("_silver_invalid_timeseries_keys")


def build_clean_timeseries_df(df, invalid_keys):
    """Return only valid timeseries rows with is_valid annotation retained."""
    clean_df = df.join(invalid_keys.withColumn("_is_invalid", F.lit(True)), on=_TIMESERIES_KEY_COLUMNS, how="left")
    return (
        clean_df.withColumn("is_valid", F.when(F.col("_is_invalid") == True, F.lit(False)).otherwise(F.lit(True)))
        .filter(F.col("is_valid") == True)
        .drop("_is_invalid")
    )


def main():
    """Main entry point for silver timeseries data-quality evaluation."""
    args = get_dq_job_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    layers = layer_variables("_2_b2s")
    medal = medallion_variables(layers["write_layer"], environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    logger.info(f"{'='*60}")
    logger.info("Silver Timeseries DQ")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Catalog.Schema: {catalog}.{medal['uc_schema']}")
    logger.info(f"  Integration test: {args.is_integration_test}")
    logger.info(f"  Cleanup mode: {args.cleanup_mode}")
    logger.info(f"  Blocking rules only: {args.apply_to_blocking_only}")
    logger.info(f"{'='*60}")

    source_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries",
        is_integration_test=args.is_integration_test,
    )
    result_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries_dq_result",
        is_integration_test=args.is_integration_test,
    )
    summary_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries_dq_summary",
        is_integration_test=args.is_integration_test,
    )
    rule_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="dim_dq_rule",
        is_integration_test=args.is_integration_test,
    )
    clean_table = build_table_name(
        unity_catalog=catalog,
        schema=medal["uc_schema"],
        prefix=medal["table_prefix"],
        table="fact_timeseries_clean",
        is_integration_test=args.is_integration_test,
    )

    if args.cleanup_mode not in {"annotate", "delete_invalid", "write_clean_table"}:
        raise ValueError(
            "Unsupported cleanup_mode. Expected one of: annotate, delete_invalid, write_clean_table"
        )

    silver_layer = "silver_int_test" if args.is_integration_test else medal["table_prefix"]
    checkpoint_path = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="_checkpoints/dq_timeseries",
    )
    result_location = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="fact_timeseries_dq_result",
    )
    summary_location = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="fact_timeseries_dq_summary",
    )
    rule_location = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="dim_dq_rule",
    )
    clean_location = build_external_table_location(
        storage_account=storage_account,
        container=medal["adls_container"],
        layer=silver_layer,
        table_name="fact_timeseries_clean",
    )

    rules_df = spark.createDataFrame([
        {
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "target_column": rule.target_column,
            "rule_type": rule.rule_type,
            "severity": rule.severity,
            "params_json": json.dumps(rule.params, sort_keys=True),
            "enabled": rule.enabled,
            "is_blocking": rule.is_blocking,
        }
        for rule in DEFAULT_TIMESERIES_RULES
    ])
    write_to_delta(rules_df, rule_table, mode="overwrite", location=rule_location)
    logger.info(f"Wrote DQ rule dimension -> {rule_table}")
    logger.info(f"Streaming new rows from: {source_table}")

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        failures = evaluate_rules(batch_df, DEFAULT_TIMESERIES_RULES).withColumn(
            "observed_value",
            F.coalesce(F.col("value_str"), F.col("value").cast("string")),
        ).withColumn("dq_batch_id", F.lit(batch_id))
        summary = summarize_failures(failures).withColumn("dq_batch_id", F.lit(batch_id))
        invalid_keys = _select_invalid_keys(failures, args.apply_to_blocking_only)

        write_to_delta(failures, result_table, mode="append", location=result_location)
        write_to_delta(summary, summary_table, mode="append", location=summary_location)

        if args.cleanup_mode == "write_clean_table":
            clean_df = build_clean_timeseries_df(batch_df, invalid_keys).withColumn("dq_batch_id", F.lit(batch_id))
            write_to_delta(clean_df, clean_table, mode="append", location=clean_location)
        elif args.cleanup_mode == "annotate":
            apply_is_valid_flags(spark, source_table, invalid_keys)
        elif args.cleanup_mode == "delete_invalid":
            delete_invalid_rows(spark, source_table, invalid_keys)

        logger.info(f"Batch {batch_id}: DQ processing completed")

    query = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(source_table)
        .writeStream
        .foreachBatch(process_batch)
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
    logger.info(f"DQ evaluated {rows_read:,} new source row(s)")


if __name__ == "__main__":
    main()
