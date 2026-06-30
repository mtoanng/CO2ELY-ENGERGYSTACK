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

from pyspark.sql import SparkSession, functions as F

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
    """Mark silver fact rows as valid/invalid based on DQ failures."""
    invalid_count = invalid_keys.count()

    logger.info(f"Invalid row keys identified for annotation: {invalid_count}")
    spark.sql(f"UPDATE {source_table} SET is_valid = true")

    if invalid_count == 0:
        return invalid_count

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
    return invalid_count


def delete_invalid_rows(spark, source_table: str, invalid_keys):
    """Delete invalid fact rows from the source table in place."""
    invalid_count = invalid_keys.count()
    logger.info(f"Invalid row keys identified for deletion: {invalid_count}")

    if invalid_count == 0:
        return invalid_count

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
    return invalid_count


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

    logger.info(f"Reading from: {source_table}")
    df = spark.read.table(source_table)
    failures = evaluate_rules(df, DEFAULT_TIMESERIES_RULES).withColumn(
        "observed_value",
        F.coalesce(F.col("value_str"), F.col("value").cast("string")),
    )
    summary = summarize_failures(failures)
    invalid_keys = _select_invalid_keys(failures, args.apply_to_blocking_only)
    logger.info(f"DQ rules evaluated: {len(DEFAULT_TIMESERIES_RULES)}")

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

    output_tables = [
        (result_table, failures, "fact_timeseries_dq_result"),
        (summary_table, summary, "fact_timeseries_dq_summary"),
        (rule_table, rules_df, "dim_dq_rule"),
    ]

    if args.cleanup_mode == "write_clean_table":
        output_tables.append(
            (clean_table, build_clean_timeseries_df(df, invalid_keys), "fact_timeseries_clean")
        )
    elif args.cleanup_mode not in {"annotate", "delete_invalid"}:
        raise ValueError(
            "Unsupported cleanup_mode. Expected one of: annotate, delete_invalid, write_clean_table"
        )

    for table_name, table_df, entity_name in output_tables:
        logger.info(f"Writing to: {table_name}")
        location = build_external_table_location(
            storage_account=storage_account,
            container=medal["adls_container"],
            layer=medal["table_prefix"],
            table_name=entity_name,
        )
        write_to_delta(table_df, table_name, mode="overwrite", location=location)
        logger.info(f"Wrote {entity_name} -> {table_name}")

    if args.cleanup_mode == "annotate":
        invalid_count = apply_is_valid_flags(spark, source_table, invalid_keys)
        logger.info(f"Updated is_valid flags in: {source_table} (invalid rows: {invalid_count})")
    elif args.cleanup_mode == "delete_invalid":
        invalid_count = delete_invalid_rows(spark, source_table, invalid_keys)
        logger.info(f"Deleted invalid rows from: {source_table} (deleted rows: {invalid_count})")
    else:
        invalid_count = invalid_keys.count()
        logger.info(f"Wrote clean-table output with invalid rows excluded: {clean_table} (invalid rows: {invalid_count})")


if __name__ == "__main__":
    main()
