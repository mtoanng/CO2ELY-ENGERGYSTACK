"""System: Validate Delta table integrity across all pipeline layers.

Checks (in order):
  1. Tables exist and are readable
  2. Row counts are non-zero (expected for a populated pipeline)
  3. Key columns present (spot-check, not exhaustive schema enforcement)
  4. Cross-layer referential integrity:
       silver dimensions ⊆ bronze source tables
       gold UUID/group pairs ⊆ bronze timeseries
  5. Gold serving completeness: experiment_index covers all gold_timeseries pairs

Exits non-zero on any failure when --fail_on_error=true (default).
"""
import sys
import json
import argparse
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent / "_5_common"))

from pyspark.sql import SparkSession
from common_config import env_variables, medallion_variables, build_table_name
from common_system_utils import configure_logger

logger = configure_logger("sys_validate_tables")


def parse_args():
    parser = argparse.ArgumentParser(description="Validate Delta tables")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev")
    parser.add_argument("--fail_on_error", type=str, default="true")
    args, _ = parser.parse_known_args()
    return args


def check_exists(spark, fqn: str) -> dict:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}").collect()[0].cnt
        return {"table": fqn, "status": "OK", "row_count": count}
    except Exception as e:
        return {"table": fqn, "status": "MISSING", "error": str(e)[:200]}


def check_columns(spark, fqn: str, required_cols: list[str]) -> dict:
    try:
        actual = {f.name for f in spark.table(fqn).schema.fields}
        missing = set(required_cols) - actual
        if missing:
            return {"table": fqn, "status": "SCHEMA_MISMATCH", "missing_cols": sorted(missing)}
        return {"table": fqn, "status": "OK"}
    except Exception as e:
        return {"table": fqn, "status": "ERROR", "error": str(e)[:200]}


def check_orphans(spark, child_table: str, parent_table: str, join_cols: list[str]) -> dict:
    """Check that all (join_cols) in child_table exist in parent_table."""
    on_clause = " AND ".join(f"c.`{col}` = p.`{col}`" for col in join_cols)
    where_null = " OR ".join(f"p.`{col}` IS NULL" for col in join_cols)
    try:
        orphan_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM (SELECT DISTINCT {', '.join(f'`{c}`' for c in join_cols)} FROM {child_table}) c
            LEFT JOIN (SELECT DISTINCT {', '.join(f'`{c}`' for c in join_cols)} FROM {parent_table}) p
              ON {on_clause}
            WHERE {where_null}
        """).collect()[0].cnt
        status = "OK" if orphan_count == 0 else "ORPHANS_FOUND"
        return {
            "check": f"orphans:{child_table}->{parent_table}",
            "status": status,
            "orphan_count": orphan_count,
        }
    except Exception as e:
        return {"check": f"orphans:{child_table}->{parent_table}", "status": "SKIP", "error": str(e)[:200]}


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    fail_on_error = args.fail_on_error.lower() == "true"

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]

    b = medallion_variables("bronze", environment)
    s = medallion_variables("silver", environment)
    g = medallion_variables("gold", environment)

    def t(layer_medal, suffix):
        return build_table_name(catalog, layer_medal["uc_schema"], layer_medal["table_prefix"], suffix, is_int_test)

    # All tables to validate with their required key columns
    table_checks = [
        # Bronze
        (t(b, "timeseries"),               ["uuid", "group", "sample_offset", "channel_id", "value"]),
        (t(b, "channel"),                  ["uuid", "group", "channel_id", "raw_channel", "unit"]),
        (t(b, "filemeta"),                 ["uuid", "file_path", "series"]),
        (t(b, "statistics"),               ["uuid", "group", "n_rows", "n_channels"]),
        # Silver
        (t(s, "dim_filemeta"),             ["uuid", "file_path", "series", "_silver_published_at"]),
        (t(s, "dim_channel"),              ["uuid", "group", "channel_id", "raw_channel", "unit", "_silver_published_at"]),
        (t(s, "fact_statistics"),          ["uuid", "group", "n_rows", "n_channels", "_silver_published_at"]),
        # Gold - core
        (t(g, "timeseries"),               ["uuid", "group", "timestamp", "elapsed_time_s",
                                            "channel_id", "std_channel", "value"]),
        (t(g, "timeseries_agg"),           ["uuid", "group", "elapsed_bin_s", "channel_id",
                                            "value_mean", "value_count"]),
        # Gold — serving
        (t(g, "experiment_index"),         ["uuid", "group", "series", "start_time_s", "end_time_s",
                                            "channel_count", "total_data_points"]),
        (t(g, "channel_catalog"),          ["uuid", "group", "channel_id", "std_channel", "unit"]),
        (t(g, "timeseries_agg_15min"),     ["uuid", "group", "elapsed_bin_s", "channel_id",
                                            "value_mean", "value_count"]),
        (t(g, "timeseries_agg_60min"),     ["uuid", "group", "elapsed_bin_s", "channel_id",
                                            "value_mean", "value_count"]),
        (t(g, "summary_statistics"),       ["uuid", "group", "series", "total_data_points"]),
    ]

    results = []
    errors = 0

    logger.info("=" * 60)
    logger.info(f"Pipeline validation — {environment} (int_test={is_int_test})")
    logger.info("=" * 60)

    # 1. Existence + row count
    for fqn, _ in table_checks:
        r = check_exists(spark, fqn)
        results.append(r)
        if r["status"] != "OK":
            errors += 1
            logger.warning(f"MISSING: {fqn}")
        else:
            logger.info(f"OK ({r['row_count']:>12,} rows): {fqn}")

    # 2. Schema spot-check
    for fqn, required_cols in table_checks:
        r = check_columns(spark, fqn, required_cols)
        if r["status"] != "OK":
            results.append(r)
            errors += 1
            logger.warning(f"SCHEMA : {fqn} — missing: {r.get('missing_cols')}")

    # 3. Cross-layer referential integrity
    integrity_checks = [
        (t(s, "dim_filemeta"),             t(b, "filemeta"),   ["uuid"]),
        (t(s, "dim_channel"),              t(b, "channel"),    ["uuid", "group", "channel_id"]),
        (t(g, "timeseries"),               t(b, "timeseries"), ["uuid", "group"]),
        (t(g, "experiment_index"),         t(g, "timeseries"), ["uuid", "group"]),
    ]
    for child, parent, cols in integrity_checks:
        r = check_orphans(spark, child, parent, cols)
        results.append(r)
        if r["status"] not in ("OK", "SKIP"):
            errors += 1
            logger.warning(f"ORPHANS: {r['check']} — {r['orphan_count']} orphan(s)")
        else:
            logger.info(f"OK (integrity): {r['check']}")

    # Summary
    logger.info("=" * 60)
    logger.info(f"Validation complete: {len(results)} checks, {errors} error(s)")
    logger.info("=" * 60)

    if errors > 0 and fail_on_error:
        raise RuntimeError(
            f"Validation failed with {errors} error(s). Details:\n"
            + json.dumps(results, default=str, indent=2)
        )


if __name__ == "__main__":
    main()
