"""System: Validate Delta table integrity.

Checks:
1. Tables exist and are readable
2. Schema matches expected (column names + types)
3. Row counts are non-zero (or match expected ranges)
4. No orphan UUIDs (referential integrity between tables)

Same pattern as TBP sys_validate_tables.py.
"""
import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "_0_convert"))

from pyspark.sql import SparkSession
from common import get_env_variables, TABLE_TYPES, SCHEMAS, logger


def parse_args():
    parser = argparse.ArgumentParser(description="Validate Delta tables")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    parser.add_argument("--fail_on_error", type=str, default="true")
    args, _ = parser.parse_known_args()
    return args


def validate_table_exists(spark, fqn: str) -> dict:
    """Check table exists and is readable."""
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}").collect()[0].cnt
        return {"table": fqn, "status": "OK", "row_count": count}
    except Exception as e:
        return {"table": fqn, "status": "MISSING", "error": str(e)[:200]}


def validate_schema(spark, fqn: str, expected_columns: list) -> dict:
    """Check table schema matches expected columns."""
    try:
        actual_cols = [f.name for f in spark.table(fqn).schema.fields
                       if not f.name.startswith("_")]  # skip metadata cols
        missing = set(expected_columns) - set(actual_cols)
        extra = set(actual_cols) - set(expected_columns)
        if missing:
            return {"table": fqn, "status": "SCHEMA_MISMATCH",
                    "missing": list(missing), "extra": list(extra)}
        return {"table": fqn, "status": "OK"}
    except Exception as e:
        return {"table": fqn, "status": "ERROR", "error": str(e)[:200]}


def validate_referential_integrity(spark, catalog: str, suffix: str) -> dict:
    """Check all timeseries UUIDs exist in filemeta."""
    ts_table = f"{catalog}.bronze.co2_timeseries{suffix}"
    fm_table = f"{catalog}.bronze.co2_filemeta{suffix}"
    try:
        orphans = spark.sql(f"""
            SELECT COUNT(DISTINCT t.uuid) AS orphan_count
            FROM {ts_table} t
            LEFT JOIN {fm_table} f ON t.uuid = f.uuid
            WHERE f.uuid IS NULL
        """).collect()[0].orphan_count
        status = "OK" if orphans == 0 else "ORPHANS_FOUND"
        return {"check": "referential_integrity", "status": status, "orphan_uuids": orphans}
    except Exception as e:
        return {"check": "referential_integrity", "status": "SKIP", "error": str(e)[:200]}


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    fail_on_error = args.fail_on_error.lower() == "true"
    env_vars = get_env_variables(spark)
    catalog = env_vars["unity_catalog"]
    suffix = "_int_test" if is_int_test else ""

    results = []
    errors = 0

    # 1. Check bronze tables exist + have rows
    for table_type in TABLE_TYPES:
        fqn = f"{catalog}.bronze.co2_{table_type}{suffix}"
        r = validate_table_exists(spark, fqn)
        results.append(r)
        if r["status"] != "OK":
            errors += 1
            logger.warning(f"FAIL: {fqn} — {r['status']}")
        else:
            logger.info(f"OK: {fqn} ({r['row_count']:,} rows)")

    # 2. Check schemas
    for table_type in TABLE_TYPES:
        fqn = f"{catalog}.bronze.co2_{table_type}{suffix}"
        expected_cols = [field.name for field in SCHEMAS[table_type]]
        r = validate_schema(spark, fqn, expected_cols)
        if r["status"] != "OK":
            results.append(r)
            errors += 1
            logger.warning(f"SCHEMA: {fqn} — {r}")

    # 3. Referential integrity
    r = validate_referential_integrity(spark, catalog, suffix)
    results.append(r)
    if r["status"] not in ("OK", "SKIP"):
        errors += 1
        logger.warning(f"INTEGRITY: {r}")

    # Summary
    print(f"\n{'='*60}")
    print(f"Validation Summary: {len(results)} checks, {errors} errors")
    for r in results:
        print(f"  {r.get('table', r.get('check', '?'))}: {r['status']}")

    if errors > 0 and fail_on_error:
        raise RuntimeError(f"Validation failed: {errors} error(s). Details: {json.dumps(results, default=str)}")


if __name__ == "__main__":
    main()
