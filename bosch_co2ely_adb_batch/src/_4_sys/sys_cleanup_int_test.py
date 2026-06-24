"""System: List integration test tables for manual cleanup.

Lists all _int_test suffixed tables created during integration testing.
Operator uses output to manually clean via SQL editor.

Usage:
    spark_python_task:
        python_file: ../src/_4_sys/sys_cleanup_int_test.py
        parameters: ["--env", "dev"]
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "_0_convert"))

from pyspark.sql import SparkSession
from converter_utils import get_env_variables, TABLE_TYPES, logger


INT_TEST_TABLES = [
    "converter.tmp_int_test_file_tracking",
    *[f"bronze.co2_{tt}_int_test" for tt in TABLE_TYPES],
    "bronze.bronze_ingest_tracking_int_test",
]


def parse_args():
    parser = argparse.ArgumentParser(description="List integration test artifacts")
    parser.add_argument("--env", type=str, default="dev_user")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    env_vars = get_env_variables(spark)
    catalog = env_vars["unity_catalog"]

    logger.info(f"Catalog: {catalog}")
    logger.info(f"Checking {len(INT_TEST_TABLES)} integration test table(s):")

    existing = []
    for table_suffix in INT_TEST_TABLES:
        fqn = f"{catalog}.{table_suffix}"
        try:
            exists = spark.catalog.tableExists(fqn)
            status = "EXISTS" if exists else "not found"
            logger.info(f"  [{status}] {fqn}")
            if exists:
                existing.append(fqn)
        except Exception as e:
            logger.info(f"  [error] {fqn}: {e}")

    print(f"\n{'='*60}")
    print(f"Found {len(existing)} integration test table(s).")
    if existing:
        print("To clean up, run in SQL editor:")
        for fqn in existing:
            print(f"  -- {fqn}")


if __name__ == "__main__":
    main()
