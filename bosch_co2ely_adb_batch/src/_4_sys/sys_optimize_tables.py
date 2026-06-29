"""System: OPTIMIZE Delta tables.

Runs OPTIMIZE (bin-packing) on all co2ely Delta tables.
Scheduled weekly or on-demand.

"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "_0_convert"))

from pyspark.sql import SparkSession
from converter_utils import get_env_variables, TABLE_TYPES, logger


BRONZE_TABLES = [f"co2_{tt}" for tt in TABLE_TYPES]
SYSTEM_TABLES = ["file_tracking", "bronze_ingest_tracking"]


def parse_args():
    parser = argparse.ArgumentParser(description="Optimize Delta tables")
    parser.add_argument("--is_integration_test", type=str, default="false")
    parser.add_argument("--env", type=str, default="dev_user")
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    is_int_test = args.is_integration_test.lower() == "true"
    env_vars = get_env_variables(spark)
    catalog = env_vars["unity_catalog"]

    suffix = "_int_test" if is_int_test else ""
    optimized = 0

    # Converter tracking tables
    for table in SYSTEM_TABLES:
        fqn = f"{catalog}.converter.{table}{suffix}"
        try:
            spark.sql(f"OPTIMIZE {fqn}")
            logger.info(f"Optimized: {fqn}")
            optimized += 1
        except Exception as e:
            logger.warning(f"Skip {fqn}: {e}")

    # Bronze tables
    for table in BRONZE_TABLES:
        fqn = f"{catalog}.bronze.{table}{suffix}"
        try:
            spark.sql(f"OPTIMIZE {fqn}")
            logger.info(f"Optimized: {fqn}")
            optimized += 1
        except Exception as e:
            logger.warning(f"Skip {fqn}: {e}")

    logger.info(f"Table optimization complete. {optimized} tables optimized.")


if __name__ == "__main__":
    main()
