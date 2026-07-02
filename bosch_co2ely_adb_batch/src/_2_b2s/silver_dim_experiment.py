"""Silver dimension: Experiment identity table.

Assigns a deterministic `experiment_id` (BIGINT) to each unique (uuid, group)
pair across the dataset.  This is the canonical experiment identity used by all
downstream Gold fact tables.

Key design decisions:
  - experiment_id = xxhash64(uuid, group) → deterministic, stateless, idempotent.
    No sequential counter coordination, no merge-race risk.
  - Incremental append: only new (uuid, group) pairs are written each run.
  - Source: bronze_filemeta (uuid, series) + bronze_statistics (uuid, group).
  - Only experiments with a known series (from filemeta) are registered.
    Orphan statistics without filemeta are logged as warnings.

Output table: silver_dim_experiment
    experiment_id  BIGINT   -- xxhash64(uuid, group)
    uuid           STRING   -- file identifier
    group          STRING   -- sheet name within file
    series         STRING   -- from filemeta (folder path classification)

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/silver_dim_experiment.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent
sys.path.insert(0, str(_THIS_DIR.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
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

logger = configure_logger("silver_dim_experiment")


def _existing_experiment_ids(spark: SparkSession, table: str) -> DataFrame:
    """Return existing experiment_id values to avoid re-inserting."""
    try:
        return spark.read.table(table).select("experiment_id")
    except AnalysisException:
        return spark.createDataFrame([], "experiment_id long")


def build_dim_experiment(
    filemeta_df: DataFrame, statistics_df: DataFrame,
) -> DataFrame:
    """Build the experiment dimension from bronze sources.

    Joins bronze_statistics (uuid, group) with bronze_filemeta (uuid, series)
    and assigns experiment_id = xxhash64(uuid, group).

    Uses INNER join: only sheets whose UUID exists in filemeta (i.e., has a
    known series) produce an experiment. Orphan sheets without filemeta are
    skipped — this prevents NULL series from propagating downstream.
    """
    # statistics has (uuid, group) — one row per sheet
    sheets = statistics_df.select("uuid", "group").distinct()

    # filemeta has (uuid, series) — one row per file
    series_lookup = filemeta_df.select("uuid", "series").distinct()

    return (
        sheets
        .join(series_lookup, on="uuid", how="inner")
        .withColumn("experiment_id", F.xxhash64(F.col("uuid"), F.col("group")))
        .select("experiment_id", "uuid", "group", "series")
    )


def main():
    """Main entry point for silver_dim_experiment."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    env = env_variables(spark, env_override=args.env)
    environment = env["environment"]
    catalog = env["unity_catalog"]
    layers = layer_variables("_2_b2s")
    bronze_medal = medallion_variables(layers["read_layer"], environment)
    silver_medal = medallion_variables(layers["write_layer"], environment)
    adls_domain = env.get("adls_domain") or ""
    storage_account = adls_domain.replace(".dfs.core.windows.net", "")

    logger.info(f"{'='*60}")
    logger.info("Silver Dimension: Experiment Identity")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Catalog.Schema: {catalog}.{silver_medal['uc_schema']}")
    logger.info(f"  Integration test: {args.is_integration_test}")
    logger.info(f"{'='*60}")

    # Source tables
    bronze_fm_table = build_table_name(
        catalog, bronze_medal["uc_schema"], bronze_medal["table_prefix"],
        "filemeta", args.is_integration_test,
    )
    bronze_stats_table = build_table_name(
        catalog, bronze_medal["uc_schema"], bronze_medal["table_prefix"],
        "statistics", args.is_integration_test,
    )

    # Target table
    target_table = build_table_name(
        catalog, silver_medal["uc_schema"], silver_medal["table_prefix"],
        "dim_experiment", args.is_integration_test,
    )

    silver_layer = "silver_int_test" if args.is_integration_test else silver_medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=silver_medal["adls_container"],
        layer=silver_layer,
        table_name="dim_experiment",
    )

    # Build full dimension from bronze
    filemeta_df = spark.read.table(bronze_fm_table)
    statistics_df = spark.read.table(bronze_stats_table)
    all_experiments = build_dim_experiment(filemeta_df, statistics_df)

    # DQ check: detect orphan sheets without filemeta (would have been NULL series)
    orphan_sheets = (
        statistics_df.select("uuid", "group").distinct()
        .join(filemeta_df.select("uuid").distinct(), on="uuid", how="left_anti")
    )
    orphan_count = orphan_sheets.count()
    if orphan_count > 0:
        logger.warning(
            f"  {orphan_count} sheet(s) in bronze_statistics have no matching filemeta "
            "(skipped — series unknown). Investigate bronze ingestion."
        )

    # Incremental: only append new experiment_id values
    existing_ids = _existing_experiment_ids(spark, target_table)
    new_experiments = all_experiments.join(existing_ids, on="experiment_id", how="left_anti")

    new_count = new_experiments.count()
    logger.info(f"  New experiments to register: {new_count}")

    if new_count == 0:
        logger.info("  Nothing new. Exiting.")
        return

    write_to_delta(new_experiments, target_table, mode="append", location=location)
    logger.info(f"  Written {new_count} row(s) -> {target_table}")


if __name__ == "__main__":
    main()
