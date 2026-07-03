"""Silver dimension: Signal identity + canonical channel mapping.

Assigns a deterministic `signal_id` (BIGINT) and enriches each channel with
the canonical `std_channel` name from the mapping JSON config.  This is the
single source of truth for channel display metadata.

Key design decisions:
  - signal_id = xxhash64(uuid, group, channel_id) → deterministic, stable.
  - experiment_id = xxhash64(uuid, group) → FK to silver_dim_experiment.
  - std_channel = COALESCE(mapped_std_channel, raw_channel) — falls back to
    the raw name when no mapping entry exists.
  - Includes a DQ uniqueness check: if the same (experiment_id, std_channel)
    maps to multiple channel_ids within an experiment, the pipeline fails with
    a detailed diagnostic to fix the mapping config.
  - Incremental append: only new signal_id values are written each run.
  - Source: bronze_channel + silver_dim_experiment + mapping JSON.

Output table: silver_dim_signal
    signal_id       BIGINT   -- xxhash64(uuid, group, channel_id)
    experiment_id   BIGINT   -- FK to dim_experiment
    uuid            STRING   -- natural key (needed for bronze join at Gold)
    group           STRING   -- natural key (needed for bronze join at Gold)
    channel_id      STRING   -- original header identifier (lineage key)
    raw_channel     STRING   -- source display name from xlsx
    std_channel     STRING   -- canonical mapped name
    unit            STRING   -- measurement unit
    series          STRING   -- denormalized for convenience
    column_index    INT      -- original Excel column position

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_b2s/silver_dim_signal.py
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
from _2_b2s.silver_channel_mapping import build_channel_mapping_df

logger = configure_logger("silver_dim_signal")


def _existing_signal_ids(spark: SparkSession, table: str) -> DataFrame:
    """Return existing signal_id values to avoid re-inserting."""
    if not spark.catalog.tableExists(table):
        return spark.createDataFrame([], "signal_id long")
    return spark.read.table(table).select("signal_id")


def build_dim_signal(
    channel_df: DataFrame,
    experiment_df: DataFrame,
    mapping_df: DataFrame,
) -> DataFrame:
    """Build the signal dimension with canonical std_channel mapping.

    Args:
        channel_df: bronze_channel (uuid, group, channel_id, raw_channel, unit, column_index)
        experiment_df: silver_dim_experiment (experiment_id, uuid, group, series)
        mapping_df: channel mapping (series, raw_channel, mapped_std_channel)

    Returns:
        DataFrame with schema:
            signal_id, experiment_id, uuid, group, channel_id, raw_channel,
            std_channel, unit, series, column_index
    """
    # Get experiment_id + series from dim_experiment
    exp_lookup = experiment_df.select("experiment_id", "uuid", "group", "series")

    # Join channel with experiment to get experiment_id and series
    enriched = (
        channel_df
        .select("uuid", "group", "channel_id", "raw_channel", "unit", "column_index")
        .join(exp_lookup, on=["uuid", "group"], how="inner")
    )

    # Join mapping to get std_channel (left join — not all channels have mappings)
    # mapping_df.raw_channel holds file_column values, which are channel_id identifiers
    # (Excel ROW 1), not raw_channel display names (ROW 2). Rename before joining.
    enriched = (
        enriched
        .join(
            F.broadcast(mapping_df.withColumnRenamed("raw_channel", "channel_id")),
            on=["series", "channel_id"],
            how="left",
        )
        .withColumn(
            "std_channel",
            F.coalesce(F.col("mapped_std_channel"), F.col("raw_channel")),
        )
        .drop("mapped_std_channel")
    )

    # Assign signal_id
    enriched = enriched.withColumn(
        "signal_id",
        F.xxhash64(F.col("uuid"), F.col("group"), F.col("channel_id")),
    )

    return enriched.select(
        "signal_id",
        "experiment_id",
        "uuid",
        "group",
        "channel_id",
        "raw_channel",
        "std_channel",
        "unit",
        "series",
        "column_index",
    )


def check_std_channel_uniqueness(dim_signal: DataFrame) -> None:
    """Fail if the same std_channel maps to multiple channel_ids per experiment.

    This guards against ambiguous Gold facts where (experiment_id, std_channel)
    cannot uniquely identify a physical signal.
    """
    duplicates = (
        dim_signal
        .groupBy("experiment_id", "std_channel")
        .agg(
            F.count("*").alias("signal_count"),
            F.collect_set("channel_id").alias("channel_ids"),
            F.collect_set("raw_channel").alias("raw_channels"),
        )
        .filter(F.col("signal_count") > 1)
    )

    dup_count = duplicates.count()
    if dup_count > 0:
        # Collect up to 10 conflicts for the error message
        conflicts = duplicates.limit(10).collect()
        msg_lines = [
            f"  experiment_id={row.experiment_id}, std_channel='{row.std_channel}', "
            f"count={row.signal_count}, channel_ids={row.channel_ids}"
            for row in conflicts
        ]
        detail = "\n".join(msg_lines)
        raise ValueError(
            f"std_channel uniqueness violation: {dup_count} conflict(s) detected.\n"
            f"Fix channel mapping JSON or investigate duplicate headers.\n"
            f"Conflicts:\n{detail}"
        )
    logger.info("  DQ check passed: std_channel unique per experiment.")


def main():
    """Main entry point for silver_dim_signal."""
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
    logger.info("Silver Dimension: Signal Identity + Channel Mapping")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Catalog.Schema: {catalog}.{silver_medal['uc_schema']}")
    logger.info(f"  Integration test: {args.is_integration_test}")
    logger.info(f"{'='*60}")

    # Source tables
    bronze_ch_table = build_table_name(
        catalog, bronze_medal["uc_schema"], bronze_medal["table_prefix"],
        "channel", args.is_integration_test,
    )
    dim_experiment_table = build_table_name(
        catalog, silver_medal["uc_schema"], silver_medal["table_prefix"],
        "dim_experiment", args.is_integration_test,
    )

    # Target table
    target_table = build_table_name(
        catalog, silver_medal["uc_schema"], silver_medal["table_prefix"],
        "dim_signal", args.is_integration_test,
    )

    silver_layer = "silver_int_test" if args.is_integration_test else silver_medal["table_prefix"]
    location = build_external_table_location(
        storage_account=storage_account,
        container=silver_medal["adls_container"],
        layer=silver_layer,
        table_name="dim_signal",
    )

    # Load sources
    channel_df = spark.read.table(bronze_ch_table)
    experiment_df = spark.read.table(dim_experiment_table)
    mapping_df = build_channel_mapping_df(spark)

    # Build full dimension
    all_signals = build_dim_signal(channel_df, experiment_df, mapping_df)

    # DQ uniqueness check — fail early if mapping produces ambiguity
    check_std_channel_uniqueness(all_signals)

    # Incremental: only append new signal_id values
    existing_ids = _existing_signal_ids(spark, target_table)
    new_signals = all_signals.join(existing_ids, on="signal_id", how="left_anti")

    new_count = new_signals.count()
    logger.info(f"  New signals to register: {new_count}")

    if new_count == 0:
        logger.info("  Nothing new. Exiting.")
        return

    write_to_delta(new_signals, target_table, mode="append", location=location)
    logger.info(f"  Written {new_count} row(s) -> {target_table}")


if __name__ == "__main__":
    main()
