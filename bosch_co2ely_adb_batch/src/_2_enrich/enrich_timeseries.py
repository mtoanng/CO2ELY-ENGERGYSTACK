"""Silver enrichment: Apply Polars engine to compute derived metrics.

Reads bronze Delta table, applies the 12-step enrichment pipeline
from the CO_energystacck app (Energy Efficiency, Current Density,
flow rates, SPCE, etc.), writes enriched data to silver Delta table.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_2_enrich/enrich_timeseries.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from pyspark.sql import SparkSession

from _common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _common.io_utils import polars_to_spark, write_to_delta

logger = configure_logger("enrich_timeseries")

# Active area for current density calculation (cm²)
ACTIVE_AREA = 88.0

# Units mapping for plausibility clipping (from CO_energystacck app)
UNITS = {
    "Faradaic Efficiency of CO": "%",
    "Faradaic Efficiency of H2": "%",
    "Faradaic Efficiency of O2": "%",
    "Faradaic Efficiency of CO and H2": "%",
    "Energy Efficiency": "%",
    "Single Pass Conversion Efficiency": "%",
    "CO2:O2 ratio in anode product gas": "%",
}


def enrich_dataframe(pl_df: pl.DataFrame, active_area: float) -> pl.DataFrame:
    """Apply the 12-step enrichment pipeline (migrated from CO_energystacck).

    Computed columns:
        1. Energy Efficiency (%)
        2. Δp Anolyte (bar)
        3. Current density (mA/cm²)
        4. Faradaic Efficiency of CO and H2 (%)
        5. Flow CO out (nL/min)
        6. Flow H2 out (nL/min)
        7. Flow O2 out (nL/min)
        8. Flow CO2 out, total (nL/min)
        9. Flow CO2 out, anode (nL/min)
        10. Flow CO2 out, cathode (nL/min)
        11. CO/H2 ratio recalculated
        12. Single Pass Conversion Efficiency (%)

    Args:
        pl_df: Bronze Polars DataFrame.
        active_area: Cell active area in cm².

    Returns:
        Enriched Polars DataFrame with derived columns.
    """
    # TODO: Import the actual enrichment functions from the CO_energystacck
    # polars_engine module once packaged as a wheel.
    #
    # For now, this is a placeholder showing the structure.
    # Replace with:
    #   from co_energystack.backend.polars_engine import clean_data, enrich
    #   pl_df = clean_data(pl_df)
    #   pl_df = enrich(pl_df, active_area=active_area, units=UNITS)

    # Current density (mA/cm²) = Current (A) * 1000 / active_area
    if "Current" in pl_df.columns:
        pl_df = pl_df.with_columns(
            (pl.col("Current") * 1000.0 / active_area).alias("Current density")
        )

    logger.info(f"Enrichment applied: {pl_df.shape[0]} rows × {pl_df.shape[1]} cols")
    return pl_df


def main():
    """Main entry point for silver enrichment."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve environment
    env = env_variables(spark, env_override=args.env)
    read_layer = layer_variables("_2_enrich")
    read_medal = medallion_variables(read_layer["read_layer"])
    write_medal = medallion_variables(read_layer["write_layer"])

    logger.info(f"Environment: {env['environment']}")

    # Source: bronze table
    source_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=read_medal["uc_schema"],
        prefix=read_medal["table_prefix"],
        table="co2_timeseries",
        is_integration_test=args.is_integration_test,
    )

    # Target: silver enriched table
    target_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_timeseries_enriched",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Reading from: {source_table}")
    logger.info(f"Writing to: {target_table}")

    # Read bronze data → Polars for enrichment
    df_spark = spark.read.table(source_table)

    # Drop internal metadata columns for enrichment
    enrich_cols = [c for c in df_spark.columns if not c.startswith("_")]
    df_pd = df_spark.select(enrich_cols).toPandas()
    pl_df = pl.from_pandas(df_pd)

    # Apply enrichment
    pl_df = enrich_dataframe(pl_df, active_area=ACTIVE_AREA)

    # Convert back to Spark and write
    df_enriched = polars_to_spark(spark, pl_df)
    write_to_delta(df_enriched, target_table, mode="overwrite")
    logger.info(f"Successfully wrote enriched data to {target_table}")


if __name__ == "__main__":
    main()
