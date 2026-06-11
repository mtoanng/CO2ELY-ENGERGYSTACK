"""Bronze ingestion: Read raw Excel files from UC Volume → Delta table.

Migration from CO_energystacck Dash app to production Databricks pipeline.
Uses Polars (calamine engine) for Excel reads — same engine as the existing
Dash application — then converts to Spark DataFrame for Delta writes.

Usage (via Databricks job):
    spark_python_task:
        python_file: ../src/_1_ingest/ingest_excel_to_bronze.py
        parameters: ["--env", "dev", "--is_integration_test", "false"]
"""

import sys
from pathlib import Path

# Add src root to path for sibling package imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from _common.common_utils import (
    build_table_name,
    configure_logger,
    env_variables,
    get_job_args,
    layer_variables,
    medallion_variables,
)
from _common.io_utils import write_to_delta

logger = configure_logger("ingest_excel_to_bronze")


def read_excel_polars(file_path: str) -> pl.DataFrame:
    """Read a single Excel file using Polars calamine engine.

    This mirrors the CO_energystacck Dash app's read pattern:
    polars.read_excel(engine='calamine') for fast xlsx parsing.

    Args:
        file_path: Path to the .xlsx file (Volume or local).

    Returns:
        Polars DataFrame with inferred types.
    """
    return pl.read_excel(
        file_path,
        engine="calamine",
        infer_schema_length=10000,
    )


def ingest_volume_excels(spark: SparkSession, volume_path: str) -> "pyspark.sql.DataFrame":
    """Scan UC Volume for .xlsx files and ingest via Polars → Spark.

    Args:
        spark: Active SparkSession.
        volume_path: Volume path, e.g. '/Volumes/catalog/schema/volume_name'.

    Returns:
        Spark DataFrame with all rows + _source_file metadata column.
    """
    import os

    xlsx_files = [
        os.path.join(volume_path, f)
        for f in os.listdir(volume_path)
        if f.endswith(".xlsx") and not f.startswith("~$")
    ]

    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files found in {volume_path}")

    logger.info(f"Found {len(xlsx_files)} Excel file(s) to ingest")

    frames = []
    for fpath in xlsx_files:
        logger.info(f"  Reading: {os.path.basename(fpath)}")
        pl_df = read_excel_polars(fpath)
        # Tag with source filename for lineage
        pl_df = pl_df.with_columns(
            pl.lit(os.path.basename(fpath)).alias("_source_file")
        )
        frames.append(pl_df)

    # Concat all frames (align schemas)
    combined = pl.concat(frames, how="diagonal_relaxed")

    # Convert Polars → Pandas → Spark
    # (Polars Arrow export → createDataFrame is the fastest path)
    df_spark = spark.createDataFrame(combined.to_pandas())
    return df_spark


def main():
    """Main entry point for bronze ingestion."""
    args = get_job_args()
    spark = SparkSession.builder.getOrCreate()

    # Resolve environment
    env = env_variables(spark, env_override=args.env)
    layer = layer_variables("_1_ingest")
    write_medal = medallion_variables(layer["write_layer"])

    logger.info(f"Environment: {env['environment']}")
    logger.info(f"Integration test: {args.is_integration_test}")

    # Build source path (UC Volume containing raw Excel uploads)
    source_volume = (
        f"/Volumes/{env['unity_catalog']}/{write_medal['uc_schema']}/co2_raw_uploads"
    )

    # Build target table
    target_table = build_table_name(
        unity_catalog=env["unity_catalog"],
        schema=write_medal["uc_schema"],
        prefix=write_medal["table_prefix"],
        table="co2_timeseries",
        is_integration_test=args.is_integration_test,
    )

    logger.info(f"Reading from Volume: {source_volume}")
    logger.info(f"Writing to table: {target_table}")

    # Ingest Excel files via Polars (calamine) → Spark DataFrame
    df = ingest_volume_excels(spark, source_volume)

    # Add ingestion metadata
    df = df.withColumn("_ingestion_timestamp", current_timestamp())

    row_count = df.count()
    logger.info(f"Ingested {row_count} rows from Excel files")

    # Write to bronze Delta table
    write_to_delta(df, target_table, mode="overwrite")
    logger.info(f"Successfully wrote {row_count} rows to {target_table}")


if __name__ == "__main__":
    main()
