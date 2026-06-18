"""Polars-specific utilities for the enrichment layer.

These functions require polars to be installed (enrichment/gold jobs only).
Do NOT import this module from jobs that don't have polars
(e.g. bronze ingestion).

Functions:
    polars_to_spark    - Convert Polars DataFrame to Spark DataFrame via Arrow
    read_excel_polars  - Read Excel file using Polars calamine engine
"""

import polars as pl
from pyspark.sql import DataFrame, SparkSession


def polars_to_spark(spark: SparkSession, pl_df: pl.DataFrame) -> DataFrame:
    """Convert Polars DataFrame to Spark DataFrame via Arrow.

    Uses pandas as intermediate (Polars -> pandas -> Spark).
    This is the fastest serialization path available.

    Args:
        spark: Active SparkSession.
        pl_df: Polars DataFrame to convert.

    Returns:
        PySpark DataFrame.
    """
    return spark.createDataFrame(pl_df.to_pandas())


def read_excel_polars(
    file_path: str,
    sheet_name: str | None = None,
    infer_schema_length: int = 10000,
) -> pl.DataFrame:
    """Read an Excel file using Polars calamine engine.

    Calamine is significantly faster than openpyxl and doesn't need JARs.
    Same engine used in the CO_energystacck Dash app.

    Args:
        file_path: Path to the .xlsx file.
        sheet_name: Optional sheet name. None = first sheet.
        infer_schema_length: Rows to scan for type inference.

    Returns:
        Polars DataFrame.
    """
    kwargs = {
        "source": file_path,
        "engine": "calamine",
        "infer_schema_length": infer_schema_length,
    }
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
    return pl.read_excel(**kwargs)
