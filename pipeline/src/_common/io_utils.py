"""I/O utilities for reading/writing Delta tables and ADLS paths.

Migrated from CO_energystacck app patterns:
- Polars (calamine) for Excel reads (replaces com.crealytics.spark.excel)
- Delta table writes with overwrite/append semantics
- ADLS abfss:// path generation
"""

import os
from typing import Optional

import polars as pl
from pyspark.sql import DataFrame, SparkSession


def read_excel_polars(
    file_path: str,
    sheet_name: str | None = None,
    infer_schema_length: int = 10000,
) -> pl.DataFrame:
    """Read an Excel file using Polars calamine engine.

    This is the same engine used in the CO_energystacck Dash app.
    Calamine is significantly faster than openpyxl and doesn't need JARs.

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


def write_to_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
) -> None:
    """Write a DataFrame to a Delta table in Unity Catalog.

    Args:
        df: Spark DataFrame to write.
        table_name: Fully qualified table name (catalog.schema.table).
        mode: Write mode ('overwrite' or 'append'). Default 'overwrite'.
        partition_by: Optional partition columns.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)


def build_abfss_path(adls_domain: str, container: str, path: str) -> str:
    """Build an abfss:// URI for ADLS Gen2.

    Args:
        adls_domain: ADLS domain (e.g. 'stpsbdodxdevdatalake.dfs.core.windows.net').
        container: Blob container name.
        path: Relative path within container.

    Returns:
        Full abfss:// URI string.
    """
    return f"abfss://{container}@{adls_domain}/{path.lstrip('/')}"


def polars_to_spark(spark: SparkSession, pl_df: pl.DataFrame) -> DataFrame:
    """Convert Polars DataFrame to Spark DataFrame via Arrow.

    Uses pandas as intermediate (Polars → pandas → Spark).
    This is the fastest serialization path available.

    Args:
        spark: Active SparkSession.
        pl_df: Polars DataFrame to convert.

    Returns:
        PySpark DataFrame.
    """
    return spark.createDataFrame(pl_df.to_pandas())
