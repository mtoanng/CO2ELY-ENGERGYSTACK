"""I/O and ADLS utilities for the CO2 ELY pipeline.

Utilities categories:

  ADLS path builders:
    build_abfss_path              - Build abfss:// URI from components
    build_external_table_location - Build ABFSS location for external Delta table

  Delta write operations (Spark):
    write_to_delta                - Write Spark DataFrame to Delta (managed or external)

  Polars conversion utilities:
    polars_to_spark               - Convert Polars DataFrame to Spark DataFrame
    read_excel_polars             - Read Excel file via Polars calamine engine
"""

# Polars-dependent functions (only importable when polars is installed)
try:
    from _2_b2s.enrich_utils import polars_to_spark, read_excel_polars
except ImportError:
    # polars not installed (e.g. bronze job) - provide stub that raises
    def polars_to_spark(*args, **kwargs):
        raise ImportError("polars_to_spark requires polars. Import from _2_b2s.enrich_utils instead.")

    def read_excel_polars(*args, **kwargs):
        raise ImportError("read_excel_polars requires polars. Import from _2_b2s.enrich_utils instead.")


# =============================================================================
# ADLS PATH BUILDERS
# =============================================================================

def build_abfss_path(storage_account: str, container: str, path: str) -> str:
    """Build abfss:// URI for ADLS access via UC External Location.

    Args:
        storage_account: e.g. "stpsbdodxdev2datalake"
        container: e.g. "co2elyd-data"
        path: blob path within container

    Returns:
        Full abfss:// URI.
    """
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"


def build_external_table_location(
    storage_account: str, container: str, layer: str, table_name: str
) -> str:
    """Build ABFSS location for an external Delta table.

    External tables store data in ADLS instead of UC managed locations.
    Pattern: abfss://container@storage/layer/table_name/

    Args:
        storage_account: e.g. "stpsbdodxdev2datalake"
        container: e.g. "co2elyd-data"
        layer: Medallion layer (e.g. "bronze", "silver", "gold")
        table_name: Base table name (e.g. "timeseries", "filemeta")

    Returns:
        Full ABFSS location URI.

    Example:
        >>> build_external_table_location(
        ...     "stpsbdodxdev2datalake",
        ...     "co2elyd-data",
        ...     "bronze",
        ...     "timeseries"
        ... )
        'abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/bronze/timeseries/'
    """
    return build_abfss_path(storage_account, container, f"{layer}/{table_name}/")


# =============================================================================
# DELTA WRITE OPERATIONS
# =============================================================================

def write_to_delta(df, table_name, mode="append", partition_by=None, location=None):
    """Write DataFrame to Delta table (managed or external).

    Args:
        df: Spark DataFrame to write
        table_name: Fully qualified table name (catalog.schema.table)
        mode: Write mode ("append", "overwrite", "ignore", "error")
        partition_by: Optional list of columns to partition by
        location: Optional ABFSS location for external table.
                 If provided, creates external table in ADLS; otherwise creates
                 managed table in UC.

    Examples:
        # Managed table (default)
        write_to_delta(df, "catalog.schema.table", mode="overwrite")

        # External table — build location with helper then pass it
        location = build_external_table_location(storage_account, container, "silver", "timeseries")
        write_to_delta(df, "catalog.schema.table", mode="overwrite", location=location)
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if location:
        writer.option("path", location).saveAsTable(table_name)
    else:
        writer.saveAsTable(table_name)
