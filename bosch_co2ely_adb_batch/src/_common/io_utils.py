"""I/O utilities - BACKWARD COMPATIBILITY SHIM.

Functions have been relocated:
    polars_to_spark  -> _2_enrich/enrich_utils.py (requires polars)
    read_excel_polars -> _2_enrich/enrich_utils.py (requires polars)
    write_to_delta   -> Use df.write.format("delta").mode(...).saveAsTable() directly
    build_abfss_path -> _common/spark_utils.py

This shim re-exports from new locations so existing imports don't break.
New code should import from the canonical modules directly.
"""

# Polars-dependent functions (only importable when polars is installed)
try:
    from enrich_utils import polars_to_spark, read_excel_polars
except ImportError:
    # polars not installed (e.g. bronze job) - provide stub that raises
    def polars_to_spark(*args, **kwargs):
        raise ImportError("polars_to_spark requires polars. Import from _2_enrich.enrich_utils instead.")

    def read_excel_polars(*args, **kwargs):
        raise ImportError("read_excel_polars requires polars. Import from _2_enrich.enrich_utils instead.")

# Generic Spark utilities
from spark_utils import build_abfss_path


def write_to_delta(df, table_name, mode="append", partition_by=None):
    """Write DataFrame to Delta table. Thin wrapper for backward compat."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)
