"""CSV -> Parquet converter.

Header logic (scan first 3 rows):
  Row 1: col (identifier/description)
  Row 2: col_name (DataFrame header)
  Row 3: channel_unit if special chars detected, else first data row

Timeseries: wide->long melt (all columns), row index as sample_offset.
"""
import io
import polars as pl
import pyarrow as pa
from typing import List, Optional
from datetime import datetime

from common import (
    SCHEMAS, ConversionResult, generic_unpivot,
    build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
)


def convert(
    file_bytes: bytes,
    file_path: str,
    file_size: int,
    last_modified: Optional[datetime] = None,
) -> List[ConversionResult]:
    """Convert a CSV file to 4 Parquet tables."""
    buf = io.BytesIO(file_bytes)

    # Read first 3 rows raw (all string)
    header_df = pl.read_csv(
        buf, has_header=False, skip_rows=0, n_rows=3,
        infer_schema_length=0, ignore_errors=True,
    )

    if header_df.shape[0] < 2:
        logger.warning(f"  {file_path}: less than 2 rows, skipping")
        return []

    n_cols = header_df.shape[1]
    row1_col = [str(header_df[c][0] or "") for c in header_df.columns]
    row2_col_name = [str(header_df[c][1] or "") for c in header_df.columns]
    row2_col_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_col_name)]

    has_units = False
    units = [""] * n_cols
    if header_df.shape[0] >= 3:
        row3_values = [str(header_df[c][2] or "") for c in header_df.columns]
        has_units = detect_units_row(row3_values)
        if has_units:
            units = [v.strip() if v else "" for v in row3_values]

    # Read data (skip header rows)
    data_skip_rows = 3 if has_units else 2
    buf.seek(0)
    df = pl.read_csv(
        buf, has_header=False, skip_rows=data_skip_rows,
        infer_schema_length=10_000, ignore_errors=True,
    )

    if df.is_empty():
        return []

    rename_map = {df.columns[i]: row2_col_name[i] for i in range(min(len(df.columns), len(row2_col_name)))}
    df = df.rename(rename_map)
    columns = df.columns
    n_rows = df.shape[0]
    n_channels = len(columns)
    group = "data"

    filemeta = build_filemeta(file_path, file_size, last_modified)
    channel = build_channel(file_path, group, row1_col, row2_col_name, units)
    timeseries = generic_unpivot(df, file_path, group, columns)
    statistics = build_statistics(file_path, group, n_channels, n_rows)

    return [ConversionResult(
        tables={"filemeta": filemeta, "channel": channel,
                "timeseries": timeseries, "statistics": statistics},
        group_name=None, n_rows=n_rows, n_channels=n_channels,
    )]
