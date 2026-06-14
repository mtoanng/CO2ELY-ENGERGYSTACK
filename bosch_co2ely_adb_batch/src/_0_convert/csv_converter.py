"""CSV -> Parquet converter.

Header logic (scan first 3 rows):
  Row 1: channel (original identifier) — used as join key in timeseries
  Row 2: channel_name (display name)
  Row 3: unit if special chars detected, else first data row

Timeseries: wide->long melt (all columns), row index as sample_offset.
DataFrame columns are named by ROW 1 (channel) so timeseries.channel = row1 values.

I/O: Receives raw bytes (downloaded by Azure SDK in worker).
Polars reads CSV from BytesIO — Rust I/O, fastest possible path.
"""
import io
import polars as pl
import pyarrow as pa
from typing import List, Optional
from datetime import datetime

from common import (
    SCHEMAS, ConversionResult, generic_unpivot, generate_file_uuid,
    build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
)


def convert(
    file_bytes: bytes,
    relative_path: str,
    file_size: int,
    last_modified: Optional[datetime] = None,
    abfss_file_path: Optional[str] = None,
) -> List[ConversionResult]:
    """Convert CSV bytes to 4 Parquet tables (filemeta, channel, timeseries, statistics).

    Parses the 3-row header structure to extract channel identifiers, display names,
    and optional units. Then reads data rows, renames columns to row 1 values, and
    produces a wide-to-long melt for timeseries output.

    Args:
        file_bytes: Raw file content (downloaded by Azure SDK in worker).
        relative_path: Env-independent path for UUID generation and tracking.
        file_size: File size in bytes (stored in filemeta).
        last_modified: Blob modification timestamp (stored in filemeta).
        abfss_file_path: Full abfss:// URI stored in filemeta.file_path.
            Falls back to relative_path if None.

    Returns:
        List[ConversionResult]: Single-element list containing a ConversionResult
            with 4 PyArrow tables (filemeta, channel, timeseries, statistics).
            Returns empty list if file has < 2 rows or no data after headers.
    """
    # UUID from relative_path (environment-independent, matches tracking table)
    file_uuid = generate_file_uuid(relative_path)

    # Read first 3 rows raw (all string) — for header analysis
    header_df = pl.read_csv(
        io.BytesIO(file_bytes), has_header=False, skip_rows=0, n_rows=3,
        infer_schema_length=0, ignore_errors=True,
    )

    if header_df.shape[0] < 2:
        logger.warning(f"  {relative_path}: less than 2 rows, skipping")
        return []

    n_cols = header_df.shape[1]
    col_names = header_df.columns

    # Row 1: channel (original identifier) — becomes timeseries.channel
    row1_channel = [str(header_df[c][0] or "") for c in col_names]
    # Row 2: channel_name (display name)
    row2_channel_name = [str(header_df[c][1] or "") for c in col_names]
    row2_channel_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_channel_name)]

    # Ensure unique channel identifiers
    seen = {}
    unique_channels = []
    for ch in row1_channel:
        ch = ch.strip() if ch.strip() else "unnamed"
        if ch in seen:
            seen[ch] += 1
            unique_channels.append(f"{ch}_{seen[ch]}")
        else:
            seen[ch] = 0
            unique_channels.append(ch)
    row1_channel = unique_channels

    # Row 3: unit detection
    has_units = False
    units = [""] * n_cols
    if header_df.shape[0] >= 3:
        row3_values = [str(header_df[c][2] or "") for c in col_names]
        has_units = detect_units_row(row3_values)
        if has_units:
            units = [v.strip() if v else "" for v in row3_values]

    # Read data (skip header rows) — Polars reads from BytesIO
    data_skip_rows = 3 if has_units else 2
    df = pl.read_csv(
        io.BytesIO(file_bytes), has_header=False, skip_rows=data_skip_rows,
        infer_schema_length=10_000, ignore_errors=True,
    )

    if df.is_empty():
        return []

    # Rename columns to ROW 1 (channel identifiers) — NOT row 2 (channel_name)
    df_cols = df.columns
    rename_map = {df_cols[i]: row1_channel[i] for i in range(min(len(df_cols), len(row1_channel)))}
    df = df.rename(rename_map)
    columns = list(rename_map.values())
    n_rows = df.shape[0]
    n_channels = len(columns)
    group = "data"

    # file_path: full abfss:// URI if available, else relative_path
    filemeta = build_filemeta(abfss_file_path or relative_path, file_uuid, file_size, last_modified)
    channel = build_channel(file_uuid, group, row1_channel, row2_channel_name, units)
    timeseries = generic_unpivot(df, file_uuid, group, columns)
    statistics = build_statistics(file_uuid, group, n_channels, n_rows)

    return [ConversionResult(
        tables={"filemeta": filemeta, "channel": channel,
                "timeseries": timeseries, "statistics": statistics},
        group_name=None, n_rows=n_rows, n_channels=n_channels,
    )]
