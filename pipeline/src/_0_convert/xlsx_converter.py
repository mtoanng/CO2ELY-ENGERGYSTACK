"""XLSX/XLS -> Parquet converter.

Same 3-row header logic as CSV, applied per sheet.
CRITICAL: NO Float64 pre-cast. Raw strings go to generic_unpivot() which
handles type splitting correctly (cast chain works on String input).

Schema:
- DataFrame columns are named by ROW 1 (channel = original identifier)
- channel_name = ROW 2 (display name)
- timeseries.channel references channel.channel (row 1) for joins

I/O: Receives raw bytes (downloaded by Azure SDK in worker).
Polars + calamine engine (Rust-native parsing, releases GIL).
Parallelism: Sheets are processed in parallel via ThreadPoolExecutor.
"""
import io
import polars as pl
import pyarrow as pa
from typing import List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    SCHEMAS, ConversionResult, generic_unpivot, generate_file_uuid,
    build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
)


def _process_sheet(
    file_bytes: bytes,
    relative_path: str,
    file_uuid: str,
    file_size: int,
    sheet_name: str,
    last_modified: Optional[datetime],
) -> Optional[ConversionResult]:
    """Process a single sheet. Thread-safe (no shared mutable state)."""
    try:
        header_df = pl.read_excel(
            io.BytesIO(file_bytes), engine="calamine",
            sheet_name=sheet_name,
            has_header=False, infer_schema_length=0,
        )
    except Exception as e:
        logger.warning(f"  Skip sheet '{sheet_name}': {e}")
        return None

    if header_df.shape[0] < 2 or header_df.shape[1] < 1:
        return None

    n_cols = header_df.shape[1]
    col_names = header_df.columns

    # Row 1: channel (original identifier) — used as join key in timeseries
    row1_channel = [str(header_df[c][0] or "") for c in col_names]
    # Row 2: channel_name (display name)
    row2_channel_name = [str(header_df[c][1] or "") for c in col_names]
    row2_channel_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_channel_name)]

    # Ensure unique channel identifiers (row 1 may have duplicates)
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

    data_start_row = 3 if has_units else 2
    if header_df.shape[0] <= data_start_row:
        return None

    # Slice data rows and rename columns to ROW 1 (channel identifiers)
    df = header_df.slice(data_start_row)
    df_cols = df.columns
    rename_map = {df_cols[i]: row1_channel[i]
                  for i in range(min(len(df_cols), len(row1_channel)))}
    df = df.rename(rename_map)
    columns = list(rename_map.values())

    # NO Float64 pre-cast. Keep raw strings.
    n_rows = df.shape[0]
    n_channels = len(columns)
    if n_rows == 0:
        return None

    # Use relative_path for filemeta (environment-independent, matches tracking table)
    filemeta = build_filemeta(relative_path, file_uuid, file_size, last_modified)
    channel = build_channel(file_uuid, sheet_name, row1_channel, row2_channel_name, units)
    timeseries = generic_unpivot(df, file_uuid, sheet_name, columns)
    statistics = build_statistics(file_uuid, sheet_name, n_channels, n_rows)

    return ConversionResult(
        tables={"filemeta": filemeta, "channel": channel,
                "timeseries": timeseries, "statistics": statistics},
        group_name=sheet_name, n_rows=n_rows, n_channels=n_channels,
    )


def convert(
    file_bytes: bytes,
    relative_path: str,
    file_size: int,
    last_modified: Optional[datetime] = None,
) -> List[ConversionResult]:
    """Convert XLSX bytes to 4 Parquet tables per sheet.

    Args:
        file_bytes: Raw file content (downloaded by Azure SDK in worker).
        relative_path: Env-independent path for UUID + filemeta (join key).
        file_size: File size in bytes.
        last_modified: Blob modification timestamp.

    Polars + calamine parses from BytesIO (Rust, releases GIL).
    Sheets are processed in parallel threads:
    - Each sheet reads from the same file_bytes (immutable, shared safely)
    - Produces independent ConversionResult (no shared state)
    """
    import fastexcel

    # UUID from relative_path (environment-independent, matches tracking table)
    file_uuid = generate_file_uuid(relative_path)

    # fastexcel for sheet name discovery (reads from bytes)
    excel_file = fastexcel.read_excel(file_bytes)
    sheet_names = excel_file.sheet_names

    if len(sheet_names) <= 1:
        # Single sheet -> no threading overhead
        results = []
        for sheet_name in sheet_names:
            r = _process_sheet(file_bytes, relative_path, file_uuid, file_size, sheet_name, last_modified)
            if r:
                results.append(r)
        return results

    # Multi-sheet -> parallel processing (Polars releases GIL)
    logger.info(f"  {relative_path}: {len(sheet_names)} sheets -> parallel processing")
    results = []
    with ThreadPoolExecutor(max_workers=len(sheet_names)) as executor:
        futures = {
            executor.submit(
                _process_sheet, file_bytes, relative_path, file_uuid, file_size, sheet_name, last_modified
            ): sheet_name
            for sheet_name in sheet_names
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    return results
