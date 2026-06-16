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

Memory safety:
- Large sheets (n_rows × n_cols > TIMESERIES_CHUNK_THRESHOLD) use chunked unpivot:
  process CHUNK_ROWS at a time → write Parquet row groups to temp file.
  Peak memory bounded to chunk_size × n_cols × ~80 bytes regardless of file size.
- Sheet threads capped at MAX_SHEET_THREADS to prevent thread explosion.
"""
import io
import polars as pl
import pyarrow as pa
from typing import List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    SCHEMAS, ConversionResult, generic_unpivot, generic_unpivot_chunked,
    generate_file_uuid, build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
    CHUNK_ROWS, TIMESERIES_CHUNK_THRESHOLD,
)

# Sheet-level parallelism DISABLED when file-level threading is active.
# With THREADS_PER_PARTITION=4, file-level concurrency already saturates CPU.
# Nested sheet threads would cause: 4 tasks × 4 files × N sheets = thread explosion.
# Each sheet still re-decompresses the full xlsx via calamine (~1.5 GB per call).
# Sequential sheets within a file keeps memory predictable.
MAX_SHEET_THREADS = 1


def _process_sheet(
    file_bytes: bytes,
    relative_path: str,
    file_uuid: str,
    file_size: int,
    sheet_name: str,
    last_modified: Optional[datetime],
    abfss_file_path: Optional[str] = None,
) -> Optional[ConversionResult]:
    """Process a single Excel sheet into 4 Parquet tables. Thread-safe (no shared mutable state).

    Applies the 3-row header logic (channel, channel_name, unit detection),
    reads data rows, renames columns to row 1 identifiers, and produces
    wide-to-long melt for timeseries output.

    Args:
        file_bytes: Raw Excel file content (shared across threads, immutable).
        relative_path: Env-independent path for UUID generation.
        file_uuid: Pre-computed deterministic UUID for this file.
        file_size: File size in bytes (stored in filemeta).
        sheet_name: Name of the sheet to process.
        last_modified: Blob modification timestamp.
        abfss_file_path: Full abfss:// URI for filemeta.file_path.
            Falls back to relative_path if None.

    Returns:
        ConversionResult with 4 PyArrow tables (filemeta, channel, timeseries,
        statistics) for this sheet. Returns None if sheet is empty, has < 2 rows,
        or fails to parse.
    """
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

    # file_path: full abfss:// URI if available, else relative_path
    filemeta = build_filemeta(abfss_file_path or relative_path, file_uuid, file_size, last_modified)
    channel = build_channel(file_uuid, sheet_name, row1_channel, row2_channel_name, units)
    statistics = build_statistics(file_uuid, sheet_name, n_channels, n_rows)

    # Decide: in-memory unpivot (small) vs chunked unpivot (large)
    total_timeseries_cells = n_rows * n_channels

    if total_timeseries_cells > TIMESERIES_CHUNK_THRESHOLD:
        # CHUNKED PATH: write timeseries to BytesIO buffer (no disk I/O).
        # Peak memory = CHUNK_ROWS × n_channels × ~80 bytes + compressed buffer.
        ts_rows, ts_buffer = generic_unpivot_chunked(
            df, file_uuid, sheet_name, columns, CHUNK_ROWS
        )
        logger.info(f"    Chunked unpivot: {n_rows} rows × {n_channels} cols = "
                    f"{ts_rows:,} ts rows, chunk_size={CHUNK_ROWS}")

        return ConversionResult(
            tables={"filemeta": filemeta, "channel": channel,
                    "timeseries": None, "statistics": statistics},
            group_name=sheet_name, n_rows=n_rows, n_channels=n_channels,
            timeseries_buffer=ts_buffer,
        )
    else:
        # IN-MEMORY PATH: small file, standard unpivot (fast, no disk I/O)
        timeseries = generic_unpivot(df, file_uuid, sheet_name, columns)
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
    abfss_file_path: Optional[str] = None,
) -> List[ConversionResult]:
    """Convert XLSX/XLS bytes to 4 Parquet tables per sheet.

    Discovers sheet names via fastexcel, then processes each sheet in parallel
    (multi-sheet) or sequentially (single-sheet) using the 3-row header logic.
    Each sheet produces an independent ConversionResult.

    Args:
        file_bytes: Raw Excel file content (downloaded by Azure SDK in worker).
        relative_path: Env-independent path for UUID generation and tracking.
        file_size: File size in bytes (stored in filemeta).
        last_modified: Blob modification timestamp (stored in filemeta).
        abfss_file_path: Full abfss:// URI stored in filemeta.file_path.
            Falls back to relative_path if None.

    Returns:
        List[ConversionResult]: One ConversionResult per non-empty sheet, each
        containing 4 PyArrow tables (filemeta, channel, timeseries, statistics).
        Returns empty list if all sheets are empty or fail to parse.
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
            r = _process_sheet(file_bytes, relative_path, file_uuid, file_size, sheet_name, last_modified, abfss_file_path)
            if r:
                results.append(r)
        return results

    # Multi-sheet -> parallel processing (Polars releases GIL)
    # Cap threads to MAX_SHEET_THREADS to avoid thread explosion + memory pressure
    n_threads = min(len(sheet_names), MAX_SHEET_THREADS)
    logger.info(f"  {relative_path}: {len(sheet_names)} sheets -> parallel ({n_threads} threads)")
    results = []
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {
            executor.submit(
                _process_sheet, file_bytes, relative_path, file_uuid, file_size, sheet_name, last_modified, abfss_file_path
            ): sheet_name
            for sheet_name in sheet_names
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    return results
