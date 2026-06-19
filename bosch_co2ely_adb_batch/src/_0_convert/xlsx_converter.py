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
- Large sheets (n_rows * n_cols > TIMESERIES_CHUNK_THRESHOLD) use chunked unpivot:
  process CHUNK_ROWS at a time -> write Parquet row groups to temp file.
  Peak memory bounded to chunk_size * n_cols * ~80 bytes regardless of file size.
- Sheet threads capped at MAX_SHEET_THREADS to prevent thread explosion.
"""
import io
import re
import polars as pl
import pyarrow as pa
from typing import List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    SCHEMAS, ConversionResult, unpivot_timeseries,
    generate_file_uuid, build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
    CHUNK_ROWS, TIMESERIES_CHUNK_THRESHOLD,
)

# Sheet-level parallelism DISABLED when file-level threading is active.
MAX_SHEET_THREADS = 2

# Pattern for detecting "Real time" merged header (case-insensitive)
_REALTIME_PATTERN = re.compile(r"^real\s*time$", re.IGNORECASE)
# Excel epoch date that calamine produces when converting time-only serial fractions
_EXCEL_EPOCH_DATE = "1899-12-31"
# Zero time that calamine appends to date-only serial numbers
_ZERO_TIME = "00:00:00"


def _merge_datetime_columns(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    units: List[str],
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str]]:
    """Detect and merge split date+time columns from merged 'Real time' header.

    Excel stores dates as serial numbers and times as day-fractions internally.
    When a merged header spans 2 columns (one date, one time), calamine converts:
      - Date serial (e.g. 46031 for "2/14/2026") -> "2026-02-14 00:00:00"
      - Time fraction (e.g. 0.41768 for "10:01:27 AM") -> "1899-12-31 10:01:27"

    The "1899-12-31" prefix and "00:00:00" suffix are calamine artifacts from
    interpreting Excel's internal numeric representation, NOT actual data.

    This function combines them into a single "timestamp" column:
      "2026-02-14 00:00:00" + "1899-12-31 10:01:27" -> "2026-02-14 10:01:27"

    Channel metadata:
      - channel (row1) = "timestamp"
      - channel_name (row2) = preserved from the original first column header

    Args:
        df: DataFrame with string columns (post-rename, pre-unpivot).
        columns: List of column names (from rename_map).
        row1_channel: Channel identifiers for build_channel().
        row2_channel_name: Display names for build_channel().
        units: Unit strings for build_channel().

    Returns:
        Tuple of (modified_df, columns, row1_channel, row2_channel_name, units)
        with the time column removed and date column replaced by timestamp.
        Returns inputs unchanged if no merge pattern detected.
    """
    # Find "Real time" column index
    rt_idx = None
    for i, ch in enumerate(row1_channel):
        if _REALTIME_PATTERN.match(ch):
            rt_idx = i
            break

    if rt_idx is None or rt_idx + 1 >= len(columns):
        return df, columns, row1_channel, row2_channel_name, units

    date_col = columns[rt_idx]
    time_col = columns[rt_idx + 1]

    # Verify the next column is the split partner (unnamed or duplicate)
    next_ch = row1_channel[rt_idx + 1]
    if not (next_ch.startswith("unnamed") or next_ch.startswith("Real time") or
            next_ch.startswith("real time") or next_ch == ""):
        return df, columns, row1_channel, row2_channel_name, units

    # Peek at first few non-null values to confirm calamine date/time pattern
    sample_date = df[date_col].drop_nulls().head(5).to_list()
    sample_time = df[time_col].drop_nulls().head(5).to_list()

    if not sample_date or not sample_time:
        return df, columns, row1_channel, row2_channel_name, units

    # Confirm calamine pattern:
    #   date col: "2026-02-14 00:00:00" (serial -> datetime with zero time)
    #   time col: "1899-12-31 10:01:27" (fraction -> datetime with epoch date)
    date_str = str(sample_date[0])
    time_str = str(sample_time[0])

    has_date_pattern = (_ZERO_TIME in date_str or len(date_str) == 10)
    has_time_pattern = (_EXCEL_EPOCH_DATE in time_str or "1899-12-30" in time_str)

    if not (has_date_pattern or has_time_pattern):
        # Neither pattern detected — don't merge
        logger.info(f"    'Real time' columns found but no calamine date/time split pattern detected")
        return df, columns, row1_channel, row2_channel_name, units

    logger.info(f"    Merging split 'Real time' columns: '{date_col}' (date) + '{time_col}' (time) -> 'timestamp'")
    logger.info(f"    Sample: date='{date_str}', time='{time_str}'")

    # Combine: extract date part from col1 + time part from col2
    # Calamine output formats:
    #   date: "2026-02-14 00:00:00" -> slice first 10 chars -> "2026-02-14"
    #   time: "1899-12-31 10:01:27" -> slice from char 11  -> "10:01:27"
    df = df.with_columns(
        (
            # Extract date part (first 10 chars = YYYY-MM-DD)
            pl.col(date_col).cast(pl.String).str.slice(0, 10)
            + " "
            + pl.when(
                pl.col(time_col).cast(pl.String).str.contains(_EXCEL_EPOCH_DATE)
            )
            .then(
                # Strip "1899-12-31 " prefix (11 chars) to get HH:MM:SS
                pl.col(time_col).cast(pl.String).str.slice(11)
            )
            .otherwise(
                # Already a time string (no epoch prefix), use as-is
                pl.col(time_col).cast(pl.String)
            )
        ).alias("timestamp")
    )

    # Drop the original two columns, replace with "timestamp"
    df = df.drop([date_col, time_col])

    # Update metadata lists: remove time_col entry, rename date_col to "timestamp"
    # Preserve original channel_name from the first column (row 2 header)
    original_channel_name = row2_channel_name[rt_idx]

    new_columns = [c for c in columns if c != date_col and c != time_col]
    new_columns.insert(rt_idx, "timestamp")

    new_row1 = [ch for i, ch in enumerate(row1_channel) if i != rt_idx and i != rt_idx + 1]
    new_row1.insert(rt_idx, "timestamp")

    new_row2 = [n for i, n in enumerate(row2_channel_name) if i != rt_idx and i != rt_idx + 1]
    new_row2.insert(rt_idx, original_channel_name)

    new_units = [u for i, u in enumerate(units) if i != rt_idx and i != rt_idx + 1]
    new_units.insert(rt_idx, "")

    # Reorder DataFrame columns to match new_columns
    df = df.select(new_columns)

    return df, new_columns, new_row1, new_row2, new_units


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

    # --- Merge split "Real time" date+time columns (calamine edge case) ---
    df, columns, row1_channel, row2_channel_name, units = _merge_datetime_columns(
        df, columns, row1_channel, row2_channel_name, units
    )

    # NO Float64 pre-cast. Keep raw strings.
    n_rows = df.shape[0]
    n_channels = len(columns)
    if n_rows == 0:
        return None

    # file_path: full abfss:// URI if available, else relative_path
    filemeta = build_filemeta(abfss_file_path or relative_path, file_uuid, file_size, last_modified)
    channel = build_channel(file_uuid, sheet_name, row1_channel, row2_channel_name, units)
    statistics = build_statistics(file_uuid, sheet_name, n_channels, n_rows)

    total_timeseries_cells = n_rows * n_channels

    if total_timeseries_cells > TIMESERIES_CHUNK_THRESHOLD:
        # Write timeseries to BytesIO buffer.
        ts_rows, ts_buffer = unpivot_timeseries(
            df, file_uuid, sheet_name, columns, chunk_rows=CHUNK_ROWS
        )
        logger.info(f"    Chunked unpivot: {n_rows} rows * {n_channels} cols = "
                    f"{ts_rows:,} ts rows, chunk_size={CHUNK_ROWS}")

        return ConversionResult(
            tables={"filemeta": filemeta, "channel": channel,
                    "timeseries": None, "statistics": statistics},
            group_name=sheet_name, n_rows=n_rows, n_channels=n_channels,
            timeseries_buffer=ts_buffer,
        )
    else:
        # Standard unpivot
        timeseries = unpivot_timeseries(df, file_uuid, sheet_name, columns)
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

    # UUID from relative_path
    file_uuid = generate_file_uuid(relative_path)

    # fastexcel for sheet name discovery (reads from bytes)
    excel_file = fastexcel.read_excel(file_bytes)
    sheet_names = excel_file.sheet_names

    if len(sheet_names) <= 1:
        # Single sheet -> no threading overhead
        results = []
        name = sheet_names[0] if sheet_names else None
        if name:
            r = _process_sheet(file_bytes, relative_path, file_uuid,
                               file_size, name, last_modified, abfss_file_path)
            if r:
                results.append(r)
        return results

    # Multi-sheet -> thread pool (capped at MAX_SHEET_THREADS)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_SHEET_THREADS) as executor:
        futures = {
            executor.submit(
                _process_sheet, file_bytes, relative_path, file_uuid,
                file_size, name, last_modified, abfss_file_path
            ): name
            for name in sheet_names
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    return results
