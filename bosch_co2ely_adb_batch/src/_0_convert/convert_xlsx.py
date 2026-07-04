"""XLSX/XLS -> Parquet converter.

Same 3-row header logic as CSV, applied per sheet.
CRITICAL: NO Float64 pre-cast. Raw strings go to generic_unpivot() which
handles type splitting correctly (cast chain works on String input).

Schema:
- DataFrame columns are named by ROW 1 (channel_id = original identifier)
- raw_channel = ROW 2 (display name)
- timeseries.channel_id references channel.channel_id (row 1) for joins
- timestamp and elapsed time are preserved as structural columns, not signal rows

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
from typing import List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from convert_utils import (
    SCHEMAS, ConversionResult, unpivot_timeseries,
    generate_file_uuid, build_filemeta, build_channel, build_statistics,
    detect_units_row, logger,
    CHUNK_ROWS, TIMESERIES_CHUNK_THRESHOLD,
)
from derived_metrics import apply_derived_metrics

# Sheet-level parallelism DISABLED when file-level threading is active.
MAX_SHEET_THREADS = 2

# Pattern for detecting "Real time" merged header (case-insensitive)
_REALTIME_PATTERN = re.compile(r"^real\s*time$", re.IGNORECASE)
# Excel epoch date that calamine produces when converting time-only serial fractions
_EXCEL_EPOCH_DATE = "1899-12-31"
# Zero time that calamine appends to date-only serial numbers
_ZERO_TIME = "00:00:00"


def _norm_channel(value: object) -> str:
    return str(value or "").strip().lower().replace("_", " ")


def _is_elapsed_channel(channel_id: str, raw_channel: str, std_channel: str) -> bool:
    candidates = {_norm_channel(channel_id), _norm_channel(raw_channel), _norm_channel(std_channel)}
    return any(value in {"time", "elapsed time", "test time"} for value in candidates)


def _split_structural_channels(
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    std_channels: List[str],
    units: List[str],
) -> Tuple[List[str], List[str], List[str], List[str], List[str], Optional[str], Optional[str]]:
    signal_columns: list[str] = []
    signal_row1: list[str] = []
    signal_row2: list[str] = []
    signal_std: list[str] = []
    signal_units: list[str] = []
    timestamp_column: Optional[str] = None
    elapsed_column: Optional[str] = None

    for col_id, row1, raw, std, unit in zip(columns, row1_channel, row2_channel_name, std_channels, units):
        if _norm_channel(col_id) == "timestamp" or _norm_channel(row1) == "timestamp":
            timestamp_column = col_id
            continue
        if _is_elapsed_channel(row1, raw, std):
            elapsed_column = col_id
            continue
        signal_columns.append(col_id)
        signal_row1.append(row1)
        signal_row2.append(raw)
        signal_std.append(std)
        signal_units.append(unit)

    return signal_columns, signal_row1, signal_row2, signal_std, signal_units, timestamp_column, elapsed_column


def _merge_datetime_columns(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    units: List[str],
    mapping: Optional[List[dict]] = None,
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str]]:
    """Detect and merge split date+time columns into a canonical timestamp channel.

    Prefer explicit mapping entries where schema_column is Date/Time. If those
    are unavailable, fall back to the legacy "Real time" + adjacent unnamed
    column heuristic.
    """
    def norm(value: object) -> str:
        return str(value or "").strip().lower()

    def find_column_index(file_column: str) -> Optional[int]:
        target = norm(file_column)
        if not target:
            return None
        for idx, values in enumerate(zip(columns, row1_channel, row2_channel_name)):
            if target in {norm(value) for value in values}:
                return idx
        return None

    date_idx = None
    time_idx = None
    if mapping:
        for entry in mapping:
            schema_col = norm(entry.get("schema_column"))
            file_col = str(entry.get("file_column") or "")
            if schema_col == "date":
                date_idx = find_column_index(file_col)
            elif schema_col == "time":
                time_idx = find_column_index(file_col)

    used_mapping = date_idx is not None and time_idx is not None
    if not used_mapping:
        # Find "Real time" column index. Some files keep that label in row 1 while
        # row 2 has a friendlier display name such as "Measurement Time".
        for idx, (row1, row2, col) in enumerate(zip(row1_channel, row2_channel_name, columns)):
            if any(_REALTIME_PATTERN.match(str(value).strip()) for value in (row1, row2, col)):
                date_idx = idx
                break

        if date_idx is None or date_idx + 1 >= len(columns):
            return df, columns, row1_channel, row2_channel_name, units
        time_idx = date_idx + 1

        # Verify the next column is the split partner, not a real named signal.
        next_values = [
            str(columns[time_idx]).strip(),
            str(row1_channel[time_idx]).strip(),
            str(row2_channel_name[time_idx]).strip(),
        ]
        is_split_partner = any(
            not value
            or value.lower().startswith("unnamed")
            or value.lower().startswith("column_")
            or re.match(r"^real\s*time(_\d+)?$", value, re.IGNORECASE)
            for value in next_values
        )
        if not is_split_partner:
            return df, columns, row1_channel, row2_channel_name, units

    date_col = columns[date_idx]
    time_col = columns[time_idx]

    # Peek at first few non-null values to confirm calamine date/time pattern.
    # If both columns are entirely null, still merge structurally so downstream
    # logic sees a canonical timestamp column instead of a split date/time pair.
    sample_date = df[date_col].drop_nulls().head(5).to_list()
    sample_time = df[time_col].drop_nulls().head(5).to_list()

    date_str = str(sample_date[0]) if sample_date else None
    time_str = str(sample_time[0]) if sample_time else None

    if date_str is None and time_str is None:
        logger.info(
            f"    Merging split 'Real time' columns: '{date_col}' (date) + '{time_col}' (time) -> 'timestamp' (all-null pair)"
        )
    else:
        # Confirm calamine pattern:
        #   date col: "2026-02-14 00:00:00" (serial -> datetime with zero time)
        #   time col: "1899-12-31 10:01:27" (fraction -> datetime with epoch date)
        has_date_pattern = bool(date_str) and (_ZERO_TIME in date_str or len(date_str) == 10)
        has_time_pattern = bool(time_str) and (_EXCEL_EPOCH_DATE in time_str or "1899-12-30" in time_str)

        if not (has_date_pattern or has_time_pattern):
            # Neither pattern detected — don't merge
            logger.info("    'Real time' columns found but no calamine date/time split pattern detected")
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

    # Drop the original two columns, replace them with "timestamp" at the
    # earlier of the two positions. Preserve the display name from the date side.
    df = df.drop([date_col, time_col])
    timestamp_idx = min(date_idx, time_idx)
    removed_indices = {date_idx, time_idx}
    original_channel_name = row2_channel_name[date_idx]

    new_columns = [c for i, c in enumerate(columns) if i not in removed_indices]
    new_columns.insert(timestamp_idx, "timestamp")

    new_row1 = [ch for i, ch in enumerate(row1_channel) if i not in removed_indices]
    new_row1.insert(timestamp_idx, "timestamp")

    new_row2 = [name for i, name in enumerate(row2_channel_name) if i not in removed_indices]
    new_row2.insert(timestamp_idx, original_channel_name)

    new_units = [unit for i, unit in enumerate(units) if i not in removed_indices]
    new_units.insert(timestamp_idx, "")

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
    mapping: Optional[List[dict]] = None,
    series: Optional[str] = None,
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
        series: Governed series name resolved from the ADLS source folder
            (e.g. "PoC Stack VI"), same lookup used to select the channel
            mapping. Stored on filemeta so Gold no longer has to re-derive
            it via regex on file_path.

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
    # Row 2: candidate channel_name (display name) — but may actually be units
    row2_raw = [str(header_df[c][1] or "") for c in col_names]

    # Detect if Row 2 is a units row (2-row header: name + unit, no separate Row 3 units).
    # Common in derived/calculated measurement sheets where Row 1 has descriptive
    # names and Row 2 has unit symbols like '%', 'V', 'bar', '°C'.
    row2_is_units = detect_units_row(row2_raw)

    if row2_is_units:
        # Row 1 = descriptive names (both identifier AND display name)
        # Row 2 = units (NOT a display name)
        row2_channel_name = [str(header_df[c][0] or "") for c in col_names]
        row2_channel_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_channel_name)]
        units = [v.strip() if v else "" for v in row2_raw]
        logger.info(f"    Row 2 detected as units row (2-row header format)")
    else:
        # Normal 3-row header: Row 1 = identifier, Row 2 = display name
        row2_channel_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_raw)]
        units = [""] * n_cols

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

    # Row 3: unit detection (only if Row 2 was not already units)
    has_units = row2_is_units  # if Row 2 was units, we already have them
    if not has_units and header_df.shape[0] >= 3:
        row3_values = [str(header_df[c][2] or "") for c in col_names]
        has_units = detect_units_row(row3_values)
        if has_units:
            units = [v.strip() if v else "" for v in row3_values]

    # data_start_row: skip header rows before actual measurement data
    # - 3-row header (Row 1 + Row 2 + Row 3 units): skip 3
    # - 2-row header (Row 1 + Row 2 as units OR Row 1 + Row 2 name): skip 2
    if row2_is_units:
        data_start_row = 2  # Row 0=names, Row 1=units, Row 2+=data
    else:
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

    # --- Merge split date+time columns into canonical timestamp ---
    # Prefer Date/Time mapping entries; fall back to Real-time heuristics.
    df, columns, row1_channel, row2_channel_name, units = _merge_datetime_columns(
        df, columns, row1_channel, row2_channel_name, units, mapping=mapping
    )

    n_rows = df.shape[0]
    if n_rows == 0:
        return None

    # Keep Bronze raw-ish: canonical channel mapping is applied in Silver.
    raw_lookup_channels = row2_channel_name.copy()

    # --- Compute derived metrics + plausibility limits (mirrors CO_energystacck) ---
    df, columns, row1_channel, row2_channel_name, raw_lookup_channels, units = apply_derived_metrics(
        df, columns, row1_channel, row2_channel_name, raw_lookup_channels, units, series=series
    )

    (
        signal_columns,
        signal_row1,
        signal_row2,
        _signal_lookup_channels,
        signal_units,
        timestamp_column,
        elapsed_column,
    ) = _split_structural_channels(columns, row1_channel, row2_channel_name, raw_lookup_channels, units)

    # n_channels and channel catalog include signal/derived channels only.
    # Structural timestamp/elapsed columns are repeated on timeseries rows.
    n_channels = len(signal_columns)

    # file_path: full abfss:// URI if available, else relative_path
    filemeta = build_filemeta(
        abfss_file_path or relative_path,
        file_uuid,
        file_size,
        last_modified,
        series,
        group=sheet_name,
    )
    channel = build_channel(file_uuid, sheet_name, signal_row1, signal_row2, signal_units)
    statistics = build_statistics(file_uuid, sheet_name, n_channels, n_rows)

    total_timeseries_cells = n_rows * n_channels

    if total_timeseries_cells > TIMESERIES_CHUNK_THRESHOLD:
        # Write timeseries to BytesIO buffer.
        ts_rows, ts_buffer = unpivot_timeseries(
            df,
            file_uuid,
            sheet_name,
            signal_columns,
            chunk_rows=CHUNK_ROWS,
            timestamp_column=timestamp_column,
            elapsed_column=elapsed_column,
        )
        logger.info(f"    Chunked unpivot: {n_rows} rows * {n_channels} signal cols = "
                    f"{ts_rows:,} ts rows, chunk_size={CHUNK_ROWS}")

        return ConversionResult(
            tables={"filemeta": filemeta, "channel": channel,
                    "timeseries": None, "statistics": statistics},
            group_name=sheet_name, n_rows=n_rows, n_channels=n_channels,
            timeseries_buffer=ts_buffer,
        )
    else:
        # Standard unpivot
        timeseries = unpivot_timeseries(
            df,
            file_uuid,
            sheet_name,
            signal_columns,
            timestamp_column=timestamp_column,
            elapsed_column=elapsed_column,
        )
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
    mapping: Optional[List[dict]] = None,
    series: Optional[str] = None,
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
        series: Governed series name resolved from the ADLS source folder,
            stored directly on filemeta.

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
                               file_size, name, last_modified, abfss_file_path,
                               mapping=mapping, series=series)
            if r:
                results.append(r)
        return results

    # Multi-sheet -> thread pool (capped at MAX_SHEET_THREADS)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_SHEET_THREADS) as executor:
        futures = {
            executor.submit(
                _process_sheet, file_bytes, relative_path, file_uuid,
                file_size, name, last_modified, abfss_file_path, mapping, series
            ): name
            for name in sheet_names
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    return results
