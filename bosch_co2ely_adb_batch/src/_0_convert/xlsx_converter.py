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

from converter_utils import (
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

# =============================================================================
# DERIVED METRIC CONSTANTS  (mirrored from CO_energystacck DataEnrichment)
# =============================================================================
_ACTIVE_AREA_CM2 = 88.0       # Active cell area (cm²) for current density
_FARADAY_CONST   = 96485.3    # Faraday constant (C / mol)
_VM_STP          = 22.414     # Molar volume of ideal gas at STP (L / mol)


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
    for i, ch in enumerate(row2_channel_name):
        if _REALTIME_PATTERN.match(ch):
            rt_idx = i
            break

    if rt_idx is None or rt_idx + 1 >= len(columns):
        return df, columns, row1_channel, row2_channel_name, units

    date_col = columns[rt_idx]
    time_col = columns[rt_idx + 1]

    # Verify the next column is the split partner (unnamed or duplicate)
    next_ch = row2_channel_name[rt_idx + 1]
    # if not (next_ch.startswith("unnamed") or next_ch.startswith("Real time") or
    #         next_ch.startswith("real time") or next_ch == ""):
    #     return df, columns, row1_channel, row2_channel_name, units

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


# =============================================================================
# DERIVED METRICS
# =============================================================================

def _f64(col_id: str) -> pl.Expr:
    """Cast a (string) column to Float64 for arithmetic."""
    return pl.col(col_id).cast(pl.Float64, strict=False)


def _safe_str(float_expr: pl.Expr) -> pl.Expr:
    """Replace ±inf and NaN with null, then cast expression to String.

    Must be applied to a Float64 expression.  Polars `fill_nan` replaces
    IEEE NaN with null; the `when/then` guard replaces ±inf.
    """
    cleaned = float_expr.fill_nan(None)
    return pl.when(cleaned.is_infinite()).then(None).otherwise(cleaned).cast(pl.String)


def _apply_derived_metrics(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    units: List[str],
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str]]:
    """Compute the 12 derived metrics from the CO_energystacck enrichment pipeline.

    Matches required input signals by channel_name (row 2 display header),
    case-insensitive.  Formulas are applied on the WIDE DataFrame before
    unpivot so each derived metric becomes a regular channel in bronze/gold
    — no separate silver enrichment step required for the demo.

    Derived columns added (same formulas as DataEnrichment in CO_energystacck):
        1.  Energy Efficiency              [%]
        2.  Δp Anolyte                     [bar]
        3.  Current density                [mA/cm²]
        4.  Faradaic Efficiency of CO and H2 [%]
        5.  Flow CO out                    [nL/min]
        6.  Flow H2 out                    [nL/min]
        7.  Flow O2 out                    [nL/min]
        8.  Flow CO2 out, total            [nL/min]
        9.  Flow CO2 out, anode            [nL/min]
        10. Flow CO2 out, cathode          [nL/min]
        11. CO/H2 ratio recalculated       [-]
        12. Single Pass Conversion Efficiency [%]

    Any formula whose required raw inputs are absent is silently skipped.
    Formulas that depend on earlier derived columns (8-11) work because df
    is updated in-place before each subsequent formula is attempted.

    Args:
        df: Wide Polars DataFrame with string columns named by row 1 ids.
        columns / row1_channel / row2_channel_name / units: parallel metadata
            lists (same semantics as in _process_sheet).

    Returns:
        5-tuple (df, columns, row1_channel, row2_channel_name, units) with
        derived columns appended to all lists.
    """
    # Lookup: display_name_lower -> col_id currently in df
    name_to_col = {n.strip().lower(): c for n, c in zip(row2_channel_name, columns)}

    def _req(*canonical_names: str) -> Optional[List[str]]:
        """Return col_ids for the canonical display names, or None if any is missing."""
        result = []
        for name in canonical_names:
            col_id = name_to_col.get(name.lower())
            if col_id is None:
                return None
            result.append(col_id)
        return result

    def _add(col_name: str, unit: str, expr: pl.Expr) -> None:
        """Append one derived column to df and the parallel metadata lists."""
        nonlocal df, columns, row1_channel, row2_channel_name, units
        if col_name in df.columns:
            logger.debug(f"    Derived '{col_name}' already exists as raw column — skipped")
            return
        try:
            df = df.with_columns(_safe_str(expr).alias(col_name))
            columns          = columns          + [col_name]
            row1_channel     = row1_channel     + [col_name]
            row2_channel_name = row2_channel_name + [col_name]
            units            = units            + [unit]
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"    Derived metric '{col_name}' failed: {exc}")

    # ------------------------------------------------------------------
    # Formulas — order matters: 5-7 must precede 8-11 (chain dependency)
    # ------------------------------------------------------------------

    # 1. Energy Efficiency [%] = 1.48 * FE_CO / (Stack Voltage / 5)
    ids = _req("Faradaic Efficiency of CO", "Stack Voltage")
    if ids:
        fe_co_id, sv_id = ids
        _add("Energy Efficiency", "%",
             1.48 * _f64(fe_co_id) / (_f64(sv_id) / 5.0))

    # 2. Δp Anolyte [bar] = inlet - outlet
    ids = _req("Anolyte inlet pressure", "Anolyte outlet pressure")
    if ids:
        inlet_id, outlet_id = ids
        _add("Δp Anolyte", "bar", _f64(inlet_id) - _f64(outlet_id))

    # 3. Current density [mA/cm²] = 1000 * Current / active_area
    ids = _req("Current")
    if ids:
        _add("Current density", "mA/cm²",
             1000.0 * _f64(ids[0]) / _ACTIVE_AREA_CM2)

    # 4. Faradaic Efficiency of CO and H2 [%] = FE_CO + FE_H2
    ids = _req("Faradaic Efficiency of CO", "Faradaic Efficiency of H2")
    if ids:
        fe_co_id, fe_h2_id = ids
        _add("Faradaic Efficiency of CO and H2", "%",
             _f64(fe_co_id) + _f64(fe_h2_id))

    # 5. Flow CO out [nL/min] = (FE_CO/100 * I / (2F)) * Vm * 60
    ids = _req("Faradaic Efficiency of CO", "Current")
    if ids:
        fe_co_id, cur_id = ids
        _add("Flow CO out", "nL/min",
             (_f64(fe_co_id) / 100.0 * _f64(cur_id) / (2.0 * _FARADAY_CONST))
             * _VM_STP * 60.0)

    # 6. Flow H2 out [nL/min] = (FE_H2/100 * I / (2F)) * Vm * 60
    ids = _req("Faradaic Efficiency of H2", "Current")
    if ids:
        fe_h2_id, cur_id = ids
        _add("Flow H2 out", "nL/min",
             (_f64(fe_h2_id) / 100.0 * _f64(cur_id) / (2.0 * _FARADAY_CONST))
             * _VM_STP * 60.0)

    # 7. Flow O2 out [nL/min] = (FE_O2/100 * I / (4F)) * Vm * 60
    ids = _req("Faradaic Efficiency of O2", "Current")
    if ids:
        fe_o2_id, cur_id = ids
        _add("Flow O2 out", "nL/min",
             (_f64(fe_o2_id) / 100.0 * _f64(cur_id) / (4.0 * _FARADAY_CONST))
             * _VM_STP * 60.0)

    # 8. Flow CO2 out, total [nL/min] = Cathode CO2 inlet - Flow CO out
    ids = _req("Cathode inlet CO2 gas flow")
    if ids and "Flow CO out" in df.columns:
        _add("Flow CO2 out, total", "nL/min",
             _f64(ids[0]) - _f64("Flow CO out"))

    # 9. Flow CO2 out, anode [nL/min] = (ratio/100 * O2_out) / (1 - ratio/100)
    ids = _req("CO2:O2 ratio in anode product gas")
    if ids and "Flow O2 out" in df.columns:
        r = _f64(ids[0]) / 100.0
        _add("Flow CO2 out, anode", "nL/min",
             (r * _f64("Flow O2 out")) / (1.0 - r))

    # 10. Flow CO2 out, cathode = total - anode
    if "Flow CO2 out, total" in df.columns and "Flow CO2 out, anode" in df.columns:
        _add("Flow CO2 out, cathode", "nL/min",
             _f64("Flow CO2 out, total") - _f64("Flow CO2 out, anode"))

    # 11. CO/H2 ratio recalculated = Flow CO out / Flow H2 out
    if "Flow CO out" in df.columns and "Flow H2 out" in df.columns:
        _add("CO/H2 ratio recalculated", "",
             _f64("Flow CO out") / _f64("Flow H2 out"))

    # 12. SPCE [%]
    #   FECO          = FE_CO / 100
    #   CO_form_rate  = I * FECO / (2 * F)            [mol/s]
    #   CO2_in_rate   = (CO2_flow_nlpm / 60 / 5) / Vm  [mol/s per cell]
    #   SPCE          = 100 * CO_form_rate / CO2_in_rate
    ids = _req("Faradaic Efficiency of CO", "Current", "Cathode inlet CO2 gas flow")
    if ids:
        fe_co_id, cur_id, co2_id = ids
        feco        = _f64(fe_co_id) / 100.0
        co_form     = _f64(cur_id) * feco / (2.0 * _FARADAY_CONST)
        co2_in_rate = (_f64(co2_id) / 60.0 / 5.0) / _VM_STP
        _add("Single Pass Conversion Efficiency", "%",
             100.0 * co_form / co2_in_rate)

    n_derived = len(columns) - len(name_to_col)
    if n_derived:
        logger.info(f"    Derived metrics added: {n_derived}")

    return df, columns, row1_channel, row2_channel_name, units


# =============================================================================
# CHANNEL MAPPING
# =============================================================================

def _apply_mapping(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    units: List[str],
    mapping: List[dict],
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str]]:
    """Rename raw file display names to canonical schema column names.

    Applied after _merge_datetime_columns (so Real-time / Unnamed columns are
    already gone) and before _apply_derived_metrics (so formula lookups find
    their inputs by canonical name).

    Matches each mapping entry by file_column (trimmed, case-insensitive)
    against row2_channel_name.  Entries with empty file_column
    (origin=calculation) are silently skipped.  The 'timestamp' column
    produced by _merge_datetime_columns is not in the mapping and is
    preserved as-is.  Duplicate schema_column targets (two raw columns
    mapping to the same canonical name) keep the first match.

    Args:
        df: Wide Polars DataFrame with columns named by row1_channel ids.
        columns / row1_channel / row2_channel_name / units: parallel metadata.
        mapping: List of dicts with 'file_column' and 'schema_column' keys
                 (same format as PoCVI_mapping.json in sys_files/config_files).

    Returns:
        5-tuple (df, columns, row1_channel, row2_channel_name, units) with
        renamed columns and updated metadata.
    """
    if not mapping:
        return df, columns, row1_channel, row2_channel_name, units

    # Build lookup: file_column_lower -> schema_column
    file_to_schema: dict = {}
    for entry in mapping:
        file_col = str(entry.get("file_column") or "").strip()
        schema_col = str(entry.get("schema_column") or "").strip()
        if file_col and schema_col:
            file_to_schema[file_col.lower()] = schema_col

    if not file_to_schema:
        return df, columns, row1_channel, row2_channel_name, units

    new_columns: List[str] = []
    new_row1: List[str] = []
    new_row2: List[str] = []
    new_units: List[str] = []
    df_renames: dict = {}       # old_col_id -> new_col_id
    seen_targets: set = set()   # guard against duplicate targets

    for col_id, r1, r2, u in zip(columns, row1_channel, row2_channel_name, units):
        schema_col = file_to_schema.get(r2.strip().lower())
        if schema_col and schema_col not in seen_targets:
            seen_targets.add(schema_col)
            if col_id != schema_col:
                df_renames[col_id] = schema_col
            new_columns.append(schema_col)
            new_row1.append(schema_col)
            new_row2.append(schema_col)
        else:
            new_columns.append(col_id)
            new_row1.append(r1)
            new_row2.append(r2)
        new_units.append(u)

    if df_renames:
        df = df.rename(df_renames)
        logger.info(f"    Mapping applied: {len(df_renames)} channel(s) renamed to canonical names")

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

    n_rows = df.shape[0]
    if n_rows == 0:
        return None

    # --- Apply channel mapping: raw file display names -> canonical schema names ---
    # Runs AFTER _merge_datetime_columns (Real-time columns already consumed)
    # and BEFORE _apply_derived_metrics (formula lookups need canonical names).
    if mapping:
        df, columns, row1_channel, row2_channel_name, units = _apply_mapping(
            df, columns, row1_channel, row2_channel_name, units, mapping
        )

    # --- Compute derived metrics before unpivot (12 formulas from CO_energystacck) ---
    df, columns, row1_channel, row2_channel_name, units = _apply_derived_metrics(
        df, columns, row1_channel, row2_channel_name, units
    )

    # n_channels includes derived metrics; computed AFTER _apply_derived_metrics.
    # NO Float64 pre-cast. Keep raw strings (unpivot handles cast chain).
    n_channels = len(columns)

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
    mapping: Optional[List[dict]] = None,
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
                               file_size, name, last_modified, abfss_file_path,
                               mapping=mapping)
            if r:
                results.append(r)
        return results

    # Multi-sheet -> thread pool (capped at MAX_SHEET_THREADS)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_SHEET_THREADS) as executor:
        futures = {
            executor.submit(
                _process_sheet, file_bytes, relative_path, file_uuid,
                file_size, name, last_modified, abfss_file_path, mapping
            ): name
            for name in sheet_names
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    return results
