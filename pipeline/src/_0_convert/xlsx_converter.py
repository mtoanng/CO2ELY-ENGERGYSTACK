"""XLSX/XLS -> Parquet converter.

Same 3-row header logic as CSV, applied per sheet.
CRITICAL: NO Float64 pre-cast. Raw strings go to generic_unpivot() which
handles type splitting correctly (cast chain works on String input).
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
    """Convert an XLSX file to 4 Parquet tables per sheet."""
    import fastexcel

    # fastexcel needs raw bytes (not BytesIO)
    excel_file = fastexcel.read_excel(file_bytes)
    sheet_names = excel_file.sheet_names
    results = []

    for sheet_name in sheet_names:
        try:
            header_df = pl.read_excel(
                io.BytesIO(file_bytes), engine="calamine",
                sheet_name=sheet_name,
                has_header=False, infer_schema_length=0,
            )
        except Exception as e:
            logger.warning(f"  Skip sheet '{sheet_name}': {e}")
            continue

        if header_df.shape[0] < 2 or header_df.shape[1] < 1:
            continue

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

        data_start_row = 3 if has_units else 2
        if header_df.shape[0] <= data_start_row:
            continue

        # Slice data rows
        df = header_df.slice(data_start_row)
        rename_map = {df.columns[i]: row2_col_name[i]
                      for i in range(min(len(df.columns), len(row2_col_name)))}
        df = df.rename(rename_map)
        columns = df.columns

        # NO Float64 pre-cast. Keep raw strings.
        # generic_unpivot() handles type splitting via cast chain on String input.
        n_rows = df.shape[0]
        n_channels = len(columns)
        if n_rows == 0:
            continue

        filemeta = build_filemeta(file_path, file_size, last_modified)
        channel = build_channel(file_path, sheet_name, row1_col, row2_col_name, units)
        timeseries = generic_unpivot(df, file_path, sheet_name, columns)
        statistics = build_statistics(file_path, sheet_name, n_channels, n_rows)

        results.append(ConversionResult(
            tables={"filemeta": filemeta, "channel": channel,
                    "timeseries": timeseries, "statistics": statistics},
            group_name=sheet_name, n_rows=n_rows, n_channels=n_channels,
        ))

    return results
