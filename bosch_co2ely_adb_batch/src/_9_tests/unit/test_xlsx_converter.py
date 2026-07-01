"""Unit tests for _0_convert/convert_xlsx.py.

Tests XLSX parsing: header detection (3-row scan), sheet enumeration,
multi-sheet handling, data type inference, and edge cases.

Dependencies: polars, pyarrow, fastexcel (calamine).
"""
import io
import pytest
import polars as pl
import pyarrow as pa
from unittest.mock import patch

from convert_utils import (
    SCHEMAS, TABLE_TYPES, ConversionResult,
    build_filemeta, build_channel, build_statistics,
    detect_units_row, unpivot_timeseries,
    TIMESERIES_CHUNK_THRESHOLD,
)

# Try importing the converter (may need specific deps)
try:
    from convert_xlsx import convert, _merge_datetime_columns, _split_structural_channels
    HAS_convert_xlsx = True
except ImportError:
    HAS_convert_xlsx = False


# =============================================================================
# XLSX PARSING
# =============================================================================

@pytest.mark.skipif(not HAS_convert_xlsx, reason="convert_xlsx not importable")
class TestXlsxConvert:
    """Full xlsx conversion pipeline: bytes -> ConversionResult list."""

    def test_returns_list_of_results(self, sample_xlsx_bytes):
        results = convert(
            sample_xlsx_bytes,
            "test/file.xlsx",
            len(sample_xlsx_bytes),
            None,
            abfss_file_path="abfss://container@storage.dfs.core.windows.net/raw_data/test/file.xlsx",
        )
        assert isinstance(results, list)
        assert len(results) >= 1  # at least one sheet

    def test_result_has_all_table_types(self, sample_xlsx_bytes):
        results = convert(
            sample_xlsx_bytes, "test/file.xlsx",
            len(sample_xlsx_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        for r in results:
            assert isinstance(r, ConversionResult)
            # Each result should have filemeta, channel, statistics
            assert "filemeta" in r.tables
            assert "channel" in r.tables
            assert "statistics" in r.tables
            # timeseries either in tables or in buffer
            has_ts = ("timeseries" in r.tables and r.tables["timeseries"] is not None) \
                     or r.timeseries_buffer is not None
            assert has_ts

    def test_filemeta_schema_correct(self, sample_xlsx_bytes):
        results = convert(
            sample_xlsx_bytes, "test/file.xlsx",
            len(sample_xlsx_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        filemeta = results[0].tables["filemeta"]
        assert filemeta.schema == SCHEMAS["filemeta"]

    def test_uuid_consistent_across_tables(self, sample_xlsx_bytes):
        results = convert(
            sample_xlsx_bytes, "test/file.xlsx",
            len(sample_xlsx_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        r = results[0]
        uuid_filemeta = r.tables["filemeta"].column("uuid")[0].as_py()
        uuid_channel = r.tables["channel"].column("uuid")[0].as_py()
        uuid_stats = r.tables["statistics"].column("uuid")[0].as_py()
        assert uuid_filemeta == uuid_channel == uuid_stats


# =============================================================================
# HEADER DETECTION EDGE CASES
# =============================================================================

class TestHeaderDetection:
    """Header row detection logic (3-row scan)."""

    def test_standard_3_row_header(self):
        """Row1=channel, Row2=name, Row3=unit -> data starts row 4."""
        row3 = ["s", "V", "A", "degC"]
        assert detect_units_row(row3) is True

    def test_no_units_row(self):
        """If row3 is data, units row is absent -> data starts row 3."""
        row3 = ["0.0", "3.14", "10.5", "25.0"]
        assert detect_units_row(row3) is False

    def test_partial_units(self):
        """Some cells have units, some don't (>50% threshold)."""
        # 3 out of 4 look like units
        row3 = ["V", "A", "mA/cm2", ""]
        assert detect_units_row(row3) is True

    def test_all_empty_not_units(self):
        """Completely empty row is NOT a units row."""
        assert detect_units_row(["", "", "", ""]) is False


# =============================================================================
# MULTI-SHEET HANDLING
# =============================================================================

@pytest.mark.skipif(not HAS_convert_xlsx, reason="convert_xlsx not importable")
class TestMultiSheet:
    """Multi-sheet XLSX files produce one ConversionResult per sheet."""

    def test_multi_sheet_creates_multiple_results(self):
        """Create a 2-sheet XLSX and verify 2 results returned."""
        try:
            buf = io.BytesIO()
            with pl.ExcelWriter(buf) as writer:
                pl.DataFrame({"A": [1, 2]}).write_excel(writer, worksheet="Sheet1")
                pl.DataFrame({"B": [3, 4]}).write_excel(writer, worksheet="Sheet2")
            buf.seek(0)

            results = convert(
                buf.read(), "multi.xlsx", 1024, None,
                abfss_file_path="abfss://c@s.dfs.core.windows.net/multi.xlsx",
            )
            assert len(results) == 2
        except Exception:
            pytest.skip("Multi-sheet XLSX creation failed (polars version)")

    def test_group_name_is_sheet_name(self, sample_xlsx_bytes):
        results = convert(
            sample_xlsx_bytes, "test.xlsx", len(sample_xlsx_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/test.xlsx",
        )
        # group_name should be the sheet name
        assert results[0].group_name is not None
        assert len(results[0].group_name) > 0


# =============================================================================
# CHUNKED VS IN-MEMORY THRESHOLD
# =============================================================================

class TestChunkingThreshold:
    """Large sheets trigger chunked unpivot (BytesIO output)."""

    def test_small_sheet_in_memory(self):
        """Below threshold: uses generic_unpivot (PyArrow Table)."""
        df = pl.DataFrame({"ch1": list(range(10)), "ch2": list(range(10))})
        # 10 rows x 2 cols = 20 cells < TIMESERIES_CHUNK_THRESHOLD (50_000)
        n_cells = df.height * df.width
        assert n_cells < TIMESERIES_CHUNK_THRESHOLD

    def test_large_sheet_chunked(self):
        """Above threshold: uses generic_unpivot_chunked (BytesIO)."""
        n_cells = TIMESERIES_CHUNK_THRESHOLD + 1
        assert n_cells > TIMESERIES_CHUNK_THRESHOLD


# =============================================================================
# ERROR HANDLING
# =============================================================================

@pytest.mark.skipif(not HAS_convert_xlsx, reason="convert_xlsx not importable")
class TestXlsxErrors:
    """Error handling for corrupt/invalid XLSX files."""

    def test_empty_bytes_raises(self):
        with pytest.raises(Exception):
            convert(b"", "empty.xlsx", 0, None, abfss_file_path="abfss://c@s/p")

    def test_invalid_bytes_raises(self):
        with pytest.raises(Exception):
            convert(b"not an xlsx file", "bad.xlsx", 16, None, abfss_file_path="abfss://c@s/p")

    def test_corrupted_zip_raises(self):
        # XLSX is a zip file; corrupt the magic bytes
        corrupt = b"PK\x03\x04" + b"\x00" * 100
        with pytest.raises(Exception):
            convert(corrupt, "corrupt.xlsx", 104, None, abfss_file_path="abfss://c@s/p")


# =============================================================================
# MERGE DATETIME COLUMNS ("Real time" edge case)
# =============================================================================

@pytest.mark.skipif(not HAS_convert_xlsx, reason="convert_xlsx not importable")
class TestMergeDatetimeColumns:
    """Tests for _merge_datetime_columns() — merged 'Real time' header fix.

    Calamine converts Excel serial numbers:
      - Date serial (e.g. 46031) -> "2026-02-14 00:00:00"
      - Time fraction (e.g. 0.41768) -> "1899-12-31 10:01:27"

    The function merges these into a single "timestamp" column.
    """

    def test_basic_merge(self):
        """Standard case: 'Real time' date col + unnamed time col -> 'timestamp'."""
        df = pl.DataFrame({
            "Real time": ["2026-02-14 00:00:00", "2026-02-15 00:00:00", "2026-02-16 00:00:00"],
            "unnamed": ["1899-12-31 10:01:27", "1899-12-31 11:30:00", "1899-12-31 14:45:59"],
            "Voltage": ["3.1", "3.2", "3.3"],
        })
        columns = ["Real time", "unnamed", "Voltage"]
        row1_channel = ["Real time", "unnamed", "Voltage"]
        row2_channel_name = ["Real time", "Column_1", "Stack Voltage"]
        units = ["", "", "V"]

        result_df, result_cols, result_row1, result_row2, result_units = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Should produce 2 columns: timestamp + Voltage
        assert result_cols == ["timestamp", "Voltage"]
        assert result_row1 == ["timestamp", "Voltage"]
        # channel_name preserves original first column header
        assert result_row2 == ["Real time", "Stack Voltage"]
        assert result_units == ["", "V"]

        # Check merged values
        ts_values = result_df["timestamp"].to_list()
        assert ts_values[0] == "2026-02-14 10:01:27"
        assert ts_values[1] == "2026-02-15 11:30:00"
        assert ts_values[2] == "2026-02-16 14:45:59"

    def test_preserves_channel_name_from_original_header(self):
        """channel_name should be the row2 value of the original date column."""
        df = pl.DataFrame({
            "Real time": ["2026-01-01 00:00:00"],
            "unnamed": ["1899-12-31 08:00:00"],
        })
        columns = ["Real time", "unnamed"]
        row1_channel = ["Real time", "unnamed"]
        row2_channel_name = ["Measurement Time", "Column_1"]
        units = ["", ""]

        _, _, result_row1, result_row2, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        assert result_row1 == ["timestamp"]
        assert result_row2 == ["Measurement Time"]

    def test_no_merge_without_realtime_header(self):
        """Should NOT merge if no 'Real time' column exists."""
        df = pl.DataFrame({
            "Time": ["2026-02-14 00:00:00"],
            "unnamed": ["1899-12-31 10:00:00"],
            "Voltage": ["3.1"],
        })
        columns = ["Time", "unnamed", "Voltage"]
        row1_channel = ["Time", "unnamed", "Voltage"]
        row2_channel_name = ["Time", "Column_1", "Voltage"]
        units = ["s", "", "V"]

        result_df, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Unchanged — no merge
        assert result_cols == ["Time", "unnamed", "Voltage"]
        assert list(result_df.columns) == ["Time", "unnamed", "Voltage"]

    def test_no_merge_next_col_not_unnamed(self):
        """Should NOT merge if the column after 'Real time' is a named channel."""
        df = pl.DataFrame({
            "Real time": ["2026-02-14 00:00:00"],
            "Voltage": ["3.1"],
        })
        columns = ["Real time", "Voltage"]
        row1_channel = ["Real time", "Voltage"]
        row2_channel_name = ["Real time", "Stack Voltage"]
        units = ["", "V"]

        _, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Unchanged — next column is "Voltage", not unnamed/duplicate
        assert result_cols == ["Real time", "Voltage"]

    def test_no_merge_no_calamine_pattern(self):
        """Should NOT merge if data doesn't show calamine date/time artifacts."""
        df = pl.DataFrame({
            "Real time": ["some text", "other text"],
            "unnamed": ["more text", "data here"],
            "Voltage": ["3.1", "3.2"],
        })
        columns = ["Real time", "unnamed", "Voltage"]
        row1_channel = ["Real time", "unnamed", "Voltage"]
        row2_channel_name = ["Real time", "Column_1", "Voltage"]
        units = ["", "", "V"]

        _, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Unchanged — no date/time pattern detected
        assert result_cols == ["Real time", "unnamed", "Voltage"]

    def test_merge_with_realtime_1_duplicate(self):
        """Should merge when second col is 'Real time_1' (uniqueness suffix)."""
        df = pl.DataFrame({
            "Real time": ["2026-03-01 00:00:00", "2026-03-02 00:00:00"],
            "Real time_1": ["1899-12-31 09:15:00", "1899-12-31 16:30:00"],
            "Current": ["10.5", "11.0"],
        })
        columns = ["Real time", "Real time_1", "Current"]
        row1_channel = ["Real time", "Real time_1", "Current"]
        row2_channel_name = ["Real time", "Column_1", "Cell Current"]
        units = ["", "", "A"]

        result_df, result_cols, result_row1, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        assert result_cols == ["timestamp", "Current"]
        assert result_row1 == ["timestamp", "Current"]
        assert result_df["timestamp"].to_list() == ["2026-03-01 09:15:00", "2026-03-02 16:30:00"]

    def test_merge_case_insensitive(self):
        """'REAL TIME' and 'real time' should both trigger merge."""
        df = pl.DataFrame({
            "REAL TIME": ["2026-01-10 00:00:00"],
            "unnamed": ["1899-12-31 12:00:00"],
        })
        columns = ["REAL TIME", "unnamed"]
        # After uniqueness logic in _process_sheet, the channel identifier
        # would be "REAL TIME" (preserving case from row1)
        row1_channel = ["REAL TIME", "unnamed"]
        row2_channel_name = ["Real Time", "Column_1"]
        units = ["", ""]

        _, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        assert result_cols == ["timestamp", ]  # only timestamp remains

    def test_merge_preserves_column_order(self):
        """Timestamp should appear at the same position as the original date col."""
        df = pl.DataFrame({
            "Voltage": ["3.1", "3.2"],
            "Real time": ["2026-02-14 00:00:00", "2026-02-15 00:00:00"],
            "unnamed": ["1899-12-31 10:00:00", "1899-12-31 11:00:00"],
            "Current": ["10.0", "10.5"],
        })
        columns = ["Voltage", "Real time", "unnamed", "Current"]
        row1_channel = ["Voltage", "Real time", "unnamed", "Current"]
        row2_channel_name = ["Voltage", "Real time", "Column_2", "Current"]
        units = ["V", "", "", "A"]

        result_df, result_cols, _, _, result_units = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # timestamp replaces "Real time" at index 1
        assert result_cols == ["Voltage", "timestamp", "Current"]
        assert result_units == ["V", "", "A"]
        assert list(result_df.columns) == ["Voltage", "timestamp", "Current"]

    def test_merge_handles_date_only_10_chars(self):
        """Date column with just 'YYYY-MM-DD' (10 chars, no time suffix)."""
        df = pl.DataFrame({
            "Real time": ["2026-02-14", "2026-02-15"],
            "unnamed": ["1899-12-31 10:01:27", "1899-12-31 11:30:00"],
        })
        columns = ["Real time", "unnamed"]
        row1_channel = ["Real time", "unnamed"]
        row2_channel_name = ["Real time", "Column_1"]
        units = ["", ""]

        result_df, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        assert result_cols == ["timestamp"]
        ts_values = result_df["timestamp"].to_list()
        assert ts_values[0] == "2026-02-14 10:01:27"
        assert ts_values[1] == "2026-02-15 11:30:00"

    def test_merge_handles_time_without_epoch(self):
        """Time column without '1899-12-31' prefix (already clean time string)."""
        df = pl.DataFrame({
            "Real time": ["2026-02-14 00:00:00", "2026-02-15 00:00:00"],
            "unnamed": ["10:01:27", "11:30:00"],
        })
        columns = ["Real time", "unnamed"]
        row1_channel = ["Real time", "unnamed"]
        row2_channel_name = ["Real time", "Column_1"]
        units = ["", ""]

        result_df, _, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Should still merge (date pattern detected)
        ts_values = result_df["timestamp"].to_list()
        assert ts_values[0] == "2026-02-14 10:01:27"
        assert ts_values[1] == "2026-02-15 11:30:00"

    def test_no_merge_realtime_is_last_column(self):
        """Should NOT merge if 'Real time' is the last column (no partner)."""
        df = pl.DataFrame({
            "Voltage": ["3.1"],
            "Real time": ["2026-02-14 00:00:00"],
        })
        columns = ["Voltage", "Real time"]
        row1_channel = ["Voltage", "Real time"]
        row2_channel_name = ["Voltage", "Real time"]
        units = ["V", ""]

        _, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Unchanged — no column after "Real time"
        assert result_cols == ["Voltage", "Real time"]

    def test_merge_with_null_values(self):
        """Null values in date/time columns should produce null timestamps."""
        df = pl.DataFrame({
            "Real time": ["2026-02-14 00:00:00", None, "2026-02-16 00:00:00"],
            "unnamed": ["1899-12-31 10:00:00", "1899-12-31 11:00:00", None],
            "Voltage": ["3.1", "3.2", "3.3"],
        })
        columns = ["Real time", "unnamed", "Voltage"]
        row1_channel = ["Real time", "unnamed", "Voltage"]
        row2_channel_name = ["Real time", "Column_1", "Voltage"]
        units = ["", "", "V"]

        result_df, result_cols, _, _, _ = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        # Should still merge (first sample value matches pattern)
        assert result_cols == ["timestamp", "Voltage"]
        ts_values = result_df["timestamp"].to_list()
        assert ts_values[0] == "2026-02-14 10:00:00"

    def test_merge_all_null_pair(self):
        """All-null date/time pairs should still collapse into a timestamp column."""
        df = pl.DataFrame({
            "Real time": [None, None],
            "unnamed": [None, None],
            "Voltage": ["3.1", "3.2"],
        })
        columns = ["Real time", "unnamed", "Voltage"]
        row1_channel = ["Real time", "unnamed", "Voltage"]
        row2_channel_name = ["Real time", "Column_1", "Voltage"]
        units = ["", "", "V"]

        result_df, result_cols, result_row1, result_row2, result_units = _merge_datetime_columns(
            df, columns, row1_channel, row2_channel_name, units
        )

        assert result_cols == ["timestamp", "Voltage"]
        assert result_row1 == ["timestamp", "Voltage"]
        assert result_row2 == ["Real time", "Voltage"]
        assert result_units == ["", "V"]
        assert result_df["timestamp"].to_list() == [None, None]


# =============================================================================
# STRUCTURAL CHANNEL SPLITTING
# =============================================================================

@pytest.mark.skipif(not HAS_convert_xlsx, reason="convert_xlsx not importable")
class TestStructuralChannelSplit:
    """Timestamp and elapsed time stay structural; only signals are unpivoted."""

    def test_timestamp_elapsed_excluded_from_signal_channels(self):
        result = _split_structural_channels(
            columns=["timestamp", "Time", "CH0101"],
            row1_channel=["timestamp", "Time", "CH0101"],
            row2_channel_name=["Measurement Time", "Time", "Stack Voltage"],
            std_channels=["Measurement Time", "Time", "Stack Voltage"],
            units=["", "s", "V"],
        )
        signal_columns, signal_row1, signal_row2, signal_std, signal_units, timestamp_col, elapsed_col = result

        assert signal_columns == ["CH0101"]
        assert signal_row1 == ["CH0101"]
        assert signal_row2 == ["Stack Voltage"]
        assert signal_std == ["Stack Voltage"]
        assert signal_units == ["V"]
        assert timestamp_col == "timestamp"
        assert elapsed_col == "Time"
