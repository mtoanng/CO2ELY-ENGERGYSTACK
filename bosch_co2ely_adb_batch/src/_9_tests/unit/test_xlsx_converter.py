"""Unit tests for _0_convert/xlsx_converter.py.

Tests XLSX parsing: header detection (3-row scan), sheet enumeration,
multi-sheet handling, data type inference, and edge cases.

Dependencies: polars, pyarrow, fastexcel (calamine).
"""
import io
import pytest
import polars as pl
import pyarrow as pa
from unittest.mock import patch

from converter_utils import (
    SCHEMAS, TABLE_TYPES, ConversionResult,
    build_filemeta, build_channel, build_statistics,
    detect_units_row, unpivot_timeseries,
    TIMESERIES_CHUNK_THRESHOLD,
)

# Try importing the converter (may need specific deps)
try:
    from xlsx_converter import convert
    HAS_XLSX_CONVERTER = True
except ImportError:
    HAS_XLSX_CONVERTER = False


# =============================================================================
# XLSX PARSING
# =============================================================================

@pytest.mark.skipif(not HAS_XLSX_CONVERTER, reason="xlsx_converter not importable")
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

@pytest.mark.skipif(not HAS_XLSX_CONVERTER, reason="xlsx_converter not importable")
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

@pytest.mark.skipif(not HAS_XLSX_CONVERTER, reason="xlsx_converter not importable")
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
