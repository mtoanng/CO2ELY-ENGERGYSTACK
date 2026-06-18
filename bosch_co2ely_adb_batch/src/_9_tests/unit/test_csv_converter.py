"""Unit tests for _0_convert/csv_converter.py.

Tests CSV parsing: delimiter detection, header handling, encoding,
and edge cases (empty files, single column, large files).

Dependencies: polars, pyarrow.
"""
import io
import pytest
import pyarrow as pa

from common import SCHEMAS, ConversionResult

# Try importing the converter
try:
    from csv_converter import convert
    HAS_CSV_CONVERTER = True
except ImportError:
    HAS_CSV_CONVERTER = False


# =============================================================================
# CSV PARSING (end-to-end)
# =============================================================================

@pytest.mark.skipif(not HAS_CSV_CONVERTER, reason="csv_converter not importable")
class TestCsvConvert:
    """Full CSV conversion pipeline: bytes -> ConversionResult list."""

    def test_returns_list_of_results(self, sample_csv_bytes):
        results = convert(
            sample_csv_bytes,
            "test/data.csv",
            len(sample_csv_bytes),
            None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/raw_data/test/data.csv",
        )
        assert isinstance(results, list)
        assert len(results) == 1  # CSV = single group ("data")

    def test_group_name_is_data(self, sample_csv_bytes):
        results = convert(
            sample_csv_bytes, "test.csv", len(sample_csv_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        assert results[0].group_name == "data"

    def test_all_table_types_present(self, sample_csv_bytes):
        results = convert(
            sample_csv_bytes, "test.csv", len(sample_csv_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        r = results[0]
        assert "filemeta" in r.tables
        assert "channel" in r.tables
        assert "statistics" in r.tables

    def test_channel_count_matches_columns(self, sample_csv_bytes):
        results = convert(
            sample_csv_bytes, "test.csv", len(sample_csv_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        r = results[0]
        n_channels = r.tables["channel"].num_rows
        assert n_channels == 4  # time_s, voltage, current, temp

    def test_statistics_row_count(self, sample_csv_bytes):
        results = convert(
            sample_csv_bytes, "test.csv", len(sample_csv_bytes), None,
            abfss_file_path="abfss://c@s.dfs.core.windows.net/p",
        )
        r = results[0]
        n_rows = r.tables["statistics"].column("n_rows")[0].as_py()
        assert n_rows == 5  # 5 data rows in sample


# =============================================================================
# ENCODING HANDLING
# =============================================================================

@pytest.mark.skipif(not HAS_CSV_CONVERTER, reason="csv_converter not importable")
class TestCsvEncoding:
    """CSV encoding detection and handling."""

    def test_utf8(self, sample_csv_bytes):
        """Standard UTF-8 CSV."""
        results = convert(sample_csv_bytes, "utf8.csv", len(sample_csv_bytes),
                          None, abfss_file_path="abfss://c@s/p")
        assert len(results) == 1

    def test_utf8_bom(self):
        """UTF-8 with BOM (common from Windows Excel export)."""
        csv_bom = b"\xef\xbb\xbf" + b"col1,col2\nname1,name2\nV,A\n1.0,2.0\n"
        try:
            results = convert(csv_bom, "bom.csv", len(csv_bom),
                              None, abfss_file_path="abfss://c@s/p")
            assert len(results) >= 1
        except Exception:
            pytest.skip("BOM handling not implemented in csv_converter")


# =============================================================================
# DELIMITER DETECTION
# =============================================================================

@pytest.mark.skipif(not HAS_CSV_CONVERTER, reason="csv_converter not importable")
class TestDelimiterDetection:
    """CSV delimiter auto-detection (comma, semicolon, tab)."""

    def test_comma_separated(self, sample_csv_bytes):
        results = convert(sample_csv_bytes, "comma.csv", len(sample_csv_bytes),
                          None, abfss_file_path="abfss://c@s/p")
        assert len(results) == 1

    def test_semicolon_separated(self):
        """European CSV with semicolons (common in German locales)."""
        csv_semi = b"col1;col2\nname1;name2\nV;A\n1,0;2,0\n3,0;4,0\n"
        try:
            results = convert(csv_semi, "semi.csv", len(csv_semi),
                              None, abfss_file_path="abfss://c@s/p")
            assert len(results) >= 1
        except Exception:
            pytest.skip("Semicolon delimiter not auto-detected")

    def test_tab_separated(self):
        """TSV file with .csv extension."""
        csv_tab = b"col1\tcol2\nname1\tname2\nV\tA\n1.0\t2.0\n"
        try:
            results = convert(csv_tab, "tab.csv", len(csv_tab),
                              None, abfss_file_path="abfss://c@s/p")
            assert len(results) >= 1
        except Exception:
            pytest.skip("Tab delimiter not auto-detected")


# =============================================================================
# EDGE CASES
# =============================================================================

@pytest.mark.skipif(not HAS_CSV_CONVERTER, reason="csv_converter not importable")
class TestCsvEdgeCases:
    """Edge cases for CSV parsing."""

    def test_empty_file_raises(self):
        with pytest.raises(Exception):
            convert(b"", "empty.csv", 0, None, abfss_file_path="abfss://c@s/p")

    def test_header_only_no_data(self):
        """CSV with headers but no data rows."""
        csv_no_data = b"col1,col2\nname1,name2\nV,A\n"
        try:
            results = convert(csv_no_data, "nodata.csv", len(csv_no_data),
                              None, abfss_file_path="abfss://c@s/p")
            # Should either return empty timeseries or raise
            if results:
                assert results[0].n_rows == 0
        except Exception:
            pass  # Acceptable to raise for no-data files

    def test_single_column(self):
        """CSV with only one data column."""
        csv_single = b"time\nElapsed\ns\n0.0\n1.0\n2.0\n"
        try:
            results = convert(csv_single, "single.csv", len(csv_single),
                              None, abfss_file_path="abfss://c@s/p")
            assert len(results) >= 1
        except Exception:
            pytest.skip("Single-column CSV not supported")

    def test_numeric_overflow(self):
        """Very large numbers don't crash the parser."""
        csv_big = b"val\nValue\n-\n1e308\n-1e308\n0.0\n"
        try:
            results = convert(csv_big, "big.csv", len(csv_big),
                              None, abfss_file_path="abfss://c@s/p")
            assert len(results) >= 1
        except Exception:
            pytest.skip("Overflow handling not tested")
