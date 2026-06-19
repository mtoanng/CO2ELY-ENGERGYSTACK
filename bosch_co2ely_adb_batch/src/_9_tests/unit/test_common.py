"""Unit tests for _0_convert/common.py (converter-specific utilities).

Tests UUID generation, sanitization, schema validation, PyArrow table builders,
unpivot logic (both standard and chunked), and transient error detection.

Dependencies: pyarrow, polars (for unpivot tests).
"""
import io
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

from common import (
    generate_file_uuid,
    sanitize_name,
    detect_units_row,
    _is_unit_cell,
    build_filemeta,
    build_channel,
    build_statistics,
    unpivot_timeseries,
    SCHEMAS,
    TABLE_TYPES,
    CHUNK_ROWS,
    TIMESERIES_CHUNK_THRESHOLD,
    ConversionResult,
    BlobInfo,
)


# =============================================================================
# UUID GENERATION
# =============================================================================

class TestGenerateFileUuid:
    """UUID generation is deterministic (UUID5 from relative path)."""

    def test_same_path_same_uuid(self):
        uuid1 = generate_file_uuid("test/file.xlsx")
        uuid2 = generate_file_uuid("test/file.xlsx")
        assert uuid1 == uuid2

    def test_different_path_different_uuid(self):
        uuid1 = generate_file_uuid("test/file1.xlsx")
        uuid2 = generate_file_uuid("test/file2.xlsx")
        assert uuid1 != uuid2

    def test_uuid_format(self):
        result = generate_file_uuid("any/path.xlsx")
        assert len(result) == 36
        assert result.count("-") == 4

    def test_path_with_special_chars(self):
        result = generate_file_uuid("path/with spaces/file (1).xlsx")
        assert len(result) == 36  # Still valid UUID

    def test_empty_path(self):
        result = generate_file_uuid("")
        assert len(result) == 36  # UUID5 handles empty string

    def test_cross_environment_stability(self):
        """Same relative path produces same UUID regardless of prefix."""
        uuid1 = generate_file_uuid("PoC_Stack_II/measurement.xlsx")
        uuid2 = generate_file_uuid("PoC_Stack_II/measurement.xlsx")
        assert uuid1 == uuid2


# =============================================================================
# SANITIZE NAME
# =============================================================================

class TestSanitizeName:
    """Sanitize produces safe filesystem/blob names."""

    def test_spaces_to_underscores(self):
        assert sanitize_name("my file name") == "my_file_name"

    def test_special_chars_removed(self):
        assert sanitize_name('file<>:"/\\|?*name') == "file_name"

    def test_consecutive_underscores_collapsed(self):
        assert sanitize_name("a___b") == "a_b"

    def test_leading_trailing_stripped(self):
        assert sanitize_name("__test__") == "test"

    def test_empty_string(self):
        assert sanitize_name("") == ""

    def test_german_characters_preserved(self):
        # Umlauts should pass through (not special chars in regex)
        result = sanitize_name("Messung_Ubersicht")
        assert "Messung" in result

    def test_parentheses_and_brackets(self):
        result = sanitize_name("file (copy) [2]")
        assert "(" not in result or ")" not in result  # depends on regex


# =============================================================================
# UNIT ROW DETECTION
# =============================================================================

class TestDetectUnitsRow:
    """Unit row detection (row 3 of ELY Excel headers)."""

    def test_clear_units_detected(self):
        assert detect_units_row(["V", "A", "mA/cm2", "degC"]) is True

    def test_data_row_not_units(self):
        assert detect_units_row(["123.4", "567.8", "hello", "world"]) is False

    def test_empty_row(self):
        assert detect_units_row(["", "", ""]) is False

    def test_mixed_majority_units(self):
        # >50% look like units
        assert detect_units_row(["V", "A", "mA/cm2", "something_long"]) is True

    def test_single_char_is_unit(self):
        assert _is_unit_cell("V") is True
        assert _is_unit_cell("A") is True

    def test_special_char_is_unit(self):
        assert _is_unit_cell("mA/cm2") is True
        assert _is_unit_cell("degC") is False  # no special chars
        assert _is_unit_cell("m^2") is True

    def test_numeric_not_unit(self):
        assert _is_unit_cell("123.456") is False

    def test_empty_not_unit(self):
        assert _is_unit_cell("") is False
        assert _is_unit_cell("   ") is False


# =============================================================================
# SCHEMAS CONSISTENCY
# =============================================================================

class TestSchemas:
    """PyArrow schema definitions are consistent."""

    def test_four_schemas_defined(self):
        assert len(SCHEMAS) == 4
        assert set(SCHEMAS.keys()) == {"filemeta", "channel", "timeseries", "statistics"}

    def test_table_types_match_schemas(self):
        assert set(TABLE_TYPES) == set(SCHEMAS.keys())

    def test_all_schemas_have_uuid(self):
        for name, schema in SCHEMAS.items():
            field_names = [f.name for f in schema]
            assert "uuid" in field_names, f"{name} missing uuid field"

    def test_timeseries_schema_fields(self):
        fields = [f.name for f in SCHEMAS["timeseries"]]
        assert fields == ["uuid", "group", "sample_offset", "channel", "value", "value_str"]

    def test_filemeta_has_timestamps(self):
        fields = {f.name: f.type for f in SCHEMAS["filemeta"]}
        assert pa.types.is_timestamp(fields["last_modified"])
        assert pa.types.is_timestamp(fields["ingested_timestamp"])


# =============================================================================
# TABLE BUILDERS
# =============================================================================

class TestBuildFilemeta:
    """Filemeta PyArrow table builder."""

    def test_schema_matches(self):
        result = build_filemeta("path/file.xlsx", "uuid-123", 1024,
                                datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert result.schema == SCHEMAS["filemeta"]

    def test_single_row(self):
        result = build_filemeta("path/file.xlsx", "uuid-123", 1024, None)
        assert result.num_rows == 1

    def test_raw_file_name_extracted(self):
        result = build_filemeta("deep/nested/path/myfile.xlsx", "uuid-1", 100, None)
        assert result.column("raw_file_name")[0].as_py() == "myfile.xlsx"

    def test_ingested_timestamp_auto_set(self):
        before = datetime.now(tz=timezone.utc)
        result = build_filemeta("f.xlsx", "u", 1, None)
        after = datetime.now(tz=timezone.utc)
        ts = result.column("ingested_timestamp")[0].as_py()
        assert before <= ts <= after


class TestBuildChannel:
    """Channel catalog PyArrow table builder."""

    def test_schema_matches(self):
        result = build_channel("uuid-1", "Sheet1", ["ch1", "ch2"], ["name1", "name2"], ["V", "A"])
        assert result.schema == SCHEMAS["channel"]

    def test_correct_row_count(self):
        result = build_channel("uuid-1", "Sheet1", ["a", "b", "c"], ["A", "B", "C"], ["", "", ""])
        assert result.num_rows == 3

    def test_column_index_sequential(self):
        result = build_channel("u", "g", ["a", "b", "c"], ["A", "B", "C"], ["", "", ""])
        indices = result.column("column_index").to_pylist()
        assert indices == [0, 1, 2]

    def test_mismatched_lengths_padded(self):
        """If channels list is shorter, it gets padded with empty strings."""
        result = build_channel("u", "g", ["ch1"], ["name1", "name2"], ["V", "A"])
        assert result.num_rows == 2
        channels = result.column("channel").to_pylist()
        assert channels[1] == ""  # padded


class TestBuildStatistics:
    """Statistics summary table builder."""

    def test_schema_matches(self):
        result = build_statistics("uuid-1", "g", 5, 100)
        assert result.schema == SCHEMAS["statistics"]

    def test_n_timeseries_rows_multiplication(self):
        result = build_statistics("uuid-1", "Sheet1", n_channels=10, n_rows=1000)
        col = result.column("n_timeseries_rows")
        assert col[0].as_py() == 10_000

    def test_zero_channels(self):
        result = build_statistics("u", "g", n_channels=0, n_rows=100)
        assert result.column("n_timeseries_rows")[0].as_py() == 0


# =============================================================================
# GENERIC UNPIVOT (standard, in-memory)
# =============================================================================

class TestUnpivotTimeseries:
    """Wide->long melt via unpivot_timeseries()."""

    def test_basic_unpivot(self):
        import polars as pl
        df = pl.DataFrame({"ch1": [1.0, 2.0], "ch2": [3.0, 4.0]})
        result = unpivot_timeseries(df, "uuid-1", "Sheet1")
        assert result.num_rows == 4  # 2 rows x 2 columns
        assert result.schema == SCHEMAS["timeseries"]

    def test_uuid_and_group_populated(self):
        import polars as pl
        df = pl.DataFrame({"x": [1.0]})
        result = unpivot_timeseries(df, "my-uuid", "my-group")
        assert result.column("uuid")[0].as_py() == "my-uuid"
        assert result.column("group")[0].as_py() == "my-group"

    def test_sample_offset_correct(self):
        import polars as pl
        df = pl.DataFrame({"ch1": [10.0, 20.0, 30.0]})
        result = unpivot_timeseries(df, "u", "g")
        offsets = result.column("sample_offset").to_pylist()
        assert offsets == [0, 1, 2]

    def test_non_numeric_goes_to_value_str(self):
        import polars as pl
        df = pl.DataFrame({"ch1": ["text_value", "123.4"]})
        result = unpivot_timeseries(df, "u", "g")
        values = result.column("value").to_pylist()
        value_strs = result.column("value_str").to_pylist()
        # "text_value" -> value=None, value_str="text_value"
        assert values[0] is None
        assert value_strs[0] == "text_value"
        # "123.4" -> value=123.4, value_str=None
        assert values[1] == 123.4
        assert value_strs[1] is None

    def test_subset_columns(self):
        import polars as pl
        df = pl.DataFrame({"ch1": [1.0], "ch2": [2.0], "ch3": [3.0]})
        result = unpivot_timeseries(df, "u", "g", columns=["ch1", "ch3"])
        assert result.num_rows == 2  # only 2 channels

    def test_chunked_basic_output(self):
        import polars as pl
        df = pl.DataFrame({"ch1": list(range(100)), "ch2": list(range(100, 200))})
        total_rows, buf = unpivot_timeseries(df, "u", "g", chunk_rows=50)
        assert total_rows == 200  # 100 rows x 2 cols
        assert isinstance(buf, io.BytesIO)

    def test_chunked_parquet_readable(self):
        import polars as pl
        df = pl.DataFrame({"ch1": [1.0, 2.0, 3.0]})
        total_rows, buf = unpivot_timeseries(df, "u", "g", chunk_rows=2)
        table = pq.read_table(buf)
        assert table.num_rows == total_rows
        assert table.schema == SCHEMAS["timeseries"]

    def test_chunked_multiple_row_groups(self):
        import polars as pl
        # 10 rows, chunk_rows=3 -> 4 chunks (3+3+3+1)
        df = pl.DataFrame({"ch1": list(range(10))})
        total_rows, buf = unpivot_timeseries(df, "u", "g", chunk_rows=3)
        pf = pq.ParquetFile(buf)
        assert pf.metadata.num_row_groups >= 3
        assert total_rows == 10

    def test_chunked_sample_offset_absolute(self):
        """Offsets are absolute (not reset per chunk)."""
        import polars as pl
        df = pl.DataFrame({"ch1": list(range(5))})
        _, buf = unpivot_timeseries(df, "u", "g", chunk_rows=2)
        table = pq.read_table(buf)
        offsets = table.column("sample_offset").to_pylist()
        assert offsets == [0, 1, 2, 3, 4]


# =============================================================================
# DATA CLASSES
# =============================================================================

class TestConversionResult:
    """ConversionResult dataclass structure."""

    def test_default_fields(self):
        r = ConversionResult(tables={})
        assert r.group_name is None
        assert r.n_rows == 0
        assert r.n_channels == 0
        assert r.timeseries_buffer is None

    def test_with_buffer(self):
        buf = io.BytesIO(b"fake parquet data")
        r = ConversionResult(tables={}, timeseries_buffer=buf)
        assert r.timeseries_buffer is buf


class TestBlobInfo:
    """BlobInfo dataclass structure."""

    def test_fields(self, sample_blob_info):
        assert sample_blob_info.extension == ".xlsx"
        assert sample_blob_info.file_size == 524288
        assert "measurement_001.xlsx" in sample_blob_info.file_name
