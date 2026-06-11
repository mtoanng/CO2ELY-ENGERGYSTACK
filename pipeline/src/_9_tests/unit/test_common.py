"""Unit tests for _0_convert/common.py.

Run locally or on Databricks:
    pytest src/_9_tests/ -v
"""
import pytest
import pyarrow as pa
from datetime import datetime, timezone

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "_0_convert"))

from common import (
    generate_file_uuid, sanitize_name, detect_units_row,
    build_filemeta, build_channel, build_statistics,
    SCHEMAS, TABLE_TYPES,
)


class TestGenerateFileUuid:
    """UUID generation is deterministic and consistent."""

    def test_same_path_same_uuid(self):
        uuid1 = generate_file_uuid("test/file.xlsx")
        uuid2 = generate_file_uuid("test/file.xlsx")
        assert uuid1 == uuid2

    def test_different_path_different_uuid(self):
        uuid1 = generate_file_uuid("test/file1.xlsx")
        uuid2 = generate_file_uuid("test/file2.xlsx")
        assert uuid1 != uuid2

    def test_uuid_format(self):
        uuid = generate_file_uuid("any/path.csv")
        assert len(uuid) == 36  # UUID string format
        assert uuid.count("-") == 4


class TestSanitizeName:
    """Sanitize produces safe filesystem names."""

    def test_spaces_to_underscores(self):
        assert sanitize_name("my file name") == "my_file_name"

    def test_special_chars_removed(self):
        assert sanitize_name('file<>:"/\\|?*name') == "file_name"

    def test_consecutive_underscores_collapsed(self):
        assert sanitize_name("a___b") == "a_b"

    def test_empty_string(self):
        assert sanitize_name("") == ""


class TestDetectUnitsRow:
    """Unit row detection based on special characters."""

    def test_units_detected(self):
        assert detect_units_row(["V", "A", "mA/cm2", "degC"]) is True

    def test_data_row_not_units(self):
        assert detect_units_row(["123.4", "567.8", "hello", "world"]) is False

    def test_empty_row(self):
        assert detect_units_row(["", "", ""]) is False

    def test_mixed_mostly_units(self):
        # >50% look like units
        assert detect_units_row(["V", "A", "mA/cm2", "something"]) is True


class TestBuildFilemeta:
    """Filemeta builder produces correct schema."""

    def test_schema_matches(self):
        result = build_filemeta("path/file.xlsx", "uuid-123", 1024, datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert result.schema == SCHEMAS["filemeta"]

    def test_row_count(self):
        result = build_filemeta("path/file.xlsx", "uuid-123", 1024, None)
        assert result.num_rows == 1


class TestBuildChannel:
    """Channel builder handles various input lengths."""

    def test_schema_matches(self):
        result = build_channel("uuid-1", "Sheet1", ["ch1", "ch2"], ["name1", "name2"], ["V", "A"])
        assert result.schema == SCHEMAS["channel"]

    def test_correct_row_count(self):
        result = build_channel("uuid-1", "Sheet1", ["a", "b", "c"], ["A", "B", "C"], ["", "", ""])
        assert result.num_rows == 3


class TestBuildStatistics:
    """Statistics builder computes n_timeseries_rows correctly."""

    def test_multiplication(self):
        result = build_statistics("uuid-1", "Sheet1", n_channels=10, n_rows=1000)
        col = result.column("n_timeseries_rows")
        assert col[0].as_py() == 10_000

    def test_schema_matches(self):
        result = build_statistics("uuid-1", "g", 5, 100)
        assert result.schema == SCHEMAS["statistics"]


class TestTableTypes:
    """TABLE_TYPES constant is consistent with SCHEMAS."""

    def test_four_tables(self):
        assert len(TABLE_TYPES) == 4

    def test_matches_schema_keys(self):
        assert set(TABLE_TYPES) == set(SCHEMAS.keys())
