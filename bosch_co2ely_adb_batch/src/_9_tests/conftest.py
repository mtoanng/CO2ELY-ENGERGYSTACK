"""Shared pytest fixtures for CO2 ELY pipeline tests.

Usage:
    pytest src/_9_tests/ -v
    pytest src/_9_tests/unit/ -v --no-header
    pytest src/_9_tests/unit/test_common.py -v

Path setup:
    Fixtures handle sys.path insertion for _5_common, _0_convert, _2_b2s.
    Individual test files do NOT need sys.path manipulation.
"""

import sys
import os
import io
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Path setup (ensures imports work regardless of where pytest is invoked)
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent  # src/
sys.path.insert(0, str(_REPO_ROOT / "_5_common"))
sys.path.insert(0, str(_REPO_ROOT / "_0_convert"))
sys.path.insert(0, str(_REPO_ROOT / "_2_b2s"))


# ---------------------------------------------------------------------------
# Mock SparkSession (for unit tests that don't need a real cluster)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    """Mock SparkSession that returns configurable workspace URL.

    Usage:
        def test_env_detection(mock_spark):
            mock_spark.conf.get.return_value = "adb-1032635496032522.2.azuredatabricks.net"
            env = env_variables(mock_spark)
            assert env["environment"] == "dev"
    """
    spark = MagicMock()
    spark.conf.get.return_value = "adb-1032635496032522.2.azuredatabricks.net"
    return spark


@pytest.fixture
def mock_spark_qa():
    """Mock SparkSession for QA environment."""
    spark = MagicMock()
    spark.conf.get.return_value = "adb-7376334951991000.0.azuredatabricks.net"
    return spark


@pytest.fixture
def mock_spark_prod():
    """Mock SparkSession for prod environment."""
    spark = MagicMock()
    spark.conf.get.return_value = "adb-5407587042408609.9.azuredatabricks.net"
    return spark


@pytest.fixture
def mock_spark_local():
    """Mock SparkSession where workspace URL is not available."""
    spark = MagicMock()
    spark.conf.get.side_effect = Exception("Not in Databricks")
    return spark


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_xlsx_bytes():
    """Minimal valid XLSX file bytes for testing converters.

    Creates a simple workbook with:
    - Sheet "TestSheet" with 3 header rows + 5 data rows
    - 4 channels: Time, Voltage, Current, Temperature
    """
    try:
        import polars as pl

        # 3 header rows: channel, channel_name, unit
        # Polars writes xlsx via xlsxwriter
        df = pl.DataFrame({
            "Time": [0.0, 1.0, 2.0, 3.0, 4.0],
            "Voltage": [3.1, 3.2, 3.3, 3.4, 3.5],
            "Current": [10.0, 10.5, 11.0, 10.8, 10.2],
            "Temperature": [25.0, 25.1, 25.3, 25.2, 25.0],
        })
        buf = io.BytesIO()
        df.write_excel(buf, worksheet="TestSheet")
        buf.seek(0)
        return buf.read()
    except ImportError:
        pytest.skip("polars not installed (required for xlsx fixture)")


@pytest.fixture
def sample_csv_bytes():
    """Minimal valid CSV file bytes for testing converters.

    Format:
        Row 1: channel identifiers (header)
        Row 2: channel display names
        Row 3: units (V, A, degC)
        Rows 4+: data
    """
    csv_content = (
        "time_s,voltage,current,temp\n"
        "Elapsed Time,Stack Voltage,Cell Current,Temperature\n"
        "s,V,A,degC\n"
        "0.0,3.1,10.0,25.0\n"
        "1.0,3.2,10.5,25.1\n"
        "2.0,3.3,11.0,25.3\n"
        "3.0,3.4,10.8,25.2\n"
        "4.0,3.5,10.2,25.0\n"
    )
    return csv_content.encode("utf-8")


@pytest.fixture
def sample_blob_info():
    """Sample BlobInfo object for testing tracker/listing."""
    from converter_utils import BlobInfo

    return BlobInfo(
        blob_path="raw_data/test/PoC_Stack_II/measurement_001.xlsx",
        relative_path="test/PoC_Stack_II/measurement_001.xlsx",
        file_name="measurement_001.xlsx",
        file_size=524288,
        last_modified=datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc),
        extension=".xlsx",
    )


@pytest.fixture
def tmp_parquet_dir(tmp_path):
    """Temporary directory with sample Parquet files for bronze ingestion tests."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Create sample Parquet for each table type
    tables = {
        "filemeta": pa.table({
            "uuid": ["uuid-001", "uuid-002"],
            "file_path": ["path/a.xlsx", "path/b.xlsx"],
            "raw_file_name": ["a.xlsx", "b.xlsx"],
            "file_size": [1024, 2048],
            "last_modified": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2,
            "ingested_timestamp": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2,
        }),
        "channel": pa.table({
            "uuid": ["uuid-001", "uuid-001"],
            "group": ["Sheet1", "Sheet1"],
            "channel": ["ch1", "ch2"],
            "channel_name": ["Voltage", "Current"],
            "unit": ["V", "A"],
            "column_index": [0, 1],
        }),
        "timeseries": pa.table({
            "uuid": ["uuid-001"] * 4,
            "group": ["Sheet1"] * 4,
            "sample_offset": [0, 0, 1, 1],
            "channel": ["ch1", "ch2", "ch1", "ch2"],
            "value": [3.1, 10.0, 3.2, 10.5],
            "value_str": [None, None, None, None],
        }),
        "statistics": pa.table({
            "uuid": ["uuid-001"],
            "group": ["Sheet1"],
            "n_channels": [2],
            "n_rows": [2],
            "n_timeseries_rows": [4],
        }),
    }

    for table_type, table in tables.items():
        out_dir = tmp_path / table_type
        out_dir.mkdir()
        pq.write_table(table, str(out_dir / "test_file.parquet"))

    return tmp_path
