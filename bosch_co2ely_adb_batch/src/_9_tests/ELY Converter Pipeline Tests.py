# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install polars[calamine] fastexcel pyarrow --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,ELY Converter Pipeline Tests
# MAGIC %md
# MAGIC # ELY Converter Pipeline — Integration Tests
# MAGIC
# MAGIC Comprehensive test suite for the converter pipeline (`src/_0_convert/`).
# MAGIC
# MAGIC **No Azure credentials required** — all tests use synthetic data, mock SDK clients, and local Delta tables.
# MAGIC
# MAGIC Tests cover: environment detection, UUID generation, 3-row header parsing, CSV/XLSX converters, unpivot logic, Parquet roundtrip, partition math, retry classification, IncrementalTracker, and full mini-pipeline.

# COMMAND ----------

# DBTITLE 1,Setup imports and paths
import sys
import os
import io
import json
import uuid
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Add converter source to path
CONVERTER_SRC = "/Workspace/Users/got4hc@bosch.com/CO2ELY_ENERGYSTACK/bosch_co2ely_adb_batch/src/_0_convert"
sys.path.insert(0, CONVERTER_SRC)

# Import converter modules
from common import (
    ENVIRONMENT_CONFIG, CONVERTER_CONFIG, SCHEMAS, TABLE_TYPES,
    get_env_variables, get_adls_config, build_abfss_path,
    generate_file_uuid, sanitize_name, detect_units_row,
    generic_unpivot, build_filemeta, build_channel, build_statistics,
    ConversionResult, BlobInfo,
)
from xlsx_converter import convert as convert_xlsx, _process_sheet
from csv_converter import convert as convert_csv

# Test results collector
TEST_RESULTS = []

def record(test_name: str, passed: bool, details: str = ""):
    status = "PASS ✓" if passed else "FAIL ✗"
    TEST_RESULTS.append({"test": test_name, "passed": passed, "details": details})
    print(f"  [{status}] {test_name}" + (f" — {details}" if details else ""))

print("Setup complete. Converter source loaded from:")
print(f"  {CONVERTER_SRC}")
print(f"  Modules: common, xlsx_converter, csv_converter")

# COMMAND ----------

# DBTITLE 1,Test 1: Environment detection
print("="*60)
print("TEST 1: Environment Detection")
print("="*60)

# Test known workspace URLs
for url, expected in [
    ("adb-1032635496032522.2.azuredatabricks.net", "dev"),
    ("adb-7376334951991000.0.azuredatabricks.net", "qa"),
    ("adb-5407587042408609.9.azuredatabricks.net", "prod"),
]:
    config = ENVIRONMENT_CONFIG.get(url)
    record(
        f"Env detection: {expected}",
        config is not None and config["environment"] == expected,
        f"URL={url} → env={config['environment'] if config else 'None'}"
    )

# Test ADLS config derivation
env_vars = ENVIRONMENT_CONFIG["adb-1032635496032522.2.azuredatabricks.net"]
adls = get_adls_config(env_vars)
record(
    "ADLS config derivation",
    adls["tracking_table"] == "co2elyd_dev.converter.file_tracking"
    and adls["source_prefix"] == "raw_data"
    and adls["output_prefix"] == "parquet_raw",
    f"tracking={adls['tracking_table']}, source={adls['source_prefix']}"
)

# Test build_abfss_path
path = build_abfss_path("stpsbdodxdev2datalake", "co2elyd-data", "raw_data/test.xlsx")
expected_path = "abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/raw_data/test.xlsx"
record("build_abfss_path", path == expected_path, f"{path[:60]}...")

# COMMAND ----------

# DBTITLE 1,Test 2: UUID determinism
print("\n" + "="*60)
print("TEST 2: UUID Determinism")
print("="*60)

# Same path → same UUID (deterministic)
path1 = "test/PoC Stack II/PoC II Stack Measurement.xlsx"
uuid1 = generate_file_uuid(path1)
uuid2 = generate_file_uuid(path1)
record("Same path → same UUID", uuid1 == uuid2, f"UUID={uuid1[:16]}...")

# Different path → different UUID
path2 = "test/PoC Stack III/PoC III Stack Measurement.xlsx"
uuid3 = generate_file_uuid(path2)
record("Different path → different UUID", uuid1 != uuid3, f"{uuid1[:8]} ≠ {uuid3[:8]}")

# Valid UUID format
try:
    parsed = uuid.UUID(uuid1)
    record("Valid UUID5 format", parsed.version == 5, f"version={parsed.version}")
except ValueError as e:
    record("Valid UUID5 format", False, str(e))

# sanitize_name
record("sanitize_name spaces", sanitize_name("PoC Stack II") == "PoC_Stack_II")
record("sanitize_name special", sanitize_name('file<>:"/test') == "file_test")

# COMMAND ----------

# DBTITLE 1,Test 3: XLSX header parsing (3-row logic)
print("\n" + "="*60)
print("TEST 3: XLSX Header Parsing (3-row logic)")
print("="*60)

# Create synthetic xlsx with: row1=channel IDs, row2=display names, row3=units, then data
header_row1 = ["Time", "Voltage_Cell1", "Current_Cell1", "Temp_Stack"]
header_row2 = ["Time [s]", "Cell Voltage 1", "Cell Current 1", "Stack Temperature"]
units_row = ["s", "V", "mA", "°C"]
data_rows = [
    [0.0, 1.23, 450.5, 65.2],
    [0.1, 1.24, 451.0, 65.3],
    [0.2, 1.22, 449.8, 65.1],
    [0.3, 1.25, 452.1, 65.4],
]

# Build DataFrame with all rows (header + units + data) as strings
all_rows = [header_row1, header_row2, units_row] + data_rows
df_raw = pl.DataFrame(
    {f"col_{i}": [str(row[i]) for row in all_rows] for i in range(4)}
)

# Write to xlsx bytes
buf = io.BytesIO()
df_raw.write_excel(buf, worksheet="TestSheet", has_header=False)
xlsx_bytes = buf.getvalue()
print(f"  Synthetic xlsx: {len(xlsx_bytes)} bytes, 4 channels, 4 data rows")

# Run xlsx converter
results = convert_xlsx(
    xlsx_bytes, "test/synthetic.xlsx", len(xlsx_bytes),
    datetime.now(tz=timezone.utc), abfss_file_path="abfss://test@account.dfs.core.windows.net/test/synthetic.xlsx"
)

record("XLSX produces results", len(results) > 0, f"{len(results)} group(s)")

if results:
    r = results[0]
    # Check filemeta
    filemeta = r.tables["filemeta"]
    record("filemeta has 1 row", filemeta.num_rows == 1)
    record("filemeta.file_path is abfss",
           filemeta.column("file_path")[0].as_py().startswith("abfss://"))

    # Check channel table
    channel = r.tables["channel"]
    record("channel has 4 rows", channel.num_rows == 4, f"got {channel.num_rows}")

    # Check units detected
    units_col = channel.column("unit").to_pylist()
    record("Units detected correctly",
           "V" in units_col and "mA" in units_col and "°C" in units_col,
           f"units={units_col}")

    # Check timeseries (4 data rows * 4 channels = 16 long-format rows)
    ts = r.tables["timeseries"]
    record("timeseries row count", ts.num_rows == 16,
           f"expected 16 (4*4), got {ts.num_rows}")

    # Check statistics
    stats = r.tables["statistics"]
    record("statistics n_channels=4",
           stats.column("n_channels")[0].as_py() == 4)

# COMMAND ----------

# DBTITLE 1,Test 4: CSV converter end-to-end
print("\n" + "="*60)
print("TEST 4: CSV Converter End-to-End")
print("="*60)

# Create synthetic CSV: row1=channel, row2=channel_name, row3=units, then data
csv_content = """Time,Voltage,Current,Pressure
Time [s],Cell Voltage,Cell Current,Gas Pressure
s,V,A,bar
0.0,1.50,10.2,1.013
0.5,1.51,10.3,1.014
1.0,1.49,10.1,1.012
"""
csv_bytes = csv_content.encode("utf-8")
print(f"  Synthetic CSV: {len(csv_bytes)} bytes, 4 channels, 3 data rows")

results = convert_csv(
    csv_bytes, "test/synthetic.csv", len(csv_bytes),
    datetime.now(tz=timezone.utc), abfss_file_path="abfss://container@account.dfs.core.windows.net/test/synthetic.csv"
)

record("CSV produces results", len(results) > 0, f"{len(results)} group(s)")

if results:
    r = results[0]
    # All 4 table types present
    record("CSV has all 4 tables",
           set(r.tables.keys()) == {"filemeta", "channel", "timeseries", "statistics"},
           f"keys={list(r.tables.keys())}")

    # 3 data rows * 4 channels = 12 timeseries rows
    ts = r.tables["timeseries"]
    record("CSV timeseries count", ts.num_rows == 12,
           f"expected 12 (3*4), got {ts.num_rows}")

    # Verify numeric values parsed
    values = ts.column("value").to_pylist()
    non_null = [v for v in values if v is not None]
    record("CSV numeric values parsed", len(non_null) == 12,
           f"{len(non_null)}/12 non-null")

# COMMAND ----------

# DBTITLE 1,Test 5: Unpivot logic
print("\n" + "="*60)
print("TEST 5: Wide-to-Long Unpivot")
print("="*60)

# Create a simple wide DataFrame
wide_df = pl.DataFrame({
    "ch_A": ["1.0", "2.0", "3.0"],
    "ch_B": ["4.0", "5.0", "hello"],  # 'hello' tests value_str
})

result = generic_unpivot(wide_df, "test-uuid", "group1", ["ch_A", "ch_B"])

# Should be 3 rows * 2 channels = 6 rows
record("Unpivot row count", result.num_rows == 6, f"got {result.num_rows}")

# Check schema matches SCHEMAS["timeseries"]
expected_cols = ["uuid", "group", "sample_offset", "channel", "value", "value_str"]
actual_cols = result.column_names
record("Unpivot schema correct", actual_cols == expected_cols,
       f"cols={actual_cols}")

# Check value_str for non-numeric 'hello'
df_result = pl.from_arrow(result)
hello_rows = df_result.filter(pl.col("value_str").is_not_null())
record("value_str captures non-numeric",
       hello_rows.shape[0] == 1 and hello_rows["value_str"][0] == "hello",
       f"value_str rows={hello_rows.shape[0]}")

# Check sample_offset is sequential
offsets = df_result.filter(pl.col("channel") == "ch_A")["sample_offset"].to_list()
record("sample_offset sequential", offsets == [0, 1, 2], f"offsets={offsets}")

# COMMAND ----------

# DBTITLE 1,Test 6: Parquet roundtrip
print("\n" + "="*60)
print("TEST 6: Parquet Write/Read Roundtrip")
print("="*60)

tmp_dir = tempfile.mkdtemp(prefix="ely_test_parquet_")

try:
    file_uuid = generate_file_uuid("test/roundtrip.xlsx")

    # Build all 4 table types
    filemeta = build_filemeta("abfss://test/roundtrip.xlsx", file_uuid, 1024,
                             datetime.now(tz=timezone.utc))
    channel = build_channel(file_uuid, "Sheet1",
                           ["ch_0", "ch_1"], ["Channel 0", "Channel 1"], ["V", "A"])
    stats = build_statistics(file_uuid, "Sheet1", 2, 100)

    wide_df = pl.DataFrame({"ch_0": ["1.0", "2.0"], "ch_1": ["3.0", "4.0"]})
    timeseries = generic_unpivot(wide_df, file_uuid, "Sheet1", ["ch_0", "ch_1"])

    tables = {"filemeta": filemeta, "channel": channel,
              "timeseries": timeseries, "statistics": stats}

    # Write each to Parquet
    for table_type, table in tables.items():
        path = os.path.join(tmp_dir, f"{table_type}.parquet")
        pq.write_table(table, path, compression="zstd")

        # Read back and verify schema
        read_back = pq.read_table(path)
        expected_schema = SCHEMAS[table_type]
        schemas_match = read_back.schema.equals(expected_schema)
        record(f"Parquet roundtrip: {table_type}",
               schemas_match and read_back.num_rows == table.num_rows,
               f"rows={read_back.num_rows}, schema_match={schemas_match}")
finally:
    shutil.rmtree(tmp_dir, ignore_errors=True)

# COMMAND ----------

# DBTITLE 1,Test 7: Partition calculation
print("\n" + "="*60)
print("TEST 7: Partition Calculation")
print("="*60)

import math

def calc_partitions(n_files, files_per_partition):
    return max(1, math.ceil(n_files / files_per_partition))

test_cases = [
    (1, 8, 1),    # 1 file → 1 partition
    (4, 8, 1),    # 4 files < 8 → still 1 partition
    (8, 8, 1),    # exactly 8 → 1 partition
    (9, 8, 2),    # 9 files → 2 partitions
    (48, 8, 6),   # 48 → 6 partitions
    (96, 8, 12),  # 96 → 12 partitions
    (100, 8, 13), # 100 → 13 partitions
    (0, 8, 1),    # edge case: max(1, 0) = 1
]

for n_files, fpp, expected in test_cases:
    actual = calc_partitions(n_files, fpp)
    record(f"Partitions: {n_files} files / {fpp} FPP = {expected}",
           actual == expected, f"got {actual}")

# Verify concurrent files formula
spark_task_cpus = 4
threads_per_partition = 2
worker_cores = 16
slots_per_worker = worker_cores // spark_task_cpus
files_concurrent_per_worker = slots_per_worker * threads_per_partition
record("Concurrent formula: 16cores/4cpus*2threads=8",
       files_concurrent_per_worker == 8)

# COMMAND ----------

# DBTITLE 1,Test 8: Retry classification
print("\n" + "="*60)
print("TEST 8: Retry Error Classification")
print("="*60)

sys.path.insert(0, CONVERTER_SRC)
from run_converter import _is_transient_error, TRANSIENT_ERRORS

# Transient errors (should retry)
class ConnectionError(Exception): pass
class TimeoutError(Exception): pass
class ServiceRequestError(Exception): pass

record("ConnectionError → transient", _is_transient_error(ConnectionError("reset")))
record("TimeoutError → transient", _is_transient_error(TimeoutError("timed out")))

# HTTP 429 in message
generic_429 = Exception("HTTP 429 Too Many Requests: throttled")
record("HTTP 429 in message → transient", _is_transient_error(generic_429))

# HTTP 500 in message
generic_500 = Exception("Server returned 500 Internal Server Error")
record("HTTP 500 in message → transient", _is_transient_error(generic_500))

# Permanent errors (should NOT retry)
record("ValueError → permanent", not _is_transient_error(ValueError("corrupt file")))
record("KeyError → permanent", not _is_transient_error(KeyError("missing column")))
record("TypeError → permanent", not _is_transient_error(TypeError("bad type")))

# Cause chain detection
class HttpResponseError(Exception): pass
wrapped = Exception("wrapper")
wrapped.__cause__ = HttpResponseError("throttled")
record("Wrapped HttpResponseError → transient", _is_transient_error(wrapped))

# COMMAND ----------

# DBTITLE 1,Test 9: IncrementalTracker with local Delta
print("\n" + "="*60)
print("TEST 9: IncrementalTracker (Local Delta Table)")
print("="*60)

from common import IncrementalTracker
from pyspark.sql import SparkSession, Row as SparkRow
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# Use a temp catalog/schema-qualified name (or just a path-based Delta)
test_tracking_table = "default.ely_test_file_tracking"

# Clean up from previous runs
spark.sql(f"DROP TABLE IF EXISTS {test_tracking_table}")

# Create tracker (should auto-create table)
tracker = IncrementalTracker(test_tracking_table, spark)

# Verify table exists
table_exists = spark.catalog.tableExists(test_tracking_table)
record("Tracker creates table", table_exists)

# Verify schema has retry_count
cols = [f.name for f in spark.table(test_tracking_table).schema.fields]
record("Table has retry_count column", "retry_count" in cols, f"cols={cols[:5]}...")

# Test watermark (should be None on empty table)
wm = tracker._get_watermark()
record("Empty table → watermark is None", wm is None)

# Simulate batch_merge_results with mock result DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

result_schema = StructType([
    StructField("blob_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("file_uuid", StringType(), True),
    StructField("last_modified", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("output_paths", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
])

now = datetime.now(tz=timezone.utc)
mock_results = spark.createDataFrame([
    SparkRow(blob_path="test/file_a.xlsx", file_name="file_a.xlsx", file_size=1000,
             file_uuid="uuid-a", last_modified=now, status="SUCCESS",
             output_paths='{"timeseries": "ts/file_a.parquet"}',
             error_message=None, duration_seconds=5.0),
    SparkRow(blob_path="test/file_b.csv", file_name="file_b.csv", file_size=500,
             file_uuid="uuid-b", last_modified=now, status="FAILED",
             output_paths=None,
             error_message="Parse error: corrupt", duration_seconds=1.0),
], schema=result_schema)

tracker.batch_merge_results(mock_results)

# Verify merge results
tracking_df = spark.table(test_tracking_table)
record("Merge inserted 2 rows", tracking_df.count() == 2)

success_row = tracking_df.filter("status = 'SUCCESS'").collect()[0]
record("SUCCESS retry_count = 0", success_row.retry_count == 0)

failed_row = tracking_df.filter("status = 'FAILED'").collect()[0]
record("FAILED retry_count = 1", failed_row.retry_count == 1)

# Run merge again (same FAILED file) → retry_count should increment
tracker.batch_merge_results(mock_results.filter("status = 'FAILED'"))
failed_row2 = spark.table(test_tracking_table).filter("status = 'FAILED'").collect()[0]
record("Re-merge FAILED → retry_count = 2", failed_row2.retry_count == 2)

# Watermark should now be set
wm2 = tracker._get_watermark()
record("Watermark updated after SUCCESS", wm2 is not None)

# Cleanup
spark.sql(f"DROP TABLE IF EXISTS {test_tracking_table}")

# COMMAND ----------

# DBTITLE 1,Test 10: Full mapPartitions pipeline (mock Azure SDK)
print("\n" + "="*60)
print("TEST 10: Full mapPartitions Pipeline (Spark + ThreadPool + Mock Azure)")
print("="*60)
print("  This test exercises the ACTUAL Spark mapPartitions code path:")
print("  DataFrame → repartition → mapPartitions(_process_partition) → collect")
print("  Azure SDK is mocked at module level (works in local/single-node mode)")
print()

import math
import logging
import importlib
from pyspark.sql import SparkSession, Row as SparkRow
from unittest.mock import MagicMock, patch, call
from run_converter import (
    _process_single_file, _process_partition,
    RESULT_SCHEMA, FILES_PER_PARTITION, THREADS_PER_PARTITION,
)
import common as common_module

spark = SparkSession.builder.getOrCreate()

# =============================================================================
# 1. CREATE SYNTHETIC FILES (8 files → tests 2 partitions with FPP=4)
# =============================================================================
N_TEST_FILES = 8
TEST_FPP = 4  # files per partition for this test → 2 partitions

def make_xlsx_bytes(n_rows=10, n_cols=3, sheet_name="Data"):
    """Create synthetic xlsx: row1=channel IDs, row2=names, row3=units, then data."""
    data = {f"col_{i}": [f"Ch{i}"] + [f"Channel {i}"] + ["V"] + [str(float(j)) for j in range(n_rows)]
            for i in range(n_cols)}
    df = pl.DataFrame(data)
    buf = io.BytesIO()
    df.write_excel(buf, worksheet=sheet_name, has_header=False)
    return buf.getvalue()

def make_csv_bytes(n_rows=10, n_cols=3):
    """Create synthetic CSV: row1=channel, row2=name, row3=units, then data."""
    lines = []
    lines.append(",".join([f"Ch{i}" for i in range(n_cols)]))
    lines.append(",".join([f"Channel {i}" for i in range(n_cols)]))
    lines.append(",".join(["V"] * n_cols))
    for j in range(n_rows):
        lines.append(",".join([str(float(j + i * 0.1)) for i in range(n_cols)]))
    return "\n".join(lines).encode("utf-8")

# Create 8 files: 4 xlsx + 4 csv (varied sizes to simulate real workload)
test_file_registry = {
    "raw_data/exp1/data_a.xlsx": make_xlsx_bytes(20, 4),
    "raw_data/exp1/data_b.xlsx": make_xlsx_bytes(15, 3),
    "raw_data/exp2/data_c.xlsx": make_xlsx_bytes(30, 5),
    "raw_data/exp2/data_d.xlsx": make_xlsx_bytes(10, 2),
    "raw_data/exp3/readings_1.csv": make_csv_bytes(25, 5),
    "raw_data/exp3/readings_2.csv": make_csv_bytes(20, 4),
    "raw_data/exp4/log_a.csv": make_csv_bytes(10, 2),
    "raw_data/exp4/log_b.csv": make_csv_bytes(15, 3),
}

print(f"  Created {len(test_file_registry)} synthetic files:")
for name, data in test_file_registry.items():
    print(f"    {name}: {len(data):,} bytes")

# =============================================================================
# 2. MOCK AZURE SDK (module-level patch for _process_partition lazy imports)
# =============================================================================

class MockBlobDownload:
    def __init__(self, data): self._data = data
    def readall(self): return self._data

class MockBlobClient:
    def __init__(self, data): self._data = data
    def download_blob(self, max_concurrency=1):
        return MockBlobDownload(self._data)

class MockContainerClient:
    """Returns pre-created bytes based on blob_path lookup."""
    def __init__(self, file_map):
        self._files = file_map
        self.download_count = 0
    def get_blob_client(self, blob_path):
        if blob_path in self._files:
            self.download_count += 1
            return MockBlobClient(self._files[blob_path])
        raise FileNotFoundError(f"Mock: {blob_path} not found")

class MockBlobServiceClient:
    """Mock BlobServiceClient that returns MockContainerClient."""
    def __init__(self, file_map):
        self._container = MockContainerClient(file_map)
    def get_container_client(self, container_name):
        return self._container

# ParquetWriter mock → writes to local temp dir instead of Azure
tmp_output = tempfile.mkdtemp(prefix="ely_mappartitions_test_")
parquet_write_log = []  # track what was written

class MockParquetWriter:
    """Writes Parquet to local filesystem (mocks Azure upload)."""
    def __init__(self, storage_account, container, output_prefix):
        self.output_dir = tmp_output
        self.storage_account = storage_account

    def write_result(self, result, base_filename):
        output_paths = {}
        group_suffix = f"_{sanitize_name(result.group_name)}" if result.group_name else ""
        for table_type, table in result.tables.items():
            if table.num_rows == 0 and table_type not in ("filemeta", "statistics"):
                continue
            filename = f"{base_filename}{group_suffix}_{table_type}.parquet"
            path = os.path.join(self.output_dir, table_type, filename)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pq.write_table(table, path, compression="zstd")
            output_paths[table_type] = f"{table_type}/{filename}"
            parquet_write_log.append(path)
        return output_paths

# =============================================================================
# 3. BUILD SPARK DATAFRAME (same schema as real main() produces)
# =============================================================================

file_rows = []
for blob_path in test_file_registry:
    ext = os.path.splitext(blob_path)[1].lower()
    relative_path = blob_path[len("raw_data/"):]
    file_rows.append(SparkRow(
        blob_path=blob_path,
        relative_path=relative_path,
        file_name=os.path.basename(blob_path),
        file_size=len(test_file_registry[blob_path]),
        last_modified=datetime.now(tz=timezone.utc),
        extension=ext,
        storage_account="mockaccount",
        container="mockcontainer",
        output_prefix=tmp_output,
    ))

input_df = spark.createDataFrame(file_rows)

# Repartition using the SAME formula as real pipeline
num_partitions = max(1, math.ceil(len(file_rows) / TEST_FPP))
expected_partitions = 2  # 8 files / 4 FPP = 2
print(f"\n  Partitioning: {len(file_rows)} files / {TEST_FPP} FPP = {num_partitions} partitions")

distributed_df = input_df.repartition(num_partitions)
actual_partitions = distributed_df.rdd.getNumPartitions()
record("Partition count correct", actual_partitions == expected_partitions,
       f"expected={expected_partitions}, actual={actual_partitions}")

# Verify partition distribution (files spread across partitions)
partition_sizes = distributed_df.rdd.mapPartitions(
    lambda it: [sum(1 for _ in it)]
).collect()
print(f"  Partition sizes: {partition_sizes} (sum={sum(partition_sizes)})")
record("All files distributed", sum(partition_sizes) == N_TEST_FILES,
       f"sum={sum(partition_sizes)}")
record("No empty partitions", all(s > 0 for s in partition_sizes),
       f"sizes={partition_sizes}")

# =============================================================================
# 4. RUN mapPartitions WITH MOCKED AZURE SDK
#    _process_partition does: from common import get_blob_service_client, ParquetWriter
#    Patching at module level BEFORE the action triggers the function.
# =============================================================================

print("\n  Executing mapPartitions with mocked Azure SDK...")

mock_sdk = MockBlobServiceClient(test_file_registry)

# Patch at the module level so _process_partition's lazy import picks up mocks
with patch.object(common_module, 'get_blob_service_client', return_value=mock_sdk):
    with patch.object(common_module, 'ParquetWriter', MockParquetWriter):
        # This is the EXACT same call as main() line 490:
        results_rdd = distributed_df.rdd.mapPartitions(_process_partition)
        results_df = spark.createDataFrame(results_rdd, schema=RESULT_SCHEMA)

        # Force execution (this triggers the actual processing)
        results_df.cache()
        total_count = results_df.count()

print(f"  mapPartitions complete: {total_count} results collected")

# =============================================================================
# 5. VERIFY RESULTS (same checks the real main() does)
# =============================================================================

record("mapPartitions processed all files", total_count == N_TEST_FILES,
       f"expected={N_TEST_FILES}, got={total_count}")

success_count = results_df.filter("status = 'SUCCESS'").count()
failed_count = results_df.filter("status = 'FAILED'").count()
record("All files succeeded via mapPartitions", success_count == N_TEST_FILES,
       f"{success_count} SUCCESS, {failed_count} FAILED")

# Show failures if any
if failed_count > 0:
    print("\n  FAILED FILES:")
    for row in results_df.filter("status = 'FAILED'").select("blob_path", "error_message").collect():
        print(f"    ✗ {row.blob_path}: {row.error_message[:100]}")

# Verify result schema matches RESULT_SCHEMA
actual_fields = set(results_df.columns)
expected_fields = set(f.name for f in RESULT_SCHEMA.fields)
record("Result schema matches RESULT_SCHEMA",
       actual_fields == expected_fields,
       f"actual={sorted(actual_fields)}")

# Verify output_paths JSON is valid and contains expected tables
result_rows = results_df.filter("status = 'SUCCESS'").collect()
for row in result_rows[:3]:  # check first 3 to avoid spam
    paths = json.loads(row.output_paths)
    has_required = "timeseries" in paths and "filemeta" in paths
    record(f"output_paths valid: {row.file_name}",
           has_required, f"tables={list(paths.keys())}")

# Verify file_uuid is set for all successes
uuids = [row.file_uuid for row in result_rows]
record("All UUIDs populated", all(u is not None for u in uuids),
       f"{sum(1 for u in uuids if u)}/{len(uuids)} non-null")
record("All UUIDs unique", len(set(uuids)) == len(uuids),
       f"{len(set(uuids))} unique / {len(uuids)} total")

# Verify Parquet files actually written to local filesystem
parquet_files = list(Path(tmp_output).rglob("*.parquet"))
record("Parquet files written to disk", len(parquet_files) > 0,
       f"{len(parquet_files)} parquet files")

# Verify all 4 table types present in output
table_types_written = set(p.parent.name for p in parquet_files)
expected_types = {"filemeta", "channel", "timeseries", "statistics"}
record("All 4 table types in output",
       table_types_written == expected_types,
       f"types={sorted(table_types_written)}")

# Verify duration_seconds is reasonable (synthetic files should be fast)
durations = [row.duration_seconds for row in result_rows]
avg_duration = sum(durations) / len(durations) if durations else 0
record("Duration recorded for all", all(d > 0 for d in durations),
       f"avg={avg_duration:.2f}s, max={max(durations):.2f}s")

# =============================================================================
# 6. VERIFY THREADPOOL BEHAVIOR (key architectural feature)
# =============================================================================

print(f"\n  ThreadPool verification:")
print(f"    THREADS_PER_PARTITION = {THREADS_PER_PARTITION}")
print(f"    Expected rounds per partition (with FPP={TEST_FPP}): {TEST_FPP} / {THREADS_PER_PARTITION} = {math.ceil(TEST_FPP / THREADS_PER_PARTITION)}")
print(f"    Partition sizes: {partition_sizes}")
for i, size in enumerate(partition_sizes):
    rounds = math.ceil(size / THREADS_PER_PARTITION)
    print(f"    Partition {i}: {size} files → {rounds} rounds * ~25s ≈ {rounds * 25}s (estimated)")

record("ThreadPool multi-file path exercised",
       any(s > 1 for s in partition_sizes),
       f"max partition has {max(partition_sizes)} files → ThreadPool code path used")

# =============================================================================
# 7. CLEANUP
# =============================================================================

results_df.unpersist()
shutil.rmtree(tmp_output, ignore_errors=True)
print(f"\n  ✓ Cleaned up: {tmp_output}")
print(f"  ✓ Test 10 complete — full mapPartitions pipeline validated")

# COMMAND ----------

# DBTITLE 1,Test Summary
print("\n" + "="*60)
print("TEST SUMMARY")
print("="*60)

passed = sum(1 for t in TEST_RESULTS if t["passed"])
total = len(TEST_RESULTS)
failed_tests = [t for t in TEST_RESULTS if not t["passed"]]

print(f"\n  Total: {total} tests")
print(f"  Passed: {passed} ✓")
print(f"  Failed: {total - passed} ✗")
print(f"  Rate: {passed/total*100:.1f}%")

if failed_tests:
    print(f"\n  FAILED TESTS:")
    for t in failed_tests:
        print(f"    ✗ {t['test']}: {t['details']}")
else:
    print(f"\n  ALL TESTS PASSED ✓")

print("\n" + "="*60)
