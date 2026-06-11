"""ELY Data Converter - shared utilities.

Only CSV + XLSX. 4 output tables: filemeta, channel, timeseries, statistics.

Header logic (scan first 3 rows):
- Row 1: col (original column identifier/description)
- Row 2: col_name (used as DataFrame header)
- Row 3: if cells contain special chars or are single-char -> channel_unit
         else -> first data row (timeseries starts here)

Timeseries: pure wide->long melt, row index (sample_offset) as identifier.
"""
import io
import re
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, ContainerClient

logger = logging.getLogger("ely_converter")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(h)


# =============================================================================
# SCHEMAS (4 output tables)
# =============================================================================

SCHEMAS = {
    "filemeta": pa.schema([
        ("file_path", pa.string()),
        ("raw_file_name", pa.string()),
        ("file_size", pa.int64()),
        ("last_modified", pa.timestamp("us")),
    ]),
    "channel": pa.schema([
        ("file_path", pa.string()),
        ("group", pa.string()),
        ("col", pa.string()),
        ("col_name", pa.string()),
        ("channel_unit", pa.string()),
        ("column_index", pa.int32()),
    ]),
    "timeseries": pa.schema([
        ("file_path", pa.string()),
        ("group", pa.string()),
        ("channel", pa.string()),
        ("sample_offset", pa.int64()),
        ("value", pa.float64()),
        ("value_str", pa.string()),
    ]),
    "statistics": pa.schema([
        ("file_path", pa.string()),
        ("group", pa.string()),
        ("n_channels", pa.int32()),
        ("n_rows", pa.int64()),
        ("n_timeseries_rows", pa.int64()),
    ]),
}

TABLE_TYPES = list(SCHEMAS.keys())


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ConversionResult:
    """Output of a converter."""
    tables: Dict[str, pa.Table]
    group_name: Optional[str] = None
    n_rows: int = 0
    n_channels: int = 0


@dataclass
class FileInfo:
    """Metadata about a source file in ADLS."""
    blob_path: str
    file_name: str
    file_size: int
    last_modified: datetime
    extension: str


# =============================================================================
# HELPERS
# =============================================================================

def sanitize_name(name: str) -> str:
    """Clean string for safe filesystem usage."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.replace(" ", "_")
    return re.sub(r"_+", "_", name).strip("_")


def _is_unit_cell(value: str) -> bool:
    """Check if a cell value looks like a channel unit."""
    if not value or not value.strip():
        return False
    v = value.strip()
    if len(v) == 1:
        return True
    if re.search(r'[^a-zA-Z0-9\s.,_-]', v):
        return True
    return False


def detect_units_row(row_values: List[str]) -> bool:
    """Check if a row is a units row (majority of non-empty cells look like units)."""
    non_empty = [v for v in row_values if v and v.strip()]
    if not non_empty:
        return False
    unit_count = sum(1 for v in non_empty if _is_unit_cell(v))
    return unit_count / len(non_empty) > 0.5


# =============================================================================
# GENERIC UNPIVOT (Polars native .unpivot)
# =============================================================================

def generic_unpivot(
    df: pl.DataFrame,
    file_path: str,
    group: str,
    columns: Optional[List[str]] = None,
) -> pa.Table:
    """Wide -> long melt using Polars .unpivot().

    Row index (sample_offset) is the identifier.
    All columns become channels. Numeric -> value, non-numeric -> value_str.
    """
    if columns is None:
        columns = df.columns

    # Add row index
    df_indexed = df.select(columns).with_row_index("sample_offset")

    # Native melt
    long_df = df_indexed.unpivot(
        index=["sample_offset"],
        on=columns,
        variable_name="channel",
        value_name="raw_value",
    )

    # Split numeric vs string
    long_df = long_df.with_columns(
        pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).alias("value"),
        pl.when(
            pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).is_null()
            & pl.col("raw_value").is_not_null()
        )
        .then(pl.col("raw_value").cast(pl.String))
        .otherwise(None)
        .alias("value_str"),
    )

    # Add provenance
    long_df = long_df.with_columns(
        pl.lit(file_path).alias("file_path"),
        pl.lit(group).alias("group"),
    )

    # Select in schema order
    result = long_df.select(
        ["file_path", "group", "channel", "sample_offset", "value", "value_str"]
    )

    return result.to_arrow().cast(SCHEMAS["timeseries"])


# =============================================================================
# TABLE BUILDERS
# =============================================================================

def build_filemeta(
    file_path: str, file_size: int, last_modified: Optional[datetime]
) -> pa.Table:
    return pa.table({
        "file_path": [file_path],
        "raw_file_name": [file_path.rsplit("/", 1)[-1]],
        "file_size": [file_size],
        "last_modified": [last_modified],
    }, schema=SCHEMAS["filemeta"])


def build_channel(
    file_path: str, group: str,
    cols: List[str], col_names: List[str], units: List[str],
) -> pa.Table:
    """Build channel catalog from the 3-row header scan."""
    n = len(col_names)
    return pa.table({
        "file_path": [file_path] * n,
        "group": [group] * n,
        "col": cols[:n] if len(cols) >= n else cols + [""] * (n - len(cols)),
        "col_name": col_names,
        "channel_unit": units[:n] if len(units) >= n else units + [""] * (n - len(units)),
        "column_index": list(range(n)),
    }, schema=SCHEMAS["channel"])


def build_statistics(
    file_path: str, group: str, n_channels: int, n_rows: int,
) -> pa.Table:
    return pa.table({
        "file_path": [file_path],
        "group": [group],
        "n_channels": [n_channels],
        "n_rows": [n_rows],
        "n_timeseries_rows": [n_rows * n_channels],
    }, schema=SCHEMAS["statistics"])


# =============================================================================
# PARQUET WRITER
# =============================================================================

class ParquetWriter:
    """Writes Arrow tables to ADLS as Parquet."""

    def __init__(self, storage_account, container, output_prefix, credential,
                 compression="zstd", compression_level=3):
        self.container = container
        self.output_prefix = output_prefix.rstrip("/")
        self.credential = credential
        self.compression = compression
        self.compression_level = compression_level
        self._url = f"https://{storage_account}.blob.core.windows.net"

    def write_result(self, result: ConversionResult, base_filename: str) -> Dict[str, str]:
        output_paths = {}
        group_suffix = f"_{sanitize_name(result.group_name)}" if result.group_name else ""

        for table_type, table in result.tables.items():
            if table.num_rows == 0 and table_type not in ("filemeta", "statistics"):
                continue

            filename = f"{base_filename}{group_suffix}_{table_type}.parquet"
            blob_path = f"{self.output_prefix}/{table_type}/{filename}"

            buf = io.BytesIO()
            pq.write_table(table, buf, compression=self.compression,
                           compression_level=self.compression_level,
                           write_statistics=True)
            buf.seek(0)

            blob = BlobClient(account_url=self._url, container_name=self.container,
                              blob_name=blob_path, credential=self.credential)
            blob.upload_blob(buf, overwrite=True, max_concurrency=4)
            output_paths[table_type] = blob_path

        return output_paths


# =============================================================================
# INCREMENTAL TRACKER
# =============================================================================

class IncrementalTracker:
    """Delta table tracking for incremental file processing."""

    SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

    def __init__(self, tracking_table: str, spark_session):
        self.table = tracking_table
        self.spark = spark_session
        self._ensure_exists()

    def _ensure_exists(self):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                blob_path STRING, file_name STRING, file_size BIGINT,
                last_modified TIMESTAMP, status STRING, processed_at TIMESTAMP,
                output_paths STRING, error_message STRING, duration_seconds DOUBLE
            ) USING DELTA
        """)

    def get_new_files(self, container_client, source_prefix) -> List[FileInfo]:
        all_files = []
        for blob in container_client.list_blobs(name_starts_with=source_prefix):
            ext = "." + blob.name.rsplit(".", 1)[-1].lower() if "." in blob.name else ""
            if ext in self.SUPPORTED_EXTENSIONS:
                all_files.append(FileInfo(
                    blob_path=blob.name, file_name=blob.name.split("/")[-1],
                    file_size=blob.size, last_modified=blob.last_modified, extension=ext,
                ))

        if not all_files:
            return []

        processed = set()
        try:
            rows = self.spark.sql(
                f"SELECT blob_path, file_size, last_modified FROM {self.table} WHERE status='SUCCESS'"
            ).collect()
            processed = {(r.blob_path, r.file_size, r.last_modified) for r in rows}
        except Exception:
            pass

        new_files = [f for f in all_files if (f.blob_path, f.file_size, f.last_modified) not in processed]
        logger.info(f"Found {len(all_files)} total files, {len(new_files)} new/modified")
        return new_files

    def mark_success(self, fi: FileInfo, paths: Dict, duration: float):
        self.spark.sql(f"""MERGE INTO {self.table} t USING (SELECT '{fi.blob_path}' AS blob_path) s
            ON t.blob_path = s.blob_path
            WHEN MATCHED THEN UPDATE SET status='SUCCESS', processed_at=current_timestamp(),
                output_paths='{json.dumps(paths)}', duration_seconds={duration:.2f}
            WHEN NOT MATCHED THEN INSERT (blob_path,file_name,file_size,last_modified,status,processed_at,output_paths,duration_seconds)
                VALUES('{fi.blob_path}','{fi.file_name}',{fi.file_size},timestamp'{fi.last_modified.isoformat()}','SUCCESS',current_timestamp(),'{json.dumps(paths)}',{duration:.2f})""")

    def mark_failed(self, fi: FileInfo, error: str, duration: float):
        safe = error.replace("'", "''")[:500]
        self.spark.sql(f"""MERGE INTO {self.table} t USING (SELECT '{fi.blob_path}' AS blob_path) s
            ON t.blob_path = s.blob_path
            WHEN MATCHED THEN UPDATE SET status='FAILED', error_message='{safe}', duration_seconds={duration:.2f}
            WHEN NOT MATCHED THEN INSERT (blob_path,file_name,file_size,last_modified,status,processed_at,error_message,duration_seconds)
                VALUES('{fi.blob_path}','{fi.file_name}',{fi.file_size},timestamp'{fi.last_modified.isoformat()}','FAILED',current_timestamp(),'{safe}',{duration:.2f})""")
