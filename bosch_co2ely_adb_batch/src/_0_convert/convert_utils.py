"""ELY Data Converter - shared utilities.

Architecture: Distributed Polars on Spark Workers + Azure SDK.
- Auth: SP secret via spark_env_vars ({{secrets/...}} resolved at cluster start).
- I/O: Azure Storage SDK (parallel HTTP download/upload, zero JVM).
- Distribution: Spark mapPartitions distributes blob paths to workers.
- Parsing: Polars + calamine (Rust-native). Never touches JVM heap.
- Environment: auto-detected from workspace URL (dev/qa/prod).

4 output tables: filemeta, channel, timeseries, statistics.
Join key: UUID (deterministic UUID5 from relative blob path).

Schema naming:
- channel.channel_id = original column identifier (row 1 header)
- channel.raw_channel = display name (row 2 header)
- channel.unit = measurement unit (row 3 if detected)
- timeseries.channel_id = references channel.channel_id (the original identifier)
- timeseries.timestamp / elapsed_time_s = preserved structural columns, not signal rows

Header logic (scan first 3 rows):
- Row 1: channel_id (original column identifier/description)
- Row 2: raw_channel (display name)
- Row 3: if cells contain special chars or are single-char -> unit
         else -> first data row (timeseries starts here)

Timeseries: signal-only wide->long melt with sample_offset plus structural time.
"""
import os
import re
import io
import json
import uuid
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

try:
    _COMMON_DIR = Path(__file__).resolve().parent
except NameError:
    _COMMON_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent

_SHARED_DIR = _COMMON_DIR.parent / "_5_common"
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

logger = logging.getLogger("ely_converter")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(h)


def get_unity_catalog_path(env_vars: dict) -> str:
    """Build fully qualified UC path: catalog.schema."""
    return f"{env_vars['unity_catalog']}.{env_vars['unity_schema']}"


def build_abfss_path(storage_account: str, container: str, blob_path: str) -> str:
    """Construct full abfss:// URI from components.

    Args:
        storage_account: ADLS Gen2 storage account name (e.g. "stpsbdodxdev2datalake").
        container: Blob container name (e.g. "co2elyd-data").
        blob_path: Relative blob path within the container (e.g. "raw_data/sub/file.xlsx").

    Returns:
        Full abfss:// URI string for use with Spark or External Locations.
        Example: "abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/raw_data/sub/file.xlsx"
    """
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{blob_path}"


# =============================================================================
# AZURE SDK CLIENT FACTORY
# =============================================================================

def get_blob_service_client(storage_account: str = None):
    """Create BlobServiceClient from environment variables.

    Credentials come from spark_env_vars (resolved from {{secrets/...}}).
    Safe to call on driver or inside mapPartitions workers.

    Args:
        storage_account: ADLS Gen2 storage account name. If None, reads from
            os.environ (not recommended).

    Returns:
        azure.storage.blob.BlobServiceClient authenticated via ClientSecretCredential.

    Raises:
        KeyError: If AZURE_TENANT_ID, AZURE_CLIENT_ID, or AZURE_CLIENT_SECRET
            are not set in os.environ.
    """
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient

    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    account_url = f"https://{storage_account}.blob.core.windows.net"
    return BlobServiceClient(account_url, credential=credential)


# =============================================================================
# UUID GENERATION (deterministic from relative blob path)
# =============================================================================

_CO2ELY_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def generate_file_uuid(relative_path: str) -> str:
    """Generate deterministic UUID from relative blob path.

    Uses UUID5 with a fixed namespace so the same file always produces the
    same UUID regardless of environment (dev/qa/prod).

    Args:
        relative_path: Environment-independent blob path (e.g. "test/PoC Stack II/file.xlsx").

    Returns:
        UUID string (e.g. "a1b2c3d4-...") deterministically derived from the path.
    """
    return str(uuid.uuid5(_CO2ELY_NAMESPACE, relative_path))


# =============================================================================
# SCHEMAS
# =============================================================================

SCHEMAS = {
    "filemeta": pa.schema([
        ("uuid", pa.string()),
        ("file_path", pa.string()),
        ("raw_file_name", pa.string()),
        ("file_size", pa.int64()),
        ("last_modified", pa.timestamp("us")),
        ("ingested_timestamp", pa.timestamp("us")),
        ("series", pa.string()),
    ]),
    "channel": pa.schema([
        ("uuid", pa.string()),
        ("group", pa.string()),
        ("channel_id", pa.string()),
        ("raw_channel", pa.string()),
        ("unit", pa.string()),
        ("column_index", pa.int32()),
    ]),
    "timeseries": pa.schema([
        ("uuid", pa.string()),
        ("group", pa.string()),
        ("sample_offset", pa.int64()),
        ("timestamp", pa.string()),
        ("elapsed_time_s", pa.float64()),
        ("channel_id", pa.string()),
        ("value", pa.float64()),
        ("value_str", pa.string()),
    ]),
    "statistics": pa.schema([
        ("uuid", pa.string()),
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
    """Output of a converter.

    For small files, tables["timeseries"] is an in-memory PyArrow Table.
    For large files (chunked processing), tables["timeseries"] may be None
    and timeseries_buffer holds a BytesIO with Parquet row groups.
    ParquetWriter handles both cases transparently.
    """
    tables: Dict[str, pa.Table]
    group_name: Optional[str] = None
    n_rows: int = 0
    n_channels: int = 0
    timeseries_buffer: Optional[io.BytesIO] = None  # BytesIO with Parquet for chunked output


@dataclass
class BlobInfo:
    """Metadata about a source blob discovered via Azure SDK listing."""
    blob_path: str       # full blob name: raw_data/path/to/file.xlsx
    relative_path: str   # path relative to source prefix: path/to/file.xlsx
    file_name: str
    file_size: int
    last_modified: datetime
    extension: str


# =============================================================================
# HELPERS
# =============================================================================

def sanitize_name(name: str) -> str:
    """Clean string for safe filesystem/blob name usage.

    Args:
        name: Raw string (e.g. file stem, sheet name) potentially containing
            unsafe characters.

    Returns:
        Cleaned string with special chars replaced by '_', spaces replaced,
        consecutive underscores collapsed, and leading/trailing '_' stripped.
    """
    name = re.sub(r'[<>:"/\\|?*()\[\]]', "_", name)
    name = name.replace(" ", "_")
    return re.sub(r"_+", "_", name).strip("_")


def _is_unit_cell(value: str) -> bool:
    """Check if a cell value looks like a channel unit.

    Args:
        value: Cell string value from row 3 of the header.

    Returns:
        True if value is a single character or contains special chars
        (indicating a unit like 'V', 'mA', '°C', 'µm').
    """
    if not value or not value.strip():
        return False
    v = value.strip()
    if len(v) == 1:
        return True
    if re.search(r'[^a-zA-Z0-9\s.,_-]', v):
        return True
    return False


def detect_units_row(row_values: List[str]) -> bool:
    """Check if a row is a units row (majority of non-empty cells look like units).

    Args:
        row_values: List of string values from row 3 of the header.

    Returns:
        True if > 50% of non-empty cells pass _is_unit_cell() check,
        indicating this row contains measurement units rather than data.
    """
    non_empty = [v for v in row_values if v and v.strip()]
    if not non_empty:
        return False
    unit_count = sum(1 for v in non_empty if _is_unit_cell(v))
    return unit_count / len(non_empty) > 0.5


# =============================================================================
# BLOB LISTING
# =============================================================================

SUPPORTED_EXTENSIONS = {".xlsx", ".xls"}


def list_source_blobs(storage_account: str, container: str, source_prefix: str) -> List[BlobInfo]:
    """List source blobs via Azure SDK (ContainerClient.list_blobs).

    Scans all blobs under source_prefix/ and returns metadata for supported
    file types (.xlsx, .xls).

    Args:
        storage_account: ADLS Gen2 storage account name.
        container: Blob container name.
        source_prefix: Blob path prefix to scan (e.g. "raw_data").

    Returns:
        List[BlobInfo] with metadata for each discovered blob (path, size,
        last_modified, extension). Only includes supported extensions.
    """
    client = get_blob_service_client(storage_account)
    container_client = client.get_container_client(container)

    results = []
    for blob in container_client.list_blobs(name_starts_with=f"{source_prefix}/"):
        name = blob.name
        ext = os.path.splitext(name)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            continue

        relative_path = name[len(source_prefix) + 1:]
        file_name = os.path.basename(name)

        results.append(BlobInfo(
            blob_path=name,
            relative_path=relative_path,
            file_name=file_name,
            file_size=blob.size,
            last_modified=blob.last_modified.replace(tzinfo=timezone.utc)
                         if blob.last_modified else datetime.now(tz=timezone.utc),
            extension=ext,
        ))

    return results


# =============================================================================
# UNPIVOT TIMESERIES
# =============================================================================

# Chunk processing constants
CHUNK_ROWS = 50_000  # rows per chunk during unpivot (controls peak RAM)
TIMESERIES_CHUNK_THRESHOLD = 1_000_000  # total timeseries rows (n_rows * n_cols) before chunking kicks in


def unpivot_timeseries(
    df: pl.DataFrame,
    file_uuid: str,
    group: str,
    columns: Optional[List[str]] = None,
    chunk_rows: Optional[int] = None,
    timestamp_column: Optional[str] = None,
    elapsed_column: Optional[str] = None,
):
    """Wide -> long melt using Polars .unpivot().

    Converts signal columns into long-format rows with preserved structural
    timestamp and elapsed-time columns. Structural time columns are not melted
    as channels.

    Args:
        df: Polars DataFrame with data rows. Columns named by channel identifiers.
        file_uuid: Deterministic UUID for this file (join key).
        group: Group identifier (sheet name for xlsx).
        columns: Signal column names to unpivot. If None, uses all columns.
        chunk_rows: If provided, processes in row-wise chunks of this size and
            writes each chunk as a Parquet row group into a BytesIO buffer.
            Use this for large files to bound peak memory usage.
        timestamp_column: Optional structural timestamp column to preserve.
        elapsed_column: Optional structural elapsed-time column to preserve.

    Returns:
        - If chunk_rows is None: pa.Table cast to SCHEMAS["timeseries"].
        - If chunk_rows is set: tuple of (total_ts_rows: int, buffer: io.BytesIO)
          where buffer contains a complete Parquet file with multiple row groups.
    """
    if columns is None:
        columns = df.columns

    if not columns:
        empty_table = pa.Table.from_pylist([], schema=SCHEMAS["timeseries"])
        if chunk_rows is None:
            return empty_table
        buf = io.BytesIO()
        writer = pq.ParquetWriter(
            buf,
            SCHEMAS["timeseries"],
            compression="zstd",
            compression_level=3,
            write_statistics=True,
        )
        try:
            writer.write_table(empty_table)
        finally:
            writer.close()
        buf.seek(0)
        return 0, buf

    def structural_exprs() -> list[pl.Expr]:
        exprs: list[pl.Expr] = []
        if timestamp_column and timestamp_column in df.columns:
            exprs.append(pl.col(timestamp_column).cast(pl.String).alias("timestamp"))
        else:
            exprs.append(pl.lit(None, dtype=pl.String).alias("timestamp"))
        if elapsed_column and elapsed_column in df.columns:
            exprs.append(pl.col(elapsed_column).cast(pl.String).cast(pl.Float64, strict=False).alias("elapsed_time_s"))
        else:
            exprs.append(pl.lit(None, dtype=pl.Float64).alias("elapsed_time_s"))
        return exprs

    if chunk_rows is None:
        # --- In-memory path ---
        df_indexed = df.with_row_index("sample_offset").select(
            ["sample_offset", *columns, *structural_exprs()]
        )

        long_df = df_indexed.unpivot(
            index=["sample_offset", "timestamp", "elapsed_time_s"],
            on=columns,
            variable_name="channel_id",
            value_name="raw_value",
        )

        long_df = long_df.with_columns(
            pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).alias("value"),
            pl.when(
                pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).is_null()
                & pl.col("raw_value").is_not_null()
            )
            .then(pl.col("raw_value").cast(pl.String))
            .otherwise(None)
            .alias("value_str"),
            pl.lit(file_uuid).alias("uuid"),
            pl.lit(group).alias("group"),
        )

        return (
            long_df
            .select(["uuid", "group", "sample_offset", "timestamp", "elapsed_time_s", "channel_id", "value", "value_str"])
            .to_arrow()
            .cast(SCHEMAS["timeseries"])
        )

    else:
        # --- Chunked path: bounded memory via BytesIO ---
        n_rows = df.height
        total_ts_rows = 0
        buf = io.BytesIO()
        writer = pq.ParquetWriter(
            buf,
            SCHEMAS["timeseries"],
            compression="zstd",
            compression_level=3,
            write_statistics=True,
        )

        try:
            for start in range(0, n_rows, chunk_rows):
                length = min(chunk_rows, n_rows - start)
                chunk = df.slice(start, length)

                chunk_indexed = chunk.with_row_index("sample_offset", offset=start).select(
                    ["sample_offset", *columns, *structural_exprs()]
                )

                long_chunk = chunk_indexed.unpivot(
                    index=["sample_offset", "timestamp", "elapsed_time_s"],
                    on=columns,
                    variable_name="channel_id",
                    value_name="raw_value",
                )

                long_chunk = long_chunk.with_columns(
                    pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).alias("value"),
                    pl.when(
                        pl.col("raw_value").cast(pl.String).cast(pl.Float64, strict=False).is_null()
                        & pl.col("raw_value").is_not_null()
                    )
                    .then(pl.col("raw_value").cast(pl.String))
                    .otherwise(None)
                    .alias("value_str"),
                    pl.lit(file_uuid).alias("uuid"),
                    pl.lit(group).alias("group"),
                )

                arrow_chunk = (
                    long_chunk
                    .select(["uuid", "group", "sample_offset", "timestamp", "elapsed_time_s", "channel_id", "value", "value_str"])
                    .to_arrow()
                    .cast(SCHEMAS["timeseries"])
                )
                writer.write_table(arrow_chunk)
                total_ts_rows += arrow_chunk.num_rows

                del chunk, chunk_indexed, long_chunk, arrow_chunk

        finally:
            writer.close()

        buf.seek(0)
        return total_ts_rows, buf


# =============================================================================
# TABLE BUILDERS
# =============================================================================

def build_filemeta(
    file_path: str, file_uuid: str, file_size: int,
    last_modified: Optional[datetime],
    series: Optional[str] = None,
    group: Optional[str] = None,
) -> pa.Table:
    """Build filemeta PyArrow table (1 row per file-group).

    Args:
        file_path: Full abfss:// URI or relative path for traceability.
        file_uuid: Deterministic UUID for this file.
        file_size: File size in bytes.
        last_modified: Blob last modification timestamp.
        series: Governed series name resolved from the ADLS source folder
            during channel-mapping lookup (e.g. "PoC Stack VI"). None if no
            configured series folder matched the file's relative path.
        group: Group identifier for the sheet / logical subgroup.

    Returns:
        PyArrow Table with schema: uuid, file_path, raw_file_name, file_size,
        last_modified, ingested_timestamp, series, group.
    """
    now = datetime.now(tz=timezone.utc)
    return pa.table({
        "uuid": [file_uuid],
        "file_path": [file_path],
        "raw_file_name": [file_path.rsplit("/", 1)[-1]],
        "file_size": [file_size],
        "last_modified": [last_modified],
        "ingested_timestamp": [now],
        "series": [series],
        "group": [group],
    }, schema=SCHEMAS["filemeta"])


def build_channel(
    file_uuid: str, group: str,
    channel_ids: List[str], raw_channels: List[str], units: List[str],
) -> pa.Table:
    """Build channel catalog PyArrow table from the 3-row header scan.

    Args:
        file_uuid: Deterministic UUID for this file (join key).
        group: Group identifier (sheet name or "data").
        channel_ids: List of channel identifiers (row 1 values).
        raw_channels: List of raw display names (row 2 values).
        units: List of unit strings (row 3 values, empty string if no unit).

    Returns:
        PyArrow Table with schema: uuid, group, channel_id, raw_channel, unit,
        column_index. One row per signal channel.
    """
    n = len(raw_channels)
    return pa.table({
        "uuid": [file_uuid] * n,
        "group": [group] * n,
        "channel_id": channel_ids[:n] if len(channel_ids) >= n else channel_ids + [""] * (n - len(channel_ids)),
        "raw_channel": raw_channels,
        "unit": units[:n] if len(units) >= n else units + [""] * (n - len(units)),
        "column_index": list(range(n)),
    }, schema=SCHEMAS["channel"])


def build_statistics(
    file_uuid: str, group: str, n_channels: int, n_rows: int,
) -> pa.Table:
    """Build statistics summary PyArrow table (1 row per group).

    Args:
        file_uuid: Deterministic UUID for this file (join key).
        group: Group identifier (sheet name or "data").
        n_channels: Number of data channels (columns).
        n_rows: Number of data rows (excluding header rows).

    Returns:
        PyArrow Table with schema: uuid, group, n_channels, n_rows,
        n_timeseries_rows (= n_rows * n_channels).
    """
    return pa.table({
        "uuid": [file_uuid],
        "group": [group],
        "n_channels": [n_channels],
        "n_rows": [n_rows],
        "n_timeseries_rows": [n_rows * n_channels],
    }, schema=SCHEMAS["statistics"])


# =============================================================================
# PARQUET WRITER
# =============================================================================

class ParquetWriter:
    """Writes Arrow tables as Parquet to ADLS via Azure SDK.

    Creates one Parquet file per table_type per group (e.g. timeseries/filename_sheetname.parquet).
    Uses Zstd compression for optimal parquet size.
    """

    def __init__(self, storage_account: str, container: str, output_prefix: str,
                 compression="zstd", compression_level=3):
        """Initialize ParquetWriter with ADLS coordinates.

        Args:
            storage_account: ADLS Gen2 storage account name.
            container: Blob container name.
            output_prefix: Base blob path for output.
            compression: Parquet compression codec (default: "zstd").
            compression_level: Compression level (default: 3).
        """
        self.storage_account = storage_account
        self.container = container
        self.output_prefix = output_prefix
        self.compression = compression
        self.compression_level = compression_level
        self._client = None

    @property
    def client(self):
        """Lazy init — only create SDK client when first write happens."""
        if self._client is None:
            self._client = get_blob_service_client(self.storage_account)
        return self._client

    def write_result(self, result: ConversionResult, base_filename: str) -> Dict[str, str]:
        """Write 4 Parquet tables to ADLS for a single conversion result.

        Handles two modes:
        - In-memory: table is a PyArrow Table in result.tables (small files)
        - Chunked: timeseries is in a BytesIO buffer (result.timeseries_buffer)
          and result.tables["timeseries"] is None. Buffer uploaded directly.

        Args:
            result: ConversionResult containing PyArrow tables for each table_type.
                If result.timeseries_buffer is set, timeseries is uploaded from buffer.
            base_filename: Sanitized base name for output files (derived from source file stem).

        Returns:
            Dict mapping table_type to relative blob path (e.g.
            {"timeseries": "timeseries/filename_sheetname_timeseries.parquet"}).
        """
        output_paths = {}
        group_suffix = f"_{sanitize_name(result.group_name)}" if result.group_name else ""
        container_client = self.client.get_container_client(self.container)

        for table_type, table in result.tables.items():
            filename = f"{base_filename}{group_suffix}_{table_type}.parquet"
            blob_name = f"{self.output_prefix}/{table_type}/{filename}"

            # Chunked timeseries: upload directly from BytesIO buffer
            if table_type == "timeseries" and result.timeseries_buffer:
                blob_client = container_client.get_blob_client(blob_name)
                result.timeseries_buffer.seek(0)
                blob_client.upload_blob(
                    result.timeseries_buffer, overwrite=True, max_concurrency=4
                )
                output_paths[table_type] = f"{table_type}/{filename}"
                # Free buffer after upload
                result.timeseries_buffer.close()
                result.timeseries_buffer = None
                continue

            # In-memory tables (filemeta, channel, statistics, or small timeseries)
            if table is None:
                continue
            if table.num_rows == 0 and table_type not in ("filemeta", "statistics"):
                continue

            buf = io.BytesIO()
            pq.write_table(table, buf,
                           compression=self.compression,
                           compression_level=self.compression_level,
                           write_statistics=True)
            buf.seek(0)

            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buf, overwrite=True, max_concurrency=4)

            output_paths[table_type] = f"{table_type}/{filename}"

        return output_paths


# =============================================================================
# INCREMENTAL TRACKER
# =============================================================================

class IncrementalTracker:
    """Delta table tracking for incremental file processing.

    Incremental strategy:
    1. Query tracking table for high watermark (max last_modified of SUCCESS files)
    2. List blobs via Azure SDK, skip older than watermark
    3. Cross-check candidates against tracking table (small set)

    Retry tracking:
    - retry_count column tracks how many times a FAILED file has been retried
    - On SUCCESS: retry_count resets to 0
    - On FAILED: retry_count increments by 1
    - Driver skips files where retry_count >= MAX_TOTAL_RETRIES (permanently broken)

    Deduplication:
    - MERGE ON blob_path guarantees exactly-once tracking per file
    - Even if same file is submitted twice in the same batch, MERGE deduplicates
    """

    def __init__(self, tracking_table: str, spark_session):
        """Initialize tracker and ensure table schema exists.

        Args:
            tracking_table: Fully qualified Delta table name
                (e.g. "co2elyd_dev.converter.file_tracking").
            spark_session: Active SparkSession for SQL operations.
        """
        self.table = tracking_table
        self.spark = spark_session
        self._ensure_exists()

    def _ensure_exists(self):
        """Create tracking table if not exists. Add retry_count for existing tables.

        Returns:
            None. Side effect: Delta table created/migrated in Unity Catalog.
        """
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                blob_path STRING, file_name STRING, file_size BIGINT,
                file_uuid STRING,
                last_modified TIMESTAMP, status STRING, processed_at TIMESTAMP,
                output_paths STRING, error_message STRING, duration_seconds DOUBLE,
                retry_count INT
            ) USING DELTA
        """)
        # Schema migration: add retry_count to existing tables that lack it
        try:
            cols = [f.name for f in self.spark.table(self.table).schema.fields]
            if "retry_count" not in cols:
                self.spark.sql(f"ALTER TABLE {self.table} ADD COLUMNS (retry_count INT)")
                logger.info(f"Added retry_count column to {self.table}")
        except Exception:
            pass

    def _get_watermark(self) -> Optional[datetime]:
        """Get high watermark: max last_modified of successfully processed files.

        Returns:
            datetime of the most recent successfully processed file's last_modified,
            or None if no SUCCESS records exist.
        """
        try:
            row = self.spark.sql(
                f"SELECT MAX(last_modified) AS wm FROM {self.table} WHERE status='SUCCESS'"
            ).collect()[0]
            return row.wm.replace(tzinfo=timezone.utc) if row.wm else None
        except Exception:
            return None

    def get_new_files(self, storage_account: str, container: str,
                      source_prefix: str) -> List[BlobInfo]:
        """Discover new/modified blobs using SDK listing + watermark.

        Strategy:
        1. Get watermark (max last_modified of SUCCESS)
        2. List all blobs via Azure SDK
        3. Filter: skip blobs older than watermark
        4. Cross-check: skip blobs already SUCCESS in tracking table

        Args:
            storage_account: ADLS Gen2 storage account name.
            container: Blob container name.
            source_prefix: Blob path prefix to scan (e.g. "raw_data").

        Returns:
            List[BlobInfo] of blobs that need processing.
        """
        watermark = self._get_watermark()
        if watermark:
            logger.info(f"Watermark: {watermark.isoformat()} (skipping older blobs)")
        else:
            logger.info("No watermark (first run) - processing all blobs")

        all_blobs = list_source_blobs(storage_account, container, source_prefix)

        if watermark:
            candidates = [b for b in all_blobs if b.last_modified >= watermark]
            skipped = len(all_blobs) - len(candidates)
            logger.info(f"Blob listing: {skipped} skipped (before watermark), "
                        f"{len(candidates)} candidates")
        else:
            candidates = all_blobs

        if not candidates:
            return []

        # Cross-check candidates against tracking table (avoid re-processing SUCCESS files)
        processed = set()
        try:
            candidate_paths_df = self.spark.createDataFrame(
                [(c.blob_path,) for c in candidates], "blob_path STRING"
            ).dropDuplicates(["blob_path"])
            candidate_paths_df.createOrReplaceTempView("_tracker_candidate_paths")
            rows = self.spark.sql(f"""
                SELECT t.blob_path, t.file_size, t.last_modified
                FROM {self.table} t
                INNER JOIN _tracker_candidate_paths c
                    ON t.blob_path = c.blob_path
                WHERE t.status = 'SUCCESS'
            """).collect()
            self.spark.catalog.dropTempView("_tracker_candidate_paths")
            processed = {(r.blob_path, r.file_size, r.last_modified) for r in rows}
        except Exception:
            pass

        new_blobs = [b for b in candidates
                     if (b.blob_path, b.file_size, b.last_modified) not in processed]
        logger.info(f"New blobs to process: {len(new_blobs)}")
        return new_blobs

    def batch_merge_results(self, results_df):
        """Batch MERGE tracking results into Delta table.

        Performs atomic upsert: updates existing records or inserts new ones.
        Handles retry_count logic:
        - SUCCESS: reset retry_count to 0 (file recovered after transient failure)
        - FAILED: increment retry_count (tracks cumulative failures across runs)
        - New file: retry_count = 0 for SUCCESS, 1 for FAILED

        Args:
            results_df: Spark DataFrame with RESULT_SCHEMA columns (blob_path,
                file_name, file_size, file_uuid, last_modified, status,
                output_paths, error_message, duration_seconds).

        Returns:
            None. Side effect: tracking table updated via MERGE INTO.
        """
        results_df.createOrReplaceTempView("_converter_batch_results")
        self.spark.sql(f"""
            MERGE INTO {self.table} t
            USING _converter_batch_results s
            ON t.blob_path = s.blob_path
            WHEN MATCHED AND s.status = 'SUCCESS' THEN UPDATE SET
                status = s.status, processed_at = current_timestamp(),
                file_size = s.file_size, file_uuid = s.file_uuid,
                last_modified = s.last_modified,
                output_paths = s.output_paths,
                error_message = NULL,
                duration_seconds = s.duration_seconds,
                retry_count = 0
            WHEN MATCHED AND s.status != 'SUCCESS' THEN UPDATE SET
                status = s.status, processed_at = current_timestamp(),
                file_size = s.file_size, file_uuid = s.file_uuid,
                last_modified = s.last_modified,
                output_paths = s.output_paths,
                error_message = s.error_message,
                duration_seconds = s.duration_seconds,
                retry_count = COALESCE(t.retry_count, 0) + 1
            WHEN NOT MATCHED THEN INSERT (
                blob_path, file_name, file_size, file_uuid,
                last_modified, status, processed_at,
                output_paths, error_message, duration_seconds, retry_count
            ) VALUES (
                s.blob_path, s.file_name, s.file_size, s.file_uuid,
                s.last_modified, s.status, current_timestamp(),
                s.output_paths, s.error_message, s.duration_seconds,
                CASE WHEN s.status = 'SUCCESS' THEN 0 ELSE 1 END
            )
        """)
        self.spark.catalog.dropTempView("_converter_batch_results")
