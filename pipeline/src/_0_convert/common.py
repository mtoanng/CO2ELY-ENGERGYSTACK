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
- channel.channel = original column identifier (row 1 header)
- channel.channel_name = display name (row 2 header)
- channel.unit = measurement unit (row 3 if detected)
- timeseries.channel = references channel.channel (the original identifier)

Header logic (scan first 3 rows):
- Row 1: channel (original column identifier/description)
- Row 2: channel_name (display name, used as DataFrame header)
- Row 3: if cells contain special chars or are single-char -> unit
         else -> first data row (timeseries starts here)

Timeseries: pure wide->long melt, row index (sample_offset) as identifier.
"""
import os
import re
import io
import json
import uuid
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger("ely_converter")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(h)


# =============================================================================
# ENVIRONMENT CONFIG (same pattern as TBP common_utils.py)
# =============================================================================

# Auto-detect environment from Databricks workspace URL
ENVIRONMENT_CONFIG = {
    "adb-1032635496032522.2.azuredatabricks.net": {
        "environment": "dev",
        "storage_account": "stpsbdodxdev2datalake",
        "container": "co2elyd-data",
        "unity_catalog": "co2elyd_dev",
    },
    "adb-7376334951991000.0.azuredatabricks.net": {
        "environment": "qa",
        "storage_account": "stpsbdodxqadatalake",
        "container": "co2elyd-data",
        "unity_catalog": "co2elyd_qa",
    },
    "adb-5407587042408609.9.azuredatabricks.net": {
        "environment": "prod",
        "storage_account": "stpsbdodxproddatalake",
        "container": "co2elyd-data",
        "unity_catalog": "co2elyd_prod",
    },
}

# Blob prefixes within the container (source/output)
CONVERTER_CONFIG = {
    "schema": "converter",
    "source_prefix": "raw_data",
    "output_prefix": "parquet_raw",
    "tracking_table_name": "file_tracking",
}


def get_env_variables(spark) -> dict:
    """Retrieve environment config based on workspace URL (same as TBP)."""
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        logger.warning("Workspace URL not found (local mode?)")
        return {"environment": "local", "storage_account": None,
                "container": None, "unity_catalog": None}

    config = ENVIRONMENT_CONFIG.get(workspace_url)
    if config is None:
        logger.warning(f"Unrecognized workspace: {workspace_url}, using dev defaults")
        return ENVIRONMENT_CONFIG["adb-1032635496032522.2.azuredatabricks.net"]

    return config


def get_adls_config(env_vars: dict) -> dict:
    """Resolve ADLS blob paths for the current environment.

    Returns:
        storage_account: e.g. "stpsbdodxdev2datalake"
        container: e.g. "co2elyd-data"
        source_prefix: e.g. "raw_data"
        output_prefix: e.g. "parquet_raw"
        tracking_table: e.g. "co2elyd_dev.converter.file_tracking"
    """
    catalog = env_vars["unity_catalog"]
    schema = CONVERTER_CONFIG["schema"]
    return {
        "storage_account": env_vars["storage_account"],
        "container": env_vars["container"],
        "source_prefix": CONVERTER_CONFIG["source_prefix"],
        "output_prefix": CONVERTER_CONFIG["output_prefix"],
        "tracking_table": f"{catalog}.{schema}.{CONVERTER_CONFIG['tracking_table_name']}",
    }


# =============================================================================
# AZURE SDK CLIENT FACTORY (worker-safe)
# =============================================================================

def get_blob_service_client(storage_account: str = None):
    """Create BlobServiceClient from environment variables.

    Credentials come from spark_env_vars (resolved from {{secrets/...}}).
    Safe to call on driver or inside mapPartitions workers.
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

# Namespace UUID for CO2ELY project (fixed, used as UUID5 namespace)
_CO2ELY_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def generate_file_uuid(relative_path: str) -> str:
    """Generate deterministic UUID from relative blob path.

    Same path always produces same UUID (UUID5 = SHA-1 based).
    This is the join key across filemeta, channel, timeseries, and statistics.
    Uses relative_path (environment-independent) so UUID is stable across dev/qa/prod.
    """
    return str(uuid.uuid5(_CO2ELY_NAMESPACE, relative_path))


# =============================================================================
# SCHEMAS (4 output tables)
# =============================================================================

SCHEMAS = {
    "filemeta": pa.schema([
        ("uuid", pa.string()),
        ("file_path", pa.string()),
        ("raw_file_name", pa.string()),
        ("file_size", pa.int64()),
        ("last_modified", pa.timestamp("us")),
        ("ingested_timestamp", pa.timestamp("us")),
    ]),
    "channel": pa.schema([
        ("uuid", pa.string()),
        ("group", pa.string()),
        ("channel", pa.string()),
        ("channel_name", pa.string()),
        ("unit", pa.string()),
        ("column_index", pa.int32()),
    ]),
    "timeseries": pa.schema([
        ("uuid", pa.string()),
        ("group", pa.string()),
        ("sample_offset", pa.int64()),
        ("channel", pa.string()),
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
    """Output of a converter."""
    tables: Dict[str, pa.Table]
    group_name: Optional[str] = None
    n_rows: int = 0
    n_channels: int = 0


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
    """Clean string for safe filesystem/blob name usage."""
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
# BLOB LISTING (Azure SDK — parallel HTTP, no JVM)
# =============================================================================

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def list_source_blobs(storage_account: str, container: str, source_prefix: str) -> List[BlobInfo]:
    """List source blobs via Azure SDK (ContainerClient.list_blobs).

    Parallel HTTP pagination, no JVM, no Hadoop FS.
    Returns only supported file extensions.
    """
    client = get_blob_service_client(storage_account)
    container_client = client.get_container_client(container)

    results = []
    for blob in container_client.list_blobs(name_starts_with=f"{source_prefix}/"):
        name = blob.name  # e.g. "raw_data/subdir/file.xlsx"
        ext = os.path.splitext(name)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            continue

        # Relative path: strip source prefix
        relative_path = name[len(source_prefix) + 1:]  # "subdir/file.xlsx"
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
# GENERIC UNPIVOT (Polars native .unpivot)
# =============================================================================

def generic_unpivot(
    df: pl.DataFrame,
    file_uuid: str,
    group: str,
    columns: Optional[List[str]] = None,
) -> pa.Table:
    """Wide -> long melt using Polars .unpivot().

    Row index (sample_offset) is the identifier.
    All columns become channels. Numeric -> value, non-numeric -> value_str.

    NOTE: df columns are named by row1 (channel = original identifier),
    NOT by row2 (channel_name). This ensures timeseries.channel matches
    channel.channel for joins.
    """
    if columns is None:
        columns = df.columns

    # Add row index
    df_indexed = df.select(columns).with_row_index("sample_offset")

    # Native melt — variable_name becomes "channel" (the original col identifier)
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
        pl.lit(file_uuid).alias("uuid"),
        pl.lit(group).alias("group"),
    )

    # Select in schema order
    result = long_df.select(
        ["uuid", "group", "sample_offset", "channel", "value", "value_str"]
    )

    return result.to_arrow().cast(SCHEMAS["timeseries"])


# =============================================================================
# TABLE BUILDERS
# =============================================================================

def build_filemeta(
    file_path: str, file_uuid: str, file_size: int,
    last_modified: Optional[datetime],
) -> pa.Table:
    now = datetime.now(tz=timezone.utc)
    return pa.table({
        "uuid": [file_uuid],
        "file_path": [file_path],
        "raw_file_name": [file_path.rsplit("/", 1)[-1]],
        "file_size": [file_size],
        "last_modified": [last_modified],
        "ingested_timestamp": [now],
    }, schema=SCHEMAS["filemeta"])


def build_channel(
    file_uuid: str, group: str,
    channels: List[str], channel_names: List[str], units: List[str],
) -> pa.Table:
    """Build channel catalog from the 3-row header scan.

    Args:
        channels: row 1 values (original identifier) — used as join key
        channel_names: row 2 values (display name)
        units: row 3 values (measurement unit, if detected)
    """
    n = len(channel_names)
    return pa.table({
        "uuid": [file_uuid] * n,
        "group": [group] * n,
        "channel": channels[:n] if len(channels) >= n else channels + [""] * (n - len(channels)),
        "channel_name": channel_names,
        "unit": units[:n] if len(units) >= n else units + [""] * (n - len(units)),
        "column_index": list(range(n)),
    }, schema=SCHEMAS["channel"])


def build_statistics(
    file_uuid: str, group: str, n_channels: int, n_rows: int,
) -> pa.Table:
    return pa.table({
        "uuid": [file_uuid],
        "group": [group],
        "n_channels": [n_channels],
        "n_rows": [n_rows],
        "n_timeseries_rows": [n_rows * n_channels],
    }, schema=SCHEMAS["statistics"])


# =============================================================================
# PARQUET WRITER (Azure SDK upload — parallel HTTP PUT, zero JVM)
# =============================================================================

class ParquetWriter:
    """Writes Arrow tables as Parquet to ADLS via Azure SDK.

    Zero JVM overhead. PyArrow serializes to bytes, SDK uploads parallel chunks.
    """

    def __init__(self, storage_account: str, container: str, output_prefix: str,
                 compression="zstd", compression_level=3):
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
        """Write 4 Parquet tables to ADLS. Returns {table_type: blob_path}."""
        output_paths = {}
        group_suffix = f"_{sanitize_name(result.group_name)}" if result.group_name else ""
        container_client = self.client.get_container_client(self.container)

        for table_type, table in result.tables.items():
            if table.num_rows == 0 and table_type not in ("filemeta", "statistics"):
                continue

            filename = f"{base_filename}{group_suffix}_{table_type}.parquet"
            blob_name = f"{self.output_prefix}/{table_type}/{filename}"

            # PyArrow → bytes buffer → SDK upload (parallel PUT)
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
# INCREMENTAL TRACKER (watermark + Azure SDK listing)
# =============================================================================

class IncrementalTracker:
    """Delta table tracking for incremental file processing.

    Incremental strategy:
    1. Query tracking table for high watermark (max last_modified of SUCCESS files)
    2. List blobs via Azure SDK, skip older than watermark
    3. Cross-check candidates against tracking table (small set)

    Bronze integration:
    - Bronze ingest queries this same tracking table (status='SUCCESS')
    - Only ingests Parquet files listed in output_paths of successful conversions
    - No Auto Loader needed — converter tracking IS the source of truth
    """

    def __init__(self, tracking_table: str, spark_session):
        self.table = tracking_table
        self.spark = spark_session
        self._ensure_exists()

    def _ensure_exists(self):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                blob_path STRING, file_name STRING, file_size BIGINT,
                file_uuid STRING,
                last_modified TIMESTAMP, status STRING, processed_at TIMESTAMP,
                output_paths STRING, error_message STRING, duration_seconds DOUBLE
            ) USING DELTA
        """)

    def _get_watermark(self) -> Optional[datetime]:
        """Get high watermark: max last_modified of successfully processed files."""
        try:
            row = self.spark.sql(
                f"SELECT MAX(last_modified) AS wm FROM {self.table} WHERE status='SUCCESS'"
            ).collect()[0]
            return row.wm
        except Exception:
            return None

    def get_new_files(self, storage_account: str, container: str,
                      source_prefix: str) -> List[BlobInfo]:
        """Discover new/modified blobs using SDK listing + watermark."""
        watermark = self._get_watermark()
        if watermark:
            logger.info(f"Watermark: {watermark.isoformat()} (skipping older blobs)")
        else:
            logger.info("No watermark (first run) — processing all blobs")

        # List blobs via Azure SDK (parallel HTTP pagination)
        all_blobs = list_source_blobs(storage_account, container, source_prefix)

        # Filter by watermark
        if watermark:
            candidates = [b for b in all_blobs if b.last_modified > watermark]
            skipped = len(all_blobs) - len(candidates)
            logger.info(f"Blob listing: {skipped} skipped (before watermark), "
                        f"{len(candidates)} candidates")
        else:
            candidates = all_blobs

        if not candidates:
            return []

        # Cross-check candidates against tracking table (avoid re-processing)
        processed = set()
        try:
            paths_sql = ",".join(f"'{c.relative_path}'" for c in candidates)
            rows = self.spark.sql(
                f"SELECT blob_path, file_size, last_modified FROM {self.table} "
                f"WHERE status='SUCCESS' AND blob_path IN ({paths_sql})"
            ).collect()
            processed = {(r.blob_path, r.file_size, r.last_modified) for r in rows}
        except Exception:
            pass

        new_blobs = [b for b in candidates
                     if (b.relative_path, b.file_size, b.last_modified) not in processed]
        logger.info(f"New blobs to process: {len(new_blobs)}")
        return new_blobs

    def batch_merge_results(self, results_df):
        """Batch MERGE tracking results from mapPartitions into Delta table.

        Much more efficient than individual MERGE per file.
        """
        results_df.createOrReplaceTempView("_converter_batch_results")
        self.spark.sql(f"""
            MERGE INTO {self.table} t
            USING _converter_batch_results s
            ON t.blob_path = s.blob_path
            WHEN MATCHED THEN UPDATE SET
                status = s.status, processed_at = current_timestamp(),
                file_size = s.file_size, file_uuid = s.file_uuid,
                last_modified = s.last_modified,
                output_paths = s.output_paths,
                error_message = s.error_message,
                duration_seconds = s.duration_seconds
            WHEN NOT MATCHED THEN INSERT (
                blob_path, file_name, file_size, file_uuid,
                last_modified, status, processed_at,
                output_paths, error_message, duration_seconds
            ) VALUES (
                s.blob_path, s.file_name, s.file_size, s.file_uuid,
                s.last_modified, s.status, current_timestamp(),
                s.output_paths, s.error_message, s.duration_seconds
            )
        """)
        self.spark.catalog.dropTempView("_converter_batch_results")
