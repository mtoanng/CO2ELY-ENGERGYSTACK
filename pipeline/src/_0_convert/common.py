"""ELY Data Converter - shared utilities.

Architecture: Distributed Polars on Spark Workers + UC Volume FUSE.
- Auth: Unity Catalog Managed Identity (Access Connector). Zero credentials.
- I/O: Direct FUSE filesystem read/write (/Volumes/...). Zero JVM overhead.
- Distribution: Spark mapPartitions distributes file paths to workers.
- Parsing: Polars + calamine (Rust-native). Never touches JVM heap.
- Environment: auto-detected from workspace URL (dev/qa/prod).

4 output tables: filemeta, channel, timeseries, statistics.
Join key: UUID (deterministic UUID5 from file_path).

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
import json
import uuid
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path
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
        "adls_domain": "stpsbdodxdev2datalake.dfs.core.windows.net",
        "unity_catalog": "co2elyd_dev",
    },
    "adb-7376334951991000.0.azuredatabricks.net": {
        "environment": "qa",
        "adls_domain": "stpsbdodxqadatalake.dfs.core.windows.net",
        "unity_catalog": "co2elyd_qa",
    },
    "adb-5407587042408609.9.azuredatabricks.net": {
        "environment": "prod",
        "adls_domain": "stpsbdodxproddatalake.dfs.core.windows.net",
        "unity_catalog": "co2elyd_prod",
    },
}

# Converter-specific config (UC schema + volume names)
CONVERTER_CONFIG = {
    "schema": "converter",
    "source_volume": "raw_data",
    "output_volume": "parquet_raw",
    "tracking_table_name": "file_tracking",
}


def get_env_variables(spark) -> dict:
    """Retrieve environment config based on workspace URL (same as TBP)."""
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        logger.warning("Workspace URL not found (local mode?)")
        return {"environment": "local", "adls_domain": None, "unity_catalog": None}

    config = ENVIRONMENT_CONFIG.get(workspace_url)
    if config is None:
        logger.warning(f"Unrecognized workspace: {workspace_url}, using dev defaults")
        return ENVIRONMENT_CONFIG["adb-1032635496032522.2.azuredatabricks.net"]

    return config


def get_volume_paths(unity_catalog: str) -> dict:
    """Resolve FUSE paths for UC Volumes based on environment catalog.

    Returns:
        source_dir: /Volumes/<catalog>/converter/raw_data
        output_dir: /Volumes/<catalog>/converter/parquet_raw
        tracking_table: <catalog>.converter.file_tracking
    """
    schema = CONVERTER_CONFIG["schema"]
    return {
        "source_dir": f"/Volumes/{unity_catalog}/{schema}/{CONVERTER_CONFIG['source_volume']}",
        "output_dir": f"/Volumes/{unity_catalog}/{schema}/{CONVERTER_CONFIG['output_volume']}",
        "tracking_table": f"{unity_catalog}.{schema}.{CONVERTER_CONFIG['tracking_table_name']}",
    }


# =============================================================================
# UUID GENERATION (deterministic from file_path)
# =============================================================================

# Namespace UUID for CO2ELY project (fixed, used as UUID5 namespace)
_CO2ELY_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def generate_file_uuid(file_path: str) -> str:
    """Generate deterministic UUID from file_path.

    Same file_path always produces same UUID (UUID5 = SHA-1 based).
    This is the join key across filemeta, channel, timeseries, and statistics.
    """
    return str(uuid.uuid5(_CO2ELY_NAMESPACE, file_path))


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
class FileInfo:
    """Metadata about a source file discovered via FUSE listing."""
    fuse_path: str       # /Volumes/catalog/schema/volume/path/to/file.xlsx
    relative_path: str   # path/to/file.xlsx (relative to volume root)
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
# FILE LISTING (FUSE — direct filesystem, no JVM)
# =============================================================================

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def list_source_files(source_dir: str) -> List[FileInfo]:
    """List source files recursively via FUSE mount (os.walk).

    UC Volume FUSE paths (/Volumes/...) appear as regular filesystem.
    No JVM, no Hadoop FS, no Py4J bridge. Direct kernel I/O.
    """
    results = []
    source_path = Path(source_dir)

    for root, _, files in os.walk(source_dir):
        for fname in files:
            ext = Path(fname).suffix.lower()
            if ext not in SUPPORTED_EXTENSIONS:
                continue

            full_path = os.path.join(root, fname)
            stat = os.stat(full_path)
            rel_path = os.path.relpath(full_path, source_dir)

            results.append(FileInfo(
                fuse_path=full_path,
                relative_path=rel_path,
                file_name=fname,
                file_size=stat.st_size,
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
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
# PARQUET WRITER (direct FUSE write — no JVM, no Hadoop FS)
# =============================================================================

class ParquetWriter:
    """Writes Arrow tables directly to UC Volume FUSE paths.

    Zero JVM overhead. PyArrow writes directly to the filesystem.
    UC Managed Identity handles auth transparently via FUSE mount.
    """

    def __init__(self, output_dir: str, compression="zstd", compression_level=3):
        self.output_dir = output_dir
        self.compression = compression
        self.compression_level = compression_level

    def write_result(self, result: ConversionResult, base_filename: str) -> Dict[str, str]:
        output_paths = {}
        group_suffix = f"_{sanitize_name(result.group_name)}" if result.group_name else ""

        for table_type, table in result.tables.items():
            if table.num_rows == 0 and table_type not in ("filemeta", "statistics"):
                continue

            filename = f"{base_filename}{group_suffix}_{table_type}.parquet"
            out_subdir = os.path.join(self.output_dir, table_type)
            os.makedirs(out_subdir, exist_ok=True)
            out_path = os.path.join(out_subdir, filename)

            # Direct filesystem write via PyArrow (FUSE → ADLS)
            pq.write_table(table, out_path,
                           compression=self.compression,
                           compression_level=self.compression_level,
                           write_statistics=True)
            output_paths[table_type] = f"{table_type}/{filename}"

        return output_paths


# =============================================================================
# INCREMENTAL TRACKER (watermark + FUSE listing)
# =============================================================================

class IncrementalTracker:
    """Delta table tracking for incremental file processing.

    Incremental strategy:
    1. Query tracking table for high watermark (max last_modified of SUCCESS files)
    2. List files via FUSE (os.walk), skip older than watermark
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

    def get_new_files(self, source_dir: str) -> List[FileInfo]:
        """Discover new/modified files using FUSE listing + watermark."""
        watermark = self._get_watermark()
        if watermark:
            logger.info(f"Watermark: {watermark.isoformat()} (skipping older files)")
        else:
            logger.info("No watermark (first run) — processing all files")

        # List files via FUSE (os.walk — direct kernel I/O)
        all_files = list_source_files(source_dir)

        # Filter by watermark
        if watermark:
            candidates = [f for f in all_files if f.last_modified > watermark]
            skipped = len(all_files) - len(candidates)
            logger.info(f"File listing: {skipped} skipped (before watermark), "
                        f"{len(candidates)} candidates")
        else:
            candidates = all_files

        if not candidates:
            return []

        # Cross-check candidates against tracking table
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

        new_files = [f for f in candidates
                     if (f.relative_path, f.file_size, f.last_modified) not in processed]
        logger.info(f"New files to process: {len(new_files)}")
        return new_files

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
