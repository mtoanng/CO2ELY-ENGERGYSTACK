"""Silver-side channel mapping helpers.

Bronze preserves raw channel metadata. Silver owns canonicalization by loading
series-specific mapping JSON files and exposing a small Spark DataFrame that can
be joined to channel metadata by (series, raw_channel).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    _THIS_DIR = Path(__file__).resolve().parent
except NameError:
    _THIS_DIR = Path(sys._getframe().f_code.co_filename).resolve().parent

from pyspark.sql import SparkSession, DataFrame


def repo_root() -> Path:
    return _THIS_DIR.parent.parent


def build_channel_mapping_df(spark: SparkSession) -> DataFrame:
    mappings_dir = repo_root() / "sys_files" / "config_files" / "mappings"
    series_config_path = mappings_dir / "series_config.json"
    rows: list[tuple[str, str, str]] = []

    if series_config_path.exists():
        series_config = json.loads(series_config_path.read_text(encoding="utf-8"))
        for series, mapping_file in series_config.items():
            if series.startswith("_"):
                continue
            mapping_path = mappings_dir / mapping_file
            if not mapping_path.exists():
                continue
            entries = json.loads(mapping_path.read_text(encoding="utf-8"))
            for entry in entries:
                raw_channel = str(entry.get("file_column") or "").strip()
                std_channel = str(entry.get("schema_column") or "").strip()
                origin = str(entry.get("origin") or "").strip().lower()
                if raw_channel and std_channel and origin != "calculation" and std_channel not in {"Date", "Time"}:
                    rows.append((series, raw_channel, std_channel))

    return spark.createDataFrame(rows, "series string, raw_channel string, mapped_std_channel string")
