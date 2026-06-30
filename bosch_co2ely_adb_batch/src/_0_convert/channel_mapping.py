"""Static channel mapping utilities for converter-stage canonicalization.

Mappings use the original Dash app JSON format:
    [{"schema_column": "Current", "file_column": "Current", ...}, ...]

The converter resolves a mapping by matching the source series folder in ADLS
(e.g. `PoC Stack VI`) to `sys_files/config_files/mappings/series_config.json`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

from converter_utils import logger


def load_series_mapping(repo_root: Path) -> dict[str, list[dict]]:
    """Load all configured series mapping JSON files.

    Args:
        repo_root: Path to `bosch_co2ely_adb_batch`.

    Returns:
        Dictionary keyed by series folder name.
    """
    mappings_dir = repo_root / "sys_files" / "config_files" / "mappings"
    series_config_path = mappings_dir / "series_config.json"

    if not series_config_path.exists():
        logger.warning(f"Series config not found at {series_config_path}. Mapping disabled.")
        return {}

    series_config = json.loads(series_config_path.read_text(encoding="utf-8"))
    result: dict[str, list[dict]] = {}
    for folder_name, mapping_file in series_config.items():
        if folder_name.startswith("_"):
            continue
        mapping_path = mappings_dir / mapping_file
        if not mapping_path.exists():
            logger.warning(f"  Mapping file not found: {mapping_path}")
            continue
        entries = json.loads(mapping_path.read_text(encoding="utf-8"))
        result[folder_name] = entries
        logger.info(f"  Mapping loaded: '{folder_name}' <- {mapping_file} ({len(entries)} entries)")
    return result


def resolve_mapping_for_path(relative_path: str, series_mapping: dict[str, list[dict]]) -> list[dict]:
    """Return the best mapping for an ADLS relative path.

    The integration-test layout may include extra prefixes such as
    `test/PoC Stack VI/file.xlsx`, so this checks every path component, not only
    the first component.
    """
    if not series_mapping:
        return []
    parts = [part.strip() for part in relative_path.replace("\\", "/").split("/") if part.strip()]
    for part in parts:
        mapping = series_mapping.get(part)
        if mapping:
            return mapping
    return []


def apply_mapping(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    units: List[str],
    mapping: Optional[List[dict]],
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str]]:
    """Rename raw file display names to canonical schema column names.

    Mapping is applied after timestamp merge and before derived formulas.
    Matching uses row 2 display names (`row2_channel_name`) because that is the
    format used by the original Dash app mapping files. Empty `file_column`
    calculation entries are ignored.
    """
    if not mapping:
        return df, columns, row1_channel, row2_channel_name, units

    file_to_schema: dict[str, str] = {}
    for entry in mapping:
        file_col = str(entry.get("file_column") or "").strip()
        schema_col = str(entry.get("schema_column") or "").strip()
        if file_col and schema_col:
            file_to_schema[file_col.lower()] = schema_col

    if not file_to_schema:
        return df, columns, row1_channel, row2_channel_name, units

    new_columns: list[str] = []
    new_row1: list[str] = []
    new_row2: list[str] = []
    new_units: list[str] = []
    df_renames: dict[str, str] = {}
    seen_targets: set[str] = set()

    for col_id, row1, row2, unit in zip(columns, row1_channel, row2_channel_name, units):
        schema_col = file_to_schema.get(row2.strip().lower())
        if schema_col and schema_col not in seen_targets:
            seen_targets.add(schema_col)
            if col_id != schema_col:
                df_renames[col_id] = schema_col
            new_columns.append(schema_col)
            new_row1.append(schema_col)
            new_row2.append(schema_col)
        else:
            new_columns.append(col_id)
            new_row1.append(row1)
            new_row2.append(row2)
        new_units.append(unit)

    if df_renames:
        df = df.rename(df_renames)
        logger.info(f"    Mapping applied: {len(df_renames)} channel(s) renamed to canonical names")

    return df, new_columns, new_row1, new_row2, new_units
