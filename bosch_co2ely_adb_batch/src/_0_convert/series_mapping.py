"""Series mapping lookup utilities for converter-stage file classification.

Mappings use the original Dash app JSON format:
    [{"schema_column": "Current", "file_column": "Current", ...}, ...]

The converter resolves the mapping file by matching the source series folder in
ADLS (e.g. `PoC Stack VI`) to `series_config.json`. Canonical channel mapping
is applied later in Silver; the converter only needs the mapping entries that
identify structural Date/Time columns and the matched series name.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from convert_utils import logger


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


def resolve_mapping_for_path(
    relative_path: str, series_mapping: dict[str, list[dict]]
) -> tuple[list[dict], Optional[str]]:
    """Return the best mapping and matched series name for an ADLS relative path.

    The integration-test layout may include extra prefixes such as
    `test/PoC Stack VI/file.xlsx`, so this checks every path component, not only
    the first component.

    Returns:
        Tuple of (mapping, series). ``series`` is the matched folder name
        (e.g. "PoC Stack VI") used as the governed series identifier — the
        same lookup that already selects the channel mapping, captured once
        at convert time instead of re-derived later via regex on file_path.
        Both are empty/None if no configured series folder matched.
    """
    if not series_mapping:
        return [], None
    parts = [part.strip() for part in relative_path.replace("\\", "/").split("/") if part.strip()]
    for part in parts:
        mapping = series_mapping.get(part)
        if mapping:
            return mapping, part
    return [], None

