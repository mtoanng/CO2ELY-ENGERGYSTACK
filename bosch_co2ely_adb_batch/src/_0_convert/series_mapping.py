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
    """Return the channel mapping and series name for an ADLS relative path.

    Series is always the direct parent folder of the file (one level up),
    e.g. ``PoC Stack VIII`` from ``PoC Stack VIII/file.xlsx``.
    This handles both production paths (``Series/file.xlsx``) and
    integration-test paths (``test/Series/file.xlsx``) without scanning
    all components.

    If no channel mapping JSON is configured for that folder, mapping is
    returned as an empty list — the converter falls back to the "Real time"
    heuristic for date/time column detection and continues without crashing.

    Returns:
        Tuple of (mapping, series). ``series`` is the direct parent folder
        name (e.g. ``"PoC Stack VIII"``), or None if the path has fewer than
        2 components. ``mapping`` is the configured list of channel-mapping
        entries, or ``[]`` if none is registered for this series.
    """
    parts = [part.strip() for part in relative_path.replace("\\", "/").split("/") if part.strip()]
    if len(parts) < 2:
        return [], None
    series = parts[-2]  # direct parent folder of the file
    mapping = series_mapping.get(series, [])
    if not mapping:
        logger.info(
            f"  No channel mapping configured for series '{series}' "
            "— date/time detection uses 'Real time' heuristic fallback."
        )
    return mapping, series

