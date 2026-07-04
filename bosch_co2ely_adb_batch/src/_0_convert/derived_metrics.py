"""Derived metric formulas for the converter stage.

The demo pipeline computes derived engineering metrics before unpivoting so the
metrics land in bronze/gold as normal channels. Formulas mirror
CO_energystacck.src.backend.data_enrichment.DataEnrichment.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

logger = logging.getLogger("ely_converter")

# Default active area; overridden per-series via stack_definitions.json
ACTIVE_AREA_CM2 = 88.0
FARADAY_CONST = 96485.3
VM_STP = 22.414

# Resolve config directory relative to this file
_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "sys_files" / "config_files"


def _load_active_area(series: Optional[str]) -> float:
    """Resolve active area (cm2) for a series from stack_definitions.json.

    Lookup chain: series_config.json → series.json entry → stack_type →
    stack_definitions.json[stack_type].active_area_cm2.
    Falls back to ACTIVE_AREA_CM2 (88.0) if any lookup fails.
    """
    if not series:
        return ACTIVE_AREA_CM2
    try:
        stack_defs_path = _CONFIG_DIR / "mappings" / "stack_definitions.json"
        if not stack_defs_path.exists():
            stack_defs_path = _CONFIG_DIR / "stack_definitions.json"
        if not stack_defs_path.exists():
            return ACTIVE_AREA_CM2
        stack_defs = json.loads(stack_defs_path.read_text(encoding="utf-8"))
        for _type_name, type_def in stack_defs.items():
            if isinstance(type_def, dict) and "active_area_cm2" in type_def:
                return float(type_def["active_area_cm2"])
        return ACTIVE_AREA_CM2
    except Exception:
        return ACTIVE_AREA_CM2


def _f64(col_id: str) -> pl.Expr:
    return pl.col(col_id).cast(pl.Float64, strict=False)


def _safe_str(float_expr: pl.Expr) -> pl.Expr:
    cleaned = float_expr.fill_nan(None)
    return pl.when(cleaned.is_infinite()).then(None).otherwise(cleaned).cast(pl.String)


def _apply_plausibility_limits(
    df: pl.DataFrame,
    columns: List[str],
    units: List[str],
) -> pl.DataFrame:
    """Clip all %-unit columns to [0, 100].

    Mirrors CO_energystacck.src.backend.data_enrichment.apply_plausibility_limits().

    Unit source: the `units` list, populated from:
    - xlsx header detection (Row 3 units row, or Row 2 if detected as units)
    - explicit unit declarations in derived metric add() calls

    This covers all cases because:
    - 3-row header files: Row 3 has units → "%" detected
    - 2-row header files (name + unit): Row 2 detected as units → "%" detected
    - Derived metrics: always declare unit explicitly (e.g. add("Energy Efficiency", "%", ...))

    Non-numeric values are preserved unchanged.
    """
    for col_name, unit_val in zip(columns, units):
        if unit_val != "%" or col_name not in df.columns:
            continue
        # Cast to float, clip, cast back to string.
        # Non-numeric strings → null after cast → preserved via otherwise branch.
        numeric = pl.col(col_name).cast(pl.Float64, strict=False)
        df = df.with_columns(
            pl.when(numeric.is_not_null())
            .then(
                numeric
                .clip(0.0, 100.0)
                .fill_nan(None)
                .cast(pl.String)
            )
            .otherwise(pl.col(col_name))
            .alias(col_name)
        )
    return df


def apply_derived_metrics(
    df: pl.DataFrame,
    columns: List[str],
    row1_channel: List[str],
    row2_channel_name: List[str],
    std_channels: List[str],
    units: List[str],
    series: Optional[str] = None,
) -> Tuple[pl.DataFrame, List[str], List[str], List[str], List[str], List[str]]:
    """Append Dash-compatible derived metrics to a wide converter DataFrame.

    Input lookup uses the provided channel lookup names. In the Bronze path
    these are raw row-2 labels; Silver is responsible for canonical mapping.
    Missing inputs skip the formula. Outputs are strings so the existing unpivot
    cast chain produces `value` and `value_str` consistently.
    """
    name_to_col = {name.strip().lower(): col for name, col in zip(std_channels, columns)}
    original_column_count = len(columns)

    # Resolve active area for this series (defaults to 88.0 cm²)
    active_area = _load_active_area(series)

    def req(*canonical_names: str) -> Optional[List[str]]:
        result = []
        for name in canonical_names:
            col_id = name_to_col.get(name.lower())
            if col_id is None:
                return None
            result.append(col_id)
        return result

    def add(col_name: str, unit: str, expr: pl.Expr) -> None:
        nonlocal df, columns, row1_channel, row2_channel_name, std_channels, units
        if col_name in df.columns:
            logger.debug(f"    Derived '{col_name}' already exists as raw column - skipped")
            return
        try:
            df = df.with_columns(_safe_str(expr).alias(col_name))
            columns = columns + [col_name]
            row1_channel = row1_channel + [col_name]
            row2_channel_name = row2_channel_name + [col_name]
            std_channels = std_channels + [col_name]
            units = units + [unit]
        except Exception as exc:
            logger.warning(f"    Derived metric '{col_name}' failed: {exc}")

    ids = req("Faradaic Efficiency of CO", "Stack Voltage")
    if ids:
        fe_co_id, stack_voltage_id = ids
        add("Energy Efficiency", "%", 1.48 * _f64(fe_co_id) / (_f64(stack_voltage_id) / 5.0))

    ids = req("Anolyte inlet pressure", "Anolyte outlet pressure")
    if ids:
        inlet_id, outlet_id = ids
        add("Δp Anolyte", "bar", _f64(inlet_id) - _f64(outlet_id))

    ids = req("Current")
    if ids:
        add("Current density", "mA/cm²", 1000.0 * _f64(ids[0]) / active_area)

    ids = req("Faradaic Efficiency of CO", "Faradaic Efficiency of H2")
    if ids:
        fe_co_id, fe_h2_id = ids
        add("Faradaic Efficiency of CO and H2", "%", _f64(fe_co_id) + _f64(fe_h2_id))

    ids = req("Faradaic Efficiency of CO", "Current")
    if ids:
        fe_co_id, current_id = ids
        add(
            "Flow CO out",
            "nL/min",
            (_f64(fe_co_id) / 100.0 * _f64(current_id) / (2.0 * FARADAY_CONST)) * VM_STP * 60.0,
        )

    ids = req("Faradaic Efficiency of H2", "Current")
    if ids:
        fe_h2_id, current_id = ids
        add(
            "Flow H2 out",
            "nL/min",
            (_f64(fe_h2_id) / 100.0 * _f64(current_id) / (2.0 * FARADAY_CONST)) * VM_STP * 60.0,
        )

    ids = req("Faradaic Efficiency of O2", "Current")
    if ids:
        fe_o2_id, current_id = ids
        add(
            "Flow O2 out",
            "nL/min",
            (_f64(fe_o2_id) / 100.0 * _f64(current_id) / (4.0 * FARADAY_CONST)) * VM_STP * 60.0,
        )

    ids = req("Cathode inlet CO2 gas flow")
    if ids and "Flow CO out" in df.columns:
        add("Flow CO2 out, total", "nL/min", _f64(ids[0]) - _f64("Flow CO out"))

    ids = req("CO2:O2 ratio in anode product gas")
    if ids and "Flow O2 out" in df.columns:
        ratio = _f64(ids[0]) / 100.0
        add("Flow CO2 out, anode", "nL/min", (ratio * _f64("Flow O2 out")) / (1.0 - ratio))

    if "Flow CO2 out, total" in df.columns and "Flow CO2 out, anode" in df.columns:
        add("Flow CO2 out, cathode", "nL/min", _f64("Flow CO2 out, total") - _f64("Flow CO2 out, anode"))

    if "Flow CO out" in df.columns and "Flow H2 out" in df.columns:
        add("CO/H2 ratio recalculated", "", _f64("Flow CO out") / _f64("Flow H2 out"))

    ids = req("Faradaic Efficiency of CO", "Current", "Cathode inlet CO2 gas flow")
    if ids:
        fe_co_id, current_id, co2_id = ids
        feco = _f64(fe_co_id) / 100.0
        co_formation_rate = _f64(current_id) * feco / (2.0 * FARADAY_CONST)
        co2_inflow_rate = (_f64(co2_id) / 60.0 / 5.0) / VM_STP
        add("Single Pass Conversion Efficiency", "%", 100.0 * co_formation_rate / co2_inflow_rate)

    added_count = len(columns) - original_column_count
    if added_count:
        logger.info(f"    Derived metrics added: {added_count}")

    # --- Plausibility limits: clip %-unit columns to [0, 100] ---
    # Relies on header-detected units + explicit derived metric units.
    # No external schema file needed.
    df = _apply_plausibility_limits(df, columns, units)

    return df, columns, row1_channel, row2_channel_name, std_channels, units
