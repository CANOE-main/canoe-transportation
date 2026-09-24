"""Small common-column comparison for legacy-reproduction bootstrap tables."""

from __future__ import annotations

import sqlite3
from math import isclose
from statistics import median
from collections.abc import Sequence
from pathlib import Path
from typing import Any


PROVENANCE_ONLY_COLUMNS = {
    "data_id",
    "data_source",
    "dq_cred",
    "dq_geog",
    "dq_struc",
    "dq_tech",
    "dq_time",
}


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def _row_set(
    connection: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
) -> set[tuple[Any, ...]]:
    projection = ", ".join(f'"{column}"' for column in columns)
    return {tuple(row) for row in connection.execute(f'SELECT {projection} FROM "{table}"')}


def compare_legacy_tables(
    candidate_path: Path,
    reference_path: Path,
    *,
    tables: Sequence[str],
) -> dict[str, Any]:
    """Compare common non-provenance columns without treating v4 additions as drift."""
    if not reference_path.is_file():
        raise FileNotFoundError(f"Legacy comparison database is missing: {reference_path}")
    results: dict[str, Any] = {}
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(
        reference_path
    ) as reference:
        for table in tables:
            candidate_columns = _columns(candidate, table)
            reference_columns = _columns(reference, table)
            if not candidate_columns or not reference_columns:
                results[table] = {
                    "comparable": False,
                    "reason": "table missing from candidate or legacy reference",
                }
                continue
            common = [
                column
                for column in candidate_columns
                if column in reference_columns and column not in PROVENANCE_ONLY_COLUMNS
            ]
            candidate_rows = _row_set(candidate, table, common)
            reference_rows = _row_set(reference, table, common)
            results[table] = {
                "comparable": True,
                "common_columns": common,
                "candidate_rows": len(candidate_rows),
                "reference_rows": len(reference_rows),
                "candidate_only_rows": len(candidate_rows - reference_rows),
                "reference_only_rows": len(reference_rows - candidate_rows),
                "expected_v4_only_columns": [
                    column
                    for column in candidate_columns
                    if column not in reference_columns
                ],
            }
    return {"enabled": True, "reference": str(reference_path), "tables": results}


def compare_legacy_existing_capacity(
    candidate_path: Path,
    reference_path: Path,
) -> dict[str, Any]:
    """Report Ontario transport capacity overlap without claiming false parity."""
    if not reference_path.is_file():
        raise FileNotFoundError(reference_path)
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(reference_path) as reference:
        current = {
            (str(tech), int(vintage)): (float(capacity), str(units))
            for tech, vintage, capacity, units in candidate.execute(
                "SELECT tech, vintage, capacity, units FROM existing_capacity WHERE region = 'ON'"
            )
        }
        previous = {
            (str(tech), int(vintage)): (float(capacity), str(units))
            for tech, vintage, capacity, units in reference.execute(
                "SELECT tech, vintage, capacity, units FROM ExistingCapacity "
                "WHERE region = 'ON' AND tech LIKE 'T_%' AND tech NOT LIKE '%CHRG%'"
            )
        }
    unit_map = {"k units": "k vehicles", "bpkm": "bn passenger-km", "btkm": "bn tonne-km"}
    shared = sorted(set(current) & set(previous))
    differences: list[dict[str, Any]] = []
    unit_mismatches: list[dict[str, Any]] = []
    for tech, vintage in shared:
        value, units = current[tech, vintage]
        legacy_value, legacy_units = previous[tech, vintage]
        if units != unit_map.get(legacy_units, legacy_units):
            unit_mismatches.append({"tech": tech, "vintage": vintage, "current": units, "legacy": legacy_units})
        if not isclose(value, legacy_value, rel_tol=0, abs_tol=1e-6):
            differences.append({
                "tech": tech,
                "vintage": vintage,
                "current_capacity": value,
                "legacy_capacity": legacy_value,
                "absolute_difference": abs(value - legacy_value),
                "relative_difference": abs(value - legacy_value) / abs(legacy_value) if legacy_value else None,
            })
    return {
        "scope": "Ontario transport existing capacity; reference excludes chargers",
        "candidate_rows": len(current),
        "reference_rows": len(previous),
        "shared_tech_vintage_keys": len(shared),
        "candidate_only_keys": len(set(current) - set(previous)),
        "reference_only_keys": len(set(previous) - set(current)),
        "candidate_vintages": sorted({vintage for _, vintage in current}),
        "reference_vintages": sorted({vintage for _, vintage in previous}),
        "unit_mismatches": unit_mismatches,
        "value_differences_over_1e_6": len(differences),
        "largest_absolute_differences": sorted(differences, key=lambda row: row["absolute_difference"], reverse=True)[:10],
        "status": "diagnostic; vintage coverage and derivation differ; no parity tolerance accepted",
    }


def compare_legacy_demand(candidate_path: Path, reference_path: Path) -> dict[str, Any]:
    """Compare Ontario service demand on shared keys without accepting parity gaps."""
    if not reference_path.is_file():
        raise FileNotFoundError(reference_path)
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(reference_path) as reference:
        current = {
            (str(commodity), int(period)): (float(value), str(units))
            for commodity, period, value, units in candidate.execute(
                "SELECT commodity, period, demand, units FROM demand WHERE region = 'ON'"
            )
        }
        previous = {
            (str(commodity), int(period)): (float(value), str(units))
            for commodity, period, value, units in reference.execute(
                "SELECT commodity, period, demand, units FROM demand "
                "WHERE region = 'ON' AND commodity LIKE 'T_D_%' "
                "AND commodity != 'T_D_pj_off'"
            )
        }
    unit_map = {"bpkm": "bn passenger-km", "btkm": "bn tonne-km"}
    shared = sorted(set(current) & set(previous))
    differences: list[dict[str, Any]] = []
    mismatched_units: list[dict[str, Any]] = []
    for commodity, period in shared:
        value, units = current[commodity, period]
        legacy_value, legacy_units = previous[commodity, period]
        if units != unit_map.get(legacy_units, legacy_units):
            mismatched_units.append({"commodity": commodity, "period": period,
                                     "current": units, "legacy": legacy_units})
        if not isclose(value, legacy_value, rel_tol=0, abs_tol=1e-6):
            differences.append({"commodity": commodity, "period": period,
                                "current_demand": value, "legacy_demand": legacy_value,
                                "absolute_difference": abs(value - legacy_value)})
    return {
        "scope": "Ontario passenger-km and tonne-km service commodities",
        "candidate_rows": len(current), "reference_rows": len(previous),
        "shared_keys": len(shared),
        "candidate_only_keys": len(set(current) - set(previous)),
        "reference_only_keys": len(set(previous) - set(current)),
        "candidate_periods": sorted({period for _, period in current}),
        "reference_periods": sorted({period for _, period in previous}),
        "unit_mismatches": mismatched_units,
        "value_differences_over_1e_6": len(differences),
        "largest_absolute_differences": sorted(
            differences, key=lambda row: row["absolute_difference"], reverse=True
        )[:10],
        "status": "diagnostic; baseline and projection reconciliation pending; no parity tolerance accepted",
    }


def compare_legacy_lifetime_tech(candidate_path: Path, reference_path: Path) -> dict[str, Any]:
    """Compare numeric Ontario fixed lifetimes on exact template technology keys."""
    if not reference_path.is_file():
        raise FileNotFoundError(reference_path)
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(reference_path) as reference:
        current = {
            str(tech): float(lifetime)
            for tech, lifetime in candidate.execute(
                "SELECT tech, lifetime FROM lifetime_tech WHERE region = 'ON'"
            )
        }
        raw = reference.execute(
            "SELECT tech, lifetime FROM LifetimeTech WHERE region = 'ON'"
        ).fetchall()
    previous = {str(tech): float(value) for tech, value in raw if value not in (None, "")}
    shared = sorted(set(current) & set(previous))
    differences = [
        {"tech": tech, "candidate_years": current[tech], "legacy_years": previous[tech]}
        for tech in shared if not isclose(current[tech], previous[tech], rel_tol=0, abs_tol=1e-9)
    ]
    return {
        "scope": "Ontario numeric fixed lifetimes on exact technology keys; curve technologies excluded",
        "candidate_rows": len(current), "reference_numeric_rows": len(previous),
        "reference_blank_rows": len(raw) - len(previous), "shared_keys": len(shared),
        "equal_values": len(shared) - len(differences),
        "value_differences": len(differences),
        "candidate_only_keys": len(set(current) - set(previous)),
        "reference_only_keys": len(set(previous) - set(current)),
        "difference_examples": differences[:15],
        "status": "diagnostic; reviewed source choices can differ from legacy; no parity tolerance accepted",
    }


def compare_legacy_efficiency(
    candidate_path: Path, reference_path: Path, *, absolute_tolerance: float,
    relative_tolerance: float,
) -> dict[str, Any]:
    """Report exact-edge Ontario overlap; do not infer commodity/technology aliases."""
    if not reference_path.is_file():
        raise FileNotFoundError(reference_path)
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(reference_path) as reference:
        current = {
            (str(tech), int(vintage), str(input_comm), str(output_comm)): float(value)
            for tech, vintage, input_comm, output_comm, value in candidate.execute(
                "SELECT tech, vintage, input_comm, output_comm, efficiency FROM efficiency WHERE region = 'ON'"
            )
        }
        previous = {
            (str(tech), int(vintage), str(input_comm), str(output_comm)): float(value)
            for tech, vintage, input_comm, output_comm, value in reference.execute(
                "SELECT tech, vintage, input_comm, output_comm, efficiency FROM efficiency "
                "WHERE region = 'ON' AND tech LIKE 'T_%'"
            ) if value not in (None, "")
        }
    shared = sorted(set(current) & set(previous))
    differences = [
        {"tech": key[0], "vintage": key[1], "input_comm": key[2], "output_comm": key[3],
         "candidate_efficiency": current[key], "legacy_efficiency": previous[key],
         "absolute_difference": abs(current[key] - previous[key]),
         "relative_difference": abs(current[key] - previous[key]) / abs(previous[key]) if previous[key] else None}
        for key in shared if not isclose(current[key], previous[key], rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
    ]
    return {
        "scope": "Ontario exact technology/vintage/input/output keys; legacy service-per-PJ numeric values",
        "candidate_rows": len(current), "reference_rows": len(previous), "shared_keys": len(shared),
        "candidate_only_keys": len(set(current) - set(previous)),
        "reference_only_keys": len(set(previous) - set(current)),
        "equal_within_diagnostic_tolerance": len(shared) - len(differences),
        "value_differences": len(differences),
        "absolute_tolerance": absolute_tolerance, "relative_tolerance": relative_tolerance,
        "largest_absolute_differences": sorted(differences, key=lambda row: row["absolute_difference"], reverse=True)[:15],
        "status": "diagnostic only; MTO weights, source editions, HHV PHEV calculation, vintage semantics and network representation differ; parity not asserted",
    }


def compare_legacy_costs(
    candidate_path: Path, reference_path: Path, *, absolute_tolerance: float,
    relative_tolerance: float,
) -> dict[str, Any]:
    """Report Ontario exact-key cost overlap without treating changed inputs as parity."""
    if not reference_path.is_file():
        raise FileNotFoundError(reference_path)
    results: dict[str, Any] = {}
    with sqlite3.connect(candidate_path) as candidate, sqlite3.connect(reference_path) as reference:
        for family, keys in (
            ("invest", ("tech", "vintage")),
            ("variable", ("tech", "period", "vintage")),
        ):
            table = "cost_" + family
            columns = ", ".join((*keys, "cost"))
            current = {
                tuple(row[:-1]): float(row[-1])
                for row in candidate.execute(
                    f'SELECT {columns} FROM "{table}" WHERE region = ?', ("ON",)
                ) if row[-1] is not None
            }
            previous = {
                tuple(row[:-1]): float(row[-1])
                for row in reference.execute(
                    f'SELECT {columns} FROM "Cost{family.title()}" WHERE region = ?',
                    ("ON",),
                ) if row[-1] is not None
            }
            shared = sorted(set(current) & set(previous))
            differences = [
                {
                    "key": key, "candidate_cost": current[key],
                    "legacy_cost": previous[key],
                    "absolute_difference": abs(current[key] - previous[key]),
                    "relative_difference": (
                        abs(current[key] - previous[key]) / abs(previous[key])
                        if previous[key] else None
                    ),
                }
                for key in shared
                if not isclose(
                    current[key], previous[key], abs_tol=absolute_tolerance,
                    rel_tol=relative_tolerance,
                )
            ]
            results[family] = {
                "candidate_ontario_rows": len(current),
                "legacy_ontario_rows": len(previous),
                "shared_keys": len(shared),
                "candidate_only_keys": len(set(current) - set(previous)),
                "legacy_only_keys": len(set(previous) - set(current)),
                "equal_within_diagnostic_tolerance": len(shared) - len(differences),
                "value_differences": len(differences),
                "median_candidate_to_legacy_ratio": median(
                    current[key] / previous[key]
                    for key in shared if previous[key] != 0
                ) if any(previous[key] != 0 for key in shared) else None,
                "largest_absolute_differences": sorted(
                    differences, key=lambda row: row["absolute_difference"], reverse=True
                )[:15],
            }
    return {
        "scope": "Ontario exact cost technology/vintage and period keys; activity-unit interpretations and source editions may differ",
        "absolute_tolerance": absolute_tolerance,
        "relative_tolerance": relative_tolerance,
        "status": "diagnostic only; cost source bases and representations require reviewed parity reconciliation",
        **results,
    }
