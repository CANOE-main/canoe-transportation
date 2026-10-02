"""Read-only, key-based comparison of databases produced by the current backend."""

from __future__ import annotations

import sqlite3
from collections import Counter
from contextlib import closing
from math import isclose, isfinite
from pathlib import Path
from typing import Any

from validation.sqlite_utils import open_sqlite_readonly, quote_identifier


PROVENANCE_TABLES = {"data_set", "data_source", "data_source_label"}
PROVENANCE_COLUMNS = {
    "data_id", "data_source", "dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time",
    "notes",
}


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }


def _table_info(connection: sqlite3.Connection, table: str) -> list[tuple[Any, ...]]:
    return connection.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()


def _require_v4(connection: sqlite3.Connection, label: str) -> None:
    try:
        version = dict(connection.execute(
            "SELECT element, value FROM metadata WHERE element IN ('DB_MAJOR', 'DB_MINOR')"
        ))
    except sqlite3.Error as exc:
        raise ValueError(f"Scenario comparison requires a v4 {label} database") from exc
    if version != {"DB_MAJOR": 4, "DB_MINOR": 0}:
        raise ValueError(f"Scenario comparison requires a v4 {label} database; found {version}")


def validate_scenario_comparison_reference(path: Path) -> None:
    """Reject an incompatible reference before expensive scenario preparation."""
    with closing(open_sqlite_readonly(path)) as connection:
        _require_v4(connection, "reference")


def _rows(
    connection: sqlite3.Connection, table: str, columns: list[str], keys: list[str],
) -> tuple[dict[tuple[Any, ...], tuple[Any, ...]], int]:
    indices = [columns.index(key) for key in keys]
    projection = ", ".join(quote_identifier(column) for column in columns)
    rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    duplicates = 0
    for row in connection.execute(f"SELECT {projection} FROM {quote_identifier(table)}"):
        key = tuple(row[index] for index in indices)
        duplicates += key in rows
        rows[key] = tuple(row)
    return rows, duplicates


def compare_scenario_databases(
    candidate_path: Path, reference_path: Path, *, absolute_tolerance: float,
    relative_tolerance: float, include_provenance: bool,
) -> dict[str, Any]:
    """Compare all regions/tables on exact model keys and configurable value tolerances.

    Source registries, source/DQ fields and notes are optional. Removing a provenance
    key must not silently collapse multiple model rows: ambiguous keys are reported
    as non-comparable. Units and other text are compared exactly, without aliases.
    """
    if any(not isfinite(value) or value < 0 for value in (
        absolute_tolerance, relative_tolerance,
    )):
        raise ValueError("Comparison tolerances must be finite and non-negative")
    results: dict[str, Any] = {}
    with closing(open_sqlite_readonly(candidate_path)) as candidate, closing(
        open_sqlite_readonly(reference_path)
    ) as reference:
        for label, connection in (("candidate", candidate), ("reference", reference)):
            _require_v4(connection, label)
        tables = _tables(candidate) | _tables(reference)
        if not include_provenance:
            tables -= PROVENANCE_TABLES
        for table in sorted(tables):
            current_info = _table_info(candidate, table)
            previous_info = _table_info(reference, table)
            if not current_info or not previous_info or current_info != previous_info:
                results[table] = {
                    "comparable": False, "reason": "missing table or different schema",
                    "candidate_columns": [row[1] for row in current_info],
                    "reference_columns": [row[1] for row in previous_info],
                }
                continue
            ignored = (
                set() if include_provenance or table.startswith("data_quality_")
                else PROVENANCE_COLUMNS
            )
            columns = [row[1] for row in current_info if row[1] not in ignored]
            keys = [
                row[1] for row in sorted(current_info, key=lambda row: row[5])
                if row[5] and row[1] not in ignored
            ]
            if not keys:
                # Solver result tables may be unkeyed; preserve multiplicity without
                # inventing a business key or aligning unrelated numeric values.
                projection = ", ".join(quote_identifier(column) for column in columns)
                current = Counter(candidate.execute(f"SELECT {projection} FROM {quote_identifier(table)}"))
                previous = Counter(reference.execute(f"SELECT {projection} FROM {quote_identifier(table)}"))
                results[table] = {
                    "comparable": True, "comparison_grain": "unkeyed exact row multiset",
                    "candidate_rows": current.total(), "reference_rows": previous.total(),
                    "candidate_only_rows": (current - previous).total(),
                    "reference_only_rows": (previous - current).total(),
                }
                continue
            current, current_duplicates = _rows(candidate, table, columns, keys)
            previous, previous_duplicates = _rows(reference, table, columns, keys)
            if current_duplicates or previous_duplicates:
                results[table] = {
                    "comparable": False,
                    "reason": "multiple rows share a key after excluding provenance",
                    "candidate_duplicate_keys": current_duplicates,
                    "reference_duplicate_keys": previous_duplicates,
                }
                continue
            shared = current.keys() & previous.keys()
            differences = 0
            examples = []
            # Sorting by repr also handles nullable keys without comparing None to text.
            for key in sorted(shared, key=repr):
                changes = {}
                for column, value, previous_value in zip(
                    columns, current[key], previous[key], strict=True,
                ):
                    if column not in keys and column not in PROVENANCE_COLUMNS and isinstance(value, (int, float)) and isinstance(
                        previous_value, (int, float),
                    ):
                        equal = isclose(value, previous_value, abs_tol=absolute_tolerance,
                                        rel_tol=relative_tolerance)
                    else:
                        equal = value == previous_value
                    if not equal:
                        changes[column] = {"candidate": value, "reference": previous_value}
                        if isinstance(value, (int, float)) and isinstance(previous_value, (int, float)):
                            changes[column]["absolute_difference"] = abs(value - previous_value)
                if changes:
                    differences += 1
                    if len(examples) < 15:
                        examples.append({"key": key, "changes": changes})
            results[table] = {
                "comparable": True, "key_columns": keys, "compared_columns": columns,
                "candidate_rows": len(current), "reference_rows": len(previous),
                "shared_keys": len(shared),
                "candidate_only_keys": len(current.keys() - previous.keys()),
                "reference_only_keys": len(previous.keys() - current.keys()),
                "value_differences": differences, "difference_examples": examples,
            }
    changed = [
        table for table, result in results.items()
        if not result["comparable"] or any(result.get(field, 0) for field in (
            "candidate_only_keys", "reference_only_keys", "value_differences",
            "candidate_only_rows", "reference_only_rows",
        ))
    ]
    return {
        "enabled": True, "mode": "scenario", "reference": str(reference_path),
        "absolute_tolerance": absolute_tolerance, "relative_tolerance": relative_tolerance,
        "include_provenance": include_provenance, "equivalent": not changed,
        "changed_tables": changed, "tables": results,
        "status": "diagnostic; scenario differences do not imply failed integrity or feasibility",
    }
