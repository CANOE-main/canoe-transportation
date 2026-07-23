"""Small common-column comparison for legacy-reproduction bootstrap tables."""

from __future__ import annotations

import sqlite3
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
