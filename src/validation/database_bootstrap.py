"""Focused integrity checks for an initial template-loaded Temoa database."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def validate_database(
    connection: sqlite3.Connection,
    *,
    expected_primary_keys: Mapping[str, Sequence[tuple[Any, ...]]],
) -> dict[str, Any]:
    """Check schema presence, loaded keys/counts, foreign keys, and file integrity."""
    schema_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    errors: list[str] = []
    tables: dict[str, dict[str, Any]] = {}

    for table, expected_keys_sequence in expected_primary_keys.items():
        if table not in schema_tables:
            errors.append(f"Schema is missing loaded table: {table}")
            continue
        table_info = connection.execute(
            f"PRAGMA table_info({_quote_identifier(table)})"
        ).fetchall()
        primary_key_columns = [
            row[1] for row in sorted(table_info, key=lambda row: row[5]) if row[5]
        ]
        if not primary_key_columns:
            errors.append(f"Loaded table has no primary key: {table}")
            continue
        selected_columns = ", ".join(
            _quote_identifier(column) for column in primary_key_columns
        )
        actual_keys = {
            tuple(row)
            for row in connection.execute(
                f"SELECT {selected_columns} FROM {_quote_identifier(table)}"
            ).fetchall()
        }
        expected_keys = {tuple(key) for key in expected_keys_sequence}
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(table)}"
        ).fetchone()[0]
        keys_match = actual_keys == expected_keys
        if row_count != len(expected_keys_sequence):
            errors.append(
                f"{table} row count {row_count} does not match source count "
                f"{len(expected_keys_sequence)}"
            )
        if not keys_match:
            errors.append(f"{table} primary-key values do not match the template")
        tables[table] = {
            "row_count": row_count,
            "primary_key_columns": primary_key_columns,
            "primary_keys_match": keys_match,
        }

    integrity_rows = [row[0] for row in connection.execute("PRAGMA integrity_check")]
    if integrity_rows != ["ok"]:
        errors.append(f"SQLite integrity_check failed: {integrity_rows}")
    foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_rows:
        errors.append(f"SQLite foreign_key_check found {len(foreign_key_rows)} violation(s)")
    foreign_keys_enabled = bool(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    if not foreign_keys_enabled:
        errors.append("SQLite foreign-key enforcement is disabled")

    return {
        "ok": not errors,
        "errors": errors,
        "schema_table_count": len(schema_tables),
        "tables": tables,
        "integrity_check": integrity_rows,
        "foreign_key_violations": len(foreign_key_rows),
        "foreign_keys_enabled": foreign_keys_enabled,
    }
