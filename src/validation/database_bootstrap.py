"""Focused integrity checks for an initial template-loaded Temoa database."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from validation.sqlite_utils import quote_identifier


def validate_database(
    connection: sqlite3.Connection,
    *,
    expected_primary_keys: Mapping[str, Sequence[tuple[Any, ...]]],
    touched_tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Check touched rows/provenance, foreign keys, and file integrity."""
    schema_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    errors: list[str] = []
    tables: dict[str, dict[str, Any]] = {}
    provenance_audit: dict[str, dict[str, int]] = {}

    for table, expected_keys_sequence in expected_primary_keys.items():
        if table not in schema_tables:
            errors.append(f"Schema is missing loaded table: {table}")
            continue
        table_info = connection.execute(
            f"PRAGMA table_info({quote_identifier(table)})"
        ).fetchall()
        primary_key_columns = [
            row[1] for row in sorted(table_info, key=lambda row: row[5]) if row[5]
        ]
        if not primary_key_columns:
            errors.append(f"Loaded table has no primary key: {table}")
            continue
        selected_columns = ", ".join(
            quote_identifier(column) for column in primary_key_columns
        )
        actual_keys = {
            tuple(row)
            for row in connection.execute(
                f"SELECT {selected_columns} FROM {quote_identifier(table)}"
            ).fetchall()
        }
        expected_keys = {tuple(key) for key in expected_keys_sequence}
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table)}"
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

    for table in dict.fromkeys(touched_tables or expected_primary_keys):
        if table not in schema_tables:
            errors.append(f"Schema is missing touched table: {table}")
            continue
        columns = {
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({quote_identifier(table)})"
            ).fetchall()
        }
        row_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(table)}"
            ).fetchone()[0]
        )
        audit = {"row_count": row_count, "null_data_id": 0, "unregistered_data_id": 0,
                 "unregistered_source_pair": 0}
        if "data_id" in columns and table != "data_set":
            audit["null_data_id"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)} "
                    "WHERE data_id IS NULL"
                ).fetchone()[0]
            )
            audit["unregistered_data_id"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)} AS touched "
                    "LEFT JOIN data_set AS registered ON registered.data_id = touched.data_id "
                    "WHERE touched.data_id IS NOT NULL AND registered.data_id IS NULL"
                ).fetchone()[0]
            )
        if {"data_source", "data_id"}.issubset(columns) and table != "data_source":
            audit["unregistered_source_pair"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)} AS touched "
                    "LEFT JOIN data_source AS registered "
                    "ON registered.source_id = touched.data_source "
                    "AND registered.data_id = touched.data_id "
                    "WHERE touched.data_source IS NOT NULL "
                    "AND touched.data_id IS NOT NULL "
                    "AND registered.source_id IS NULL"
                ).fetchone()[0]
            )
        for check in ("null_data_id", "unregistered_data_id", "unregistered_source_pair"):
            if audit[check]:
                errors.append(f"{table} provenance audit {check}={audit[check]}")
        provenance_audit[table] = audit

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
        "touched_table_audit": provenance_audit,
        "integrity_check": integrity_rows,
        "foreign_key_violations": len(foreign_key_rows),
        "foreign_keys_enabled": foreign_keys_enabled,
    }
