"""Pinned CANOE v4 schema integration and the transport notes extension."""

from __future__ import annotations

import hashlib
import importlib.metadata
import sqlite3
from typing import Any

import canoe_schema
from canoe_schema.v4_0 import (
    DataQualityCredibilityLevel,
    DataQualityGeographyLevel,
    DataQualityStructureLevel,
    DataQualityTechnologyLevel,
    DataQualityTimeLevel,
    Technology,
)


SCHEMA_VERSION = "4.0"
SCHEMA_PACKAGE = "canoe-schema"
SCHEMA_COMMIT = "32740578f62fe9cfc760034be0201b4dcaf7c653"
TECHNOLOGY_NOTES_EXTENSION_SQL = "ALTER TABLE technology ADD COLUMN notes TEXT;"


class TransportationTechnology(Technology):
    """Temporary v4 Technology extension for dataset-specific transport notes.

    Remove this subclass and the DDL extension when the pinned upstream v4 model and
    schema both expose ``technology.notes``.
    """

    notes: str | None = None


def packaged_ddl() -> str:
    """Return the pinned package's canonical v4 DDL text."""
    return canoe_schema.get_sql_schema(SCHEMA_VERSION)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def schema_evidence() -> dict[str, str]:
    """Return reproducibility evidence for the installed schema contract."""
    ddl = packaged_ddl()
    return {
        "package": SCHEMA_PACKAGE,
        "package_version": importlib.metadata.version(SCHEMA_PACKAGE),
        "package_commit": SCHEMA_COMMIT,
        "schema_version": SCHEMA_VERSION,
        "ddl_sha256": sha256_text(ddl),
        "technology_notes_extension_sha256": sha256_text(
            TECHNOLOGY_NOTES_EXTENSION_SQL
        ),
    }


def _column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}


def apply_technology_notes_extension(connection: sqlite3.Connection) -> bool:
    """Apply the approved one-column compatibility extension when still required."""
    columns = _column_names(connection, "technology")
    if not columns:
        raise RuntimeError("Packaged v4 DDL did not create the technology table")
    if "notes" in columns:
        return False
    connection.execute(TECHNOLOGY_NOTES_EXTENSION_SQL)
    if "notes" not in _column_names(connection, "technology"):
        raise RuntimeError("technology.notes compatibility extension was not applied")
    return True


def preflight_schema(connection: sqlite3.Connection) -> dict[str, Any]:
    """Verify the small set of package contracts required before backend writes."""
    rates = {
        str(name): float(value)
        for name, value in connection.execute(
            "SELECT element, value FROM metadata_real "
            "WHERE element IN ('global_discount_rate', 'default_loan_rate')"
        )
    }
    expected_rates = {
        "global_discount_rate": 0.03,
        "default_loan_rate": 0.03,
    }
    if rates != expected_rates:
        raise RuntimeError(
            f"Packaged v4 discount-rate contract changed: {rates}; "
            f"expected {expected_rates}"
        )

    version = dict(
        connection.execute(
            "SELECT element, value FROM metadata "
            "WHERE element IN ('DB_MAJOR', 'DB_MINOR')"
        )
    )
    if version != {"DB_MAJOR": 4, "DB_MINOR": 0}:
        raise RuntimeError(f"Packaged schema version is not 4.0: {version}")

    enum_contracts = {
        "data_quality_credibility": DataQualityCredibilityLevel,
        "data_quality_geography": DataQualityGeographyLevel,
        "data_quality_structure": DataQualityStructureLevel,
        "data_quality_technology": DataQualityTechnologyLevel,
        "data_quality_time": DataQualityTimeLevel,
    }
    dq_lookups: dict[str, list[int]] = {}
    for table, enum_type in enum_contracts.items():
        column = next(iter(_column_names(connection, table) - {"description"}))
        values = [
            int(row[0])
            for row in connection.execute(
                f'SELECT "{column}" FROM "{table}" ORDER BY "{column}"'
            )
        ]
        expected = [int(member.value) for member in enum_type]
        if values != expected:
            raise RuntimeError(
                f"Packaged DQ lookup {table} changed: {values}; expected {expected}"
            )
        dq_lookups[table] = values

    if "notes" not in _column_names(connection, "technology"):
        raise RuntimeError("Effective schema is missing approved technology.notes")
    if not connection.execute("PRAGMA foreign_keys").fetchone()[0]:
        raise RuntimeError("SQLite foreign-key enforcement is disabled")

    return {
        "schema_version": SCHEMA_VERSION,
        "rates": rates,
        "data_quality_lookups": dq_lookups,
        "technology_notes": True,
        "foreign_keys_enabled": True,
    }


def create_v4_schema(connection: sqlite3.Connection) -> dict[str, Any]:
    """Create the pinned package schema, add notes, and enable foreign keys."""
    connection.executescript(packaged_ddl())
    extension_applied = apply_technology_notes_extension(connection)
    connection.execute("PRAGMA foreign_keys = ON")
    preflight = preflight_schema(connection)
    preflight["technology_notes_extension_applied"] = extension_applied
    return preflight
