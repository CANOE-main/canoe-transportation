"""Pinned CANOE v4 schema integration and the transport notes extension."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
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
SCHEMA_REPOSITORY = "https://github.com/CANOE-main/canoe-schema.git"
SCHEMA_BRANCH = "main"
SCHEMA_COMMIT = "1e68c377d5a7499c78b009d7c472ffd5a6b44901"
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


def installed_vcs_evidence() -> dict[str, str]:
    """Verify the installed package came from the reviewed main-branch commit."""
    distribution = importlib.metadata.distribution(SCHEMA_PACKAGE)
    direct_url_text = distribution.read_text("direct_url.json")
    if direct_url_text is None:
        raise RuntimeError(f"Installed {SCHEMA_PACKAGE} has no direct_url.json")
    try:
        direct_url = json.loads(direct_url_text)
        repository = str(direct_url["url"])
        vcs_info = direct_url["vcs_info"]
        vcs = str(vcs_info["vcs"])
        requested_revision = str(vcs_info["requested_revision"])
        commit = str(vcs_info["commit_id"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Installed {SCHEMA_PACKAGE} has invalid VCS direct_url.json"
        ) from exc

    actual = {
        "package_repository": repository,
        "package_vcs": vcs,
        "package_requested_revision": requested_revision,
        "package_commit": commit,
    }
    expected = {
        "package_repository": SCHEMA_REPOSITORY,
        "package_vcs": "git",
        "package_requested_revision": SCHEMA_BRANCH,
        "package_commit": SCHEMA_COMMIT,
    }
    if actual != expected:
        raise RuntimeError(
            f"Installed {SCHEMA_PACKAGE} VCS contract changed: {actual}; "
            f"expected {expected}"
        )
    return actual


def schema_evidence() -> dict[str, str]:
    """Return reproducibility evidence for the installed schema contract."""
    ddl = packaged_ddl()
    vcs_evidence = installed_vcs_evidence()
    return {
        "package": SCHEMA_PACKAGE,
        "package_version": importlib.metadata.version(SCHEMA_PACKAGE),
        **vcs_evidence,
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
