"""Create an initial Temoa database and load template metadata tables."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sqlite3
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from utils import (
    load_config_bundle,
    resolve_configured_path,
    resolve_input_path,
    resolve_repo_path,
    validate_config_bundle,
)
from validation.database_bootstrap import validate_database


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TemplateTable:
    """One explicitly supported bootstrap template."""

    table: str
    filename: str


@dataclass(frozen=True)
class SchemaColumn:
    """Relevant SQLite column metadata returned by PRAGMA table_info."""

    name: str
    declared_type: str
    not_null: bool
    default: str | None
    primary_key_position: int


@dataclass(frozen=True)
class TableLoadResult:
    """Auditable result of loading one CSV template."""

    table: str
    template: str
    source_encoding: str
    source_rows: int
    inserted_rows: int
    csv_columns: list[str]
    target_columns: list[str]
    inserted_columns: list[str]
    ignored_fields: list[str]
    missing_optional_fields: list[str]
    schema_defaults_used: dict[str, int]
    blank_values_as_null: dict[str, int]
    primary_key_columns: list[str]
    primary_keys: list[tuple[Any, ...]]


TEMPLATE_TABLES = (
    TemplateTable(table="technology", filename="technology.csv"),
    TemplateTable(table="commodity", filename="commodity.csv"),
)


class TemplateLoadError(ValueError):
    """Raised when a template cannot be mapped safely to its target table."""


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def inspect_table(connection: sqlite3.Connection, table: str) -> list[SchemaColumn]:
    """Return target-table metadata without assuming a fixed column contract."""
    rows = connection.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    if not rows:
        raise TemplateLoadError(f"Target schema does not define table {table!r}")
    return [
        SchemaColumn(
            name=row[1],
            declared_type=row[2],
            not_null=bool(row[3]),
            default=row[4],
            primary_key_position=int(row[5]),
        )
        for row in rows
    ]


def _read_template(
    path: Path,
) -> tuple[list[str], list[dict[str, str | None]], str]:
    if not path.is_file():
        raise FileNotFoundError(f"Template CSV does not exist: {path}")
    fields: list[str] | None = None
    rows: list[dict[str, str | None]] = []
    selected_encoding = ""
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames
                rows = list(reader)
        except UnicodeDecodeError:
            continue
        selected_encoding = encoding
        break
    if fields is None:
        raise TemplateLoadError(f"Template CSV has no readable header: {path}")
    if not fields:
        raise TemplateLoadError(f"Template CSV has no header: {path}")
    duplicates = sorted(field for field, count in Counter(fields).items() if count > 1)
    if duplicates:
        raise TemplateLoadError(f"Template CSV has duplicate fields {duplicates}: {path}")
    for row_number, row in enumerate(rows, start=2):
        if None in row:
            raise TemplateLoadError(f"Template CSV row {row_number} has extra values: {path}")
    return fields, rows, selected_encoding


def load_template_table(
    connection: sqlite3.Connection,
    *,
    table: str,
    template_path: Path,
) -> TableLoadResult:
    """Insert compatible CSV fields, using SQLite defaults for blank defaulted fields."""
    schema_columns = inspect_table(connection, table)
    columns_by_name = {column.name: column for column in schema_columns}
    csv_columns, rows, source_encoding = _read_template(template_path)
    ignored_fields = [field for field in csv_columns if field not in columns_by_name]
    missing_fields = [column for column in schema_columns if column.name not in csv_columns]
    missing_required = [
        column.name
        for column in missing_fields
        if column.primary_key_position or (column.not_null and column.default is None)
    ]
    if missing_required:
        raise TemplateLoadError(
            f"Template {template_path} is missing required {table} fields: {missing_required}"
        )

    inserted_columns = [field for field in csv_columns if field in columns_by_name]
    primary_key_columns = [
        column.name
        for column in sorted(schema_columns, key=lambda item: item.primary_key_position)
        if column.primary_key_position
    ]
    default_counts: Counter[str] = Counter()
    null_counts: Counter[str] = Counter()
    primary_keys: list[tuple[Any, ...]] = []

    for row_number, row in enumerate(rows, start=2):
        values: dict[str, Any] = {}
        for field in inserted_columns:
            raw_value = row[field]
            column = columns_by_name[field]
            if raw_value == "" and column.default is not None:
                default_counts[field] += 1
                continue
            value = None if raw_value == "" else raw_value
            if value is None and (column.primary_key_position or column.not_null):
                raise TemplateLoadError(
                    f"Template {template_path} row {row_number} has a blank required field: {field}"
                )
            if value is None:
                null_counts[field] += 1
            values[field] = value

        primary_keys.append(tuple(row[field] for field in primary_key_columns))
        quoted_columns = ", ".join(_quote_identifier(field) for field in values)
        placeholders = ", ".join("?" for _ in values)
        statement = (
            f"INSERT INTO {_quote_identifier(table)} ({quoted_columns}) "
            f"VALUES ({placeholders})"
        )
        try:
            connection.execute(statement, tuple(values.values()))
        except sqlite3.Error as exc:
            raise TemplateLoadError(
                f"Failed to insert {template_path} row {row_number} into {table}: {exc}"
            ) from exc

    return TableLoadResult(
        table=table,
        template=str(template_path),
        source_encoding=source_encoding,
        source_rows=len(rows),
        inserted_rows=len(rows),
        csv_columns=csv_columns,
        target_columns=[column.name for column in schema_columns],
        inserted_columns=inserted_columns,
        ignored_fields=ignored_fields,
        missing_optional_fields=[column.name for column in missing_fields],
        schema_defaults_used=dict(sorted(default_counts.items())),
        blank_values_as_null=dict(sorted(null_counts.items())),
        primary_key_columns=primary_key_columns,
        primary_keys=primary_keys,
    )


def bootstrap_database(
    *,
    schema_path: Path,
    template_dir: Path,
    database_path: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Build and validate a database before atomically publishing it."""
    if database_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing SQLite database: {database_path}. "
            "Pass --overwrite to replace it explicitly."
        )
    if not schema_path.is_file():
        raise FileNotFoundError(f"Temoa schema does not exist: {schema_path}")
    if not template_dir.is_dir():
        raise FileNotFoundError(f"Template directory does not exist: {template_dir}")

    database_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{database_path.name}.",
        suffix=".tmp",
        dir=database_path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(temporary_path)
        connection.executescript(schema_path.read_text(encoding="utf-8"))
        connection.execute("PRAGMA foreign_keys = ON")
        load_results: list[TableLoadResult] = []
        with connection:
            for specification in TEMPLATE_TABLES:
                load_results.append(
                    load_template_table(
                        connection,
                        table=specification.table,
                        template_path=template_dir / specification.filename,
                    )
                )

        validation = validate_database(
            connection,
            expected_primary_keys={
                result.table: result.primary_keys for result in load_results
            },
        )
        if not validation["ok"]:
            raise TemplateLoadError(
                f"Database validation failed before publish: {validation['errors']}"
            )
        connection.close()
        connection = None
        os.replace(temporary_path, database_path)
    except Exception:
        if connection is not None:
            connection.close()
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "ok": True,
        "database": str(database_path),
        "schema": str(schema_path),
        "templates": [
            {key: value for key, value in asdict(result).items() if key != "primary_keys"}
            for result in load_results
        ],
        "validation": validation,
    }


def write_validation_report(report: dict[str, Any], path: Path) -> None:
    """Write the concise configured validation artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def build_from_scenario(
    scenario_path: str | Path,
    *,
    overwrite: bool = False,
) -> tuple[dict[str, Any], Path]:
    """Resolve scenario paths, bootstrap the database, and write validation JSON."""
    bundle = load_config_bundle(scenario_path)
    config_errors = validate_config_bundle(bundle)
    if config_errors:
        raise ValueError(f"Invalid configuration: {config_errors}")
    if bundle.scenario["switches"].get("compile_sqlite") is not True:
        raise ValueError(
            "Scenario switch switches.compile_sqlite must be true for database bootstrap"
        )

    database_path = resolve_configured_path(
        bundle,
        "outputs",
        "sqlite",
        bundle.scenario["outputs"]["sqlite_name"],
    )
    report = bootstrap_database(
        schema_path=resolve_input_path(bundle, "schema"),
        template_dir=resolve_input_path(bundle, "template"),
        database_path=database_path,
        overwrite=overwrite,
    )
    report.update(
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "scenario": bundle.scenario["scenario"]["name"],
            "scenario_path": str(bundle.scenario_path),
        }
    )
    validation_path = resolve_repo_path(
        bundle.repo_root,
        bundle.scenario["outputs"]["validation_report"],
    )
    write_validation_report(report, validation_path)
    return report, validation_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
        help="Path to the scenario YAML file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly replace the configured SQLite output if it already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report, validation_path = build_from_scenario(
            args.scenario,
            overwrite=args.overwrite,
        )
    except (FileNotFoundError, FileExistsError, TemplateLoadError, ValueError) as exc:
        raise SystemExit(f"Database bootstrap failed: {exc}") from exc
    LOGGER.info(
        "Created %s and loaded %s template tables; validation: %s",
        report["database"],
        len(report["templates"]),
        validation_path,
    )


if __name__ == "__main__":
    main()
