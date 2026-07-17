from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from build_transport import TemplateLoadError, bootstrap_database


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA = REPO_ROOT / "inputs" / "temoa_schema_v4.sql"
TEMPLATES = REPO_ROOT / "inputs" / "0_canoe_template"


def csv_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except UnicodeDecodeError:
        with path.open("r", encoding="cp1252", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))


def test_bootstrap_creates_valid_database_and_loads_current_templates(tmp_path: Path) -> None:
    database = tmp_path / "transport.sqlite"

    report = bootstrap_database(
        schema_path=SCHEMA,
        template_dir=TEMPLATES,
        database_path=database,
    )

    expected_counts = {
        "technology": csv_row_count(TEMPLATES / "technology.csv"),
        "commodity": csv_row_count(TEMPLATES / "commodity.csv"),
    }
    assert database.is_file()
    assert report["ok"] is True
    assert report["validation"]["integrity_check"] == ["ok"]
    assert report["validation"]["foreign_key_violations"] == 0
    assert report["validation"]["foreign_keys_enabled"] is True
    with sqlite3.connect(database) as connection:
        for table, expected_count in expected_counts.items():
            actual_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert actual_count == expected_count


def test_bootstrap_reports_ignored_template_fields_and_schema_defaults(tmp_path: Path) -> None:
    report = bootstrap_database(
        schema_path=SCHEMA,
        template_dir=TEMPLATES,
        database_path=tmp_path / "transport.sqlite",
    )
    table_reports = {item["table"]: item for item in report["templates"]}

    assert table_reports["technology"]["ignored_fields"] == ["notes"]
    assert table_reports["technology"]["source_encoding"] == "utf-8-sig"
    assert table_reports["technology"]["schema_defaults_used"]
    assert table_reports["commodity"]["ignored_fields"] == []
    assert table_reports["commodity"]["missing_optional_fields"] == []


def test_bootstrap_rejects_missing_required_template_field(tmp_path: Path) -> None:
    schema = tmp_path / "schema.sql"
    templates = tmp_path / "templates"
    templates.mkdir()
    schema.write_text(
        "CREATE TABLE technology (tech TEXT PRIMARY KEY, flag TEXT NOT NULL);",
        encoding="utf-8",
    )
    (templates / "technology.csv").write_text("tech\nT_ONE\n", encoding="utf-8")
    (templates / "commodity.csv").write_text("name\nC_ONE\n", encoding="utf-8")

    with pytest.raises(TemplateLoadError, match="missing required technology fields.*flag"):
        bootstrap_database(
            schema_path=schema,
            template_dir=templates,
            database_path=tmp_path / "transport.sqlite",
        )


def test_bootstrap_protects_existing_database_without_overwrite(tmp_path: Path) -> None:
    database = tmp_path / "transport.sqlite"
    sentinel = b"existing database contents"
    database.write_bytes(sentinel)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        bootstrap_database(
            schema_path=SCHEMA,
            template_dir=TEMPLATES,
            database_path=database,
        )

    assert database.read_bytes() == sentinel
