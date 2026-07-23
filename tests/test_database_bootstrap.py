from __future__ import annotations

import csv
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from build_transport import TemplateLoadError, bootstrap_database
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"
TEMPLATES = REPO_ROOT / "inputs" / "0_canoe_template"


def csv_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except UnicodeDecodeError:
        with path.open("r", encoding="cp1252", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


def test_bootstrap_uses_packaged_v4_and_loads_validated_templates(
    bundle, tmp_path: Path
) -> None:
    database = tmp_path / "transport.sqlite"

    report = bootstrap_database(
        bundle=bundle,
        template_dir=TEMPLATES,
        database_path=database,
    )

    expected_counts = {
        "technology": csv_row_count(TEMPLATES / "technology.csv"),
        "commodity": csv_row_count(TEMPLATES / "commodity.csv"),
    }
    assert database.is_file()
    assert report["ok"] is True
    assert report["schema"]["package_commit"].startswith("32740578")
    assert report["preflight"]["packaged_rates"] == {
        "global_discount_rate": 0.03,
        "default_loan_rate": 0.03,
    }
    assert report["preflight"]["configured_rates"] == {
        "global_discount_rate": 0.03,
        "default_loan_rate": 0.03,
    }
    assert report["preflight"]["technology_notes"] is True
    assert report["source_id_mapping"] == {
        "nrcan_ceud_transport_provincial": "T01",
        "nrcan_ceud_transport_national": "T02",
        "ontario_ministry_transport_vehicle_population": "T03",
        "statcan_transport_tables": "T04",
        "cer_canadas_energy_future": "T05",
        "nlr_atb_transportation_2024": "T06",
        "anl_autonomie_bean_2022": "T07",
    }
    assert report["template"]["kind"] == "backend_internal_reference"
    assert report["template"]["data_id"].startswith("canoe-transport-template:")
    assert report["validation"]["integrity_check"] == ["ok"]
    assert report["validation"]["foreign_key_violations"] == 0
    assert report["validation"]["foreign_keys_enabled"] is True
    with sqlite3.connect(database) as connection:
        for table, expected_count in expected_counts.items():
            actual_count = connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            assert actual_count == expected_count
        assert connection.execute(
            "SELECT COUNT(*) FROM technology WHERE data_id IS NULL"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM commodity WHERE data_id IS NULL"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT notes FROM technology WHERE tech = 'T_LDV_C_GSL_EX'"
        ).fetchone()[0].startswith("Vehicle class corresponds")
        assert connection.execute("SELECT COUNT(*) FROM data_source").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_source_label"
        ).fetchone()[0] == 0


def test_bootstrap_applies_scenario_economics_and_technology_note(
    bundle, tmp_path: Path
) -> None:
    scenario_payload = bundle.scenario.model_dump(mode="python")
    scenario_payload["economics"] = {
        "global_discount_rate": 0.04,
        "default_loan_rate": 0.05,
    }
    scenario_payload["row_note_overrides"]["technology"] = {
        "T_LDV_C_GSL_EX": "Legacy reproduction override."
    }
    configured_bundle = replace(
        bundle,
        scenario=type(bundle.scenario).model_validate(scenario_payload),
    )
    database = tmp_path / "configured.sqlite"

    report = bootstrap_database(
        bundle=configured_bundle,
        template_dir=TEMPLATES,
        database_path=database,
    )

    assert report["preflight"]["packaged_rates"] == {
        "global_discount_rate": 0.03,
        "default_loan_rate": 0.03,
    }
    assert report["preflight"]["configured_rates"] == {
        "global_discount_rate": 0.04,
        "default_loan_rate": 0.05,
    }
    with sqlite3.connect(database) as connection:
        assert dict(
            connection.execute(
                "SELECT element, value FROM metadata_real "
                "WHERE element IN ('global_discount_rate', 'default_loan_rate')"
            )
        ) == report["preflight"]["configured_rates"]
        assert connection.execute(
            "SELECT notes FROM technology WHERE tech = 'T_LDV_C_GSL_EX'"
        ).fetchone()[0] == "Legacy reproduction override."


def test_bootstrap_rejects_unknown_technology_note_override(
    bundle, tmp_path: Path
) -> None:
    scenario_payload = bundle.scenario.model_dump(mode="python")
    scenario_payload["row_note_overrides"]["technology"] = {
        "T_UNKNOWN": "stale key"
    }
    configured_bundle = replace(
        bundle,
        scenario=type(bundle.scenario).model_validate(scenario_payload),
    )

    with pytest.raises(TemplateLoadError, match="unknown technologies.*T_UNKNOWN"):
        bootstrap_database(
            bundle=configured_bundle,
            template_dir=TEMPLATES,
            database_path=tmp_path / "transport.sqlite",
        )


def test_bootstrap_preserves_notes_and_reports_model_defaults(bundle, tmp_path: Path) -> None:
    report = bootstrap_database(
        bundle=bundle,
        template_dir=TEMPLATES,
        database_path=tmp_path / "transport.sqlite",
    )
    table_reports = {item["table"]: item for item in report["templates"]}

    assert table_reports["technology"]["ignored_fields"] == []
    assert "notes" in table_reports["technology"]["target_columns"]
    assert table_reports["technology"]["source_encoding"] == "utf-8-sig"
    assert table_reports["technology"]["schema_defaults_used"]
    assert table_reports["commodity"]["ignored_fields"] == []
    assert table_reports["commodity"]["missing_optional_fields"] == []


def test_bootstrap_rejects_missing_required_model_field(bundle, tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "technology.csv").write_text("tech\nT_ONE\n", encoding="utf-8")
    shutil.copyfile(TEMPLATES / "commodity.csv", templates / "commodity.csv")

    with pytest.raises(TemplateLoadError, match="missing required technology.*flag"):
        bootstrap_database(
            bundle=bundle,
            template_dir=templates,
            database_path=tmp_path / "transport.sqlite",
        )


def test_bootstrap_rejects_invalid_v4_row_and_leaves_target_unchanged(
    bundle, tmp_path: Path
) -> None:
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "technology.csv").write_text(
        "tech,flag,notes\nT_ONE,not-a-v4-flag,kept\n",
        encoding="utf-8",
    )
    shutil.copyfile(TEMPLATES / "commodity.csv", templates / "commodity.csv")
    database = tmp_path / "transport.sqlite"
    sentinel = b"existing database contents"
    database.write_bytes(sentinel)

    with pytest.raises(TemplateLoadError, match="Invalid technology row"):
        bootstrap_database(
            bundle=bundle,
            template_dir=templates,
            database_path=database,
            overwrite=True,
        )

    assert database.read_bytes() == sentinel


def test_bootstrap_protects_existing_database_without_overwrite(
    bundle, tmp_path: Path
) -> None:
    database = tmp_path / "transport.sqlite"
    sentinel = b"existing database contents"
    database.write_bytes(sentinel)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        bootstrap_database(
            bundle=bundle,
            template_dir=TEMPLATES,
            database_path=database,
        )

    assert database.read_bytes() == sentinel
