from __future__ import annotations

import csv
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
from canoe_schema.v4_0 import Region, TimePeriod

from build_transport import (
    TemplateLoadError,
    bootstrap_database,
    insert_transport_contribution,
    prepare_transport_contribution,
)
from utils import load_config_bundle
from validation.database_bootstrap import validate_database
from validation.insertion import insert_models
from validation.schema_contract import create_v4_schema, packaged_ddl


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
        "region": csv_row_count(TEMPLATES / "region.csv"),
        "time_period": csv_row_count(TEMPLATES / "time_period.csv"),
    }
    assert database.is_file()
    assert report["ok"] is True
    assert report["lifetimes"]["fixed_rows"] == 570
    assert report["lifetimes"]["curve_rows"] == 9420
    assert report["costs"]["cost_invest_rows"] == 4050
    assert report["costs"]["cost_variable_rows"] > 0
    with sqlite3.connect(database) as lifetime_connection:
        assert lifetime_connection.execute("SELECT COUNT(*) FROM lifetime_tech").fetchone()[0] == 570
        assert lifetime_connection.execute("SELECT COUNT(*) FROM lifetime_survival_curve").fetchone()[0] == 9420
        assert lifetime_connection.execute("SELECT COUNT(*) FROM cost_invest").fetchone()[0] == report["costs"]["cost_invest_rows"]
        assert lifetime_connection.execute("SELECT COUNT(*) FROM cost_variable").fetchone()[0] == report["costs"]["cost_variable_rows"]
        assert lifetime_connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert report["schema"]["package_commit"].startswith("1e68c377")
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
        "nrcan_fuel_consumption_ratings": "T08",
        "nhtsa_cafe_2024_ldv_survival": "T09",
        "eia_nems_hd_truck_scrappage": "T10",
        "jgcri_gcam_motorcycle_inputs": "T11",
        "epri_us_regen_2025_transportation": "T12",
        "faa_economic_values_2024": "T13",
        "wards_intelligence_2022_sales_shares": "T14",
        "emrg_sfu_cims_model": "T15",
        "open_energy_outlook_2022": "T16",
        "argonne_rd_greet_2025_rev1": "T17",
        "epa_moves4_population_activity_2023": "T18",
        "canada_energy_policy_simulator_3_4_7": "T19",
        "argonne_hdsam_4_5": "T20",
        "fueleconomy_gov_vehicle_data": "T21",
        "reviewed_mto_make_model_evidence": "T22",
        "nhtsa_vpic_vehicle_models": "T23",
        "dunsky_ev_charging_infrastructure_2024": "T24",
        "transport_canada_ev_dashboard": "T25",
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
        assert connection.execute(
            "SELECT category, sub_category FROM technology WHERE tech = 'T_MDV_T_BEV_EX'"
        ).fetchone() == ("medium_trucks", "bev")
        assert connection.execute(
            "SELECT flag FROM time_period WHERE period = 2023"
        ).fetchone() == ("e",)
        assert connection.execute("SELECT COUNT(*) FROM existing_capacity").fetchone()[0] == report["existing_capacity"]["parameter_rows"]
        assert connection.execute("SELECT MIN(capacity) FROM existing_capacity").fetchone()[0] >= bundle.scenario.existing_capacity.cleanup_epsilon
        assert connection.execute("SELECT COUNT(*) FROM demand").fetchone()[0] == report["demand"]["parameter_rows"]
        assert connection.execute("SELECT COUNT(DISTINCT region) FROM demand").fetchone()[0] == len(bundle.scenario.geography.regions)
        assert connection.execute("SELECT COUNT(DISTINCT period) FROM demand").fetchone()[0] == len(bundle.scenario.periods.model)
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity").fetchone()[0] == report["road_utilization"]["capacity_to_activity_rows"]
        assert connection.execute("SELECT COUNT(*) FROM limit_annual_capacity_factor").fetchone()[0] == report["road_utilization"]["flat_factor_rows"]
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity WHERE data_source IS NOT NULL OR dq_cred IS NOT NULL").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity WHERE data_id NOT IN (SELECT data_id FROM data_set)").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM data_source").fetchone()[0] > 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_source_label"
        ).fetchone()[0] > 0


def test_fixed_lifetime_mode_inserts_one_row_per_modeled_technology(bundle, tmp_path: Path) -> None:
    scenario = bundle.scenario.model_copy(update={
        "switches": bundle.scenario.switches.model_copy(update={"survival_curves": False}),
    })
    database = tmp_path / "fixed-lifetimes.sqlite"
    report = bootstrap_database(
        bundle=replace(bundle, scenario=scenario),
        template_dir=TEMPLATES,
        database_path=database,
    )
    assert report["lifetimes"]["fixed_rows"] == 1210
    assert report["lifetimes"]["curve_rows"] == 0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lifetime_tech").fetchone()[0] == 1210
        assert connection.execute("SELECT COUNT(*) FROM lifetime_survival_curve").fetchone()[0] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_age_mode_inserts_only_supported_flat_road_factors(bundle, tmp_path: Path) -> None:
    scenario = bundle.scenario.model_copy(update={
        "switches": bundle.scenario.switches.model_copy(update={"vkt_schedules": True}),
    })
    database = tmp_path / "age-mode.sqlite"
    report = bootstrap_database(
        bundle=replace(bundle, scenario=scenario),
        template_dir=TEMPLATES,
        database_path=database,
    )
    assert report["road_utilization"]["sqlite_insertion"] == "blocked_upstream_vintage_plus_period_schema_gap"
    assert report["road_utilization"]["age_factor_rows"] > 0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity").fetchone()[0] == report["road_utilization"]["capacity_to_activity_rows"]
        assert connection.execute("SELECT COUNT(*) FROM limit_annual_capacity_factor").fetchone()[0] == report["road_utilization"]["flat_factor_rows"]
        assert "period" not in {
            row[1] for row in connection.execute("PRAGMA table_info(limit_annual_capacity_factor)")
        }


def test_transport_contribution_uses_a_caller_owned_transaction(
    bundle, tmp_path: Path
) -> None:
    database = tmp_path / "caller-owned.sqlite"
    connection = sqlite3.connect(database, isolation_level=None)
    create_v4_schema(connection)

    contribution = prepare_transport_contribution(
        connection,
        bundle=bundle,
        template_dir=TEMPLATES,
        include_existing_capacity=False,
        include_demand=False,
        include_road_utilization=False,
        include_lifetimes=False,
        include_efficiencies=False,
        include_costs=False,
    )

    assert connection.execute("SELECT COUNT(*) FROM data_set").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM technology").fetchone()[0] == 0

    connection.execute("BEGIN IMMEDIATE")
    inserted = insert_transport_contribution(connection, contribution)
    validation = validate_database(
        connection,
        expected_primary_keys={
            table: [tuple(getattr(row, field) for field in row.__primary_key__)
                    for row in rows]
            for table, rows in inserted.items()
        },
        touched_tables=list(inserted),
    )

    assert validation["ok"] is True
    assert set(inserted) == {
        "data_set",
        "technology_label",
        "commodity_label",
        "technology",
        "commodity",
    }
    assert connection.execute("SELECT COUNT(*) FROM region").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM time_period").fetchone()[0] == 0
    connection.rollback()
    assert connection.execute("SELECT COUNT(*) FROM technology").fetchone()[0] == 0
    connection.close()


def test_existing_capacity_uses_the_same_caller_owned_insertion(bundle) -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(connection)
    contribution = prepare_transport_contribution(
        connection, bundle=bundle, template_dir=TEMPLATES
    )
    assert len(contribution.parameter_rows) == contribution.parameter_audit["parameter_rows"]
    assert len(contribution.demand_rows) == contribution.demand_audit["parameter_rows"]
    assert contribution.parameter_audit["cleanup"]["removed_count"] > 0
    assert connection.execute("SELECT COUNT(*) FROM existing_capacity").fetchone()[0] == 0

    connection.execute("BEGIN IMMEDIATE")
    insert_models(
        connection,
        [Region(region=region) for region in contribution.parameter_audit["regions"]],
    )
    insert_models(
        connection,
        [TimePeriod(period=period, flag="e") for period in bundle.scenario.periods.existing],
    )
    insert_models(
        connection,
        [TimePeriod(period=period, flag="f") for period in bundle.scenario.periods.model],
    )
    inserted = insert_transport_contribution(connection, contribution)
    validation = validate_database(
        connection,
        expected_primary_keys={
            table: [tuple(getattr(row, field) for field in row.__primary_key__) for row in rows]
            for table, rows in inserted.items()
        },
        touched_tables=list(inserted),
    )
    assert validation["ok"] is True
    assert len(inserted["existing_capacity"]) == len(contribution.parameter_rows)
    assert len(inserted["demand"]) == len(contribution.demand_rows)
    connection.rollback()
    assert connection.execute("SELECT COUNT(*) FROM existing_capacity").fetchone()[0] == 0
    connection.close()


def test_transport_contribution_rejects_an_incompatible_caller_schema(
    bundle,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.executescript(packaged_ddl())

    with pytest.raises(TemplateLoadError, match="missing transport columns.*notes"):
        prepare_transport_contribution(
            connection,
            bundle=bundle,
            template_dir=TEMPLATES,
        )

    connection.close()


def test_bootstrap_applies_scenario_economics_and_technology_note(
    bundle, tmp_path: Path
) -> None:
    scenario_payload = bundle.scenario.model_dump(mode="python")
    scenario_payload["economics"] = {
        "global_discount_rate": 0.04,
        "default_loan_rate": 0.05,
        "cost_reference_currency": "CAD",
        "cost_reference_year": 2020,
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
    shutil.copyfile(TEMPLATES / "region.csv", templates / "region.csv")
    shutil.copyfile(TEMPLATES / "time_period.csv", templates / "time_period.csv")

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
    shutil.copyfile(TEMPLATES / "region.csv", templates / "region.csv")
    shutil.copyfile(TEMPLATES / "time_period.csv", templates / "time_period.csv")
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
