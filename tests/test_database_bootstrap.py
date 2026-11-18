from __future__ import annotations

import csv
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
import pandas as pd
from canoe_schema.v4_0 import Region, TimePeriod

from build_transport import (
    TemplateLoadError,
    bootstrap_database,
    insert_transport_contribution,
    prepare_transport_contribution,
)
from parameterization.charger_representation import select_charger_technologies
from utils import load_config_bundle, resolve_artifact_path, resolve_input_path
from validation.database_bootstrap import validate_database
from validation.insertion import insert_models
from validation.schema_contract import create_v4_schema, packaged_ddl


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/validation_opening_periods.yaml"
TEMPLATES = REPO_ROOT / "inputs" / "0_canoe_template"


def csv_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except UnicodeDecodeError:
        with path.open("r", encoding="cp1252", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))


def selected_template_technologies(bundle, template_dir: Path) -> pd.DataFrame:
    with (template_dir / "technology.csv").open(encoding="utf-8-sig", newline="") as handle:
        return select_charger_technologies(bundle, pd.DataFrame(csv.DictReader(handle)))


def modeled_technologies(bundle, template_dir: Path) -> set[str]:
    selected = selected_template_technologies(bundle, template_dir)
    return set(selected.loc[selected.category.ne("") & selected.tech.ne("T_OFF"), "tech"])


@pytest.mark.parametrize("mode,expected_chargers", [
    ("aggregated", {"T_LDV_CHRG_EX", "T_LDV_CHRG_N", "T_MHDV_CHRG_EX", "T_MHDV_CHRG_N"}),
    ("explicit", {"T_LDV_CHRG_PUB_L2", "T_LDV_CHRG_PUB_FC", "T_LDV_CHRG_PRIV",
                  "T_MHDV_CHRG_NIGHT", "T_MHDV_CHRG_FC"}),
])
def test_template_selection_preserves_every_noncharger_owner(mode, expected_chargers):
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "ev_chargers": bundle.scenario.ev_chargers.model_copy(update={"charger_type_classes": mode}),
    }))
    template = resolve_input_path(bundle, "template")
    master = pd.read_csv(template / "technology.csv", keep_default_na=False)
    nonchargers = set(master.loc[master.category.ne("charger"), "tech"])
    selected = selected_template_technologies(bundle, template)
    assert set(selected.tech) == nonchargers | expected_chargers
    assert len(selected) == len(nonchargers) + len(expected_chargers)
    assert modeled_technologies(bundle, template) == set(selected.loc[
        selected.category.ne("") & selected.tech.ne("T_OFF"), "tech",
    ])


@pytest.fixture
def bundle(isolated_legacy_bundle):
    # These cases exercise schema/publication behavior and lifetime switches.
    # Full ten-region production parity is covered by the offline scenario replay.
    payload = isolated_legacy_bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON"]
    return replace(isolated_legacy_bundle, scenario=type(isolated_legacy_bundle.scenario).model_validate(payload))


def test_bootstrap_uses_packaged_v4_and_loads_validated_templates(
    bundle, tmp_path: Path
) -> None:
    database = tmp_path / "transport.sqlite"

    report = bootstrap_database(
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        database_path=database,
    )

    templates = resolve_input_path(bundle, "template")
    expected_technologies = set(selected_template_technologies(bundle, templates).tech)
    expected_counts = {
        "technology": len(expected_technologies),
        "commodity": csv_row_count(templates / "commodity.csv"),
        "region": csv_row_count(templates / "region.csv"),
        "time_period": len(bundle.scenario.periods.all_years()) + 1,
    }
    assert database.is_file()
    assert report["ok"] is True
    regions = len(bundle.scenario.geography.regions)
    expected_lifetime_owners = modeled_technologies(bundle, templates)
    assert report["lifetimes"]["fixed_rows"] == len(expected_lifetime_owners) * regions
    assert report["lifetimes"]["curve_rows"] == 0
    assert report["costs"]["cost_invest_rows"] == 405 * regions
    assert report["costs"]["cost_variable_rows"] > 0
    with sqlite3.connect(database) as lifetime_connection:
        assert set(lifetime_connection.execute("SELECT region, tech FROM lifetime_tech")) == {
            (region, tech) for region in bundle.scenario.geography.regions for tech in expected_lifetime_owners
        }
        assert lifetime_connection.execute("SELECT COUNT(*) FROM lifetime_survival_curve").fetchone()[0] == 0
        assert lifetime_connection.execute("SELECT COUNT(*) FROM cost_invest").fetchone()[0] == report["costs"]["cost_invest_rows"] + report["ev_chargers"]["invest_rows"]
        assert lifetime_connection.execute("SELECT COUNT(*) FROM cost_fixed").fetchone()[0] == report["ev_chargers"]["fixed_rows"]
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
        "nlr_atb_transportation": "T06",
        "anl_autonomie_bean": "T07",
        "nrcan_fuel_consumption_ratings": "T08",
        "nhtsa_cafe_ldv_survival": "T09",
        "eia_nems_hd_truck_scrappage": "T10",
        "jgcri_gcam_motorcycle_inputs": "T11",
        "epri_us_regen_transportation": "T12",
        "faa_economic_values": "T13",
        "wards_intelligence_sales_shares": "T14",
        "emrg_sfu_cims_model": "T15",
        "open_energy_outlook": "T16",
        "argonne_rd_greet": "T17",
        "canada_energy_policy_simulator": "T19",
        "argonne_hdsam": "T20",
        "fueleconomy_gov_vehicle_data": "T21",
        "nhtsa_vpic_vehicle_models": "T23",
        "dunsky_ev_charging_infrastructure": "T24",
        "afdc_alternative_fueling_stations": "T38",
        "nhts_household_travel": "T37",
        "pollution_probe_ev_charging_survey": "T26",
        "nlr_alternative_fueling_infrastructure": "T27",
        "icct_ev_charge": "T28",
        "icct_hdv_charge": "T29",
        "iea_future_of_hydrogen": "T30",
        "legacy_charging_profiles": "T31",
        "epa_omega_baseline": "T33",
        "hatch_original_growth_evidence": "T34",
        "hatch_extended_growth_evidence": "T35",
        "eafo_norway_growth_evidence": "T36",
        "epa_moves_heavy_truck_survival": "T39",
        "transport_canada_izev": "T40",
        "jonas_ev_charging_utilization": "T41",
        "bea_us_gdp_deflator": "T42",
    }
    assert report["template"]["kind"] == "backend_internal_reference"
    assert report["template"]["data_id"].startswith("canoe-transport-template:")
    assert report["validation"]["integrity_check"] == ["ok"]
    assert report["validation"]["foreign_key_violations"] == 0
    assert report["validation"]["foreign_keys_enabled"] is True
    with sqlite3.connect(database) as connection:
        assert set(row[0] for row in connection.execute("SELECT tech FROM technology")) == expected_technologies
        assert set(row[0] for row in connection.execute("SELECT tech FROM technology_label")) == expected_technologies
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
        assert connection.execute("SELECT season, sequence, segment_fraction FROM time_season").fetchall() == [
            ("YEAR", 1, 1.0),
        ]
        assert connection.execute("SELECT tod, sequence, hours FROM time_of_day").fetchall() == [("ALL", 1, 24.0)]
        assert connection.execute("SELECT COUNT(*) FROM capacity_factor_tech").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM existing_capacity").fetchone()[0] == report["existing_capacity"]["parameter_rows"] + report["ev_chargers"]["capacity_rows"]
        assert connection.execute("SELECT MIN(capacity) FROM existing_capacity").fetchone()[0] >= 0
        # The vehicle/offroad owner applies its cleanup threshold before the
        # separate charger owner contributes capacities in GW.
        assert connection.execute(
            "SELECT MIN(e.capacity) FROM existing_capacity e "
            "JOIN technology t USING (tech) WHERE t.category <> 'charger'"
        ).fetchone()[0] >= bundle.scenario.existing_capacity.cleanup_tolerance
        assert connection.execute("SELECT COUNT(*) FROM demand").fetchone()[0] == report["demand"]["parameter_rows"]
        assert connection.execute("SELECT COUNT(DISTINCT region) FROM demand").fetchone()[0] == len(bundle.scenario.geography.regions)
        assert connection.execute("SELECT COUNT(DISTINCT period) FROM demand").fetchone()[0] == len(bundle.scenario.periods.model)
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity").fetchone()[0] == report["road_utilization"]["capacity_to_activity_rows"] + report["ev_chargers"]["capacity_to_activity_rows"]
        assert connection.execute(
            "SELECT COUNT(*) FROM capacity_to_activity c JOIN technology t USING (tech) "
            "WHERE t.category = 'charger' AND c.c2a = 31.536 AND c.units = 'PJ/GW'"
        ).fetchone()[0] == 4 * regions
        assert connection.execute("SELECT COUNT(*) FROM limit_annual_capacity_factor").fetchone()[0] == report["road_utilization"]["flat_factor_rows"] + report["ev_chargers"]["utilization_rows"]
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity WHERE data_source IS NOT NULL OR dq_cred IS NOT NULL").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity WHERE data_id NOT IN (SELECT data_id FROM data_set)").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM data_source").fetchone()[0] > 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_source_label"
        ).fetchone()[0] > 0


def test_fixed_lifetime_mode_inserts_one_row_per_modeled_technology(bundle, tmp_path: Path) -> None:
    scenario = bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": False}),
    })
    database = tmp_path / "fixed-lifetimes.sqlite"
    report = bootstrap_database(
        bundle=replace(bundle, scenario=scenario),
        template_dir=resolve_input_path(bundle, "template"),
        database_path=database,
    )
    expected_rows = len(modeled_technologies(bundle, resolve_input_path(bundle, "template"))) * len(bundle.scenario.geography.regions)
    assert report["lifetimes"]["fixed_rows"] == expected_rows
    assert report["lifetimes"]["curve_rows"] == 0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lifetime_tech").fetchone()[0] == expected_rows
        assert connection.execute("SELECT COUNT(*) FROM lifetime_survival_curve").fetchone()[0] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_invalid_tail_method_fails_before_capacity_publication_or_prerequisite_io(tmp_path, monkeypatch):
    from build_transport import prepare_scenario_inputs
    from parameterization import build_existing_capacity as capacity

    original = load_config_bundle("config/scenarios/default.yaml", repo_root=REPO_ROOT)
    scenario = original.scenario.model_copy(update={
        "lifetimes": original.scenario.lifetimes.model_copy(update={"survival_curves": True, "tail_derivation": "unsupported"}),
    })
    routes = dict(original.paths.artifacts)
    routes["existing_capacity_validation"] = routes["existing_capacity_validation"].model_copy(
        update={"path": str(tmp_path / "capacity_validation")})
    pending = replace(original, scenario=scenario, paths=original.paths.model_copy(update={"artifacts": routes}))
    marker = resolve_artifact_path(pending, "existing_capacity_validation", "integrity.json")
    marker.parent.mkdir()
    marker.write_bytes(b"previous completed family")

    def forbidden(*_args, **_kwargs):
        pytest.fail("Invalid tail method reached prerequisite I/O or capacity publication")

    monkeypatch.setattr(capacity, "read_ceud_input", forbidden)
    monkeypatch.setattr(capacity, "write_dataframe_atomic", forbidden)
    with pytest.raises(ValueError, match="tail_derivation"):
        capacity.prepare_existing_capacity_rows(pending)
    assert marker.read_bytes() == b"previous completed family"
    with pytest.raises(ValueError, match="tail_derivation"):
        prepare_scenario_inputs(pending, download=False)
    with pytest.raises(ValueError, match="tail_derivation"):
        prepare_transport_contribution(None, bundle=pending, template_dir=tmp_path / "unread-template")


def test_curve_mode_inserts_complete_annual_curves_with_process_endpoints(bundle, tmp_path):
    payload = bundle.scenario.model_dump(mode="python")
    payload["lifetimes"].update(survival_curves=True, survival_curve_max_age=None)
    selected = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    database = tmp_path / "survival-curves.sqlite"
    report = bootstrap_database(
        bundle=selected, template_dir=resolve_input_path(selected, "template"), database_path=database,
    )
    assert report["ok"]
    assert report["lifetimes"]["curve_rows"] > report["lifetimes"]["process_rows"] > 0
    assert report["existing_capacity"]["grouped_tail_reference_loss_k_vehicles"] == 0
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT c.region,c.tech,c.vintage,MIN(c.period),MAX(c.period),COUNT(*),p.lifetime "
            "FROM lifetime_survival_curve c JOIN lifetime_process p USING (region,tech,vintage) "
            "GROUP BY c.region,c.tech,c.vintage"
        ).fetchall()
        assert len(rows) == report["lifetimes"]["process_rows"]
        for region, tech, vintage, first, last, count, lifetime in rows:
            assert first == vintage and last == vintage + lifetime and count == last - first + 1
            assert connection.execute(
                "SELECT fraction FROM lifetime_survival_curve WHERE region=? AND tech=? AND vintage=? AND period IN (?,?) ORDER BY period",
                (region, tech, vintage, first, last),
            ).fetchall() == [(1.0,), (0.0,)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_age_evidence_cannot_publish_a_database_missing_road_factors(bundle, tmp_path: Path) -> None:
    # Complete source support still cannot supply the missing v4 period dimension.
    payload = bundle.scenario.model_dump(mode="python")
    payload["lifetimes"]["survival_curve_max_age"] = 25
    payload["road_utilization"].update(vkt_schedules=True)
    scenario = type(bundle.scenario).model_validate(payload)
    database = tmp_path / "age-mode.sqlite"
    with pytest.raises(ValueError, match="vkt_schedules requires dual-index LACF insertion"):
        bootstrap_database(
            bundle=replace(bundle, scenario=scenario),
            template_dir=resolve_input_path(bundle, "template"),
            database_path=database,
        )
    assert not database.exists()


def test_full_survival_horizon_holds_registered_vkt_endpoints(bundle):
    from parameterization.road_utilization import _survival_age_limit, prepare_age_profiles

    # The configured endpoint policy extends the production MOVES horizon;
    # unchanged registered source observations remain the physical evidence.
    assert bundle.scenario.lifetimes.survival_curve_max_age is None
    ceiling = _survival_age_limit(bundle)
    assert ceiling > 33
    profiles = prepare_age_profiles(bundle, output_regions={"ON": "ON"})
    assert set(profiles.endpoint_policy) == {"hold_last"}
    for _, group in profiles.groupby("nrcan_ceud_class"):
        assert group.age.tolist() == list(range(ceiling + 1))
        assert group.loc[group.age.ge(34), "normalized_utilization"].nunique() == 1


def test_transport_contribution_uses_a_caller_owned_transaction(
    bundle, tmp_path: Path
) -> None:
    database = tmp_path / "caller-owned.sqlite"
    connection = sqlite3.connect(database, isolation_level=None)
    create_v4_schema(connection)

    contribution = prepare_transport_contribution(
        connection,
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        include_existing_capacity=False,
        include_demand=False,
        include_road_utilization=False,
        include_lifetimes=False,
        include_efficiencies=False,
        include_costs=False,
        include_emission_embodied=False,
        include_charging_profiles=False,
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
    assert connection.execute("SELECT COUNT(*) FROM time_season").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM time_of_day").fetchone()[0] == 0
    connection.rollback()
    assert connection.execute("SELECT COUNT(*) FROM technology").fetchone()[0] == 0
    connection.close()


def test_existing_capacity_uses_the_same_caller_owned_insertion(bundle) -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(connection)
    contribution = prepare_transport_contribution(
        connection, bundle=bundle, template_dir=resolve_input_path(bundle, "template")
    )
    assert len(contribution.parameter_rows) == contribution.parameter_audit["parameter_rows"] + contribution.charger_audit["capacity_rows"]
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
            template_dir=resolve_input_path(bundle, "template"),
        )

    connection.close()


def test_bootstrap_applies_scenario_economics_and_technology_note(
    bundle, tmp_path: Path
) -> None:
    scenario_payload = bundle.scenario.model_dump(mode="python")
    scenario_payload["economics"] = {
        "cer_scenario": scenario_payload["economics"]["cer_scenario"],
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
        template_dir=resolve_input_path(bundle, "template"),
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
            template_dir=resolve_input_path(bundle, "template"),
            database_path=tmp_path / "transport.sqlite",
        )


def test_bootstrap_preserves_notes_and_reports_model_defaults(bundle, tmp_path: Path) -> None:
    report = bootstrap_database(
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
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
            template_dir=resolve_input_path(bundle, "template"),
            database_path=database,
        )

    assert database.read_bytes() == sentinel


@pytest.mark.parametrize("interruption", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_real_capacity_publication_failure_requires_complete_repreparation(
    bundle, monkeypatch, interruption,
) -> None:
    """A partial real family cannot masquerade as the previously completed input."""
    from parameterization import build_existing_capacity as capacity

    prepared = capacity.prepare_existing_capacity_rows(bundle)
    rows, contexts, audit = prepared.existing_capacity_rows, prepared.provenance_contexts, prepared.audit
    capacity.validate_capacity_handoff(bundle, rows=rows, contexts=contexts, audit=audit)
    marker = resolve_artifact_path(bundle, "existing_capacity_validation", "integrity.json")
    previous_marker = marker.read_bytes()
    previous_artifacts = {}
    for artifact in audit["prepared_handoff"]["artifacts"]:
        path = bundle.repo_root / artifact["path"]
        assert path.is_relative_to(bundle.repo_root)
        previous_artifacts[path] = path.read_bytes()

    original_write = capacity.write_dataframe_atomic
    writes = 0

    def interrupt_second_write(frame, path):
        nonlocal writes
        writes += 1
        assert not marker.exists()
        if writes == 2:
            raise interruption("interrupted capacity family publication")
        original_write(frame, path)

    with monkeypatch.context() as patch:
        patch.setattr(capacity, "write_dataframe_atomic", interrupt_second_write)
        with pytest.raises(interruption, match="interrupted capacity family publication"):
            capacity.prepare_existing_capacity_rows(bundle)
    assert writes == 2
    assert not marker.exists()
    with pytest.raises(ValueError, match="no readable completion marker"):
        capacity.validate_capacity_handoff(bundle, rows=rows, contexts=contexts, audit=audit)

    recovered = capacity.prepare_existing_capacity_rows(bundle)
    recovered_rows, recovered_contexts, recovered_audit = (
        recovered.existing_capacity_rows, recovered.provenance_contexts, recovered.audit
    )
    assert recovered_rows == rows
    assert recovered_contexts == contexts
    assert recovered_audit == audit
    assert marker.read_bytes() == previous_marker
    assert {path: path.read_bytes() for path in previous_artifacts} == previous_artifacts
    capacity.validate_capacity_handoff(
        bundle, rows=recovered_rows, contexts=recovered_contexts, audit=recovered_audit,
    )
