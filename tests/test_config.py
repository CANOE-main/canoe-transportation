from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from utils import (
    configured_directories,
    load_config_bundle,
    load_harmonization_rules,
    load_parameter_yaml,
    load_yaml,
    resolve_artifact_path,
    resolve_input_path,
)
from validation.config_models import ScenarioRowNoteOverrides, SourceComponent
from validation.config_smoke import run_smoke_validation


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def test_config_bundle_loads_typed_contracts() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert bundle.paths.inputs.template == "inputs/0_canoe_template"
    assert bundle.paths.inputs.validation == "inputs/validation"
    assert bundle.scenario.scenario.name == "legacy_reproduction"
    assert bundle.scenario.geography.regions == ["ON", "AB", "BCT", "MB", "NB", "NL", "NS", "PE", "QC", "SK"]
    assert bundle.scenario.periods.existing == [2000, 2005, 2010, 2015, 2020, 2023]
    assert bundle.scenario.periods.model == [
        2025,
        2030,
        2035,
        2040,
        2045,
    ]
    assert bundle.scenario.periods.all_years() == [
        2000,
        2005,
        2010,
        2015,
        2020,
        2023,
        2025,
        2030,
        2035,
        2040,
        2045,
    ]
    assert bundle.scenario.sources.selections[
        "cer_canadas_energy_future"
    ].edition == 2026
    assert bundle.scenario.demand.cer_scenario == "Current Measures"
    assert bundle.scenario.demand.future_car_demand == "GDP-indexed"
    assert bundle.scenario.sources.selections["transport_canada_ev_dashboard"].year == 2026
    assert bundle.scenario.aggregation_sources.source_for("stock_age", "QC") == "ontario_ministry_transport_vehicle_population"
    assert bundle.scenario.existing_capacity.cleanup_tolerance == 0.001
    assert bundle.scenario.aggregation_sources.source_for("medium_trucks", "QC") == "wards_intelligence_2022_sales_shares"
    assert bundle.scenario.ev_chargers.ld_evs_per_port == 1.0
    assert bundle.scenario.ev_chargers.mhd_evs_per_port == 1.5
    assert bundle.scenario.economics.global_discount_rate == 0.03
    assert bundle.sources.sources["statcan_transport_tables"].component(
        "20-10-0021-01"
    ).short_name == "archived_ldv_new_registrations"
    assert bundle.sources.sources["cer_canadas_energy_future"].component(
        "macro-indicators"
    ).short_name == "macro_indicators_for_demand_and_currency_conversion"


def test_registry_owns_source_availability_and_template_is_not_a_source() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert "active" not in type(bundle.scenario.sources).model_fields
    assert bundle.sources.sources["cer_canadas_energy_future"].status == "active"
    assert (
        bundle.sources.sources["wards_intelligence_2022_sales_shares"].status
        == "active"
    )
    assert "canoe_transport_template" not in bundle.sources.sources


def test_source_component_vocabularies_are_canonical() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    architecture_modules = {
        "ev_chargers",
        "offroad_capex_opex",
        "offroad_efficiencies",
        "offroad_lifetimes",
        "offroad_stocks_and_demands",
        "road_aggregation",
        "road_utilization",
        "road_capex_opex",
        "road_efficiencies",
        "road_lifetimes_survival",
        "road_stocks_and_demands",
    }
    forbidden_aliases = {
        "benchmarking",
        "fit-active_stock",
        "on_road_effs_and_costs",
        "on_road_variable_costs",
        "road_vehicle_class_mapping",
        "urban_transit",
        "weightclass",
    }
    displaced_module_owners = {
        "capex_opex",
        "efficiencies",
        "lifetimes_survival",
        "stocks_and_demands",
    }

    for source in bundle.sources.sources.values():
        for component in source.components.values():
            for field_name in (
                "inputs",
                "applies_to",
                "produces",
                "parameter_modules",
            ):
                values = getattr(component, field_name)
                assert not forbidden_aliases.intersection(values)
                assert all(re.fullmatch(r"[a-z][a-z0-9_]*", value) for value in values)
            assert set(component.parameter_modules) <= architecture_modules
            assert not displaced_module_owners.intersection(
                component.parameter_modules
            )


def test_source_components_follow_road_and_offroad_owners() -> None:
    sources = load_config_bundle(SCENARIO, repo_root=REPO_ROOT).sources.sources

    provincial = sources["nrcan_ceud_transport_provincial"]
    assert provincial.component("20").parameter_modules == [
        "road_stocks_and_demands"
    ]
    assert provincial.component("14").parameter_modules == [
        "offroad_stocks_and_demands"
    ]
    assert sources["nhtsa_cafe_2024_ldv_survival"].component(
        "ldv_survival_rates"
    ).parameter_modules == ["road_lifetimes_survival"]
    assert sources["emrg_sfu_cims_model"].component(
        "transport_process_lifetimes"
    ).parameter_modules == ["offroad_lifetimes"]
    assert sources["dunsky_ev_charging_infrastructure_2024"].component(
        "charger_shares_and_utilization"
    ).parameter_modules == ["ev_chargers"]


def test_absolute_and_relative_component_roles_are_distinct() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    sources = bundle.sources.sources

    assert sources["epri_us_regen_2025_transportation"].component(
        "nonroad_cost_invest_multipliers"
    ).produces == ["cost_invest_multiplier"]
    assert sources["epri_us_regen_2025_transportation"].component(
        "nonroad_efficiency_multipliers"
    ).produces == ["efficiency_multiplier"]
    assert sources["open_energy_outlook_2022"].component(
        "transport_variable_cost_multipliers"
    ).produces == ["cost_variable_multiplier"]
    assert sources["argonne_rd_greet_2025_rev1"].component(
        "marine_hfo_energy_intensity"
    ).produces == ["efficiency_multiplier"]

    assert sources["emrg_sfu_cims_model"].component(
        "transport_service_output_and_capex"
    ).produces == ["cost_invest"]
    assert sources["jgcri_gcam_motorcycle_inputs"].component(
        "canada_motorcycle_inputs"
    ).produces == ["vehicle_efficiency", "vehicle_cost", "vehicle_variable_costs"]


def test_inactive_registry_source_cannot_be_selected(tmp_path: Path) -> None:
    _, sources_path, scenario = _write_config_copy(tmp_path)
    source_payload = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
    source_payload["sources"]["wards_intelligence_2022_sales_shares"]["status"] = "inactive"
    sources_path.write_text(yaml.safe_dump(source_payload, sort_keys=False), encoding="utf-8")
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["aggregation_sources"]["medium_trucks"]["other"] = "ontario_ministry_transport_vehicle_population"
    payload["sources"]["selections"]["wards_intelligence_2022_sales_shares"] = {
        "year": 2022
    }
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="source selection is inactive"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_path_resolution_and_directory_list() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    directories = configured_directories(bundle)

    assert REPO_ROOT / "outputs" / "logs" in directories
    assert resolve_input_path(bundle, "manual") == (
        REPO_ROOT / "inputs" / "0_manual_params"
    )
    assert resolve_artifact_path(bundle, "road_aggregation") == (
        REPO_ROOT / "inputs" / "2_processed" / "road_aggregation"
    )
    assert resolve_artifact_path(bundle, "road_lifetimes_survival") == (
        REPO_ROOT / "inputs" / "2_processed" / "road_lifetimes_survival"
    )
    assert REPO_ROOT / "inputs" / "validation" in directories


def test_artifact_route_cannot_escape_its_declared_layer(tmp_path: Path) -> None:
    paths, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(paths.read_text(encoding="utf-8"))
    payload["artifacts"]["road_aggregation"]["path"] = (
        "outputs/validation/road_aggregation"
    )
    paths.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match="must be within the processed root"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_artifact_validation_surface_must_be_a_dotted_interface(
    tmp_path: Path,
) -> None:
    paths, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(paths.read_text(encoding="utf-8"))
    payload["artifacts"]["road_aggregation"]["validation_surfaces"] = [
        "tests/test_road_aggregation.py"
    ]
    paths.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match="validation_surfaces"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_shared_energy_conversion_is_not_scenario_local() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    conversions = load_parameter_yaml(bundle, "conversion.yaml")

    assert conversions["energy"]["gigawatt_year_to_petajoule"] == 31.536


def test_setup_smoke_status_uses_packaged_schema_without_building() -> None:
    status = run_smoke_validation(SCENARIO)

    assert status["ok"] is True
    assert status["scenario"] == "legacy_reproduction"
    assert status["periods"] == {
        "base_year": 2023,
        "existing": [2000, 2005, 2010, 2015, 2020, 2023],
        "model": [2025, 2030, 2035, 2040, 2045],
        "step": 5,
    }
    assert status["packaged_schema"]["package_commit"].startswith("1e68c377")
    assert status["reference_sqlite_exists"] is True
    assert "cer_canadas_energy_future" in status["active_sources"]
    assert "wards_intelligence_2022_sales_shares" in status["active_sources"]
    assert status["parameter_options"]["lifetimes"] == {
        "survival_curves": True,
        "survival_curve_max_age": 25,
    }
    assert status["aggregation_sources"]["medium_trucks"]["other"] == "wards_intelligence_2022_sales_shares"


def _write_config_copy(tmp_path: Path) -> tuple[Path, Path, Path]:
    config_dir = tmp_path / "config"
    scenario_dir = config_dir / "scenarios"
    scenario_dir.mkdir(parents=True)
    paths = config_dir / "paths.yaml"
    sources = config_dir / "sources.yaml"
    scenario = scenario_dir / "scenario.yaml"
    paths.write_text(
        (REPO_ROOT / "config" / "paths.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    sources.write_text(
        (REPO_ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    scenario.write_text(
        (REPO_ROOT / SCENARIO).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return paths, sources, scenario


def test_extra_nested_config_field_is_rejected(tmp_path: Path) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["outputs"]["unexpected"] = "forbidden"
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match="unexpected"):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize("missing", ["aggregation_sources", "stock_age", "ldv", "medium_trucks", "heavy_truck_haul"])
def test_every_scenario_requires_its_aggregation_roles(tmp_path: Path, missing: str) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    if missing == "aggregation_sources":
        payload.pop(missing)
    else:
        payload["aggregation_sources"].pop(missing)
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError, match=missing):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_source_registry_rejects_obsolete_aggregation_selections(tmp_path: Path) -> None:
    _, sources, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(sources.read_text(encoding="utf-8"))
    payload["aggregation_sources"] = yaml.safe_load(scenario.read_text(encoding="utf-8"))["aggregation_sources"]
    sources.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError, match="aggregation_sources"):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize("selection", ["unknown", "inactive", "unsupported"])
def test_scenario_aggregation_sources_are_checked_against_registry_before_io(
    tmp_path: Path, selection: str,
) -> None:
    _, sources, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    source_payload = yaml.safe_load(sources.read_text(encoding="utf-8"))
    if selection == "unknown":
        selected = "unregistered_qc"
        message = "unknown or inactive"
    elif selection == "inactive":
        selected = "wards_intelligence_2022_sales_shares"
        source_payload["sources"][selected]["status"] = "inactive"
        message = "unknown or inactive"
    else:
        selected = "nrcan_ceud_transport_provincial"
        message = "no implemented adapter"
    payload["aggregation_sources"]["medium_trucks"]["QC"] = selected
    sources.write_text(yaml.safe_dump(source_payload, sort_keys=False), encoding="utf-8")
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_cer_scenario_has_one_scenario_authority(tmp_path: Path) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["sources"]["selections"]["cer_canadas_energy_future"]["scenario"] = "Lower Scenario"
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError, match="scenario"):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize("selector", [None, "unreviewed"])
def test_future_car_demand_requires_a_supported_selector(
    tmp_path: Path, selector: str | None,
) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    if selector is None:
        payload["demand"].pop("future_car_demand")
    else:
        payload["demand"]["future_car_demand"] = selector
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError, match="future_car_demand"):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload: payload["periods"].update(
                {"model": [2025, 2031], "step": 5}
            ),
            "periods.model must follow the configured step",
        ),
        (
            lambda payload: payload["sources"]["selections"].update(
                {"inactive_source": {"year": 2021}}
            ),
            "source selection not defined in sources.yaml",
        ),
        (
            lambda payload: payload["economics"].update(
                {"global_discount_rate": 1.1}
            ),
            "global_discount_rate",
        ),
        (
            lambda payload: payload["lifetimes"].update(
                {"survival_curve_max_age": 0}
            ),
            "survival_curve_max_age",
        ),
        (
            lambda payload: payload["existing_capacity"].update(
                {"cleanup_tolerance": -1}
            ),
            "cleanup_tolerance",
        ),
    ],
)
def test_invalid_scenario_choices_are_rejected(
    tmp_path: Path, mutate, message: str
) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    mutate(payload)
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises((ValidationError, ValueError), match=message):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("comparison", "mode"),
        ("comparison", "reference_sqlite"),
        ("comparison", "absolute_tolerance"),
        ("comparison", "relative_tolerance"),
        ("comparison", "include_provenance"),
        ("lifetimes", "survival_curves"),
        ("lifetimes", "survival_curve_max_age"),
        ("existing_capacity", "vehicle_population_year"),
        ("existing_capacity", "cleanup_tolerance"),
        ("demand", "cer_scenario"),
        ("economics", "cer_scenario"),
        ("road_utilization", "vkt_schedules"),
        ("road_utilization", "vkt_max_age"),
        ("ev_chargers", "ld_evs_per_port"),
        ("ev_chargers", "mhd_evs_per_port"),
        ("efficiencies", "atb_trajectory"),
        ("costs", "atb_trajectory"),
        ("row_note_overrides", "technology"),
    ],
)
def test_user_selectable_scenario_values_have_no_python_fallback(
    tmp_path: Path,
    section: str,
    field: str,
) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload[section].pop(field)
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match=field):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_data_quality_outside_v4_enum_is_rejected(tmp_path: Path) -> None:
    _, sources, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(sources.read_text(encoding="utf-8"))
    first_source = next(iter(payload["sources"].values()))
    first_source["data_quality"] = {"dq_cred": 6}
    sources.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match="dq_cred"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_source_data_quality_placeholders_are_registry_owned() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    quality = bundle.sources.sources[
        "nrcan_ceud_transport_provincial"
    ].data_quality

    assert set(quality.missing_fields()) == {"dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"}
    with pytest.raises(ValueError, match="Unresolved data_quality"):
        quality.row_fields()


@pytest.mark.parametrize("missing_field", ["defaults", "required", "component_required", "data_quality"])
def test_source_registry_defaults_are_required(tmp_path: Path, missing_field: str) -> None:
    _, sources, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(sources.read_text(encoding="utf-8"))
    if missing_field == "defaults":
        payload.pop("defaults")
    else:
        payload["defaults"].pop(missing_field)
    sources.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match=missing_field):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_collection_defaults_are_not_shared() -> None:
    first = SourceComponent(label="first", short_name="first", required=True)
    second = SourceComponent(label="second", short_name="second", required=True)

    first.adapter["x"] = 1
    first.inputs.append("stock")

    assert second.adapter == {}
    assert second.inputs == []

    first_notes = ScenarioRowNoteOverrides(technology={})
    second_notes = ScenarioRowNoteOverrides(technology={})
    first_notes.technology["T01"] = "scenario-specific note"
    assert second_notes.technology == {}


def test_yaml_rejects_duplicate_keys_but_allows_explicit_merge_overrides(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("module:\n  selector: first\n  selector: second\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate YAML key 'selector'"):
        load_yaml(path)
    path.write_text(
        "defaults: &defaults\n  selector: first\nmodule:\n  <<: *defaults\n  selector: second\n",
        encoding="utf-8",
    )
    assert load_yaml(path)["module"]["selector"] == "second"


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("efficiencies", "atb_trajectory", "unreviewed", "efficiencies.atb_trajectory"),
        ("costs", "atb_trajectory", "unreviewed", "costs.atb_trajectory"),
        ("economics", "cer_scenario", "Global Net-zero", "economics.cer_scenario"),
        ("demand", "cer_scenario", "Global Net-zero", "demand.cer_scenario"),
        ("lifetimes", "survival_curve_max_age", 1, "cannot contain a full period"),
        ("road_utilization", "vkt_max_age", 1, "cannot contain a full period"),
    ],
)
def test_parameter_selectors_are_checked_before_io(
    tmp_path: Path, section: str, field: str, value: object, message: str,
) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload[section][field] = value
    if section == "road_utilization":
        payload[section]["vkt_schedules"] = True
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises((ValueError, ValidationError), match=message):
        load_config_bundle(scenario, repo_root=tmp_path)


@pytest.mark.parametrize(
    "selection", [{}, {"edition": 2023}, {"year": 2023, "edition": 2023}],
)
def test_source_selections_cannot_silently_use_adapter_defaults(tmp_path: Path, selection: dict) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["sources"]["selections"]["nrcan_ceud_transport_provincial"] = selection
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match="sources.selections.nrcan_ceud_transport_provincial"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_demand_and_currency_cer_choices_are_independent(tmp_path: Path) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["demand"]["cer_scenario"] = "Higher Scenario"
    payload["economics"]["cer_scenario"] = "Lower Scenario"
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    selected = load_config_bundle(scenario, repo_root=tmp_path)
    assert selected.scenario.demand.cer_scenario == "Higher Scenario"
    assert selected.scenario.economics.cer_scenario == "Lower Scenario"


def test_rules_modules_have_one_owner_and_source_placeholders_are_explicit() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    payload = load_parameter_yaml(bundle, "rules.yaml")
    assert not (payload["fetching"].keys() & payload["parameterization"].keys())
    for modules in (payload["fetching"], payload["parameterization"]):
        for name, rules in modules.items():
            assert load_harmonization_rules(bundle, name) == rules
    raw_sources = load_yaml(bundle.sources_path)["sources"]
    for source in raw_sources.values():
        assert source["database_note"] == ""
        assert source["data_quality"] == dict.fromkeys(
            ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"), None,
        )
