from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from utils import (
    configured_directories,
    load_config_bundle,
    load_parameter_yaml,
    resolve_input_path,
)
from validation.config_models import ScenarioRowNoteOverrides, SourceComponent
from validation.config_smoke import run_smoke_validation


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def test_config_bundle_loads_typed_contracts() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert bundle.paths.inputs.template == "inputs/0_canoe_template"
    assert bundle.scenario.scenario.name == "legacy_reproduction"
    assert bundle.scenario.geography.regions == ["ON"]
    assert bundle.scenario.periods.existing == [2021]
    assert bundle.scenario.periods.model == [
        2025,
        2030,
        2035,
        2040,
        2045,
        2050,
    ]
    assert bundle.scenario.periods.all_years() == [
        2021,
        2025,
        2030,
        2035,
        2040,
        2045,
        2050,
    ]
    assert bundle.scenario.sources.selections[
        "cer_canadas_energy_future"
    ].edition == 2026
    assert bundle.scenario.currency.target == "CAD"
    assert bundle.scenario.economics.global_discount_rate == 0.03
    assert bundle.sources.sources["statcan_transport_tables"].component(
        "20-10-0021-01"
    ).short_name == "archived_ldv_new_registrations"
    assert bundle.sources.sources["cer_canadas_energy_future"].component(
        "macro-indicators"
    ).short_name == "macro_indicators_for_demand_and_currency_conversion"


def test_active_sources_are_registered_and_template_is_not_a_source() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    for source_name in bundle.scenario.sources.active:
        assert bundle.sources.sources[source_name].status == "active"
    assert (
        bundle.sources.sources["wards_intelligence_2022_sales_shares"].status
        == "inactive"
    )
    assert "canoe_transport_template" not in bundle.sources.sources


def test_source_component_vocabularies_are_canonical() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    current_modules = {
        "capex_opex",
        "efficiencies",
        "lifetimes_survival",
        "road_aggregation",
        "sector_coupling",
        "stocks_and_demands",
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
            assert set(component.parameter_modules) <= current_modules


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


def test_inactive_registry_source_cannot_be_scenario_active(tmp_path: Path) -> None:
    _, _, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(scenario.read_text(encoding="utf-8"))
    payload["sources"]["active"].append("wards_intelligence_2022_sales_shares")
    scenario.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="scenario activates inactive source"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_path_resolution_and_directory_list() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    directories = configured_directories(bundle)

    assert REPO_ROOT / "outputs" / "logs" in directories
    assert resolve_input_path(bundle, "manual") == (
        REPO_ROOT / "inputs" / "0_manual_params"
    )


def test_shared_energy_conversion_is_not_scenario_local() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    conversions = load_parameter_yaml(bundle, "conversion.yaml")

    assert conversions["energy"]["gigawatt_year_to_petajoule"] == 31.536


def test_setup_smoke_status_uses_packaged_schema_without_building() -> None:
    status = run_smoke_validation(SCENARIO)

    assert status["ok"] is True
    assert status["scenario"] == "legacy_reproduction"
    assert status["periods"] == {
        "base_year": 2021,
        "existing": [2021],
        "model": [2025, 2030, 2035, 2040, 2045, 2050],
        "step": 5,
    }
    assert status["packaged_schema"]["package_commit"].startswith("1e68c377")
    assert status["reference_sqlite_exists"] is True
    assert status["switches"] == {
        "legacy_equivalent": True,
        "debug": False,
        "download_sources": False,
        "compile_sqlite": True,
        "transform_parameters": False,
        "include_existing_capacity": True,
        "survival_curves": False,
        "survival_curve_max_age": 30,
    }


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
            "sources.selections contains inactive source keys",
        ),
        (
            lambda payload: payload["economics"].update(
                {"global_discount_rate": 1.1}
            ),
            "global_discount_rate",
        ),
        (
            lambda payload: payload["validation"]["parameter_tolerances"].update(
                {"efficiency": -0.01}
            ),
            "must be non-negative",
        ),
        (
            lambda payload: payload["switches"].update(
                {"survival_curve_max_age": 0}
            ),
            "survival_curve_max_age",
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

    with pytest.raises(ValidationError, match=message):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_data_quality_outside_v4_enum_is_rejected(tmp_path: Path) -> None:
    _, sources, scenario = _write_config_copy(tmp_path)
    payload = yaml.safe_load(sources.read_text(encoding="utf-8"))
    first_source = next(iter(payload["sources"].values()))
    first_source["data_quality"] = {"dq_cred": 6}
    sources.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValidationError, match="dq_cred"):
        load_config_bundle(scenario, repo_root=tmp_path)


def test_source_data_quality_defaults_to_fives() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    quality = bundle.sources.sources[
        "nrcan_ceud_transport_provincial"
    ].data_quality

    assert quality.row_fields() == {
        "dq_cred": 5,
        "dq_geog": 5,
        "dq_struc": 5,
        "dq_tech": 5,
        "dq_time": 5,
    }


def test_collection_defaults_are_not_shared() -> None:
    first = SourceComponent(label="first", short_name="first")
    second = SourceComponent(label="second", short_name="second")

    first.adapter["x"] = 1
    first.inputs.append("stock")

    assert second.adapter == {}
    assert second.inputs == []

    first_notes = ScenarioRowNoteOverrides()
    second_notes = ScenarioRowNoteOverrides()
    first_notes.technology["T01"] = "scenario-specific note"
    assert second_notes.technology == {}
