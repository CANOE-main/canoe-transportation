"""Publisher-defined vocational scenarios preserve complete parameter coverage."""

from dataclasses import replace
from zipfile import ZipFile

import pandas as pd
import pytest
import yaml

from fetching import nlr_atb_autonomie as atb
from parameterization.road_capex_opex import select_nlr_purchase_prices
from parameterization.road_efficiencies import aggregate_atb, select_atb_consumption
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules


VOCATIONAL = {"Class 7 Medium Vocational", "Class 8 Refuse Heavy", "Class 8 Vocational Heavy"}


@pytest.fixture(scope="module")
def registered_atb():
    bundle = load_config_bundle("config/scenarios/default.yaml")
    request = atb.build_atb_request(bundle)
    rules = atb.module_rules(bundle)
    members = atb.discover_zip_members(request.cache_path, request.components)
    with ZipFile(request.cache_path) as archive:
        frames = {key: pd.read_csv(archive.open(members[key])) for key in (
            "vehicles", "phev_vehicle_inputs", "phev_utility_factor_ldv", "phev_utility_factor_mdhd",
        )}
    component = next(row for row in request.components if row.component_id == "vehicles")
    normalized = atb.normalize_vehicles(
        frames["vehicles"], request=request, component=component,
        source_member=members["vehicles"], default_trajectory="Mid", rules=rules,
    )
    return bundle, request, rules, members, frames, component, normalized


def test_only_publisher_vocational_exceptions_are_harmonized_for_all_source_years(registered_atb):
    _, _, _, _, frames, _, normalized = registered_atb
    raw = frames["vehicles"]
    assert set(raw.loc[raw.scenario.eq("Constant"), "vehicle_class"]) == VOCATIONAL
    assert set(normalized.source_trajectory) == {"Advanced", "Mid", "Conservative", "Constant"}
    changed = normalized.loc[normalized.trajectory.ne(normalized.source_trajectory)]
    assert set(changed.vehicle_class) == VOCATIONAL
    assert set(changed.source_trajectory) == {"Constant"}
    assert set(changed.trajectory) == {"Conservative"}
    assert set(changed.year) == {2023, 2025, 2030, 2035, 2040, 2045, 2050}
    for _, group in changed.groupby(["vehicle_class", "vehicle_powertrain", "vehicle_detail", "metric"]):
        assert set(group.year) == {2023, 2025, 2030, 2035, 2040, 2045, 2050}
    keys = ["year", "vehicle_class", "vehicle_powertrain", "vehicle_detail", "metric"]
    joined = changed.merge(raw.loc[raw.scenario.eq("Constant")], on=keys, suffixes=("_normalized", "_native"),
                           validate="one_to_one")
    assert len(joined) == len(changed)
    assert joined.value_normalized.equals(joined.value_native)
    for trajectory in ("Mid", "Advanced"):
        selected = normalized.loc[normalized.trajectory.eq(trajectory)]
        assert selected.source_trajectory.eq(trajectory).all()
        joined = selected.merge(raw.loc[raw.scenario.eq(trajectory)], on=keys,
                                suffixes=("_normalized", "_native"), validate="one_to_one")
        assert len(joined) == len(selected)
        assert joined.value_normalized.equals(joined.value_native)


def test_conservative_consumption_and_price_audits_keep_native_labels_and_all_vocations(registered_atb):
    bundle, _, rules, members, frames, _, normalized = registered_atb
    conversions = load_conversion_factors(bundle)
    phev = atb.derive_phev_efficiency(
        frames["phev_vehicle_inputs"], ldv_utility_factors=frames["phev_utility_factor_ldv"],
        mdhd_utility_factors=frames["phev_utility_factor_mdhd"], output_vehicles=frames["vehicles"],
        rules=rules["components"]["phev_efficiency"], conversions=conversions,
        source_members={"vehicles": members["vehicles"], **{key: members[key] for key in (
            "phev_vehicle_inputs", "phev_utility_factor_ldv", "phev_utility_factor_mdhd",
        )}}, default_trajectory="Conservative",
    )
    phev["trajectory"] = phev.trajectory.replace(rules["components"]["phev_efficiency"]["reconciliation"]["scenario_aliases"])
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    selected = select_atb_consumption(normalized, phev, trajectory="Conservative", rules=efficiency, conversions=conversions)
    medium_heavy = selected.loc[selected.family.eq("mhdv")]
    classes = set(medium_heavy.vehicle_class)
    assert VOCATIONAL <= classes
    aggregate_atb(medium_heavy, dict.fromkeys(classes, 1 / len(classes)), tolerance=1e-8)
    assert set(medium_heavy.loc[medium_heavy.vehicle_class.isin(VOCATIONAL), "source_trajectory"]) == {"Constant"}
    prices = select_nlr_purchase_prices(
        normalized, trajectory="Conservative", efficiency_rules=efficiency,
        rpe_markups={"ldv": 1.5, "mhdv": 1.2}, source_currency="USD", source_dollar_year=2022,
    )
    vocational = prices.loc[prices.vehicle_class.isin(VOCATIONAL)]
    assert set(vocational.vehicle_class) == VOCATIONAL
    assert set(vocational.source_trajectory) == {"Constant"}
    assert set(vocational.source_year) == {2023, 2025, 2030, 2035, 2040, 2045, 2050}


def test_alias_does_not_hide_conflicting_publisher_rows(registered_atb):
    _, request, rules, members, frames, component, _ = registered_atb
    raw = frames["vehicles"]
    duplicate = raw.loc[raw.scenario.eq("Constant") & raw.metric.eq("Fuel Economy (mi/dge)")].iloc[:1].assign(scenario="Conservative")
    with pytest.raises(ValueError, match="keys overlap"):
        atb.normalize_vehicles(
            pd.concat([raw, duplicate], ignore_index=True), request=request, component=component,
            source_member=members["vehicles"], default_trajectory="Conservative", rules=rules,
        )


@pytest.mark.parametrize("parameter", ["efficiencies", "costs"])
def test_global_constant_rejected_by_registered_scenario_contract(parameter, tmp_path):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    scenario = bundle.scenario.model_copy(update={
        parameter: getattr(bundle.scenario, parameter).model_copy(update={"atb_trajectory": "Constant"}),
    })
    with pytest.raises(ValueError, match="discontinued Constant"):
        atb.configured_trajectory(replace(bundle, scenario=scenario), parameter=parameter)
    path = tmp_path / "constant.yaml"
    path.write_text(yaml.safe_dump(scenario.model_dump(mode="json", by_alias=True)), encoding="utf-8")
    with pytest.raises(ValueError, match=f"{parameter}.atb_trajectory is not supported"):
        load_config_bundle(path, repo_root=bundle.repo_root)
