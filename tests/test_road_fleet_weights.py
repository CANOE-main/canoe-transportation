"""Regional source precedence and shared GVWR/vocation aggregation contracts."""

from copy import deepcopy
from dataclasses import replace

import pandas as pd
import pytest
import yaml
from pydantic import ValidationError

from parameterization.build_costs import _region_weights
from parameterization.build_efficiencies import efficiency_components
from parameterization.road_fleet_weights import (
    MTO, WARDS, FleetAggregationEvidence, load_fleet_aggregation_evidence,
    medium_vocation_weights, report4_gvwr_shares, wards_gvwr_shares,
)
from parameterization.road_utilization import prepare_age_profiles
from utils import load_config_bundle, load_harmonization_rules, load_yaml, resolve_input_path, validate_config_bundle
from validation.config_models import AggregationSources, ScenarioConfig, SourcesConfig


@pytest.fixture
def bundle():
    return load_config_bundle("config/scenarios/legacy_reproduction.yaml")


def test_regional_precedence_and_bc_model_alias(bundle):
    payload = bundle.scenario.aggregation_sources.model_dump()
    payload["medium_trucks"].update({"BC": MTO, "BCT": WARDS, "QC": MTO})
    policy = AggregationSources.model_validate(payload)
    assert policy.source_for("medium_trucks", "ON") == MTO
    assert policy.source_for("medium_trucks", "QC") == MTO
    assert policy.source_for("medium_trucks", "AB") == WARDS
    assert policy.source_for("medium_trucks", "BCT") == WARDS
    payload["medium_trucks"].pop("BCT")
    assert AggregationSources.model_validate(payload).source_for("medium_trucks", "BCT") == MTO
    for region in bundle.scenario.geography.regions:
        assert policy.source_for("stock_age", region) == MTO
        assert policy.source_for("ldv", region) == MTO


@pytest.mark.parametrize("role", ["stock_age", "ldv", "medium_trucks", "heavy_truck_haul"])
@pytest.mark.parametrize("key", ["ON", "other"])
def test_every_role_requires_explicit_on_and_other(bundle, role, key):
    payload = bundle.scenario.aggregation_sources.model_dump()
    payload[role].pop(key)
    with pytest.raises(ValidationError, match="requires ON and other"):
        AggregationSources.model_validate(payload)


@pytest.mark.parametrize("selection", ["unknown", "inactive"])
def test_selected_aggregation_sources_must_be_registered_and_active(bundle, selection):
    payload = bundle.sources.model_dump()
    scenario_payload = bundle.scenario.model_dump()
    if selection == "unknown":
        scenario_payload["aggregation_sources"]["medium_trucks"]["QC"] = "unregistered_qc"
    else:
        payload["sources"][WARDS]["status"] = "inactive"
    changed = replace(
        bundle, sources=SourcesConfig.model_validate(payload),
        scenario=ScenarioConfig.model_validate(scenario_payload),
    )
    assert any("unknown or inactive" in error for error in validate_config_bundle(changed))


def test_report4_class2b_maps_to_class2_and_equal_available_vocations(bundle):
    rules = deepcopy(load_harmonization_rules(bundle, "road_aggregation")["medium_trucks"])
    rules["report4_gvwr"] = {"MDV2b": 2, "MDV6": 6}
    report = pd.DataFrame({"EPA_GVWR": ["MDV2b", "MDV6"], "NATIVE_COUNT": [3, 1]})
    labels = ["Class 2 Medium Van", "Class 6 Box", "Class 6 Box duplicate",
              "Class 6 Vocational", "Class 6 School", "Class 6 Refuse", "Class 6 Transit"]
    weights = medium_vocation_weights(report4_gvwr_shares(report, rules=rules), labels, rules=rules)
    assert weights == {
        "Class 2 Medium Van": pytest.approx(0.75),
        "Class 6 Box": pytest.approx(1 / 12),
        "Class 6 Box duplicate": pytest.approx(1 / 12),
        "Class 6 Vocational": pytest.approx(1 / 12),
    }
    with pytest.raises(ValueError, match="Class 2"):
        medium_vocation_weights({2: 1}, labels[1:], rules=rules)


def test_wards_coverage_and_sum_are_required(bundle):
    rules = deepcopy(load_harmonization_rules(bundle, "road_aggregation")["medium_trucks"])
    rules["wards_classes"] = [6]
    wards = pd.DataFrame([
        {"vehicle_scope": "mhdv", "nrcan_ceud_class": "Medium Trucks", "year": 2020,
         "wards_size_class": "Class 6", "market_share": 1.0},
    ])
    assert wards_gvwr_shares(wards, rules=rules) == {6: 1}
    for frame in (wards.assign(market_share=0.5), pd.concat([wards, wards]), wards.assign(year=2019)):
        with pytest.raises(ValueError, match="incomplete or invalid"):
            wards_gvwr_shares(frame, rules=rules)


def test_heavy_vocations_share_regional_tonne_km_weights(bundle):
    freight = pd.DataFrame([
        {"scenario_region": "ON", "haul_class": "regional", "tonne_kilometres": 1},
        {"scenario_region": "ON", "haul_class": "long_haul", "tonne_kilometres": 3},
    ])
    rules = load_harmonization_rules(bundle, "road_aggregation")
    evidence = FleetAggregationEvidence(bundle, rules, pd.DataFrame(), {}, freight, ())
    weights = evidence.heavy_weights("ON")
    assert weights["Class 8 Longhaul Sleeper"] == pytest.approx(0.75)
    assert sum(weights[c] for c in rules["heavy_trucks"]["vocations"]["regional"]) == pytest.approx(0.25)
    assert len(set(weights[c] for c in rules["heavy_trucks"]["vocations"]["regional"])) == 1


def test_cost_utilization_and_efficiency_use_the_same_regional_fleet_evidence(bundle):
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    mileage = pd.read_csv(resolve_input_path(
        bundle, "interim", atb["interim_subdir"], atb["components"]["vmt"]["output_file"],
    ))
    classes = sorted(mileage.vehicle_class.unique())
    fleet = load_fleet_aggregation_evidence(bundle)
    on = fleet.medium_weights("ON", classes)
    qc = fleet.medium_weights("QC", classes)
    assert on["Class 2 Medium Van"] > 0
    assert "Class 2 Medium Van" not in qc
    assert set(fleet.medium_shares[WARDS]) == {3, 4, 5, 6, 7}
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    costs = _region_weights(bundle, pd.DataFrame({"family": "mhdv", "vehicle_class": classes}), [], efficiency)
    assert costs["ON", "medium_trucks"] == on
    assert costs["QC", "medium_trucks"] == qc
    for region in bundle.scenario.geography.regions:
        assert costs[region, "heavy_trucks"] == fleet.heavy_weights(region)
    assert costs["ON", "heavy_trucks"] != costs["QC", "heavy_trucks"]
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    offroad = load_harmonization_rules(bundle, "offroad_stocks_and_demands")["demand"]
    for region, expected in (("ON", (MTO, 4)), ("QC", (WARDS, "vehicle_class_market_shares"))):
        components = efficiency_components(bundle, "medium_trucks", efficiency, road, offroad, region=region)
        assert expected in components
        assert ((MTO, 4) in components) == (region == "ON")
    profiles = prepare_age_profiles(
        bundle, output_regions={"ON": "ON", "QC": "QC"}, fleet=fleet,
    )
    sources = profiles.loc[profiles.nrcan_ceud_class.eq("Medium Trucks")].groupby("region").weight_source.first()
    assert sources.to_dict() == {"ON": MTO, "QC": WARDS}
    heavy_sources = profiles.loc[profiles.nrcan_ceud_class.eq("Heavy Trucks")].groupby("region").weight_source.first()
    assert heavy_sources.to_dict() == {"ON": "statcan_transport_tables", "QC": "statcan_transport_tables"}
    for region in ("ON", "QC"):
        components = efficiency_components(bundle, "heavy_trucks", efficiency, road, offroad, region=region)
        assert ("statcan_transport_tables", "23-10-0142-01") in components
        heavy_weights = fleet.heavy_weights(region)
        selected = mileage.loc[mileage.vehicle_class.isin(heavy_weights) & mileage.year_index.le(25)].copy()
        selected["weighted"] = selected.vmt_mi * selected.vehicle_class.map(heavy_weights)
        annual = selected.groupby("year_index").weighted.sum()
        actual = profiles.loc[profiles.region.eq(region) & profiles.nrcan_ceud_class.eq("Heavy Trucks")]
        assert actual.normalized_utilization.tolist() == pytest.approx((annual / annual.max()).tolist())
    # Both profiles must follow the same weighted physical VMT, normalized at its peak.
    for region, weights in (("ON", on), ("QC", qc)):
        selected = mileage.loc[mileage.vehicle_class.isin(weights) & mileage.year_index.le(25)].copy()
        selected["weighted"] = selected.vmt_mi * selected.vehicle_class.map(weights)
        annual = selected.groupby("year_index").weighted.sum()
        actual = profiles.loc[profiles.region.eq(region) & profiles.nrcan_ceud_class.eq("Medium Trucks")]
        assert actual.normalized_utilization.tolist() == pytest.approx((annual / annual.max()).tolist())


def test_regional_override_changes_all_medium_weight_consumers(bundle):
    policy = bundle.scenario.aggregation_sources.model_copy(update={"medium_trucks": {"ON": MTO, "other": MTO}})
    changed = replace(bundle, scenario=bundle.scenario.model_copy(update={"aggregation_sources": policy}))
    evidence = load_fleet_aggregation_evidence(changed)
    assert set(evidence.medium_shares) == {MTO}
    assert all(path.name != "vehicle_class_market_shares.csv" for path in evidence.paths)


def test_two_scenarios_share_registry_with_independent_aggregation_choices(bundle, tmp_path):
    first_path, second_path = tmp_path / "first.yaml", tmp_path / "second.yaml"
    payload = load_yaml(bundle.scenario_path)
    first_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    payload["scenario"]["name"] = "alternative_aggregation"
    payload["aggregation_sources"]["medium_trucks"]["other"] = MTO
    second_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    first, second = [load_config_bundle(
        path, repo_root=bundle.repo_root,
        paths_path=bundle.paths_path, sources_path=bundle.sources_path,
    ) for path in (first_path, second_path)]
    assert first.sources_path == second.sources_path
    assert first.sources == second.sources
    assert first.scenario.aggregation_sources.source_for("medium_trucks", "QC") == WARDS
    assert second.scenario.aggregation_sources.source_for("medium_trucks", "QC") == MTO
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    mileage = pd.read_csv(resolve_input_path(
        bundle, "interim", atb["interim_subdir"], atb["components"]["vmt"]["output_file"],
    ))
    classes = sorted(mileage.vehicle_class.unique())
    fleets = [load_fleet_aggregation_evidence(scenario) for scenario in (first, second)]
    weights = [fleet.medium_weights("QC", classes) for fleet in fleets]
    assert "Class 2 Medium Van" not in weights[0]
    assert weights[1]["Class 2 Medium Van"] > 0
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    prices = pd.DataFrame({"family": "mhdv", "vehicle_class": classes})
    for scenario, fleet, expected in zip((first, second), fleets, weights, strict=True):
        assert _region_weights(scenario, prices, [], efficiency)["QC", "medium_trucks"] == expected
        profiles = prepare_age_profiles(scenario, output_regions={"QC": "QC"}, fleet=fleet)
        actual = profiles.loc[profiles.nrcan_ceud_class.eq("Medium Trucks")]
        assert set(actual.weight_source) == {scenario.scenario.aggregation_sources.source_for("medium_trucks", "QC")}
    assert fleets[0].medium_weights("ON", classes) == fleets[1].medium_weights("ON", classes)


def test_wards_citation_is_validated_before_aggregation(bundle, tmp_path):
    from parameterization.manual_parameters import ManualParameterError

    file = tmp_path / "vehicle_class_market_shares.csv"
    wards = pd.read_csv(resolve_input_path(bundle, "manual", file.name))
    wards.loc[0, "source -> data_source"] = "Unregistered evidence"
    wards.to_csv(file, index=False)
    inputs = bundle.paths.inputs.model_copy(update={"manual": str(tmp_path)})
    changed = replace(bundle, paths=bundle.paths.model_copy(update={"inputs": inputs}))
    with pytest.raises(ManualParameterError):
        load_fleet_aggregation_evidence(changed)
