"""Registered source and parameter contracts for both production charger modes."""

from copy import deepcopy
from dataclasses import replace

from canoe_schema.v4_0 import ExistingCapacity, LifetimeTech
import pandas as pd
import pytest
from pydantic import ValidationError

from fetching.dunsky_charger_inputs import infrastructure_frames, load_evidence
from parameterization import ev_chargers
from parameterization.charger_representation import (
    ChargerRepresentationSettings, aggregate_components, build_representation,
    capacity_share_targets, select_charger_technologies, source_value_and_years,
)
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.config_models import ScenarioConfig, ScenarioEvChargers, SourcesConfig
from validation.provenance import resolve_provenance


@pytest.fixture
def source_bundle():
    return load_config_bundle("config/scenarios/default.yaml")


@pytest.fixture
def representation(source_bundle):
    manual = pd.read_csv(resolve_input_path(source_bundle, "manual", "charger_parameters.csv"),
                         dtype=str, keep_default_na=False)
    settings, result, converter, _ = ev_chargers.production_representation(source_bundle, manual)
    return settings, result, converter, manual


@pytest.mark.parametrize("ratios", [[1., 1.5], [1., 1.5, 1.5, 2.], [True, 1.5, 1.5],
                                    [0., 1.5, 1.5], [1., float("nan"), 1.5]])
def test_ratios_require_three_positive_finite_nonboolean_values(ratios):
    with pytest.raises(ValidationError):
        ScenarioEvChargers(evs_per_port=ratios, charger_type_classes="aggregated", ld_home_utilization=.08)


def test_explicit_hourly_profiles_require_an_explicit_disabled_handoff(source_bundle):
    payload = source_bundle.scenario.model_dump(mode="python")
    payload["ev_chargers"]["charger_type_classes"] = "explicit"
    with pytest.raises(ValidationError, match="travel_behavior_source: none"):
        ScenarioConfig.model_validate(payload)
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    assert ScenarioConfig.model_validate(payload).ev_chargers.charger_type_classes == "explicit"
    selected = replace(source_bundle, scenario=ScenarioConfig.model_validate(payload))
    with pytest.raises(ValueError, match="requires prepare_ev_charger_rows"):
        ev_chargers.prepare_charger_utilization_targets(selected)


def test_cost_matched_dcfc_power_holds_last_source_point_and_l2_stays_native(representation):
    settings, result, _, _ = representation
    parameters = result.parameters.set_index(["archetype", "year"])
    fast = settings.archetypes.ldv_public_fast
    assert [parameters.loc[(fast, year), "rated_power_kw"] for year in [2025, 2030, 2035, 2040, 2045, 2050]] == [125., 175., 250., 300., 300., 300.]
    l2 = settings.archetypes.ldv_public_l2
    assert [parameters.loc[(l2, year), "rated_power_kw"] for year in [2025, 2030, 2035, 2040]] == [7.2, 10., 12., 12.]
    assert parameters.loc[(fast, 2025), "annual_utilization"] == .065
    assert parameters.loc[(fast, 2050), "annual_utilization"] == .12


def test_all_archetypes_conserve_component_expenditure_capacity_and_energy(representation):
    _, result, _, _ = representation
    for row in result.parameters.itertuples(index=False):
        pieces = result.components.loc[result.components.year.eq(row.year) & result.components.archetype.eq(row.archetype)]
        capacity = float((pieces.ports * pieces.power_kw).sum())
        expenditure = float((pieces.ports * pieces.cost_cad_per_port).sum())
        energy = float((pieces.ports * pieces.power_kw * pieces.utilization).sum())
        assert row.rated_power_kw * row.port_count == pytest.approx(capacity)
        assert row.capex_mcad_per_gw * capacity == pytest.approx(expenditure)
        assert row.annual_utilization * capacity == pytest.approx(energy)
    assert result.parameters.groupby(["year", "vehicle_class"]).port_share.sum().to_numpy() == pytest.approx(1.)
    assert result.parameters.groupby(["year", "vehicle_class"]).capacity_share.sum().to_numpy() == pytest.approx(1.)


def test_mhdv_operating_inputs_remain_reviewable_and_use_time_before_capacity_pooling(representation):
    settings, result, _, _ = representation
    selected = result.components.loc[result.components.year.eq(2025) & result.components.vehicle_class.eq("MHDV")].set_index("component")
    assert selected.loc["Private Overnight", "session_hours"] == 8
    assert selected.loc["Private Overnight", "vehicles_per_day"] == 1
    assert selected.loc["Public Overnight", "session_hours"] == 6
    assert selected.loc["Public Overnight", "vehicles_per_day"] == 1.5
    assert selected.loc["Private FC", "session_hours"] == .5
    assert selected.loc["Private FC", "vehicles_per_day"] == 3
    assert selected.loc["Public FC", "vehicles_per_day"] == 6
    assert set(selected.operating_days) == {300}
    assert selected.loc["Private Overnight", "utilization"] == pytest.approx(8 * 300 / 8760)
    assert selected.loc["Public Overnight", "utilization"] == pytest.approx(6 * 1.5 * 300 / 8760)
    assert selected.loc["Public Overnight", "power_kw"] == 100
    assert selected.loc["Public Overnight", "cost_cad_per_port"] == pytest.approx(2 * selected.loc["Private Overnight", "cost_cad_per_port"])
    assert settings.mhdv_operating_days == 300


def test_compositions_evolve_then_hold_without_averaged_or_rounded_fixed_weights(representation):
    _, result, converter, manual = representation
    for category in ("ldv", "mhdv"):
        first = result.composition.loc[result.composition.year.eq(2025) & result.composition.vehicle_class.eq(category.upper())]
        # Literal source component sums; Table 38's printed 43,318 subtotal is inconsistent.
        assert first.ports.sum() == {"ldv": 1103000., "mhdv": 43310.}[category]
        later = result.composition.loc[result.composition.year.eq(2040) & result.composition.vehicle_class.eq(category.upper())]
        assert first.population_share.tolist() != pytest.approx(later.population_share.tolist())
        held = result.composition.loc[result.composition.year.eq(2050) & result.composition.vehicle_class.eq(category.upper())]
        assert held.ports.tolist() == later.ports.tolist()
        assert set(held.source_year) == {2040}
        rate, audit = ev_chargers.charger_cost_components(manual, category, 2030, converter, representation=result)
        rows = result.components.loc[result.components.year.eq(2030) & result.components.vehicle_class.eq(category.upper())]
        assert rate == pytest.approx((rows.ports * rows.cost_cad_per_port).sum() / (rows.ports * rows.power_kw).sum())
        assert sum(row["weighted_cad_2020_cost_per_gw"] for row in audit) == pytest.approx(rate)
        assert sum(row["count_share"] for row in audit) == pytest.approx(1)


def test_core_rejects_an_infeasible_daily_schedule(source_bundle, representation):
    settings, _, converter, manual = representation
    evidence, _ = load_evidence(source_bundle)
    frames = infrastructure_frames(evidence)
    frames["operating"].loc[0, "vehicles_per_day"] = 4
    l1 = manual.loc[manual.parameter.eq("L1_capex")].iloc[0]
    with pytest.raises(ValueError, match="exceed available hours"):
        build_representation(frames, settings, [2025], converter, l1_input=l1,
                             fixed_om_fraction=.01, ld_home_utilization=.08)


def test_capacity_constraints_use_capacity_shares_and_preserve_port_share_audit(representation):
    settings, result, _, _ = representation
    rows, groups = capacity_share_targets(result.parameters, settings, ["ON", "AB"], {2025: 2030, 2045: 2050})
    assert len(rows) == 24
    assert len(groups) == 8
    assert sum(map(len, groups.values())) == 12
    assert set(groups).isdisjoint(settings.archetypes.model_dump().values())
    for _, group in rows.groupby(["region", "period", "super_group"]):
        assert group.share.sum() == pytest.approx(1)
        assert group.port_share.sum() == pytest.approx(1)
    fast_group = next(group for group, members in groups.items() if members == [settings.archetypes.ldv_public_fast])
    public = rows.loc[rows.sub_group.eq(fast_group)].iloc[0]
    assert public.share != pytest.approx(public.port_share)


def test_source_audit_distinguishes_exact_interpolated_and_held_charger_values():
    source = pd.DataFrame({"year": [2025, 2040], "value": [100., 400.]})
    assert source_value_and_years(source, 2025) == (100., (2025,))
    assert source_value_and_years(source, 2030) == (200., (2025, 2040))
    assert source_value_and_years(source, 2050) == (400., (2040,))


@pytest.mark.parametrize("mode,expected", [("aggregated", 4), ("explicit", 5)])
def test_template_selection_preserves_nonchargers_and_does_not_mutate(source_bundle, mode, expected):
    rules = load_harmonization_rules(source_bundle, "ev_chargers")
    settings = ChargerRepresentationSettings.model_validate(rules["production_representation"])
    aggregate = [mapping[kind] for mapping in rules["technology"].values() for kind in ("existing", "new")]
    technology = pd.DataFrame([{"tech": tech, "category": "charger"}
                               for tech in [*aggregate, *settings.archetypes.model_dump().values()]]
                              + [{"tech": "road-car", "category": "cars"}])
    original = technology.copy(deep=True)
    bundle = replace(source_bundle, scenario=source_bundle.scenario.model_copy(update={
        "ev_chargers": source_bundle.scenario.ev_chargers.model_copy(update={"charger_type_classes": mode})}))
    selected = select_charger_technologies(bundle, technology)
    assert len(selected) == expected + 1
    assert "road-car" in set(selected.tech)
    pd.testing.assert_frame_equal(technology, original)


@pytest.mark.parametrize("mode", ["aggregated", "explicit"])
@pytest.mark.parametrize(("scenario_name", "evaluation_year"), [("validation_opening_periods", 2025), ("default", 2030)])
def test_parameter_rows_use_independent_md_hd_port_ratios_and_full_provenance(
    mode, scenario_name, evaluation_year, tmp_path,
):
    source_bundle = load_config_bundle(f"config/scenarios/{scenario_name}.yaml")
    payload = source_bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON"]
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    payload["ev_chargers"].update(charger_type_classes=mode, evs_per_port=[1., 2., 4.])
    routes = dict(source_bundle.paths.artifacts)
    for family in ("ev_chargers_processed", "ev_chargers_validation"):
        routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    bundle = replace(source_bundle, scenario=ScenarioConfig.model_validate(payload),
                     paths=source_bundle.paths.model_copy(update={"artifacts": routes}))
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    context = resolve_provenance(bundle.sources, source_key="nrcan_ceud_transport_provincial", component_key=21,
                                 transformation="explicit isolated charger stock fixture", transformation_version="1")
    stock, physical = [], []
    for category, vehicles in [("cars", 500000.), ("medium_trucks", 10000.), ("heavy_trucks", 20000.)]:
        tech = technology.loc[technology.category.eq(category) & technology.sub_category.str.startswith("bev"), "tech"].iloc[0]
        stock.append(ExistingCapacity(region="ON", tech=tech, vintage=2020, capacity=2 * vehicles / 1000,
                                      units="k vehicles", **context.parameter_fields()))
        physical.append({"region": "ON", "tech": tech, "vintage": 2020, "surviving_k_vehicles": vehicles / 1000,
                         "base_year": bundle.scenario.periods.base_year, "units": "k vehicles", "data_id": context.data_id})
    selected = select_charger_technologies(bundle, technology)
    lifetimes = [LifetimeTech(region="ON", tech=tech, lifetime=15, data_id="isolated-reviewed-lifetime")
                 for tech in selected.loc[selected.category.isin(["charger", "h2_refuel"]), "tech"]]
    if mode == "aggregated":
        output = tmp_path / "ev_chargers_processed"
        output.mkdir()
        stale = output / load_harmonization_rules(bundle, "ev_chargers")["capacity_share_output_file"]
        stale.write_text("previous explicit output\n", encoding="utf-8")
    prepared = ev_chargers.prepare_ev_charger_rows(bundle, existing_capacity_rows=stock,
        existing_capacity_contexts=[context], physical_stock=pd.DataFrame(physical), fixed_lifetime_rows=lifetimes)
    reconciliation = pd.DataFrame(prepared.audit["port_reconciliation"]).set_index("charger_class")
    assert reconciliation.loc["ldv", "required_ports"] == 500000
    assert reconciliation.loc["mhdv", "required_ports"] == 10000 / 2 + 20000 / 4
    assert {item["source_period"] for item in prepared.audit["hydrogen_refueling"]["cost_conversion"]} == {"all"}
    if mode == "explicit":
        assert len(prepared.capacity_rows) == 5
        assert len(prepared.tech_group_rows) == 8
        assert len(prepared.tech_group_member_rows) == 12
        assert len(prepared.capacity_share_rows) == 6 * len(bundle.scenario.periods.model)
        fast = "T_LDV_CHRG_PUB_FC"
        fast_group = next(row.group_name for row in prepared.tech_group_member_rows
                          if row.tech == fast and row.group_name.endswith("_GROUP"))
        assert fast_group != fast
        source_ports, power, native_cost, factor, public_l2_kw = (
            (9000, 125, 160000, .065, 91500 * 7.2) if evaluation_year == 2025
            else (22000, 175, 240000, .08, 212500 * 10)
        )
        fast_mix = next(item for item in prepared.audit["installed_capacity_share_targets"]
                        if item["sub_group"] == fast_group and item["period"] == 2025)
        assert fast_mix["source_year"] == evaluation_year
        assert fast_mix["share"] == pytest.approx(source_ports * power / (source_ports * power + public_l2_kw))
        share = next(row for row in prepared.capacity_share_rows if row.sub_group == fast_group and row.period == 2025)
        assert share.share == pytest.approx(fast_mix["share"])
        assert share.data_source and share.data_id and share.dq_time is not None
        costs = pd.DataFrame(prepared.audit["cost_conversion"])
        component = costs.loc[costs.archetype.eq(fast) & costs.vintage.eq(2025)].iloc[0]
        assert component.evaluation_year == evaluation_year
        assert component.power_kw == power and component.native_cost_per_port == native_cost
        assert component.native_dollar_year == 2022
        assert component.cost_native_years == component.power_native_years == (evaluation_year,)
        assert component.count_native_years == component.utilization_native_years == (evaluation_year,)
        assert set(costs.loc[costs.vintage.eq(bundle.scenario.periods.latest_observed_vintage), "evaluation_year"]) == {2025}
        rate = next(row for row in prepared.invest_rows if row.tech == fast and row.vintage == 2025)
        assert rate.cost == pytest.approx(component.cost_cad_per_port / power)
        uf = prepared.utilization.loc[prepared.utilization.tech_or_group.eq(fast)]
        selected = uf.loc[uf.vintage.eq(2025)].iloc[0]
        assert selected.source_period == str(evaluation_year) and selected.factor == pytest.approx(factor)
        private = [row for row in prepared.utilization_rows if row.tech_or_group == "T_LDV_CHRG_PRIV"]
        assert private and all(row.data_source == "T41" for row in private)
        expected = bundle.sources.resolved_data_quality("jonas_ev_charging_utilization", "residential_l2_utilization").row_fields()
        assert all({field: int(getattr(row, field)) for field in expected} == expected for row in private)
        mhdv = [row for row in prepared.utilization_rows if row.tech_or_group.startswith("T_MHDV")]
        contexts = {context.data_id: context for context in prepared.provenance_contexts}
        assert all("operating day" in contexts[row.data_id].dataset_description
                   and "8760" in contexts[row.data_id].dataset_description for row in mhdv)
        assert all("Table 19" in row.notes and "300" in row.notes for row in mhdv)
    else:
        assert len(prepared.capacity_rows) == 2
        assert not prepared.capacity_share_rows
        assert {row.factor for row in prepared.utilization_rows} == {.2, .3}
        assert not stale.exists()
        costs = pd.DataFrame(prepared.audit["cost_conversion"])
        fast = costs.loc[costs.charger_type.eq("DCFC - community") & costs.vintage.eq(2025)].iloc[0]
        assert fast.evaluation_year == evaluation_year
        assert fast.cost_native_years == fast.power_native_years == (evaluation_year,)
    assert all(row.efficiency == 1 for row in prepared.efficiency_rows if "CHRG" in row.tech)


def test_aggregate_component_validation_rejects_hidden_nonfinite_inputs():
    pieces = [{"ports": 1., "power_kw": 10., "cost_cad_per_port": 100., "utilization": .2}]
    broken = deepcopy(pieces)
    broken[0]["utilization"] = float("nan")
    with pytest.raises(ValueError, match="Invalid charger component"):
        aggregate_components(broken)


def test_internal_group_identity_tracks_configured_parent_names(source_bundle, monkeypatch):
    payload = source_bundle.scenario.model_dump(mode="python")
    payload["ev_chargers"]["charger_type_classes"] = "explicit"
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    bundle = replace(source_bundle, scenario=ScenarioConfig.model_validate(payload))
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    configured = deepcopy(load_harmonization_rules(bundle, "ev_chargers"))
    monkeypatch.setattr(ev_chargers, "load_harmonization_rules", lambda _bundle, family: configured
                        if family == "ev_chargers" else load_harmonization_rules(_bundle, family))
    before, _ = ev_chargers.prepare_charger_capacity_to_activity(bundle, technology=technology, regions={"ON"})
    configured["production_representation"]["groups"]["ldv_all"] += "_ALTERNATIVE"
    after, _ = ev_chargers.prepare_charger_capacity_to_activity(bundle, technology=technology, regions={"ON"})
    assert before.data_id != after.data_id


def test_private_utilization_keeps_registry_reviewed_scores_over_dunsky_precedence(source_bundle):
    payload = source_bundle.sources.model_dump(mode="python")
    reviewed = {"dq_cred": 5, "dq_geog": 4, "dq_struc": 3, "dq_tech": 4, "dq_time": 5}
    payload["sources"]["jonas_ev_charging_utilization"]["data_quality"] = reviewed
    bundle = replace(source_bundle, sources=SourcesConfig.model_validate(payload))
    context = ev_chargers._context(
        bundle, [("dunsky_ev_charging_infrastructure", "published_report"),
                 ("jonas_ev_charging_utilization", "residential_l2_utilization")],
        name="private_utilization_review_regression", digest="isolated-review-fixture",
        governing="jonas_ev_charging_utilization")
    fields = context.parameter_fields("limit_annual_capacity_factor", category="charger", sub_category="ldv", tech="T_LDV_CHRG_PRIV")
    assert fields["data_source"] == "T41"
    assert {field: fields[field] for field in reviewed} == reviewed
