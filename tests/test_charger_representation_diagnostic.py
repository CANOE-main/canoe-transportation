"""Conservation and research sensitivities around the production charger method."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml
from pydantic import ValidationError

from validation import charger_representation_diagnostic as preview
from parameterization import charger_representation as production
from utils import load_config_bundle, load_harmonization_rules
from validation.charger_stock_diagnostic import ChargerStockEvidence


@pytest.fixture
def config():
    path = Path(__file__).resolve().parents[1] / "config/parameters/charger_infrastructure_diagnosis.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["representation_diagnostic"]


@pytest.fixture
def settings(config):
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml")
    return preview.ChargerDiagnosticSettings.model_validate({
        **load_harmonization_rules(bundle, "ev_chargers")["production_representation"], **config,
        "private_home_utilization_reference": bundle.scenario.ev_chargers.ld_home_utilization,
        "fixed_om_fraction": 0.01,
    })


@pytest.mark.parametrize("change", [
    {"diagnostic_only": False}, {"years": [2030, 2025]},
    {"mhdv_operating_days": 0}, {"private_home_utilization_reference": 1.01},
    {"capacity_basis": "port_share_as_GW"}, {"private_home_utilization_reference": float("nan")},
    {"mhdv_recharge_fractions": [0.75, 0.60]}, {"mhdv_recharge_fractions": [0.60, 1.01]},
    {"public_dcfc_utilization_basis": "conditional_nameplate_energy_transfer"},
    {"negligible_public_l1": False},
])
def test_diagnostic_settings_reject_incoherent_choices(settings, change):
    with pytest.raises(ValidationError):
        preview.ChargerDiagnosticSettings.model_validate({**settings.model_dump(), **change})


def test_display_sensitivities_include_nonlisted_scenario_reference_without_mutation(settings):
    choices = [0.30, 0.10, 0.20, 0.10]
    payload = {**settings.model_dump(), "private_home_utilization_reference": 0.09,
               "private_home_utilization_cases": choices}
    original = deepcopy(payload)

    result = preview.ChargerDiagnosticSettings.model_validate(payload)

    assert result.private_home_utilization_cases == [0.09, 0.10, 0.20, 0.30]
    assert result.model_dump()["private_home_utilization_cases"] == [0.09, 0.10, 0.20, 0.30]
    assert payload == original
    assert choices == [0.30, 0.10, 0.20, 0.10]


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), 0, -0.01, 1.01])
def test_invalid_display_sensitivities_reject_before_reference_expansion(settings, invalid):
    with pytest.raises(ValidationError):
        preview.ChargerDiagnosticSettings.model_validate({
            **settings.model_dump(), "private_home_utilization_reference": 0.09,
            "private_home_utilization_cases": [0.10, invalid],
        })


def test_source_points_interpolate_and_hold_without_back_extrapolation():
    frame = pd.DataFrame({"charger": ["a", "a"], "year": [2025, 2040], "value": [100.0, 400.0]})
    assert preview.source_value(frame, 2030, charger="a") == 200
    assert preview.source_value(frame, 2050, charger="a") == 400
    with pytest.raises(ValueError, match="precedes"):
        preview.source_value(frame, 2020, charger="a")
    with pytest.raises(ValueError, match="duplicate"):
        preview.source_value(pd.concat([frame, frame]), 2025, charger="a")


def test_aggregate_conserves_expenditure_power_and_energy_instead_of_averaging_rates():
    pieces = [
        {"ports": 1, "power_kw": 1, "cost_cad_per_port": 100, "utilization": 0.1},
        {"ports": 1, "power_kw": 9, "cost_cad_per_port": 900, "utilization": 0.9},
    ]
    original = deepcopy(pieces)
    result = preview.aggregate_components(pieces)
    assert result["port_count"] == 2
    assert result["rated_power_kw"] == 5
    assert result["capex_mcad_per_gw"] == 100
    assert result["annual_utilization"] == pytest.approx(0.82)
    assert result["annual_utilization"] != 0.5
    assert pieces == original


@pytest.fixture
def parameters(settings):
    a = settings.archetypes
    records = []
    for year in settings.years:
        for name, count, power in [
            (a.ldv_public_l2, 90, 10), (a.ldv_public_fast, 10, 100),
            (a.ldv_private, 900, 5), (a.mhdv_overnight, 90, 50), (a.mhdv_fast, 10, 350),
        ]:
            records.append({"year": year, "archetype": name, "port_count": count, "rated_power_kw": power})
    return pd.DataFrame(records)


def test_available_capacity_constraints_use_power_shares_and_period_reference_years(parameters, settings):
    rows, groups = production.capacity_share_targets(parameters, settings, ["ON", "QC"], {2025: 2030, 2050: 2055})
    assert len(rows) == 24
    assert set(rows.operator) == {"e"}
    assert set(rows.capacity_units) == {"GW"}
    fast_group = next(group for group, members in groups.items() if members == [settings.archetypes.mhdv_fast])
    selected = rows.loc[rows.region.eq("ON") & rows.period.eq(2025) & rows.sub_group.eq(fast_group)].iloc[0]
    assert selected.source_year == 2030
    assert selected.port_share == 0.1
    assert selected.share == pytest.approx(3500 / (4500 + 3500))
    assert "data_id" not in rows  # No invented publication provenance.
    for _, group in rows.groupby(["region", "period", "super_group"]):
        assert group.share.sum() == pytest.approx(1)


@pytest.fixture
def mhdv_components(settings):
    return pd.DataFrame([
        {"year": 2025, "archetype": settings.archetypes.mhdv_overnight,
         "location": "Private", "charger": "Overnight", "ports": 80,
         "power_kw": 50, "session_hours": 8, "vehicles_per_day": 1},
        {"year": 2025, "archetype": settings.archetypes.mhdv_overnight,
         "location": "Public", "charger": "Overnight", "ports": 20,
         "power_kw": 100, "session_hours": 6, "vehicles_per_day": 1.5},
        {"year": 2025, "archetype": settings.archetypes.mhdv_fast,
         "location": "Private", "charger": "FC", "ports": 10,
         "power_kw": 350, "session_hours": 0.5, "vehicles_per_day": 3},
        {"year": 2025, "archetype": settings.archetypes.mhdv_fast,
         "location": "Public", "charger": "UFC", "ports": 2,
         "power_kw": 2000, "session_hours": 0.5, "vehicles_per_day": 6},
    ])


def test_mhdv_hours_and_capacity_weighted_utilization_match_hand_calculation(mhdv_components, settings):
    batteries = pd.DataFrame([{"year": 2025, "battery_kwh": 400}])
    components = preview.mhdv_utilization_components(mhdv_components, settings, batteries)
    overnight = components.loc[components.archetype.eq(settings.archetypes.mhdv_overnight)]
    recharge = overnight.loc[overnight["case"].eq("battery_recharge_0.6")].set_index("location")
    assert recharge.loc["Private", "charging_hours"] == pytest.approx(4.8)
    assert recharge.loc["Public", "charging_hours"] == pytest.approx(2.4)
    assert recharge.loc["Public", "occupied_hours_per_day"] == pytest.approx(3.6)
    assert recharge.loc["Public", "annual_utilization"] == pytest.approx(3.6 * 300 / 8760)

    cases = preview.mhdv_utilization_cases(components)
    pooled = cases.loc[cases.archetype.eq(settings.archetypes.mhdv_overnight)].set_index("case")
    # Private/public capacities are 4,000/2,000 kW, not an 80/20 port weighting.
    assert pooled.loc["table19_duration", "annual_utilization"] == pytest.approx(50_000 * 300 / (8760 * 6000))
    expected_recharge = 26_400 * 300 / (8760 * 6000)
    assert pooled.loc["battery_recharge_0.6", "annual_utilization"] == pytest.approx(expected_recharge)
    assert pooled.loc["battery_recharge_0.6", "annual_utilization"] != pytest.approx((0.8 * 4.8 + 0.2 * 3.6) * 300 / 8760)
    assert pooled.loc["battery_recharge_0.75", "annual_utilization"] / expected_recharge == pytest.approx(1.25)
    assert set(cases.operating_days) == {300}
    assert cases.status.str.contains("not measured").all()


def test_table19_reference_time_does_not_depend_on_battery(mhdv_components, settings):
    small = preview.mhdv_utilization_components(mhdv_components, settings, pd.DataFrame({"year": [2025], "battery_kwh": [400]}))
    large = preview.mhdv_utilization_components(mhdv_components, settings, pd.DataFrame({"year": [2025], "battery_kwh": [800]}))
    reference = small["case"].eq("table19_duration")
    pd.testing.assert_series_equal(small.loc[reference, "annual_utilization"], large.loc[reference, "annual_utilization"])
    recharge = small["case"].eq("battery_recharge_0.6")
    assert (large.loc[recharge, "charging_hours"] / small.loc[recharge, "charging_hours"]).eq(2).all()


@pytest.mark.parametrize("batteries, message", [
    (pd.DataFrame({"year": [2025, 2025], "battery_kwh": [400, 400]}), "one value"),
    (pd.DataFrame({"year": [2030], "battery_kwh": [400]}), "Every charger year"),
    (pd.DataFrame({"year": [2025], "battery_kwh": [0]}), "positive finite"),
    (pd.DataFrame({"year": [2025], "battery_kwh": [float("nan")]}), "positive finite"),
])
def test_mhdv_rejects_unusable_battery_evidence(mhdv_components, settings, batteries, message):
    with pytest.raises(ValueError, match=message):
        preview.mhdv_utilization_components(mhdv_components, settings, batteries)


@pytest.mark.parametrize("field,value", [
    ("ports", -1), ("power_kw", 0), ("session_hours", float("nan")), ("vehicles_per_day", -2),
])
def test_mhdv_rejects_invalid_physical_charging_inputs(mhdv_components, settings, field, value):
    invalid = mhdv_components.copy()
    invalid.loc[0, field] = value
    with pytest.raises(ValueError, match=field):
        preview.mhdv_utilization_components(invalid, settings, pd.DataFrame({"year": [2025], "battery_kwh": [400]}))


def test_impossible_daily_occupation_is_flagged_without_clipping(mhdv_components, settings):
    components = preview.mhdv_utilization_components(
        mhdv_components, settings, pd.DataFrame({"year": [2025], "battery_kwh": [10_000]}))
    private = components.loc[components.location.eq("Private") & components.charger.eq("Overnight")
                             & components["case"].eq("battery_recharge_0.75")].iloc[0]
    assert private.charging_hours == 150
    assert private.occupied_hours_per_day == 150
    assert private.annual_utilization == pytest.approx(150 * 300 / 8760)
    assert private.annual_utilization > 1
    assert private.exceeds_reference_duration
    assert not private.feasible_daily_schedule
    cases = preview.mhdv_utilization_cases(components)
    assert not cases.loc[cases.archetype.eq(settings.archetypes.mhdv_overnight)
                         & cases["case"].eq("battery_recharge_0.75"), "feasible_daily_schedule"].item()


def test_source_cost_preserves_native_currency_and_year():
    diagnostic = SimpleNamespace(costs=pd.DataFrame([
        {"vehicle_class": "LDV", "charger": "L2", "year": 2025, "value": 100, "unit": "CAD2022/port"},
    ]))
    calls = []

    def convert(value, **kwargs):
        calls.append((value, kwargs))
        return SimpleNamespace(target_cad_value=value * 1.7)

    result = production._source_cost(diagnostic.costs, SimpleNamespace(convert=convert), 2025, "LDV", "L2")
    assert result == (170, 100, "CAD", 2022, (2025,))
    assert calls == [(100, {"currency": "CAD", "dollar_year": 2022})]


@pytest.fixture
def fleet_counts():
    # Partial MHDV evidence is a known lower bound, not an observed complete fleet.
    return pd.DataFrame([
        {"region": "ON", "vehicle_class": "LDV", "road_class": "cars", "bev_vehicles": 40.0, "known_bev_vehicles": 40.0, "source_year": 2023},
        {"region": "ON", "vehicle_class": "LDV", "road_class": "light_trucks", "bev_vehicles": 60.0, "known_bev_vehicles": 60.0, "source_year": 2023},
        {"region": "ON", "vehicle_class": "MHDV", "road_class": "medium_trucks", "bev_vehicles": 30.0, "known_bev_vehicles": 30.0, "source_year": 2023},
        {"region": "ON", "vehicle_class": "MHDV", "road_class": "heavy_trucks", "bev_vehicles": float("nan"), "known_bev_vehicles": 15.0, "source_year": 2023},
        {"region": "QC", "vehicle_class": "LDV", "road_class": "cars", "bev_vehicles": 50.0, "known_bev_vehicles": 50.0, "source_year": 2023},
        {"region": "QC", "vehicle_class": "LDV", "road_class": "light_trucks", "bev_vehicles": 50.0, "known_bev_vehicles": 50.0, "source_year": 2023},
        {"region": "QC", "vehicle_class": "MHDV", "road_class": "medium_trucks", "bev_vehicles": 60.0, "known_bev_vehicles": 60.0, "source_year": 2023},
        {"region": "QC", "vehicle_class": "MHDV", "road_class": "heavy_trucks", "bev_vehicles": 30.0, "known_bev_vehicles": 30.0, "source_year": 2023},
    ])


def existing_inputs(tmp_path, monkeypatch):
    pd.DataFrame([
        {"source_region": region, "as_of": "2025-12-31", "public_chargers": 20, "level_1_ports": 2,
         "level_2_ports": 10, "dc_fast_ports": 8} for region in ("Ontario", "Quebec")
    ]).to_csv(tmp_path / "public.csv", index=False)
    monkeypatch.setattr(preview, "resolve_artifact_path", lambda bundle, route, name: tmp_path / name)
    monkeypatch.setattr(preview, "load_harmonization_rules", lambda bundle, key: (
        {"public_charger_region_map": {"ON": ["Ontario"], "QC": ["Quebec"]}}
        if key == "ev_chargers" else {"province_output_file": "public.csv"}))
    return SimpleNamespace(scenario=SimpleNamespace(
        ev_chargers=SimpleNamespace(evs_per_port=[1, 1.5, 1.5], ld_home_utilization=0.08, charger_type_classes="aggregated"),
        periods=SimpleNamespace(base_year=2023)))


def stock_evidence(fleet):
    return ChargerStockEvidence(fleet_counts=fleet, components=pd.DataFrame(), metadata={"basis": "point-year fixture"})


def test_existing_private_residual_excludes_negligible_public_l1_but_audits_it(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    rows, metadata = preview._existing_capacity(parameters, stock_evidence(fleet_counts), bundle, settings)
    assert set(rows.archetype) == set(settings.archetypes.model_dump().values())
    assert not rows.archetype.str.contains("L1").any()
    for region in ("ON", "QC"):
        ldv = rows.loc[rows.region.eq(region) & rows.vehicle_class.eq("LDV")].set_index("archetype")
        assert ldv.loc[settings.archetypes.ldv_private, "ports"] == 80
        assert ldv.loc[settings.archetypes.ldv_public_l2, "ports"] == 10
        assert ldv.loc[settings.archetypes.ldv_public_fast, "ports"] == 8
        # All 20 public ports leave the private residual; only 18 are represented.
        calculation = next(item for item in metadata["stock_calculations"]
                           if item["region"] == region and item["vehicle_class"] == "LDV")
        assert calculation["ignored_public_l1_ports"] == 2
        assert ldv.ports.sum() + calculation["ignored_public_l1_ports"] == calculation["total_inferred_ports"] == 100
        assert ldv.loc[settings.archetypes.ldv_private, "capacity_gw"] == pytest.approx(80 * 5 / 1e6)
    assert metadata["stock_regions"] == ["ON", "QC"]
    assert metadata["afdc_snapshot_dates"] == ["2025-12-31"]
    assert set(rows.stock_reference_year) == {2023}
    assert set(rows.stock_observation_year) == {2023}
    assert set(rows.power_source_year) == {2025}


def test_partial_mhdv_stock_is_a_lower_bound_using_national_mix_in_each_region(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    rows, metadata = preview._existing_capacity(parameters, stock_evidence(fleet_counts), bundle, settings)
    mhdv = rows.loc[rows.vehicle_class.eq("MHDV")]
    expected = {"ON": (27, 3), "QC": (54, 6)}
    for region, (night, fast) in expected.items():
        regional = mhdv.loc[mhdv.region.eq(region)].set_index("archetype")
        assert regional.loc[settings.archetypes.mhdv_overnight, "ports"] == pytest.approx(night)
        assert regional.loc[settings.archetypes.mhdv_fast, "ports"] == pytest.approx(fast)
        assert regional.loc[settings.archetypes.mhdv_overnight, "ports"] / regional.ports.sum() == pytest.approx(0.9)
    assert mhdv.loc[mhdv.region.eq("ON"), "status"].str.contains("Lower bound").all()
    assert mhdv.loc[mhdv.region.eq("QC"), "status"].str.contains("Point-year").all()
    on = next(item for item in metadata["stock_calculations"] if item["region"] == "ON" and item["vehicle_class"] == "MHDV")
    assert on["known_bev_vehicles"] == 45
    assert on["total_inferred_ports"] == 30


def test_unpublished_ldv_components_do_not_yield_a_spurious_private_residual(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    fleet_counts.loc[fleet_counts.region.eq("ON") & fleet_counts.road_class.eq("light_trucks"),
                     ["bev_vehicles", "known_bev_vehicles"]] = float("nan")
    rows, _ = preview._existing_capacity(parameters, stock_evidence(fleet_counts), bundle, settings)
    private = rows.loc[rows.region.eq("ON") & rows.archetype.eq(settings.archetypes.ldv_private)]
    assert private.ports.isna().all()
    assert private.capacity_gw.isna().all()


def test_fully_missing_stock_stays_missing_instead_of_zero(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    fleet_counts.loc[fleet_counts.region.eq("ON"), ["bev_vehicles", "known_bev_vehicles"]] = float("nan")
    rows, metadata = preview._existing_capacity(parameters, stock_evidence(fleet_counts), bundle, settings)
    on = rows.loc[rows.region.eq("ON")]
    inferred = on.loc[on.vehicle_class.eq("MHDV") | on.archetype.eq(settings.archetypes.ldv_private)]
    assert inferred.ports.isna().all()
    assert inferred.capacity_gw.isna().all()
    assert on.status.str.contains("unavailable").all()
    calculations = pd.DataFrame(metadata["stock_calculations"])
    assert calculations.loc[calculations.region.eq("ON"), "total_inferred_ports"].isna().all()


def test_inconsistent_existing_public_inventory_is_not_clipped_to_zero(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    ldv = fleet_counts.vehicle_class.eq("LDV")
    fleet_counts.loc[ldv, ["bev_vehicles", "known_bev_vehicles"]] = 5.0
    with pytest.raises(ValueError, match="exceed"):
        preview._existing_capacity(parameters, stock_evidence(fleet_counts), bundle, settings)


def test_empty_stock_evidence_preserves_filterable_schema(tmp_path, monkeypatch, parameters, settings, fleet_counts):
    bundle = existing_inputs(tmp_path, monkeypatch)
    rows, metadata = preview._existing_capacity(parameters, stock_evidence(fleet_counts.iloc[:0]), bundle, settings)
    assert rows.empty
    assert rows.loc[rows.vehicle_class.eq("LDV")].empty
    assert set(rows) == {"region", "vehicle_class", "archetype", "ports", "rated_power_kw",
                         "capacity_gw", "stock_reference_year", "power_source_year",
                         "stock_observation_year", "status"}
    assert metadata["stock_regions"] == []


@pytest.fixture
def source_counts(settings):
    light = {"L2 - community": 80, "L2 - workplace": 10,
             "DCFC - community": 5, "DCFC - corridor + rural": 3, "DCFC - taxis + rideshare": 2,
             "L1 - home": 100, "L2 - home": 790, "L2 - work": 10, "Total Private": 900}
    freight = {
        2025: {"Public": [10, 5, 0], "Private": [80, 5, 0]},
        2040: {"Public": [20, 20, 5], "Private": [120, 30, 5]},
    }
    records = []
    for year, multiplier in ((2025, 1), (2040, 2)):
        records.extend({"table": 6, "scenario": settings.ldv_scenario, "year": year,
                        "charger": component, "value": count * multiplier}
                       for component, count in light.items())
        records.extend({"table": 38, "scenario": settings.mhdv_scenario, "year": year,
                        "location": access, "charger": charger, "value": count}
                       for access, counts in freight[year].items()
                       for charger, count in zip(("Overnight", "FC", "UFC"), counts, strict=True))
        records.append({"table": 38, "scenario": settings.mhdv_scenario, "year": year,
                        "location": "Total Ports", "charger": "Total Ports",
                        "value": 108 if year == 2025 else 200})
    records.append({"table": 6, "scenario": "Another scenario", "year": 2025,
                    "charger": "L2 - community", "value": 999_999})
    return pd.DataFrame(records)


def test_composition_keeps_distinct_workplace_halves_and_holds_after_2040(source_counts, settings):
    rows = production.composition_components({
        "ldv": source_counts.loc[source_counts.table.eq(6)],
        "mhdv": source_counts.loc[source_counts.table.eq(38)],
    }, settings, settings.years)
    base = rows.loc[rows.year.eq(2025)]
    assert base.loc[base.vehicle_class.eq("LDV"), "ports"].sum() == 1000
    assert base.loc[base.vehicle_class.eq("MHDV"), "ports"].sum() == 100
    # Published MHDV total 108 is audited separately; components define the mix.
    assert base.loc[base.component.isin(["L2 - workplace", "L2 - work"]), "ports"].tolist() == [10, 10]
    assert set(rows.archetype) == set(settings.archetypes.model_dump().values())
    assert rows.groupby(["vehicle_class", "year"]).population_share.sum().tolist() == pytest.approx([1] * 12)
    assert rows.groupby(["archetype", "year"]).within_archetype_share.sum().tolist() == pytest.approx([1] * 30)
    projected = rows.loc[rows.year.eq(2030) & rows.component.eq("L2 - community"), "ports"].item()
    assert projected == pytest.approx(80 + (160 - 80) / 3)
    keys = ["vehicle_class", "archetype", "component"]
    held = rows.loc[rows.year.eq(2050)].set_index(keys).sort_index()
    end = rows.loc[rows.year.eq(2040)].set_index(keys).sort_index()
    pd.testing.assert_series_equal(held.ports, end.ports)
    pd.testing.assert_series_equal(held.population_share, end.population_share)
    assert set(held.source_year) == {2040}


def test_shared_production_core_preserves_reported_ldv_utilization_and_component_conservation(
    tmp_path, monkeypatch, config, settings, fleet_counts, source_counts,
):
    bundle = existing_inputs(tmp_path, monkeypatch)
    bundle.scenario.economics = SimpleNamespace(cost_reference_year=2020)
    bundle.scenario.geography = SimpleNamespace(regions=["ON", "QC"])
    bundle.scenario.periods.model = [2025, 2050]
    bundle.scenario.periods.evaluation_year = lambda period: period
    config_path = tmp_path / "diagnostic.yaml"
    config_path.write_text(yaml.safe_dump({"representation_diagnostic": config}), encoding="utf-8")
    pd.DataFrame([{"sub_category": "ldv", "parameter": "L1_capex", "value": 550,
                   "unit": "USD", "currency_year": 2021},
                  {"sub_category": "all", "parameter": "fixed_to_capex_ratio", "value": 0.01,
                   "unit": "fraction", "currency_year": ""}]).to_csv(tmp_path / "manual.csv", index=False)
    monkeypatch.setattr(preview, "resolve_parameter_path", lambda *_: config_path)
    fingerprints = {"rules.yaml": "b" * 64, "charging_and_range_representation.yaml": "c" * 64}
    monkeypatch.setattr(preview, "harmonization_rule_fingerprints", lambda _: fingerprints)
    monkeypatch.setattr(preview, "resolve_input_path", lambda *_: tmp_path / "manual.csv")
    harmonization = {
        "ev_chargers": {"manual_file": "manual.csv", "public_charger_region_map": {"ON": ["Ontario"], "QC": ["Quebec"]},
                        "production_representation": settings.model_dump(include=set(production.ChargerRepresentationSettings.model_fields))},
        "afdc": {"province_output_file": "public.csv"},
        "road_stocks_and_demands": {"existing_capacity": {"region_output_map": {}}},
    }
    monkeypatch.setattr(preview, "load_harmonization_rules", lambda _, key: harmonization[key])
    # Currency acquisition and stock/battery extraction have their own boundaries;
    # the preview's source-to-parameter calculations below remain real.
    monkeypatch.setattr(preview, "_converter", lambda *_: SimpleNamespace(
        convert=lambda value, **_: SimpleNamespace(target_cad_value=value * 1.7)))
    battery = SimpleNamespace(pooled_battery=pd.DataFrame({"year": settings.years, "battery_kwh": [400] * len(settings.years)}))
    monkeypatch.setattr(preview, "load_battery_evidence", lambda *_, **__: battery)
    monkeypatch.setattr(preview, "load_stock_evidence", lambda *_, **__: stock_evidence(fleet_counts))
    monkeypatch.setattr(preview, "prepare_charger_utilization_targets", lambda *_, **__: (
        pd.DataFrame([{"region": "ON", "tech_or_group": "AGGREGATED_LDV", "vintage": 2025, "target_utilization": .2}]), None))
    powers, costs, utilizations = [], [], []
    for year in (2025, 2040):
        powers.extend({"charger": charger, "year": year, "value": power} for charger, power in {
            "Home - L1": 1.5, "Home - L2": 7.2, "Workplace": 11,
            "Public - L2": 10 if year == 2025 else 12,
            "Public - DCFC": 200 if year == 2025 else 400,
        }.items())
        costs.extend({"vehicle_class": vehicle_class, "charger": charger, "year": year,
                      "value": cost, "rated_power_kw": rating, "unit": "CAD2022/port"}
                     for vehicle_class, charger, cost, rating in [
                         ("LDV", "L2", 10_000, 10),
                         ("LDV", "DCFC", 200_000, 125 if year == 2025 else 300),
                         ("MHDV", "Overnight", 50_000, 50),
                         ("MHDV", "Fast Charge", 100_000, 350),
                         ("MHDV", "Ultra-Fast Charge", 1_000_000, 2000),
                     ])
        utilizations.extend({"charger": charger, "year": year, "value": factor} for charger, factor in {
            "Public - L2": 0.15 if year == 2025 else 0.20,
            "Public - DCFC": 0.065 if year == 2025 else 0.11,
            "Workplace": 0.10 if year == 2025 else 0.20,
        }.items())
    schedule = pd.DataFrame([
        {"location": access, "charger": charger, "power_kw": power, "session_hours": hours, "vehicles_per_day": throughput}
        for access in ("Private", "Public")
        for charger, power, hours, throughput in [
            ("Overnight (CCS)", 50 if access == "Private" else 100, 8 if access == "Private" else 6, 1 if access == "Private" else 1.5),
            ("Fast (CCS)", 350, 0.5, 3 if access == "Private" else 6),
            ("Ultrafast (MCS)", 2000, 0.5, 3 if access == "Private" else 6),
        ]
    ])
    diagnostic = SimpleNamespace(counts=source_counts, power=pd.DataFrame(powers), costs=pd.DataFrame(costs),
                                 utilization=pd.DataFrame(utilizations), mhd_utilization=schedule,
                                 metadata={"source_sha256": "a" * 64})

    result = preview.build_diagnostic(diagnostic, bundle=bundle)

    assert len(result.parameters) == 30
    assert len(result.constraints) == 24
    assert result.metadata["constraint_period_source_years"] == {2025: 2025, 2050: 2050}
    assert result.metadata["read_only"] and result.metadata["production_adoption"]
    assert result.metadata["configured_representation"] == "aggregated"
    assert result.metadata["production_rules_fingerprints"] == fingerprints
    assert result.metadata["stock_alternative_adopted"] is False
    assert result.metadata["type_specific_hourly_profiles"] == "pending"
    assert result.metadata["configured_aggregate_utilization_targets"][0]["target_utilization"] == .2
    fast = result.parameters.loc[result.parameters.archetype.eq(settings.archetypes.ldv_public_fast)].set_index("year")
    assert fast.loc[2025, "rated_power_kw"] == 125
    assert fast.loc[2025, "installed_nameplate_kw"] == 200
    assert fast.loc[2025, "annual_utilization"] == pytest.approx(0.065)
    assert fast.loc[2050, "annual_utilization"] == pytest.approx(0.11)
    assert fast.loc[2050, "count_source_year"] == 2040
    private = result.parameters.loc[result.parameters.archetype.eq(settings.archetypes.ldv_private)
                                    & result.parameters.year.eq(2025)].iloc[0]
    assert private.rated_power_kw == pytest.approx((100 * 1.5 + 790 * 7.2 + 10 * 11) / 900)
    assert private.annual_utilization == pytest.approx((100 * 1.5 * 0.08 + 790 * 7.2 * 0.08 + 10 * 11 * 0.10) / 5948)
    assert pd.isna(private.source_annual_utilization)
    assert result.parameters.groupby(["vehicle_class", "year"]).capacity_share.sum().tolist() == pytest.approx([1] * 12)
    for (year, archetype), components in result.composition.groupby(["year", "archetype"]):
        parameter = result.parameters.loc[result.parameters.year.eq(year) & result.parameters.archetype.eq(archetype)].iloc[0]
        assert parameter.port_count == pytest.approx(components.ports.sum())
    mhdv = result.parameters.loc[result.parameters.vehicle_class.eq("MHDV")]
    assert (mhdv["recharge_0.75_utilization"] / mhdv["recharge_0.6_utilization"]).tolist() == pytest.approx([1.25] * 12)
    singleton_groups = {group: members[0] for group, members in result.metadata["group_members"].items()
                        if len(members) == 1}
    assert set(singleton_groups.values()) == set(settings.archetypes.model_dump().values())
    assert set(singleton_groups).isdisjoint(singleton_groups.values())
