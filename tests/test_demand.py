from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from parameterization.build_demand import prepare_demand_rows, real_gdp_indices
from parameterization.offroad_stocks_and_demands import derive_offroad_baseline_demand
from parameterization.road_stocks_and_demands import (
    derive_road_baseline_demand,
    extrapolate_passenger_ldv_demand,
)
from utils import load_config_bundle, load_harmonization_rules, resolve_artifact_path


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def isolated_demand_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "parameterization.build_demand.resolve_artifact_path",
        lambda bundle, family, *parts: tmp_path.joinpath(family, *parts)
        if family == "demand_processed" else resolve_artifact_path(bundle, family, *parts),
    )


def energy_only_rules():
    return {"region_output_map": {"NL": "NLLAB", "PE": "PEI"},
            "energy_intensity_series": {}, "energy_only_series": {"off_road": {
                "provincial_table": 7, "energy_series": "Energy Use by Transportation Mode (PJ)|Off-Road1",
                "commodity": "T_D_pj_off", "units": "PJ"}}}


def test_energy_only_baseline_preserves_pj_and_needs_no_national_intensity():
    provincial = pd.DataFrame([
        {"region": region, "year": 2023, "table_id": 7,
         "raw_series": "Energy Use by Transportation Mode (PJ)|Off-Road1", "unit": "PJ", "value": value}
        for region, value in (("NL", 3.25), ("PE", 0.0))
    ])
    result = derive_offroad_baseline_demand(
        provincial, pd.DataFrame(), regions=["NL", "PE"], base_year=2023, rules=energy_only_rules(),
    )
    assert result.set_index("region").baseline_demand.to_dict() == {"NLLAB": 3.25, "PEI": 0.0}
    assert set(result.commodity) == {"T_D_pj_off"}
    assert set(result.units) == {"PJ"}


@pytest.mark.parametrize("change", ["missing", "duplicate", "year", "unit", "negative", "nan", "infinite", "nonnumeric"])
def test_energy_only_baseline_rejects_bad_source_values(change):
    provincial = pd.DataFrame([{
        "region": "ON", "year": 2023, "table_id": 7,
        "raw_series": "Energy Use by Transportation Mode (PJ)|Off-Road1", "unit": "PJ", "value": 49.69,
    }])
    if change == "missing":
        provincial = provincial.iloc[0:0]
    elif change == "duplicate":
        provincial = pd.concat([provincial, provincial])
    elif change == "year":
        provincial["year"] = 2022
    elif change == "unit":
        provincial["unit"] = "TJ"
    else:
        provincial["value"] = {"negative": -1, "nan": float("nan"),
                               "infinite": float("inf"), "nonnumeric": "bad"}[change]
    with pytest.raises(ValueError):
        derive_offroad_baseline_demand(
            provincial, pd.DataFrame(), regions=["ON"], base_year=2023, rules=energy_only_rules(),
        )


def test_new_energy_demand_preserves_existing_rows_and_uses_regional_provenance(monkeypatch):
    bundle = load_config_bundle("config/scenarios/legacy_comparison.yaml", repo_root=ROOT)
    new_rows, contexts, audit = prepare_demand_rows(bundle)
    energy = [row for row in new_rows if row.commodity == "T_D_pj_off"]
    assert len(energy) == 10 * 5
    baselines = {row["region"]: row["baseline_demand"] for row in audit["energy_only_baselines"]}
    assert baselines["ON"] == pytest.approx(49.69)
    for row in energy:
        assert row.units == "PJ"
        assert row.demand == pytest.approx(baselines[row.region] * audit["gdp_index_by_period"][row.period])
        context = next(c for c in contexts if c.data_id == row.data_id)
        contributors = {c.source_key for c in context.contributors}
        assert contributors == {"nrcan_ceud_transport_provincial", "cer_canadas_energy_future"}
        provincial = next(c for c in context.contributors if c.source_key == "nrcan_ceud_transport_provincial")
        assert {c.component_key for c in provincial.components} == {"7"}
        assert row.data_source == "T01"
    original_rules = load_harmonization_rules

    def without_energy(config, owner):
        rules = original_rules(config, owner)
        if owner == "offroad_stocks_and_demands":
            rules["demand"].pop("energy_only_series")
        return rules

    monkeypatch.setattr("parameterization.build_demand.load_harmonization_rules", without_energy)
    old_rows, _, _ = prepare_demand_rows(bundle)
    assert {r.commodity for r in old_rows} == set(audit["commodities"]) - {"T_D_pj_off"}
    assert [r.model_dump() for r in new_rows if r.commodity != "T_D_pj_off"] == [r.model_dump() for r in old_rows]


def test_road_and_offroad_baselines_use_native_ceud_units() -> None:
    road = pd.DataFrame([{"region": "ON", "year": 2023, "table_id": 20,
                          "raw_series": "Cars|Activity|Passenger-kilometres (millions)",
                          "unit": "millions", "value": 2500.0}])
    road_rules = {"activity_to_billion_factor": 0.001, "region_output_map": {},
                  "activity_series": {"cars": {"table_id": 20,
                      "raw_series": "Cars|Activity|Passenger-kilometres (millions)",
                      "commodity": "T_D_pkm_ldv_c", "units": "bn passenger-km"}}}
    result = derive_road_baseline_demand(road, regions=["ON"], base_year=2023, rules=road_rules)
    assert result.iloc[0].baseline_demand == pytest.approx(2.5)
    with pytest.raises(ValueError, match="Missing or duplicate"):
        derive_road_baseline_demand(pd.concat([road, road]), regions=["ON"],
                                    base_year=2023, rules=road_rules)

    provincial = pd.DataFrame([{"region": "ON", "year": 2023, "table_id": 14,
                                "raw_series": "Passenger Air Transportation Energy Use (PJ)",
                                "unit": "PJ", "value": 12.0}])
    national = pd.DataFrame([{"year": 2023, "table_id": 20,
                              "raw_series": "Passenger Air|Energy Intensity1 (MJ/Pkm)",
                              "unit": "MJ/Pkm", "value": 1.5}])
    rules = {"region_output_map": {}, "energy_intensity_series": {
        "passenger_air": {"provincial_table": 14,
            "energy_series": "Passenger Air Transportation Energy Use (PJ)",
            "national_table": 20,
            "intensity_series": "Passenger Air|Energy Intensity1 (MJ/Pkm)",
            "commodity": "T_D_pkm_hdv_aj", "units": "bn passenger-km"}}}
    result = derive_offroad_baseline_demand(provincial, national, regions=["ON"],
                                             base_year=2023, rules=rules)
    assert result.iloc[0].baseline_demand == pytest.approx(8.0)
    with pytest.raises(ValueError, match="intensity"):
        derive_offroad_baseline_demand(provincial, national.assign(unit="PJ"),
                                       regions=["ON"], base_year=2023, rules=rules)


def test_real_gdp_maps_start_period_to_end_year_and_rejects_gaps() -> None:
    macro = pd.DataFrame([
        {"scenario": "Current Measures", "region": "Canada", "variable_key": "real_gdp",
         "year": year, "value": value, "unit": "$2017 Millions"}
        for year, value in [(2023, 100.0), (2025, 105.0), (2030, 120.0), (2035, 150.0)]
    ])
    assert real_gdp_indices(macro, scenario="Current Measures", base_year=2023,
                            periods=[2025, 2030], step=5) == {2025: 1.2, 2030: 1.5}
    with pytest.raises(ValueError, match="lacks base or period-end"):
        real_gdp_indices(macro.loc[macro.year.ne(2035)], scenario="Current Measures",
                         base_year=2023, periods=[2025, 2030], step=5)


def test_car_cagr_stays_positive_and_preserves_passenger_ldv_total() -> None:
    series = "Cars|Activity|Passenger-kilometres (millions)"
    provincial = pd.DataFrame([
        {"region": "ON", "year": year, "table_id": 20,
         "raw_series": series, "unit": "millions", "value": value}
        for year, value in [(2013, 100000.0), (2023, 50000.0)]
    ])
    baseline = pd.DataFrame([
        {"region": "ON", "service": "cars", "baseline_demand": 50.0},
        {"region": "ON", "service": "passenger_light_trucks", "baseline_demand": 80.0},
    ])
    rules = {
        "activity_to_billion_factor": 0.001, "region_output_map": {},
        "activity_series": {"cars": {"table_id": 20, "raw_series": series}},
        "car_extrapolation": {"history_start_year": 2013,
                              "method": "endpoint_cagr_anchored_to_base_year"},
    }
    projected, audit = extrapolate_passenger_ldv_demand(
        provincial, baseline, regions=["ON"], base_year=2023,
        periods=[2025, 2045], step=5,
        gdp_indices={2025: 1.2, 2045: 1.5}, rules=rules,
    )
    assert projected["ON", 2025, "cars"] == pytest.approx(50 * 0.5**(7 / 10))
    assert projected["ON", 2045, "cars"] == pytest.approx(50 * 0.5**(27 / 10))
    assert projected["ON", 2045, "cars"] > 0
    assert projected["ON", 2045, "cars"] + projected["ON", 2045, "passenger_light_trucks"] == pytest.approx(195.0)
    assert audit[0]["annual_car_cagr"] == pytest.approx(0.5**0.1 - 1)
    with pytest.raises(ValueError, match="endpoints must be finite and positive"):
        extrapolate_passenger_ldv_demand(
            provincial.assign(value=[0.0, 50000.0]), baseline, regions=["ON"],
            base_year=2023, periods=[2025], step=5,
            gdp_indices={2025: 1.2}, rules=rules,
        )
    with pytest.raises(ValueError, match="Missing or duplicate CEUD car CAGR endpoints"):
        extrapolate_passenger_ldv_demand(
            provincial.loc[provincial.year.eq(2023)], baseline, regions=["ON"],
            base_year=2023, periods=[2025], step=5,
            gdp_indices={2025: 1.2}, rules=rules,
        )
    with pytest.raises(ValueError, match="exceeds GDP-indexed"):
        extrapolate_passenger_ldv_demand(
            provincial, baseline, regions=["ON"], base_year=2023,
            periods=[2025], step=5, gdp_indices={2025: 0.1}, rules=rules,
        )


def test_extrapolated_selector_only_reallocates_car_and_passenger_light_trucks() -> None:
    bundle = load_config_bundle("config/scenarios/legacy_comparison.yaml", repo_root=ROOT)
    gdp_rows, _, gdp_audit = prepare_demand_rows(bundle)
    selected = replace(bundle, scenario=bundle.scenario.model_validate({
        **bundle.scenario.model_dump(mode="python"),
        "demand": {"cer_scenario": bundle.scenario.demand.cer_scenario,
                   "future_car_demand": "extrapolated"},
    }))
    extrapolated_rows, _, audit = prepare_demand_rows(selected)
    original = {(row.region, row.period, row.commodity): row for row in gdp_rows}
    changed = {(row.region, row.period, row.commodity): row for row in extrapolated_rows}
    car = "T_D_pkm_ldv_c"
    light_truck = "T_D_pkm_ldv_t"
    assert original.keys() == changed.keys()
    assert audit["future_car_demand"] == "extrapolated"
    assert len(audit["car_extrapolation"]["rows"]) == 10 * 5
    for region in audit["regions"]:
        for period in audit["periods"]:
            assert changed[region, period, car].demand > 0
            assert (changed[region, period, car].demand +
                    changed[region, period, light_truck].demand) == pytest.approx(
                        original[region, period, car].demand +
                        original[region, period, light_truck].demand
                    )
    for key, row in original.items():
        if key[2] not in {car, light_truck}:
            assert changed[key].demand == row.demand
            assert changed[key].data_id == row.data_id
    assert gdp_audit["gdp_index_by_period"] == audit["gdp_index_by_period"]


@pytest.mark.parametrize(("edition", "cer_scenario"), [
    (2026, "Current Measures"), (2026, "Canada Net-zero"),
    (2026, "Higher Scenario"), (2026, "Lower Scenario"),
    (2023, "Global Net-zero"),
])
def test_registered_cer_scenarios_produce_deterministic_coverage(
    edition: int, cer_scenario: str,
) -> None:
    bundle = load_config_bundle("config/scenarios/legacy_comparison.yaml", repo_root=ROOT)
    payload = bundle.scenario.model_dump(mode="python")
    payload["demand"]["cer_scenario"] = cer_scenario
    payload["sources"]["selections"]["cer_canadas_energy_future"]["edition"] = edition
    scenario = bundle.scenario.model_validate(payload)
    selected = replace(bundle, scenario=scenario)
    rows, contexts, audit = prepare_demand_rows(selected)
    assert len(rows) == 10 * 15 * 5
    assert len(contexts) == 15
    assert audit["cer_scenario"] == cer_scenario
    assert set(audit["gdp_index_by_period"]) == set(scenario.periods.model)
    on_car = next(row for row in rows if (row.region, row.period, row.commodity)
                  == ("ON", 2025, "T_D_pkm_ldv_c"))
    on_air = next(row for row in rows if (row.region, row.period, row.commodity)
                  == ("ON", 2025, "T_D_pkm_hdv_aj"))
    assert on_car.demand == pytest.approx(84.208902285 * audit["gdp_index_by_period"][2025])
    assert on_air.demand == pytest.approx(
        (94.288491 / 1.26786988510577) * audit["gdp_index_by_period"][2025], rel=1e-5
    )
    assert on_car.units == on_air.units == "bn passenger-km"
    assert all(row.data_id and row.data_source and row.dq_time is not None for row in rows)
