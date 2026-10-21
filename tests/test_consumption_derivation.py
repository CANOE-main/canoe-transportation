"""Consumption-space source calculations and exact historical rating support."""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from parameterization.offroad_efficiencies import derive_offroad_consumption, manual_ratio
from parameterization.road_efficiencies import RoadEfficiencyEvidence, observed_value, require_observed_value, source_point


@pytest.fixture
def rules():
    path = Path(__file__).resolve().parents[1] / "config/parameters/rules.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["parameterization"]["efficiencies"]


def evidence(rules, ratings=(), atb=(), *, modes=("cars",), ceud=None, gcam=None, regen=None):
    rated = pd.DataFrame([
        {"ceud_class": "Car", "powertrain": powertrain, "year": year,
         "native_class_mean": value, "class_mean_mj_per_vkm": value, "weight": 1}
        for powertrain, year, value in ratings
    ], columns=["ceud_class", "powertrain", "year", "native_class_mean", "class_mean_mj_per_vkm", "weight"])
    projected = pd.DataFrame([
        {"region": "ON", "mode": mode, "powertrain": powertrain, "year": year,
         "weighted_consumption": value}
        for mode, powertrain, year, value in atb
    ], columns=["region", "mode", "powertrain", "year", "weighted_consumption"])
    loads = pd.DataFrame([
        {"region": "ON", "mode": mode, "year": year, "load_factor": load}
        for mode in modes for year, load in [(2000, 5), (2010, 6), (2011, 7), (2012, 8), (2023, 10)]
    ])
    return RoadEfficiencyEvidence(
        ceud=pd.DataFrame() if ceud is None else ceud, loads=loads,
        rating_audit=rated, atb_audit=projected,
        gcam=pd.DataFrame() if gcam is None else gcam, regen=pd.DataFrame() if regen is None else regen,
        base_year=2023, rules=rules,
    )


def test_exact_observations_never_fill_gaps_or_require_unrelated_years():
    source = pd.DataFrame({"year": [2010, 2012, 2013], "value": [2, 4, float("nan")]})
    assert observed_value(source, 2010, "value") == 2
    assert observed_value(source, 2011, "value") is None
    assert observed_value(source, 2013, "value") is None
    with pytest.raises(ValueError, match="Ambiguous"):
        observed_value(pd.concat([source, source]), 2010, "value")
    with pytest.raises(ValueError, match="positive"):
        observed_value(pd.DataFrame({"year": [2010], "value": [-1]}), 2010, "value")
    with pytest.raises(ValueError, match="Missing exact"):
        require_observed_value(source, 2011, "value")


def test_exact_energy_lookup_allows_zero_only_when_explicitly_requested():
    source = pd.DataFrame({"year": [2023, 2025, 2030], "value": [0., -1., float("inf")]})
    assert observed_value(source, 2023, "value", allow_zero=True) == 0
    assert require_observed_value(source, 2023, "value", allow_zero=True) == 0
    with pytest.raises(ValueError, match="positive"):
        observed_value(source, 2023, "value")
    for year in [2025, 2030]:
        with pytest.raises(ValueError, match="finite and non-negative"):
            require_observed_value(source, year, "value", allow_zero=True)


@pytest.mark.parametrize("requested,expected", [(2010, (4, 2020)), (2023, (4, 2020)),
                                               (2025, (2, 2025)), (2030, (2, 2025))])
def test_source_point_holds_observed_values_and_reports_the_selected_year(requested, expected):
    source = pd.DataFrame({"year": [2020, 2025], "value": [4, 2]})
    assert source_point(source, requested, "value") == expected
    with pytest.raises(ValueError, match="ambiguous"):
        source_point(pd.concat([source, source]), requested, "value")


def test_period_uses_observed_years_without_filling_partial_support(rules):
    inputs = evidence(rules, ratings=[
        ("diesel", 2010, 2), ("diesel", 2023, 1),
        ("gasoline", 2010, 4), ("gasoline", 2011, 3), ("gasoline", 2023, 2),
    ])
    assert inputs.rating("cars", "diesel", 2011, "mj") is None
    assert inputs.derive_consumption("ON", "cars", "diesel", 2011) is None
    observations = inputs.period_observations(
        "ON", "cars", "diesel", [2010, 2011], allow_analogue_when_empty=True,
    )
    assert set(observations) == {2010}
    assert observations[2010]["native_intensity"] == 2
    assert "efficiency" not in observations[2010]


def test_empty_period_proxy_requires_explicit_opt_in(rules):
    inputs = evidence(rules, ratings=[
        ("diesel", 2023, 1), ("gasoline", 2010, 4), ("gasoline", 2011, 3), ("gasoline", 2023, 2),
    ])
    assert inputs.period_observations("ON", "cars", "diesel", [2010, 2011]) == {}
    observations = inputs.period_observations(
        "ON", "cars", "diesel", [2010, 2011], allow_analogue_when_empty=True,
    )
    assert [row["native_intensity"] for row in observations.values()] == [2, 1.5]
    assert {row["analogue"] for row in observations.values()} == {"gasoline"}
    assert all("approved_analogue" in row["treatment"] for row in observations.values())


@pytest.mark.parametrize("powertrain", ["diesel", "bev_150_miles"])
def test_empty_period_proxy_omits_years_without_any_exact_proxy_support(rules, powertrain):
    inputs = evidence(rules, ratings=[
        ("diesel", 2023, 1), ("bev_300_miles", 2023, 1),
        ("gasoline", 2012, 4), ("gasoline", 2023, 2),
    ], atb=[("cars", "bev_150_miles", 2023, 1)])
    observations = inputs.period_observations(
        "ON", "cars", powertrain, [2010, 2011, 2012], allow_analogue_when_empty=True,
    )
    assert set(observations) == {2012}
    assert observations[2012]["native_intensity"] == 2
    assert inputs.period_observations(
        "ON", "cars", powertrain, [2010, 2011], allow_analogue_when_empty=True,
    ) == {}
    with pytest.raises(ValueError, match="No approved rating index"):
        inputs.historical_rating_ratio("cars", powertrain, 2010, 2023)


@pytest.mark.parametrize("family,ranges", [
    ("bev", ["bev_150_miles", "bev_200_miles", "bev_300_miles", "bev_400_miles"]),
    ("phev", ["phev_35_miles", "phev_50_miles"]),
])
def test_pev_shared_observed_anchor_indexes_consumption_only(rules, family, ranges):
    anchor = rules["pev_historical_anchors"][family]
    inputs = evidence(rules, ratings=[(anchor, 2010, 9), (anchor, 2023, 6)], atb=[
        ("cars", powertrain, year, (rank + 1) * factor)
        for rank, powertrain in enumerate(ranges) for year, factor in [(2023, 1), (2030, .8)]
    ])
    for rank, powertrain in enumerate(ranges):
        row = inputs.derive_consumption("ON", "cars", powertrain, 2010)
        assert row["native_intensity"] == pytest.approx((rank + 1) * 1.5)
        assert row["historical_index_anchor"] == row["analogue"] == anchor
        assert row["native_unit"] == "MJ/vkm"
        assert row["load_factor"] == 6
        assert "efficiency" not in row
        assert inputs.derive_consumption("ON", "cars", powertrain, 2011) is None
        assert inputs.derive_consumption("ON", "cars", powertrain, 2023)["native_intensity"] == rank + 1
        assert inputs.derive_consumption("ON", "cars", powertrain, 2030)["native_intensity"] == pytest.approx((rank + 1) * .8)
    with pytest.raises(ValueError, match="Missing exact ATB"):
        inputs.atb_value("ON", "cars", ranges[0], 2027)


def test_future_diesel_baseline_uses_latest_own_consumption_and_exact_proxy_ratio(rules):
    inputs = evidence(rules, ratings=[
        ("gasoline", 2010, 4), ("gasoline", 2023, 3), ("diesel", 2010, 2), ("diesel", 2026, .01),
    ], atb=[("cars", "diesel", 2023, 10), ("cars", "diesel", 2030, 8)])
    assert inputs.ldv_baseline("ON", "cars", "diesel") == (1.5, "gasoline")
    result = inputs.derive_consumption("ON", "cars", "diesel", 2030)
    assert result["native_intensity"] == pytest.approx(1.2)
    assert result["load_factor"] == 10


def test_mhdv_and_bus_history_remain_consumption_with_separate_loads(rules):
    truck, bus = (rules["road_classes"][mode] for mode in ["medium_trucks", "urban_transit"])
    ceud = pd.DataFrame([
        {"region": "ON", "table_id": table, "raw_series": series, "unit": unit, "year": year, "value": value}
        for table, series, unit, values in [
            (truck["stock_table"], truck["cs_prefix"] + "|Diesel Fuel Oil", "L/100 km", [40, 20]),
            (bus["intensity_table"], bus["intensity"], "MJ/Pkm", [2, 1]),
        ] for year, value in zip([2000, 2023], values, strict=True)
    ])
    inputs = evidence(rules, modes=("medium_trucks", "urban_transit"), ceud=ceud,
                      atb=[(mode, "diesel", 2023, 8) for mode in ["medium_trucks", "urban_transit"]])
    result = inputs.derive_consumption("ON", "medium_trucks", "diesel", 2000)
    assert result["native_intensity"] == 16
    assert result["load_factor"] == 5
    result = inputs.derive_consumption("ON", "urban_transit", "diesel", 2000)
    assert result["native_intensity"] == 8
    assert result["load_factor"] == 5
    assert result["native_unit"] == "MJ/vkm"


def test_motorcycle_service_intensity_is_converted_to_vehicle_consumption(rules):
    spec = rules["road_classes"]["motorcycles"]
    ceud = pd.DataFrame([
        {"region": "ON", "table_id": spec["intensity_table"], "raw_series": spec["intensity"],
         "unit": "MJ/Pkm", "year": year, "value": value}
        for year, value in [(2000, 2), (2023, 1)]
    ])
    gcam = pd.DataFrame([
        {"source_variable": "intensity", "source_unit": "MJ/vkm", "source_technology": tech,
         "source_year": year, "source_value": value}
        for tech, year, value in [("Liquids", 2020, 4), ("Liquids", 2025, 2), ("BEV", 2030, 1)]
    ])
    inputs = evidence(rules, modes=("motorcycles",), ceud=ceud, gcam=gcam)
    history = inputs.derive_consumption("ON", "motorcycles", "gasoline", 2000)
    future = inputs.derive_consumption("ON", "motorcycles", "bev", 2030)
    assert history["native_intensity"] == 10
    assert future["native_intensity"] == 2.5
    assert history["native_unit"] == future["native_unit"] == "MJ/vkm"
    assert future["index_reference_year"] == 2020
    assert future["index_source_year"] == 2030


def test_intercity_source_fuel_economy_is_converted_at_the_held_source_points(rules):
    spec = rules["road_classes"]["inter_city_buses"]
    ceud = pd.DataFrame([
        {"region": "ON", "table_id": spec["intensity_table"], "raw_series": spec["intensity"],
         "unit": "MJ/Pkm", "year": year, "value": value}
        for year, value in [(2000, 2), (2023, 1)]
    ])
    regen = pd.DataFrame([
        {"source_technology_key": tech, "metric": "efficiency", "source_year": year, "source_value": value}
        for tech, year, value in [("icev-dsl", 2020, 4), ("icev-dsl", 2025, 8),
                                 ("bev", 2020, 20), ("bev", 2025, 50)]
    ])
    inputs = evidence(rules, modes=("inter_city_buses",), ceud=ceud, regen=regen,
                      atb=[("urban_transit", "diesel", 2023, 8)])
    result = inputs.derive_consumption("ON", "inter_city_buses", "bev", 2000)
    assert result["native_intensity"] == pytest.approx(1.6)
    assert result["relative_source_year"] == result["relative_reference_year"] == 2020
    assert result["source_point_selection"] == "previous_observation_clamped"


def test_offroad_ratios_and_growth_adjust_consumption_before_final_reciprocal(rules):
    national = pd.DataFrame({"table_id": [28, 28], "raw_series": ["marine"] * 2,
                            "unit": ["MJ/Tkm"] * 2, "year": [2000, 2023], "value": [.4, .2]})
    manual = pd.DataFrame({"category": ["freight_marine"] * 2, "sub_category": ["hfo", "all"],
                           "parameter": ["mdo_relative_energy_intensity", "annual_improvement_rate"],
                           "period": ["all"] * 2, "value": [1.25, .01]})
    selectors = {"freight_marine": {"national_table": 28, "intensity_series": "marine", "units": "bn tonne-km"}}
    result = derive_offroad_consumption(national, manual, mode="freight_marine", powertrain="hfo",
        year=2030, base_year=2023, selectors=selectors, rules=rules["offroad"])
    assert result["native_intensity"] == pytest.approx(.2 * 1.25 * 1.01**-7)
    assert result["load_factor"] == 1
    assert result["ceud_native_intensity"] == .2
    assert result["relative_consumption_multiplier"] == 1.25
    assert "efficiency" not in result


@pytest.mark.parametrize("requested,expected", [(2030, .5), (2040, .5), (2050, .25), (2060, .25)])
def test_manual_efficiency_ratios_use_held_consumption_endpoints(rules, requested, expected):
    rows = pd.DataFrame({"period": ["through_2035", "through_2050"], "value": [2., 4.]})
    assert manual_ratio(rows, requested, rules=rules["offroad"], interpretation="efficiency_ratio") == expected


def test_manual_audit_identifies_only_the_endpoint_actually_used(rules):
    national = pd.DataFrame({"table_id": [25], "raw_series": ["rail"],
                            "unit": ["MJ/Pkm"], "year": [2023], "value": [2.]})
    manual = pd.DataFrame({"category": ["passenger_rail"] * 3, "sub_category": ["h2"] * 3,
                           "parameter": ["diesel_relative_energy_intensity"] * 2 + ["annual_improvement_rate"],
                           "period": ["through_2035", "through_2050", "all"], "value": [2., 4., .01]})
    selectors = {"passenger_rail": {"national_table": 25, "intensity_series": "rail", "units": "bn passenger-km"}}
    result = derive_offroad_consumption(national, manual, mode="passenger_rail", powertrain="h2",
        year=2040, base_year=2023, selectors=selectors, rules=rules["offroad"])
    assert result["native_intensity"] == pytest.approx(1.01**-17)
    assert result["manual_source_year"] == 2035
    assert result["manual_rows"] == "0|2"
