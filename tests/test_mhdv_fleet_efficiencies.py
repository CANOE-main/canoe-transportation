"""Truck fleet consumption and cohort anchors keep ATB on a relative basis."""

from copy import deepcopy

import pandas as pd
import pytest

from parameterization.road_efficiencies import RoadEfficiencyEvidence
from parameterization.build_efficiencies import efficiency_components
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules


@pytest.fixture
def truck():
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml")
    rules = deepcopy(load_harmonization_rules(bundle, "efficiencies"))
    conversion = load_conversion_factors(bundle)
    mode = "medium_trucks"
    spec = rules["road_classes"][mode]
    ceud = pd.DataFrame([
        {"region": "ON", "table_id": spec["stock_table"], "raw_series": spec["cs_prefix"] + "|" + rules["fuel_series"][fuel],
         "unit": "L/100 km", "year": year, "value": value}
        for fuel, values in {"gasoline": [40, 30, 20], "diesel": [30, 20, 10]}.items()
        for year, value in zip([2021, 2022, 2023], values)
    ])
    loads = pd.DataFrame([{"region": "ON", "mode": mode, "year": year, "load_factor": load}
                          for year, load in [(2021, 1), (2022, 2), (2023, 4)]])
    atb = pd.DataFrame([
        {"region": "ON", "mode": mode, "powertrain": fuel, "year": year, "weighted_consumption": value}
        for fuel, values in {"diesel": [200, 100], "gasoline": [300, 20], "bev": [50, 20], "fcev": [80, 24]}.items()
        for year, value in zip([2025, 2030], values)
    ])
    stock = pd.DataFrame([
        {"region": "ON", "road_class": mode, "powertrain": fuel, "vintage_year": year, "raw_capacity": count}
        for fuel, counts in {"gasoline": [1, 2, 7], "diesel": [7, 2, 1], "bev": [1, 3, 6]}.items()
        for year, count in zip([2021, 2022, 2023], counts)
    ])
    return RoadEfficiencyEvidence(
        ceud=ceud, loads=loads, rating_audit=pd.DataFrame(columns=["year", "native_class_mean", "weight", "class_mean_mj_per_vkm", "ceud_class", "powertrain"]),
        atb_audit=atb, gcam=pd.DataFrame(), regen=pd.DataFrame(), base_year=2023, rules=rules,
        conversions=conversion, projection_years=(2021, 2022, 2023), projection_reference_year=2025,
        cohort_stock=stock,
    )


@pytest.mark.parametrize("fuel,litres", [("gasoline", 30), ("diesel", 20)])
def test_incumbent_history_uses_own_physical_fuel_volume(truck, fuel, litres):
    result = truck.derive_consumption("ON", "medium_trucks", fuel, 2022)
    assert result["native_intensity"] == pytest.approx(litres * truck.conversions["fuel_hhv"][fuel + "_mj_per_litre"] / 100)
    assert result["treatment"] == "CEUD_observed_fleet_consumption"
    assert result["source_fuel"] == fuel
    assert result["load_factor"] == 2


def test_existing_emerging_uses_same_year_diesel_and_reference_year_relative_level(truck):
    diesel = truck.derive_consumption("ON", "medium_trucks", "diesel", 2022)
    electric = truck.derive_consumption("ON", "medium_trucks", "bev", 2022)
    assert electric["native_intensity"] == pytest.approx(diesel["native_intensity"] * 50 / 200)
    assert electric["treatment"] == "CEUD_diesel_fleet_ATB_relative_powertrain"
    assert electric["consumption_index"] == .25
    assert electric["source_fuel"] == "diesel"
    assert electric["projection_reference_year"] == 2025


@pytest.mark.parametrize("fuel,expected_litres,rate", [("gasoline", 24, .5), ("diesel", 26, .5), ("bev", 15 * .25, .4)])
def test_latest_existing_period_connects_exactly_to_future_reference(truck, fuel, expected_litres, rate):
    anchor = truck.projection_anchor("ON", "medium_trucks", fuel)
    hhv = truck.conversions["fuel_hhv"][(fuel if fuel in {"gasoline", "diesel"} else "diesel") + "_mj_per_litre"]
    assert anchor["native_intensity"] == pytest.approx(expected_litres * hhv / 100)
    assert anchor["stock_powertrain"] == fuel and not anchor["stock_proxy"]
    first = truck.derive_consumption("ON", "medium_trucks", fuel, 2025)
    future = truck.derive_consumption("ON", "medium_trucks", fuel, 2030)
    assert first["native_intensity"] == anchor["native_intensity"]
    assert future["native_intensity"] == pytest.approx(anchor["native_intensity"] * rate)
    assert future["load_factor"] == first["load_factor"] == anchor["load_factor"]
    assert future["treatment"] == "CEUD_cohort_anchor_ATB_relative_projection"
    assert first["projection_source_years"] == [2021, 2022, 2023]


def test_future_without_own_existing_stock_uses_explicit_diesel_cohort_proxy(truck):
    anchor = truck.projection_anchor("ON", "medium_trucks", "fcev")
    diesel = truck.projection_anchor("ON", "medium_trucks", "diesel")
    assert anchor["stock_proxy"] and anchor["stock_powertrain"] == "diesel"
    assert anchor["weights"] == diesel["weights"]
    assert anchor["native_intensity"] == pytest.approx(diesel["native_intensity"] * 80 / 200)
    future = truck.derive_consumption("ON", "medium_trucks", "fcev", 2030)
    assert future["native_intensity"] == pytest.approx(diesel["native_intensity"] * 24 / 200)
    assert future["projection_stock_proxy"]


def test_absolute_atb_rescaling_cannot_change_truck_parameters(truck):
    expected = {(fuel, year): truck.derive_consumption("ON", "medium_trucks", fuel, year)["native_intensity"]
                for fuel in ["diesel", "gasoline", "bev", "fcev"] for year in [2022, 2025, 2030]}
    truck.atb["weighted_consumption"] *= 10
    truck.atb_value.cache_clear()
    truck.projection_anchor.cache_clear()
    truck.derive_consumption.cache_clear()
    for (fuel, year), value in expected.items():
        assert truck.derive_consumption("ON", "medium_trucks", fuel, year)["native_intensity"] == pytest.approx(value)


def test_gasoline_diesel_rank_is_not_artificially_forced(truck):
    gas = truck.derive_consumption("ON", "medium_trucks", "gasoline", 2023)
    diesel = truck.derive_consumption("ON", "medium_trucks", "diesel", 2023)
    assert gas["native_intensity"] / diesel["native_intensity"] == pytest.approx(
        2 * truck.conversions["fuel_hhv"]["gasoline_mj_per_litre"] / truck.conversions["fuel_hhv"]["diesel_mj_per_litre"])


def test_no_registered_positive_anchor_stock_fails_instead_of_using_atb_level(truck):
    truck.cohort_stock["raw_capacity"] = 0
    with pytest.raises(ValueError, match="No positive own or configured-analogue cohort stock"):
        truck.derive_consumption("ON", "medium_trucks", "bev", 2030)


@pytest.mark.parametrize("mode,region,aggregation", [
    ("medium_trucks", "ON", ("ontario_ministry_transport_vehicle_population", 4)),
    ("medium_trucks", "QC", ("wards_intelligence_sales_shares", "vehicle_class_market_shares")),
    ("heavy_trucks", "ON", ("statcan_transport_tables", "23-10-0142-01")),
])
def test_truck_lineage_matches_actual_fleet_and_relative_inputs(mode, region, aggregation):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    rules = load_harmonization_rules(bundle, "efficiencies")
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    offroad = load_harmonization_rules(bundle, "offroad_stocks_and_demands")["demand"]
    arguments = dict(region=region, powertrain="diesel", result={}, manual=pd.DataFrame(), rating_inputs=None, baseline_inputs=None)
    history = efficiency_components(bundle, mode, rules, road, offroad, year=2023, **arguments)
    future = efficiency_components(bundle, mode, rules, road, offroad, year=2030, **arguments)
    assert ("argonne_rd_greet", "fuel_heating_values") in history
    assert aggregation not in history and aggregation in future
    assert not any(source == "nlr_atb_transportation" for source, _ in history)
    assert ("nlr_atb_transportation", "vehicles") in future
