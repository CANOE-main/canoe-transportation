"""Efficiency contracts: source-native audit, inverse indexing, schema and coupling."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization import build_efficiencies as layer
from parameterization.ldv_ev_ranges import module_rules as range_module_rules
from parameterization.road_fleet_weights import medium_vocation_weights, report4_gvwr_shares
from parameterization.offroad_efficiencies import (
    derive_offroad_efficiency,
    manual_ratio,
)
from parameterization.road_efficiencies import (
    RoadEfficiencyEvidence,
    aggregate_atb,
    aggregate_ratings,
    backcast_efficiency,
    classify_ratings,
    derive_load_factors,
    interpolate,
    prepare_bus_annual_efficiency_evidence,
    select_atb_consumption,
)
from utils import (
    file_sha256,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.rating_stock_benchmark import aggregate_stock_weighted_ratings


@pytest.fixture
def bundle(legacy_bundle):
    return legacy_bundle


@pytest.fixture
def rules(bundle):
    return load_harmonization_rules(bundle, "efficiencies")


def test_greet_hhv_and_dimensional_conversions(bundle):
    c = load_conversion_factors(bundle)
    for fuel in ("gasoline", "diesel"):
        si = (
            c["fuel_hhv"][fuel + "_btu_it_per_us_gallon"]
            * c["energy"]["btu_it_to_joule"]
            * c["energy"]["joule_to_mj"]
            / c["volume"]["us_gallon_to_litre"]
        )
        assert si == pytest.approx(c["fuel_hhv"][fuel + "_mj_per_litre"], rel=1e-14)
    assert c["energy"]["kwh_to_mj"] * c["energy"]["wh_to_kwh"] == pytest.approx(
        c["energy"]["wh_to_mj"]
    )
    factor = c["derived"][
        "us_gallon_equivalent_per_mile_to_litre_equivalent_per_100_km"
    ]
    assert factor == pytest.approx(
        c["volume"]["us_gallon_to_litre"]
        / c["length"]["mile_to_km"]
        / c["service"]["per_100_km_to_per_km"]
    )
    assert 1 / ((factor / 50) / factor) == pytest.approx(50)


def test_consumption_backcast_direction_and_interpolation():
    assert backcast_efficiency(2, 20, 30) == pytest.approx(4 / 3)
    series = pd.DataFrame({"year": [2020, 2030], "value": [30, 20]})
    assert interpolate(series, 2023, "value") == 27
    with pytest.raises(ValueError, match="extrapolation"):
        interpolate(series, 2019, "value")
    with pytest.raises(ValueError, match="ambiguous"):
        interpolate(pd.concat([series, series]), 2023, "value")


def test_mhdv_and_bus_backcasts_use_consumption_direction_without_double_load(rules):
    medium = rules["road_classes"]["medium_trucks"]
    bus = rules["road_classes"]["urban_transit"]
    ceud = pd.DataFrame(
        [
            {
                "region": "ON",
                "table_id": table,
                "raw_series": series,
                "unit": unit,
                "year": year,
                "value": value,
            }
            for table, series, unit, values in [
                (
                    medium["stock_table"],
                    medium["cs_prefix"] + "|Diesel Fuel Oil",
                    "L/100 km",
                    [40, 20],
                ),
                (bus["intensity_table"], bus["intensity"], "MJ/Pkm", [2, 1]),
            ]
            for year, value in zip([2000, 2023], values, strict=True)
        ]
    )
    loads = pd.DataFrame(
        [
            {"region": "ON", "mode": mode, "year": year, "load_factor": load}
            for mode in ["medium_trucks", "urban_transit"]
            for year, load in [(2000, 5), (2023, 10)]
        ]
    )
    atb = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": mode,
                "powertrain": "diesel",
                "year": year,
                "weighted_consumption": value,
            }
            for mode in ["medium_trucks", "urban_transit"]
            for year, value in [(2020, 8), (2025, 6), (2030, 4)]
        ]
    )
    ratings = pd.DataFrame(
        columns=[
            "native_class_mean",
            "weight",
            "class_mean_mj_per_vkm",
            "ceud_class",
            "powertrain",
            "year",
        ]
    )
    evidence = RoadEfficiencyEvidence(
        ceud=ceud,
        loads=loads,
        rating_audit=ratings,
        atb_audit=atb,
        gcam=pd.DataFrame(),
        regen=pd.DataFrame(),
        base_year=2023,
        rules=rules,
    )
    assert evidence.derive("ON", "medium_trucks", "diesel", 2000)[
        "efficiency"
    ] == pytest.approx(5 / (6.8 * 2))
    assert evidence.derive("ON", "medium_trucks", "diesel", 2030)[
        "efficiency"
    ] == pytest.approx(10 / 4)
    assert evidence.derive("ON", "urban_transit", "diesel", 2000)[
        "efficiency"
    ] == pytest.approx((10 / 6.8) / 2)


def test_ldv_keeps_last_own_rating_and_indexes_without_future_observations(rules):
    ratings = pd.DataFrame(
        [
            {
                "ceud_class": "Car",
                "powertrain": fuel,
                "year": year,
                "native_class_mean": value,
                "class_mean_mj_per_vkm": value,
                "weight": 1,
            }
            for fuel, year, value in [
                ("gasoline", 2019, 4),
                ("gasoline", 2023, 3),
                ("diesel", 2019, 2),
                ("diesel", 2026, 0.01),
            ]
        ]
    )
    atb = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "cars",
                "powertrain": "diesel",
                "year": year,
                "weighted_consumption": value,
            }
            for year, value in [(2023, 10), (2030, 8)]
        ]
    )
    evidence = RoadEfficiencyEvidence(
        ceud=pd.DataFrame(),
        loads=pd.DataFrame(
            {"region": ["ON"], "mode": ["cars"], "year": [2023], "load_factor": [2]}
        ),
        rating_audit=ratings,
        atb_audit=atb,
        gcam=pd.DataFrame(),
        regen=pd.DataFrame(),
        base_year=2023,
        rules=rules,
    )
    assert evidence.rating("cars", "diesel", 2023, "mj") is None
    assert evidence.ldv_baseline("ON", "cars", "diesel") == (1.5, "gasoline")
    assert evidence.derive("ON", "cars", "diesel", 2030)["efficiency"] == pytest.approx(
        2 / (1.5 * 0.8)
    )
    assert any(
        "latest_own_rating_year=2019" in finding[-1] for finding in evidence.findings
    )


def test_load_factor_is_activity_over_vehicle_distance(rules, bundle):
    configured = deepcopy(rules)
    configured["road_classes"] = {"cars": rules["road_classes"]["cars"]}
    activity = {"cars": {"table_id": 20, "raw_series": "activity"}}
    data = pd.DataFrame(
        [
            {
                "region": "ON",
                "year": 2023,
                "table_id": 20,
                "raw_series": "activity",
                "unit": "millions",
                "value": 600,
            },
            {
                "region": "ON",
                "year": 2023,
                "table_id": 21,
                "raw_series": configured["road_classes"]["cars"]["stock"],
                "unit": "thousands",
                "value": 20,
            },
            {
                "region": "ON",
                "year": 2023,
                "table_id": 21,
                "raw_series": configured["road_classes"]["cars"]["distance"],
                "unit": "km",
                "value": 15000,
            },
        ]
    )
    result = derive_load_factors(
        data,
        rules=configured,
        activity_rules=activity,
        conversions=load_conversion_factors(bundle),
    )
    assert result.load_factor.iloc[0] == 2
    with pytest.raises(ValueError, match="wrong-unit"):
        derive_load_factors(
            data.assign(unit="PJ"),
            rules=configured,
            activity_rules=activity,
            conversions=load_conversion_factors(bundle),
        )


def test_hybrid_evidence_requires_unambiguous_exact_key(rules, bundle):
    source = pd.DataFrame(
        [
            {
                "source_row": i,
                "component": "conventional",
                "Model year": 2023,
                "Make": "Example",
                "Model": model,
                "Vehicle class": "Compact",
                "Fuel type": "X",
                "Combined (L/100 km)": 6,
            }
            for i, model in enumerate(
                ["Hidden", "Ambiguous", "Named Hybrid", "Unmatched"], 2
            )
        ]
    )
    epa = pd.DataFrame(
        {
            "year": [2023] * 3,
            "make": ["Example"] * 3,
            "model": ["Hidden", "Ambiguous", "Ambiguous"],
            "atvType": ["Hybrid", "Hybrid", ""],
        }
    )
    result = classify_ratings(
        [source], epa, rules=rules, conversions=load_conversion_factors(bundle), range_rules=range_module_rules(bundle)
    ).set_index("Model")
    assert result.loc["Hidden", "powertrain"] == "hev"
    assert result.loc["Ambiguous", "powertrain"] == "gasoline"
    assert result.loc["Ambiguous", "ambiguous_epa_match"]
    assert result.loc["Named Hybrid", "powertrain"] == "hev"
    assert result.loc["Unmatched", "powertrain"] == "gasoline"


@pytest.mark.parametrize("index_mode,expected,unit", [("CD", 1.8, "Le/100 km (CD; index only)"), ("CS", 5.5, "L/100 km (CS; index only)")])
def test_phev_rating_keeps_both_components_and_selects_recorded_index(rules, bundle, index_mode, expected, unit):
    rules["ratings"]["phev_historical_index"] = f"combined_{index_mode.lower()}_consumption"
    source = pd.DataFrame(
        [
            {
                "source_row": 2,
                "component": "phev",
                "Model year": 2023,
                "Vehicle class": "Compact",
                "Range 1 (km)": 50,
                "Combined (L/100 km)": 5.5,
                "Combined Le/100 km": "1.8 ([15.8 kWh + 0.0 L]/100 km)",
            }
        ]
    )
    epa = pd.DataFrame(columns=rules["ratings"]["evidence_columns"])
    result = classify_ratings(
        [source],
        epa,
        rules=rules,
        conversions=load_conversion_factors(bundle),
        range_rules=range_module_rules(bundle),
        phev_contract=load_harmonization_rules(bundle, "nlr_atb_autonomie")[
            "components"
        ]["phev_efficiency"]["future_nrcan_phev_contract"],
    ).iloc[0]
    assert result.combined_cd_consumption_Le_100_km == 1.8
    assert result.combined_cs_consumption_L_100_km == 5.5
    assert "15.8 kWh" in result.combined_cd_consumption_Le_100_km_raw
    assert pd.isna(result.consumption_mj_per_vkm)
    assert result.native_consumption == expected
    assert result.native_unit == unit
    assert result.classification_method == f"omega_range_bucket_{index_mode.lower()}_index_only"


def test_phev_cd_switch_changes_only_historical_index_preserving_atb_level_and_gaps(rules, bundle):
    source = pd.DataFrame([
        {"source_row": row, "component": "phev", "Model year": year, "Vehicle class": "Compact",
         "Range 1 (km)": 50, "Combined (L/100 km)": cs, "Combined Le/100 km": cd}
        for row, year, cs, cd in [(2, 2015, 8, "1.0 ([8.0 kWh + 0.1 L]/100 km)"),
                                  (3, 2023, 4, "2.0 (17.8 kWh/100 km)"), (4, 2026, 100, "100.0")]
    ])
    weights = pd.DataFrame([{"report_year": 2025, "weight_basis": "all_vintages", "nrcan_ceud_class": "Car",
                             "nrcan_vehicle_class": "Compact", "aggregation_weight": 1}])
    atb = pd.DataFrame([{"region": "ON", "mode": "cars", "powertrain": "phev_35_miles", "year": year,
                         "weighted_consumption": value} for year, value in [(2023, 2), (2030, 1.5)]])
    loads = pd.DataFrame([{"region": "ON", "mode": "cars", "year": year, "load_factor": 2} for year in [2000, 2023]])
    values = {}
    for index_mode in ["CD", "CS"]:
        selected_rules = deepcopy(rules)
        selected_rules["ratings"]["phev_historical_index"] = f"combined_{index_mode.lower()}_consumption"
        classified = classify_ratings([source], pd.DataFrame(columns=rules["ratings"]["evidence_columns"]),
            rules=selected_rules, conversions=load_conversion_factors(bundle), range_rules=range_module_rules(bundle),
            phev_contract=load_harmonization_rules(bundle, "nlr_atb_autonomie")["components"]["phev_efficiency"]["future_nrcan_phev_contract"])
        analogue = pd.DataFrame([{"source_row": year, "year": year, "powertrain": "hev", "rating_class": "Compact",
                                  "native_consumption": value, "native_unit": "L/100 km", "consumption_mj_per_vkm": value,
                                  "included": True} for year, value in [(2000, 12), (2023, 6)]])
        audit = aggregate_ratings(pd.concat([classified, analogue], ignore_index=True), weights,
                                  rules=selected_rules, weight_basis="all_vintages")
        evidence = RoadEfficiencyEvidence(ceud=pd.DataFrame(), loads=loads, rating_audit=audit, atb_audit=atb,
            gcam=pd.DataFrame(), regen=pd.DataFrame(), base_year=2023, rules=selected_rules)
        values[index_mode] = {year: evidence.derive("ON", "cars", "phev_35_miles", year) for year in [2000, 2015, 2019, 2023, 2030]}
        assert evidence.ratings.year.max() == 2023
        assert values[index_mode][2000]["native_intensity"] == 4
        assert values[index_mode][2000]["analogue"] == "hev"
        assert values[index_mode][2023]["native_intensity"] == 2
        assert values[index_mode][2030]["native_intensity"] == 1.5
        assert values[index_mode][2015]["treatment"] == f"ATB_PHEV_level_NRCan_{index_mode}_or_analogue_history"
    assert values["CD"][2015]["native_intensity"] == 1
    assert values["CS"][2015]["native_intensity"] == 4
    assert values["CD"][2019]["native_intensity"] == 1.5
    assert values["CS"][2019]["native_intensity"] == 3
    assert values["CD"][2015]["efficiency"] == 2
    assert values["CS"][2015]["efficiency"] == .5


def test_nrcan_bev_range_mapping_keeps_all_four_omega_buckets(rules, bundle):
    conversions = load_conversion_factors(bundle)
    source = pd.DataFrame([
        {"source_row": index, "component": "bev", "Model year": 2023,
         "Vehicle class": "Compact", "Range (km)": miles * conversions["length"]["mile_to_km"],
         "Combined (kWh/100 km)": 15}
        for index, miles in enumerate((140, 200, 300, 400), 2)
    ])
    result = classify_ratings(
        [source], pd.DataFrame(columns=rules["ratings"]["evidence_columns"]),
        rules=rules, conversions=conversions, range_rules=range_module_rules(bundle),
    )
    assert list(result.powertrain) == ["bev_150_miles", "bev_200_miles", "bev_300_miles", "bev_400_miles"]
    assert len(set(result.legacy_range_label)) == 3
    assert list(result.range_km) == list(source["Range (km)"])
    assert result.consumption_mj_per_vkm.eq(0.54).all()


def test_unsplit_suv_sums_weights_and_renormalizes_observed(rules):
    classified = pd.DataFrame(
        [
            {
                "year": 2010,
                "powertrain": "gasoline",
                "rating_class": "Sport utility vehicle",
                "native_consumption": 12,
                "consumption_mj_per_vkm": 4,
                "native_unit": "L/100 km",
                "source_row": 2,
                "included": True,
            }
        ]
    )
    weights = pd.DataFrame(
        [
            {
                "report_year": 2025,
                "weight_basis": "all_vintages",
                "nrcan_ceud_class": "Light Truck",
                "nrcan_vehicle_class": name,
                "aggregation_weight": weight,
            }
            for name, weight in [
                ("Sport utility vehicle: Small", 0.4),
                ("Sport utility vehicle: Standard", 0.3),
                ("Pickup truck: Standard", 0.3),
            ]
        ]
    )
    result = aggregate_ratings(classified, weights, rules=rules, weight_basis="all_vintages")
    assert result.original_weight.iloc[0] == pytest.approx(0.7)
    assert result.weight.iloc[0] == 1
    assert result.native_class_mean.iloc[0] == 12


def test_rating_stock_weights_replace_pooled_class_shares_and_expose_whole_cohort_fallback(bundle):
    rules = load_harmonization_rules(bundle, "rating_stock_benchmark")
    catalogue = pd.DataFrame([
        {
            "ceud_class": "Car", "year": year, "powertrain": powertrain,
            "rating_class": rating_class, "native_class_mean": value,
            "class_mean_mj_per_vkm": value * 0.036, "native_unit": "kWh/100 km",
            "observations": 1, "original_weight": share, "observed_weight_sum": 1,
            "weight": share, "range_class_mean_km": 300, "matched_stock": 0,
            "cohort_matched_stock": 0, "aggregation_method": "catalogue_class_weighted",
        }
        for year, powertrain in [(2023, "bev_200_miles"), (2022, "bev_200_miles")]
        for rating_class, value, share in [("Compact", 10, 0.2), ("Mid-size", 20, 0.8)]
    ])
    allocations = pd.DataFrame([
        {
            "year": 2023, "powertrain": "bev_200_miles", "rating_class": size,
            "native_consumption": value, "consumption_mj_per_vkm": value * 0.036,
            "native_unit": "kWh/100 km", "range_km": range_km,
            "allocated_fit_active_stock": stock, "mto_model_year": 2023,
            "mto_report_year": 2025, "mto_nrcan_ceud_class": "Car",
            "rating_variant_id": size, "mto_stock_row_id": index,
        }
        for index, (size, value, stock, range_km) in enumerate([
            ("Compact", 10, 90, 280), ("Mid-size", 20, 10, 380),
        ])
    ])
    result = aggregate_stock_weighted_ratings(allocations, catalogue, rules=rules)
    weighted = result.loc[result.year.eq(2023)]
    assert (weighted.native_class_mean * weighted.weight).sum() == pytest.approx(11)
    assert (weighted.range_class_mean_km * weighted.weight).sum() == pytest.approx(290)
    assert weighted.matched_stock.sum() == 100
    assert set(weighted.cohort_matched_stock) == {100}
    assert set(weighted.aggregation_method) == {"mto_stock_weighted"}
    fallback = result.loc[result.year.eq(2022)]
    assert (fallback.native_class_mean * fallback.weight).sum() == pytest.approx(18)
    assert set(fallback.aggregation_method) == {"catalogue_fallback_no_resolved_stock"}
    # A resolved class does not acquire a catalogue proxy count for absent classes.
    partial = aggregate_stock_weighted_ratings(allocations.iloc[:1], catalogue, rules=rules)
    assert partial.loc[partial.year.eq(2023), "rating_class"].tolist() == ["Compact"]
    assert partial.loc[partial.year.eq(2023), "weight"].item() == 1
    allocations.loc[0, "allocated_fit_active_stock"] = -1
    with pytest.raises(ValueError, match="Invalid allocated"):
        aggregate_stock_weighted_ratings(allocations, catalogue, rules=rules)


def test_catalogue_rating_lineage_includes_all_observed_rows_without_mto_powertrain_catalogue(rules):
    classified = pd.DataFrame([
        {
            "year": 2023, "powertrain": "bev_200_miles", "rating_class": "Compact",
            "included": True, "component": component, "source_row": row,
            "native_unit": "kWh/100 km", "classification_method": "omega_range_bucket",
        }
        for row, component in [(1, "battery_electric_first"), (2, "battery_electric_second")]
    ])
    annual = pd.DataFrame([{
        "ceud_class": "Car", "year": 2023, "powertrain": "bev_200_miles",
        "rating_class": "Compact", "aggregation_method": "catalogue_class_weighted",
    }])
    resolver, _ = layer.ldv_rating_input_resolvers(
        SimpleNamespace(base_year=2023), classified, annual, rules=rules,
    )
    assert set(resolver("cars", "bev_200_miles", 2023, "mj")) == {
        ("nrcan_fuel_consumption_ratings", "battery_electric_first"),
        ("nrcan_fuel_consumption_ratings", "battery_electric_second"),
    }


def test_vocational_consumption_average_is_harmonic_fuel_economy(bundle):
    r = deepcopy(load_harmonization_rules(bundle, "road_aggregation")["medium_trucks"])
    r["report4_gvwr"] = {"MDV3": 3, "MDV6": 6}
    report = pd.DataFrame({"EPA_GVWR": ["MDV3", "MDV6"], "NATIVE_COUNT": [1, 3]})
    classes = [
        "Class 3 Medium Van",
        "Class 3 Medium Pickup",
        "Class 3 Medium School",
        "Class 6 Box",
    ]
    weights = medium_vocation_weights(report4_gvwr_shares(report, rules=r), classes, rules=r)
    assert weights == {
        "Class 3 Medium Van": 0.125,
        "Class 3 Medium Pickup": 0.125,
        "Class 6 Box": 0.75,
    }
    atb = pd.DataFrame(
        [
            {
                "powertrain": "diesel",
                "year": 2030,
                "vehicle_class": label,
                "consumption_mj_per_vkm": 1 / mpg,
            }
            for label, mpg in zip(weights, [10, 20, 5], strict=True)
        ]
    )
    result = aggregate_atb(atb, weights, tolerance=1e-9)
    assert 1 / result.weighted_consumption.sum() == pytest.approx(
        1 / (0.125 / 10 + 0.125 / 20 + 0.75 / 5)
    )
    with pytest.raises(ValueError, match="coverage"):
        aggregate_atb(atb.iloc[:2], weights, tolerance=1e-9)


@pytest.mark.parametrize(
    "basis,category,powertrain,detail",
    [
        ("gge", "Light Duty", "phev_35_miles", "LDV PHEV"),
        (
            "dge",
            "Medium/Heavy Duty",
            "phev",
            "Diesel Series Plug-in Hybrid Electric Vehicle",
        ),
    ],
)
def test_phev_uses_derived_source_inputs_and_hhv_not_output_fe(
    bundle, rules, basis, category, powertrain, detail
):
    c = load_conversion_factors(bundle)
    r = deepcopy(rules)
    r["ldv_archetypes"] = {powertrain: {"range_mi": 35}} if basis == "gge" else {}
    r["mhdv_archetypes"] = {powertrain: {"detail": detail}} if basis == "dge" else {}
    h = c["energy"]["wh_per_" + basis]
    fuel, electric = 0.01, 120
    total = fuel * h + electric
    source = pd.DataFrame(
        [
            {
                "year": 2030,
                "trajectory": "Mid",
                "vehicle_weight_category": category,
                "vehicle_class": "Example",
                "vehicle_detail": detail,
                "electric_range_mi": 35,
                "fuel_equivalent_basis": basis,
                "total_utility_weighted_energy_wh_equivalent_per_mi": total,
                "electricity_input_share": electric / total,
                "liquid_fuel_input_share": fuel * h / total,
                "utility_weighted_fuel_consumption_gallon_equivalent_per_mi": fuel,
                "utility_weighted_electricity_consumption_wh_per_mi": electric,
            }
        ]
    )
    output = pd.DataFrame(
        [{"value": 99999}]
    )  # Not even a usable authoritative PHEV schema.
    result = select_atb_consumption(
        output, source, trajectory="Mid", rules=r, conversions=c
    ).iloc[0]
    physical_fuel = r["atb"]["equivalent_gallon_hhv_fuel"][basis]
    fuel_mj = (
        fuel
        * c["fuel_hhv"][physical_fuel + "_mj_per_litre"]
        * c["volume"]["us_gallon_to_litre"]
    )
    electric_mj = electric * c["energy"]["wh_to_mj"]
    assert result.basis == basis
    assert result.consumption_mj_per_vkm == pytest.approx(
        (fuel_mj + electric_mj) / c["length"]["mile_to_km"]
    )
    assert result.electricity_input_share == pytest.approx(
        electric_mj / (electric_mj + fuel_mj)
    )
    assert result.source_electricity_input_share == pytest.approx(electric / total)
    assert result.electricity_input_share != pytest.approx(
        result.source_electricity_input_share
    )


def test_offroad_hfo_consumption_ratio_is_reciprocal(rules):
    national = pd.DataFrame(
        {
            "table_id": [28, 28],
            "raw_series": ["marine"] * 2,
            "unit": ["MJ/Tkm"] * 2,
            "year": [2000, 2023],
            "value": [0.4, 0.2],
        }
    )
    manual = pd.DataFrame(
        {
            "category": ["freight_marine"] * 2,
            "sub_category": ["hfo", "all"],
            "parameter": ["mdo_relative_energy_intensity", "annual_improvement_rate"],
            "period": ["all"] * 2,
            "value": [1.25, 0.01],
        }
    )
    selectors = {
        "freight_marine": {
            "national_table": 28,
            "intensity_series": "marine",
            "units": "bn tonne-km",
        }
    }

    def derive(year):
        return derive_offroad_efficiency(
            national,
            manual,
            mode="freight_marine",
            powertrain="hfo",
            year=year,
            base_year=2023,
            selectors=selectors,
            rules=rules["offroad"],
        )

    assert derive(2000)["efficiency"] == 2
    assert derive(2030)["efficiency"] == pytest.approx(4 * 1.01**7)
    ratios = pd.DataFrame(
        {"period": ["through_2035", "through_2050"], "value": [1.5, 1.8]}
    )
    assert manual_ratio(ratios, 2040, rules=rules["offroad"]) == pytest.approx(1.6)


def test_template_edges_periods_and_default_exclusions(bundle, rules):
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"][
        "activity_series"
    ]
    offroad = load_harmonization_rules(bundle, "offroad_stocks_and_demands")["demand"][
        "energy_intensity_series"
    ]
    technology = pd.read_csv(
        bundle.repo_root / "inputs/0_canoe_template/technology.csv"
    )
    commodities = pd.read_csv(
        bundle.repo_root / "inputs/0_canoe_template/commodity.csv"
    )
    edges = layer.technology_relationships(
        technology, commodities, rules=rules, road_outputs=road, offroad_outputs=offroad
    ).set_index("tech")
    assert edges.loc["T_LDV_C_GSL_N", "input_comm"] == "T_gsl"
    assert edges.loc["T_LDV_C_GSL_PHEV50_N", "input_comm"] == "T_gsl_elc_phev50"
    assert edges.loc["T_HDV_T_FCEV_N", "output_comm"] == "T_D_tkm_hdv_t"
    assert set(edges.loc[edges.kind.eq("unit")].index) == {
        "T_OFF",
        *(v["tech"] for v in rules["phev_blends"].values()),
        *(v["tech"] for v in rules["existing_phev_blends"].values()),
    }
    assert not edges.index.str.contains("CHRG|REFUEL|dummy").any()
    assert rules["ldv_archetypes"]["fcev"]["analogue"] == "gasoline"
    assert bundle.scenario.periods.historical_years(2023) == [2021, 2022, 2023]


@pytest.mark.parametrize("include_bus_capacity", [False, True])
def test_offline_deterministic_preparation_and_caller_owned_insertion(
    bundle, tmp_path, monkeypatch, include_bus_capacity
):
    from build_transport import (
        insert_transport_contribution,
        prepare_transport_contribution,
    )
    from validation.insertion import insert_models
    from validation.schema_contract import create_v4_schema
    from canoe_schema.v4_0 import Region, TimePeriod
    from parameterization import ldv_ev_ranges, road_aggregation

    payload = bundle.scenario.model_dump()
    payload["geography"]["regions"] = ["ON"]
    configured = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    original_path = layer.resolve_artifact_path

    def output_path(b, family, *parts):
        if family.startswith(("efficiencies_", "ldv_range_")):
            return tmp_path / family / Path(*parts)
        return original_path(b, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", output_path)
    monkeypatch.setattr(ldv_ev_ranges, "resolve_artifact_path", output_path)

    def no_powertrain_catalogue(*_args, **_kwargs):
        raise AssertionError("Production efficiency preparation requested diagnostic model-family evidence")

    monkeypatch.setattr(road_aggregation, "load_rating_evidence", no_powertrain_catalogue)

    def no_network(*args, **kwargs):
        raise AssertionError("Offline preparation attempted a network connection")

    monkeypatch.setattr(socket, "create_connection", no_network)
    capacity = [
        SimpleNamespace(region="ON", tech="T_MDV_T_BEV_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_C_GSL_PHEV_EX", vintage=2020, capacity=1),
        SimpleNamespace(region="ON", tech="T_HDV_BT_GSL_EX", vintage=2023, capacity=0),
    ]
    if include_bus_capacity:
        capacity.append(
            SimpleNamespace(
                region="ON", tech="T_HDV_BT_DSL_EX", vintage=2020, capacity=1
            )
        )
    # Plan 046 added bus capacity, retiring plan 045's source-only historical bus rows.
    rules = load_harmonization_rules(configured, "efficiencies")
    assert rules["historical_classes_without_capacity"] == []
    first = layer.prepare_efficiency_rows(configured, existing_capacity_rows=capacity)
    hashes = {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    second = layer.prepare_efficiency_rows(configured, existing_capacity_rows=capacity)
    assert hashes == {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    assert first.audit == second.audit
    assert first.audit["rating_aggregation"] == "catalogue_class_weighted"
    assert "rating_stock_weighting" not in first.audit
    aggregation_rules = load_harmonization_rules(configured, "road_aggregation")
    evidence_names = {Path(item["path"]).name for item in first.audit["input_evidence"]}
    assert aggregation_rules["mapped_current_stock_file"] not in evidence_names
    assert aggregation_rules["fueleconomy_evidence_file"] not in evidence_names
    assert not (tmp_path / "efficiencies_interim/rating_stock_match_audit.csv").exists()
    assert not (tmp_path / "efficiencies_interim/stock_weighted_rating_evidence.csv").exists()
    # Registered-input reference before the diagnostic model-stock experiment.
    expected_ltp_bev = {
        "T_LDV_LTP_BEV150_N": 2.3827052850879324,
        "T_LDV_LTP_BEV200_N": 2.077557141984629,
        "T_LDV_LTP_BEV300_N": 2.1851978965230563,
        "T_LDV_LTP_BEV400_N": 1.9536350082012415,
    }
    assert {
        row.tech: row.efficiency
        for row in first.efficiency_rows
        if row.vintage == 2025 and row.tech in expected_ltp_bev
    } == pytest.approx(expected_ltp_bev, rel=1e-12)
    assert {r.vintage for r in first.efficiency_rows if r.tech == "T_MDV_T_BEV_EX"} == {
        2023
    }
    historical_keys = {
        (row.region, row.tech, row.vintage)
        for row in first.efficiency_rows
        if row.tech.endswith(rules["existing_suffix"]) and row.units != "PJ/PJ"
    }
    assert historical_keys == {
        (row.region, row.tech, row.vintage) for row in capacity if row.capacity > 0
    }
    assert first.audit["historical_source_backed_rows_without_capacity"] == 0
    assert len(first.split_rows) == 70
    annual = pd.read_csv(
        tmp_path / "efficiencies_interim/annual_efficiency_evidence.csv"
    )
    assert set(annual.loc[annual.vintage.eq(2025), "source_year"]) == {2030}
    assert set(annual.loc[annual.tech.eq("T_MDV_T_BEV_EX"), "source_year"]) == {2023}
    weights = first.audit["existing_ev_representatives"]["weights"]
    for powertrain, tech in (("BEV", "T_LDV_C_BEV_EX"), ("PHEV", "T_LDV_C_GSL_PHEV_EX")):
        components = annual.loc[
            annual.tech.eq(tech) & annual.range_representative_component.eq(True)
        ].groupby("powertrain").efficiency.mean().to_dict()
        shares = {entry["bucket"]: entry["share_of_all_sales"] for entry in weights
                  if entry["market_class"] == "car" and entry["powertrain"] == powertrain}
        row = next(row for row in first.efficiency_rows if row.tech == tech)
        assert set(components) == set(shares)
        assert row.efficiency == pytest.approx(1 / sum(shares[key] / value for key, value in components.items()))
        context = next(context for context in first.provenance_contexts if context.data_id == row.data_id)
        assert "epa_omega_baseline" in {contributor.source_key for contributor in context.contributors}
    if include_bus_capacity:
        ceud_rules = load_harmonization_rules(configured, "nrcan_ceud")
        provincial = pd.read_csv(
            resolve_input_path(
                configured,
                "interim",
                ceud_rules["interim_subdir"],
                ceud_rules["region_output_template"].format(region="on"),
            )
        )
        shared_bus, _ = prepare_bus_annual_efficiency_evidence(configured, provincial)
        bus_annual = annual.loc[annual.tech.eq("T_HDV_BT_DSL_EX")]
        assert set(bus_annual.source_year) == {2016, 2017, 2018, 2019, 2020}
        expected_bus = shared_bus.loc[
            shared_bus.tech.eq("T_HDV_BT_DSL_EX")
            & shared_bus.year.isin(bus_annual.source_year)
        ].sort_values("year")
        assert list(bus_annual.sort_values("source_year").efficiency) == pytest.approx(
            list(expected_bus.efficiency),
            abs=1e-12,
        )
        bus_row = next(
            row for row in first.efficiency_rows if row.tech == "T_HDV_BT_DSL_EX"
        )
        assert bus_row.efficiency == pytest.approx(
            expected_bus.efficiency.mean(), abs=1e-12
        )
    assert first.audit["phev_energy_basis"] == "greet_hhv_fuel_plus_electricity"
    with sqlite3.connect(tmp_path / "transport.sqlite") as connection:
        create_v4_schema(connection)
        for model, file in ((Region, "region.csv"), (TimePeriod, "time_period.csv")):
            frame = pd.read_csv(
                bundle.repo_root / "inputs/0_canoe_template" / file
            ).fillna("")
            records = [
                {k: v for k, v in record.items() if v != ""}
                for record in frame.to_dict("records")
            ]
            insert_models(
                connection, [model.model_validate(record) for record in records]
            )
        structural = prepare_transport_contribution(
            connection,
            bundle=configured,
            template_dir=bundle.repo_root / "inputs/0_canoe_template",
            include_existing_capacity=False,
            include_demand=False,
            include_road_utilization=False,
            include_lifetimes=False,
            include_efficiencies=False,
            include_costs=False,
            include_emission_embodied=False,
            include_charging_profiles=False,
        )
        contribution = replace(
            structural,
            efficiency_rows=first.efficiency_rows,
            input_split_rows=first.split_rows,
            efficiency_assumption_dataset=first.assumption_dataset,
            provenance_contexts=first.provenance_contexts,
        )
        connection.commit()
        insert_transport_contribution(connection, contribution)
        assert connection.in_transaction
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT DISTINCT operator FROM limit_tech_input_split"
        ).fetchall() == [("e",)]
        connection.rollback()
        assert connection.execute("SELECT COUNT(*) FROM efficiency").fetchone()[0] == 0


def test_legacy_efficiency_comparison_does_not_invent_network_aliases(tmp_path):
    from validation.legacy_compare import compare_legacy_efficiency

    candidate, reference = tmp_path / "candidate.sqlite", tmp_path / "legacy.sqlite"
    for path, value, output in (
        (candidate, 2.0, "T_D_pkm"),
        (reference, 1.0, "T_D_pkm"),
    ):
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE efficiency(region, tech, vintage, input_comm, output_comm, efficiency)"
            )
            connection.execute(
                "INSERT INTO efficiency VALUES('ON','T_ONE',2025,'T_gsl',?,?)",
                (output, value),
            )
    result = compare_legacy_efficiency(
        candidate, reference, absolute_tolerance=1e-9, relative_tolerance=1e-6
    )
    assert result["shared_keys"] == result["value_differences"] == 1
    with sqlite3.connect(reference) as connection:
        connection.execute("UPDATE efficiency SET output_comm='T_annual_pkm'")
    result = compare_legacy_efficiency(
        candidate, reference, absolute_tolerance=1e-9, relative_tolerance=1e-6
    )
    assert result["shared_keys"] == 0


def test_efficiency_metadata_uses_the_designated_component_dq():
    from validation.config_models import DataQuality

    bundle = load_config_bundle(
        "config/scenarios/default.yaml", repo_root=Path(__file__).resolve().parents[1],
    )
    sources = bundle.sources.model_copy(deep=True)
    primary = DataQuality(dq_cred=2, dq_geog=1, dq_struc=3, dq_tech=2, dq_time=1)
    secondary = DataQuality(dq_cred=4, dq_geog=3, dq_struc=2, dq_tech=4, dq_time=3)
    sources.sources[layer.CEUD].components[21].data_quality = primary
    sources.sources[layer.CEUD].components[20].data_quality = secondary
    sources.sources[layer.ATB].components["vehicles"].data_quality = secondary
    context = layer.efficiency_context(
        replace(bundle, sources=sources),
        [(layer.CEUD, 21), (layer.CEUD, 20), (layer.ATB, "vehicles")],
        "fixture", {}, {"transformation_version": "1"},
    )
    assert context.data_quality == primary
    assert len(context.contributors) == 2


def test_efficiency_metadata_selects_only_used_manual_rows():
    manual = pd.DataFrame([
        {"registered_source_id": "epri_us_regen_transportation", "registered_component_id": "nonroad_efficiency_multipliers"},
        {"registered_source_id": "argonne_rd_greet", "registered_component_id": "marine_hfo_energy_intensity"},
    ])
    base = [("nrcan_ceud_transport_national", 33)]
    args = dict(
        bundle=None, mode="freight_marine", rules={"road_classes": {}}, road={},
        offroad={"energy_intensity_series": {"freight_marine": {"national_table": 33}}},
        region="ON", powertrain="hfo", year=2020, manual=manual,
        rating_inputs=None, baseline_inputs=None,
    )
    assert layer.efficiency_components(**args, result={"manual_rows": ""}) == base
    assert layer.efficiency_components(**args, result={"manual_rows": "1"}) == [
        *base, ("argonne_rd_greet", "marine_hfo_energy_intensity"),
    ]
    assert layer.efficiency_components(**args, result={"manual_rows": "0|1"}) == [
        *base, *map(tuple, manual.to_numpy()),
    ]


def test_efficiency_metadata_narrows_road_family_powertrain_and_time():
    rules = {
        "road_classes": {
            "school_buses": {"pathway": "bus", "stock_table": 31, "intensity_table": 28},
            "inter_city_buses": {"pathway": "intercity", "stock_table": 31, "intensity_table": 30},
            "motorcycles": {"pathway": "motorcycle", "intensity_table": 32},
        },
        "atb": {"fuel_equivalent_hhv_powertrains": ["diesel", "hev"]},
    }
    bundle = SimpleNamespace(scenario=SimpleNamespace(periods=SimpleNamespace(base_year=2023)))

    def components(mode, powertrain, year):
        return layer.efficiency_components(
            bundle, mode, rules, {"activity_series": {mode: {"table_id": 27}}}, {},
            region="ON", powertrain=powertrain, year=year, result={},
            manual=pd.DataFrame(), rating_inputs=None, baseline_inputs=None,
        )

    assert components("motorcycles", "gasoline", 2020) == [(layer.CEUD, 32)]
    assert components("motorcycles", "gasoline", 2030)[-1] == (
        "jgcri_gcam_motorcycle_inputs", "canada_motorcycle_inputs",
    )
    bev = components("school_buses", "bev", 2030)
    assert bev == [(layer.CEUD, 27), (layer.CEUD, 31), (layer.ATB, "vehicles")]
    phev = components("school_buses", "phev", 2020)
    assert (layer.ATB, "phev_utility_factor_mdhd") in phev
    assert (layer.ATB, "phev_utility_factor_ldv") not in phev
    assert (layer.ATB, "vehicles") not in phev
    assert (layer.CEUD, 28) in phev
    regen = ("epri_us_regen_transportation", "intercity_bus_charts")
    assert regen in components("inter_city_buses", "bev", 2030)
    assert regen not in components("inter_city_buses", "hev", 2030)


def test_efficiency_metadata_rating_editions_follow_interpolation_endpoints():
    classified = pd.DataFrame([
        {"year": year, "powertrain": "gasoline", "rating_class": "Compact", "included": True,
         "component": component, "classification_method": "source_fuel", "native_unit": "L/100 km"}
        for year, component in ((2014, "historical"), (2023, "current"), (2026, "future"))
    ])
    audit = classified[["year", "powertrain", "rating_class"]].assign(ceud_class="Car")
    rules = {"road_classes": {"cars": {"weights": "Car"}}, "ratings": {"evidence_source": "epa"}}
    rating_inputs, _ = layer.ldv_rating_input_resolvers(
        SimpleNamespace(base_year=2023), classified, audit, rules=rules,
    )
    assert rating_inputs("cars", "gasoline", 2014, "native") == (
        ("nrcan_fuel_consumption_ratings", "historical"),
    )
    assert set(rating_inputs("cars", "gasoline", 2018, "mj")) == {
        ("nrcan_fuel_consumption_ratings", "historical"),
        ("nrcan_fuel_consumption_ratings", "current"),
        ("argonne_rd_greet", "fuel_heating_values"),
    }
