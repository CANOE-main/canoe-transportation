"""Efficiency contracts: observed cohorts, relative projection, schema and coupling."""

from copy import deepcopy
from dataclasses import replace
import json
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
    derive_offroad_consumption,
    manual_ratio,
)
from parameterization.road_efficiencies import (
    RoadEfficiencyEvidence,
    aggregate_atb,
    aggregate_ratings,
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
    series = pd.DataFrame({"year": [2020, 2030], "value": [30, 20]})
    assert interpolate(series, 2023, "value") == 27
    with pytest.raises(ValueError, match="extrapolation"):
        interpolate(series, 2019, "value")
    with pytest.raises(ValueError, match="ambiguous"):
        interpolate(pd.concat([series, series]), 2023, "value")


def test_truck_fleet_levels_and_unchanged_bus_backcast_do_not_double_load(rules):
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
            for year, value in [(2020, 8), (2023, 6.8), (2025, 6), (2030, 4)]
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
        conversions=load_conversion_factors(load_config_bundle("config/scenarios/notebook_diagnostics.yaml")),
        projection_years=(2023,), projection_reference_year=2025,
        cohort_stock=pd.DataFrame([{"region": "ON", "road_class": "medium_trucks", "powertrain": "diesel",
                                    "vintage_year": 2023, "raw_capacity": 1}]),
    )
    medium_history = evidence.derive_consumption("ON", "medium_trucks", "diesel", 2000)
    hhv = evidence.conversions["fuel_hhv"]["diesel_mj_per_litre"]
    assert medium_history["native_intensity"] == pytest.approx(40 * hhv / 100)
    assert medium_history["load_factor"] == 5
    assert evidence.derive_consumption("ON", "medium_trucks", "diesel", 2030)["native_intensity"] == pytest.approx(20 * hhv / 100 * 4 / 6)
    bus_history = evidence.derive_consumption("ON", "urban_transit", "diesel", 2000)
    assert bus_history["native_intensity"] == pytest.approx(6.8 * 2 * 5 / 10)
    assert bus_history["load_factor"] == 5
    assert "efficiency" not in bus_history


def test_empty_bin_proxy_keeps_last_own_rating_without_future_observations(rules):
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


def test_phev_rating_keeps_components_and_combines_cd_cs_with_explicit_uf(rules, bundle):
    source = pd.DataFrame(
        [
            {
                "source_row": 2,
                "component": "phev",
                "Make": "Fixture",
                "Model": "PHEV",
                "Model year": 2023,
                "Vehicle class": "Compact",
                "Range 1 (km)": 50,
                "Combined (L/100 km)": 5.5,
                "Combined Le/100 km": "1.8 ([15.8 kWh + 0.0 L]/100 km)",
                "Fuel type 2": "X",
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
        uf_by_powertrain={"phev_35_miles": 0.6},
        phev_contract=load_harmonization_rules(bundle, "nlr_atb_autonomie")[
            "components"
        ]["phev_efficiency"]["future_nrcan_phev_contract"],
    ).iloc[0]
    assert result.combined_cd_consumption_Le_100_km == 1.8
    assert result.combined_cs_consumption_L_100_km == 5.5
    assert "15.8 kWh" in result.combined_cd_consumption_Le_100_km_raw
    hhv = load_conversion_factors(bundle)["fuel_hhv"]["gasoline_mj_per_litre"]
    assert result.consumption_mj_per_vkm == pytest.approx((0.6 * 15.8 * 3.6 + 0.4 * 5.5 * hhv) / 100)
    assert result.cd_electricity_kwh_per_100km == 15.8
    assert result.cd_fuel_l_per_100km == 0
    assert result.fleet_utility_factor == 0.6
    assert result.native_consumption == 1.8
    assert result.native_unit == "Le/100 km (CD)"
    assert result.classification_method == "omega_range_bucket_cd_cs_atb_uf"


def test_nrcan_bev_range_mapping_keeps_all_four_omega_buckets(rules, bundle):
    conversions = load_conversion_factors(bundle)
    source = pd.DataFrame([
        {"source_row": index, "component": "bev", "Model year": 2023, "Make": "Fixture", "Model": f"BEV {miles}",
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
    result = aggregate_ratings(classified.assign(Make="Fixture", Model="SUV"), weights,
                               rules=rules, weight_basis="all_vintages", statistic="mean")
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
            "cohort_matched_stock": 0, "aggregation_method": "distinct_model_class_weighted", "rating_statistic": "mean",
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
    assert set(fallback.aggregation_method) == {"distinct_model_fallback_no_resolved_stock"}
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
        "rating_class": "Compact", "aggregation_method": "distinct_model_class_weighted",
    }])
    resolver, _ = layer.ldv_rating_input_resolvers(
        SimpleNamespace(base_year=2023), classified, annual, rules=rules,
    )
    assert set(resolver("cars", "bev_200_miles", 2023, "mj")) == {
        ("nrcan_fuel_consumption_ratings", "battery_electric_first"),
        ("nrcan_fuel_consumption_ratings", "battery_electric_second"),
    }


@pytest.mark.parametrize("powertrain,anchor", [
    ("bev_400_miles", "bev_300_miles"), ("phev_50_miles", "phev_35_miles"),
])
@pytest.mark.parametrize("year,fallback", [(2000, True), (2015, False), (2023, False), (2030, False)])
def test_pev_lineage_uses_observed_history_and_projection_ratio_sources(
    bundle, rules, powertrain, anchor, year, fallback,
):
    calls = []

    def rating_inputs(mode, source_powertrain, source_year, column):
        calls.append((mode, source_powertrain, source_year, column))
        return (("nrcan_fuel_consumption_ratings", source_powertrain),)

    def baseline_inputs(*_args):
        return (("nrcan_fuel_consumption_ratings", "baseline"),)

    result = {
        "treatment": "NRCan_missing_year_approved_analogue_index" if fallback else "NRCan_observed_class_weighted" if year <= 2023 else "NRCan_cohort_anchor_ATB_relative_projection",
        "analogue": "gasoline" if fallback else anchor,
    }
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    components = layer.efficiency_components(
        bundle, "cars", rules, road, {}, region="ON", powertrain=powertrain,
        year=year, result=result, manual=pd.DataFrame(),
        rating_inputs=rating_inputs, baseline_inputs=baseline_inputs,
    )
    if fallback:
        assert calls == [("cars", "gasoline", year, "native"), ("cars", "gasoline", 2023, "native")]
        assert ("nrcan_fuel_consumption_ratings", "baseline") in components
    elif year <= 2023:
        assert calls == [("cars", powertrain, year, "mj")]
        assert ("nrcan_fuel_consumption_ratings", powertrain) in components
    else:
        assert not calls  # The preparation caller adds each observed anchor year and stock lineage.
        assert set(layer.atb_efficiency_components(powertrain, "ldv", rules)).issubset(components)
        assert set(layer.atb_efficiency_components(anchor, "ldv", rules)).issubset(components)



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


def test_phev_split_requires_exact_consumption_endpoints_and_accepts_zero_electricity():
    atb = pd.DataFrame([
        {"region": "ON", "mode": "cars", "powertrain": "phev_35_miles", "year": year,
         "weighted_consumption": consumption, "electricity_input_share": electricity}
        for year, consumption, electricity in [(2030, 2, 0.0), (2040, 1, .5)]
    ])
    arguments = {
        "rules": {"existing_phev_blends": {"cars": {"tech": "BLND"}}},
        "range_weights": {("cars", "phev"): {"phev_35_miles": 1.0}}, "regions": ["ON"],
    }
    result = layer.existing_phev_split_evidence(atb, endpoints=[2030], **arguments)
    assert result.electricity_share.tolist() == [0.0]
    assert result.fuel_share.tolist() == [1.0]
    with pytest.raises(ValueError, match="Missing exact"):
        layer.existing_phev_split_evidence(atb, endpoints=[2035], **arguments)


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
        return derive_offroad_consumption(
            national,
            manual,
            mode="freight_marine",
            powertrain="hfo",
            year=year,
            base_year=2023,
            selectors=selectors,
            rules=rules["offroad"],
        )

    assert derive(2000)["native_intensity"] == .5
    assert derive(2030)["native_intensity"] == pytest.approx(.25 / 1.01**7)
    assert "efficiency" not in derive(2030)
    ratios = pd.DataFrame(
        {"period": ["through_2035", "through_2050"], "value": [1.5, 1.8]}
    )
    assert manual_ratio(ratios, 2040, rules=rules["offroad"]) == 1.5


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


@pytest.mark.parametrize("include_bus_capacity,missing_rating_year", [(False, None), (True, 2017)])
def test_offline_deterministic_preparation_and_caller_owned_insertion(
    bundle, tmp_path, monkeypatch, include_bus_capacity, missing_rating_year
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
    routes = dict(bundle.paths.artifacts)
    for family in routes:
        if family.startswith(("efficiencies_", "ldv_range_")) or family == "existing_capacity_interim":
            routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    configured = replace(bundle, scenario=type(bundle.scenario).model_validate(payload),
                         paths=bundle.paths.model_copy(update={"artifacts": routes}))
    original_path = layer.resolve_artifact_path

    def output_path(b, family, *parts):
        if family.startswith(("efficiencies_", "ldv_range_")) or family == "existing_capacity_interim":
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
    if missing_rating_year is not None:
        original_consumption = RoadEfficiencyEvidence.derive_consumption

        def with_missing_observation(self, region, mode, powertrain, year, *, allow_analogue=False):
            if mode == "cars" and powertrain.startswith(("bev", "phev")) and year == missing_rating_year:
                assert not allow_analogue, "A partially observed period must not fill gaps with an analogue"
                return None
            return original_consumption(self, region, mode, powertrain, year, allow_analogue=allow_analogue)

        monkeypatch.setattr(RoadEfficiencyEvidence, "derive_consumption", with_missing_observation)
    capacity = [
        SimpleNamespace(region="ON", tech="T_MDV_T_BEV_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_MDV_T_DSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_MDV_T_GSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_HDV_T_DSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_C_GSL_PHEV_EX", vintage=2020, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_C_GSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_LTP_GSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_LDV_LTF_GSL_EX", vintage=2023, capacity=1),
        SimpleNamespace(region="ON", tech="T_HDV_BT_GSL_EX", vintage=2023, capacity=0),
    ]
    if include_bus_capacity:
        capacity.append(
            SimpleNamespace(
                region="ON", tech="T_HDV_BT_DSL_EX", vintage=2020, capacity=1
            )
        )
    from validation.provenance import resolve_provenance

    capacity_context = resolve_provenance(
        configured.sources, source_key="statcan_transport_tables", component_key="20-10-0021-01",
        transformation="synthetic annual stock fixture", transformation_version="1",
    )
    stock_records = []
    stock_modes = {
        "T_MDV_T_BEV_EX": "medium_trucks",
        "T_MDV_T_DSL_EX": "medium_trucks",
        "T_MDV_T_GSL_EX": "medium_trucks",
        "T_HDV_T_DSL_EX": "heavy_trucks",
        "T_LDV_C_BEV_EX": "cars",
        "T_LDV_C_GSL_PHEV_EX": "cars",
        "T_LDV_C_GSL_EX": "cars",
        "T_LDV_LTP_GSL_EX": "passenger_light_trucks",
        "T_LDV_LTF_GSL_EX": "freight_light_trucks",
    }
    for row in capacity:
        row.data_id = capacity_context.data_id
        if row.tech not in stock_modes:
            continue
        cohorts = ([(2021, .2), (2022, .3), (2023, .5)] if row.vintage == 2023
                   else [(2016, .1), (2017, .15), (2018, .2), (2019, .25), (2020, .3)])
        stock_records.extend({"region": row.region, "road_class": stock_modes[row.tech],
                              "tech": row.tech, "vintage": row.vintage, "vintage_year": year, "capacity": amount,
                              "raw_capacity": cohorts[-index - 1][1]}
                             for index, (year, amount) in enumerate(cohorts))
    stock_file = load_harmonization_rules(configured, "road_stocks_and_demands")["existing_capacity"]["fuel_shares_file"]
    stock_path = output_path(configured, "existing_capacity_interim", stock_file)
    stock_path.parent.mkdir(parents=True)
    pd.DataFrame(stock_records).to_csv(stock_path, index=False)
    capacity_audit = {"period_mapping": configured.scenario.periods.audit(),
                      "annual_stock_evidence": {"path": str(stock_path), "sha256": file_sha256(stock_path)}}
    handoff = {"existing_capacity_rows": capacity, "existing_capacity_contexts": [capacity_context],
               "existing_capacity_audit": capacity_audit}
    # Plan 046 added bus capacity, retiring plan 045's source-only historical bus rows.
    rules = load_harmonization_rules(configured, "efficiencies")
    assert rules["historical_classes_without_capacity"] == []
    first = layer.prepare_efficiency_rows(configured, **handoff)
    hashes = {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    second = layer.prepare_efficiency_rows(configured, **handoff)
    assert hashes == {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    assert first.audit == second.audit
    assert first.audit["rating_aggregation"] == "distinct_model_class_weighted"
    assert first.audit["rating_statistic"] == configured.scenario.efficiencies.rating_statistic
    assert first.audit["annual_stock_evidence"] == capacity_audit["annual_stock_evidence"]
    assert "rating_stock_weighting" not in first.audit
    aggregation_rules = load_harmonization_rules(configured, "road_aggregation")
    evidence_names = {Path(item["path"]).name for item in first.audit["input_evidence"]}
    assert aggregation_rules["mapped_current_stock_file"] not in evidence_names
    assert aggregation_rules["fueleconomy_evidence_file"] not in evidence_names
    assert stock_file in evidence_names
    assert not (tmp_path / "efficiencies_interim/rating_stock_match_audit.csv").exists()
    assert not (tmp_path / "efficiencies_interim/stock_weighted_rating_evidence.csv").exists()
    # Future levels use the latest observed cohort; ATB supplies relative range/time effects.
    ltp_bev_ranges = {
        "T_LDV_LTP_BEV150_N": "bev_150_miles",
        "T_LDV_LTP_BEV200_N": "bev_200_miles",
        "T_LDV_LTP_BEV300_N": "bev_300_miles",
        "T_LDV_LTP_BEV400_N": "bev_400_miles",
    }
    atb_evidence = pd.read_csv(tmp_path / "efficiencies_interim/aggregated_atb_consumption.csv")
    future_ltp = atb_evidence.loc[
        atb_evidence.region.eq("ON") & atb_evidence["mode"].eq("passenger_light_trucks")
        & atb_evidence.year.eq(2030)
    ].groupby("powertrain").weighted_consumption.sum()
    atb_anchor_2025 = atb_evidence.loc[
        atb_evidence.region.eq("ON") & atb_evidence["mode"].eq("passenger_light_trucks")
        & atb_evidence.year.eq(2025) & atb_evidence.powertrain.eq("bev_300_miles"),
        "weighted_consumption",
    ].sum()
    rating_evidence = pd.read_csv(tmp_path / "efficiencies_interim/aggregated_rating_consumption.csv")
    annual_ratings = rating_evidence.assign(
        weighted_mj=rating_evidence.class_mean_mj_per_vkm * rating_evidence.weight,
    ).groupby(["ceud_class", "powertrain", "year"]).weighted_mj.sum()
    ltp_anchor_ratings = annual_ratings.loc[("Light Truck", "bev_300_miles")].loc[[2021, 2022, 2023]]
    anchor_weights = pd.Series({2021: .5, 2022: .3, 2023: .2})
    ltp_anchor_consumption = (ltp_anchor_ratings * anchor_weights).sum()
    load_evidence = pd.read_csv(tmp_path / "efficiencies_interim/road_load_factors.csv")
    ltp_load = load_evidence.loc[
        load_evidence.region.eq("ON") & load_evidence["mode"].eq("passenger_light_trucks")
        & load_evidence.year.isin(anchor_weights.index), ["year", "load_factor"]
    ].set_index("year").load_factor
    ltp_anchor_load = (ltp_load * anchor_weights).sum()
    actual_ltp_bev = {
        row.tech: row.efficiency
        for row in first.efficiency_rows
        if row.vintage == 2025 and row.tech in ltp_bev_ranges
    }
    assert actual_ltp_bev == pytest.approx({
        tech: ltp_anchor_load / (ltp_anchor_consumption * future_ltp[bucket] / atb_anchor_2025)
        for tech, bucket in ltp_bev_ranges.items()
    }, rel=1e-12)
    assert [actual_ltp_bev[tech] for tech in ltp_bev_ranges] == sorted(actual_ltp_bev.values(), reverse=True)
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
    periods = pd.read_csv(tmp_path / "efficiencies_interim/period_consumption_evidence.csv")
    assert len(periods) == len(first.efficiency_rows)
    assert "efficiency" not in annual
    assert periods.efficiency.tolist() == pytest.approx((periods.load_factor / periods.native_intensity).tolist())
    assert set(annual.loc[annual.vintage.eq(2025), "source_year"]) == {2030}
    projected_bevs = annual.loc[annual.tech.isin(ltp_bev_ranges) & annual.vintage.eq(2025)]
    assert set(projected_bevs.treatment) == {"NRCan_cohort_anchor_ATB_relative_projection"}
    assert set(projected_bevs.projection_anchor_powertrain) == {"bev_300_miles"}
    assert projected_bevs.projection_source_years.map(json.loads).tolist() == [[2021, 2022, 2023]] * 4
    assert projected_bevs.projection_reference_year.eq(2025).all()
    assert set(projected_bevs.projection_stock_powertrain) == {"gasoline"}
    assert projected_bevs.projection_stock_proxy.eq(True).all()
    assert projected_bevs.projection_stock_coverage.eq(1).all()
    for tech in ltp_bev_ranges:
        projected_row = next(row for row in first.efficiency_rows if row.tech == tech and row.vintage == 2025)
        context = next(context for context in first.provenance_contexts if context.data_id == projected_row.data_id)
        assert {"nrcan_fuel_consumption_ratings", "nlr_atb_transportation", "statcan_transport_tables"}.issubset(
            contributor.source_key for contributor in context.contributors
        )
    medium = annual.loc[annual.tech.eq("T_MDV_T_BEV_EX")].sort_values("source_year")
    assert medium.source_year.tolist() == [2021, 2022, 2023]
    assert medium.temporal_weight.tolist() == pytest.approx([.5, .3, .2])
    assert medium.cohort_stock_k_vehicles.tolist() == pytest.approx([.5, .3, .2])
    medium_row = next(row for row in first.efficiency_rows if row.tech == "T_MDV_T_BEV_EX")
    expected_medium = (medium.load_factor * medium.temporal_weight).sum() / (medium.native_intensity * medium.temporal_weight).sum()
    assert medium_row.efficiency == pytest.approx(expected_medium)
    assert medium_row.efficiency != pytest.approx((medium.load_factor / medium.native_intensity * medium.temporal_weight).sum(), rel=1e-10)
    assert annual.loc[annual.vintage.eq(2025), "temporal_weight"].eq(1).all()
    weights = first.audit["existing_ev_representatives"]["weights"]
    for powertrain, tech in (("BEV", "T_LDV_C_BEV_EX"), ("PHEV", "T_LDV_C_GSL_PHEV_EX")):
        component_rows = annual.loc[
            annual.tech.eq(tech) & annual.range_representative_component.eq(True)
        ]
        assert set(component_rows.temporal_weight_basis) == {"age_distribution_cohort_stock"}
        assert component_rows.groupby("powertrain").temporal_weight.sum().tolist() == pytest.approx([1] * component_rows.powertrain.nunique())
        components = component_rows.assign(weighted=component_rows.native_intensity * component_rows.temporal_weight).groupby("powertrain").weighted.sum().to_dict()
        shares = {entry["bucket"]: entry["share_of_all_sales"] for entry in weights
                  if entry["market_class"] == "car" and entry["powertrain"] == powertrain}
        row = next(row for row in first.efficiency_rows if row.tech == tech)
        assert set(components) == set(shares)
        representative = annual.loc[annual.tech.eq(tech) & annual.range_representative_component.ne(True)]
        weighted_load = (representative.load_factor * representative.temporal_weight).sum()
        assert row.efficiency == pytest.approx(weighted_load / sum(shares[key] * value for key, value in components.items()))
        raw_weights = representative.set_index("source_year").cohort_weight.sort_index()
        assert raw_weights.tolist() == pytest.approx([.3, .25, .2, .15, .1])
        omitted = representative.loc[~representative.included_in_period]
        assert omitted.temporal_weight.eq(0).all()
        assert omitted.native_intensity.isna().all()
        period = periods.loc[periods.tech.eq(tech)].iloc[0]
        assert period.candidate_year_count == 5
        assert period.included_year_count == representative.included_in_period.sum()
        range_coverage = component_rows.loc[component_rows.included_in_period].groupby("powertrain").cohort_weight.sum()
        assert json.loads(period.range_stock_coverage) == pytest.approx(range_coverage.to_dict())
        assert period.cohort_stock_coverage == pytest.approx(sum(shares[bucket] * value for bucket, value in range_coverage.items()))
        if period.observation_coverage < 1 - rules["tolerances"]["weight_sum"]:
            reported = next(item for item in first.audit["incomplete_observation_coverage"]
                            if item["tech"] == tech and item["vintage"] == period.vintage)
            assert reported["observation_coverage"] == pytest.approx(period.observation_coverage)
            assert json.loads(reported["range_stock_coverage"]) == pytest.approx(range_coverage.to_dict())
        assert json.loads(period.range_source_years) == {
            bucket: sorted(group.loc[group.included_in_period, "source_year"].tolist())
            for bucket, group in component_rows.groupby("powertrain")
        }
        for bucket, group in component_rows.groupby("powertrain"):
            observed_years = set(annual_ratings.loc[("Car", bucket)].index) & set(group.source_year)
            observed_years.discard(missing_rating_year)
            if observed_years:
                included = group.loc[group.included_in_period]
                assert set(included.source_year) == observed_years
                assert set(included.treatment) == {"NRCan_observed_class_weighted"}
        own_observations = component_rows.loc[component_rows.treatment.eq("NRCan_observed_class_weighted")]
        assert not own_observations.empty
        for item in own_observations.itertuples():
            assert item.native_intensity == pytest.approx(annual_ratings.loc[("Car", item.powertrain, item.source_year)])
        if missing_rating_year is not None:
            missing_components = component_rows.loc[component_rows.source_year.eq(missing_rating_year)]
            assert not missing_components.included_in_period.any()
            assert missing_components.temporal_weight.eq(0).all()
            assert missing_components.native_intensity.isna().all()
            missing = omitted.loc[omitted.source_year.eq(missing_rating_year)].iloc[0]
            assert missing.cohort_weight == .25
            assert missing.omission_reason == "missing_own_or_anchor_observation"
            assert missing.cohort_stock_k_vehicles == .25
            assert period.cohort_stock_coverage <= .75
            assert first.audit["omitted_annual_observations"] >= 2
        context = next(context for context in first.provenance_contexts if context.data_id == row.data_id)
        assert "epa_omega_baseline" in {contributor.source_key for contributor in context.contributors}
        assert ("statcan_transport_tables", "20-10-0021-01") in {
            (contributor.source_key, component.component_key)
            for contributor in context.contributors for component in contributor.components
        }
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
        assert bus_annual.temporal_weight.eq(.2).all()
        assert set(bus_annual.temporal_weight_basis) == {"equal_annual_observations"}
        expected_bus = shared_bus.loc[
            shared_bus.tech.eq("T_HDV_BT_DSL_EX")
            & shared_bus.year.isin(bus_annual.source_year)
        ].sort_values("year")
        assert list((bus_annual.load_factor / bus_annual.native_intensity).sort_index()) == pytest.approx(
            list(expected_bus.efficiency),
            abs=1e-12,
        )
        bus_row = next(
            row for row in first.efficiency_rows if row.tech == "T_HDV_BT_DSL_EX"
        )
        assert bus_row.efficiency == pytest.approx(
            bus_annual.load_factor.mean() / bus_annual.native_intensity.mean(), abs=1e-12
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
            "SELECT DISTINCT operator FROM limit_tech_input_split_annual"
        ).fetchall() == [("e",)]
        assert connection.execute("SELECT COUNT(*) FROM limit_tech_input_split").fetchone()[0] == 0
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


def test_efficiency_metadata_rating_editions_require_exact_observation_years():
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
    assert set(rating_inputs("cars", "gasoline", 2023, "mj")) == {
        ("nrcan_fuel_consumption_ratings", "current"),
        ("argonne_rd_greet", "fuel_heating_values"),
    }
    with pytest.raises(ValueError, match="Missing exact annual rating provenance"):
        rating_inputs("cars", "gasoline", 2018, "mj")
