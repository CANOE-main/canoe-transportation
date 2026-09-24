import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import (
    aggregate_normalized_mileage_profiles,
    derive_road_annual_utilization,
    derive_ldv_age_distributions,
    flat_road_capacity_factor_records,
    mean_age_utilization_for_period,
    median_lifetime_map,
    reconcile_road_capacity_to_activity,
)


def mapped_stock_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_year": [2025] * 6,
            "MODEL_YEAR": [2025, 2011, 2010, 1994, 2026, 2000],
            "FIT_ACTIVE": [10, 20, 30, 40, 50, 60],
            "nrcan_ceud_class": [
                "Car",
                "Car",
                "Car",
                "Car",
                "Light Truck",
                "Light Truck",
            ],
            "nlr_atb_class": [
                "Compact",
                "Compact",
                "Compact",
                "Compact",
                "Small SUV",
                "Small SUV",
            ],
            "mapping_accepted": [True] * 6,
        }
    )


def test_median_mode_truncates_by_class_and_reports_exclusions() -> None:
    distribution, findings = derive_ldv_age_distributions(
        mapped_stock_fixture(),
        survival_curves=False,
        maximum_age=30,
        median_lifetimes={"Car": 14, "Light Truck": 15},
        historical_review_minimum_model_year=2000,
    )

    car = distribution.loc[distribution["nrcan_ceud_class"].eq("Car")]
    assert car["age"].tolist() == [0, 14]
    light_truck = findings.loc[
        findings["nrcan_ceud_class"].eq("Light Truck")
    ].iloc[0]
    assert light_truck["negative_age_fit_active_stock"] == 50
    assert light_truck["over_cutoff_fit_active_stock"] == 60
    assert findings.loc[
        findings["nrcan_ceud_class"].eq("Car"),
        "before_historical_review_year_fit_active_stock",
    ].item() == 40
    assert car["age_distribution"].sum() == pytest.approx(1.0)


def test_survival_curve_mode_uses_configured_maximum_age() -> None:
    distribution, findings = derive_ldv_age_distributions(
        mapped_stock_fixture(),
        survival_curves=True,
        maximum_age=30,
        median_lifetimes={"Car": 14, "Light Truck": 15},
        historical_review_minimum_model_year=2000,
    )

    car = distribution.loc[distribution["nrcan_ceud_class"].eq("Car")]
    assert car["age"].tolist() == [0, 14, 15]
    assert set(findings["cutoff_basis"]) == {"scenario_survival_curve_max_age"}
    assert set(findings["cutoff_age"]) == {30.0}


def test_median_lifetime_map_rejects_duplicate_ceud_rows() -> None:
    medians = pd.DataFrame(
        {
            "target_system": ["nrcan_ceud", "nrcan_ceud"],
            "target_class": ["Car", "Car"],
            "median_equivalent_age": [14, 15],
        }
    )

    with pytest.raises(ValueError, match="Duplicate"):
        median_lifetime_map(medians)


def test_road_capacity_to_activity_uses_template_category_owners() -> None:
    manual = pd.DataFrame([
        {"category": "cars", "sub_category": "all", "units": "bn passenger-km/k units",
         "c2a": 3.504, "notes": "reviewed"},
        {"category": "charger", "sub_category": "all", "units": "PJ/GW",
         "c2a": 31.536, "notes": "out of scope"},
    ])
    technology = pd.DataFrame([
        {"tech": "T_CAR_EX", "category": "cars"},
        {"tech": "T_CAR_N", "category": "cars"},
        {"tech": "T_CHRG", "category": "charger"},
    ])
    rows = reconcile_road_capacity_to_activity(
        manual, technology, road_classes={"cars"},
        activity_units={"cars": "bn passenger-km"},
    )
    assert rows["tech"].tolist() == ["T_CAR_EX", "T_CAR_N"]
    assert rows["c2a"].tolist() == [3.504, 3.504]
    with pytest.raises(ValueError, match="unit mismatch"):
        reconcile_road_capacity_to_activity(
            manual.assign(units="PJ/GW"), technology, road_classes={"cars"},
            activity_units={"cars": "bn passenger-km"},
        )


def test_road_annual_utilization_checks_matching_ceud_activity_and_stock() -> None:
    provincial = pd.DataFrame([
        {"region": "ON", "year": 2023, "table_id": 20,
         "raw_series": "Cars|Activity", "unit": "millions", "value": 3504.0},
        {"region": "ON", "year": 2023, "table_id": 21,
         "raw_series": "Stock|Cars", "unit": "thousands", "value": 2.0},
    ])
    kwargs = {
        "regions": ["ON"], "years": [2023],
        "activity_series": {"cars": {"table_id": 20, "raw_series": "Cars|Activity"}},
        "stock_series": {"cars": {"table_id": 21, "raw_series": "Stock|Cars"}},
        "capacity_by_class": {"cars": 3.504}, "region_output_map": {},
        "activity_to_billion_factor": 0.001,
    }
    result = derive_road_annual_utilization(provincial, **kwargs)
    assert result.iloc[0].utilization == pytest.approx(0.5)
    assert result.iloc[0].unit == "dimensionless"
    with pytest.raises(ValueError, match="Missing or duplicate"):
        derive_road_annual_utilization(
            pd.concat([provincial, provincial.iloc[[0]]]), **kwargs
        )
    with pytest.raises(ValueError, match="unit"):
        derive_road_annual_utilization(
            provincial.assign(unit="PJ"), **kwargs
        )


def test_weighted_age_profile_preserves_pattern_and_rejects_gaps() -> None:
    mileage = pd.DataFrame([
        {"vehicle_class": vehicle_class, "year_index": age, "vmt_mi": value,
         "unit": "mi/vehicle-year"}
        for vehicle_class, values in (("Compact", [10, 8, 6]), ("Midsize", [20, 10, 5]))
        for age, value in enumerate(values)
    ])
    weights = pd.DataFrame([
        {"nrcan_ceud_class": "Car", "nlr_atb_class": "Compact",
         "aggregation_weight": 0.25},
        {"nrcan_ceud_class": "Car", "nlr_atb_class": "Midsize",
         "aggregation_weight": 0.75},
    ])
    result = aggregate_normalized_mileage_profiles(
        mileage, weights, mile_to_km=1.609344
    )
    assert result["normalized_utilization"].tolist() == pytest.approx(
        [1, 9.5 / 17.5, 5.25 / 17.5]
    )
    assert result["weighted_vmt_km"].iloc[0] == pytest.approx(17.5 * 1.609344)
    with pytest.raises(ValueError, match="unequal age coverage"):
        aggregate_normalized_mileage_profiles(
            mileage.loc[~(mileage.vehicle_class.eq("Midsize") & mileage.year_index.eq(2))],
            weights, mile_to_km=1.609344,
        )


def test_flat_road_factor_records_keep_period_only_v4_grain() -> None:
    baseline = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "utilization": 0.2},
        {"region": "AB", "road_class": "cars", "utilization": 0.3},
    ])
    technology = pd.DataFrame([
        {"tech": "T_CAR_EX", "category": "cars"},
        {"tech": "T_CAR_N", "category": "cars"},
    ])
    rows = flat_road_capacity_factor_records(
        baseline, technology, periods=[2025, 2030],
        commodity_by_class={"cars": "T_D_pkm_ldv_c"},
    )
    assert len(rows) == 8
    assert {(row["region"], row["tech_or_group"], row["vintage"])
            for row in rows} == {
        (region, tech, period)
        for region in ("AB", "ON")
        for tech in ("T_CAR_EX", "T_CAR_N")
        for period in (2025, 2030)
    }
    assert {row["operator"] for row in rows} == {"e"}
    assert {row["output_comm"] for row in rows} == {"T_D_pkm_ldv_c"}
    with pytest.raises(ValueError, match="unit interval"):
        flat_road_capacity_factor_records(
            baseline.assign(utilization=1.2), technology, periods=[2025],
            commodity_by_class={"cars": "T_D_pkm_ldv_c"},
        )


def test_age_utilization_uses_five_annual_values_after_period_start() -> None:
    profile = pd.DataFrame({
        "age": [0, 1, 2, 3, 4, 5],
        "normalized_utilization": [1.0, 0.9, 0.8, 0.7, 0.6, 0.5],
    })
    result = mean_age_utilization_for_period(
        profile, baseline_utilization=0.2, vintage=2025, period=2025, step=5,
    )
    assert result == pytest.approx(0.2 * 0.7)
    with pytest.raises(ValueError, match="does not cover"):
        mean_age_utilization_for_period(
            profile.iloc[:-1], baseline_utilization=0.2,
            vintage=2025, period=2025, step=5,
        )
