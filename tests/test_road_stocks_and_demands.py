import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import (
    aggregate_normalized_mileage_profiles,
    derive_road_annual_utilization,
    flat_road_capacity_factor_records,
    mean_age_utilization_for_period,
    reconcile_road_capacity_to_activity,
)


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
    with_explanation = reconcile_road_capacity_to_activity(
        manual.assign(speed="not consumed", load=99999, hours=-1),
        technology, road_classes={"cars"}, activity_units={"cars": "bn passenger-km"},
    )
    pd.testing.assert_frame_equal(rows, with_explanation)
    assert not {"speed", "load", "hours"} & set(with_explanation)
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


def test_flat_road_factor_records_apply_constant_values_to_owned_vintages() -> None:
    baseline = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "utilization": 0.2},
        {"region": "AB", "road_class": "cars", "utilization": 0.3},
    ])
    technology = pd.DataFrame([
        {"tech": "T_CAR_EX", "category": "cars"},
        {"tech": "T_CAR_N", "category": "cars"},
    ])
    vintages = {("ON", "T_CAR_EX"): [2015, 2020], ("AB", "T_CAR_EX"): [],
                ("ON", "T_CAR_N"): [2025, 2030], ("AB", "T_CAR_N"): [2025, 2030]}
    rows = flat_road_capacity_factor_records(
        baseline, technology, vintages_by_region_tech=vintages,
        commodity_by_class={"cars": "T_D_pkm_ldv_c"},
    )
    assert len(rows) == 6
    assert {(row["region"], row["tech_or_group"], row["vintage"])
            for row in rows} == {
        (region, tech, vintage)
        for region in ("AB", "ON")
        for tech in ("T_CAR_EX", "T_CAR_N")
        for vintage in vintages[region, tech]
    }
    assert {row["operator"] for row in rows} == {"e"}
    assert {row["output_comm"] for row in rows} == {"T_D_pkm_ldv_c"}
    assert {row["factor"] for row in rows if row["region"] == "ON"} == {0.2}
    assert {row["factor"] for row in rows if row["region"] == "AB"} == {0.3}
    with pytest.raises(ValueError, match="unit interval"):
        flat_road_capacity_factor_records(
            baseline.assign(utilization=1.2), technology, vintages_by_region_tech=vintages,
            commodity_by_class={"cars": "T_D_pkm_ldv_c"},
        )


@pytest.mark.parametrize("vintages", [{}, {("ON", "CAR"): [2015, 2015]},
                                     {("ON", "CAR"): [2015], ("AB", "CAR"): [2025]}])
def test_flat_road_factor_vintage_mapping_requires_exact_valid_ownership(vintages) -> None:
    with pytest.raises(ValueError, match="vintage"):
        flat_road_capacity_factor_records(
            pd.DataFrame([{"region": "ON", "road_class": "cars", "utilization": 0.2}]),
            pd.DataFrame([{"tech": "CAR", "category": "cars"}]),
            vintages_by_region_tech=vintages, commodity_by_class={"cars": "TRAVEL"},
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
