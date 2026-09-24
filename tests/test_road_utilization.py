"""Focused contracts for road mileage weighting and the v4 age boundary."""

from dataclasses import replace

import pandas as pd
import pytest

from parameterization.road_utilization import (
    heavy_haul_atb_weights,
    national_medium_atb_weights,
    prepare_road_utilization,
)
from utils import load_config_bundle, load_harmonization_rules


def test_national_medium_weights_average_distinct_freight_curves() -> None:
    wards = pd.DataFrame([
        {"vehicle_scope": "mhdv", "nrcan_ceud_class": "Medium Trucks",
         "year": 2020, "wards_size_class": "Class 6", "market_share": 1.0},
    ])
    mileage = pd.DataFrame([
        {"vehicle_class": name, "year_index": age, "vmt_mi": value}
        for name, values in (
            ("Class 6 Box", [10, 8]),
            ("Class 6 Box duplicate", [10, 8]),
            ("Class 6 Vocational", [20, 9]),
            ("Class 6 School Bus", [30, 7]),
        )
        for age, value in enumerate(values)
    ])
    rules = {
        "national_medium_share_year": 2020,
        "medium_atb_classes": ["Class 6"],
        "medium_atb_class_pattern": r"^Class ([3-7])$",
        "medium_within_class_method": "equal_distinct_freight_profiles",
        "excluded_medium_atb_labels": ["School", "Transit"],
    }
    result = national_medium_atb_weights(wards, mileage, rules=rules, max_age=1)
    assert result.nlr_atb_class.tolist() == ["Class 6 Box", "Class 6 Vocational"]
    assert result.aggregation_weight.tolist() == pytest.approx([0.5, 0.5])


def test_heavy_haul_weights_use_tonne_kilometres() -> None:
    freight = pd.DataFrame([
        {"scenario_region": "ON", "haul_class": "regional", "tonne_kilometres": 1},
        {"scenario_region": "ON", "haul_class": "long_haul", "tonne_kilometres": 3},
    ])
    rules = {"heavy_haul_atb_classes": {
        "regional": "Class 8 Regional DayCab",
        "long_haul": "Class 8 Longhaul Sleeper",
    }}
    result = heavy_haul_atb_weights(freight, source_region="ON", rules=rules)
    assert dict(zip(result.nlr_atb_class, result.aggregation_weight, strict=True)) == {
        "Class 8 Regional DayCab": pytest.approx(0.25),
        "Class 8 Longhaul Sleeper": pytest.approx(0.75),
    }


def test_age_mode_retains_vintage_period_grain_and_flat_only_classes() -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    scenario = bundle.scenario.model_copy(update={
        "switches": bundle.scenario.switches.model_copy(update={"vkt_schedules": True}),
    })
    prepared = prepare_road_utilization(replace(bundle, scenario=scenario))
    rows = prepared.age_factor_artifact
    assert rows is not None
    assert prepared.audit["sqlite_insertion"] == "blocked_upstream_vintage_plus_period_schema_gap"
    technology = pd.read_csv(bundle.repo_root / "inputs" / "0_canoe_template" / "technology.csv")
    categories = technology.set_index("tech").category.to_dict()
    assert {categories[row.tech_or_group] for row in prepared.flat_factor_rows} == {
        "motorcycles", "school_buses", "urban_transit", "inter_city_buses",
    }
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["capacity_factor"]
    profile = pd.read_csv(
        bundle.repo_root / "inputs" / "2_processed" / "road_utilization"
        / rules["age_profile_file"]
    )
    car = profile.loc[profile.region.eq("ON") & profile.nrcan_ceud_class.eq("Car")]
    assert car.normalized_utilization.max() == pytest.approx(1.0)
    assert car.normalized_utilization.nunique() > 1
    selected = rows.loc[
        rows.region.eq("ON") & rows.road_class.eq("cars")
        & rows.tech_or_group.eq("T_LDV_C_GSL_N")
        & rows.vintage.eq(2025) & rows.period.eq(2025)
    ].iloc[0]
    expected = selected.baseline_utilization * car.loc[
        car.age.between(1, 5), "normalized_utilization"
    ].mean()
    assert selected.factor == pytest.approx(expected)
    assert selected.age_first_year == 1
    assert selected.age_last_year == 5
    assert {"vintage", "period"} <= set(rows)
    assert rows[["region", "tech_or_group", "vintage", "period", "output_comm"]].duplicated().sum() == 0
