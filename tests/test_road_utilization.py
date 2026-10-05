"""Focused contracts for road mileage weighting and the v4 age boundary."""

from dataclasses import replace

import pandas as pd
import pytest

from parameterization.road_utilization import (
    prepare_road_utilization,
)
from utils import load_config_bundle, load_harmonization_rules


def test_age_mode_retains_vintage_period_grain_and_flat_only_classes() -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    scenario = bundle.scenario.model_copy(update={
        "road_utilization": bundle.scenario.road_utilization.model_copy(
            update={"vkt_schedules": True, "vkt_max_age": 10},
        ),
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
    rules = load_harmonization_rules(bundle, "road_utilization")
    profile = pd.read_csv(
        bundle.repo_root / "inputs" / "2_processed" / "road_utilization"
        / rules["age_profile_file"]
    )
    car = profile.loc[profile.region.eq("ON") & profile.nrcan_ceud_class.eq("Car")]
    assert car.age.max() == 10
    assert scenario.lifetimes.survival_curve_max_age == 29
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
