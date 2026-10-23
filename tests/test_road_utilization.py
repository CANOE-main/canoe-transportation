"""Focused contracts for road mileage weighting and the v4 age boundary."""

from dataclasses import replace
import sys

import pandas as pd
import pytest
from canoe_schema.v4_0 import ExistingCapacity

from parameterization.road_utilization import (
    prepare_road_utilization,
    validate_road_utilization,
)
from utils import load_config_bundle, load_harmonization_rules


@pytest.fixture
def isolated_bundle(tmp_path, monkeypatch):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    scenario = bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON"]}),
    })

    def artifact_path(config, family, *parts):
        assert family in {"road_utilization_processed", "road_utilization_validation"}
        return tmp_path.joinpath(family, *parts)

    monkeypatch.setattr("parameterization.road_utilization.resolve_artifact_path", artifact_path)
    return replace(bundle, scenario=scenario)


def test_age_mode_retains_vintage_period_grain_and_flat_only_classes(isolated_bundle, tmp_path) -> None:
    bundle = isolated_bundle
    scenario = bundle.scenario.model_copy(update={
        "road_utilization": bundle.scenario.road_utilization.model_copy(
            update={"vkt_schedules": True, "vkt_max_age": 10},
        ),
    })
    prepared = prepare_road_utilization(replace(bundle, scenario=scenario), existing_capacity_rows=[])
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
        tmp_path / "road_utilization_processed" / rules["age_profile_file"]
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


def test_flat_mode_applies_existing_cohort_limits_without_age_profiles(isolated_bundle, tmp_path) -> None:
    bundle = isolated_bundle
    scenario = bundle.scenario.model_copy(update={
        "road_utilization": bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": False}),
    })
    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=vintage, capacity=capacity, data_id="test-stock")
             for vintage, capacity in ((2000, 0.0), (2015, 1.0), (2020, 2.0))]
    prepared = prepare_road_utilization(replace(bundle, scenario=scenario), existing_capacity_rows=stock)
    assert prepared.age_factor_artifact is None
    assert prepared.audit["vkt_schedules"] is False
    rows = prepared.flat_factor_rows
    existing_suffix = load_harmonization_rules(bundle, "efficiencies")["existing_suffix"]
    for item in prepared.audit["flat_vintages_by_region_technology"]:
        tech, vintages = item["tech"], item["vintages"]
        expected = ([row.vintage for row in stock if row.tech == tech and row.capacity > 0]
                    if tech.endswith(existing_suffix) else scenario.periods.model)
        assert vintages == expected
        selected = [row for row in rows if row.tech_or_group == tech]
        assert {row.vintage for row in selected} == set(expected)
        if expected:
            assert len({row.factor for row in selected}) == 1
            assert {row.operator for row in selected} == {"e"}

    rules = load_harmonization_rules(bundle, "road_utilization")
    output = tmp_path / "road_utilization_processed"
    annual = pd.read_csv(output / rules["annual_audit_file"])
    baseline = pd.read_csv(output / rules["baseline_file"])
    expected_factor = baseline.loc[baseline.road_class.eq("cars"), "utilization"].item()
    cohort = [row for row in rows if row.tech_or_group == "T_LDV_C_GSL_EX" and row.vintage == 2015]
    assert len(cohort) == 1 and cohort[0].factor == pytest.approx(expected_factor)
    assert not any(row.tech_or_group.endswith(existing_suffix) and row.vintage in scenario.periods.model for row in rows)

    # Reintroducing the old future-year key is rejected before publication.
    wrong_vintage = [row.model_copy(update={"vintage": scenario.periods.model[0]})
                     if row is cohort[0] else row for row in rows]
    with pytest.raises(ValueError, match="Flat annual factor row coverage"):
        validate_road_utilization(
            annual, baseline, prepared.capacity_to_activity_rows, wrong_vintage,
            regions={"ON"}, road_classes=set(baseline.road_class), years=prepared.audit["baseline_years"],
            vintages_by_region_tech={(item["region"], item["tech"]): item["vintages"]
                                    for item in prepared.audit["flat_vintages_by_region_technology"]},
            flat_classes=set(baseline.road_class),
            technology=pd.read_csv(bundle.repo_root / "inputs/0_canoe_template/technology.csv"),
        )


def test_existing_utilization_keeps_regional_cohort_coverage(isolated_bundle) -> None:
    bundle = isolated_bundle
    scenario = bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON", "AB"]}),
        "road_utilization": bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": False}),
    })
    stock = [ExistingCapacity(region=region, tech="T_LDV_C_GSL_EX", vintage=vintage, capacity=1, data_id="test-stock")
             for region, vintage in (("ON", 2015), ("AB", 2020), ("QC", 2000))]
    prepared = prepare_road_utilization(replace(bundle, scenario=scenario), existing_capacity_rows=stock)
    existing = [row for row in prepared.flat_factor_rows if row.tech_or_group.endswith("_EX")]
    assert {(row.region, row.tech_or_group, row.vintage) for row in existing} == {
        ("ON", "T_LDV_C_GSL_EX", 2015), ("AB", "T_LDV_C_GSL_EX", 2020),
    }


def test_existing_utilization_rejects_positive_stock_outside_declared_vintages(isolated_bundle) -> None:
    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=1990, capacity=1, data_id="test-stock")]
    with pytest.raises(ValueError, match="outside the scenario"):
        prepare_road_utilization(isolated_bundle, existing_capacity_rows=stock)


def test_cli_explicitly_prepares_stock_once_before_utilization(monkeypatch) -> None:
    from parameterization import road_utilization as layer

    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=2015, capacity=1, data_id="test-stock")]
    events = []

    def prepare_stock(bundle):
        events.append("stock")
        return stock, [], {}

    def prepare_utilization(bundle, *, existing_capacity_rows):
        assert existing_capacity_rows is stock
        events.append("utilization")

    monkeypatch.setattr("parameterization.build_existing_capacity.prepare_existing_capacity_rows", prepare_stock)
    monkeypatch.setattr(layer, "prepare_road_utilization", prepare_utilization)
    monkeypatch.setattr(sys, "argv", ["road_utilization"])
    layer.main()
    assert events == ["stock", "utilization"]
