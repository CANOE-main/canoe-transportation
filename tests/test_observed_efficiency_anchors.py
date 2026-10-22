"""Observed LDV histories and cohort anchors precede future ATB improvement ratios."""

from copy import deepcopy
import json

import pandas as pd
import pytest

from parameterization.road_efficiencies import RoadEfficiencyEvidence
from utils import load_config_bundle, load_harmonization_rules


@pytest.fixture
def rules():
    return deepcopy(load_harmonization_rules(
        load_config_bundle("config/scenarios/notebook_diagnostics.yaml"), "efficiencies",
    ))


def evidence(rules, *, ratings=None, counts=None, atb=None, years=(2021, 2022, 2023),
             reference=2025, region="ON", stock_region=None, mode="cars"):
    ratings = ratings if ratings is not None else {"gasoline": {2021: 10, 2022: 20, 2023: 30, 2025: 0.01}}
    counts = counts if counts is not None else {"gasoline": {2021: 1, 2022: 2, 2023: 7}}
    atb = atb if atb is not None else {"gasoline": {2020: 200, 2023: 150, 2025: 100, 2030: 80}}
    rating_frame = pd.DataFrame([
        {"ceud_class": "Car", "powertrain": powertrain, "year": year,
         "native_class_mean": value, "class_mean_mj_per_vkm": value, "weight": 1}
        for powertrain, annual in ratings.items() for year, value in annual.items()
    ], columns=["ceud_class", "powertrain", "year", "native_class_mean", "class_mean_mj_per_vkm", "weight"])
    atb_frame = pd.DataFrame([
        {"region": region, "mode": mode, "powertrain": powertrain, "year": year,
         "weighted_consumption": value}
        for powertrain, annual in atb.items() for year, value in annual.items()
    ], columns=["region", "mode", "powertrain", "year", "weighted_consumption"])
    loads = pd.DataFrame([
        {"region": region, "mode": mode, "year": year, "load_factor": load}
        for year, load in [(2020, 1), (2021, 1), (2022, 2), (2023, 3)]
    ])
    stock = pd.DataFrame([
        {"region": stock_region or region, "road_class": mode, "powertrain": powertrain,
         "vintage_year": year, "raw_capacity": count}
        for powertrain, annual in counts.items() for year, count in annual.items()
    ], columns=["region", "road_class", "powertrain", "vintage_year", "raw_capacity"])
    return RoadEfficiencyEvidence(
        ceud=pd.DataFrame(), loads=loads, rating_audit=rating_frame, atb_audit=atb_frame,
        gcam=pd.DataFrame(), regen=pd.DataFrame(), base_year=2023, rules=rules,
        projection_years=years, projection_reference_year=reference, cohort_stock=stock,
    )


def test_future_incumbent_uses_unequal_observed_cohorts_and_atb_2025_reference(rules):
    result = evidence(rules)
    anchor = result.projection_anchor("ON", "cars", "gasoline")
    assert anchor["native_intensity"] == pytest.approx(26)
    assert anchor["load_factor"] == pytest.approx(2.6)
    assert anchor["weights"] == {2021: 0.1, 2022: 0.2, 2023: 0.7}
    assert anchor["candidate_years"] == anchor["included_years"] == [2021, 2022, 2023]
    assert anchor["cohort_stock_coverage"] == 1
    assert anchor["atb_reference_year"] == 2025
    assert not anchor["stock_proxy"]
    assert result.ratings.year.max() == 2023
    first = result.derive_consumption("ON", "cars", "gasoline", 2025)
    future = result.derive_consumption("ON", "cars", "gasoline", 2030)
    assert first["native_intensity"] == pytest.approx(26)
    assert future["native_intensity"] == pytest.approx(26 * 80 / 100)
    assert first["load_factor"] == future["load_factor"] == pytest.approx(2.6)
    assert future["projection_reference_year"] == 2025
    assert future["projection_source_years"] == [2021, 2022, 2023]
    assert future["treatment"] == "NRCan_cohort_anchor_ATB_relative_projection"
    assert "historical_index_anchor" not in future
    json.dumps(anchor)


@pytest.mark.parametrize("family,anchor_powertrain,alternative", [
    ("bev", "bev_300_miles", "bev_150_miles"),
    ("phev", "phev_35_miles", "phev_50_miles"),
])
def test_each_range_keeps_own_history_and_future_ranges_use_same_year_atb_ratio(
    rules, family, anchor_powertrain, alternative,
):
    result = evidence(
        rules,
        ratings={anchor_powertrain: {2021: 1, 2022: 2, 2023: 4}, alternative: {2021: 9, 2022: 8, 2023: 7}},
        counts={family: {2021: 1, 2022: 2, 2023: 7}},
        atb={anchor_powertrain: {2025: 100, 2030: 80}, alternative: {2025: 60, 2030: 40}},
    )
    assert result.derive_consumption("ON", "cars", alternative, 2021)["native_intensity"] == 9
    assert result.derive_consumption("ON", "cars", anchor_powertrain, 2021)["native_intensity"] == 1
    assert result.derive_consumption("ON", "cars", alternative, 2023)["native_intensity"] == 7
    anchor = result.projection_anchor("ON", "cars", alternative)
    assert anchor["powertrain"] == anchor_powertrain
    assert anchor["stock_powertrain"] == family
    assert anchor["native_intensity"] == pytest.approx(3.3)
    assert result.derive_consumption("ON", "cars", anchor_powertrain, 2025)["native_intensity"] == pytest.approx(3.3)
    target = result.derive_consumption("ON", "cars", alternative, 2030)
    anchor_future = result.derive_consumption("ON", "cars", anchor_powertrain, 2030)
    assert target["native_intensity"] == pytest.approx(3.3 * 40 / 100)
    assert target["native_intensity"] / anchor_future["native_intensity"] == pytest.approx(40 / 80)
    # Invert only at the service-efficiency seam: this also satisfies the user's
    # efficiency-variant / efficiency-anchor same-year ATB relationship.
    assert (target["load_factor"] / target["native_intensity"]) / (
        anchor_future["load_factor"] / anchor_future["native_intensity"]
    ) == pytest.approx(80 / 40)


def test_ranges_do_not_require_a_common_historical_observation_year(rules):
    result = evidence(
        rules, ratings={"bev_300_miles": {2021: 2, 2023: 1}, "bev_150_miles": {2022: 3}},
        counts={"bev": {2021: 1, 2022: 2, 2023: 7}},
        atb={"bev_300_miles": {2025: 100, 2030: 80}, "bev_150_miles": {2025: 60, 2030: 40}},
    )
    anchor_history = result.period_observations("ON", "cars", "bev_300_miles", [2021, 2022, 2023])
    other_history = result.period_observations("ON", "cars", "bev_150_miles", [2021, 2022, 2023])
    assert set(anchor_history) == {2021, 2023}
    assert set(other_history) == {2022}
    assert other_history[2022]["native_intensity"] == 3


def test_sparse_ratings_renormalize_only_observed_cohorts_and_expose_coverage(rules):
    result = evidence(rules, ratings={"gasoline": {2021: 10, 2023: 30}})
    anchor = result.projection_anchor("ON", "cars", "gasoline")
    assert anchor["weights"] == {2021: 0.125, 2023: 0.875}
    assert anchor["cohort_stock_coverage"] == pytest.approx(0.8)
    assert anchor["native_intensity"] == pytest.approx(27.5)
    assert anchor["load_factor"] == pytest.approx(2.75)
    assert set(anchor["observations"]) == {2021, 2023}
    assert all(row["treatment"] == "NRCan_observed_class_weighted" for row in anchor["observations"].values())
    assert any("projection_cohort_stock_coverage=" in finding[-1] for finding in result.findings)


def test_wholly_empty_rating_bin_uses_explicit_existing_analogue_policy(rules):
    result = evidence(
        rules, ratings={"diesel": {2020: 10}, "gasoline": {2020: 10, 2021: 12, 2022: 14, 2023: 16}},
        counts={"diesel": {2021: 1, 2022: 2, 2023: 7}},
        atb={"diesel": {2025: 100, 2030: 80}},
    )
    assert result.period_observations("ON", "cars", "diesel", [2021, 2022, 2023]) == {}
    anchor = result.projection_anchor("ON", "cars", "diesel")
    assert anchor["native_intensity"] == pytest.approx(15.2)
    assert anchor["stock_powertrain"] == "diesel"
    assert {row["analogue"] for row in anchor["observations"].values()} == {"gasoline"}
    assert {row["treatment"] for row in anchor["observations"].values()} == {"NRCan_missing_year_approved_analogue_index"}


def test_missing_own_stock_uses_configured_analogue_counts_and_reports_proxy(rules):
    result = evidence(
        rules, ratings={"fcev": {2021: 2, 2022: 3, 2023: 4}},
        atb={"fcev": {2025: 100, 2030: 80}},
    )
    anchor = result.projection_anchor("ON", "cars", "fcev")
    assert anchor["stock_powertrain"] == "gasoline"
    assert anchor["stock_proxy"]
    assert anchor["native_intensity"] == pytest.approx(3.6)
    assert any("projection_cohort_stock_analogue=gasoline" in finding[-1] for finding in result.findings)
    assert result.derive_consumption("ON", "cars", "fcev", 2030)["projection_stock_proxy"]


def test_missing_all_stock_never_silently_equal_weights(rules):
    result = evidence(rules, counts={"gasoline": {2021: 0, 2022: 0, 2023: 0}})
    with pytest.raises(ValueError, match="No positive own or configured-analogue cohort stock"):
        result.derive_consumption("ON", "cars", "gasoline", 2025)


def test_exhausted_observation_coverage_has_a_narrow_diagnostic_signal(rules):
    result = evidence(rules, ratings={})
    with pytest.raises(ValueError, match="^No observed or approved-proxy LDV projection anchor:"):
        result.projection_anchor("ON", "cars", "gasoline")


def test_positive_stock_without_rating_overlap_does_not_switch_weight_source(rules):
    result = evidence(rules, ratings={"diesel": {2021: 10, 2023: 30}},
                      counts={"diesel": {2022: 8}, "gasoline": {2021: 1, 2023: 7}},
                      atb={"diesel": {2025: 100, 2030: 80}})
    with pytest.raises(ValueError, match="No overlap"):
        result.projection_anchor("ON", "cars", "diesel")


def test_stock_region_aliases_follow_the_existing_output_mapping(rules):
    result = evidence(rules, region="NL", stock_region="NLLAB")
    assert result.projection_anchor("NL", "cars", "gasoline")["native_intensity"] == pytest.approx(26)


@pytest.mark.parametrize("years,reference", [((), 2025), ((2021, 2021), 2025), ((2023, 2024), 2025), ((2023,), None), ((2023,), 2023)])
def test_future_requires_explicit_observed_cohort_window_and_model_reference(rules, years, reference):
    result = evidence(rules, years=years, reference=reference)
    assert result.derive_consumption("ON", "cars", "gasoline", 2023)["native_intensity"] == 30
    with pytest.raises(ValueError, match="LDV projection requires"):
        result.derive_consumption("ON", "cars", "gasoline", 2025)


@pytest.mark.parametrize("count", [-1, float("nan"), float("inf")])
def test_invalid_stock_evidence_is_rejected_before_weighting(rules, count):
    result = evidence(rules, counts={"gasoline": {2021: count, 2022: 2, 2023: 7}})
    with pytest.raises(ValueError, match="projection.*stock|projection cohort stock"):
        result.projection_anchor("ON", "cars", "gasoline")


def test_future_mhdv_requires_existing_fleet_anchor_instead_of_absolute_atb(rules):
    result = evidence(rules, mode="medium_trucks", ratings={}, counts={},
                      atb={"diesel": {2030: 2}}, years=(), reference=None)
    with pytest.raises(ValueError, match="MD/HD projection requires"):
        result.derive_consumption("ON", "medium_trucks", "diesel", 2030)


@pytest.mark.parametrize("key", ["ldv_projection_baseline", "ldv_projection_reference", "ldv_future_load_factor", "projection_missing_stock"])
def test_projection_policies_cannot_silently_select_an_unsupported_method(rules, key):
    rules[key] = "unsupported"
    with pytest.raises(ValueError, match=key):
        evidence(rules)
