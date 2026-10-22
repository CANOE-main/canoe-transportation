"""Catalogue outlier decisions use whole make/model cohorts without pooling years."""

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from parameterization.road_efficiencies import apply_rating_outlier_filter
from utils import load_config_bundle, load_harmonization_rules


@pytest.fixture
def rules():
    return deepcopy(load_harmonization_rules(
        load_config_bundle("config/scenarios/notebook_diagnostics.yaml"), "efficiencies",
    ))


def catalogue(values, **overrides):
    return pd.DataFrame([
        {"year": 2023, "rating_class": "Compact", "powertrain": "gasoline",
         "native_unit": "L/100 km", "Make": "Fixture", "Model": f"M{index}",
         "included": True, "native_consumption": 2.0, "consumption_mj_per_vkm": value,
         "source_row": index + 2, **overrides}
        for index, value in enumerate(values)
    ])


def test_symmetric_fences_use_variant_means_and_retain_native_rows(rules):
    source = catalogue([.5, 5, 6, .1, 8, 9, 18])
    variants = source.loc[source.Model.isin(["M3", "M6"])].copy()
    variants["consumption_mj_per_vkm"] = [13.9, 22]
    variants["source_row"] += 10
    source = pd.concat([source, variants], ignore_index=True)
    source.index = range(20, 29)
    original = source.copy(deep=True)
    result = apply_rating_outlier_filter(source, rules=rules)
    pd.testing.assert_frame_equal(source, original)
    pd.testing.assert_frame_equal(result[source.columns.drop("included")], source.drop(columns="included"))
    assert result.outlier_cohort_model_count.eq(7).all()
    assert result.outlier_q1_mj_per_vkm.eq(5.5).all()
    assert result.outlier_q3_mj_per_vkm.eq(8.5).all()
    assert result.outlier_lower_fence_mj_per_vkm.eq(1).all()
    assert result.outlier_upper_fence_mj_per_vkm.eq(13).all()
    assert set(result.loc[result.outlier_excluded, "Model"]) == {"M0", "M6"}
    assert result.loc[result.Model.eq("M6"), "outlier_model_mean_mj_per_vkm"].eq(20).all()
    assert result.loc[result.Model.eq("M3"), "included"].all()
    assert result.loc[result.Model.eq("M3"), "outlier_model_mean_mj_per_vkm"].eq(7).all()
    assert result.groupby("Model").outlier_excluded.nunique().eq(1).all()
    assert set(result.loc[result.outlier_excluded, "filter_reason"]) == {"below_lower_fence", "above_upper_fence"}
    assert result.loc[~result.outlier_excluded, "filter_reason"].eq("").all()


def test_exact_fence_values_are_retained(rules):
    result = apply_rating_outlier_filter(catalogue([1, 5, 6, 7, 8, 9, 13]), rules=rules)
    assert result.included.all()
    assert result.outlier_lower_fence_mj_per_vkm.eq(1).all()
    assert result.outlier_upper_fence_mj_per_vkm.eq(13).all()


def test_filter_is_single_pass_not_recursive_tail_trimming(rules):
    result = apply_rating_outlier_filter(catalogue([5, 5.1, 5.2, 5.3, 7, 9]), rules=rules)
    assert result.loc[result.Model.eq("M4"), "included"].item()
    assert result.loc[result.outlier_excluded, "Model"].tolist() == ["M5"]
    assert result.outlier_cohort_model_count.eq(6).all()


@pytest.mark.parametrize("count", [1, 2, 3, 4])
def test_sparse_cohort_retains_every_model_and_reports_status(rules, count):
    result = apply_rating_outlier_filter(catalogue([100] + list(range(1, count))), rules=rules)
    assert result.included.all()
    assert not result.outlier_excluded.any()
    assert result.outlier_cohort_model_count.eq(count).all()
    assert result.outlier_status.eq("insufficient_models").all()


def test_zero_iqr_retains_whole_cohort_without_alternative_cutoff(rules):
    result = apply_rating_outlier_filter(catalogue([5, 5, 5, 5, 5, 5, 100]), rules=rules)
    assert result.included.all()
    assert result.outlier_status.eq("zero_iqr").all()
    assert result.outlier_q1_mj_per_vkm.eq(result.outlier_q3_mj_per_vkm).all()


@pytest.mark.parametrize("field,value", [
    ("year", 2022), ("rating_class", "Mid-size"),
    ("powertrain", "bev_300_miles"), ("native_unit", "kWh/100 km"),
])
def test_filter_never_pools_separate_cohort_fields(rules, field, value):
    first = catalogue([.1, 5, 6, 100])
    second = catalogue([.1, 5, 6, 100], **{field: value})
    second["Model"] += " second"
    result = apply_rating_outlier_filter(pd.concat([first, second], ignore_index=True), rules=rules)
    assert result.included.all()
    assert result.outlier_cohort_model_count.eq(4).all()
    assert result.outlier_status.eq("insufficient_models").all()


def test_phev_filter_uses_total_energy_not_displayed_cd_equivalent(rules):
    result = apply_rating_outlier_filter(
        catalogue([.5, 5, 6, 7, 8, 9, 20], powertrain="phev_35_miles", native_unit="Le/100 km (CD)"),
        rules=rules,
    )
    assert result.native_consumption.eq(2).all()
    assert result.loc[result.outlier_excluded, "Model"].tolist() == ["M0", "M6"]


def test_configured_native_only_cng_audit_is_retained_without_energy_screening(rules):
    source = catalogue([np.nan], powertrain="cng")
    result = apply_rating_outlier_filter(source, rules=rules).iloc[0]
    assert result.included_before_filter and result.included
    assert not result.outlier_excluded
    assert result.outlier_status == "unavailable_energy_metric"
    assert pd.isna(result.outlier_model_mean_mj_per_vkm)
    assert pd.isna(result.outlier_cohort_model_count)
    assert result.native_consumption == 2


@pytest.mark.parametrize("value", [0, -1, np.inf, -np.inf])
def test_cng_native_only_exception_does_not_accept_invalid_numeric_energy(rules, value):
    with pytest.raises(ValueError, match="finite positive eligible total consumption"):
        apply_rating_outlier_filter(catalogue([value], powertrain="cng"), rules=rules)


def test_native_only_exception_requires_explicit_configured_powertrain(rules):
    rules["ratings"]["outlier_filter"]["unconverted_powertrains"] = []
    with pytest.raises(ValueError, match="finite positive eligible total consumption"):
        apply_rating_outlier_filter(catalogue([np.nan], powertrain="cng"), rules=rules)


def test_preexisting_exclusions_do_not_affect_model_count_or_fences(rules):
    source = catalogue([.5, 5, 6, 7, 8, 9, 20, np.nan])
    source.loc[7, "included"] = False
    result = apply_rating_outlier_filter(source, rules=rules)
    assert result.loc[:6, "outlier_cohort_model_count"].eq(7).all()
    excluded = result.loc[7]
    assert not excluded.included_before_filter and not excluded.included
    assert not excluded.outlier_excluded
    assert excluded.outlier_status == "not_eligible"
    assert pd.isna(excluded.outlier_cohort_model_count)


def test_no_eligible_rows_is_an_explicit_empty_filter(rules):
    result = apply_rating_outlier_filter(catalogue([np.nan], included=False), rules=rules)
    assert not result.included.any()
    assert not result.outlier_excluded.any()
    assert result.outlier_status.eq("not_eligible").all()


@pytest.mark.parametrize("invalid", [0, -1, np.nan, np.inf, -np.inf])
def test_invalid_eligible_total_energy_fails(rules, invalid):
    with pytest.raises(ValueError, match="finite positive eligible total consumption"):
        apply_rating_outlier_filter(catalogue([invalid]), rules=rules)


@pytest.mark.parametrize("field", ["Make", "Model", "rating_class", "powertrain", "native_unit"])
def test_empty_eligible_identity_or_cohort_key_fails(rules, field):
    source = catalogue([5], **{field: " "})
    with pytest.raises(ValueError, match="nonblank eligible"):
        apply_rating_outlier_filter(source, rules=rules)


@pytest.mark.parametrize("field,value", [
    ("method", "z_score"), ("metric", "native_consumption"), ("variant_aggregation", "median"),
    ("cohort_fields", ["year"]), ("tails", "upper"), ("quartile_interpolation", "nearest"),
    ("insufficient_support", "pool_years"), ("zero_iqr", "drop_nonmedian"),
    ("missing_energy_metric", "skip_any_nan"),
])
def test_unsupported_policy_is_not_silently_replaced(rules, field, value):
    rules["ratings"]["outlier_filter"][field] = value
    with pytest.raises(ValueError, match="Unsupported NRCan outlier_filter"):
        apply_rating_outlier_filter(catalogue([5]), rules=rules)


@pytest.mark.parametrize("minimum", [True, 0, -1, 5.5, "5", None])
def test_minimum_model_count_requires_positive_integer(rules, minimum):
    rules["ratings"]["outlier_filter"]["minimum_models"] = minimum
    with pytest.raises(ValueError, match="positive integer"):
        apply_rating_outlier_filter(catalogue([5]), rules=rules)


@pytest.mark.parametrize("multiplier", [True, 0, -1, np.nan, np.inf, "1.5", None])
def test_iqr_multiplier_requires_finite_positive_number(rules, multiplier):
    rules["ratings"]["outlier_filter"]["iqr_multiplier"] = multiplier
    with pytest.raises(ValueError, match="finite and positive"):
        apply_rating_outlier_filter(catalogue([5]), rules=rules)


@pytest.mark.parametrize("unconverted", [None, "cng", ["cng", "cng"], [None], ["unknown"]])
def test_unconverted_powertrain_exception_requires_explicit_distinct_known_names(rules, unconverted):
    rules["ratings"]["outlier_filter"]["unconverted_powertrains"] = unconverted
    with pytest.raises(ValueError, match="distinct configured powertrains"):
        apply_rating_outlier_filter(catalogue([5]), rules=rules)
