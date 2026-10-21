"""Distinct-model NRCan statistics and their explicit scenario selection."""

from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from parameterization.road_efficiencies import (
    aggregate_ratings,
    distinct_rating_models,
    rating_class_statistics,
)
from utils import load_config_bundle
from validation.config_models import ScenarioEfficiencies


@pytest.fixture
def rules():
    return {
        "ratings": {"fields": {"make": "Make", "model": "Model"}},
        "historical_suv": {
            "source_label": "Sport utility vehicle",
            "combined_weight_classes": ["Small SUV", "Standard SUV"],
        },
        "tolerances": {"weight_sum": 1e-9},
    }


@pytest.fixture
def ratings():
    return pd.DataFrame([
        {
            "included": True, "source_row": row, "year": 2023,
            "rating_class": "Compact", "powertrain": "bev_200_miles",
            "native_unit": "kWh/100 km", "Make": "Example", "Model": model,
            "native_consumption": value, "consumption_mj_per_vkm": value * 0.036,
            "range_km": distance,
        }
        for row, model, value, distance in [
            (1, "A", 4, 100), (2, "A", 8, 200),
            (3, "B", 10, 300), (4, "C", 20, 600),
        ]
    ])


@pytest.mark.parametrize("statistic,consumption,distance", [("mean", 12, 350), ("median", 10, 300)])
def test_distinct_models_receive_equal_influence_after_variant_means(
    ratings, rules, statistic, consumption, distance,
):
    original = ratings.copy(deep=True)
    models = distinct_rating_models(ratings, rules=rules).set_index("Model")
    assert models.native_consumption.to_dict() == {"A": 6, "B": 10, "C": 20}
    assert models.catalogue_rows.to_dict() == {"A": 2, "B": 1, "C": 1}
    assert models.loc["A", "range_km"] == 150
    annual = rating_class_statistics(ratings, rules=rules, statistic=statistic).iloc[0]
    assert annual.native_consumption == consumption
    assert annual.consumption_mj_per_vkm == pytest.approx(consumption * 0.036)
    assert annual.range_km == distance
    assert annual.observations == 4
    assert annual.distinct_models == 3
    assert annual.rating_statistic == statistic
    pd.testing.assert_frame_equal(ratings, original)


@pytest.mark.parametrize("statistic", ["mean", "median"])
def test_repeating_one_models_catalogue_does_not_increase_its_class_weight(ratings, rules, statistic):
    original = rating_class_statistics(ratings, rules=rules, statistic=statistic)
    repeated = pd.concat([ratings, ratings.loc[ratings.Model.eq("A")]], ignore_index=True)
    result = rating_class_statistics(repeated, rules=rules, statistic=statistic)
    assert result.observations.item() == 6
    pd.testing.assert_frame_equal(result.drop(columns="observations"), original.drop(columns="observations"))


def test_model_identity_stays_within_exact_year_class_powertrain_and_name(ratings, rules):
    variants = pd.concat([
        ratings,
        ratings.iloc[[0]].assign(year=2022),
        ratings.iloc[[0]].assign(rating_class="Midsize"),
        ratings.iloc[[0]].assign(powertrain="bev_300_miles"),
        ratings.iloc[[0]].assign(Model="a"),
        ratings.iloc[[0]].assign(Make="Another make"),
    ], ignore_index=True)
    models = distinct_rating_models(variants, rules=rules)
    assert len(models) == 8
    assert models.catalogue_rows.sum() == len(variants)


@pytest.mark.parametrize("field", ["Make", "Model"])
@pytest.mark.parametrize("missing", [None, "", "   "])
def test_missing_model_identity_fails_instead_of_falling_back_to_catalogue_rows(ratings, rules, field, missing):
    ratings.loc[0, field] = missing
    with pytest.raises(ValueError, match="nonblank make and model identities"):
        distinct_rating_models(ratings, rules=rules)
    ratings.loc[0, "included"] = False
    assert distinct_rating_models(ratings, rules=rules).catalogue_rows.sum() == 3


def test_missing_identity_column_and_mixed_units_fail(ratings, rules):
    with pytest.raises(ValueError, match="missing columns.*Make"):
        distinct_rating_models(ratings.drop(columns="Make"), rules=rules)
    ratings.loc[0, "native_unit"] = "MJ/km"
    with pytest.raises(ValueError, match="Conflicting native units"):
        distinct_rating_models(ratings, rules=rules)


def test_identity_fields_follow_configured_source_names(ratings, rules):
    rules["ratings"]["fields"] = {"make": "Manufacturer", "model": "Name"}
    result = distinct_rating_models(
        ratings.rename(columns={"Make": "Manufacturer", "Model": "Name"}), rules=rules,
    )
    assert result.Name.tolist() == ["A", "B", "C"]


@pytest.mark.parametrize("statistic,consumption", [("mean", 12), ("median", 10)])
def test_optional_phev_components_follow_the_same_distinct_model_statistic(ratings, rules, statistic, consumption):
    rows = ratings.assign(
        powertrain="phev_35_miles", native_unit="Le/100 km (CD; index only)",
        consumption_mj_per_vkm=float("nan"),
        source_cd=ratings.native_consumption, source_cs=2 * ratings.native_consumption,
    ).drop(columns="range_km")
    result = rating_class_statistics(
        rows, rules=rules, statistic=statistic, value_columns=("source_cd", "source_cs"),
    ).iloc[0]
    assert result.source_cd == consumption
    assert result.source_cs == 2 * consumption
    assert pd.isna(result.consumption_mj_per_vkm)
    assert pd.isna(result.range_km)


@pytest.mark.parametrize("statistic,expected", [("mean", 16.5), ("median", 15)])
def test_class_weights_apply_after_the_selected_model_statistic(ratings, rules, statistic, expected):
    ratings = pd.concat([ratings, ratings.iloc[[0]].assign(
        rating_class="Midsize", native_consumption=30,
        consumption_mj_per_vkm=1.08, range_km=900,
    )], ignore_index=True)
    weights = pd.DataFrame([
        {"report_year": 2025, "weight_basis": "all_vintages", "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": size, "aggregation_weight": share}
        for size, share in [("Compact", 0.75), ("Midsize", 0.25)]
    ])
    result = aggregate_ratings(ratings, weights, rules=rules, weight_basis="all_vintages", statistic=statistic)
    assert (result.native_class_mean * result.weight).sum() == pytest.approx(expected)
    assert result.observations.sum() == 5
    assert result.distinct_models.sum() == 4
    assert set(result.rating_statistic) == {statistic}
    assert set(result.aggregation_method) == {"distinct_model_class_weighted"}
    missing_class = aggregate_ratings(
        ratings.loc[ratings.rating_class.eq("Compact")], weights,
        rules=rules, weight_basis="all_vintages", statistic=statistic,
    ).iloc[0]
    assert missing_class.original_weight == 0.75
    assert missing_class.weight == 1


def test_rating_statistic_requires_an_explicit_supported_selection(ratings, rules):
    assert ScenarioEfficiencies(atb_trajectory="Mid", rating_statistic="mean").rating_statistic == "mean"
    assert ScenarioEfficiencies(atb_trajectory="Mid", rating_statistic="median").rating_statistic == "median"
    with pytest.raises(ValidationError, match="rating_statistic"):
        ScenarioEfficiencies(atb_trajectory="Mid")
    with pytest.raises(ValidationError, match="rating_statistic"):
        ScenarioEfficiencies(atb_trajectory="Mid", rating_statistic="mode")
    with pytest.raises(ValueError, match="Unsupported distinct-model rating statistic"):
        rating_class_statistics(ratings, rules=rules, statistic="mode")


@pytest.mark.parametrize("scenario", ["default", "legacy_comparison", "notebook_diagnostics"])
def test_versioned_scenarios_explicitly_select_the_promoted_model_mean(scenario):
    bundle = load_config_bundle(
        f"config/scenarios/{scenario}.yaml", repo_root=Path(__file__).resolve().parents[1],
    )
    assert bundle.scenario.efficiencies.rating_statistic == "mean"
