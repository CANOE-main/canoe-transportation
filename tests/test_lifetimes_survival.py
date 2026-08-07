import pandas as pd
import pytest

from parameterization.lifetimes_survival import (
    aggregate_mto_survival_stages,
    aggregate_report_a_snapshots,
    cohort_transition_observations,
    legacy_wards_survival_curves,
    median_equivalent_lifetimes,
    mto_key_transition_observations,
    pool_cohort_retention,
    raw_mto_key_snapshots,
    retention_source_comparison,
    transform_source_survival_curves,
)


def test_report_a_snapshots_bucket_passenger_status_and_collapse_commercial() -> None:
    status_columns = [
        "FIT_ACTIVE",
        "FIT_INACTIVE",
        "UNFIT",
        "WRECKED",
        "OUT_OF_PROV",
        "SOLD",
        "SUSPENDED",
        "TEMPORARY",
    ]
    normalized = pd.DataFrame(
        {
            "report_year": [2025, 2025, 2025],
            "VEHICLE_CLASS": ["PASSENGER", "COMMERCIAL", "COMMERCIAL"],
            "MAKE": ["HOND", "FORD", "RAM"],
            "MODEL": ["CRV", "COF", "RTR"],
            "MODEL_YEAR": [2020, 2020, 2020],
            "FIT_ACTIVE": [100, 200, 300],
            "FIT_INACTIVE": [10, 20, 30],
            "UNFIT": [1, 2, 3],
            "WRECKED": [2, 3, 4],
            "OUT_OF_PROV": [3, 4, 5],
            "SOLD": [4, 5, 6],
            "SUSPENDED": [5, 6, 7],
            "TEMPORARY": [6, 7, 8],
        }
    )
    mapping = pd.DataFrame(
        {
            "entry_type": ["mto_crosswalk"] * 3,
            "mto_make_code": ["HOND", "FORD", "RAM"],
            "mto_model_code": ["CRV", "COF", "RTR"],
            "model_year_from": [2015] * 3,
            "model_year_to": [2025] * 3,
            "canonical_make": ["Honda", "Ford", "Ram"],
            "canonical_model": ["CR-V", "F-150", "1500"],
            "nrcan_vehicle_class": [
                "Sport utility vehicle: Small",
                "Pickup truck: Standard",
                "Pickup truck: Standard",
            ],
            "nlr_atb_class": ["Small SUV", "Pickup", "Pickup"],
            "nrcan_ceud_class": ["Light Truck"] * 3,
            "match_method": ["fixture"] * 3,
            "mapping_status": ["reviewed"] * 3,
            "evidence_source": ["fixture"] * 3,
            "supporting_rating_rows": [1] * 3,
            "supporting_model_labels": ["CR-V", "F-150", "1500"],
            "review_notes": ["fixture"] * 3,
        }
    )

    passenger, commercial = aggregate_report_a_snapshots(
        [normalized],
        mapping,
        accepted_statuses={"reviewed"},
        status_columns=status_columns,
        status_buckets={
            "FIT_ACTIVE": ["FIT_ACTIVE"],
            "NON_FIT_ACTIVE_PROXY": status_columns[1:],
        },
    )

    passenger_counts = passenger.set_index("stock_status")[
        "cohort_count"
    ].to_dict()
    assert passenger_counts == {
        "FIT_ACTIVE": 100,
        "NON_FIT_ACTIVE_PROXY": 31,
    }
    assert commercial["cohort_class"].unique().tolist() == ["Pickup"]
    assert commercial["population_group"].unique().tolist() == [
        "mapped_commercial"
    ]
    assert commercial.loc[
        commercial["stock_status"].eq("FIT_ACTIVE"),
        "cohort_count",
    ].item() == 500


def cohort_snapshots() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "population_group": ["mapped_passenger"] * 6,
            "report_year": [2020, 2021, 2021, 2022, 2020, 2021],
            "model_year": [2018, 2018, 2019, 2019, 2020, 2021],
            "cohort_class": ["Car"] * 6,
            "stock_status": ["FIT_ACTIVE"] * 6,
            "cohort_count": [100, 90, 50, 100, 0, 20],
        }
    )


def test_consecutive_cohort_pairing_preserves_ratios_above_one_and_zero_denominator() -> None:
    observations, missing = cohort_transition_observations(cohort_snapshots())

    assert missing == []
    growing = observations.loc[
        observations["report_year"].eq(2021)
        & observations["model_year"].eq(2019)
    ].iloc[0]
    assert growing["apparent_retention"] == pytest.approx(2.0)
    assert growing["apparent_retirement"] == pytest.approx(-1.0)
    assert bool(growing["retention_above_one"])
    zero = observations.loc[
        observations["report_year"].eq(2020)
        & observations["model_year"].eq(2021)
    ].iloc[0]
    assert bool(zero["zero_denominator"])
    assert pd.isna(zero["apparent_retention"])


def test_missing_annual_snapshot_is_reported_and_not_paired_across_gap() -> None:
    snapshots = cohort_snapshots()
    snapshots = snapshots.loc[~snapshots["report_year"].eq(2021)]

    observations, missing = cohort_transition_observations(snapshots)

    assert missing == [2021]
    assert observations.empty


def test_intentionally_excluded_snapshot_is_not_reported_as_missing() -> None:
    snapshots = cohort_snapshots()
    snapshots = snapshots.loc[~snapshots["report_year"].eq(2021)]

    observations, missing = cohort_transition_observations(
        snapshots,
        ignored_missing_years={2021},
    )

    assert missing == []
    assert observations.empty


def test_pooling_uses_cohort_totals_not_percentage_average() -> None:
    observations, _ = cohort_transition_observations(cohort_snapshots())

    pooled = pool_cohort_retention(observations)
    age_two = pooled.loc[
        pooled["cohort_class"].eq("Car")
        & pooled["stock_status"].eq("FIT_ACTIVE")
        & pooled["age"].eq(2)
    ].iloc[0]

    assert age_two["cohort_count_t_sum"] == 150
    assert age_two["cohort_count_t1_sum"] == 190
    assert age_two["apparent_retention_pooled"] == pytest.approx(190 / 150)
    assert age_two["apparent_retention_pooled"] != pytest.approx((0.9 + 2.0) / 2)


def test_raw_key_retention_precedes_nlr_and_ceud_pooling() -> None:
    frames = [
        pd.DataFrame(
            {
                "report_year": [2020, 2020],
                "VEHICLE_CLASS": ["PASSENGER", "PASSENGER"],
                "MAKE": ["FORD", "****"],
                "MODEL": ["FOC", "UNK"],
                "MODEL_YEAR": [2015, 2015],
                "FIT_ACTIVE": [100, 999],
            }
        ),
        pd.DataFrame(
            {
                "report_year": [2021],
                "VEHICLE_CLASS": ["PASSENGER"],
                "MAKE": ["FORD"],
                "MODEL": ["FOC"],
                "MODEL_YEAR": [2015],
                "FIT_ACTIVE": [80],
            }
        ),
    ]
    snapshots = raw_mto_key_snapshots(
        frames,
        passenger_class="PASSENGER",
        commercial_class="COMMERCIAL",
        minimum_model_year=2000,
        suppressed_code_patterns=[r"^\*+$"],
        unknown_code_labels=["UNKNOWN", "UNK", "N/A"],
    )
    observations, missing_years = mto_key_transition_observations(snapshots)

    assert missing_years == []
    assert set(snapshots["mto_make_code"]) == {"FORD"}
    assert observations.loc[0, "age"] == 5
    assert observations.loc[0, "apparent_retention"] == pytest.approx(0.8)

    mapped = observations.assign(
        nlr_atb_class="Compact",
        nrcan_ceud_class="Car",
        mapping_accepted=True,
    )
    nlr_vintage, nlr_class, ceud_vintage, ceud_class = (
        aggregate_mto_survival_stages(mapped)
    )

    assert nlr_vintage.loc[0, "model_year"] == 2015
    assert nlr_class.loc[0, "apparent_retention_pooled"] == pytest.approx(0.8)
    assert ceud_vintage.loc[0, "apparent_retention_pooled"] == pytest.approx(0.8)
    observed_ceud = ceud_class.loc[
        ceud_class["annual_survival_factor"].notna()
    ].iloc[0]
    assert observed_ceud["annual_survival_factor"] == pytest.approx(0.8)
    assert list(ceud_class.columns) == [
        "vehicle_class",
        "age",
        "annual_retirement_rate",
        "annual_survival_factor",
        "cumulative_survival",
        "cumulative_scrappage",
        "fit_active_exposure",
        "number_of_vintages",
        "number_of_transitions",
    ]


def test_raw_transition_estimator_requires_both_endpoints_and_age_two() -> None:
    snapshots = pd.DataFrame(
        {
            "population_group": ["raw_passenger"] * 6,
            "vehicle_class": ["PASSENGER"] * 6,
            "mto_make_code": ["HOND"] * 6,
            "mto_model_code": ["CIV", "NEW", "OLD", "CIV", "NEW", "GONE"],
            "model_year": [2018, 2020, 2015, 2018, 2020, 2014],
            "stock_status": ["FIT_ACTIVE"] * 6,
            "report_year": [2020, 2020, 2020, 2021, 2021, 2021],
            "cohort_count": [100, 10, 20, 110, 20, 30],
        }
    )

    observations, missing = mto_key_transition_observations(snapshots)

    assert missing == []
    assert observations[["mto_model_code", "age"]].to_records(
        index=False
    ).tolist() == [("CIV", 2)]
    row = observations.iloc[0]
    assert row["apparent_retirements"] == -10
    assert row["annual_survival_factor"] == pytest.approx(1.1)
    assert row["annual_retirement_rate"] == pytest.approx(-0.1)


def test_raw_transition_estimator_rejects_proxy_input() -> None:
    snapshots = pd.DataFrame(
        {
            "population_group": ["raw_passenger"],
            "vehicle_class": ["PASSENGER"],
            "mto_make_code": ["HOND"],
            "mto_model_code": ["CIV"],
            "model_year": [2018],
            "stock_status": ["NON_FIT_ACTIVE_PROXY"],
            "report_year": [2020],
            "cohort_count": [100],
        }
    )

    with pytest.raises(AssertionError, match="NON_FIT_ACTIVE_PROXY"):
        mto_key_transition_observations(snapshots)


def test_eia_scrappage_rates_convert_to_cumulative_survival() -> None:
    nhtsa = pd.DataFrame(
        {
            "source_id": ["nhtsa_cafe_2024_ldv_survival"] * 2,
            "source_vehicle_class_label": ["Cars", "Cars"],
            "vehicle_age": [0, 1],
            "survival_rate": [1.0, 0.9],
            "unit": ["dimensionless", "dimensionless"],
        }
    )
    eia = pd.DataFrame(
        {
            "source_id": ["eia_nems_hd_truck_scrappage"] * 2,
            "source_vehicle_class_label": ["Cls 3", "Cls 3"],
            "vehicle_age": [1, 2],
            "annual_scrappage_rate": [0.1, 0.2],
            "unit": ["fraction", "fraction"],
        }
    )

    source, transformed = transform_source_survival_curves(nhtsa, eia)

    assert set(source["source_measure"]) == {
        "survival_probability",
        "annual_scrappage_rate",
    }
    class_three = transformed.loc[
        transformed["source_class"].eq("Cls 3")
    ].sort_values("age")
    assert class_three["survival_probability"].tolist() == pytest.approx(
        [1.0, 0.9, 0.72]
    )


def test_retention_comparison_keeps_mto_nhtsa_and_eia_meanings() -> None:
    pooled = pd.DataFrame(
        {
            "population_group": ["mapped_commercial"],
            "cohort_class": ["Pickup"],
            "stock_status": ["FIT_ACTIVE"],
            "age": [10],
            "cohort_count_t_sum": [1000],
            "cohort_count_t1_sum": [920],
            "observation_count": [4],
        }
    )
    source = pd.DataFrame(
        {
            "source_id": [
                "nhtsa_cafe_2024_ldv_survival",
                "nhtsa_cafe_2024_ldv_survival",
                "eia_nems_hd_truck_scrappage",
            ],
            "source_class": ["Pickups", "Pickups", "Cls 3"],
            "age": [10, 11, 10],
            "source_measure": [
                "survival_probability",
                "survival_probability",
                "annual_scrappage_rate",
            ],
            "source_value": [0.8, 0.72, 0.12],
        }
    )

    comparison = retention_source_comparison(
        pooled,
        source,
        eia_classes=["Cls 3"],
    )

    assert sorted(comparison["one_year_retention"].tolist()) == pytest.approx(
        [0.88, 0.9, 0.92]
    )
    assert set(comparison["series_family"]) == {
        "eia_nems_hd_truck_scrappage",
        "nhtsa_cafe_2024_ldv_survival",
        "ontario_report_a",
    }


def test_legacy_light_truck_curve_uses_latest_wards_weights() -> None:
    transformed = pd.DataFrame(
        {
            "source_id": ["nhtsa_cafe_2024_ldv_survival"] * 6,
            "source_class": ["Cars", "Cars", "Vans/SUVs", "Vans/SUVs", "Pickups", "Pickups"],
            "age": [0, 1, 0, 1, 0, 1],
            "source_measure": ["survival_probability"] * 6,
            "survival_probability": [1.0, 0.9, 1.0, 0.8, 1.0, 0.6],
            "source_unit": ["dimensionless"] * 6,
            "transformation": ["source"] * 6,
        }
    )
    wards = pd.DataFrame(
        {
            "year": [2021, 2021, 2021],
            "nlr_atb_class": ["Small SUV", "Midsize SUV", "Pickup"],
            "market_share": [0.5, 0.2, 0.3],
        }
    )
    rules = {
        "legacy_survival": {
            "car_source_class": "Cars",
            "light_truck_source_classes": {
                "Vans/SUVs": ["Small SUV", "Midsize SUV"],
                "Pickups": ["Pickup"],
            },
        }
    }

    curves = legacy_wards_survival_curves(transformed, wards, rules=rules)

    light_age_one = curves.loc[
        curves["source_class"].eq("Light Truck") & curves["age"].eq(1),
        "survival_probability",
    ].item()
    assert light_age_one == pytest.approx(0.8 * 0.7 + 0.6 * 0.3)


def test_median_equivalent_age_is_first_age_at_or_below_half() -> None:
    curves = pd.DataFrame(
        {
            "source_id": ["fixture"] * 4,
            "source_class": ["Cars"] * 4,
            "age": [0, 1, 2, 3],
            "survival_probability": [1.0, 0.8, 0.5, 0.3],
        }
    )

    median = median_equivalent_lifetimes(curves, interpolation="none")

    assert median.loc[0, "median_equivalent_age"] == 2
    assert median.loc[0, "interpolation_method"] == "none"
