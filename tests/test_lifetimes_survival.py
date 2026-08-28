from pathlib import Path

import pandas as pd
import pytest

import parameterization.lifetimes_survival as lifetime_module
from parameterization.lifetimes_survival import (
    build_accepted_lifetime_artifacts,
    build_mto_survival_diagnostic_artifacts,
    aggregate_mto_survival_stages as _aggregate_mto_survival_stages,
    aggregate_report_a_snapshots,
    annotate_latest_snapshot_presence,
    cohort_transition_observations,
    legacy_wards_survival_curves,
    median_equivalent_lifetimes,
    mto_key_transition_observations,
    mto_survival_scope_comparison as _mto_survival_scope_comparison,
    pool_cohort_retention,
    parse_args,
    raw_mto_key_snapshots,
    retention_source_comparison,
    transform_source_survival_curves,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def _lifetime_rules() -> dict[str, object]:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return dict(lifetime_module.module_rules(bundle))


def aggregate_mto_survival_stages(
    mapped_observations: pd.DataFrame,
):
    rules = _lifetime_rules()
    return _aggregate_mto_survival_stages(
        mapped_observations,
        ceud_classes=[str(value) for value in rules["ceud_labels"].values()],
    )


def mto_survival_scope_comparison(
    mapped_observations: pd.DataFrame,
) -> pd.DataFrame:
    rules = _lifetime_rules()
    return _mto_survival_scope_comparison(
        mapped_observations,
        ceud_classes=[str(value) for value in rules["ceud_labels"].values()],
        sensitivity_minimum_model_year=int(
            rules["scope_sensitivity_minimum_model_year"]
        ),
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
        minimum_model_year=None,
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
    assert ceud_class.loc[ceud_class["age"].eq(0), "cumulative_survival"].item() == 1
    assert ceud_class.loc[ceud_class["age"].eq(1), "annual_retirement_rate"].isna().all()
    assert ceud_class["age"].max() == 6
    assert ceud_class.loc[ceud_class["age"].eq(6), "cumulative_survival"].item() == pytest.approx(0.8)


def test_latest_snapshot_annotation_and_scope_comparison_are_post_transition() -> None:
    mapped = pd.DataFrame(
        {
            "population_group": ["raw_passenger", "raw_passenger"],
            "vehicle_class": ["PASSENGER", "PASSENGER"],
            "mto_make_code": ["HOND", "FORD"],
            "mto_model_code": ["CIV", "FOC"],
            "model_year": [2018, 2018],
            "stock_status": ["FIT_ACTIVE", "FIT_ACTIVE"],
            "report_year": [2020, 2020],
            "next_report_year": [2021, 2021],
            "age": [2, 2],
            "cohort_count_t": [100, 300],
            "cohort_count_t1": [80, 270],
            "apparent_retirements": [20, 30],
            "annual_survival_factor": [0.8, 0.9],
            "annual_retirement_rate": [0.2, 0.1],
            "mapping_accepted": [True, True],
            "nlr_atb_class": ["Compact", "Compact"],
            "nrcan_ceud_class": ["Car", "Car"],
        }
    )
    raw_snapshots = pd.DataFrame(
        {
            "vehicle_class": ["PASSENGER", "PASSENGER"],
            "report_year": [2020, 2021],
            "mto_make_code": ["HOND", "HOND"],
            "mto_model_code": ["CIV", "CIV"],
            "model_year": [2018, 2018],
        }
    )

    annotated = annotate_latest_snapshot_presence(mapped, raw_snapshots)
    comparison = mto_survival_scope_comparison(annotated)

    presence = annotated.set_index("mto_model_code")[
        "present_in_latest_snapshot"
    ].to_dict()
    assert presence == {"CIV": True, "FOC": False}
    age_two = comparison.loc[comparison["age"].eq(2)].set_index(
        "aggregation_scope"
    )
    assert age_two.loc[
        "latest_snapshot_survivors_dynamic_floor", "annual_retirement_rate"
    ] == pytest.approx(20 / 100)
    assert age_two.loc[
        "latest_snapshot_survivors_dynamic_floor", "number_of_transitions"
    ] == 1
    assert set(comparison["aggregation_scope"]) == {
        "latest_snapshot_survivors_dynamic_floor",
        "latest_snapshot_survivors_1990_plus",
    }


def test_raw_transition_estimator_requires_both_endpoints_and_nonnegative_age() -> None:
    snapshots = pd.DataFrame(
        {
            "population_group": ["raw_passenger"] * 8,
            "vehicle_class": ["PASSENGER"] * 8,
            "mto_make_code": ["HOND"] * 8,
            "mto_model_code": [
                "CIV",
                "NEW",
                "FUT",
                "OLD",
                "CIV",
                "NEW",
                "FUT",
                "GONE",
            ],
            "model_year": [2018, 2020, 2021, 2015, 2018, 2020, 2021, 2014],
            "stock_status": ["FIT_ACTIVE"] * 8,
            "report_year": [2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021],
            "cohort_count": [100, 10, 5, 20, 110, 20, 6, 30],
        }
    )

    observations, missing = mto_key_transition_observations(snapshots)

    assert missing == []
    assert observations[["mto_model_code", "age"]].to_records(
        index=False
    ).tolist() == [("CIV", 2), ("NEW", 0)]
    row = observations.loc[observations["mto_model_code"].eq("CIV")].iloc[0]
    assert row["apparent_retirements"] == -10
    assert row["annual_survival_factor"] == pytest.approx(1.1)
    assert row["annual_retirement_rate"] == pytest.approx(-0.1)
    age_zero = observations.loc[observations["mto_model_code"].eq("NEW")]
    *_, age_zero_curve = aggregate_mto_survival_stages(
        age_zero.assign(
            nlr_atb_class="Compact",
            nrcan_ceud_class="Car",
            mapping_accepted=True,
        )
    )
    assert age_zero_curve.loc[
        age_zero_curve["age"].eq(0), "cumulative_survival"
    ].item() == 1
    assert age_zero_curve.loc[
        age_zero_curve["age"].eq(1), "cumulative_survival"
    ].item() == pytest.approx(2.0)


def test_raw_transition_estimator_caps_starting_age_without_vintage_floor() -> None:
    snapshots = pd.DataFrame(
        {
            "population_group": ["raw_passenger"] * 6,
            "vehicle_class": ["PASSENGER"] * 6,
            "mto_make_code": ["FORD"] * 6,
            "mto_model_code": ["MUS", "OLD", "OLDER"] * 2,
            "model_year": [2020, 1985, 1984] * 2,
            "stock_status": ["FIT_ACTIVE"] * 6,
            "report_year": [2020] * 3 + [2021] * 3,
            "cohort_count": [100, 50, 25, 90, 40, 20],
        }
    )

    observations, missing = mto_key_transition_observations(
        snapshots,
        maximum_transition_age=35,
    )

    assert missing == []
    assert observations[["model_year", "age"]].to_records(index=False).tolist() == [
        (2020, 0),
        (1985, 35),
    ]


def test_age_zero_rate_is_first_factor_in_cumulative_survival() -> None:
    observations = pd.DataFrame(
        {
            "nlr_atb_class": ["Compact", "Compact"],
            "nrcan_ceud_class": ["Car", "Car"],
            "model_year": [2020, 2020],
            "age": [0, 1],
            "cohort_count_t": [100, 90],
            "cohort_count_t1": [90, 72],
            "apparent_retirements": [10, 18],
            "zero_denominator": [False, False],
            "mapping_accepted": [True, True],
        }
    )

    *_, curve = aggregate_mto_survival_stages(observations)

    assert curve.loc[curve["age"].eq(0), "cumulative_survival"].item() == 1
    assert curve.loc[curve["age"].eq(1), "cumulative_survival"].item() == pytest.approx(
        0.9
    )
    assert curve.loc[curve["age"].eq(2), "cumulative_survival"].item() == pytest.approx(
        0.72
    )


def test_raw_key_snapshots_can_preserve_pre_2000_survival_evidence() -> None:
    frame = pd.DataFrame(
        {
            "report_year": [2020, 2020],
            "VEHICLE_CLASS": ["PASSENGER", "PASSENGER"],
            "MAKE": ["FORD", "FORD"],
            "MODEL": ["MUS", "FOC"],
            "MODEL_YEAR": [1965, 2015],
            "FIT_ACTIVE": [25, 100],
        }
    )

    uncapped = raw_mto_key_snapshots(
        [frame],
        passenger_class="PASSENGER",
        commercial_class="COMMERCIAL",
        minimum_model_year=None,
        suppressed_code_patterns=[r"^\*+$"],
        unknown_code_labels=["UNKNOWN", "UNK", "N/A"],
    )
    capped = raw_mto_key_snapshots(
        [frame],
        passenger_class="PASSENGER",
        commercial_class="COMMERCIAL",
        minimum_model_year=2000,
        suppressed_code_patterns=[r"^\*+$"],
        unknown_code_labels=["UNKNOWN", "UNK", "N/A"],
    )

    assert set(uncapped["model_year"]) == {1965, 2015}
    assert set(capped["model_year"]) == {2015}


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


def _accepted_frames_fixture() -> dict[str, pd.DataFrame]:
    return {
        "legacy_curves": pd.DataFrame({"value": [1]}),
        "nlr_curves": pd.DataFrame({"value": [2]}),
        "source_curves": pd.DataFrame({"value": [3]}),
        "transformed_curves": pd.DataFrame({"value": [4]}),
        "class_mappings": pd.DataFrame({"value": [5]}),
        "medians": pd.DataFrame({"value": [6]}),
    }


def test_accepted_publisher_does_not_invoke_mto_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_resolve = lifetime_module.resolve_artifact_path

    def resolve_route(bundle, family: str) -> Path:
        if family == "lifetimes_survival":
            return tmp_path
        return real_resolve(bundle, family)

    def fail_mto(*args, **kwargs):
        raise AssertionError("accepted lifetime generation invoked MTO diagnostics")

    monkeypatch.setattr(
        lifetime_module,
        "_derive_accepted_lifetime_frames",
        lambda *args, **kwargs: _accepted_frames_fixture(),
    )
    monkeypatch.setattr(
        lifetime_module,
        "_derive_mto_diagnostic_outputs",
        fail_mto,
    )
    monkeypatch.setattr(lifetime_module, "resolve_artifact_path", resolve_route)

    output_dir = build_accepted_lifetime_artifacts(
        "config/scenarios/legacy_reproduction.yaml"
    )

    assert output_dir == tmp_path
    assert {path.name for path in tmp_path.iterdir()} == {
        "road_vehicle_legacy_wards_survival_curves.csv",
        "road_vehicle_nlr_source_survival_curves.csv",
        "road_vehicle_source_survival_curves.csv",
        "road_vehicle_survival_class_mappings.csv",
        "road_vehicle_transformed_survival_curves.csv",
        "source_derived_median_lifetimes.csv",
    }


def test_mto_diagnostic_publisher_writes_only_diagnostic_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    interim_dir = tmp_path / "interim"
    validation_dir = tmp_path / "validation"
    real_resolve = lifetime_module.resolve_artifact_path

    def resolve_route(bundle, family: str) -> Path:
        if family == "vehicle_survival_interim":
            return interim_dir
        if family == "lifetime_validation":
            return validation_dir
        if family == "lifetimes_survival":
            raise AssertionError("MTO diagnostics published accepted products")
        return real_resolve(bundle, family)

    monkeypatch.setattr(
        lifetime_module,
        "_derive_accepted_lifetime_frames",
        lambda *args, **kwargs: _accepted_frames_fixture(),
    )
    monkeypatch.setattr(
        lifetime_module,
        "_derive_mto_diagnostic_outputs",
        lambda *args, **kwargs: (
            {"interim_evidence.csv": pd.DataFrame({"value": [1]})},
            {"validation_evidence.csv": pd.DataFrame({"value": [2]})},
        ),
    )
    monkeypatch.setattr(lifetime_module, "resolve_artifact_path", resolve_route)

    output_dir = build_mto_survival_diagnostic_artifacts(
        "config/scenarios/legacy_reproduction.yaml"
    )

    assert output_dir == validation_dir
    assert (interim_dir / "interim_evidence.csv").is_file()
    assert (validation_dir / "validation_evidence.csv").is_file()


def test_cli_modes_keep_accepted_generation_as_the_default() -> None:
    default = parse_args([])
    diagnostics = parse_args(["--mto-diagnostics"])
    combined = parse_args(["--all"])

    assert not default.mto_diagnostics and not default.all
    assert diagnostics.mto_diagnostics and not diagnostics.all
    assert combined.all and not combined.mto_diagnostics
