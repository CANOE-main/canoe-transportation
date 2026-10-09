"""Trust boundaries and dimensional behavior of the opt-in growth diagnostic."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from diagnostics.technology_growth import (
    GrowthDiagnosticConfig,
    annual_seed_to_period,
    annual_to_period,
    calibrate_candidates,
    compare_releases,
    fit_curve,
    limit_envelope,
    load_diagnostic_config,
    minimum_seed,
    observation_windows,
    period_additions,
    s_curve,
)
from diagnostics import technology_growth as analysis
from fetching import technology_growth_evidence as acquisition
from fetching.technology_growth_evidence import (
    EvidenceAsset,
    GrowthSourceRequest,
    normalize_eafo,
    normalize_hatch,
    validate_asset,
)
from utils import active_source_keys, load_config_bundle, load_harmonization_rules

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def bundle():
    return load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=ROOT
    )


@pytest.fixture(scope="module")
def controls(bundle):
    return load_diagnostic_config(bundle)


@pytest.mark.parametrize("annual,years", [(0, 5), (0.1, 2), (0.3, 5), (-0.1, 5)])
def test_annual_conversion_matches_compounding(annual, years):
    assert annual_to_period(annual, years) == pytest.approx((1 + annual) ** years - 1)
    if annual > 0:
        assert annual_to_period(annual, years) != pytest.approx(annual * years)


@pytest.mark.parametrize(
    "annual,years", [(-1, 5), (0.1, 0), (math.nan, 5), (0.1, math.inf)]
)
def test_annual_conversion_rejects_undefined_inputs(annual, years):
    with pytest.raises(ValueError):
        annual_to_period(annual, years)


@pytest.mark.parametrize("annual", [0, 0.2, -0.1])
def test_annual_seed_conversion_matches_repeated_annual_allowances(annual):
    level = 3.0
    for _ in range(5):
        level = 0.01 + (1 + annual) * level
    assert level == pytest.approx(
        (1 + annual_to_period(annual, 5)) * 3 + annual_seed_to_period(0.01, annual, 5)
    )


def test_growth_modes_have_different_repeated_seed_effects():
    normal = limit_envelope(
        mode="growth_new_capacity",
        initial=0,
        transition_rate=0,
        seed=0.001,
        years=[2020, 2025, 2030, 2035],
    )
    delta = limit_envelope(
        mode="growth_new_capacity_delta",
        initial=0,
        previous2=0,
        transition_rate=0,
        seed=0.001,
        years=[2020, 2025, 2030, 2035],
    )
    assert normal.capacity_k.tolist() == pytest.approx([0, 0.001, 0.002, 0.003])
    assert delta.capacity_k.tolist() == pytest.approx([0, 0.001, 0.003, 0.006])


def test_zero_seed_cannot_initiate_level_growth_from_zero():
    result = limit_envelope(
        mode="growth_capacity",
        initial=0,
        transition_rate=3,
        seed=0,
        years=[2023, 2028, 2033],
    )
    assert result.capacity_k.eq(0).all()


def test_delta_negative_prior_change_is_not_clipped_or_hidden():
    result = limit_envelope(
        mode="growth_new_capacity_delta",
        initial=1,
        previous2=3,
        transition_rate=0.5,
        seed=0,
        years=[2020, 2025],
    )
    assert result.capacity_k.iloc[-1] == -2
    assert not bool(result.feasible_nonnegative.iloc[-1])
    with pytest.raises(ValueError, match="two historical"):
        limit_envelope(
            mode="growth_new_capacity_delta",
            initial=1,
            transition_rate=0,
            seed=0,
            years=[2020, 2025],
        )


def test_minimum_seed_is_a_residual_of_the_correct_quantity():
    assert minimum_seed(
        np.array([10, 12, 20]), 0.1, mode="growth_capacity"
    ) == pytest.approx(6.8)
    # D=[2,8]; a zero-rate delta allowance needs S=8-2, not sales growth 20/12.
    assert (
        minimum_seed(np.array([10, 12, 20]), 0, mode="growth_new_capacity_delta") == 6
    )
    assert (
        minimum_seed(np.array([10, 12, 20]), 0.5, mode="growth_new_capacity_delta") == 5
    )
    with pytest.raises(ValueError):
        minimum_seed(np.array([1, np.nan, 2]), 0, mode="growth_new_capacity_delta")


def test_complete_period_additions_preserve_zeros_and_missing():
    frame = pd.DataFrame(
        {
            "series_id": ["sales"] * 15,
            "year": range(2011, 2026),
            "value": [0] * 5 + [10] * 5 + [20] * 4 + [np.nan],
        }
    )
    result = period_additions(frame, width=5)
    assert result.period.tolist() == [2010, 2015, 2020]
    assert result.new_capacity_k.iloc[0] == 0
    assert result.new_capacity_k.iloc[1] == 0.05
    assert not bool(result.complete.iloc[-1]) and np.isnan(
        result.new_capacity_k.iloc[-1]
    )
    assert result.delta_k.iloc[1] == 0.05
    assert np.isnan(result.second_difference_k.iloc[-1])


def test_differencing_does_not_skip_a_missing_whole_period():
    frame = pd.DataFrame(
        {
            "series_id": ["sales"] * 10,
            "year": [*range(2011, 2016), *range(2021, 2026)],
            "value": [1] * 10,
        }
    )
    assert period_additions(frame, width=5).delta_k.isna().all()
    with pytest.raises(ValueError, match="unique annual"):
        period_additions(pd.concat([frame, frame.iloc[:1]]), width=5)


def test_windows_recover_observed_growth_without_a_ceiling(controls):
    observations = pd.DataFrame(
        {
            "series_id": ["a"] * 11,
            "year": range(2000, 2011),
            "value": 5 * np.exp(0.2 * np.arange(11)),
        }
    )
    result = observation_windows(observations, controls.fitting)
    assert result.status.eq("usable").all()
    assert np.allclose(result.annual_rate, np.expm1(0.2))
    assert np.allclose(result.endpoint_cagr, np.expm1(0.2))
    assert np.allclose(result.native_exp_annual_rate, np.expm1(0.2))
    assert np.allclose(result.r2_log, 1)


def test_zero_is_not_an_invented_log_observation(controls):
    observations = pd.DataFrame(
        {
            "series_id": ["a"] * 8,
            "year": range(2000, 2008),
            "value": [0, 1, 2, 4, 8, 16, 32, 64],
        }
    )
    result = observation_windows(observations, controls.fitting)
    assert result.status.tolist() == ["zero_or_negative_log_undefined"]
    assert result.annual_rate.isna().all()


def test_logistic_fit_is_unit_invariant_and_recovers_complete_curve(controls):
    years = np.arange(1960, 2001)
    data = pd.DataFrame({"year": years, "value": s_curve(years, 1000, 0.2, 1980)})
    first = fit_curve(
        data, family="logistic", ceiling_multiplier=2, controls=controls.fitting
    )
    scaled = fit_curve(
        data.assign(value=data.value / 1000),
        family="logistic",
        ceiling_multiplier=2,
        controls=controls.fitting,
    )
    assert first["status"] == "fitted"
    assert first["k_per_year"] == pytest.approx(0.2, rel=1e-4)
    assert first["inflection_year"] == pytest.approx(1980, abs=1e-3)
    assert first["k_per_year"] == pytest.approx(scaled["k_per_year"], rel=1e-5)
    assert first["ceiling_native"] / 1000 == pytest.approx(scaled["ceiling_native"])
    assert first["emergence_annual_fraction"] == pytest.approx(np.expm1(0.2), rel=1e-4)
    assert first["duration_10_90_years"] == pytest.approx(np.log(81) / 0.2, rel=1e-4)


def test_gompertz_coefficient_is_not_mapped_to_emergence(controls):
    years = np.arange(1960, 2001)
    result = fit_curve(
        pd.DataFrame(
            {"year": years, "value": s_curve(years, 100, 0.1, 1980, "gompertz")}
        ),
        family="gompertz",
        ceiling_multiplier=2,
        controls=controls.fitting,
    )
    assert result["status"] == "fitted"
    assert np.isnan(result["emergence_annual_fraction"])


def test_hatch_normalization_preserves_interior_blank_and_zero(tmp_path):
    path = tmp_path / "hatch.csv"
    pd.DataFrame(
        {
            "ID": ["id"],
            "Region": ["CAN"],
            "Technology Name": ["Cars"],
            "Metric": ["Total Number"],
            "Unit": ["vehicles"],
            "Variable": ["cars"],
            "Data Source": ["source"],
            "1999": [np.nan],
            "2000": [0],
            "2001": [np.nan],
            "2002": [2],
            "2003": [np.nan],
        }
    ).to_csv(path, index=False)
    result = normalize_hatch(path, "original")
    assert result.year.tolist() == [2000, 2001, 2002]
    assert result.observation_status.tolist() == ["zero", "missing", "observed"]
    assert result.raw_row.eq(2).all()


def test_eafo_partial_year_and_rounded_zero_share_are_explicit(tmp_path):
    path = tmp_path / "fleet.csv"
    pd.DataFrame(
        {
            "YEAR": [2025, 2026],
            "BEV": [1, 2],
            "PHEV": [0, np.nan],
            "H2": [0, 0],
            "CNG": [np.nan, 1],
        }
    ).to_csv(path, index=False)
    result = normalize_eafo(
        path, vehicle_class="M1", quantity="fleet_share", snapshot_year=2026
    )
    assert result.loc[result.year.eq(2025), "complete_year"].all()
    assert not result.loc[result.year.eq(2026), "complete_year"].any()
    assert result.loc[result.powertrain.eq("H2"), "value"].eq(0).all()
    assert (
        result.loc[result.powertrain.eq("CNG") & result.year.eq(2025), "value"]
        .isna()
        .all()
    )
    assert result.unit.eq("percent").all()


def test_release_comparison_detects_cumulative_transformation_and_alias():
    base = pd.DataFrame(
        {
            "country": ["CAN"] * 3,
            "technology": ["Passenger Cars"] * 3,
            "native_source": ["CHAT"] * 3,
            "unit": ["vehicles"] * 3,
            "year": [2000, 2001, 2002],
            "series_id": ["original:old"] * 3,
            "metric": ["Total Number"] * 3,
            "variable": ["cars"] * 3,
            "value": [2, 3, 4],
        }
    )
    extended = base.assign(
        series_id="extended:new", metric="Cumulative Calculation", value=[2, 5, 9]
    )
    result = compare_releases(base, extended)
    assert result.comparison.tolist() == ["cumulative_sum_of_original"]
    assert result.changed_observations.tolist() == [2]


def test_maturity_reference_changes_window_selection_and_absence_stays_unaccepted(
    controls,
):
    common = {
        "region": "ON",
        "road_class": "cars",
        "powertrain": "bev",
        "norway_class": "M1",
        "reference_year": 2023,
        "stock_k_vehicles": 10,
        "target_id": "cars:bev",
    }
    windows = pd.DataFrame(
        {
            "series_id": ["eafo:M1:fleet:BEV"] * 2,
            "start_year": [2010, 2015],
            "end_year": [2017, 2022],
            "span_years": [7, 7],
            "annual_rate": [0.4, 0.1],
            "r2_log": [0.99, 0.99],
            "status": ["usable", "usable"],
        }
    )
    eafo = pd.DataFrame(
        {
            "vehicle_class": ["M1", "M1"],
            "quantity": ["fleet_share", "fleet_share"],
            "powertrain": ["BEV", "BEV"],
            "year": [2010, 2015],
            "value": [1, 10],
        }
    )
    domestic = pd.DataFrame(
        columns=[
            "region",
            "road_class",
            "fuel_type",
            "year",
            "quantity",
            "quantity_k_vehicles",
            "coverage_complete",
            "proxy_geography",
        ]
    )
    early = calibrate_candidates(
        pd.DataFrame([{**common, "stock_share": 0.01}]),
        windows,
        eafo,
        domestic,
        controls,
    )
    later = calibrate_candidates(
        pd.DataFrame([{**common, "stock_share": 0.1}]),
        windows,
        eafo,
        domestic,
        controls,
    )
    absent = calibrate_candidates(
        pd.DataFrame([{**common, "stock_share": 0, "stock_k_vehicles": 0}]),
        windows,
        eafo,
        domestic,
        controls,
    )
    assert (
        early.loc[early["mode"].eq("growth_capacity"), "annual_candidate_median"].iloc[
            0
        ]
        == 0.4
    )
    assert (
        later.loc[later["mode"].eq("growth_capacity"), "annual_candidate_median"].iloc[
            0
        ]
        == 0.1
    )
    assert absent.candidate_status.eq("insufficient_target_evidence").all()


def test_typed_research_boundary_rejects_promotion_and_path_escape(controls):
    with pytest.raises(ValidationError):
        GrowthDiagnosticConfig.model_validate(
            {**controls.model_dump(), "diagnostic_only": False}
        )
    with pytest.raises(ValidationError):
        EvidenceAsset(
            key="escape",
            cache_path="../outside.csv",
            url="https://example.org/data",
            sha256="a" * 64,
            bytes=10,
            format="csv",
        )


def test_physical_asset_checksum_and_adapter_uniqueness():
    content = b"a,b\n1,2\n"
    asset = EvidenceAsset(
        key="test",
        cache_path="test.csv",
        url="https://example.org/data",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        format="csv",
    )
    validate_asset(content, asset)
    with pytest.raises(ValueError, match="checksum"):
        validate_asset(content[:-1] + b"x", asset)
    with pytest.raises(ValidationError, match="Duplicate"):
        GrowthSourceRequest(assets=[asset, asset])


def test_missing_offline_source_never_calls_http(bundle, tmp_path, monkeypatch):
    asset = EvidenceAsset(
        key="missing",
        cache_path="missing.csv",
        url="https://example.org/data",
        sha256="a" * 64,
        bytes=10,
        format="csv",
    )
    monkeypatch.setattr(
        acquisition,
        "registered_assets",
        lambda _: [("hatch_original_growth_evidence", asset)],
    )
    monkeypatch.setattr(acquisition, "resolve_artifact_path", lambda *args: tmp_path)
    monkeypatch.setattr(
        acquisition.requests.Session,
        "get",
        lambda *args, **kwargs: pytest.fail("Offline acquisition attempted HTTP"),
    )
    with pytest.raises(FileNotFoundError, match="Offline"):
        acquisition.acquire_evidence(bundle, offline=True)


def test_changed_cache_is_rejected_without_download_or_overwrite(
    bundle, tmp_path, monkeypatch
):
    original = b"a,b\n1,2\n"
    asset = EvidenceAsset(
        key="changed",
        cache_path="changed.csv",
        url="https://example.org/data",
        sha256=hashlib.sha256(original).hexdigest(),
        bytes=len(original),
        format="csv",
    )
    path = tmp_path / asset.cache_path
    path.write_bytes(b"a,b\n3,4\n")
    monkeypatch.setattr(
        acquisition,
        "registered_assets",
        lambda _: [("hatch_original_growth_evidence", asset)],
    )
    monkeypatch.setattr(acquisition, "resolve_artifact_path", lambda *args: tmp_path)
    monkeypatch.setattr(
        acquisition.requests.Session,
        "get",
        lambda *args, **kwargs: pytest.fail("Changed cache triggered HTTP"),
    )
    with pytest.raises(ValueError, match="checksum"):
        acquisition.acquire_evidence(bundle, offline=False)
    assert path.read_bytes() == b"a,b\n3,4\n"


def test_registered_research_sources_do_not_enroll_production(bundle):
    for key in acquisition.SOURCE_KEYS:
        source = bundle.sources.sources[key]
        assert not source.required and key not in active_source_keys(bundle)
        assert all(
            not component.required and component.parameter_modules == []
            for component in source.components.values()
        )
    assert bundle.paths.artifacts["technology_growth_cache"].layer == "cache"
    assert (
        bundle.paths.artifacts["technology_growth_notebook"].path
        == "docs/insights/growth_constraint_diagnosis.py"
    )


def test_canadian_truck_stock_cannot_be_relabelled_as_new_registrations(
    bundle, controls, monkeypatch
):
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    car = pd.DataFrame(
        [
            {
                "scenario_region": "ON",
                "reference_year": 2023,
                "vehicle_type": t,
                "fuel_type": "Battery electric",
                "scaled_value": 1000,
                "units": "Units",
                "source_table_id": "20-10-0025-01",
                "source_period_count": 4,
            }
            for t in rules["statcan_ldv_source_types"]["cars"]
        ]
    )
    trucks = pd.DataFrame(
        [
            {
                "scenario_region": "ON",
                "reference_year": 2023,
                "vehicle_type": t,
                "fuel_type": "Battery electric",
                "scaled_value": 1000,
                "units": "Number",
            }
            for t in [
                *rules["statcan_medium_source_types"],
                rules["statcan_heavy_source_type"],
            ]
        ]
    )
    monkeypatch.setattr(
        analysis.pd,
        "read_csv",
        lambda p: car.copy() if "historical" in p.name else trucks.copy(),
    )
    monkeypatch.setattr(analysis, "file_sha256", lambda p: "f" * 64)
    result, _ = analysis.canadian_registrations(bundle, controls)
    assert (
        result.loc[result.road_class.eq("cars"), "quantity"]
        .eq("annual_new_registrations")
        .all()
    )
    assert (
        result.loc[
            result.road_class.isin(["medium_trucks", "heavy_trucks"]), "quantity"
        ]
        .eq("registered_stock")
        .all()
    )
    assert result.loc[result.road_class.eq("cars"), "quantity_k_vehicles"].iloc[
        0
    ] == len(car)
    car.loc[:, "source_period_count"] = 3
    partial, _ = analysis.canadian_registrations(bundle, controls)
    assert not partial.loc[partial.road_class.eq("cars"), "coverage_complete"].any()
    assert (
        partial.loc[partial.road_class.eq("cars"), "quantity_k_vehicles"].isna().all()
    )


def test_existing_reference_respects_cleanup_and_missing_fuel_representation(
    bundle, controls, monkeypatch
):
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    capacity_rows, share_rows, cohort_rows = [], [], []
    for road_class in controls.target_classes:
        tech = rules["fuel_technology"][road_class]["Diesel"]
        capacity_rows.append(
            {
                "region": "ON",
                "road_class": road_class,
                "tech": tech,
                "vintage": 2020,
                "capacity": 100,
                "units": "k vehicles",
            }
        )
        for vintage, value in [(2020, 100), (2015, 0.0005)]:
            share_rows.append(
                {
                    "region": "ON",
                    "road_class": road_class,
                    "tech": tech,
                    "vintage": vintage,
                    "capacity": value,
                    "source_year": 2023,
                    "source_region": "ON",
                    "proxy_geography": False,
                    "dashboard_override": False,
                }
            )
        cohort_rows.append(
            {
                "region": "ON",
                "road_class": road_class,
                "stock_k_vehicles": 100.0005,
                "age_source": "fixture",
            }
        )
    native_read = pd.read_csv
    frames = {
        "road_existing_capacity.csv": pd.DataFrame(capacity_rows),
        "road_existing_capacity_fuel_shares.csv": pd.DataFrame(share_rows),
        "road_existing_capacity_age_cohorts.csv": pd.DataFrame(cohort_rows),
    }
    monkeypatch.setattr(
        analysis.pd,
        "read_csv",
        lambda p: frames[p.name].copy() if p.name in frames else native_read(p),
    )
    monkeypatch.setattr(analysis, "file_sha256", lambda p: "f" * 64)
    references, members, _ = analysis.existing_capacity_references(bundle, controls)
    assert len(references) == 24
    assert np.allclose(references.cleanup_removed_class_k, 0.0005)
    assert (
        references.loc[references.powertrain.eq("fcev"), "deployment_status"]
        .eq("absent_existing_representation")
        .all()
    )
    assert (
        references.loc[references.powertrain.eq("fcev"), "stock_k_vehicles"].eq(0).all()
    )
    assert members.diagnostic_only.all()
    frames["road_existing_capacity.csv"].loc[0, "capacity"] += 1
    with pytest.raises(ValueError, match="differs from cleanup"):
        analysis.existing_capacity_references(bundle, controls)


@pytest.mark.parametrize("changed_kind", ["input", "artifact"])
def test_notebook_handoff_rejects_stale_input_or_edited_artifact(
    bundle, controls, tmp_path, monkeypatch, changed_kind
):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "observations.csv"
    input_path.write_text("a,b\n1,2\n")
    output_path.write_text("a,b\n1,2\n")
    digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
    (tmp_path / "integrity.json").write_text(
        json.dumps(
            {
                "input_hashes": {"input.csv": digest},
                "artifact_hashes": {"observations.csv": digest},
            }
        )
    )
    (input_path if changed_kind == "input" else output_path).write_text("a,b\n3,4\n")
    monkeypatch.setattr(analysis, "registered_assets", lambda _: [])
    monkeypatch.setattr(analysis, "load_diagnostic_config", lambda _: controls)
    monkeypatch.setattr(
        analysis, "resolve_artifact_path", lambda *args: tmp_path / "integrity.json"
    )
    monkeypatch.setattr(
        analysis, "resolve_repo_path", lambda root, name: tmp_path / name
    )
    with pytest.raises(
        ValueError, match="Stale" if changed_kind == "input" else "Changed"
    ):
        analysis.load_diagnostic_tables(bundle)
