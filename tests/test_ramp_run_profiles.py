"""Companion identity, physical-hour conversion and diagnostic denominators."""

from types import SimpleNamespace
from dataclasses import replace
import json

import numpy as np
import pandas as pd
import pytest

from fetching import ramp_run_profiles as ingestion
from utils import file_sha256, load_yaml
from validation import ldv_charging_diagnostic as diagnostic


@pytest.fixture
def settings():
    return ingestion.DiagnosticSettings.model_validate(load_yaml(
        ingestion.find_repo_root() / "config/parameters/ldv_charging_diagnostic.yaml"))


@pytest.fixture
def physical_run():
    timestamps = pd.date_range("2018-01-01", "2019-01-01", freq="min", inclusive="left", tz="America/Toronto").tz_convert("UTC").astype(str)
    return {"charging": pd.DataFrame({"Charging Profile": np.full(len(timestamps), 4.)}, index=timestamps),
            "mobility": pd.DataFrame({"Mobility Profile": np.full(len(timestamps), 3000.)}, index=timestamps),
            "usage": pd.DataFrame({"Usage": np.full(len(timestamps), 2.)}, index=timestamps)}


def test_companion_conversions_preserve_physical_year_and_usage_denominator(physical_run, settings):
    hourly, audit = ingestion.transform_run_profiles(physical_run, year=2018, timezone="America/Toronto", fleet_size=10, settings=settings)
    assert len(hourly) == 8760
    assert hourly.charging_kw.sum() == pytest.approx(4 * 8760)
    assert hourly.mobility_kw.sum() == pytest.approx(3 * 8760)
    assert hourly.active_drivers.eq(2).all()
    assert hourly.driving_share.eq(.2).all()
    assert hourly.charging_peak_normalized.eq(1).all()
    assert audit["profiles"]["usage"]["repeated_wall_time_keys"] == 1
    assert audit["conversions"]["mobility_watts_per_kw"] == 1000


def test_misaligned_companions_fail_before_aggregation(physical_run, settings):
    physical_run["usage"] = physical_run["usage"].iloc[1:]
    with pytest.raises(ValueError, match="not aligned"):
        ingestion.transform_run_profiles(physical_run, year=2018, timezone="America/Toronto", fleet_size=10, settings=settings)


@pytest.mark.parametrize("bad_value", [2.5, 11])
def test_usage_rejects_fractional_or_out_of_fleet_counts(physical_run, settings, bad_value):
    physical_run["usage"].iloc[0, 0] = bad_value
    with pytest.raises(ValueError, match="whole active drivers"):
        ingestion.transform_run_profiles(physical_run, year=2018, timezone="America/Toronto", fleet_size=10, settings=settings)


def test_missing_physical_minute_fails(physical_run, settings):
    with pytest.raises(ValueError, match="one-minute"):
        ingestion.transform_run_profiles({key: frame.drop(frame.index[1]) for key, frame in physical_run.items()},
            year=2018, timezone="America/Toronto", fleet_size=10, settings=settings)


def test_identity_and_complete_registered_coverage(tmp_path, settings):
    path = tmp_path / "Usage.csv"
    path.write_text("Usage\n2\n", encoding="utf-8")
    registration = ingestion.RegisteredProfile(registered_path=str(path), expected_sha256=file_sha256(path),
                                              expected_bytes=path.stat().st_size, expected_rows=1)
    ingestion.validate_file(path, registration)
    path.write_text("Usage\n3\n", encoding="utf-8")
    with pytest.raises(ValueError, match="frozen identity"):
        ingestion.validate_file(path, registration)
    bundle = SimpleNamespace(sources=SimpleNamespace(sources={ingestion.SOURCE: SimpleNamespace(adapter={"diagnostic_profiles": {}})}))
    with pytest.raises(ValueError, match="coverage"):
        ingestion.registered_companions(bundle, settings)


def test_import_deduplicates_equal_companions_but_authenticates_every_origin(tmp_path, settings, monkeypatch):
    registrations, requests = {}, {}
    origin = tmp_path / "origins"
    for survey in settings.surveys:
        for strategy in settings.strategies:
            component = f"{survey}_{strategy}"
            directory = origin / settings.import_case_pattern.format(
                survey=ingestion.SURVEY_LABELS[survey], strategy=strategy, year=2018)
            directory.mkdir(parents=True)
            metadata_path = tmp_path / f"{component}.json"
            metadata_path.write_text(component, encoding="utf-8")
            (directory.parents[1] / "run_metadata.json").write_text(component, encoding="utf-8")
            registrations[component] = {}
            for kind, filename in [("charging", "Charging Profiles.csv"), ("mobility", "Mobility Profiles.csv"), ("usage", "Usage.csv")]:
                path = directory / filename
                path.write_text("value\n1\n", encoding="utf-8")
                relative = f"registered/{survey}/{strategy if kind == 'charging' else 'common'}/{filename}"
                identity = dict(registered_path=relative, expected_sha256=file_sha256(path),
                                expected_bytes=path.stat().st_size, expected_rows=1)
                if kind == "charging":
                    requests[component] = SimpleNamespace(**{**identity, "registered_path": tmp_path / relative},
                        metadata_path=metadata_path, metadata_sha256=file_sha256(metadata_path), calendar_year=2018)
                else:
                    registrations[component][kind] = identity
    bundle = SimpleNamespace(repo_root=tmp_path, sources=SimpleNamespace(sources={ingestion.SOURCE:
        SimpleNamespace(adapter={"diagnostic_profiles": registrations})}))
    monkeypatch.setattr(ingestion, "load_settings", lambda bundle: settings)
    monkeypatch.setattr(ingestion, "build_request", lambda bundle, survey, strategy: requests[f"{survey}_{strategy}"])
    monkeypatch.setattr(ingestion, "validate_run_metadata", lambda request: {})
    monkeypatch.setattr(ingestion, "resolve_artifact_path", lambda bundle, family:
        tmp_path / ("registered" if family == "ramp_mobility_external" else "validation"))
    assert ingestion.import_registered_runs(bundle, origin) == {"copied": 10, "unchanged": 8}
    assert len(list((tmp_path / "registered").rglob("*.csv"))) == 10
    audit = json.loads((tmp_path / "validation" / settings.import_manifest_file).read_text())
    assert len(audit["origins"]) == 18
    assert ingestion.import_registered_runs(bundle, origin) == {"copied": 0, "unchanged": 18}
    changed_origin = origin / settings.import_case_pattern.format(survey="NHTS2022", strategy="night", year=2018) / "Usage.csv"
    changed_origin.write_text("value\n2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="frozen identity"):
        ingestion.import_registered_runs(bundle, origin)


@pytest.fixture
def normalized_bundle(tmp_path, settings, monkeypatch):
    sources = {}
    for kind in ["charging", "mobility", "usage"]:
        path = tmp_path / f"{kind}.csv"
        path.write_text("value\n1\n", encoding="utf-8")
        sources[kind] = ingestion.RegisteredProfile(registered_path=path.name,
            expected_sha256=file_sha256(path), expected_bytes=path.stat().st_size, expected_rows=1)
    companions, cases = {}, {}
    directory = tmp_path / "normalized"
    directory.mkdir()
    for survey in settings.surveys:
        for strategy in settings.strategies:
            component = f"{survey}_{strategy}"
            companions[component] = ingestion.RunCompanions(mobility=sources["mobility"], usage=sources["usage"])
            hourly = directory / settings.hourly_file.format(component=component)
            pd.DataFrame({"component": [component]}).to_csv(hourly, index=False)
            cases[component] = {"metadata_sha256": "metadata",
                "sources": {kind: value.model_dump() for kind, value in sources.items()},
                "hourly_sha256": file_sha256(hourly), "hourly_rows": 1}
    (directory / settings.manifest_file).write_text(json.dumps({
        "normalization_settings": ingestion.normalization_settings(settings), "cases": cases}), encoding="utf-8")
    request = SimpleNamespace(**{**sources["charging"].model_dump(), "registered_path": tmp_path / "charging.csv"}, metadata_sha256="metadata")
    bundle = SimpleNamespace(repo_root=tmp_path)
    monkeypatch.setattr(ingestion, "registered_companions", lambda bundle, settings: companions)
    monkeypatch.setattr(ingestion, "build_request", lambda bundle, survey, strategy: request)
    monkeypatch.setattr(ingestion, "validate_run_metadata", lambda request: {})
    monkeypatch.setattr(ingestion, "resolve_artifact_path", lambda bundle, family: directory)
    return bundle


def test_presentation_changes_reuse_authenticated_normalized_evidence(normalized_bundle, settings, monkeypatch):
    updated = settings.model_copy(update={"presentation": settings.presentation.model_copy(update={"line_width": 460}),
                                          "max_lag_hours": 12})
    monkeypatch.setattr(ingestion, "load_settings", lambda bundle: updated)
    evidence = ingestion.validate_bundle(normalized_bundle)
    assert len(evidence.hourly) == 6
    assert evidence.settings.presentation.line_width == 460
    assert evidence.settings.max_lag_hours == 12


@pytest.mark.parametrize("changes", [{"mobility_watts_per_kw": 500}, {"precision": 9}])
def test_conversion_and_precision_changes_require_normalization(normalized_bundle, settings, monkeypatch, changes):
    monkeypatch.setattr(ingestion, "load_settings", lambda bundle: settings.model_copy(update=changes))
    with pytest.raises(ValueError, match="stale/incomplete"):
        ingestion.validate_bundle(normalized_bundle)


@pytest.mark.parametrize("missing", ["manifest", "hourly"])
def test_missing_normalized_file_identifies_owning_command(normalized_bundle, settings, monkeypatch, missing):
    monkeypatch.setattr(ingestion, "load_settings", lambda bundle: settings)
    component = f"{settings.surveys[0]}_{settings.strategies[0]}"
    name = settings.manifest_file if missing == "manifest" else settings.hourly_file.format(component=component)
    (normalized_bundle.repo_root / "normalized" / name).unlink()
    with pytest.raises(FileNotFoundError, match=r"uv run --offline python -m fetching\.ramp_run_profiles"):
        ingestion.validate_bundle(normalized_bundle)


@pytest.fixture
def illustrative_evidence(settings):
    timestamps = pd.date_range("2018-01-01", "2019-01-01", freq="h", inclusive="left", tz="America/Toronto")
    frames, cases = [], {}
    for survey in settings.surveys:
        for index, strategy in enumerate(settings.strategies):
            component = f"{survey}_{strategy}"
            mobility = 3 + np.sin(timestamps.hour.to_numpy() * 2 * np.pi / 24)
            charging = 4 + np.roll(mobility, index * 3)
            frames.append(pd.DataFrame({"component": component, "survey": survey, "strategy": strategy,
                "timestamp_utc": timestamps.tz_convert("UTC").astype(str), "timestamp_local": timestamps.astype(str),
                "hour_index": np.arange(len(timestamps)), "charging_kw": charging, "mobility_kw": mobility,
                "active_drivers": mobility * 2, "driving_share": mobility / 5,
                "charging_peak_normalized": charging / charging.max()}))
            cases[component] = {"calendar_timezone": "America/Toronto"}
    return ingestion.RunProfileEvidence(pd.concat(frames, ignore_index=True), {"cases": cases}, settings)


def test_diagnostic_uses_full_year_mean_and_keeps_dst_day_denominators(illustrative_evidence):
    result = diagnostic.build_diagnostic(illustrative_evidence)
    assert len(result.annual) == 6
    assert set(result.daily.physical_hours) == {23, 24, 25}
    assert len(result.daily) == 6 * 365
    assert result.annual.hours.eq(8760).all()
    np.testing.assert_allclose(result.hourly.groupby("component").charging_kw_unit_mean.mean(), 1)
    assert len(diagnostic.lag_correlations(result)) == 6 * 25
    assert set(diagnostic.lag_correlations(result).paired_hours) == set(range(8736, 8761))


def test_cluster_note_uses_one_full_year_per_survey_without_strategy_duplication():
    daily = pd.DataFrame([
        {"survey": "tts", "strategy": "uncontrolled", "weekday": 0, "mobility_kwh": 10000},
        {"survey": "tts", "strategy": "uncontrolled", "weekday": 1, "mobility_kwh": 14000},
        {"survey": "tts", "strategy": "uncontrolled", "weekday": 6, "mobility_kwh": 6000},
        {"survey": "nhts", "strategy": "uncontrolled", "weekday": 0, "mobility_kwh": 16000},
        {"survey": "nhts", "strategy": "uncontrolled", "weekday": 5, "mobility_kwh": 15000},
        {"survey": "tts", "strategy": "night", "weekday": 0, "mobility_kwh": 90000},
    ])
    note = diagnostic.daily_cluster_note(SimpleNamespace(daily=daily))
    assert "12.00 MWh/day" in note and "6.00 MWh/day" in note
    assert "16.00 versus 15.00 MWh/day" in note
    assert "Across the full year" in note


def test_all_comparison_specs_preserve_six_cases_and_clear_units(illustrative_evidence):
    result = diagnostic.build_diagnostic(illustrative_evidence)
    charts = [diagnostic.diurnal_chart(result, "All days", "Native kW", 300, 200),
              diagnostic.behavior_chart(result, "Weekends", 620, 200),
              diagnostic.week_chart(result, "2018-03-05", 300, 200),
              diagnostic.heatmap_chart(result, "Winter", 600, 200),
              diagnostic.monthly_chart(result, 300, 200),
              diagnostic.daily_relationship_chart(result, "All days", 300, 200),
              diagnostic.lag_chart(result, 300, 200)]
    assert [chart.to_dict(validate=True)["usermeta"]["figure_id"] for chart in charts] == [f"ldv-charge-{i}" for i in range(1, 8)]
    assert len(diagnostic.annual_table(result)) == 6
    assert diagnostic.lag_peak_table(result).index.tolist() == list(range(6))
    with pytest.raises(ValueError, match="outside"):
        diagnostic.week_chart(result, "2020-01-01", 300, 200)
    with pytest.raises(ValueError, match="seven-local-day"):
        diagnostic.week_chart(result, "2018-12-31", 300, 200)


def test_hourly_comparisons_share_plot_width_and_keep_full_calendar_populations(illustrative_evidence):
    result = diagnostic.build_diagnostic(illustrative_evidence)
    behavior = diagnostic.behavior_chart(result, "All days", 620, 210).to_dict(validate=True)
    heatmap = diagnostic.heatmap_chart(result, "All days", 620, 210).to_dict(validate=True)
    assert 2 * behavior["spec"]["width"] + behavior["spacing"] == heatmap["width"]
    behavior_rows = next(iter(behavior["datasets"].values()))
    heatmap_rows = next(iter(heatmap["datasets"].values()))
    assert len(behavior_rows) == 2 * 3 * 24 * 3
    assert len(heatmap_rows) == 2 * 3 * 24
    assert {row["metric"] for row in behavior_rows} == {"Grid charging", "Battery mobility", "Active drivers"}
    encoding = behavior["spec"]["encoding"]
    assert encoding["strokeDash"]["scale"]["domain"] == encoding["color"]["scale"]["domain"]
    assert len({tuple(dash) for dash in encoding["strokeDash"]["scale"]["range"]}) == 3
    assert encoding["strokeDash"]["legend"]["symbolType"] == "stroke"
    assert sum(row["physical_hours"] for row in heatmap_rows) == 6 * 8760


def test_sparse_hourly_comparisons_keep_single_hour_observations_visible(illustrative_evidence):
    result = diagnostic.build_diagnostic(illustrative_evidence)
    sparse = replace(result, hourly=result.hourly.loc[result.hourly.local_hour.eq(3)].copy())
    behavior = diagnostic.behavior_chart(sparse, "All days", 620, 210).to_dict(validate=True)
    heatmap = diagnostic.heatmap_chart(sparse, "All days", 620, 210).to_dict(validate=True)
    assert behavior["spec"]["mark"]["point"]["size"] > 0
    assert behavior["spec"]["encoding"]["x"]["scale"]["domain"] == [0, 23]
    assert heatmap["encoding"]["x"]["scale"]["domain"] == list(range(24))
    assert len(next(iter(behavior["datasets"].values()))) == 2 * 3 * 3
    assert len(next(iter(heatmap["datasets"].values()))) == 2 * 3


def test_empty_hourly_comparisons_label_missing_observations_without_zero_values(illustrative_evidence):
    result = diagnostic.build_diagnostic(illustrative_evidence)
    empty = replace(result, hourly=result.hourly.iloc[:0].copy())
    for owner, figure in ((diagnostic.behavior_chart, "ldv-charge-2"), (diagnostic.heatmap_chart, "ldv-charge-4")):
        chart = owner(empty, "All days", 620, 210).to_dict(validate=True)
        assert chart["usermeta"]["figure_id"] == figure
        assert chart["mark"]["type"] == "text"
        assert chart["width"] == 620
        assert chart["data"]["values"] == [{"message": "No hourly observations in the selected calendar population"}]
