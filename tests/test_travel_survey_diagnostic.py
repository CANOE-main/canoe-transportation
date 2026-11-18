"""Maintained NHTS cohorts, day-domain weights and authenticated RAMP comparisons."""

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from fetching.nhts_ldv_travel import build_requests, read_tables
from utils import load_config_bundle, load_parameter_yaml
from validation import travel_survey_diagnostic as travel
from validation.travel_survey_diagnostic import (
    DERIVATION,
    TravelSurveyDiagnostic,
    activity_chart,
    curate_trips,
    day_type_weights,
    diagnose_tables,
    electric_distance_potential,
    read_ramp_reference,
)

ROOT = Path(__file__).resolve().parents[1]


def test_activity_chart_has_vertical_zero_based_histograms_and_reviewed_bands(
    native, settings
):
    tables = diagnose_tables(
        native, settings, mile_to_km=1, methods=[(DERIVATION, False)]
    )
    tables["windows"] = pd.DataFrame(
        {
            "activity": ["Worker trips", "Student trips", "Inactive trips"],
            "window_number": [1, 1, 1],
            "start_hour": [6, 6.5, 7.5],
            "end_hour": [9.5, 9.5, 19.5],
            "label": ["06:00–09:30", "06:30–09:30", "07:30–19:30"],
        }
    )
    spec = activity_chart(
        TravelSurveyDiagnostic(tables, {}, settings), method=DERIVATION
    ).to_dict(validate=True)
    assert len(spec["vconcat"]) == 3
    for panel in spec["vconcat"]:
        bars = panel["layer"][1]
        assert bars["mark"]["orient"] == "vertical"
        assert bars["encoding"]["y2"]["datum"] == 0
        assert bars["encoding"]["y"]["scale"]["domain"][0] == 0
        assert len(spec["datasets"][bars["data"]["name"]]) == 48
        assert panel["layer"][0]["mark"]["type"] == "rect"


@pytest.fixture
def settings():
    return load_parameter_yaml(
        load_config_bundle(
            ROOT / "config/scenarios/validation_charging_nhts_uncontrolled.yaml"
        ),
        "travel_survey.yaml",
    )


def test_missing_normalized_manifest_identifies_owning_command(settings, tmp_path, monkeypatch):
    monkeypatch.setattr(travel, "load_parameter_yaml", lambda *args: settings)
    monkeypatch.setattr(travel, "build_requests", lambda bundle: [])
    monkeypatch.setattr(travel, "resolve_artifact_path", lambda *args: tmp_path)
    with pytest.raises(FileNotFoundError, match=r"uv run --offline python -m fetching\.nhts_ldv_travel --scenario"):
        travel.build_diagnostic(SimpleNamespace())


def test_day_domain_weights_are_not_replaced_by_annual_weights():
    frame = pd.DataFrame(
        {
            "day_type": ["Weekday", "Saturday", "Sunday"],
            "WTTRDFIN": [100, 100, 100],
            "WTTRDFIN5D": [10, 0, 0],
            "WTTRDFIN2D": [0, 7, 3],
        }
    )
    assert day_type_weights(frame, weighted=True).tolist() == [10, 7, 3]
    assert day_type_weights(frame, weighted=False).tolist() == [1, 1, 1]
    frame.loc[1, "WTTRDFIN2D"] = 0
    with pytest.raises(ValueError, match="positive and finite"):
        day_type_weights(frame, weighted=True)


def test_exact_potential_distinguishes_day_and_distance_denominators():
    evidence = electric_distance_potential(
        np.array([10.0, 100.0]), np.array([1.0, 3.0]), [0, 10, 50, 100]
    )
    assert evidence.distance_potential.tolist() == pytest.approx(
        [0, 40 / 310, 160 / 310, 1]
    )
    assert evidence.daily_distance_cdf.tolist() == pytest.approx([0, 0.25, 0.25, 1])


@pytest.fixture
def native(settings):
    trips = pd.DataFrame(
        {
            "HOUSEID": [1, 1, 1, 2],
            "PERSONID": [1, 1, 1, 1],
            "TRIPID": [1, 2, 3, 1],
            "TRAVDAY": [2, 2, 2, 7],
            "TRPTRANS": [1, 1, 1, 1],
            "VEHTYPE": [1, 1, 1, 1],
            "DRIVER": [1, 1, 1, 1],
            "DRVR_FLG": [1, 2, 1, 1],
            "TRPHHVEH": [1, 1, 1, 1],
            "VEHID": [1, 2, 1, 1],
            "TRPMILES": [10, 10, 10, 20],
            "VMT_MILE": [10, 10, 10, 20],
            "TRVLCMIN": [30, 30, 30, 40],
            "WHYFROM": [3, 1, 1, 1],
            "WHYTO": [6, 1, 1, 1],
            "STRTTIME": [2345, 1000, 1265, 800],
            "WHYTRP1S": [20, 1, 1, 1],
            "WTTRDFIN": [1, 1, 1, 1],
            "WTTRDFIN5D": [3, 4, 5, 0],
            "WTTRDFIN2D": [0, 0, 0, 2],
        }
    )
    for column in settings["filters"]["deduplication_columns"]:
        if column not in trips:
            trips[column] = 0
    vehicles = pd.DataFrame(
        {"HOUSEID": [1, 1, 2], "VEHID": [1, 2, 1], "VEHFUEL": [5, 1, 1]}
    )
    persons = pd.DataFrame({"HOUSEID": [1, 2], "PERSONID": [1, 1], "WTPERFIN": [1, 1]})
    return {"trip": trips, "vehicle": vehicles, "person": persons}


def test_context_validates_clock_and_trip_driver_without_changing_default_method(
    native, settings
):
    context, audit = curate_trips(
        native["trip"], native["vehicle"], settings, weighted=True
    )
    assert list(zip(context.HOUSEID, context.TRIPID)) == [(1, 1), (2, 1)]
    assert context.arrival_minute.iloc[0] == 15
    assert context.work_related.iloc[0] and context.school_related.iloc[0]
    assert audit.excluded_rows.sum() == 2
    derivation, _ = curate_trips(
        native["trip"], native["vehicle"], settings, weighted=False
    )
    assert len(derivation) == 4  # Reviewed DRIVER/start screens remain as configured.


def test_selected_record_deduplication_ignores_audit_columns(native, settings):
    duplicate = native["trip"].iloc[[0]].assign(source_row=999)
    trips = pd.concat(
        [native["trip"].assign(source_row=2), duplicate], ignore_index=True
    )
    result, audit = curate_trips(trips, native["vehicle"], settings, weighted=False)
    assert len(result) == 4
    assert audit.iloc[0].excluded_rows == 1


def test_person_day_totals_remain_separate_from_range_person_vehicle_days(
    native, settings
):
    tables = diagnose_tables(
        native, settings, mile_to_km=1, methods=[(DERIVATION, False)]
    )
    assert tables["summary"].iloc[0].travelling_person_days == 2
    assert tables["summary"].iloc[0].range_days == 3
    assert tables["summary"].iloc[0].person_day_mean_km == 25
    assert tables["daily"].set_index("day_type").loc["Weekday", "mean_km"] == 30
    assert len(tables["activity"]) == 3 * 48
    assert tables["activity"].groupby(
        "activity"
    ).share.sum().to_dict() == pytest.approx(
        {"Worker trips": 1, "Student trips": 1, "Inactive trips": 1}
    )


def test_reviewed_windows_are_authenticated_before_reading(
    tmp_path, settings, monkeypatch
):
    from validation import travel_survey_diagnostic as module
    from utils import file_sha256

    country = settings["ramp_reference"]["country"]
    (tmp_path / "windows.csv").write_text(
        f",,,{country},{country}\nUser,Activity type,Window number,Start,End\nWorking,Main,1,6,9.5\nStudent,Main,1,6.5,9.5\nInactive,Main,1,7.5,19.5\n",
        encoding="utf-8",
    )
    (tmp_path / "d_tot.csv").write_text(
        ",weekday,weekend\nCA,47.39,56.13\n", encoding="utf-8"
    )
    for filename in ["d_min.csv", "t_func.csv"]:
        (tmp_path / filename).write_text(
            "Type,Day,CA\nbusiness,weekday,20\n", encoding="utf-8"
        )
    hashes = {p.name: file_sha256(p) for p in tmp_path.iterdir()}
    request = SimpleNamespace(metadata_sha256="pinned-run-metadata")
    monkeypatch.setattr(module, "build_request", lambda *a, **k: request)
    monkeypatch.setattr(
        module, "validate_run_metadata", lambda *a: {"input_sha256": hashes}
    )
    monkeypatch.setattr(module, "resolve_artifact_path", lambda *a: tmp_path)
    reference, identity = read_ramp_reference(None, settings)
    assert reference["windows"].start_hour.tolist() == [6, 6.5, 7.5]
    assert reference["windows"].end_hour.tolist() == [9.5, 9.5, 19.5]
    assert identity["run_metadata_sha256"] == "pinned-run-metadata"
    (tmp_path / "windows.csv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="authenticated run inputs"):
        read_ramp_reference(None, settings)


def test_current_registered_release_matches_independent_reviewed_method_capture(
    settings,
):
    bundle = load_config_bundle(
        ROOT / "config/scenarios/validation_charging_nhts_uncontrolled.yaml"
    )
    request = next(r for r in build_requests(bundle) if r.component == "public_use")
    if not request.cache_path.is_file():
        pytest.skip(
            "Registered current source cache unavailable; no historical data are used"
        )
    native = read_tables(request, settings["normalization"]["columns"])
    tables = diagnose_tables(
        native, settings, mile_to_km=1.609344, methods=[(DERIVATION, False)]
    )
    summary = tables["summary"].iloc[0]
    assert summary.native_trips == 31074
    assert summary.retained_trips == 19824
    assert summary.travelling_person_days == 7024
    assert summary.range_days == 7244
    daily = tables["daily"].set_index("day_type")
    assert daily.loc[
        ["Weekday", "Saturday", "Sunday", "Weekend"], "mean_km"
    ].tolist() == pytest.approx(
        [47.3868966797, 61.6235834927, 55.0361353011, 57.7488425800], abs=1e-9
    )
    assert tables["fuel"].set_index("activity").trips.to_dict() == {
        "Worker trips": 5871,
        "Student trips": 234,
        "Inactive trips": 13729,
    }
