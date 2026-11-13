"""The illustrative week never replaces the full-year normalization denominator."""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

from utils import file_sha256
from validation import charging_profile_diagnostic as diagnostic


@pytest.fixture
def source(tmp_path, monkeypatch):
    config = tmp_path / "config/parameters"
    config.mkdir(parents=True)
    (config / "documentation.yaml").write_text(yaml.safe_dump({"charging_profile": {
        "scenario": "scenario.yaml", "region": "ON", "profiles": ["tts"],
        "week_start": "2018-01-08", "figure_id": "charging-profile-1",
    }}), encoding="utf-8")
    (tmp_path / "scenario.yaml").write_text("scenario", encoding="utf-8")
    registered = tmp_path / "registered.csv"
    pd.DataFrame({"power": [1]}).to_csv(registered)
    request = SimpleNamespace(registered_path=registered, expected_bytes=registered.stat().st_size,
        expected_sha256=file_sha256(registered), expected_rows=1, calendar_year=2018,
        calendar_timezone="America/Toronto", strategy="uncontrolled", metadata_sha256="test-metadata-hash")
    timestamps = pd.date_range("2018-01-01", periods=8760, freq="h", tz="America/Toronto")
    factors = np.full(8760, .4)
    factors[168:336] = .8
    hourly = pd.DataFrame({"factor": factors, "timestamp_utc": timestamps.tz_convert("UTC").astype(str)})
    monkeypatch.setattr(diagnostic, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(diagnostic, "load_config_bundle", lambda path: None)
    monkeypatch.setattr(diagnostic, "build_request", lambda bundle, profile: request)
    monkeypatch.setattr(diagnostic, "validate_run_metadata", lambda request: {})
    monkeypatch.setattr(diagnostic, "load_harmonization_rules", lambda bundle, owner: (
        {"value_column": "power", "precision": 8, "hourly_operation": "mean",
         "normalization": "annual_hourly_peak", "label_basis": "elapsed_hours_from_local_year_start"}
        if owner == "legacy_charging_profiles"
        else {"technology": {"ldv": {"new": "CHARGER"}}}))
    monkeypatch.setattr(diagnostic, "prepare_charger_utilization_targets", lambda bundle: (
        pd.DataFrame({"region": ["ON"], "tech_or_group": ["CHARGER"], "target_utilization": [.2]}),
        SimpleNamespace(data_id="reviewed-utilization")))
    monkeypatch.setattr(diagnostic, "transform_hourly", lambda *args, **kwargs: SimpleNamespace(hourly=hourly))
    return registered


def test_week_uses_full_year_multiplier_and_retains_distinct_week_mean(source):
    result = diagnostic.load_diagnostic()
    check = result.annual.iloc[0]
    assert check.rescaled_mean == pytest.approx(.2)
    assert check.multiplier == pytest.approx(.2 / check.native_mean)
    assert check.rescaled_week_mean != pytest.approx(.2)
    native = result.week.loc[result.week.series.eq("native"), "factor"].to_numpy()
    scaled = result.week.loc[result.week.series.eq("rescaled"), "factor"].to_numpy()
    np.testing.assert_allclose(scaled, native * check.multiplier)
    assert diagnostic.profile_chart(result).to_dict(validate=True)["usermeta"]["figure_id"] == "charging-profile-1"


def test_changed_registered_source_fails_before_plotting(source):
    source.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="frozen source identity"):
        diagnostic.load_diagnostic()


def test_unsupported_normalization_rule_fails_before_source_io(source, monkeypatch):
    original = diagnostic.load_harmonization_rules
    monkeypatch.setattr(diagnostic, "load_harmonization_rules", lambda bundle, owner: {
        **original(bundle, owner), "normalization": "unsupported",
    })
    source.unlink()
    with pytest.raises(ValueError, match="reviewed production normalization rule"):
        diagnostic.load_diagnostic()
