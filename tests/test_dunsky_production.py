"""Registered report-to-production equivalents and retained assumption boundaries."""

from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from fetching.dunsky_charger_inputs import (
    SOURCE, extract_production_inputs, load_evidence, production_inputs, settings_for,
)
from fetching import dunsky_charger_inputs as dunsky
from parameterization.ev_chargers import load_charger_inputs
from parameterization import ev_chargers as chargers
from utils import resolve_input_path


def test_report_equivalents_keep_reviewed_aggregate_values_units_and_source_cells(legacy_bundle):
    frame, _ = production_inputs(legacy_bundle)
    assert len(frame) == 29
    values = frame.set_index(["sub_category", "parameter", "period"])
    # Independent literal reference from the reviewed pre-migration aggregate inputs.
    for key, expected in {
        ("ldv", "L1_rated_power", "all"): 1.5,
        ("ldv", "L2_rated_power", "2025"): 7.2,
        ("ldv", "L2_rated_power", "2030"): 10.,
        ("ldv", "L2_rated_power", "through_2050"): 12.,
        ("ldv", "L2_capex", "2025"): 15000.,
        ("ldv", "DCFC_capex", "through_2050"): 240000.,
        ("mhdv", "50kW_capex", "2030"): 74604.,
        ("mhdv", "350kW_capex", "2035"): 218747.,
        ("mhdv", "2MW_capex", "through_2050"): 616569.,
        ("ldv", "DCFC_rated_power", "2025"): 125.,
        ("ldv", "DCFC_rated_power", "2030"): 175.,
        ("ldv", "DCFC_rated_power", "2035"): 250.,
        ("ldv", "DCFC_rated_power", "through_2050"): 300.,
        ("mhdv", "existing_50kW_share", "existing"): .8,
        ("mhdv", "existing_350kW_share", "existing"): .2,
    }.items():
        assert float(values.loc[key, "value"]) == expected
    costs = frame.loc[frame.parameter.str.endswith("_capex")]
    assert set(costs.unit) == {"CAD"}
    assert set(costs.currency_year) == {"2022"}
    assert frame.notes.str.strip().ne("").all()
    assert frame.source_sha256.str.fullmatch("[0-9a-f]{64}").all()
    assert all(json.loads(value) for value in frame.source_cells)
    assert legacy_bundle.sources.resolved_data_quality(SOURCE, "published_report").missing_fields() == []


def test_only_reviewed_utilization_overrides_remain_manual_without_fixed_future_mix(legacy_bundle):
    manual = pd.read_csv(resolve_input_path(legacy_bundle, "manual", "charger_parameters.csv"),
                         dtype=str, keep_default_na=False)
    retained = manual.loc[manual["source -> data_source"].eq(legacy_bundle.sources.sources[SOURCE].citation)]
    assert len(retained) == 2
    assert set(retained.parameter) == {"max_annual_utilization"}
    combined, _ = load_charger_inputs(legacy_bundle, manual)
    assert len(combined) == 42
    assert not combined.parameter.str.endswith("capex_aggregation").any()
    assert not combined.duplicated(["category", "sub_category", "parameter", "period"]).any()
    assert not any("PUB_" in key or "NIGHT" in key for key in combined.parameter)


@pytest.mark.parametrize("change,match", [
    ("trajectory", "no longer constant"), ("row", "production row changed"),
    ("negative", "Invalid production Dunsky numeric"), ("dash", "Invalid production Dunsky numeric"),
    ("missing_population", "composition coverage changed"),
])
def test_changed_source_cells_fail_without_fallback(legacy_bundle, change, match):
    evidence, _ = load_evidence(legacy_bundle)
    evidence = deepcopy(evidence)
    if change == "trajectory":
        evidence["tables"]["30"]["matrix"][5][2] = "2.0"
    elif change == "row":
        evidence["tables"]["30"]["matrix"][5][0] = "Renamed home L1"
    elif change == "negative":
        evidence["tables"]["38"]["matrix"][1][3] = "-26"
    elif change == "dash":
        evidence["tables"]["38"]["matrix"][2][3] = "-"
    else:
        evidence["tables"]["38"]["matrix"][1][1] = "Unreviewed location"
    with pytest.raises(ValueError, match=match):
        extract_production_inputs(evidence, settings_for(legacy_bundle)["production"],
                                  legacy_bundle.sources.sources[SOURCE].citation)


def test_manual_cannot_reintroduce_fetched_authority(legacy_bundle):
    fetched, _ = production_inputs(legacy_bundle)
    with pytest.raises(ValueError, match="duplicate fetched"):
        load_charger_inputs(legacy_bundle, fetched.iloc[:1].copy())


def test_even_registered_download_is_parsed_before_cache_publication(tmp_path, monkeypatch):
    content = b"registered but malformed HTML"
    payload = {"url": "https://example.org/report", "cache_path": "report.html",
               "expected_bytes": len(content), "expected_sha256": hashlib.sha256(content).hexdigest(),
               "publication_date": "2024-02"}
    bundle = SimpleNamespace(sources=SimpleNamespace(sources={SOURCE: SimpleNamespace(
        adapter={"diagnostic_report": payload}, citation="registered citation",
    )}))
    path = tmp_path / "report.html"
    monkeypatch.setattr(dunsky, "resolve_input_path", lambda *_: path)
    monkeypatch.setattr(dunsky, "settings_for", lambda *_: {"required_edition_tokens": ["February 2024"]})
    response = SimpleNamespace(content=content, raise_for_status=lambda: None)
    with pytest.raises(ValueError, match="edition marker missing"):
        load_evidence(bundle, download=True, session=SimpleNamespace(get=lambda *_args, **_kwargs: response))
    assert not path.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_equal_values_from_different_source_cells_change_charger_dataset_identity(
    legacy_bundle, tmp_path, monkeypatch,
):
    config = dunsky.resolve_parameter_path(legacy_bundle, "dunsky_charger_inputs.yaml")
    private = tmp_path / config.name
    private.write_bytes(config.read_bytes())
    monkeypatch.setattr(dunsky, "resolve_parameter_path", lambda *_: private)
    monkeypatch.setattr(chargers, "resolve_parameter_path", lambda *_: private)
    manual = pd.read_csv(resolve_input_path(legacy_bundle, "manual", "charger_parameters.csv"),
                         dtype=str, keep_default_na=False)

    def identity():
        resolved, report = load_charger_inputs(legacy_bundle, manual)
        inputs = chargers._charger_input_identity(legacy_bundle, resolved, [report])
        digest = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
        context = chargers._context(
            legacy_bundle, [(SOURCE, "published_report")],
            name="same_value_selector_regression", digest=digest,
        )
        fetched, _ = production_inputs(legacy_bundle)
        return resolved, fetched, inputs, context

    before, before_cells, before_inputs, before_context = identity()
    settings = yaml.safe_load(private.read_text(encoding="utf-8"))
    target = next(row for row in settings["production"]["trajectories"]
                  if row["parameter"] == "L2_rated_power")
    target["labels"] = ["Home - L2"]
    private.write_text(yaml.safe_dump(settings, sort_keys=False), encoding="utf-8")
    after, after_cells, after_inputs, after_context = identity()
    pd.testing.assert_frame_equal(before, after)
    selected = before_cells.parameter.eq("L2_rated_power")
    assert before_cells.loc[selected, "source_cells"].tolist() != after_cells.loc[selected, "source_cells"].tolist()
    assert dict(before_inputs["files"])[config.name] != dict(after_inputs["files"])[config.name]
    assert before_context.data_id != after_context.data_id
