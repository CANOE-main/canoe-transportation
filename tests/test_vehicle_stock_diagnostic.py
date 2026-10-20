"""Recorded-input boundaries of the vehicle-stock notebook."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization.road_lifetimes_survival import (
    median_equivalent_lifetimes,
    transform_source_survival_curves,
)
from utils import load_config_bundle, load_harmonization_rules, resolve_artifact_path
from validation import notebook_evidence
from validation import vehicle_stock_diagnostic as diagnostic


@pytest.fixture
def bundle():
    return load_config_bundle("config/scenarios/default.yaml")


@pytest.fixture
def snapshot(bundle, tmp_path):
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    frames = {
        "source_manifest": pd.DataFrame({"year": [2024, 2025]}),
        "status_long": pd.DataFrame({"report_year": [2024, 2025], "native_count": [10, 11]}),
        "mapped_fleet": pd.DataFrame({"MODEL_YEAR": [2020, 2021], "FIT_ACTIVE": [10, 20]}),
        "mapping_config": pd.DataFrame({"entry_type": ["mto_crosswalk"]}),
    }
    for key, adapter, value_column in [
        ("nhtsa_survival_input", "nhtsa_cafe", "survival_rate"),
        ("eia_survival_input", "eia_nems", "annual_scrappage_rate"),
        ("moves5_survival_input", "epa_moves5", "annual_survival_rate"),
    ]:
        rule = assorted[adapter]
        classes = rule.get("class_columns", {"heavy": rule.get("source_vehicle_class_label")})
        frames[key] = pd.DataFrame([
            {
                "source_id": rule["source_id"],
                "source_vehicle_class_label": source_class,
                "vehicle_age": age,
                "unit": rule["unit"],
                value_column: (
                    .9 ** age if adapter == "nhtsa_cafe"
                    else (1.0 if age == 0 else .9) if adapter == "epa_moves5"
                    else .1
                ),
            }
            for source_class in classes.values()
            for age in range(rule["expected_ages"]["start"], rule["expected_ages"]["end"] + 1)
        ])
    paths = {}
    for key, frame in frames.items():
        path = tmp_path / f"{key}.csv"
        frame.to_csv(path, index=False)
        paths[key] = path
    metadata = {
        "build_id": "stock-build-1", "built_at": "2026-10-17T20:00:00Z",
        "prepare_command": "uv run python src/build_transport.py --scenario diagnostic.yaml --overwrite",
        "database": {"path": "snapshot.sqlite", "sha256": "recorded-hash"},
    }
    return SimpleNamespace(bundle=bundle, paths=paths, metadata=metadata), frames


def test_collection_keeps_comparison_reports_in_the_recorded_build_edition(bundle, monkeypatch):
    observed = []

    def validated_history(actual_bundle):
        assert actual_bundle is bundle
        observed.append("validated")
        return pd.DataFrame({"year": [2024, 2026]})

    monkeypatch.setattr(diagnostic, "load_history_manifest", validated_history)
    monkeypatch.setattr(diagnostic.pd, "read_csv", lambda *args, **kwargs: pd.DataFrame({"report_year": [2024, 2026]}))
    paths = diagnostic.collect_stock_inputs(bundle)
    assert observed == ["validated"]
    assert len(paths) == 23
    selected_year = str(bundle.scenario.existing_capacity.vehicle_population_year)
    assert selected_year in paths["report4_distribution"].name
    assert selected_year in paths["report5_distribution"].name
    assert "2026" not in paths["report4_distribution"].name
    assert paths["source_manifest"].parent == resolve_artifact_path(bundle, "ontario_vehicle_population")
    assert {"mapping_config", "nhtsa_survival_input", "eia_survival_input", "moves5_survival_input"} <= paths.keys()


def test_collection_rejects_unvalidated_history_before_capture(bundle, monkeypatch):
    def invalid_history(_bundle):
        raise ValueError("Missing registered edition")

    monkeypatch.setattr(diagnostic, "load_history_manifest", invalid_history)
    with pytest.raises(ValueError, match="Missing registered edition"):
        diagnostic.collect_stock_inputs(bundle)


def test_collection_rejects_partial_status_history_before_capture(bundle, monkeypatch):
    monkeypatch.setattr(diagnostic, "load_history_manifest", lambda _: pd.DataFrame({"year": [2024, 2025]}))
    monkeypatch.setattr(diagnostic.pd, "read_csv", lambda *args, **kwargs: pd.DataFrame({"report_year": [2025]}))
    with pytest.raises(ValueError, match="Incomplete Ontario registration-status history"):
        diagnostic.collect_stock_inputs(bundle)


def test_loader_preserves_frames_and_survival_results_without_live_source_reads(snapshot, monkeypatch):
    recorded, frames = snapshot
    calls = []

    def load_recorded(profile, *, repo_root=None):
        calls.append((profile, repo_root))
        return recorded

    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", load_recorded)
    monkeypatch.setattr(diagnostic, "load_history_manifest", lambda _: pytest.fail("Read live history"))
    loaded = diagnostic.load_stock_diagnostic(repo_root=Path("recorded-root"))
    assert calls == [("vehicle_stock", Path("recorded-root"))]
    for key in frames:
        pd.testing.assert_frame_equal(loaded[key], pd.read_csv(recorded.paths[key], low_memory=False))
    assert loaded["mapped_fleet"].FIT_ACTIVE.sum() == frames["mapped_fleet"].FIT_ACTIVE.sum()
    _, curves = transform_source_survival_curves(
        loaded["nhtsa_survival_input"], loaded["eia_survival_input"], loaded["moves5_survival_input"],
    )
    expected = curves.loc[curves.source_id.isin([
        "eia_nems_hd_truck_scrappage", "epa_moves_heavy_truck_survival",
    ]) & curves.source_class.isin([
        "Cls 7-8", "Heavy-duty Vehicles (Buses, Single Unit Trucks and Combination Trucks)",
    ])].reset_index(drop=True)
    pd.testing.assert_frame_equal(loaded["heavy_truck_survival"], expected)
    pd.testing.assert_frame_equal(
        loaded["heavy_truck_medians"],
        median_equivalent_lifetimes(expected, interpolation=loaded["heavy_truck_median_interpolation"]),
    )


def test_loader_rejects_partial_recorded_status_history(snapshot, monkeypatch):
    recorded, _ = snapshot
    pd.DataFrame({"report_year": [2025]}).to_csv(recorded.paths["status_long"], index=False)
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    with pytest.raises(ValueError, match="Incomplete Ontario registration-status history"):
        diagnostic.load_stock_diagnostic()


def test_interpretation_uses_recorded_switches(snapshot, monkeypatch):
    recorded, _ = snapshot
    lifetimes = recorded.bundle.scenario.lifetimes.model_copy(update={
        "heavy_truck_survival_source": "epa_moves_heavy_truck_survival",
    })
    recorded.bundle = replace(recorded.bundle, scenario=recorded.bundle.scenario.model_copy(update={
        "lifetimes": lifetimes,
    }))
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    loaded = diagnostic.load_stock_diagnostic()
    assert loaded["build_metadata"] is recorded.metadata
    assert loaded["heavy_truck_selected_source"] == "epa_moves_heavy_truck_survival"
    assert loaded["scenario_switches"].set_index("switch").loc[
        "heavy_truck_survival_source", "value"
    ] == "epa_moves_heavy_truck_survival"


def test_notebook_exposes_reload_without_reader_scenario_selection():
    notebook = Path("docs/insights/vehicle_stock_diagnosis.py").read_text(encoding="utf-8")
    assert "load_stock_diagnostic()" in notebook
    assert "Reload latest completed build" in notebook
    assert "scenario_selector" not in notebook
    assert "legacy_reproduction.yaml" not in notebook
    assert "default.yaml" not in notebook
