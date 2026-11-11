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
def snapshot(bundle, tmp_path, monkeypatch):
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    frames = {
        "source_manifest": pd.DataFrame({"year": [2024, 2025]}),
        "status_long": pd.DataFrame({"report_year": [2024, 2025], "native_count": [10, 11]}),
        "mapped_fleet": pd.DataFrame({"MODEL_YEAR": [2020, 2021], "FIT_ACTIVE": [10, 20]}),
        "mapping_config": pd.DataFrame({"entry_type": ["mto_crosswalk"]}),
        "unresolved_reason_detail": pd.DataFrame(columns=["MAKE", "MODEL", "unresolved_reason"]),
        "vehicle_reference_canonical": pd.DataFrame({"vehicle_id": ["MINI/COOPERSE3DOOR"], "canonical_make": ["MINI"], "canonical_model": ["Cooper SE 3 Door"], "model_year_from": [2024], "model_year_to": [2024], "powertrain": ["bev"]}),
        "vehicle_reference_links": pd.DataFrame({"vehicle_id": ["MINI:COOPERSE3DOOR"], "source": ["tc_izev"], "source_model": ["Cooper SE"]}),
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
        if key in {"mapping_config", "vehicle_reference_canonical", "vehicle_reference_links"}:
            continue
        path = tmp_path / f"{key}.csv"
        frame.to_csv(path, index=False)
        paths[key] = path
    monkeypatch.setattr(diagnostic, "load_vehicle_reference", lambda captured: SimpleNamespace(
        canonical=frames["vehicle_reference_canonical"], links=frames["vehicle_reference_links"],
    ))
    monkeypatch.setattr(diagnostic, "reference_mto_mapping", lambda *args: frames["mapping_config"])
    monkeypatch.setattr(diagnostic, "_historical_mapping_evidence", lambda *args, **kwargs: (pd.DataFrame(), pd.DataFrame()))
    metadata = {
        "build_id": "stock-build-1", "built_at": "2026-10-17T20:00:00Z",
        "prepare_command": "uv run python src/build_transport.py --scenario diagnostic.yaml --overwrite",
        "database": {"path": "snapshot.sqlite", "sha256": "recorded-hash"},
    }
    return SimpleNamespace(bundle=bundle, paths=paths, metadata=metadata), frames


def test_historical_mapping_respects_each_year_scope_and_preserves_counts(bundle):
    from parameterization.vehicle_reference import load_vehicle_reference, reference_mto_mapping

    mapping = reference_mto_mapping(
        load_vehicle_reference(bundle), load_harmonization_rules(bundle, "road_aggregation"),
        load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"],
    )
    statuses = pd.DataFrame([
        (2024, "CHEV", "ORL", 2013, 10, "FIT_ACTIVE"),
        (2025, "CHEV", "ORL", 2024, 20, "FIT_ACTIVE"),
        (2024, "LEXS", "RX", 2016, 15, "FIT_ACTIVE"),
        (2025, "LEXS", "RX", 2026, 16, "FIT_ACTIVE"),
        (2024, "NOPE", "Z", 2020, 3, "FIT_ACTIVE"),
        (2025, "NOPE", "Y", 2020, 4, "FIT_ACTIVE"),
        (2024, "****", "***", 2020, 2, "FIT_ACTIVE"),
        (2025, "CHEV", "ORL", 2013, 100, "FIT_INACTIVE"),
    ], columns=["report_year", "MAKE", "MODEL", "MODEL_YEAR", "native_count", "stock_status"])
    statuses["VEHICLE_CLASS"] = "PASSENGER"
    latest = pd.DataFrame([{
        "MAKE": "NOPE", "MODEL": "Y", "candidate_status": "ranked_candidate",
        "candidate_canonical_make": "Fixture", "candidate_canonical_model": "Y",
        "candidate_match_method": "exact_normalized",
    }])
    outcomes, detail = diagnostic._historical_mapping_evidence(
        statuses, mapping, latest,
        accepted_statuses=set(load_harmonization_rules(bundle, "road_aggregation")["accepted_mapping_statuses"]),
    )
    assert outcomes.native_count.sum() == 70
    assert len(outcomes) == 7
    assert outcomes.loc[outcomes.MAKE.eq("LEXS") & outcomes.MODEL_YEAR.eq(2026), "mapping_outcome"].item() == "mapped_ldv"
    reasons = detail.set_index(["MAKE", "MODEL", "MODEL_YEAR"]).unresolved_reason.to_dict()
    assert reasons["CHEV", "ORL", 2024] == "no_applicable_reviewed_interval"
    assert reasons["LEXS", "RX", 2016] == "reviewed_family_class_unresolved"
    assert reasons["NOPE", "Z", 2020] == "not_present_in_latest_snapshot"
    assert reasons["NOPE", "Y", 2020] == "unreviewed_high_confidence_candidate"
    assert reasons["****", "***", 2020] == "suppressed_or_unknown_code"


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
    assert len(paths) == 22
    selected_year = str(bundle.scenario.existing_capacity.vehicle_population_year)
    assert selected_year in paths["report4_distribution"].name
    assert selected_year in paths["report5_distribution"].name
    assert "2026" not in paths["report4_distribution"].name
    assert paths["source_manifest"].parent == resolve_artifact_path(bundle, "ontario_vehicle_population")
    assert {"nhtsa_survival_input", "eia_survival_input", "moves5_survival_input"} <= paths.keys()
    assert not any(key.startswith("vehicle_reference_") for key in paths)
    assert "mapping_config" not in paths


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
        expected = pd.read_csv(recorded.paths[key], low_memory=False) if key in recorded.paths else frames[key]
        pd.testing.assert_frame_equal(loaded[key], expected)
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


def test_loader_uses_captured_parameter_bundle_without_live_fallback(snapshot, monkeypatch):
    recorded, _ = snapshot
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    def unavailable(captured):
        assert captured is recorded.bundle
        raise FileNotFoundError("Captured authoritative vehicle table is missing")
    monkeypatch.setattr(diagnostic, "load_vehicle_reference", unavailable)
    with pytest.raises(FileNotFoundError, match="Captured authoritative vehicle table"):
        diagnostic.load_stock_diagnostic()


def test_interpretation_uses_recorded_switches(snapshot, monkeypatch):
    recorded, _ = snapshot
    lifetimes = recorded.bundle.scenario.lifetimes.model_copy(update={
        "road_lifetime_survival_source": "moves",
    })
    recorded.bundle = replace(recorded.bundle, scenario=recorded.bundle.scenario.model_copy(update={
        "lifetimes": lifetimes,
    }))
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    loaded = diagnostic.load_stock_diagnostic()
    assert loaded["build_metadata"] is recorded.metadata
    assert loaded["heavy_truck_selected_source"] == "epa_moves_heavy_truck_survival"
    assert loaded["scenario_switches"].set_index("switch").loc[
        "road_lifetime_survival_source", "value"
    ] == "moves"


def test_notebook_exposes_reload_without_reader_scenario_selection():
    notebook = Path("docs/insights/vehicle_stock_diagnosis.py").read_text(encoding="utf-8")
    assert "load_stock_diagnostic()" in notebook
    assert "Reload latest completed build" in notebook
    assert "scenario_selector" not in notebook
    assert "legacy_reproduction.yaml" not in notebook
    assert "default.yaml" not in notebook


def test_reference_view_keeps_attribute_grain_without_multiplying_linked_observations():
    import runpy

    tables = []
    view = runpy.run_path("docs/insights/vehicle_stock_diagnosis.py")["canonical_reference_inventory"]
    evidence = {
        "vehicle_reference_canonical": pd.DataFrame({
            "vehicle_id": ["rated", "rated", "unlinked"], "canonical_make": ["Make"] * 3,
            "canonical_model": ["Rated family", "Rated family", "No MTO link"],
            "model_year_from": [2023, 2024, 2024], "model_year_to": [2023, 2024, 2024],
            "powertrain": ["bev", "bev", "hev"], "nrcan_vehicle_class": ["Compact"] * 3,
            "evidence_status": ["catalogue_supported"] * 3, "notes": [None] * 3,
        }),
        "vehicle_reference_links": pd.DataFrame({
            "vehicle_id": ["rated", "rated"], "source": ["ontario_ministry_transport_vehicle_population"] * 2,
            "source_model": ["alias A", "alias B"], "model_year_from": [2023, 2024], "model_year_to": [2023, 2024],
        }),
        "mapped_fleet": pd.DataFrame({"FIT_ACTIVE": [17]}),
    }
    fake_mo = SimpleNamespace(
        ui=SimpleNamespace(table=lambda frame, **_kwargs: tables.append(frame.copy()) or "table"),
        accordion=lambda items: items, vstack=lambda items: items,
    )
    view.run(evidence=evidence, mo=fake_mo, reference_make=SimpleNamespace(value="All makes"),
             reference_audit_family=SimpleNamespace(value="rated"),
             reference_powertrain=SimpleNamespace(value=None), reference_year=SimpleNamespace(value=2024))
    assert len(tables[0]) == 2
    assert set(tables[0]["Canonical model"]) == {"Rated family", "No MTO link"}
    assert tables[0].loc[tables[0]["Supported powertrain"].eq("BEV"), "First model year"].item() == 2024
    assert "Median electric range (km)" not in tables[0]
    assert "notes" not in tables[1]
    assert tables[2].source_model.tolist() == ["alias B"]
    assert evidence["mapped_fleet"].FIT_ACTIVE.sum() == 17


@pytest.mark.parametrize("powertrain", ["gasoline", "hev", "unknown"])
def test_reference_view_drops_null_range_and_unused_source_columns(powertrain):
    import runpy

    tables = []
    view = runpy.run_path("docs/insights/vehicle_stock_diagnosis.py")["canonical_reference_inventory"]
    evidence = {
        "vehicle_reference_canonical": pd.DataFrame({"vehicle_id": ["one"], "canonical_make": ["Make"], "canonical_model": ["Family"], "model_year_from": [2024], "model_year_to": [2024], "powertrain": [powertrain]}),
        "vehicle_reference_links": pd.DataFrame({"vehicle_id": ["one"], "model_year_from": [2024], "model_year_to": [2024], "source_vehicle_class": [None]}),
    }
    fake_mo = SimpleNamespace(ui=SimpleNamespace(table=lambda frame, **_kwargs: tables.append(frame.copy()) or "table"),
                             accordion=lambda items: items, vstack=lambda items: items)
    view.run(evidence=evidence, mo=fake_mo, reference_make=SimpleNamespace(value="All makes"),
             reference_audit_family=SimpleNamespace(value="one"), reference_year=SimpleNamespace(value=2024),
             reference_powertrain=SimpleNamespace(value=powertrain))
    assert "Median electric range (km)" not in tables[0]
    assert "source_vehicle_class" not in tables[2]


def test_notebook_adapter_preserves_unknown_canonical_fields_without_nan_identities():
    import runpy

    adapter = runpy.run_path("docs/insights/vehicle_stock_diagnosis.py")["captured_vehicle_evidence"]
    frame = pd.DataFrame({"canonical_make": [None, "Honda", "Honda"], "canonical_model": [None, "", "Civic"],
                          "vehicle_scope": ["identity_unresolved", "identity_unresolved", "ldv"]})
    evidence = {key: frame.copy() for key in ("mapped_fleet", "mapping_config", "vehicle_class_evidence")}
    _, definitions = adapter.run(load_stock_diagnostic=lambda: evidence,
                                 reload_evidence=SimpleNamespace(value=0))
    for key in ("mapped_fleet", "mapping_config", "vehicle_class_evidence"):
        result = definitions["evidence"][key]
        assert pd.isna(result.loc[0, "canonical_make"]) and pd.isna(result.loc[0, "canonical_model"])
        assert result.loc[1, "canonical_make"] == "Honda" and result.loc[1, "canonical_model"] == ""
