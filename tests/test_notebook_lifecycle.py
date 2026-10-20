"""Opt-in review prerequisites and last-successful notebook publication."""

from dataclasses import replace
from pathlib import Path
import os
import shutil

import pytest

import build_transport
from utils import load_config_bundle
from validation.notebook_evidence import NotebookPublication


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("profiles", [[], ["vehicle_stock"], ["efficiency"], ["vehicle_stock", "efficiency"]])
def test_notebook_prerequisites_are_explicit_and_ordered(monkeypatch, profiles):
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml")
    scenario = bundle.scenario.model_copy(deep=True)
    scenario.notebook_diagnostics = profiles
    bundle = replace(bundle, scenario=scenario)
    calls = []

    def record(name):
        return lambda *args, **kwargs: calls.append((name, kwargs))

    targets = {
        "fetching.nrcan_ceud.fetch_and_normalize": "ceud",
        "fetching.nrcan_ceud.fetch_and_normalize_ratings": "ratings",
        "fetching.statcan_tables.fetch_and_normalize": "statcan",
        "fetching.cer_enerfuture.fetch_and_normalize": "cer",
        "fetching.nlr_atb_autonomie.fetch_and_normalize": "atb",
        "fetching.assorted_sources.fetch_and_normalize": "assorted",
        "fetching.assorted_sources.fetch_and_normalize_tc_dashboard": "dashboard",
        "fetching.vehicle_population.fetch_and_normalize": "ontario",
        "fetching.fueleconomy_vehicles.validate_cache": "validate_fueleconomy",
        "fetching.fueleconomy_vehicles.fetch_and_normalize": "normalize_fueleconomy",
        "parameterization.road_aggregation.build_road_aggregation_artifacts": "aggregation",
        "parameterization.road_aggregation.build_mapping_diagnostic_artifacts": "mapping_review",
        "fetching.assorted_sources.fetch_and_normalize_moves5_survival": "moves_review",
        "parameterization.road_lifetimes_survival.build_mto_survival_diagnostic_artifacts": "retention_review",
    }
    for target, name in targets.items():
        monkeypatch.setattr(target, record(name))
    monkeypatch.setattr("fetching.fueleconomy_vehicles.build_request", lambda bundle: None)
    build_transport.prepare_scenario_inputs(bundle, download=False)
    names = [name for name, kwargs in calls]
    assert ("normalize_fueleconomy" in names) == bool(profiles)
    assert ("mapping_review" in names) == ("vehicle_stock" in profiles)
    assert ("retention_review" in names) == ("vehicle_stock" in profiles)
    assert ("moves_review" in names) == ("vehicle_stock" in profiles)
    ontario = [kwargs for name, kwargs in calls if name == "ontario"]
    assert len(ontario) == (2 if "vehicle_stock" in profiles else 1)
    assert ontario[-1]["year"] == scenario.existing_capacity.vehicle_population_year
    assert all(kwargs.get("download", False) is False for _, kwargs in calls)
    if "vehicle_stock" in profiles:
        assert "year" not in ontario[0]
        assert names.index("mapping_review") < names.index("retention_review")


@pytest.mark.parametrize("publication_fails", [False, True])
def test_notebook_index_advances_only_after_database_publication(tmp_path, monkeypatch, publication_fails):
    shutil.copytree(ROOT / "config", tmp_path / "config")
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml", repo_root=tmp_path)
    monkeypatch.setattr(build_transport, "load_config_bundle", lambda path: bundle)
    monkeypatch.setattr(build_transport, "prepare_scenario_inputs", lambda *args, **kwargs: None)
    calls = []
    database = tmp_path / "outputs/sqlite" / bundle.scenario.outputs.sqlite_name
    database.parent.mkdir(parents=True)
    database.write_bytes(b"previous database")
    report_path = tmp_path / bundle.scenario.outputs.validation_report
    report_path.parent.mkdir(parents=True)
    report_path.write_bytes(b"previous report")
    index_path = tmp_path / "outputs/validation/notebook_evidence/latest.json"
    index_path.parent.mkdir(parents=True)
    index_path.write_text('{"version":1,"bundles":{}}', encoding="utf-8")
    previous_index = index_path.read_bytes()

    def bootstrap(**kwargs):
        kwargs["database_path"].write_bytes(b"validated candidate")
        return {"validation": {"foreign_key_violations": 0}}

    def stage(bundle, report):
        from validation.notebook_evidence import NotebookPublication
        calls.append("snapshot")
        assert report["database_sha256"]
        return NotebookPublication(index_path, {"vehicle_stock": "builds/example/manifest.json"})

    original_replace = os.replace

    def publish_file(source, destination):
        if Path(destination) == database:
            calls.append("database")
        if Path(destination) == index_path:
            calls.append("index")
            if publication_fails:
                raise PermissionError("index locked")
        return original_replace(source, destination)

    monkeypatch.setattr(build_transport, "bootstrap_database", bootstrap)
    monkeypatch.setattr(os, "replace", publish_file)
    monkeypatch.setattr("validation.rating_stock_benchmark.build_rating_stock_benchmark", lambda bundle: calls.append("benchmark"))
    monkeypatch.setattr("validation.notebook_evidence.stage_notebook_evidence", stage)
    monkeypatch.setattr("validation.notebook_evidence.prune_notebook_evidence",
                        lambda publication: calls.append("prune") or [])
    if publication_fails:
        with pytest.raises(PermissionError, match="index locked"):
            build_transport.build_from_scenario(bundle.scenario_path, overwrite=True)
        assert calls == ["benchmark", "snapshot", "database", "index", "database"]
        assert database.read_bytes() == b"previous database"
        assert report_path.read_bytes() == b"previous report"
        assert index_path.read_bytes() == previous_index
    else:
        build_transport.build_from_scenario(bundle.scenario_path, overwrite=True)
        assert calls == ["benchmark", "snapshot", "database", "index", "prune"]
        assert database.read_bytes() == b"validated candidate"
        assert index_path.read_bytes() != previous_index


def test_invalid_previous_index_fails_before_database_and_report_replacement(tmp_path):
    database, candidate, report, index = [tmp_path / name for name in
                                          ("db.sqlite", "candidate.sqlite", "report.json", "latest.json")]
    database.write_bytes(b"previous database")
    candidate.write_bytes(b"validated candidate")
    report.write_bytes(b"previous report")
    index.write_bytes(b"invalid previous index")
    with pytest.raises(ValueError):
        build_transport._publish_scenario(
            candidate, database, {"ok": True}, report,
            notebook_publication=NotebookPublication(index, {"vehicle_stock": "builds/new/manifest.json"}),
        )
    assert database.read_bytes() == b"previous database"
    assert report.read_bytes() == b"previous report"
    assert index.read_bytes() == b"invalid previous index"


def test_config_change_during_capture_fails_before_publication(tmp_path, monkeypatch):
    shutil.copytree(ROOT / "config", tmp_path / "config")
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml", repo_root=tmp_path)
    monkeypatch.setattr(build_transport, "load_config_bundle", lambda path: bundle)
    monkeypatch.setattr(build_transport, "prepare_scenario_inputs", lambda *args, **kwargs: None)
    monkeypatch.setattr("validation.rating_stock_benchmark.build_rating_stock_benchmark", lambda bundle: None)

    def bootstrap(**kwargs):
        kwargs["database_path"].write_bytes(b"candidate")
        return {}

    def capture(bundle, report):
        controls = tmp_path / "config/parameters/rules.yaml"
        controls.write_text(controls.read_text(encoding="utf-8") + "\n# changed during capture\n", encoding="utf-8")
        return NotebookPublication(tmp_path / "latest.json", {})

    monkeypatch.setattr(build_transport, "bootstrap_database", bootstrap)
    monkeypatch.setattr("validation.notebook_evidence.stage_notebook_evidence", capture)
    monkeypatch.setattr(build_transport, "_publish_scenario", lambda *args, **kwargs: pytest.fail("Published mismatched config"))
    with pytest.raises(ValueError, match="configuration changed while recording"):
        build_transport.build_from_scenario(bundle.scenario_path, overwrite=True)
