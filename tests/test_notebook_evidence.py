"""Recorded data identity, independent configuration and successful-build refresh."""

from dataclasses import replace
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess

import pandas as pd
import pytest
from pydantic import ValidationError
import yaml

from utils import file_sha256, load_config_bundle, load_harmonization_rules
from validation import notebook_evidence as layer
from validation.config_models import ScenarioConfig


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    shutil.copytree(ROOT / "config", tmp_path / "config")
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml", repo_root=tmp_path)
    scenario = bundle.scenario.model_copy(deep=True)
    scenario.notebook_diagnostics = ["vehicle_stock"]
    bundle = replace(bundle, scenario=scenario)
    # The on-disk scenario is what the build records.
    bundle.scenario_path.write_text(yaml.safe_dump(scenario.model_dump(mode="json")), encoding="utf-8")
    table = tmp_path / "prepared.csv"
    table.write_text("year,value\n2023,7\n", encoding="utf-8")
    database = tmp_path / "outputs/sqlite/example.sqlite"
    database.parent.mkdir(parents=True)
    database.write_bytes(b"validated database fixture")
    monkeypatch.setattr("validation.vehicle_stock_diagnostic.collect_stock_inputs", lambda bundle: {"sample": table})
    report = {"timestamp_utc": "2026-10-17T15:00:00+00:00", "database": str(database),
              "database_sha256": file_sha256(database)}
    return bundle, report, table


def test_capture_does_not_publish_until_build_succeeds(prepared):
    bundle, report, _ = prepared
    publication = layer.stage_notebook_evidence(bundle, report)
    assert not publication.index_path.exists()
    with pytest.raises(FileNotFoundError, match="Run first:.*notebook_diagnostics.yaml"):
        layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    layer.publish_notebook_evidence(publication)
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert pd.read_csv(loaded.paths["sample"]).to_dict("records") == [{"year": 2023, "value": 7}]
    assert loaded.metadata["database"]["sha256"] == report["database_sha256"]
    assert loaded.metadata["built_at"] == "2026-10-17T15:00:00Z"
    assert loaded.metadata["build_id"] == "notebook_diagnostics-20261017T150000Z"
    # Configuration refers to this native CSV name; only data copies use .csv.gz.
    mapping = loaded.bundle.paths.config.parameters
    recorded_map = Path(mapping) / "vehicle_size_class_map.csv"
    source_map = bundle.repo_root / "config/parameters/vehicle_size_class_map.csv"
    assert recorded_map.read_bytes() == source_map.read_bytes()
    assert not pd.read_csv(recorded_map).empty


def test_build_ids_are_utc_windows_safe_and_distinguish_same_second(prepared):
    bundle, report, _ = prepared
    bundle.scenario.scenario.name = "Ontario: diagnostic / run"
    report["timestamp_utc"] = "2026-10-17T11:00:00-04:00"
    first = layer.stage_notebook_evidence(bundle, report)
    second = layer.stage_notebook_evidence(bundle, report)
    assert first.bundles["vehicle_stock"].split("/")[1] == "Ontario-diagnostic-run-20261017T150000Z"
    assert second.bundles["vehicle_stock"].split("/")[1] == "Ontario-diagnostic-run-20261017T150000Z-2"
    layer.publish_notebook_evidence(second)
    removed = layer.prune_notebook_evidence(second)
    assert [path.name for path in removed] == ["Ontario-diagnostic-run-20261017T150000Z"]
    third = layer.stage_notebook_evidence(bundle, report)
    assert third.bundles["vehicle_stock"].split("/")[1] == "Ontario-diagnostic-run-20261017T150000Z-3"


def test_historical_hex_capture_can_be_loaded_and_pruned(prepared):
    bundle, report, _ = prepared
    historical = layer.stage_notebook_evidence(bundle, report)
    original = historical.index_path.parent / historical.bundles["vehicle_stock"]
    legacy_id = "a" * 32
    legacy_release = original.parents[1].with_name(legacy_id)
    original.parents[1].rename(legacy_release)
    manifest = legacy_release / "vehicle_stock/manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["build_id"] = legacy_id
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    historical = layer.NotebookPublication(historical.index_path, {
        "vehicle_stock": f"builds/{legacy_id}/vehicle_stock/manifest.json",
    })
    layer.publish_notebook_evidence(historical)
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert loaded.metadata["build_id"] == legacy_id
    current = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(current)
    assert legacy_release in layer.prune_notebook_evidence(current)


def test_live_data_and_config_edits_do_not_reinterpret_recorded_build(prepared):
    bundle, report, table = prepared
    original_version = load_harmonization_rules(bundle, "efficiencies")["transformation_version"]
    layer.publish_notebook_evidence(layer.stage_notebook_evidence(bundle, report))
    table.write_text("year,value\n2024,999\n", encoding="utf-8")
    bundle.scenario_path.write_text("invalid: edited after build\n", encoding="utf-8")
    rules_path = bundle.repo_root / "config/parameters/rules.yaml"
    rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    rules["parameterization"]["efficiencies"]["transformation_version"] = "later"
    rules_path.write_text(yaml.safe_dump(rules), encoding="utf-8")
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert pd.read_csv(loaded.paths["sample"]).to_dict("records") == [{"year": 2023, "value": 7}]
    assert loaded.bundle.scenario.periods.base_year == bundle.scenario.periods.base_year
    assert load_harmonization_rules(loaded.bundle, "efficiencies")["transformation_version"] == original_version


def test_next_successful_build_refreshes_data_then_prunes_superseded_snapshot(prepared):
    bundle, report, table = prepared
    first = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(first)
    previous = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    table.write_text("year,value\n2023,8\n", encoding="utf-8")
    second = layer.stage_notebook_evidence(bundle, report | {"timestamp_utc": "2026-10-18T15:00:00+00:00"})
    assert layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root).metadata["build_id"] == previous.metadata["build_id"]
    layer.publish_notebook_evidence(second)
    current = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert current.metadata["build_id"] != previous.metadata["build_id"]
    assert pd.read_csv(current.paths["sample"]).value.tolist() == [8]
    assert pd.read_csv(previous.paths["sample"]).value.tolist() == [7]
    removed = layer.prune_notebook_evidence(second)
    assert len(removed) == 1
    assert not previous.paths["sample"].exists()
    assert current.paths["sample"].is_file()


def test_changed_snapshot_is_rejected(prepared):
    bundle, report, _ = prepared
    layer.publish_notebook_evidence(layer.stage_notebook_evidence(bundle, report))
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    loaded.paths["sample"].write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="changed recorded notebook evidence"):
        layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)


@pytest.mark.parametrize("profile", ["vehicle_stock", "efficiency"])
def test_incompatible_captured_config_explains_rebuild_without_mutating_evidence(prepared, monkeypatch, profile):
    bundle, report, table = prepared
    bundle.scenario.notebook_diagnostics = [profile]
    monkeypatch.setattr("validation.ldv_efficiency_diagnostic.collect_efficiency_inputs", lambda bundle: {"sample": table})
    bundle.scenario_path.write_text(yaml.safe_dump(bundle.scenario.model_dump(mode="json")), encoding="utf-8")
    original = bundle.paths_path.read_bytes()
    older_paths = yaml.safe_load(original)
    del older_paths["outputs"]["build_log"]
    # Simulate a valid capture made before build_log became a required field.
    bundle.paths_path.write_text(yaml.safe_dump(older_paths), encoding="utf-8")
    try:
        publication = layer.stage_notebook_evidence(bundle, report)
    finally:
        bundle.paths_path.write_bytes(original)
    layer.publish_notebook_evidence(publication)
    manifest = publication.index_path.parent / publication.bundles[profile]
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    captured_paths = manifest.parent / metadata["config"]["paths"]["path"]
    watched = [publication.index_path, manifest, captured_paths, bundle.paths_path, table]
    hashes = {path: file_sha256(path) for path in watched}

    with pytest.raises(ValueError, match="incompatible with the current backend") as caught:
        layer.load_notebook_evidence(profile, repo_root=bundle.repo_root)

    message = str(caught.value)
    assert profile in message
    assert metadata["build_id"] in message
    assert "outputs.build_log" in message
    assert f"Rebuild with: {layer.prepare_command(bundle.repo_root)}" in message
    assert f"repository root: {bundle.repo_root}" in message
    assert isinstance(caught.value.__cause__, ValidationError)
    assert {path: file_sha256(path) for path in watched} == hashes
    # A current native build still requires the YAML-owned log path.
    with pytest.raises(ValidationError, match="outputs.build_log"):
        type(bundle.paths).model_validate(older_paths)


def test_missing_inputs_leave_latest_successful_build_unchanged(prepared):
    bundle, report, table = prepared
    publication = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(publication)
    previous = publication.index_path.read_bytes()
    table.unlink()
    with pytest.raises(FileNotFoundError, match="incomplete notebook evidence"):
        layer.stage_notebook_evidence(bundle, report)
    assert publication.index_path.read_bytes() == previous


def test_manifest_cannot_escape_evidence_root(prepared):
    bundle, report, _ = prepared
    publication = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(publication)
    publication.index_path.write_text(json.dumps({"version": 1, "bundles": {"vehicle_stock": "../../outside.json"}}))
    with pytest.raises(ValueError, match="escapes its root"):
        layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)


def test_relocated_bundle_uses_its_own_verified_parameters(prepared, tmp_path):
    bundle, report, _ = prepared
    original_version = load_harmonization_rules(bundle, "efficiencies")["transformation_version"]
    layer.publish_notebook_evidence(layer.stage_notebook_evidence(bundle, report))
    relocated = tmp_path / "relocated"
    shutil.copytree(bundle.repo_root / "config", relocated / "config")
    shutil.copytree(bundle.repo_root / "outputs/validation/notebook_evidence",
                    relocated / "outputs/validation/notebook_evidence")
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=relocated)
    assert Path(loaded.bundle.paths.config.parameters).is_relative_to(relocated)
    assert load_harmonization_rules(loaded.bundle, "efficiencies")["transformation_version"] == original_version


def test_csv_snapshot_compression_is_lossless_deterministic_and_hashes_stored_bytes(prepared):
    bundle, report, table = prepared
    table.write_text("year,value,source\n" + "2023,7,repeated provenance\n" * 1000, encoding="utf-8")
    original = table.read_bytes()
    first = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(first)
    recorded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    compressed = recorded.paths["sample"]
    assert compressed.name == "sample.csv.gz"
    assert gzip.decompress(compressed.read_bytes()) == original
    assert len(compressed.read_bytes()) < len(original) / 10
    assert table.read_bytes() == original
    assert recorded.metadata["files"]["sample"]["sha256"] == file_sha256(compressed)
    assert recorded.metadata["files"]["sample"]["sha256"] != file_sha256(table)
    manifest = Path(recorded.metadata["manifest"])
    config = manifest.parent / recorded.metadata["config"]["rules"]["path"]
    assert config.suffix == ".yaml"
    assert config.read_bytes() == (bundle.repo_root / "config/parameters/rules.yaml").read_bytes()
    second = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(second)
    latest = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert latest.paths["sample"].read_bytes() == compressed.read_bytes()


def test_existing_version_one_plain_csv_snapshots_remain_readable(prepared):
    bundle, report, table = prepared
    layer.publish_notebook_evidence(layer.stage_notebook_evidence(bundle, report))
    recorded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    manifest_path = Path(recorded.metadata["manifest"])
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    legacy = recorded.paths["sample"].with_suffix("")
    legacy.write_bytes(gzip.decompress(recorded.paths["sample"].read_bytes()))
    data["files"]["sample"]["path"] = "files/sample.csv"
    data["files"]["sample"]["sha256"] = file_sha256(legacy)
    manifest_path.write_text(json.dumps(data), encoding="utf-8")
    loaded = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    assert loaded.paths["sample"].read_bytes() == table.read_bytes()


def test_changing_original_during_compression_rejects_capture(prepared, monkeypatch):
    bundle, report, table = prepared
    original_copy = layer.shutil.copyfileobj

    def mutate_source(original, compressed):
        original_copy(original, compressed)
        table.write_text("changed during capture", encoding="utf-8")

    monkeypatch.setattr(layer.shutil, "copyfileobj", mutate_source)
    with pytest.raises(ValueError, match="changed during snapshot"):
        layer.stage_notebook_evidence(bundle, report)
    assert not (bundle.repo_root / "outputs/validation/notebook_evidence/latest.json").exists()


def test_failed_index_publication_preserves_previous_latest_and_pending_capture_is_prunable(prepared, monkeypatch):
    bundle, report, table = prepared
    first = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(first)
    previous = first.index_path.read_bytes()
    table.write_text("year,value\n2023,8\n", encoding="utf-8")
    pending = layer.stage_notebook_evidence(bundle, report)

    def fail_write(*args, **kwargs):
        raise OSError("publication failed")

    monkeypatch.setattr(layer, "write_text_atomic", fail_write)
    with pytest.raises(OSError, match="publication failed"):
        layer.publish_notebook_evidence(pending)
    assert first.index_path.read_bytes() == previous
    assert pd.read_csv(layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root).paths["sample"]).value.tolist() == [7]
    removed = layer.prune_notebook_evidence(first)
    assert len(removed) == 1
    assert first.index_path.read_bytes() == previous


def test_pruning_preserves_builds_referenced_by_either_profile_and_removes_orphans(prepared, monkeypatch):
    bundle, report, table = prepared
    monkeypatch.setattr("validation.ldv_efficiency_diagnostic.collect_efficiency_inputs", lambda bundle: {"sample": table})
    bundle.scenario.notebook_diagnostics = ["vehicle_stock", "efficiency"]
    both = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(both)
    old_efficiency = layer.load_notebook_evidence("efficiency", repo_root=bundle.repo_root)
    bundle.scenario.notebook_diagnostics = ["vehicle_stock"]
    latest_stock = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(latest_stock)
    current = layer.load_notebook_evidence("vehicle_stock", repo_root=bundle.repo_root)
    orphan = layer.stage_notebook_evidence(bundle, report)
    orphan_manifest = orphan.index_path.parent / orphan.bundles["vehicle_stock"]
    staging = orphan.index_path.parent / (".staging-" + "f" * 32)
    staging.mkdir()
    (staging / "pending.txt").write_text("interrupted capture", encoding="utf-8")
    removed = layer.prune_notebook_evidence(latest_stock)
    assert len(removed) == 2
    assert not orphan_manifest.exists()
    assert not staging.exists()
    assert current.paths["sample"].is_file()
    assert old_efficiency.paths["sample"].is_file()
    assert layer.load_notebook_evidence("efficiency", repo_root=bundle.repo_root).metadata["build_id"] == old_efficiency.metadata["build_id"]


def test_pruning_rejects_invalid_reference_before_any_deletion(prepared):
    bundle, report, _ = prepared
    first = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(first)
    pending = layer.stage_notebook_evidence(bundle, report)
    pending_manifest = pending.index_path.parent / pending.bundles["vehicle_stock"]
    first.index_path.write_text(json.dumps({"version": 1, "bundles": {"vehicle_stock": "../../outside.json"}}))
    with pytest.raises(ValueError, match="Invalid latest notebook evidence reference"):
        layer.prune_notebook_evidence(first)
    assert pending_manifest.is_file()


def test_pruning_rejects_linked_tree_before_any_deletion(prepared, tmp_path):
    bundle, report, _ = prepared
    first = layer.stage_notebook_evidence(bundle, report)
    layer.publish_notebook_evidence(first)
    pending = layer.stage_notebook_evidence(bundle, report)
    pending_manifest = pending.index_path.parent / pending.bundles["vehicle_stock"]
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "preserve.txt"
    marker.write_text("preserve me", encoding="utf-8")
    link = pending_manifest.parent / "external-link"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        if os.name != "nt":
            pytest.skip(f"Symlink creation unavailable: {error}")
        # Directory junctions need no Windows symlink privilege and exercise the
        # reparse-point guard used for safe local cleanup.
        junction = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(outside)], capture_output=True,
        )
        if junction.returncode:
            pytest.skip(f"Symlink and junction creation unavailable: {error}")
    with pytest.raises(ValueError, match="Unsafe linked notebook evidence path"):
        layer.prune_notebook_evidence(first)
    assert pending_manifest.is_file()
    assert marker.read_text(encoding="utf-8") == "preserve me"


@pytest.mark.parametrize("profiles", [["unknown"], ["vehicle_stock", "vehicle_stock"]])
def test_notebook_opt_ins_are_typed_and_unique(profiles):
    data = load_config_bundle("config/scenarios/notebook_diagnostics.yaml").scenario.model_dump()
    data["notebook_diagnostics"] = profiles
    with pytest.raises((ValidationError, ValueError)):
        ScenarioConfig.model_validate(data)


def test_ordinary_scenarios_do_not_request_review_preparation():
    assert not load_config_bundle("config/scenarios/default.yaml").scenario.notebook_diagnostics
