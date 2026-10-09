"""Native scenario publication and real registered-source lifecycle checks."""
from dataclasses import replace
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import sys

import pytest

import build_transport
from fetching.vehicle_population import registered_requests, validate_source
from utils import file_sha256, load_config_bundle, load_harmonization_rules, resolve_artifact_path, resolve_input_path
from validation.sqlite_compare import compare_scenario_databases


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def small_database(path, value):
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.execute("INSERT INTO example VALUES (?)", (value,))


@pytest.mark.parametrize("existing_report", [True, False])
def test_publication_preserves_previous_outputs_on_database_replace_failure(tmp_path, monkeypatch, existing_report):
    database, candidate, report = (tmp_path / name for name in ("final.sqlite", "candidate.sqlite", "report.json"))
    small_database(database, 1)
    small_database(candidate, 2)
    before = database.read_bytes()
    if existing_report:
        report.write_bytes(b"previous report")
    original = os.replace

    def fail_database(source, destination):
        if Path(destination) == database:
            raise PermissionError("database held open")
        original(source, destination)

    monkeypatch.setattr(os, "replace", fail_database)
    with pytest.raises(PermissionError, match="held open"):
        build_transport._publish_scenario(candidate, database, {"ok": True}, report)
    assert database.read_bytes() == before
    assert report.read_bytes() == b"previous report" if existing_report else not report.exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted([
        "final.sqlite", "candidate.sqlite", *(["report.json"] if existing_report else []),
    ])


def test_publication_serialization_failure_never_replaces_database_or_report(tmp_path):
    database, candidate, report = (tmp_path / name for name in ("final.sqlite", "candidate.sqlite", "report.json"))
    small_database(database, 1)
    small_database(candidate, 2)
    report.write_bytes(b"previous report")
    before = database.read_bytes()
    with pytest.raises(TypeError):
        build_transport._publish_scenario(candidate, database, {"bad": object()}, report)
    assert database.read_bytes() == before
    assert report.read_bytes() == b"previous report"


@pytest.mark.skipif(os.name != "nt", reason="Windows open-file replacement behavior")
def test_locked_windows_database_preserves_publication(tmp_path):
    database, candidate, report = (tmp_path / name for name in ("final.sqlite", "candidate.sqlite", "report.json"))
    small_database(database, 1)
    small_database(candidate, 2)
    report.write_bytes(b"previous report")
    before = database.read_bytes()
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(PermissionError):
            build_transport._publish_scenario(candidate, database, {"ok": True}, report)
        connection.rollback()
    assert database.read_bytes() == before
    assert report.read_bytes() == b"previous report"


def test_publication_replaces_database_and_report(tmp_path):
    database, candidate, report = (tmp_path / name for name in ("final.sqlite", "candidate.sqlite", "report.json"))
    small_database(database, 1)
    small_database(candidate, 2)
    build_transport._publish_scenario(candidate, database, {"ok": True}, report)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT value FROM example").fetchall() == [(2,)]
    assert json.loads(report.read_text()) == {"ok": True}
    assert not candidate.exists()


def test_ontario_registered_requests_do_not_require_interim_manifest(tmp_path):
    bundle = load_config_bundle(SCENARIO)
    shutil.copytree(ROOT / "config/parameters", tmp_path / "config/parameters")
    isolated = replace(bundle, repo_root=tmp_path)
    requests = registered_requests(isolated, year=2025)
    assert len(requests) == 1 and requests[0].expected_sha256
    assert not requests[0].cache_path.exists()
    with pytest.raises(FileNotFoundError):
        validate_source(requests[0])
    requests[0].cache_path.parent.mkdir(parents=True)
    requests[0].cache_path.write_bytes(b"changed archive")
    with pytest.raises(ValueError, match="SHA-256 changed"):
        validate_source(requests[0])


@pytest.mark.parametrize("change, match", [("hash", "pattern"), ("duplicate", "duplicate years"), ("year", "No registered Ontario archive")])
def test_ontario_rejects_invalid_registration_before_io(tmp_path, change, match):
    bundle = load_config_bundle(SCENARIO)
    shutil.copytree(ROOT / "config/parameters", tmp_path / "config/parameters")
    sources = bundle.sources.model_copy(deep=True)
    resources = sources.sources["ontario_ministry_transport_vehicle_population"].adapter["registered_resources"]
    if change == "hash":
        resources[0]["cache_sha256"] = "invalid"
    elif change == "duplicate":
        resources.append(resources[0])
    with pytest.raises(ValueError, match=match):
        registered_requests(replace(bundle, sources=sources, repo_root=tmp_path), year=1901 if change == "year" else 2025)


def isolated_repository(destination):
    """Only versioned controls and registered raw inputs; no generated handoffs."""
    for folder in ("config", "inputs/0_cache", "inputs/0_external_models", "inputs/0_manual_params", "inputs/0_canoe_template", "legacy_backend"):
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copyfile(ROOT / "pyproject.toml", destination / "pyproject.toml")


@pytest.mark.skipif(os.environ.get("CANOE_OFFLINE_INTEGRATION") != "1", reason="Set CANOE_OFFLINE_INTEGRATION=1 with registered caches/external inputs available")
def test_real_offline_scenario_clean_replay_freshness_and_failure(tmp_path, monkeypatch):
    """Exercise complete ten-region compilation with real inputs and network denied."""
    isolated_repository(tmp_path)
    monkeypatch.chdir(tmp_path)

    def no_network(*args, **kwargs):
        pytest.fail("network access during offline scenario build")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(sys, "argv", ["build_transport.py", "--scenario", SCENARIO])
    build_transport.main()
    bundle = load_config_bundle(SCENARIO)
    report_path = tmp_path / bundle.scenario.outputs.validation_report
    first = json.loads(report_path.read_text())
    database = Path(first["database"])
    reference = tmp_path / "reference.sqlite"
    shutil.copyfile(database, reference)
    original = (file_sha256(database), file_sha256(report_path))
    with pytest.raises(FileExistsError):
        build_transport.build_from_scenario(SCENARIO)
    assert original == (file_sha256(database), file_sha256(report_path))

    # Both corrupt existing and absent derived prerequisites must be regenerated.
    bundle = load_config_bundle(SCENARIO)
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    ceud = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"],
                              ceud_rules["region_output_template"].format(region="on"))
    ceud.write_text("stale derived evidence\n")
    aggregation = resolve_artifact_path(bundle, "road_aggregation")
    missing = next(aggregation.glob("*.csv"))
    missing.unlink()
    second, _ = build_transport.build_from_scenario(SCENARIO, overwrite=True)
    assert second["validation"]["foreign_key_violations"] == 0
    comparison = compare_scenario_databases(database, reference, absolute_tolerance=0, relative_tolerance=0, include_provenance=True)
    assert comparison["equivalent"], comparison
    assert missing.is_file() and "stale derived evidence" not in ceud.read_text()

    # A genuine absent registered cache cannot be masked by the freshly built CSVs.
    from fetching.nrcan_ceud import iter_table_requests
    request = next(request for request in iter_table_requests(load_config_bundle(SCENARIO)) if request.table_meta.required)
    request.cache_path.unlink()
    previous = (file_sha256(database), file_sha256(report_path))
    with pytest.raises(FileNotFoundError):
        build_transport.build_from_scenario(SCENARIO, overwrite=True)
    assert previous == (file_sha256(database), file_sha256(report_path))
