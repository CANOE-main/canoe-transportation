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

import pandas as pd
import pytest
import yaml

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

    # Explicit research preparation must survive every later production replay.
    from fetching.vehicle_population import fetch_and_normalize, load_history_manifest
    from fetching.fueleconomy_vehicles import fetch_and_normalize as normalize_fueleconomy
    from parameterization.road_aggregation import build_mapping_diagnostic_artifacts
    from parameterization.road_lifetimes_survival import build_mto_survival_diagnostic_artifacts
    fetch_and_normalize(SCENARIO, download=False)
    assert len(load_history_manifest(bundle)) == 11
    normalize_fueleconomy(SCENARIO, download=False)
    build_mapping_diagnostic_artifacts(SCENARIO)
    build_mto_survival_diagnostic_artifacts(SCENARIO)
    # The actual notebook loader must work without obsolete processed copies.
    import runpy
    notebook = runpy.run_path(str(ROOT / "docs/insights/vehicle_stock_diagnosis.py"))
    _, definitions = notebook["load_evidence"].run()
    evidence = definitions["load_evidence"](tmp_path / SCENARIO)
    years = set(evidence["source_manifest"].year)
    assert len(years) == 11 and set(evidence["status_long"].report_year) == years
    assert set(evidence["passenger_cohorts"].report_year) == years
    assert not evidence["legacy_survival"].empty
    diagnostic_families = ("ontario_vehicle_population", "vehicle_survival_interim", "lifetime_validation", "vehicle_mapping_review")
    history_hashes = {
        path: file_sha256(path)
        for family in diagnostic_families
        for path in resolve_artifact_path(bundle, family).rglob("*")
        if path.is_file()
    }

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
    assert history_hashes == {path: file_sha256(path) for path in history_hashes}
    assert len(load_history_manifest(bundle)) == 11

    # A genuine absent registered cache cannot be masked by the freshly built CSVs.
    from fetching.nrcan_ceud import iter_table_requests
    request = next(request for request in iter_table_requests(load_config_bundle(SCENARIO)) if request.table_meta.required)
    request.cache_path.unlink()
    previous = (file_sha256(database), file_sha256(report_path))
    with pytest.raises(FileNotFoundError):
        build_transport.build_from_scenario(SCENARIO, overwrite=True)
    assert previous == (file_sha256(database), file_sha256(report_path))


@pytest.mark.skipif(os.environ.get("CANOE_OFFLINE_INTEGRATION") != "1", reason="Set CANOE_OFFLINE_INTEGRATION=1 with registered caches/external inputs available")
def test_moves5_offline_scenario_replays_both_lifetime_modes_and_rejects_missing_cache(tmp_path, monkeypatch):
    """Run the native lifecycle serially from raw inputs, with no network or shared writes."""
    isolated_repository(tmp_path)
    monkeypatch.chdir(tmp_path)

    def no_network(*args, **kwargs):
        pytest.fail("network access during offline MOVES5 scenario build")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    scenario_path = tmp_path / SCENARIO
    payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    payload["geography"]["regions"] = ["ON"]
    payload["lifetimes"]["heavy_truck_survival_source"] = "epa_moves5_heavy_truck_survival"
    payload["comparison"]["mode"] = "none"
    for curves in (True, False):
        payload["lifetimes"]["survival_curves"] = curves
        scenario_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        report, _ = build_transport.build_from_scenario(SCENARIO, overwrite=True)
        assert report["validation"]["foreign_key_violations"] == 0
        assert report["orchestration"] == {"prerequisites": "replayed", "download_sources": False}
        bundle = load_config_bundle(SCENARIO)
        database = Path(report["database"])
        with closing(sqlite3.connect(database)) as connection:
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            for prefix, required, absent in (("T_HDV_T_", "T39", "T10"), ("T_MDV_T_", "T09", "T39")):
                data_ids = connection.execute(
                    "SELECT DISTINCT data_id FROM existing_capacity WHERE substr(tech, 1, ?) = ?",
                    (len(prefix), prefix),
                ).fetchall()
                assert data_ids
                for (data_id,) in data_ids:
                    contributors = {row[0] for row in connection.execute(
                        "SELECT source_id FROM data_source WHERE data_id = ?", (data_id,),
                    )}
                    assert required in contributors and absent not in contributors
            if curves:
                result = connection.execute(
                    "SELECT fraction, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time "
                    "FROM lifetime_survival_curve WHERE tech = 'T_HDV_T_DSL_N' AND vintage = 2025 AND period = 2045"
                ).fetchone()
                assert result[1:] == ("T39", 5, 5, 5, 5, 5)
                rules = load_harmonization_rules(bundle, "assorted_sources")
                annual = pd.read_csv(resolve_input_path(bundle, "interim", rules["interim_subdir"], rules["epa_moves5"]["output_file"]))
                survival = annual.sort_values("vehicle_age").annual_survival_rate.cumprod()
                assert result[0] == pytest.approx(survival.iloc[20:25].mean())
            else:
                assert connection.execute(
                    "SELECT lifetime, data_source FROM lifetime_tech WHERE tech = 'T_HDV_T_DSL_EX'"
                ).fetchall() == [(19, "T39")]
                assert connection.execute(
                    "SELECT lifetime, data_source FROM lifetime_tech WHERE tech = 'T_MDV_T_DSL_EX'"
                ).fetchall() == [(18, "T09")]
                assert connection.execute("SELECT COUNT(*) FROM lifetime_survival_curve").fetchone()[0] == 0

        stock_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
        cohorts = pd.read_csv(resolve_artifact_path(bundle, "existing_capacity_interim") / stock_rules["age_cohorts_file"])
        heavy = cohorts.loc[cohorts.road_class.eq("heavy_trucks")]
        if curves:
            expected = (bundle.scenario.periods.base_year - heavy.age).ge(min(bundle.scenario.periods.existing)) & (
                bundle.scenario.periods.model[0] - bundle.scenario.periods.base_year + heavy.age
            ).le(bundle.scenario.lifetimes.survival_curve_max_age)
        else:
            expected = (heavy.vintage_year + 19).gt(bundle.scenario.periods.model[0]) & (
                heavy.vintage + 19
            ).gt(bundle.scenario.periods.model[0])
        assert heavy.eligible_first_model_period.tolist() == expected.tolist()

    # A selected missing raw PDF cannot be hidden by the already normalized CSV.
    report_path = tmp_path / bundle.scenario.outputs.validation_report
    previous = file_sha256(database), file_sha256(report_path)
    cache = resolve_input_path(bundle, "cache", bundle.sources.sources["epa_moves5_heavy_truck_survival"].adapter["cache_path"])
    assert cache.is_relative_to(tmp_path)
    cache.unlink()
    with pytest.raises(FileNotFoundError, match="MOVES5|moves5|P101CUN7"):
        build_transport.build_from_scenario(SCENARIO, overwrite=True)
    assert previous == (file_sha256(database), file_sha256(report_path))
