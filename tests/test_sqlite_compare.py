from __future__ import annotations

import hashlib
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

import build_transport
from utils import load_config_bundle
from validation.config_models import ScenarioComparison
from validation.schema_contract import create_v4_schema
from validation.sqlite_compare import compare_scenario_databases


def database(path: Path, *, capacity: float = 1.0, data_id: str = "stock", notes: str = "") -> Path:
    with sqlite3.connect(path) as connection:
        create_v4_schema(connection)
        # Comparison fixtures isolate values; production insertion tests cover FKs.
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO existing_capacity (region, tech, vintage, capacity, units, data_id, notes) "
            "VALUES ('QC', 'T_TEST_EX', 2023, ?, 'k vehicles', ?, ?)",
            (capacity, data_id, notes),
        )
    return path


def compare(candidate: Path, reference: Path, **changes) -> dict:
    return compare_scenario_databases(candidate, reference, **{
        "absolute_tolerance": 1e-9, "relative_tolerance": 1e-6,
        "include_provenance": False, **changes,
    })


def test_scenario_comparison_uses_all_regions_keys_values_and_exact_units(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite", capacity=2.0)
    reference = database(tmp_path / "reference.sqlite")
    with sqlite3.connect(candidate) as connection:
        connection.execute(
            "INSERT INTO existing_capacity (region, tech, vintage, capacity, units, data_id) "
            "VALUES ('AB', 'T_EXTRA_EX', 2023, 1, 'k vehicles', 'stock')"
        )
    report = compare(candidate, reference)
    rows = report["tables"]["existing_capacity"]
    assert report["equivalent"] is False
    assert rows["candidate_only_keys"] == 1
    assert rows["value_differences"] == 1
    assert rows["difference_examples"][0]["key"][0] == "QC"
    assert rows["difference_examples"][0]["changes"]["capacity"]["absolute_difference"] == 1.0
    with sqlite3.connect(reference) as connection:
        connection.execute("UPDATE existing_capacity SET capacity=2, units='k units'")
    changes = compare(candidate, reference)["tables"]["existing_capacity"]["difference_examples"][0]["changes"]
    assert set(changes) == {"units"}


def test_numeric_tolerances_do_not_hide_exact_key_differences(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite", capacity=1.0000005)
    reference = database(tmp_path / "reference.sqlite")
    assert compare(candidate, reference)["equivalent"] is True
    assert compare(candidate, reference, relative_tolerance=0)["equivalent"] is False
    with sqlite3.connect(candidate) as connection:
        connection.execute("UPDATE existing_capacity SET vintage=2025")
    rows = compare(candidate, reference, absolute_tolerance=100)["tables"]["existing_capacity"]
    assert rows["candidate_only_keys"] == rows["reference_only_keys"] == 1


def test_provenance_and_notes_are_opt_in_and_references_remain_unchanged(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite", data_id="new", notes="new annotation")
    reference = database(tmp_path / "reference.sqlite", data_id="old", notes="old annotation")
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (candidate, reference)]
    assert compare(candidate, reference)["equivalent"] is True
    assert compare(candidate, reference, include_provenance=True)["equivalent"] is False
    assert before == [hashlib.sha256(path.read_bytes()).hexdigest() for path in (candidate, reference)]


def test_provenance_scores_are_exact_even_with_loose_numeric_tolerance(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite")
    reference = database(tmp_path / "reference.sqlite")
    for path, score in ((candidate, 4), (reference, 5)):
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE existing_capacity SET dq_cred=?", (score,))
    report = compare(candidate, reference, include_provenance=True, absolute_tolerance=100)
    assert report["tables"]["existing_capacity"]["value_differences"] == 1


def test_comparison_reports_schema_and_projected_key_ambiguity(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite")
    reference = database(tmp_path / "reference.sqlite")
    with sqlite3.connect(candidate) as connection:
        connection.execute(
            "INSERT INTO existing_capacity (region, tech, vintage, capacity, units, data_id) "
            "VALUES ('QC', 'T_TEST_EX', 2023, 1, 'k vehicles', 'another-stock')"
        )
        connection.execute("CREATE TABLE unexpected (id TEXT PRIMARY KEY)")
    report = compare(candidate, reference)
    assert report["equivalent"] is False
    assert report["tables"]["existing_capacity"]["candidate_duplicate_keys"] == 1
    assert report["tables"]["unexpected"]["comparable"] is False


def test_missing_and_legacy_references_fail_without_creating_files(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite")
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(FileNotFoundError):
        compare(candidate, missing)
    assert not missing.exists()
    legacy = tmp_path / "legacy.sqlite"
    with sqlite3.connect(legacy) as connection:
        connection.execute("CREATE TABLE ExistingCapacity (tech TEXT)")
    with pytest.raises(ValueError, match="v4 reference"):
        compare(candidate, legacy)


@pytest.mark.parametrize("mode", ["none", "scenario"])
def test_scenario_entrypoint_dispatches_optional_comparison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    reference = database(tmp_path / "reference.sqlite")
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    payload = bundle.scenario.model_dump(mode="python")
    payload["comparison"].update(mode=mode, reference_sqlite=str(reference))
    payload["outputs"]["validation_report"] = "comparison.json"
    configured = replace(bundle, repo_root=tmp_path,
                         scenario=type(bundle.scenario).model_validate(payload))
    monkeypatch.setattr(build_transport, "load_config_bundle", lambda _: configured)

    def bootstrap(**kwargs):
        kwargs["database_path"].parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(reference, kwargs["database_path"])
        result = (
            build_transport.compare_transport_database(configured, kwargs["database_path"], reference)
            if kwargs["comparison_reference"] is not None else {"enabled": False, "mode": "none"}
        )
        return {"ok": True, "validation": {"ok": True}, "comparison": result}

    monkeypatch.setattr(build_transport, "bootstrap_database", bootstrap)
    report, report_path = build_transport.build_from_scenario("scenario.yaml")
    assert report["validation"]["ok"] is True
    assert report["comparison"]["mode"] == mode
    assert report["comparison"]["enabled"] == (mode != "none")
    if mode == "scenario":
        assert report["comparison"]["equivalent"] is True
    assert report_path.exists()


@pytest.mark.parametrize("changes", [
    {"reference_sqlite": None}, {"absolute_tolerance": -1},
    {"relative_tolerance": float("nan")}, {"mode": "unknown"},
    {"mode": "legacy", "include_provenance": True},
])
def test_comparison_options_are_validated(changes: dict) -> None:
    with pytest.raises(ValueError):
        ScenarioComparison.model_validate({
            "mode": "scenario", "reference_sqlite": "reference.sqlite",
            "absolute_tolerance": 1e-9, "relative_tolerance": 1e-6,
            "include_provenance": False, **changes,
        })


def test_unkeyed_solver_outputs_preserve_duplicate_counts(tmp_path: Path) -> None:
    candidate = database(tmp_path / "candidate.sqlite")
    reference = database(tmp_path / "reference.sqlite")
    for path, count in ((candidate, 2), (reference, 1)):
        with sqlite3.connect(path) as connection:
            connection.executemany(
                "INSERT INTO output_objective VALUES ('case', 'cost', 100)", [()] * count,
            )
    rows = compare(candidate, reference)["tables"]["output_objective"]
    assert rows["comparison_grain"] == "unkeyed exact row multiset"
    assert rows["candidate_only_rows"] == 1


def test_comparison_failure_preserves_published_database_and_cleans_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    reference = database(tmp_path / "reference.sqlite")
    target = tmp_path / "target.sqlite"
    target.write_bytes(b"previous published database")
    prepare = build_transport.prepare_transport_contribution

    def structural(*args, **kwargs):
        return prepare(*args, **kwargs, include_existing_capacity=False,
                       include_demand=False, include_road_utilization=False,
                       include_lifetimes=False, include_efficiencies=False,
                       include_costs=False, include_ev_chargers=False)

    def reject(*args):
        raise ValueError("comparison failed")

    monkeypatch.setattr(build_transport, "prepare_transport_contribution", structural)
    monkeypatch.setattr(build_transport, "compare_transport_database", reject)
    with pytest.raises(ValueError, match="comparison failed"):
        build_transport.bootstrap_database(
            bundle=bundle, template_dir=bundle.repo_root / "inputs/0_canoe_template",
            database_path=target, overwrite=True, comparison_reference=reference,
        )
    assert target.read_bytes() == b"previous published database"
    assert list(tmp_path.glob(".target.sqlite.*.tmp")) == []


def test_comparison_rejects_using_output_as_reference_before_preparation(tmp_path: Path) -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    target = database(tmp_path / "target.sqlite")
    before = target.read_bytes()
    with pytest.raises(ValueError, match="must differ"):
        build_transport.bootstrap_database(
            bundle=bundle, template_dir=bundle.repo_root / "inputs/0_canoe_template",
            database_path=target, overwrite=True, comparison_reference=target,
        )
    assert target.read_bytes() == before
