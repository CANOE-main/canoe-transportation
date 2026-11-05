"""Research reports evaluate model quantities and preserve reference uncertainty."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from utils import file_sha256, load_config_bundle
from validation import transportation_research as research
from validation.transportation_research import (
    TransportationResearch, _verify_accepted_survival, _verify_build,
    compare_reference_state, fleet_activity_energy, inspect_notebook,
    road_parameter_comparison, write_research_report,
)


def fleet_fixture():
    capacity = pd.DataFrame([
        {"region": "ON", "tech": "T_GAS_EX", "vintage": 2020, "capacity": 3., "units": "k vehicles"},
        {"region": "ON", "tech": "T_PHEV_EX", "vintage": 2020, "capacity": 2., "units": "k vehicles"},
    ])
    c2a = pd.DataFrame([{"region": "ON", "tech": tech, "c2a": 4.} for tech in capacity.tech])
    factors = pd.DataFrame([{"region": "ON", "tech_or_group": tech, "vintage": 2020,
                             "output_comm": "service", "operator": "e", "factor": .5}
                            for tech in capacity.tech])
    efficiency = pd.DataFrame([
        {"region": "ON", "tech": "T_GAS_EX", "vintage": 2020, "input_comm": "gas",
         "output_comm": "service", "efficiency": 2.},
        {"region": "ON", "tech": "T_PHEV_EX", "vintage": 2020, "input_comm": "blend",
         "output_comm": "service", "efficiency": 4.},
        # Blend edges must not create a second vehicle-energy contribution.
        {"region": "ON", "tech": "T_BLEND", "vintage": 2020, "input_comm": "gas",
         "output_comm": "blend", "efficiency": 1.},
        {"region": "ON", "tech": "T_BLEND", "vintage": 2020, "input_comm": "electricity",
         "output_comm": "blend", "efficiency": 1.},
    ])
    ceud = pd.DataFrame([
        {"region": "ON", "year": 2025, "table_id": 1, "raw_series": label,
         "unit": unit, "value": value, "ceud_observed_year": 2023, "ceud_value_status": "constant"}
        for label, unit, value in (("Stock", "thousands", 7), ("Activity", "millions", 10000),
                                   ("Energy", "PJ", 5))
    ])
    controls = {
        "base_year": 2025, "regions": ["ON"],
        "road_rules": {"fuel_technology": {"cars": {"Gasoline": "T_GAS_EX", "PHEV": "T_PHEV_EX"}},
                       "ceud_stock_series": {"cars": {"table_id": 1, "raw_series": "Stock"}},
                       "region_output_map": {}},
        "demand_rules": {"activity_to_billion_factor": .001,
                         "activity_series": {"cars": {"table_id": 1, "raw_series": "Activity", "commodity": "service"}}},
        "energy_series": {"cars": {"table_id": 1, "raw_series": "Energy"}},
    }
    return [capacity, c2a, factors, efficiency, ceud], controls


def test_energy_accounting_counts_phev_blend_once_and_keeps_stock_difference():
    frames, controls = fleet_fixture()
    detail, summary = fleet_activity_energy(*frames, **controls)
    row = summary.iloc[0]
    assert len(detail) == 2
    assert row.inserted_stock_k_vehicles == 5
    assert row.stock_minus_ceud_k_vehicles == -2
    assert row.implied_activity_bn == 10
    assert row.activity_residual_bn == 0
    assert row.implied_vehicle_input_pj == 4
    assert row.energy_residual_pj == -1
    assert row.ceud_energy_status == "constant"
    assert row.ceud_energy_observed_year == 2023


@pytest.mark.parametrize("defect", ["factor", "efficiency", "duplicate", "units"])
def test_missing_or_ambiguous_physical_support_is_not_silently_dropped(defect):
    frames, controls = fleet_fixture()
    if defect == "factor":
        frames[2] = frames[2].iloc[:1]
    elif defect == "efficiency":
        frames[3] = frames[3].iloc[1:]
    elif defect == "duplicate":
        frames[3] = pd.concat([frames[3], frames[3].iloc[:1]])
    else:
        frames[0]["units"] = "vehicles"
    with pytest.raises(ValueError):
        fleet_activity_energy(*frames, **controls)


def survival_fixture():
    return pd.DataFrame([
        {"source_id": "s", "source_class": "c", "age": age, "survival_probability": fraction}
        for age, fraction in enumerate([1., .9, .8, .7, .6, .5])
    ])


def test_notebook_source_and_stale_saved_output_are_separate(tmp_path):
    notebook = {"cells": [
        {"cell_type": "code", "source": ["data = pd.read_excel('Input data/sales.xlsx')"], "outputs": []},
        {"cell_type": "code", "source": ["main_compile_data_transport(canoe_default_all='Input data/all.sqlite', fixed_lifetime=False)"],
         "outputs": [{"text": ["Assumed using fixed lifetime to generate survival"]}]},
    ]}
    path = tmp_path / "reference.ipynb"
    path.write_text(json.dumps(notebook))
    before = file_sha256(path)
    cells, metadata = inspect_notebook(path)
    assert metadata["source_output_conflict"]
    assert metadata["sqlite_dataframe_write_calls"] == []
    assert metadata["fixed_lifetime_calls"] == [{"cell_index": 1, "value": False}]
    assert len(metadata["dependencies"]) == 2
    assert all(not item["present_relative_to_notebook"] for item in metadata["dependencies"])
    assert cells.cell_index.tolist() == [1]
    assert file_sha256(path) == before


def legacy_fixture(path: Path, *, changed_demand=False, charger_capacity=1):
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE ExistingCapacity(region,tech,vintage,capacity);
            CREATE TABLE Demand(region,period,commodity,demand);
            CREATE TABLE LifetimeTech(region,tech,lifetime);
            CREATE TABLE LifetimeSurvivalCurve(region,period,tech,vintage,fraction);
            CREATE TABLE LimitAnnualCapacityFactor(region,period,tech,factor);
        """)
        connection.executemany("INSERT INTO ExistingCapacity VALUES(?,?,?,?)", [
            ("ON", "T_CAR_EX", 2020, 12), ("ON", "T_CHRG", 2020, charger_capacity),
            ("ON", "R_OTHER", 2020, 14),
        ])
        connection.execute("INSERT INTO Demand VALUES(?,?,?,?)", ("ON", 2025, "T_D_car", 13 if changed_demand else 12))
        connection.execute("INSERT INTO LifetimeTech VALUES(?,?,?)", ("ON", "T_CAR_EX", 14))


def test_reference_reconciliation_excludes_chargers_and_never_changes_reference(tmp_path):
    baseline, reference = tmp_path / "baseline.sqlite", tmp_path / "reference.sqlite"
    legacy_fixture(baseline)
    legacy_fixture(reference, changed_demand=True, charger_capacity=200)
    before = file_sha256(reference)
    rows, metadata = compare_reference_state(reference, baseline, absolute_tolerance=1e-9, relative_tolerance=1e-9)
    indexed = rows.set_index("table")
    assert indexed.loc["ExistingCapacity", "common_keys"] == 1
    assert indexed.loc["ExistingCapacity", "matching_values"] == 1
    assert indexed.loc["Demand", "different_values"] == 1
    assert metadata["survival_rows"] == 0
    assert "period" in metadata["annual_factor_columns"]
    assert metadata["foreign_key_violations"] == 0
    assert file_sha256(reference) == before


def test_reference_reconciliation_normalizes_original_minimum_maximum_factors(tmp_path):
    baseline, reference = tmp_path / "baseline.sqlite", tmp_path / "reference.sqlite"
    legacy_fixture(baseline)
    legacy_fixture(reference)
    with sqlite3.connect(baseline) as connection:
        connection.executescript("""
            DROP TABLE LimitAnnualCapacityFactor;
            CREATE TABLE MinAnnualCapacityFactor(region,period,tech,output_comm,factor);
            CREATE TABLE MaxAnnualCapacityFactor(region,period,tech,output_comm,factor);
            INSERT INTO MinAnnualCapacityFactor VALUES('ON',2025,'T_CAR_EX','service',0.1);
            INSERT INTO MaxAnnualCapacityFactor VALUES('ON',2025,'T_CAR_EX','service',0.2);
        """)
    with sqlite3.connect(reference) as connection:
        connection.executescript("""
            ALTER TABLE LimitAnnualCapacityFactor ADD COLUMN output_comm;
            ALTER TABLE LimitAnnualCapacityFactor ADD COLUMN operator;
            INSERT INTO LimitAnnualCapacityFactor VALUES('ON',2025,'T_CAR_EX',0.11,'service','ge');
            INSERT INTO LimitAnnualCapacityFactor VALUES('ON',2025,'T_CAR_EX',0.2,'service','le');
        """)
    rows, _ = compare_reference_state(reference, baseline, absolute_tolerance=1e-9, relative_tolerance=1e-9)
    row = rows.set_index("table").loc["LimitAnnualCapacityFactor"]
    assert row.common_keys == 2
    assert row.matching_values == 1
    assert row.different_values == 1
    assert row.reference_only_keys == row.baseline_only_keys == 0


def test_research_freshness_rejects_stale_database_before_reading_shared_artifacts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=root)
    database = tmp_path / "database.sqlite"
    database.write_bytes(b"not the built snapshot")
    report = {"ok": True, "scenario": bundle.scenario.scenario.name, "database_sha256": "old"}
    with pytest.raises(ValueError, match="database differs"):
        _verify_build(replace(bundle, repo_root=tmp_path), database, report)


def test_research_survival_fingerprint_accepts_exact_build_evidence():
    curves = survival_fixture()
    expected = hashlib.sha256(curves.to_csv(index=False).encode("utf-8")).hexdigest()
    assert _verify_accepted_survival(curves, {"accepted_survival_sha256": expected}) == expected


@pytest.mark.parametrize("missing", [False, True])
def test_research_survival_fingerprint_rejects_stale_or_missing_build_evidence(missing):
    curves = survival_fixture()
    expected = hashlib.sha256(curves.to_csv(index=False).encode("utf-8")).hexdigest()
    audit = {} if missing else {"accepted_survival_sha256": expected}
    curves.loc[curves.age.eq(1), "survival_probability"] = .91
    with pytest.raises(ValueError, match="survival"):
        _verify_accepted_survival(curves, audit)


def test_report_manifest_hashes_exact_exported_tables(tmp_path):
    result = TransportationResearch({"facts": pd.DataFrame({"value": [1., 2.]})},
                                    {"scope": "diagnostic"}, tmp_path / "report")
    path = write_research_report(result)
    manifest = json.loads(path.read_text())
    assert manifest["exports"]["facts.csv"] == {"rows": 2, "sha256": file_sha256(path.parent / "facts.csv")}
    assert manifest["scope"] == "diagnostic"


def test_report_replaces_only_obsolete_previously_owned_csv_exports(tmp_path):
    destination = tmp_path / "report"
    original = TransportationResearch(
        {"facts": pd.DataFrame({"value": [1.]}), "retired_method": pd.DataFrame({"value": [2.]})},
        {}, destination,
    )
    write_research_report(original)
    unrelated = destination / "unrelated.csv"
    unrelated.write_text("user evidence\n", encoding="utf-8")
    unrelated_hash = file_sha256(unrelated)
    replacement = TransportationResearch({"facts": pd.DataFrame({"value": [3.]})}, {}, destination)
    manifest = json.loads(write_research_report(replacement).read_text())
    assert set(manifest["exports"]) == {"facts.csv"}
    assert not (destination / "retired_method.csv").exists()
    assert file_sha256(unrelated) == unrelated_hash


def test_report_rejects_outside_export_from_previous_manifest_before_writing(tmp_path):
    destination = tmp_path / "report"
    destination.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("protected evidence\n", encoding="utf-8")
    (destination / "manifest.json").write_text(json.dumps({
        "exports": {"../outside.csv": {"sha256": file_sha256(outside)}},
    }), encoding="utf-8")
    result = TransportationResearch({"facts": pd.DataFrame({"value": [3.]})}, {}, destination)
    with pytest.raises(ValueError, match="previously owned"):
        write_research_report(result)
    assert outside.read_text(encoding="utf-8") == "protected evidence\n"
    assert not (destination / "facts.csv").exists()


def test_report_preserves_modified_obsolete_export(tmp_path):
    destination = tmp_path / "report"
    write_research_report(TransportationResearch({"retired": pd.DataFrame({"value": [1.]})}, {}, destination))
    retired = destination / "retired.csv"
    retired.write_text("edited by user\n", encoding="utf-8")
    replacement = TransportationResearch({"facts": pd.DataFrame({"value": [3.]})}, {}, destination)
    with pytest.raises(ValueError, match="modified after capture"):
        write_research_report(replacement)
    assert retired.read_text(encoding="utf-8") == "edited by user\n"
    assert not (destination / "facts.csv").exists()


def captured_refresh_fixture(tmp_path, monkeypatch):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    directory = tmp_path / "report"
    database, baseline, reference = [tmp_path / f"{name}.sqlite" for name in ("database", "baseline", "reference")]
    for path in (database, baseline, reference):
        legacy_fixture(path)
    notebook = tmp_path / "notebook.ipynb"
    notebook.write_text("{}", encoding="utf-8")
    evidence = tmp_path / "reviewed.csv"
    evidence.write_text("reviewed\n", encoding="utf-8")
    config = {name: {"sha256": file_sha256(path)} for name, path in (
        ("scenario", bundle.scenario_path), ("paths", bundle.paths_path), ("sources", bundle.sources_path),
    )}
    config.update({name: [{"path": str(evidence), "sha256": file_sha256(evidence)}]
                   for name in ("parameters", "manual_inputs")})
    build_report = tmp_path / "native.json"
    build_report.write_text(json.dumps({
        "ok": True, "scenario": bundle.scenario.scenario.name,
        "database_sha256": file_sha256(database), "periods": bundle.scenario.periods.model_dump(mode="json"),
        "config": config,
    }), encoding="utf-8")
    def identity(path):
        return {"path": str(path), "sha256": file_sha256(path)}
    metadata = {
        "scenario": bundle.scenario.model_dump(mode="json"), "configuration": config,
        "files": {"database": identity(database), "build_report": identity(build_report)},
        "reference_state": {"reference": identity(reference), "legacy_baseline": identity(baseline)},
        "notebook": identity(notebook), "implementation_sha256": "original-native-capture",
    }
    write_research_report(TransportationResearch({
        "native_audit": pd.DataFrame({"value": [0.12345678901234567]}),
        "capacity_comparison": pd.DataFrame({"old": [1]}),
    }, metadata, directory))
    rules = {
        "reference_route": "reference", "report_route": "report",
        "reference_sqlite": reference.name, "reference_notebook": notebook.name,
        "legacy_baseline": str(baseline), "energy_series": {},
        "notebook_scenario": "config/scenarios/default.yaml", "comparison_significance_percent": 5,
    }
    monkeypatch.setattr(research, "resolve_artifact_path", lambda bundle, route, *parts: directory if route == "report" else tmp_path)
    monkeypatch.setattr(research, "load_harmonization_rules", lambda bundle, name: rules if name == "transportation_research" else {"demand": {}})
    monkeypatch.setattr(research, "road_parameter_comparison", lambda *args, **kwargs: {
        "capacity_comparison": pd.DataFrame({"corrected": [2]}),
    })
    return bundle, directory, database, build_report


def test_comparison_refresh_preserves_native_capture_bytes_and_implementation(tmp_path, monkeypatch):
    bundle, directory, database, build_report = captured_refresh_fixture(tmp_path, monkeypatch)
    native_path = directory / "native_audit.csv"
    native_bytes = native_path.read_bytes()
    previous_hash = file_sha256(directory / "manifest.json")
    refreshed = research.refresh_research_comparisons(bundle, database=database, build_report=build_report)
    manifest = json.loads(write_research_report(refreshed).read_text(encoding="utf-8"))
    assert native_path.read_bytes() == native_bytes
    assert manifest["implementation_sha256"] == "original-native-capture"
    assert manifest["comparison_refresh"][0]["prior_manifest_sha256"] == previous_hash
    assert manifest["comparison_refresh"][0]["comparison_implementation_sha256"] == file_sha256(Path(research.__file__))
    assert manifest["exports"]["native_audit.csv"]["sha256"] == file_sha256(native_path)
    assert pd.read_csv(directory / "capacity_comparison.csv").corrected.tolist() == [2]
    assert "LimitAnnualCapacityFactor" in pd.read_csv(directory / "reference_state.csv").table.tolist()


@pytest.mark.parametrize("defect", ["captured_export", "database", "reference", "scenario"])
def test_comparison_refresh_rejects_tampered_capture_or_changed_identity(tmp_path, monkeypatch, defect):
    bundle, directory, database, build_report = captured_refresh_fixture(tmp_path, monkeypatch)
    if defect == "captured_export":
        (directory / "native_audit.csv").write_text("value\n999\n", encoding="utf-8")
    elif defect in {"database", "reference"}:
        with sqlite3.connect(database if defect == "database" else tmp_path / "reference.sqlite") as connection:
            connection.execute("UPDATE Demand SET demand=999")
    else:
        bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
            "periods": bundle.scenario.periods.model_copy(update={"base_year": 2024}),
        }))
    previous = (directory / "manifest.json").read_bytes()
    with pytest.raises(ValueError):
        research.refresh_research_comparisons(bundle, database=database, build_report=build_report)
    assert (directory / "manifest.json").read_bytes() == previous


def test_supplied_reference_state_is_measured_not_inferred_from_filename():
    root = Path(__file__).resolve().parents[1]
    reference = root / "legacy_backend/validation_1/canoe_ON_16d.sqlite"
    baseline = root / "legacy_backend/canoe_on_12d_baseline.sqlite"
    if not reference.is_file() or not baseline.is_file():
        pytest.skip("Supplied researcher reference files are not available")
    rows, metadata = compare_reference_state(reference, baseline, absolute_tolerance=1e-9, relative_tolerance=1e-9)
    indexed = rows.set_index("table")
    assert indexed.loc["ExistingCapacity", "matching_values"] == 99
    assert indexed.loc["Demand", "matching_values"] == 90
    assert indexed.loc["LifetimeTech", "matching_values"] == 122
    assert metadata["survival_rows"] == 0


def test_parameter_comparison_reconciles_class_aliases_and_retains_temporal_keys(tmp_path):
    paths = {name: tmp_path / f"{name}.sqlite" for name in ("current", "reference", "baseline")}
    for name, path in paths.items():
        current = name == "current"
        capacity_table = "existing_capacity" if current else "ExistingCapacity"
        factor_table = "limit_annual_capacity_factor" if current else "LimitAnnualCapacityFactor"
        with sqlite3.connect(path) as connection:
            connection.execute(f"CREATE TABLE {capacity_table}(region,tech,vintage,capacity,units)")
            connection.execute("CREATE TABLE Efficiency(region,tech,vintage,output_comm)")
            connection.execute("CREATE TABLE Demand(region,period,commodity,demand)")
            connection.execute(f"CREATE TABLE {factor_table}(region,tech,output_comm,factor)")
            tech = "T_CAR_BEV_EX" if current else "T_CAR_BEV150_EX"
            connection.execute(f"INSERT INTO {capacity_table} VALUES(?,?,?,?,?)", ("ON", tech, 2020, 12 if current else 10, "k vehicles"))
            if name != "baseline":
                connection.execute("INSERT INTO Efficiency VALUES(?,?,?,?)", ("ON", tech, 2020, "service"))
            connection.execute("INSERT INTO Demand VALUES(?,?,?,?)", ("ON", 2030, "service", 30 if current else 25))
            connection.execute(f"INSERT INTO {factor_table} VALUES(?,?,?,?)", ("ON", tech, "service", .2 if current else .1))
    before = {name: file_sha256(path) for name, path in paths.items()}
    frames = road_parameter_comparison(
        paths["current"], paths["reference"], paths["baseline"],
        demand_rules={"activity_series": {"cars": {"commodity": "service"}}},
    )
    capacity = frames["capacity_comparison"].iloc[0]
    assert capacity.current == 12
    assert capacity.reference == 10
    assert capacity.baseline == 10
    assert capacity.vintage == 2020
    assert capacity.relative_percent == 20
    assert frames["demand_comparison"].iloc[0].period == 2030
    assert frames["utilization_comparison"].iloc[0].current_minimum == .2
    assert before == {name: file_sha256(path) for name, path in paths.items()}
