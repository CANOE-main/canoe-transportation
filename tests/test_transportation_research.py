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
    deepen_demand_diagnosis, deepen_fleet_diagnosis, harmonized_charging_costs, road_parameter_comparison,
    write_research_report,
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


@pytest.mark.parametrize("entrypoint", [research.build_research_report, research.refresh_research_comparisons])
def test_research_rejects_curve_capacity_before_reading_report_artifacts(tmp_path, entrypoint):
    root = Path(__file__).resolve().parents[1]
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml", repo_root=root)
    curve_scenario = bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": True}),
    })
    bundle = replace(bundle, repo_root=tmp_path, scenario=curve_scenario)
    with pytest.raises(ValueError, match="requires separate physical-stock accounting"):
        entrypoint(bundle)


def test_validation_diagnosis_rejects_curve_mode_before_reading_captures(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml", repo_root=root)
    curve_scenario = bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": True}),
    })
    curve_bundle = replace(bundle, scenario=curve_scenario)
    monkeypatch.setattr(research, "load_config_bundle", lambda *args, **kwargs: curve_bundle)

    def reject_capture_read(path):
        pytest.fail("Curve-mode accounting must be rejected before captured reports are read")

    monkeypatch.setattr(research, "_load_captured_report", reject_capture_read)
    with pytest.raises(ValueError, match="requires separate physical-stock accounting"):
        research.load_validation_diagnosis(repo_root=root)


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
    # Research comparisons explicitly require the fixed-stock accounting preset.
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml")
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
        "inherited_sqlite": str(reference),
        "legacy_baseline": str(baseline), "energy_series": {},
        "notebook_scenario": "config/scenarios/validation_baseline.yaml", "comparison_significance_percent": 5,
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
    reference = root / "legacy_backend/legacy_variant_sales_cohorts/canoe_ON_16d.sqlite"
    baseline = root / "legacy_backend/canoe_on_12d_baseline.sqlite"
    if not reference.is_file() or not baseline.is_file():
        pytest.skip("Supplied researcher reference files are not available")
    rows, metadata = compare_reference_state(reference, baseline, absolute_tolerance=1e-9, relative_tolerance=1e-9)
    indexed = rows.set_index("table")
    assert indexed.loc["ExistingCapacity", "matching_values"] == 99
    assert indexed.loc["Demand", "matching_values"] == 90
    assert indexed.loc["LifetimeTech", "matching_values"] == 122
    assert metadata["survival_rows"] == 0


@pytest.mark.parametrize("defect", [None, "fleet_export", "charging_export", "retired_scenario", "other_charging_scenario", "old_database", "old_build_report"])
def test_validation_diagnosis_requires_live_scenario_and_common_build(
    tmp_path, monkeypatch, defect,
):
    root = Path(__file__).resolve().parents[1]
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=root)
    original_load = research.load_config_bundle
    loaded_scenarios = []

    def load_current_controls(scenario, **kwargs):
        loaded_scenarios.append(str(scenario))
        return original_load(scenario, **kwargs)

    monkeypatch.setattr(research, "load_config_bundle", load_current_controls)
    # Arithmetic and native freshness have separate focused tests. Here both
    # report identities must bind to the live YAML and the same database/build.
    monkeypatch.setattr(research, "deepen_fleet_diagnosis", lambda frames, metadata: {})
    monkeypatch.setattr(research, "deepen_demand_diagnosis", lambda result, control, rules: {})
    monkeypatch.setattr(research, "deepen_reference_cohorts", lambda result, control, rules: {}, raising=False)
    monkeypatch.setattr(research, "reconstruct_comparison", lambda result, control, rules: {})
    monkeypatch.setattr(research, "harmonized_charging_costs", lambda frame: frame.copy())
    monkeypatch.setattr(
        research, "resolve_artifact_path",
        lambda _bundle, route, *parts: tmp_path.joinpath(route, *parts),
    )
    rules = research.TransportationResearchRules.model_validate(
        research.load_harmonization_rules(bundle, "transportation_research")
    )
    charging_rules = research.load_harmonization_rules(bundle, "charging_cost_comparison")
    live = original_load(rules.notebook_scenario, repo_root=root)
    captured_scenario = live.scenario.model_dump(mode="json")
    database, build_path = tmp_path / "live.sqlite", tmp_path / "live.json"
    database.write_bytes(b"verified database")
    build_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(research, "resolve_configured_path", lambda *args: database)
    monkeypatch.setattr(research, "resolve_repo_path", lambda *args: build_path)
    checked = []
    monkeypatch.setattr(research, "_verify_build", lambda control, path, report: checked.append(path))
    identities = {name: {"path": str(path), "sha256": file_sha256(path)}
                  for name, path in (("database", database), ("build_report", build_path))}
    fleet_directory = tmp_path / rules.report_route / live.scenario.scenario.name
    if defect == "retired_scenario":
        captured_scenario = {"scenario": {"name": "retired_sales_case"}}
    fleet_manifest = write_research_report(TransportationResearch(
        {name: pd.DataFrame({"value": [1.25]}) for name in (
            "capacity_comparison", "utilization_comparison", "demand_comparison",
        )},
        {"scenario": captured_scenario, "files": identities},
        fleet_directory,
    ))
    charging_directory = tmp_path / charging_rules["report_route"]
    charging_identity = json.loads(json.dumps(identities))
    if defect in {"old_database", "old_build_report"}:
        charging_identity[defect.removeprefix("old_")]["sha256"] = "old"
    charging_scenario = "retired_sales_case" if defect == "other_charging_scenario" else live.scenario.scenario.name
    charging_manifest = write_research_report(TransportationResearch(
        {"comparisons": pd.DataFrame({"value": [2.5]})},
        {"scenario": charging_scenario, "files": charging_identity}, charging_directory,
    ))
    charging_manifest.rename(charging_directory / "snapshot.json")
    if defect in {"fleet_export", "charging_export"}:
        altered = (fleet_directory / "capacity_comparison.csv" if defect == "fleet_export"
                   else charging_directory / "comparisons.csv")
        altered.write_text("value\n999\n", encoding="utf-8")
    if defect:
        with pytest.raises(ValueError):
            research.load_validation_diagnosis(repo_root=root)
    else:
        metadata = json.loads(fleet_manifest.read_text(encoding="utf-8"))
        result = research.load_validation_diagnosis(repo_root=root)
        assert result["fleet_metadata"] == metadata
        assert result["fleet_metadata"]["scenario"] == captured_scenario
        assert result["charging_metadata"]["scenario"] == live.scenario.scenario.name
        assert result["fleet"]["capacity_comparison"].value.tolist() == [1.25]
        assert result["charging"]["comparisons"].value.tolist() == [2.5]
        assert checked == [database]
    assert loaded_scenarios == ["config/scenarios/default.yaml", rules.notebook_scenario]


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


def deeper_fleet_fixture():
    physical, controls = fleet_fixture()
    detail, summary = fleet_activity_energy(*physical, **controls)
    summary["ceud_stock_k_vehicles"] = 5.
    summary["stock_minus_ceud_k_vehicles"] = 0.
    summary["stock_relative_difference"] = 0.
    sales = pd.DataFrame([
        {"ceud_region": "ON", "road_class": "cars", "vintage_year": year,
         "age": 2025 - year, "sales_k_vehicles": 5., "survival_probability": survival,
         "surviving_k_vehicles": 5 * survival, "calibrated_surviving_k_vehicles": 4 * survival,
         "stock_calibration_multiplier": .8, "stock_calibration_basis": "base_year_ceud_stock",
         "ceud_stock_k_vehicles": 5., "included_history_start": 2024,
         "source_history_start": 2024, "older_history_status": "not_reconstructed"}
        for year, survival in ((2024, .5), (2025, .75))
    ])
    ages = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "vintage_year": year,
         "stock_method": "sales_survival", "raw_cohort_k_vehicles": raw,
         "cohort_k_vehicles": allocated, "eligible_first_model_period": eligible,
         "uncalibrated_surviving_k_vehicles": raw / .8, "stock_calibration_multiplier": .8,
         "eligibility_basis": "fixed_lifetime", "fixed_lifetime_years": 1.,
         "first_model_period": 2025, "vintage": year}
        for year, raw, allocated, eligible in ((2024, 2., 0., False), (2025, 3., 5., True))
    ])
    calibration = {"region": "ON", "road_class": "cars", "prior_mean_utilization": .4,
                   "utilization": .5, "capacity_activity_denominator_bn": 20., "target_activity_bn": 10.}
    return {
        "sales_cohorts": sales, "road_age_cohorts": ages,
        "fleet_cohort_accounting": detail, "stock_activity_energy": summary,
    }, {"utilization_audit": {"sales_activity_calibration": [calibration]},
        "comparison_significance_percent": 5,
        "scenario": {"comparison": {"absolute_tolerance": 1e-9, "relative_tolerance": 1e-12}}}


def test_deeper_fleet_separates_retirement_redistribution_and_energy_sensitivity():
    frames, metadata = deeper_fleet_fixture()
    originals = {name: frame.copy(deep=True) for name, frame in frames.items()}
    result = deepen_fleet_diagnosis(frames, metadata)
    annual = result["sales_cohort_reconciliation"]
    assert annual.retired_k_vehicles.tolist() == [2.5, 1.25]
    assert annual.eligibility_redistribution_k_vehicles.tolist() == [-2., 2.]
    stock = result["fleet_reconciliation"].iloc[0]
    assert stock.surviving_k_vehicles == 6.25
    assert stock.calibrated_surviving_k_vehicles == stock.inserted_stock_k_vehicles == 5
    assert stock.stock_minus_ceud_k_vehicles == 0
    assert stock.raw_stock_minus_ceud_k_vehicles == 1.25
    assert stock.raw_stock_difference_percent == 25
    assert stock.stock_calibration_multiplier == .8
    assert stock.ineligible_survivors_k_vehicles == 2
    assert stock.ineligible_raw_survivors_k_vehicles == 2.5
    assert stock.allocation_conservation_residual_k_vehicles == 0
    assert stock.raw_stock_significant
    assert not stock.stock_significant
    assert "omissions cannot explain the raw surplus" in stock.stock_explanation
    assert "closes before and after eligibility" in stock.stock_explanation
    assert stock.first_eligible_annual_cohort == stock.first_eligible_model_vintage == 2025
    assert stock.fixed_lifetime_years == 1
    calibration = result["utilization_calibration"].iloc[0]
    assert calibration.calibration_percent == pytest.approx(25)
    assert calibration.energy_closing_utilization == .625
    assert calibration.activity_at_energy_closure_bn == 12.5
    assert calibration.energy_closure_activity_difference_percent == 25
    assert "Activity closes; energy -20.0%" in calibration.energy_explanation
    assert "change activity by +25.0%" in calibration.energy_explanation
    stages = result["cohort_utilization_energy"].groupby("stage").vehicle_input_pj.sum()
    assert stages["Before calibration"] == pytest.approx(3.2)
    assert stages["Implemented activity calibration"] == 4
    assert stages["Energy closure sensitivity"] == 5
    implemented = result["cohort_utilization_energy"].query("stage == 'Implemented activity calibration'")
    assert implemented.annual_service_per_vehicle.tolist() == [2e6, 2e6]
    for name, original in originals.items():
        pd.testing.assert_frame_equal(frames[name], original)


@pytest.mark.parametrize("defect", ["missing_year", "survival", "nonfinite", "missing_audit", "wrong_inserted_factor", "nonconserving_allocation", "wrong_stock_multiplier", "wrong_calibrated_total", "wrong_fixed_cutoff"])
def test_deeper_fleet_rejects_incomplete_or_inconsistent_evidence(defect):
    frames, metadata = deeper_fleet_fixture()
    if defect == "missing_year":
        frames["sales_cohorts"] = frames["sales_cohorts"].iloc[:1]
    elif defect == "survival":
        frames["sales_cohorts"].loc[0, "surviving_k_vehicles"] = 3
    elif defect == "nonfinite":
        frames["fleet_cohort_accounting"].loc[0, "efficiency"] = float("inf")
    elif defect == "missing_audit":
        metadata = {}
    elif defect == "nonconserving_allocation":
        frames["road_age_cohorts"].loc[1, "cohort_k_vehicles"] += 1
    elif defect == "wrong_stock_multiplier":
        frames["sales_cohorts"]["stock_calibration_multiplier"] *= 1.1
    elif defect == "wrong_calibrated_total":
        frames["sales_cohorts"]["stock_calibration_multiplier"] *= .9
        frames["sales_cohorts"]["calibrated_surviving_k_vehicles"] *= .9
    elif defect == "wrong_fixed_cutoff":
        frames["road_age_cohorts"]["fixed_lifetime_years"] = 10.
    else:
        metadata["utilization_audit"]["sales_activity_calibration"][0]["utilization"] = .6
    with pytest.raises(ValueError):
        deepen_fleet_diagnosis(frames, metadata)


def test_positive_raw_stock_gap_explanation_does_not_attribute_it_to_missing_older_sales():
    frames, metadata = deeper_fleet_fixture()
    stock = deepen_fleet_diagnosis(frames, metadata)["fleet_reconciliation"].iloc[0]
    assert stock.raw_stock_difference_percent == 25
    assert stock.stock_difference_percent == 0
    assert "Raw sales × survival +25.0%" in stock.stock_explanation
    assert "cannot explain the raw surplus" in stock.stock_explanation
    assert "consistency remains unresolved" in stock.stock_explanation


def test_raw_stock_shortfall_stays_visible_after_ceud_calibration():
    frames, metadata = deeper_fleet_fixture()
    frames["sales_cohorts"]["sales_k_vehicles"] *= .6
    frames["sales_cohorts"]["surviving_k_vehicles"] *= .6
    frames["sales_cohorts"]["stock_calibration_multiplier"] = 4 / 3
    frames["road_age_cohorts"]["uncalibrated_surviving_k_vehicles"] *= .6
    frames["road_age_cohorts"]["stock_calibration_multiplier"] = 4 / 3
    stock = deepen_fleet_diagnosis(frames, metadata)["fleet_reconciliation"].iloc[0]
    assert stock.raw_stock_difference_percent == -25
    assert stock.calibrated_stock_minus_ceud_k_vehicles == 0
    assert "raw shortfall is unquantified" in stock.stock_explanation


def test_insertion_difference_is_reported_without_relaxing_allocation_conservation():
    frames, metadata = deeper_fleet_fixture()
    # The captured annual allocation conserves five thousand survivors, while
    # insertion/cleanup evidence can separately contain a small retained difference.
    detail = frames["fleet_cohort_accounting"]
    detail["capacity"] *= .9999
    detail["implied_activity_bn"] *= .9999
    detail["implied_vehicle_input_pj"] *= .9999
    summary = frames["stock_activity_energy"]
    summary["inserted_stock_k_vehicles"] *= .9999
    summary["implied_activity_bn"] *= .9999
    summary["implied_vehicle_input_pj"] *= .9999
    metadata["utilization_audit"]["sales_activity_calibration"][0]["capacity_activity_denominator_bn"] *= .9999
    stock = deepen_fleet_diagnosis(frames, metadata)["fleet_reconciliation"].iloc[0]
    assert stock.allocation_conservation_residual_k_vehicles == 0
    assert stock.calibrated_to_inserted_residual_k_vehicles == pytest.approx(-.0005)
    assert stock.survival_to_inserted_residual_k_vehicles == pytest.approx(-1.2505)


def charging_harmonization_fixture():
    return pd.DataFrame([
        {"table": "cost_invest", "charger_class": "ldv", "region": "ON",
         "reference_vintage": 2030, "reference_period": float("nan"),
         "reference_tech": tech, "reference_million_target_cad": 50.,
         "backend_cost_per_gw": 800., "backend_denominator_per_gw": 31.536 * .2,
         "backend_utilization_target": .2, "backend_vintage": 2025,
         "backend_cost_source_year": 2030, "target_dollar_year": 2020,
         "scope_status": "source_year_matched", "denominator": denominator}
        for tech in ("range150", "range200")
        for denominator in ("PJ/year_capacity", "PJ/year_at_backend_utilization")
    ])


def test_charging_harmonization_converts_utilized_throughput_and_collapses_rates():
    result = harmonized_charging_costs(charging_harmonization_fixture())
    assert len(result) == 1
    row = result.iloc[0]
    assert row.reference_million_cad_per_gw == pytest.approx(315.36)
    assert row.backend_million_cad_per_gw == 800
    assert row.harmonized_relative_percent == pytest.approx((800 / 315.36 - 1) * 100)


def test_charging_harmonization_rejects_distinct_reference_rates():
    frame = charging_harmonization_fixture()
    frame.loc[frame.reference_tech.eq("range200"), "reference_million_target_cad"] = 60
    with pytest.raises(ValueError, match="Different charging rates"):
        harmonized_charging_costs(frame)


@pytest.mark.parametrize("missing", ["backend_denominator_per_gw", "backend_utilization_target"])
def test_charging_harmonization_keeps_missing_bridge_or_utilization_missing(missing):
    frame = charging_harmonization_fixture()
    frame[missing] = float("nan")
    row = harmonized_charging_costs(frame).iloc[0]
    assert pd.isna(row.reference_million_cad_per_gw)
    assert pd.isna(row.harmonized_relative_percent)


def demand_diagnosis_fixture(tmp_path, monkeypatch):
    activity_path, growth_path = tmp_path / "activity.xlsx", tmp_path / "demand_growth_rate.xlsx"
    pd.DataFrame([{"Region": "ON", "Mode": "Cars", "Unit": "M passenger-km", 2023: 10000.}]).to_excel(activity_path, index=False)
    pd.DataFrame({"Year": [2023, 2025, 2030, 2035], "gdp": [1., 1.1, 1.2, 1.5],
                  "Gross Domestic Product Deflator": [100., 110., 120., 150.]}).to_excel(growth_path, index=False)
    source = """
def organize_activity_ceud(activity_ceud):
    map_comm = {'Cars': 'service'}
    activity_ceud_organized['demand'] = activity_ceud_organized['demand'] / 1000

def project_demand_v(activity_ceud_organized, demand_growth_rate, growth_rate_type):
    activity_2023 = activity_ceud_organized[activity_ceud_organized['period'] == 2023]
    demand_update_v['demand'] = demand_update_v['demand'] * demand_update_v['growth_factor']
    demand_update_v = demand_update_v[demand_update_v['period'].isin(range(2025, 2036, 5))]
    return demand_update_v

def shift_time_period(data_default, range_period):
    data_shifted['period'] = data_shifted['period'] - 5

main_compile_data_transport(growth_rate_type='gdp', range_period=range(2025, 2031, 5))
"""
    result = {
        "fleet_metadata": {
            "notebook": {"dependencies": [{"path": path.name, "provided_sha256": file_sha256(path)}
                                            for path in (activity_path, growth_path)]},
            "scenario": {"periods": {"base_year": 2025, "step": 5, "period_mode": "prospective"},
                         "demand": {"future_car_demand": "GDP-indexed"},
                         "sources": {"nrcan_ceud_beyond_observed": "extrapolated"}},
            "demand_audit": {
                "ceud_observed_year": 2023, "base_year": 2025, "base_year_gdp_index": 1.05,
                "gdp_index_by_period": {"2025": 1.5, "2030": 1.8},
                "observed_ceud_baselines": [{"region": "ON", "service": "cars", "commodity": "service",
                                              "ceud_observed_year": 2023, "baseline_demand": 10.,
                                              "gdp_indexed_base_year_demand": 10.5}],
            },
        },
        "fleet": {
            "notebook_source_cells": pd.DataFrame({"source": [source]}),
            "stock_activity_energy": pd.DataFrame([{
                "region": "ON", "ceud_region": "ON", "road_class": "cars", "ceud_activity_bn": 12.,
                "ceud_activity_observed_year": 2023, "ceud_activity_status": "extrapolated",
            }]),
            "demand_comparison": pd.DataFrame([
                {"region": "ON", "road_class": "cars", "period": year, "current": current,
                 "reference": reference, "baseline": reference}
                for year, current, reference in ((2025, 15., 20.), (2030, 18., 24.))
            ]),
            "reference_state": pd.DataFrame([{"table": "Demand", "matching_values": 2, "common_keys": 2}]),
        },
    }
    rules = research.TransportationResearchRules(
        reference_route="reference", report_route="report", reference_sqlite="unused.sqlite",
        reference_notebook="unused.ipynb", inherited_sqlite="unused.sqlite", legacy_baseline="unused.sqlite", energy_series={},
        notebook_scenario="config/scenarios/validation_baseline.yaml", comparison_significance_percent=5,
    )
    demand_rules = {"activity_to_billion_factor": .001,
                    "activity_series": {"cars": {"commodity": "service", "units": "bn passenger-km"}}}
    monkeypatch.setattr(research, "resolve_artifact_path", lambda *args: tmp_path)
    monkeypatch.setattr(research, "load_harmonization_rules", lambda *args: {"demand": demand_rules})
    return result, rules


def test_demand_replay_multiplies_workbook_index_and_decomposes_equivalent_anchors(tmp_path, monkeypatch):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    frames = deepen_demand_diagnosis(result, None, rules)
    replay = frames["demand_recipe_replay"]
    assert replay.period.tolist() == [2025, 2030]
    assert replay.source_year.tolist() == [2030, 2035]
    assert replay.notebook_recipe_demand_bn.tolist() == [12., 15.]
    attribution = frames["demand_attribution"]
    assert attribution.stored_to_recipe_change_bn.tolist() == [-8., -9.]
    assert attribution.baseline_change_bn.tolist() == [0., 0.]
    assert attribution.anchor_change_bn.tolist() == [0., 0.]
    assert attribution.replay_at_current_anchor_bn.tolist() == [12., 15.]
    assert attribution.current_growth_index.tolist() == [1.5, 1.8]
    assert attribution.growth_driver_change_bn.tolist() == [3., 3.]
    assert attribution.attribution_residual_bn.tolist() == pytest.approx([0., 0.], abs=1e-12)
    method = frames["demand_method_context"].set_index("evidence")
    assert "GDP-deflator ratio" in method.loc["Inspected notebook recipe", "growth"]
    assert "GDP(2023)" in method.loc["Current backend", "growth"]
    assert "1.050000" in method.loc["Current backend", "growth"]
    baseline = frames["demand_baseline_comparison"].iloc[0]
    assert baseline.current_activity_bn == baseline.notebook_activity_bn == 10.
    assert baseline.current_anchor_year == baseline.current_observed_year == 2023
    assert baseline.current_baseline_year == 2025
    assert baseline.current_baseline_status == "observed"
    bridge = frames["demand_base_year_bridge"].iloc[0]
    assert bridge.current_base_year_demand_bn == 10.5
    assert bridge.observation_to_base_year_growth_bn == .5
    assert bridge.historical_calibration_target_bn == 12.
    assert bridge.historical_calibration_target_status == "extrapolated"
    chart = frames["demand_method_comparison"]
    assert chart.loc[chart.evidence.eq("Stored legacy extract"), "calendar_year"].isna().all()
    assert chart.loc[chart.evidence.eq("Current backend"), "calendar_year"].tolist() == [2030, 2035]


@pytest.mark.parametrize("defect", ["tampered_workbook", "wrong_product", "duplicate_demand_write", "calendar_mode", "missing_growth_year"])
def test_demand_replay_rejects_changed_identity_recipe_and_calendar(tmp_path, monkeypatch, defect):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    if defect == "tampered_workbook":
        with (tmp_path / "activity.xlsx").open("ab") as stream:
            stream.write(b"changed after capture")
    elif defect == "wrong_product":
        result["fleet"]["notebook_source_cells"]["source"] = result["fleet"]["notebook_source_cells"].source.str.replace(
            "demand_update_v['demand'] * demand_update_v['growth_factor']",
            "demand_update_v['demand'] / demand_update_v['growth_factor']", regex=False,
        )
    elif defect == "duplicate_demand_write":
        result["fleet"]["notebook_source_cells"]["source"] = result["fleet"]["notebook_source_cells"].source.str.replace(
            "    return demand_update_v", "    demand_update_v['demand'] = 0\n    return demand_update_v", regex=False,
        )
    elif defect == "calendar_mode":
        result["fleet_metadata"]["scenario"]["periods"]["period_mode"] = "legacy"
    else:
        path = tmp_path / "demand_growth_rate.xlsx"
        growth = pd.read_excel(path)
        growth.loc[growth.Year.ne(2030)].to_excel(path, index=False)
        result["fleet_metadata"]["notebook"]["dependencies"][1]["provided_sha256"] = file_sha256(path)
    with pytest.raises(ValueError):
        deepen_demand_diagnosis(result, None, rules)


@pytest.mark.parametrize("defect", ["missing_audit", "missing_native_baseline", "wrong_observed_year",
                                  "wrong_base_year", "wrong_base_year_bridge", "missing_gdp_period",
                                  "current_row_disagrees"])
def test_demand_diagnosis_rejects_stale_or_inconsistent_observation_anchor(tmp_path, monkeypatch, defect):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    audit = result["fleet_metadata"]["demand_audit"]
    if defect == "missing_audit":
        result["fleet_metadata"].pop("demand_audit")
    elif defect == "missing_native_baseline":
        audit["observed_ceud_baselines"] = []
    elif defect == "wrong_observed_year":
        audit["observed_ceud_baselines"][0]["ceud_observed_year"] = 2025
    elif defect == "wrong_base_year":
        audit["base_year"] = 2024
    elif defect == "wrong_base_year_bridge":
        audit["observed_ceud_baselines"][0]["gdp_indexed_base_year_demand"] = 11.
    elif defect == "missing_gdp_period":
        audit["gdp_index_by_period"].pop("2025")
    else:
        result["fleet"]["demand_comparison"].loc[0, "current"] = 16.
    with pytest.raises(ValueError):
        deepen_demand_diagnosis(result, None, rules)


def test_demand_diagnosis_tail_policy_changes_only_historical_calibration_context(tmp_path, monkeypatch):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    original = deepen_demand_diagnosis(result, None, rules)
    result["fleet_metadata"]["scenario"]["sources"]["nrcan_ceud_beyond_observed"] = "constant"
    historical = result["fleet"]["stock_activity_energy"]
    historical["ceud_activity_bn"] = 10.
    historical["ceud_activity_status"] = "constant"
    changed = deepen_demand_diagnosis(result, None, rules)
    for name in ["demand_recipe_replay", "demand_method_comparison"]:
        pd.testing.assert_frame_equal(original[name], changed[name])
    assert original["demand_baseline_comparison"].iloc[0].current_activity_bn == 10.
    assert changed["demand_baseline_comparison"].iloc[0].current_activity_bn == 10.
    assert changed["demand_base_year_bridge"].iloc[0].historical_calibration_target_bn == 10.


@pytest.mark.parametrize("road_class", ["cars", "heavy_trucks"])
def test_production_energy_comparison_reconciles_common_cohorts_and_fitted_boundary(road_class):
    detail = pd.DataFrame([{"region": "ON", "tech": "car", "vintage": 2020, "capacity": 10.,
                            "c2a": 2., "factor": .55, "efficiency": 2.}])
    cases = pd.DataFrame([
        {"region": "ON", "road_class": road_class, "case": name, "utilization": uf,
         "current_base_year_demand_bn": 11., "implied_activity_bn": 20 * uf}
        for name, uf in [("Current backend", .55), ("Backend before calibration", .5),
                         ("Backend energy sensitivity", .4), ("Legacy recipe", .3)]
    ])
    cohort = detail.rename(columns={"capacity": "surviving_k_vehicles", "factor": "annual_utilization",
                                    "efficiency": "unadjusted_efficiency"})
    summary = pd.DataFrame([{"region": "ON", "road_class": road_class, "status": "calibrate",
                             "activity_bn": 11., "unadjusted_liquid_energy_pj": 5.,
                             "calibrated_liquid_energy_pj": 4., "ceud_liquid_target_pj": 4.}])
    fleet = {"fleet_cohort_accounting": detail, "baseline_activity_energy_cases": cases}
    calibrated = {"cohorts": cohort, "summary": summary}
    results = research.energy_calibration_cases(fleet, calibrated).set_index("case")
    assert set(results.road_class) == {road_class}
    assert "Legacy recipe" not in results.index
    assert results.loc["Current backend", "energy_difference_percent"] == pytest.approx(25.)
    assert results.loc["Backend after energy calibration", "energy_difference_percent"] == pytest.approx(0.)
    assert results.loc["Backend after energy calibration", "activity_difference_percent"] == pytest.approx(0.)
    assert results.loc["Backend before calibration", "activity_difference_percent"] < 0
    cohort["annual_utilization"] = .5
    with pytest.raises(ValueError, match="differ before fitting"):
        research.energy_calibration_cases(fleet, calibrated)
