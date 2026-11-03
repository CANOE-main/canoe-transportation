"""Research reports evaluate model quantities and preserve reference uncertainty."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from utils import file_sha256, load_config_bundle
from validation.transportation_research import (
    NotebookReplay, TransportationResearch, _verify_accepted_survival, _verify_build,
    compare_reference_state, fleet_activity_energy, inspect_notebook,
    notebook_cohort_sensitivity, write_research_report,
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


def replay_fixture():
    sales = pd.DataFrame([
        {"ceud_region": "ON", "road_class": "cars", "vintage_year": year,
         "sales_k_vehicles": 10., "survival_source_id": "s", "survival_source_class": "c"}
        for year in range(2021, 2026)
    ])
    curves = pd.DataFrame([{"source_id": "s", "source_class": "c", "age": age,
                            "survival_probability": fraction}
                           for age, fraction in enumerate([1., .9, .8, .7, .6, .5])])
    replay = NotebookReplay(sales_start_year=2021, sales_end_year=2025,
                            calibration_year=2023, vintage_step=5, extension_policy="constant")
    return sales, curves, replay


def test_notebook_sensitivity_separates_future_sales_and_representative_age_bias():
    sales, curves, replay = replay_fixture()
    row = notebook_cohort_sensitivity(sales, curves, replay).iloc[0]
    assert row.notebook_vintage == 2020
    assert row.raw_sales_k_vehicles == 50
    assert row.post_calibration_sales_k_vehicles == 20
    assert row.notebook_active_k_vehicles == 35
    assert row.annual_active_k_vehicles == 27
    assert row.future_sales_contribution_k_vehicles == 14
    assert row.timing_and_binning_difference_without_future_sales == -6
    assert row.notebook_minus_annual_k_vehicles == 8
    assert "not_missing_workbook_reproduction" in row.evidence_status


def test_notebook_replay_fails_missing_survival_instead_of_zero_filling():
    sales, curves, replay = replay_fixture()
    curves = curves.loc[curves.age.ne(3)]
    with pytest.raises(ValueError, match="representative vintage"):
        notebook_cohort_sensitivity(sales, curves, replay)


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


def test_research_freshness_rejects_stale_database_before_reading_shared_artifacts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=root)
    database = tmp_path / "database.sqlite"
    database.write_bytes(b"not the built snapshot")
    report = {"ok": True, "scenario": bundle.scenario.scenario.name, "database_sha256": "old"}
    with pytest.raises(ValueError, match="database differs"):
        _verify_build(replace(bundle, repo_root=tmp_path), database, report)


def test_research_survival_fingerprint_accepts_exact_build_evidence():
    _, curves, _ = replay_fixture()
    expected = hashlib.sha256(curves.to_csv(index=False).encode("utf-8")).hexdigest()
    assert _verify_accepted_survival(curves, {"accepted_survival_sha256": expected}) == expected


@pytest.mark.parametrize("missing", [False, True])
def test_research_survival_fingerprint_rejects_stale_or_missing_build_evidence(missing):
    _, curves, _ = replay_fixture()
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
