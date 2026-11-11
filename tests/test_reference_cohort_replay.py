"""Forensic stock comparisons retain actual SQLite and literal recipe values."""

import hashlib
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from utils import file_sha256, load_config_bundle, load_harmonization_rules, resolve_artifact_path
from validation import transportation_research as research


ROOT = Path(__file__).resolve().parents[1]


def stock_database(path, rows, *, current=False):
    table = "existing_capacity" if current else "ExistingCapacity"
    with sqlite3.connect(path) as connection:
        connection.execute(f"CREATE TABLE {table}(region,tech,vintage,capacity,units)")
        connection.execute("CREATE TABLE Efficiency(region,tech,vintage,output_comm)")
        connection.execute("CREATE TABLE LifetimeTech(region,tech,lifetime)")
        connection.execute("CREATE TABLE LifetimeSurvivalCurve(region,tech)")
        for tech, vintage, capacity in rows:
            connection.execute(f"INSERT INTO {table} VALUES(?,?,?,?,?)",
                               ("ON", tech, vintage, capacity, "k vehicles" if current else "k units"))
            connection.execute("INSERT INTO Efficiency VALUES(?,?,?,?)", ("ON", tech, vintage, "T_D_pkm_ldv_c"))
        for tech in {row[0] for row in rows}:
            connection.execute("INSERT INTO LifetimeTech VALUES(?,?,?)", ("ON", tech, 14.))
        connection.execute(f"INSERT INTO {table} VALUES('ON','RAIL',2020,999,'k units')")
        connection.execute("INSERT INTO Efficiency VALUES('ON','RAIL',2020,'other_service')")


@pytest.fixture
def forensic_fixture(tmp_path, monkeypatch):
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml", repo_root=ROOT)
    payload = load_harmonization_rules(bundle, "transportation_research")
    payload["reference_sqlite"] = "reference.sqlite"
    rules = research.TransportationResearchRules.model_validate(payload)
    native_directory = resolve_artifact_path(bundle, rules.reference_route)
    native_notebook = native_directory / rules.reference_notebook
    source, notebook = research.inspect_notebook(native_notebook)
    notebook_path = tmp_path / rules.reference_notebook
    notebook_path.write_bytes(native_notebook.read_bytes())
    size_path, share_path = tmp_path / "vehicle_sales_by_size.xlsx", tmp_path / "vehicle_sales_by_powertrain.xlsx"
    pd.DataFrame([{"Region": "ON", "Mode": "Cars", "Unit": "k units",
                   **{year: 10. for year in range(2000, 2024)}}]).to_excel(size_path, index=False)
    pd.DataFrame([{"Region": "ON", "Mode": "Cars", "Powertrain": fuel,
                   **{year: share for year in range(2005, 2024)}}
                  for fuel, share in [("Gasoline", .6), ("Battery electric", .3),
                                      ("Plug-in hybrid electric", .1)]]).to_excel(share_path, index=False)
    for dependency in notebook["dependencies"]:
        path = tmp_path / Path(dependency["path"]).name
        if path.is_file():
            dependency["provided_sha256"] = file_sha256(path)
    notebook.update(path=str(notebook_path), sha256=file_sha256(notebook_path))
    backend, reference = tmp_path / "backend.sqlite", tmp_path / "reference.sqlite"
    stock_database(backend, [("T_LDV_C_GSL_EX", 2020, 60.), ("T_LDV_C_BEV_EX", 2020, 10.),
                             ("T_LDV_C_GSL_PHEV_EX", 2020, 5.)], current=True)
    stock_database(reference, [("T_LDV_C_GSL_EX", 2020, 30.), ("T_LDV_C_BEV150_EX", 2015, 2.),
                               ("T_LDV_C_BEV150_EX", 2020, 6.), ("T_LDV_C_GSL_PHEV35_EX", 2020, 3.)])
    result = {"fleet_metadata": {
        "scenario": bundle.scenario.model_dump(mode="json"), "notebook": notebook,
        "files": {"database": {"path": str(backend), "sha256": file_sha256(backend)}},
        "reference_state": {"reference": {"path": str(reference), "sha256": file_sha256(reference)}},
    }, "fleet": {
        "notebook_source_cells": source,
        "stock_activity_energy": pd.DataFrame([{"region": "ON", "road_class": "cars", "base_year": 2025,
                                                "ceud_stock_k_vehicles": 100.}]),
        "capacity_comparison": pd.DataFrame([
            {"region": "ON", "road_class": "cars", "vintage": 2015, "current": None, "reference": 2.},
            {"region": "ON", "road_class": "cars", "vintage": 2020, "current": 75., "reference": 39.},
        ]),
    }}
    monkeypatch.setattr(research, "resolve_artifact_path", lambda *args: tmp_path)
    return result, bundle, rules, backend, reference, size_path, share_path, notebook_path


def test_actual_sqlite_parameters_preserve_range_aliases_and_unadjusted_class_totals(forensic_fixture):
    result, bundle, rules, *_ = forensic_fixture
    frames = research.deepen_reference_cohorts(result, bundle, rules)
    parameters = frames["built_stock_parameters"]
    reference = parameters.loc[parameters.evidence.eq("Sales variant SQLite")]
    assert reference.capacity_k_vehicles.sum() == 41.
    assert reference.loc[reference.tech.eq("T_LDV_C_BEV150_EX"), "capacity_k_vehicles"].tolist() == [2., 6.]
    assert reference.loc[reference.tech.eq("T_LDV_C_GSL_PHEV35_EX"), "capacity_k_vehicles"].tolist() == [3.]
    assert set(parameters.powertrain) == {"Gasoline", "BEV", "PHEV"}
    assert "RAIL" not in set(parameters.tech)
    totals = frames["built_stock_totals"].set_index("evidence")
    assert totals.loc["Backend SQLite", "capacity_k_vehicles"] == 75.
    assert totals.loc["Sales variant SQLite", "capacity_k_vehicles"] == 41.
    assert totals.loc["Sales variant SQLite", "stock_gap_k_vehicles"] == -59.
    assert totals.loc["Sales variant SQLite", "stock_gap_percent"] == -59.
    assert not any("matched" in name or "normalized" in name for name in frames)
    timing = frames["reference_timing_context"].iloc[0]
    assert timing.minimum_fixed_lifetime_years == timing.maximum_fixed_lifetime_years == 14.
    assert timing.survival_table_rows == 0
    assert "not established" in timing.base_year_status


def test_literal_recipe_is_unadjusted_and_reconciles_annual_contributions_to_native_parameters(forensic_fixture):
    result, bundle, rules, *_ = forensic_fixture
    frames = research.deepen_reference_cohorts(result, bundle, rules)
    recipe = frames["reference_recipe_parameters"]
    assert recipe.capacity_k_vehicles.sum() == pytest.approx(210.)
    assert set(recipe.evidence) == {"Sales variant notebook recipe"}
    assert not recipe.tech.str.endswith("_EX").any()
    annual = frames["reference_recipe_annual"]
    assert set(annual.vintage_year) == set(range(2005, 2026))
    assert set(annual.loc[annual.vintage.eq(2000), "vintage_year"]) == {2005}
    assert not {"survival_probability", "stock_calibration_multiplier", "fuel_share"} & set(annual)
    assert annual.raw_capacity_k_vehicles.sum() == pytest.approx(210.)
    totals = frames["reference_recipe_totals"].iloc[0]
    assert totals.stored_capacity_k_vehicles == 41.
    assert totals.recipe_capacity_k_vehicles == pytest.approx(210.)
    assert totals.recipe_minus_stored_k_vehicles == pytest.approx(169.)
    assert totals.common_keys == 4
    assert totals.matching_keys == 1
    assert totals.stored_only_keys == 0
    assert totals.recipe_only_keys == 11
    comparison = frames["reference_recipe_comparison"]
    assert comparison.loc[comparison.key_status.eq("right_only"), "stored_capacity_k_vehicles"].isna().all()
    assert frames["reference_recipe_steps"].step.max() >= 7


def test_reference_values_are_independent_of_backend_normalization_targets_and_fuel_allocation(forensic_fixture):
    result, bundle, rules, *_ = forensic_fixture
    before = research.deepen_reference_cohorts(result, bundle, rules)
    result["fleet"]["stock_activity_energy"]["ceud_stock_k_vehicles"] = 400.
    result["fleet"]["sales_cohorts"] = pd.DataFrame({"sales_k_vehicles": [-999.]})
    result["fleet"]["road_age_cohorts"] = pd.DataFrame({"cohort_k_vehicles": [999.]})
    result["fleet"]["road_fuel_shares"] = pd.DataFrame({"fuel_share": [999.]})
    result["fleet_metadata"]["cohort_audit"] = {"stock_calibration_multiplier": 999.}
    after = research.deepen_reference_cohorts(result, bundle, rules)
    for name in ["built_stock_parameters", "reference_recipe_parameters", "reference_recipe_annual", "reference_recipe_totals"]:
        pd.testing.assert_frame_equal(before[name], after[name])
    assert after["built_stock_totals"].set_index("evidence").loc["Sales variant SQLite", "stock_gap_k_vehicles"] == -359.


@pytest.mark.parametrize("defect", ["reference_hash", "backend_hash", "notebook_hash", "workbook_hash",
                                  "stale_class_coverage", "wrong_recipe", "wrong_periods"])
def test_forensic_comparison_rejects_drift_or_incomplete_native_coverage(forensic_fixture, defect):
    result, bundle, rules, backend, reference, size_path, _, notebook = forensic_fixture
    if defect in {"reference_hash", "backend_hash"}:
        with (reference if defect == "reference_hash" else backend).open("ab") as stream:
            stream.write(b"changed after capture")
    elif defect == "notebook_hash":
        notebook.write_bytes(b"changed notebook")
    elif defect == "workbook_hash":
        size_path.write_bytes(b"changed workbook")
    elif defect == "stale_class_coverage":
        result["fleet"]["capacity_comparison"].loc[1, "reference"] = 30.
    elif defect == "wrong_periods":
        result["fleet_metadata"]["scenario"]["periods"]["base_year"] = 2024
    else:
        source = result["fleet"]["notebook_source_cells"]
        selected = source.source.str.contains("def compile_data_existing_capacity")
        source.loc[selected, "source"] = source.loc[selected, "source"].str.replace("2006 <= y <= 2010", "2007 <= y <= 2010", regex=False)
        source.loc[selected, "source_sha256"] = source.loc[selected, "source"].map(lambda value: hashlib.sha256(value.encode()).hexdigest())
    with pytest.raises(ValueError):
        research.deepen_reference_cohorts(result, bundle, rules)


def test_native_service_ownership_must_be_unique_and_capacity_units_explicit(forensic_fixture):
    _, _, _, _, reference, *_ = forensic_fixture
    with sqlite3.connect(reference) as connection:
        connection.execute("INSERT INTO Efficiency VALUES('ON','T_LDV_C_BEV150_EX',2020,'truck_service')")
    with pytest.raises(ValueError, match="service ownership"):
        research._read_vehicle_capacity(reference, {"T_D_pkm_ldv_c": "cars", "truck_service": "trucks"})
