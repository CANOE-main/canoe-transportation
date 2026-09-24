from __future__ import annotations

import sqlite3
from pathlib import Path

from utils import load_config_bundle
from validation.legacy_compare import compare_legacy_demand, compare_legacy_existing_capacity, compare_legacy_lifetime_tech, compare_legacy_tables


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_database(path: Path, *, v4: bool) -> None:
    with sqlite3.connect(path) as connection:
        if v4:
            connection.execute(
                "CREATE TABLE technology (tech TEXT, flag TEXT, data_id TEXT)"
            )
            connection.execute(
                "CREATE TABLE commodity (name TEXT, flag TEXT, data_id TEXT)"
            )
            connection.execute(
                "INSERT INTO technology VALUES ('T_ONE', 'p', 'dataset')"
            )
            connection.execute(
                "INSERT INTO commodity VALUES ('fuel', 'p', 'dataset')"
            )
        else:
            connection.execute("CREATE TABLE technology (tech TEXT, flag TEXT)")
            connection.execute("CREATE TABLE commodity (name TEXT, flag TEXT)")
            connection.execute("INSERT INTO technology VALUES ('T_ONE', 'p')")
            connection.execute("INSERT INTO commodity VALUES ('fuel', 'p')")


def test_legacy_compare_projects_common_columns_and_reports_v4_additions(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate.sqlite"
    reference = tmp_path / "reference.sqlite"
    _write_database(candidate, v4=True)
    _write_database(reference, v4=False)

    report = compare_legacy_tables(
        candidate, reference, tables=("technology", "commodity")
    )

    assert report["enabled"] is True
    assert report["tables"]["technology"]["candidate_only_rows"] == 0
    assert report["tables"]["technology"]["reference_only_rows"] == 0
    assert report["tables"]["technology"]["expected_v4_only_columns"] == [
        "data_id"
    ]


def test_legacy_comparison_is_scenario_controlled() -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=REPO_ROOT
    )

    assert bundle.scenario.validation.compare_legacy is True


def test_capacity_comparison_maps_legacy_units_and_reports_value_gaps(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.sqlite"
    reference = tmp_path / "reference.sqlite"
    with sqlite3.connect(candidate) as connection:
        connection.execute("CREATE TABLE existing_capacity (region TEXT, tech TEXT, vintage INTEGER, capacity REAL, units TEXT)")
        connection.execute("INSERT INTO existing_capacity VALUES ('ON', 'T_CAR', 2020, 2.0, 'k vehicles')")
    with sqlite3.connect(reference) as connection:
        connection.execute("CREATE TABLE ExistingCapacity (region TEXT, tech TEXT, vintage INTEGER, capacity REAL, units TEXT)")
        connection.execute("INSERT INTO ExistingCapacity VALUES ('ON', 'T_CAR', 2020, 3.0, 'k units')")
        connection.execute("INSERT INTO ExistingCapacity VALUES ('ON', 'T_CHRG', 2020, 1.0, 'GW')")

    result = compare_legacy_existing_capacity(candidate, reference)

    assert result["shared_tech_vintage_keys"] == 1
    assert result["unit_mismatches"] == []
    assert result["value_differences_over_1e_6"] == 1
    assert result["reference_rows"] == 1


def test_demand_comparison_reports_numeric_gap_without_unit_false_positive(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.sqlite"
    reference = tmp_path / "reference.sqlite"
    with sqlite3.connect(candidate) as connection:
        connection.execute("CREATE TABLE demand (region TEXT, commodity TEXT, period INTEGER, demand REAL, units TEXT)")
        connection.execute("INSERT INTO demand VALUES ('ON', 'T_D_pkm_ldv_c', 2025, 2.0, 'bn passenger-km')")
    with sqlite3.connect(reference) as connection:
        connection.execute("CREATE TABLE demand (region TEXT, commodity TEXT, period INTEGER, demand REAL, units TEXT)")
        connection.execute("INSERT INTO demand VALUES ('ON', 'T_D_pkm_ldv_c', 2025, 3.0, 'bpkm')")
        connection.execute("INSERT INTO demand VALUES ('ON', 'T_D_pj_off', 2025, 1.0, 'PJ')")
    result = compare_legacy_demand(candidate, reference)
    assert result["shared_keys"] == 1
    assert result["reference_rows"] == 1
    assert result["unit_mismatches"] == []
    assert result["value_differences_over_1e_6"] == 1


def test_fixed_lifetime_comparison_reports_only_numeric_shared_keys(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.sqlite"
    reference = tmp_path / "reference.sqlite"
    with sqlite3.connect(candidate) as connection:
        connection.execute("CREATE TABLE lifetime_tech (region TEXT, tech TEXT, lifetime REAL)")
        connection.executemany("INSERT INTO lifetime_tech VALUES (?, ?, ?)", [
            ("ON", "T_ONE", 12.0), ("ON", "T_TWO", 15.0), ("QC", "T_ONE", 13.0)
        ])
    with sqlite3.connect(reference) as connection:
        connection.execute("CREATE TABLE LifetimeTech (region TEXT, tech TEXT, lifetime TEXT)")
        connection.executemany("INSERT INTO LifetimeTech VALUES (?, ?, ?)", [
            ("ON", "T_ONE", "12"), ("ON", "T_TWO", "14"), ("ON", "T_BLANK", "")
        ])
    report = compare_legacy_lifetime_tech(candidate, reference)
    assert report["shared_keys"] == 2
    assert report["equal_values"] == 1
    assert report["value_differences"] == 1
    assert report["reference_blank_rows"] == 1
