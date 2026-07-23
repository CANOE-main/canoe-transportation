from __future__ import annotations

import sqlite3
from pathlib import Path

from utils import load_config_bundle
from validation.legacy_compare import compare_legacy_tables


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
