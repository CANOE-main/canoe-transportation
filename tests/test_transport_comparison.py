"""Meaningful boundary cases for the read-only transport comparison adapter."""

from contextlib import contextmanager
from dataclasses import replace
import json
import sqlite3

import pandas as pd
import pytest
from pydantic import ValidationError

from utils import file_sha256, load_config_bundle
from validation.config_models import ScenarioPeriods
from validation.transport_comparison import (
    ComparisonConfig, Diagnostic, _assumption_excerpts, _verify_snapshot,
    compare_parameters, explanation_rows, ranked_observations, ranked_rows, write_report,
)


PERIODS = ScenarioPeriods(
    period_mode="legacy", base_year=2023,
    existing=[2000, 2005, 2010, 2015, 2020, 2023], model=[2025, 2030], step=5,
)


def config() -> ComparisonConfig:
    return ComparisonConfig(
        scenario="scenario.yaml", reference_sqlite="legacy.sqlite", assumptions_path="assumptions.md",
        existing_range=(2000, 2023), future_range=(2025, 2050), top_n=10, explanations={},
    )


@contextmanager
def databases(tmp_path):
    paths = tmp_path / "candidate.sqlite", tmp_path / "legacy.sqlite"
    with sqlite3.connect(paths[0]) as candidate, sqlite3.connect(paths[1]) as legacy:
        yield candidate, legacy, paths


def compare(paths, **kwargs):
    return compare_parameters(*paths, config(), periods=PERIODS, **kwargs)


def test_exact_keys_units_scope_and_historical_coverage_are_separate(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        candidate.executescript("""
            CREATE TABLE technology(tech TEXT);
            INSERT INTO technology VALUES ('T_CAR'), ('T_NEW');
            CREATE TABLE existing_capacity(region TEXT, tech TEXT, vintage INT, capacity REAL, units TEXT);
            INSERT INTO existing_capacity VALUES
                ('ON','T_CAR',2000,4,'k vehicles'), ('ON','T_CAR',2020,10,'k vehicles'),
                ('ON','T_CAR',2023,5,'k vehicles'), ('QC','T_CAR',2020,999,'k vehicles');
            CREATE TABLE efficiency(region TEXT, tech TEXT, vintage INT, input_comm TEXT, output_comm TEXT, efficiency REAL, units TEXT);
            INSERT INTO efficiency VALUES
                ('ON','T_CAR',2020,'gas','service',5,'bn passenger-km/PJ'),
                ('ON','T_NEW',2025,'gas','service',2,'bn passenger-km/PJ');
        """)
        legacy.executescript("""
            CREATE TABLE ExistingCapacity(region TEXT, tech TEXT, vintage INT, capacity REAL, units TEXT);
            INSERT INTO ExistingCapacity VALUES
                ('ON','T_CAR',2020,8,'k units'), ('ON','UNRELATED',2020,1000000,'GW');
            CREATE TABLE Efficiency(region TEXT, tech TEXT, vintage INT, input_comm TEXT, output_comm TEXT, efficiency REAL);
            INSERT INTO Efficiency VALUES
                ('ON','T_CAR',2020,'gas','service',4), ('ON','T_NEW',2025,'gas','service',1);
        """)
    before = [file_sha256(path) for path in paths]
    summary, rows, metadata = compare(paths)
    stock = rows.loc[rows.table.eq("existing_capacity")]
    assert set(stock.band) == {"All"}
    assert stock.loc[stock.vintage.eq(2020), "relative_percent"].item() == 25
    assert set(stock.loc[stock.vintage.isin([2000, 2023]), "status"]) == {"candidate_only"}
    assert stock.loc[stock.vintage.eq(2023), "legacy"].isna().all()
    assert len(rows.loc[rows.table.eq("efficiency")].band.unique()) == 2
    assert metadata["candidate_regions_without_legacy"] == ["QC"]
    assert metadata["scope"]["existing_capacity"]["legacy_ignored"] == 1
    assert summary.different.sum() == 3
    assert [file_sha256(path) for path in paths] == before


def test_charger_rate_and_profile_bridges_require_equal_constituents(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        candidate.executescript("""
            CREATE TABLE cost_fixed(region TEXT,tech TEXT,vintage INT,period INT,cost REAL,units TEXT);
            INSERT INTO cost_fixed VALUES
                ('ON','T_LDV_CHRG_EX',2023,2025,20,'$M 2020CAD / GW'),
                ('ON','T_LDV_CHRG_N',2025,2025,14,'$M 2020CAD / GW'),
                ('ON','T_MHDV_CHRG_N',2025,2025,11,'$M 2020CAD / GW');
            CREATE TABLE capacity_factor_tech(region TEXT,tech TEXT,season TEXT,tod TEXT,factor REAL);
            INSERT INTO capacity_factor_tech VALUES
                ('ON','T_LDV_CHRG_EX','D001','H01',0.4), ('ON','T_LDV_CHRG_N','D001','H01',0.4);
        """)
        legacy.executescript("""
            CREATE TABLE TimePeriod(period INT,flag TEXT);
            INSERT INTO TimePeriod VALUES(2020,'e'),(2021,'f');
            CREATE TABLE CostFixed(region TEXT,tech TEXT,vintage INT,period INT,cost REAL,units TEXT);
            INSERT INTO CostFixed VALUES
                ('ON','T_LDV_BEV_CHRG',2015,2025,16,'2020 CAD (M$/GW)'),
                ('ON','T_LDV_PHEV_CHRG',2020,2025,16,'2020 CAD (M$/GW)'),
                ('ON','T_LDV_BEV_CHRG',2025,2025,16,'2020 CAD (M$/GW)'),
                ('ON','T_LDV_PHEV_CHRG',2025,2025,16,'2020 CAD (M$/GW)'),
                ('ON','T_MDV_CHRG',2025,2025,10,'2020 CAD (M$/GW)'),
                ('ON','T_HDV_CHRG',2025,2025,12,'2020 CAD (M$/GW)');
            CREATE TABLE CapacityFactorTech(region TEXT,tech TEXT,season TEXT,tod TEXT,factor REAL);
            INSERT INTO CapacityFactorTech VALUES ('ON','T_LDV_BEV_CHRG','D001','H01',0.5);
        """)
    _, rows, _ = compare(paths)
    rates = rows.loc[rows.table.eq("cost_fixed")]
    assert rates.loc[rates.tech.eq("T_LDV_CHRG_EX"), "legacy"].item() == 16
    assert rates.loc[rates.tech.eq("T_LDV_CHRG_EX"), "relative_percent"].item() == 25
    assert rates.loc[rates.tech.eq("T_MHDV_CHRG_N"), "status"].item() == "representation_only"
    assert set(rows.loc[rows.table.eq("capacity_factor_tech"), "status"]) == {"different"}
    assert len(ranked_rows(rows, 10)) == 4


def test_annual_factors_project_cohorts_to_explicit_operating_period_and_bounds(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        candidate.executescript("""
            CREATE TABLE limit_annual_capacity_factor(region TEXT,tech_or_group TEXT,vintage INT,output_comm TEXT,operator TEXT,factor REAL);
            INSERT INTO limit_annual_capacity_factor VALUES
                ('ON','T_CAR',2023,'service','e',0.12),
                ('ON','T_CAR',2030,'service','e',0.08);
        """)
        legacy.executescript("""
            CREATE TABLE MinAnnualCapacityFactor(region TEXT,tech TEXT,period INT,output_comm TEXT,factor REAL);
            CREATE TABLE MaxAnnualCapacityFactor(region TEXT,tech TEXT,period INT,output_comm TEXT,factor REAL);
            INSERT INTO MinAnnualCapacityFactor VALUES
                ('ON','T_CAR',2025,'service',0.09),('ON','T_CAR',2030,'service',0.07),('ON','T_CAR',2021,'service',0.02);
            INSERT INTO MaxAnnualCapacityFactor VALUES
                ('ON','T_CAR',2025,'service',0.1),('ON','T_CAR',2030,'service',0.09),('ON','T_CAR',2021,'service',0.03);
        """)
    _, rows, _ = compare(paths)
    existing = rows.loc[rows.vintage.eq(2023)].iloc[0]
    assert existing.comparison_period == 2025
    assert existing.legacy_lower == 0.09 and existing.legacy_upper == 0.1
    assert existing.absolute_difference == pytest.approx(0.02)
    assert existing.relative_percent == pytest.approx(20)
    assert rows.loc[rows.vintage.eq(2030), "status"].item() == "equal"
    assert set(rows.loc[rows.period.eq(2021), "status"]) == {"legacy_only"}
    assert len(ranked_rows(rows, 10)) == 1


def test_survival_representation_is_not_compared_to_zero(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        candidate.executescript("""
            CREATE TABLE lifetime_survival_curve(region TEXT,tech TEXT,vintage INT,period INT,fraction REAL);
            INSERT INTO lifetime_survival_curve VALUES ('ON','T_CAR',2025,2025,0.95);
            CREATE TABLE lifetime_tech(region TEXT,tech TEXT,lifetime REAL,units TEXT);
            INSERT INTO lifetime_tech VALUES ('ON','T_BUS',15,'years');
        """)
        legacy.executescript("""
            CREATE TABLE LifetimeTech(region TEXT,tech TEXT,lifetime REAL);
            INSERT INTO LifetimeTech VALUES ('ON','T_CAR',14),('ON','T_BUS',13);
        """)
    _, rows, _ = compare(paths)
    curve = rows.loc[rows.table.eq("lifetime_survival_curve")].iloc[0]
    assert curve.status == "representation_only" and pd.isna(curve.legacy)
    car_lifetime = rows.loc[rows.table.eq("lifetime_tech") & rows.tech.eq("T_CAR")].iloc[0]
    assert car_lifetime.status == "representation_only"
    assert len(ranked_rows(rows, 10)) == 1


def test_zero_baseline_units_and_duplicate_keys_never_become_false_parity(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        for connection, table in ((candidate, "cost_invest"), (legacy, "CostInvest")):
            connection.execute(f"CREATE TABLE {table}(region TEXT,tech TEXT,vintage INT,cost REAL,units TEXT)")
        candidate.executemany("INSERT INTO cost_invest VALUES ('ON',?,2025,?,?)", [
            ("T_ZERO", 5, "$M 2020CAD / k vehicles"),
            ("T_UNIT", 20, "$M 2020CAD / k vehicles"),
            ("T_DUP", 10, "$M 2020CAD / k vehicles"),
            ("T_DUP", 11, "$M 2020CAD / k vehicles"),
        ])
        legacy.executemany("INSERT INTO CostInvest VALUES ('ON',?,2025,?,?)", [
            ("T_ZERO", 0, "2020 CAD (M$/k units)"),
            ("T_UNIT", 20, "2015 CAD (M$/k units)"),
            ("T_DUP", 10, "2020 CAD (M$/k units)"),
        ])
    _, rows, _ = compare(paths)
    assert rows.loc[rows.tech.eq("T_ZERO"), "status"].item() == "different"
    assert rows.loc[rows.tech.eq("T_ZERO"), "relative_percent"].isna().all()
    assert rows.loc[rows.tech.eq("T_UNIT"), "status"].item() == "unit_mismatch"
    assert rows.loc[rows.tech.eq("T_DUP"), "status"].item() == "ambiguous_key"
    assert ranked_rows(rows, 10).tech.tolist() == ["T_ZERO"]


def test_structural_technology_without_generated_parameter_is_out_of_scope(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        candidate.executescript("""
            CREATE TABLE technology(tech TEXT);
            INSERT INTO technology VALUES ('T_VEHICLE'),('T_H2_LDV_REFUEL');
            CREATE TABLE cost_invest(region TEXT,tech TEXT,vintage INT,cost REAL,units TEXT);
            INSERT INTO cost_invest VALUES ('ON','T_VEHICLE',2025,12,'$M 2020CAD / k vehicles');
        """)
        legacy.executescript("""
            CREATE TABLE CostInvest(region TEXT,tech TEXT,vintage INT,cost REAL,units TEXT);
            INSERT INTO CostInvest VALUES
                ('ON','T_VEHICLE',2025,10,'2020 CAD (M$/k units)'),
                ('ON','T_VEHICLE',2050,8,'2020 CAD (M$/k units)'),
                ('ON','T_H2_LDV_REFUEL',2025,999,'2020 CAD (M$/GW)');
        """)
    _, rows, metadata = compare(paths)
    assert set(rows.tech) == {"T_VEHICLE"}
    assert rows.loc[rows.vintage.eq(2050), "status"].item() == "legacy_only"
    assert metadata["scope"]["cost_invest"]["legacy_ignored"] == 1


def test_absolute_rankings_keep_units_separate_and_signed_relative_magnitude():
    rows = pd.DataFrame([
        {"table": "cost_variable", "band": "All", "status": "different", "units": units,
         "identifier": tech, "absolute_difference": delta, "relative_percent": percent}
        for units, tech, delta, percent in [
            ("passenger", "large", 100, -90), ("passenger", "small", 50, 50),
            ("freight", "freight", 5, 95),
        ]
    ])
    assert set(ranked_rows(rows, 1, "absolute_difference").identifier) == {"large", "freight"}
    assert ranked_rows(rows, 1).identifier.tolist() == ["freight"]
    assert ranked_rows(rows, 2).identifier.tolist() == ["freight", "large"]


def test_relative_rankings_retain_zero_baseline_in_its_own_unit_group():
    rows = pd.DataFrame([
        {"table": "cost_variable", "band": "All", "status": "different", "units": "passenger",
         "identifier": "finite", "absolute_difference": 5, "relative_percent": 20},
        {"table": "cost_variable", "band": "All", "status": "different", "units": "passenger",
         "identifier": "zero", "absolute_difference": 3, "relative_percent": None},
    ])
    ranked = ranked_rows(rows, 1)
    assert set(ranked.identifier) == {"finite", "zero"}
    assert ranked.loc[ranked.identifier.eq("zero"), "ranking_basis"].item().startswith("legacy zero")


def test_rankings_collapse_identifiers_and_count_repeated_values_and_coverage():
    rows = pd.DataFrame([
        {"table": "cost_variable", "band": "Existing", "units": "passenger", "region": "ON",
         "tech": "T_BUS", "vintage": vintage, "period": 2025, "identifier": f"T_BUS/{vintage}",
         "status": status, "candidate": candidate, "legacy": legacy, "delta": delta,
         "absolute_difference": abs(delta) if delta is not None else None,
         "relative_percent": percent}
        for vintage, status, candidate, legacy, delta, percent in [
            (2000, "different", 20, 10, 10, 100), (2005, "different", 20, 10, 10, 100),
            (2010, "different", 15, 10, 5, 50), (2020, "equal", 20, 20, 0, 0),
            (2023, "candidate_only", 15, None, None, None),
        ]
    ])
    other = rows.iloc[[0]].assign(tech="T_CAR", identifier="T_CAR/2000", relative_percent=75)
    future = rows.iloc[[0]].assign(band="Future", vintage=2030, identifier="T_BUS/2030")
    grouped = ranked_rows(pd.concat([rows, other, future], ignore_index=True), 2)
    existing = grouped.loc[grouped.band.eq("Existing")]
    assert existing.tech.tolist() == ["T_BUS", "T_CAR"]
    assert existing.identifier.tolist() == ["region=ON; tech=T_BUS", "region=ON; tech=T_CAR"]
    record = existing.iloc[0]
    assert record.observation_count == 2
    assert record.different_observations == 3
    assert record.comparable_observations == 4
    assert record.total_observations == 5
    assert record.value_vintages == "2000, 2005"
    assert record.value_periods == "2025"
    assert record.representative_identifier == "T_BUS/2000"
    pd.testing.assert_frame_equal(ranked_observations(rows, record), rows)
    assert len(grouped.loc[grouped.band.eq("Future")]) == 1


def test_grouped_rankings_keep_units_commodities_and_zero_baseline_counts():
    rows = pd.DataFrame([
        {"table": "demand", "band": "All", "status": "different", "units": units,
         "region": "ON", "commodity": commodity, "period": year,
         "identifier": f"ON/{commodity}/{year}", "candidate": candidate, "legacy": legacy,
         "delta": candidate - legacy, "absolute_difference": abs(candidate - legacy),
         "relative_percent": 100 * (candidate - legacy) / legacy if legacy else None}
        for units, commodity, year, candidate, legacy in [
            ("passenger", "car", 2025, 12, 10), ("passenger", "car", 2030, 50, 0),
            ("freight", "car", 2025, 8, 4), ("freight", "marine", 2025, 2, 0),
        ]
    ])
    relative = ranked_rows(rows, 10)
    assert len(relative) == 3
    passenger = relative.loc[relative.units.eq("passenger")].iloc[0]
    assert passenger.relative_percent == 20
    assert passenger.zero_baseline_observations == 1
    assert passenger.comparable_observations == 2
    assert len(ranked_observations(rows, passenger)) == 2
    absolute = ranked_rows(rows, 1, "absolute_difference")
    assert absolute.loc[absolute.units.eq("passenger"), "candidate"].item() == 50
    assert absolute.loc[absolute.units.eq("freight"), "commodity"].item() == "car"


def test_duplicate_legacy_hour_is_not_silently_collapsed_by_profile_bridge(tmp_path):
    with databases(tmp_path) as (candidate, legacy, paths):
        for connection, table, tech in (
            (candidate, "capacity_factor_tech", "T_LDV_CHRG_N"),
            (legacy, "CapacityFactorTech", "T_LDV_BEV_CHRG"),
        ):
            connection.execute(f"CREATE TABLE {table}(region TEXT,tech TEXT,season TEXT,tod TEXT,factor REAL)")
            connection.execute(f"INSERT INTO {table} VALUES ('ON',?,'D001','H01',0.4)", (tech,))
        legacy.execute("INSERT INTO CapacityFactorTech VALUES ('ON','T_LDV_BEV_CHRG','D001','H01',0.4)")
    _, rows, _ = compare(paths)
    assert rows.status.tolist() == ["ambiguous_key"]
    assert ranked_rows(rows, 10).empty


def test_explicit_report_exports_portable_rows_and_content_manifest(tmp_path):
    bundle = replace(load_config_bundle("config/scenarios/legacy_comparison.yaml"), repo_root=tmp_path)
    rows = pd.DataFrame([
        {"table": "demand", "band": "All", "status": "different", "units": "bn passenger-km",
         "candidate": 12, "legacy": 10, "identifier": "ON/2025/car",
         "absolute_difference": 2, "relative_percent": 20},
    ])
    diagnostic = Diagnostic(config(), bundle, pd.DataFrame([{"table": "demand", "different": 1}]),
                            rows, {"files": {"candidate": {"sha256": "input-identity"}}}, {})
    destination = write_report(diagnostic)
    snapshot = json.loads((destination / "snapshot.json").read_text(encoding="utf-8"))
    assert set(snapshot["exports"]) == {
        "summary.csv", "rows.csv", "ranked_relative.csv", "ranked_absolute.csv", "explanations.csv",
    }
    assert pd.read_csv(destination / "rows.csv").legacy.tolist() == [10]
    assert snapshot["metadata"]["files"]["candidate"]["sha256"] == "input-identity"
    assert all(file_sha256(destination / name) == value["sha256"]
               for name, value in snapshot["exports"].items())


def test_structured_explanation_rows_retain_source_and_assumption_trace():
    selected = ComparisonConfig.model_validate({**config().model_dump(), "explanations": {
        "demand": {"summary": "Demand calibration.", "assumption_sections": ["demand"],
                   "evidence_paths": ["demand.csv"], "drivers": [{
                       "scope": "Marine", "legacy_basis": "Historical average intensity",
                       "refactored_basis": "Same-year intensity",
                       "explanation": "A changed service denominator changes calibrated demand.",
                       "assumption_sections": ["demand", "Shared road fleet composition"],
                       "evidence_paths": ["demand.csv", "sources.yaml"],
                   }]},
    }})
    records = explanation_rows(selected)
    assert records.table.tolist() == ["demand"]
    assert records.scope.tolist() == ["Marine"]
    assert records.evidence_paths.item() == "demand.csv; sources.yaml"
    assert records.assumption_sections.item() == "demand; Shared road fleet composition"
    assert explanation_rows(selected, "efficiency").empty


def test_config_and_current_assumption_excerpts_are_reloaded(tmp_path):
    with pytest.raises(ValidationError, match="ordered and disjoint"):
        ComparisonConfig.model_validate({**config().model_dump(), "future_range": [2020, 2050]})
    path = tmp_path / "assumptions.md"
    path.write_text("## `demand`\nInitial assumption.\n## `efficiency`\nUnrelated.\n", encoding="utf-8")
    selected = ComparisonConfig.model_validate({**config().model_dump(), "explanations": {
        "demand": {"summary": "Demand calibration.", "assumption_sections": ["demand"], "evidence_paths": []},
    }})
    assert _assumption_excerpts(path, selected)["demand"][0]["text"] == "Initial assumption."
    path.write_text("## `demand`\nReviewed assumption.\n", encoding="utf-8")
    assert _assumption_excerpts(path, selected)["demand"][0]["text"] == "Reviewed assumption."


def test_snapshot_rejects_changed_recipe_or_database(tmp_path):
    bundle = load_config_bundle("config/scenarios/legacy_comparison.yaml")
    scenario = bundle.scenario.model_copy(update={"periods": PERIODS})
    bundle = replace(bundle, scenario=scenario)
    database = tmp_path / "candidate.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE time_period(period INT,flag TEXT)")
        connection.executemany("INSERT INTO time_period VALUES (?, 'e')", [(year,) for year in PERIODS.existing])
        connection.execute("CREATE TABLE demand(period INT)")
        connection.executemany("INSERT INTO demand VALUES (?)", [(year,) for year in PERIODS.model])
    parameter = tmp_path / "rules.yaml"
    manual = tmp_path / "manual.csv"
    parameter.write_text("value: 1", encoding="utf-8")
    manual.write_text("value\n1\n", encoding="utf-8")
    def fingerprints(path):
        return {"path": str(path), "sha256": file_sha256(path)}
    report = {
        "ok": True, "scenario": scenario.scenario.name, "database": str(database),
        "database_sha256": file_sha256(database), "periods": PERIODS.model_dump(mode="json"),
        "config": {
            "scenario": fingerprints(bundle.scenario_path), "paths": fingerprints(bundle.paths_path),
            "sources": fingerprints(bundle.sources_path), "parameters": [fingerprints(parameter)],
            "manual_inputs": [fingerprints(manual)],
        },
    }
    assert _verify_snapshot(bundle, database, report)["status"] == "verified"
    parameter.write_text("value: 2", encoding="utf-8")
    with pytest.raises(ValueError, match="input changed"):
        _verify_snapshot(bundle, database, report)
    report["config"]["parameters"] = [fingerprints(parameter)]
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM demand WHERE period=2030")
    with pytest.raises(ValueError, match="fingerprint"):
        _verify_snapshot(bundle, database, report)
    report["database_sha256"] = file_sha256(database)
    with pytest.raises(ValueError, match="demand periods"):
        _verify_snapshot(bundle, database, report)
