from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import distribute_existing_bus_capacity
from build_transport import bootstrap_database
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path


ROOT = Path(__file__).resolve().parents[1]


def _bus_inputs():
    bundle = load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=ROOT
    )
    family = load_harmonization_rules(bundle, "road_stocks_and_demands")
    rules = family["bus_existing_capacity"]
    road_rules = family["existing_capacity"]
    source = []
    efficiency = []
    lifetimes = {}
    for road_class, spec in rules["classes"].items():
        source.append(
            {
                "region": "ON", "year": 2023,
                "table_id": spec["stock"]["table_id"],
                "raw_series": spec["stock"]["raw_series"],
                "unit": "thousands", "value": 100.0,
            }
        )
        for powertrain, tech in spec["fuel_technology"].items():
            lifetimes[("ON", tech)] = 10.0
            for year in range(2000, 2024):
                efficiency.append(
                    {
                        "ceud_region": "ON", "region": "ON",
                        "road_class": road_class, "tech": tech,
                        "powertrain": powertrain, "year": year,
                        "efficiency": 2.0 if powertrain == "gasoline" else 1.0,
                        "units": "bn passenger-km/PJ",
                    }
                )
                for member in rules["fuel_source_members"][powertrain]:
                    source.append(
                        {
                            "region": "ON", "year": year,
                            "table_id": spec["energy_table"],
                            "raw_series": spec["energy_prefix"] + member,
                            "unit": "PJ", "value": 1.0 if member in rules["required_fuel_members"] else 0.0,
                        }
                    )
        if road_class == "urban_transit":
            source.append(
                {
                    "region": "ON", "year": 2001,
                    "table_id": spec["energy_table"],
                    "raw_series": spec["energy_prefix"] + "Propane",
                    "unit": "PJ", "value": 0.5,
                }
            )
    age = pd.DataFrame(
        [
            {"year": 2025, "VEHICLE_CLASS": "BUS", "AGE": value, "AGE_DIST": 0.5}
            for value in (0, 30)
        ]
    )
    return pd.DataFrame(source), age, pd.DataFrame(efficiency), lifetimes, road_rules, rules


def test_bus_capacity_uses_annual_activity_and_redistributes_old_cohorts() -> None:
    provincial, age, efficiency, lifetimes, road_rules, rules = _bus_inputs()
    cohorts, shares, exclusions, transfers, capacity = distribute_existing_bus_capacity(
        provincial=provincial,
        report5_age=age,
        annual_efficiency=efficiency,
        lifetimes=lifetimes,
        regions=["ON"], base_year=2023, first_model_period=2025,
        road_rules=road_rules, rules=rules,
    )
    assert set(capacity.road_class) == set(rules["classes"])
    assert capacity.groupby("road_class").capacity.sum().to_dict() == pytest.approx(
        {name: 100.0 for name in rules["classes"]}
    )
    assert set(capacity.loc[capacity.capacity.gt(0), "vintage"]) == {2023}
    assert cohorts.loc[cohorts.age.eq(30), "source_year"].eq(2000).all()
    school = shares.loc[
        shares.road_class.eq("school_buses") & shares.year.eq(2023)
        & shares.powertrain.eq("gasoline")
    ]
    assert float(school.iloc[0].activity_share) == pytest.approx(0.5)
    assert len(exclusions) == 1
    assert exclusions.iloc[0].fuel_member == "Propane"
    assert exclusions.iloc[0].excluded_energy_pj == pytest.approx(0.5)
    assert transfers.empty


def test_bus_orphaned_technology_stock_moves_within_its_class() -> None:
    provincial, age, efficiency, lifetimes, road_rules, rules = _bus_inputs()
    school = rules["classes"]["school_buses"]
    cng = school["energy_prefix"] + "Natural Gas"
    mask = (
        provincial.raw_series.eq(cng)
        & provincial.year.ge(2014)
        & provincial.table_id.eq(school["energy_table"])
    )
    provincial.loc[mask, "value"] = 0.0
    cohorts, _, _, transfers, capacity = distribute_existing_bus_capacity(
        provincial=provincial, report5_age=age, annual_efficiency=efficiency,
        lifetimes=lifetimes, regions=["ON"], base_year=2023,
        first_model_period=2025, road_rules=road_rules, rules=rules,
    )
    assert set(transfers.from_tech) == {school["fuel_technology"]["cng"]}
    assert set(transfers.to_tech) == {
        school["fuel_technology"]["gasoline"],
        school["fuel_technology"]["diesel"],
    }
    assert transfers.capacity_k_vehicles.sum() > 0
    assert capacity.loc[capacity.road_class.eq("school_buses"), "capacity"].sum() == pytest.approx(100)
    assert cohorts.loc[cohorts.tech.eq(school["fuel_technology"]["cng"]), "capacity"].sum() == 0


def test_bus_capacity_has_efficiency_and_lifetime_in_standalone_sqlite(tmp_path: Path) -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=ROOT
    )
    database = tmp_path / "transport.sqlite"
    report = bootstrap_database(
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        database_path=database,
    )
    assert report["validation"]["integrity_check"] == ["ok"]
    with sqlite3.connect(database) as connection:
        bus = "(x.tech LIKE 'T_HDV_BS_%' OR x.tech LIKE 'T_HDV_BT_%' OR x.tech LIKE 'T_HDV_BIC_%')"
        rows, technologies, regions = connection.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT x.tech), COUNT(DISTINCT x.region) "
            f"FROM existing_capacity x WHERE {bus}"
        ).fetchone()
        assert rows > 0 and technologies == 9 and regions == 10
        missing = connection.execute(
            f"SELECT COUNT(*) FROM existing_capacity x WHERE {bus} AND "
            "(NOT EXISTS (SELECT 1 FROM efficiency e WHERE e.region=x.region "
            "AND e.tech=x.tech AND e.vintage=x.vintage) OR "
            "NOT EXISTS (SELECT 1 FROM lifetime_tech l WHERE l.region=x.region "
            "AND l.tech=x.tech))"
        ).fetchone()[0]
        assert missing == 0
