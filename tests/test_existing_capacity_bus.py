from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sqlite3

import pandas as pd
import pytest
import yaml

from parameterization.road_stocks_and_demands import distribute_existing_bus_capacity
from validation.config_models import ScenarioPeriods
from build_transport import bootstrap_database
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path


ROOT = Path(__file__).resolve().parents[1]


def _bus_inputs():
    bundle = load_config_bundle(
        "config/scenarios/default.yaml", repo_root=ROOT
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
                        "consumption_mj_per_vkm": 0.5 if powertrain == "gasoline" else 1.0,
                        "load_factor": 1.0,
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
    # Passenger-km and VKT shares would differ if powertrain loads diverged.
    efficiency.loc[efficiency.powertrain.eq("bev"), ["load_factor", "efficiency"]] = 10.0
    electricity = provincial.raw_series.str.endswith("|Electricity") & provincial.year.eq(2023)
    provincial["ceud_observed_year"] = provincial.year
    provincial["ceud_value_status"] = "observed"
    provincial.loc[electricity, ["ceud_observed_year", "ceud_value_status"]] = [2022, "constant"]
    cohorts, shares, exclusions, transfers, capacity = distribute_existing_bus_capacity(
        provincial=provincial,
        report5_age=age,
        annual_efficiency=efficiency,
        lifetimes=lifetimes,
        regions=["ON"],
        period_config=ScenarioPeriods(
            period_mode="legacy", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020, 2023], model=[2025], step=5,
        ),
        vehicle_population_year=2025,
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
    bev = shares.loc[shares.powertrain.eq("bev") & shares.year.eq(2023)].iloc[0]
    assert bev.vehicle_km_bn == pytest.approx(1.0)
    assert bev.vkt_share == pytest.approx(0.2)
    assert bev.activity_share == pytest.approx(10 / 14)
    assert bev.ceud_energy_observed_years == "2022"
    assert bev.ceud_energy_status == "constant"
    assert capacity.loc[capacity.tech.eq("T_HDV_BT_BEV_EX"), "capacity"].sum() == pytest.approx(20.0)
    assert len(exclusions) == 1
    assert exclusions.iloc[0].fuel_member == "Propane"
    assert exclusions.iloc[0].excluded_energy_pj == pytest.approx(0.5)
    assert transfers.empty


@pytest.mark.parametrize("first_vintage", [1995, 2015])
def test_bus_source_year_and_lifetime_are_independent_of_first_vintage(first_vintage):
    provincial, age, efficiency, lifetimes, road_rules, rules = _bus_inputs()
    age = pd.DataFrame({
        "year": 2025, "VEHICLE_CLASS": "BUS", "AGE": [0, 8, 9, 30],
        "AGE_DIST": [0.25] * 4,
    })
    lifetimes = {key: 11 for key in lifetimes}
    periods = ScenarioPeriods(
        period_mode="legacy", base_year=2023,
        existing=[*range(first_vintage, 2021, 5), 2023], model=[2025], step=5,
    )
    cohorts, shares, _, _, capacity = distribute_existing_bus_capacity(
        provincial=provincial, report5_age=age, annual_efficiency=efficiency,
        lifetimes=lifetimes, regions=["ON"], period_config=periods,
        vehicle_population_year=2025, road_rules=road_rules, rules=rules,
    )
    assert set(shares.year) == set(range(2000, 2024))
    assert cohorts.loc[cohorts.vintage_year.eq(1993), "source_year"].eq(2000).all()
    assert cohorts.loc[cohorts.vintage_year.eq(1993), "vintage"].eq(first_vintage).all()
    assert cohorts.loc[cohorts.vintage_year.eq(2014), "source_year"].eq(2014).all()
    # 2014 + 11 equals the first period, so retirement excludes this cohort.
    assert cohorts.loc[cohorts.vintage_year.le(2014), "capacity"].eq(0).all()
    assert cohorts.loc[cohorts.vintage_year.eq(2015), "capacity"].gt(0).all()
    assert set(capacity.loc[capacity.capacity.gt(0), "vintage"]) == {2015, 2023}
    assert capacity.groupby("road_class").capacity.sum().tolist() == pytest.approx([100] * 3)


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
        lifetimes=lifetimes, regions=["ON"],
        period_config=ScenarioPeriods(
            period_mode="legacy", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020, 2023], model=[2025], step=5,
        ),
        vehicle_population_year=2025,
        road_rules=road_rules, rules=rules,
    )
    assert set(transfers.from_tech) == {school["fuel_technology"]["cng"]}
    assert set(transfers.to_tech) == {
        school["fuel_technology"]["gasoline"],
        school["fuel_technology"]["diesel"],
    }
    assert transfers.capacity_k_vehicles.sum() > 0
    assert capacity.loc[capacity.road_class.eq("school_buses"), "capacity"].sum() == pytest.approx(100)
    assert cohorts.loc[cohorts.tech.eq(school["fuel_technology"]["cng"]), "capacity"].sum() == 0


def test_prospective_bus_cohorts_also_survive_on_the_model_vintage() -> None:
    provincial, age, efficiency, lifetimes, road_rules, rules = _bus_inputs()
    age = pd.DataFrame([
        {"year": 2025, "VEHICLE_CLASS": "BUS", "AGE": value, "AGE_DIST": share}
        for value, share in ((0, 0.25), (10, 0.25), (30, 0.5))
    ])
    lifetimes = {key: 13.0 for key in lifetimes}
    cohorts, _, _, _, capacity = distribute_existing_bus_capacity(
        provincial=provincial, report5_age=age, annual_efficiency=efficiency,
        lifetimes=lifetimes, regions=["ON"],
        period_config=ScenarioPeriods(period_mode="prospective", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020], model=[2025], step=5),
        vehicle_population_year=2025, road_rules=road_rules, rules=rules,
    )
    ten_year_old = cohorts.loc[cohorts.age.eq(10)]
    assert ten_year_old.annual_cohort_eligible.all()
    assert not ten_year_old.model_vintage_eligible.any()
    assert ten_year_old.capacity.eq(0).all()
    assert set(capacity.loc[capacity.capacity.gt(0), "vintage"]) == {2020}
    assert capacity.groupby("road_class").capacity.sum().to_dict() == pytest.approx(
        {name: 100.0 for name in rules["classes"]}
    )


def test_bus_capacity_has_efficiency_and_lifetime_in_standalone_sqlite(
    isolated_legacy_bundle, tmp_path: Path,
) -> None:
    # The fixture owns a complete private artifact namespace, not only SQLite.
    # Ontario retains all three bus classes while keeping this support check bounded.
    scenario = isolated_legacy_bundle.scenario.model_copy(update={
        "geography": isolated_legacy_bundle.scenario.geography.model_copy(update={"regions": ["ON"]}),
    })
    bundle = replace(isolated_legacy_bundle, scenario=scenario)
    bundle.scenario_path.write_text(yaml.safe_dump(scenario.model_dump(mode="json", by_alias=True)), encoding="utf-8")
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
        assert rows > 0 and technologies == 9 and regions == 1
        missing = connection.execute(
            f"SELECT COUNT(*) FROM existing_capacity x WHERE {bus} AND "
            "(NOT EXISTS (SELECT 1 FROM efficiency e WHERE e.region=x.region "
            "AND e.tech=x.tech AND e.vintage=x.vintage) OR "
            "NOT EXISTS (SELECT 1 FROM lifetime_tech l WHERE l.region=x.region "
            "AND l.tech=x.tech))"
        ).fetchone()[0]
        assert missing == 0
