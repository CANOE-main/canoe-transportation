"""Hand-solvable annual energy/capacity screens independent of production formulas."""

import builtins
from dataclasses import replace
import json
import sqlite3

import pytest
import yaml

from utils import (file_sha256, harmonization_rule_paths, load_config_bundle,
                   load_harmonization_rules)
from validation import charger_feasibility as diagnostic
from validation.schema_contract import create_v4_schema


def insert(connection, table, **values):
    columns = ",".join(values)
    connection.execute(f"INSERT INTO {table} ({columns}) VALUES ({','.join('?' for _ in values)})", tuple(values.values()))


def publication(database, bundle):
    payload = {"ok": True, "database_sha256": file_sha256(database), "config": {
        key: {"sha256": file_sha256(path)} for key, path in {
            "scenario": bundle.scenario_path, "paths": bundle.paths_path, "sources": bundle.sources_path}.items()}}
    # A captured Windows path remains metadata when replayed on another host.
    payload["config"]["parameters"] = [
        {"path": rf"C:\captured\config\parameters\{path.name}", "sha256": file_sha256(path)}
        for path in harmonization_rule_paths(bundle)
    ]
    with open(bundle.scenario.outputs.validation_report, "w", encoding="utf-8") as stream:
        json.dump(payload, stream)


@pytest.fixture
def fixture(tmp_path, request):
    source = load_config_bundle("config/scenarios/default.yaml")
    payload = source.scenario.model_dump(mode="json", by_alias=True)
    payload["scenario"]["name"] = "probe"
    payload["geography"]["regions"] = ["ON"]
    mode = getattr(request, "param", "prospective")
    payload["periods"].update(
        period_mode=mode, existing=[2020], model=[2025], base_year=2020 if mode == "legacy" else 2025, step=5,
    )
    payload["ev_chargers"]["charger_type_classes"] = "explicit"
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    payload["outputs"]["validation_report"] = str(tmp_path / "publication.json")
    scenario = tmp_path / "probe.yaml"
    scenario.write_text(yaml.safe_dump(payload), encoding="utf-8")
    bundle = load_config_bundle(scenario, repo_root=source.repo_root)
    routes = dict(bundle.paths.artifacts)
    routes["charger_feasibility_diagnostic"] = routes["charger_feasibility_diagnostic"].model_copy(update={"path": str(tmp_path / "report")})
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}))
    settings = load_harmonization_rules(bundle, "ev_chargers")["production_representation"]
    a, g = settings["archetypes"], settings["groups"]
    road_series = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["activity_series"]
    car_service = road_series["cars"]["commodity"]
    database = tmp_path / "fixture.sqlite"
    with sqlite3.connect(database) as connection:
        create_v4_schema(connection)
        connection.execute("PRAGMA foreign_keys=OFF")
        for year, flag in [(2020, "e"), (2025, "f"), (2030, "f")]:
            insert(connection, "time_period", period=year, flag=flag)
        for name, flag in [("grid", "p"), ("fuel", "p"), ("ldv", "a"), ("mhdv", "a")]:
            insert(connection, "commodity", name=name, flag=flag, units="PJ")
        for series in road_series.values():
            commodity, units = series["commodity"], series["units"]
            insert(connection, "commodity", name=commodity, flag="d", units=units)
            insert(connection, "demand", region="ON", period=2025, commodity=commodity,
                   demand=10. if commodity == car_service else 0., units=units, data_id="fixture")
        for tech, category in [("old_bev", "cars"), ("new_bev", "cars"), ("new_ice", "cars"),
                               *((tech, "charger") for tech in a.values())]:
            insert(connection, "technology", tech=tech, flag="p", category=category, annual=1)
            insert(connection, "lifetime_tech", region="ON", tech=tech, lifetime=15, data_id="fixture")
            insert(connection, "capacity_to_activity", region="ON", tech=tech,
                   c2a=31.536 if category == "charger" else 1., units="PJ/GW" if category == "charger" else "billion passenger-km/k vehicles", data_id="fixture")
        insert(connection, "existing_capacity", region="ON", tech="old_bev", vintage=2020, capacity=1., data_id="fixture")
        for tech, vintage, input_comm in [("old_bev", 2020, "ldv"), ("new_bev", 2025, "ldv"), ("new_ice", 2025, "fuel")]:
            insert(connection, "efficiency", region="ON", input_comm=input_comm, tech=tech,
                   vintage=vintage, output_comm=car_service, efficiency=1., data_id="fixture")
        insert(connection, "limit_annual_capacity_factor", region="ON", tech_or_group="old_bev", vintage=2020,
               output_comm=car_service, operator="e", factor=1., data_id="fixture")
        for key, tech in a.items():
            output = "ldv" if key.startswith("ldv") else "mhdv"
            for vintage in [2020, 2025]:
                insert(connection, "efficiency", region="ON", input_comm="grid", tech=tech, vintage=vintage,
                       output_comm=output, efficiency=1., data_id="fixture")
                insert(connection, "limit_annual_capacity_factor", region="ON", tech_or_group=tech, vintage=vintage,
                       output_comm=output, operator="e", factor=.1 if key.startswith("ldv_public") else .2, data_id="fixture")
        insert(connection, "existing_capacity", region="ON", tech=a["ldv_public_l2"], vintage=2020, capacity=1., units="GW", data_id="fixture")
        groups = {**{tech: [tech] for tech in a.values()}, g["ldv_public"]: [a["ldv_public_l2"], a["ldv_public_fast"]],
                  g["ldv_all"]: [a["ldv_public_l2"], a["ldv_public_fast"], a["ldv_private"]],
                  g["mhdv_all"]: [a["mhdv_overnight"], a["mhdv_fast"]]}
        for name, members in groups.items():
            for tech in members:
                insert(connection, "tech_group_member", group_name=name, tech=tech, data_id="fixture")
        for sub, parent, share in [(a["ldv_public_l2"], g["ldv_public"], 1.), (a["ldv_public_fast"], g["ldv_public"], 0.),
                                   (g["ldv_public"], g["ldv_all"], .5), (a["ldv_private"], g["ldv_all"], .5),
                                   (a["mhdv_overnight"], g["mhdv_all"], .5), (a["mhdv_fast"], g["mhdv_all"], .5)]:
            insert(connection, "limit_capacity_share", region="ON", period=2025, sub_group=sub,
                   super_group=parent, operator="e", share=share, data_id="fixture")
    publication(database, bundle)
    return database, bundle


def mutate(fixture, sql):
    database, bundle = fixture
    with sqlite3.connect(database) as connection:
        connection.executescript(sql)
    publication(database, bundle)


@pytest.mark.parametrize("fixture", ["legacy", "prospective"], indirect=True)
def test_hand_solved_mix_bound_and_readonly_report(fixture, tmp_path):
    database, bundle = fixture
    before = file_sha256(database)
    report = diagnostic.build_diagnostic(bundle, database)
    assert report["ok"] and report["full_model_feasibility"] == "not_assessed"
    bounds = report["results"][0]["bounds"]["ldv"]
    assert bounds["existing_fixed_throughput_PJ"] == pytest.approx(31.536 * .1)
    assert bounds["minimum_charger_throughput_with_mix_PJ"] == pytest.approx(31.536 * (.1 + .2))
    assert bounds["minimum_vehicle_electricity_PJ"] == 1
    assert bounds["maximum_vehicle_electricity_PJ"] == 10
    assert bounds["uptake_coupling_lower_bound_PJ"] == pytest.approx(8.4608)
    assert not (tmp_path / "report").exists()
    diagnostic.build_diagnostic(bundle, database, write=True)
    saved = json.loads((tmp_path / "report/probe_probe.json").read_text(encoding="utf-8"))
    assert saved == report
    assert file_sha256(database) == before


def test_annual_relaxation_detects_demand_insufficient_for_mandatory_charging(fixture):
    mutate(fixture, "UPDATE demand SET demand=5 WHERE commodity='T_D_pkm_ldv_c'")
    report = diagnostic.screen_database(*fixture)
    assert not report["ok"]
    assert not report["results"][0]["annual_relaxation_feasible"]
    assert report["results"][0]["bounds"]["ldv"]["maximum_vehicle_electricity_PJ"] == 5


def test_survival_is_mean_of_annual_fractions_over_the_actual_interval(fixture):
    database, bundle = fixture
    with sqlite3.connect(database) as connection:
        for year, value in [(year, 1.) for year in range(2020, 2025)] + [(2025, 1.), (2026, .8), (2027, .6), (2028, .4), (2029, .2), (2030, 0.)]:
            insert(connection, "lifetime_survival_curve", region="ON", tech="old_bev", vintage=2020,
                   period=year, fraction=value, data_id="fixture")
    publication(database, bundle)
    report = diagnostic.screen_database(database, bundle)
    assert report["results"][0]["bounds"]["ldv"]["minimum_vehicle_electricity_PJ"] == pytest.approx(.6)


@pytest.mark.parametrize("sql,match", [
    ("UPDATE technology SET retire=1 WHERE tech='old_bev'", "retirement"),
    ("UPDATE technology SET unlim_cap=1 WHERE tech='old_bev'", "unlimited capacity"),
    ("DELETE FROM lifetime_tech WHERE tech='old_bev'", "physical lifetime"),
    ("DELETE FROM capacity_to_activity WHERE tech='old_bev'", "capacity_to_activity coverage"),
    ("UPDATE existing_capacity SET units='MW' WHERE tech LIKE '%CHRG%'", "GW capacities"),
    ("UPDATE commodity SET flag='w' WHERE name='ldv'", "non-waste"),
    ("UPDATE limit_annual_capacity_factor SET operator='ge' WHERE tech_or_group LIKE '%CHRG%'", "equality charger"),
    ("UPDATE time_period SET period=2035 WHERE period=2030", "period axes"),
    ("DELETE FROM demand WHERE commodity='T_D_pkm_ldv_m'", "configured road demand"),
    ("INSERT INTO demand(region,period,commodity,demand,units,data_id) SELECT region,period,commodity,demand,units,'duplicate' FROM demand WHERE commodity='T_D_pkm_ldv_c'", "configured road demand"),
    ("UPDATE demand SET units='PJ' WHERE commodity='T_D_pkm_ldv_c'", "Road demand units"),
    ("UPDATE demand SET demand=1 WHERE commodity='T_D_pkm_ldv_m'", "Positive road demand lacks active routes"),
    ("DELETE FROM limit_capacity_share WHERE share=0", "all six"),
    ("INSERT INTO commodity(name,flag) VALUES ('unknown','a'); INSERT INTO technology(tech,flag,annual) VALUES ('unknown','p',1); INSERT INTO efficiency(region,input_comm,tech,vintage,output_comm,efficiency,data_id) VALUES ('ON','ldv','unknown',2025,'unknown',1,'fixture')", "PHEV route"),
    ("INSERT INTO efficiency_variable(region,season,tod,input_comm,tech,vintage,output_comm,efficiency,data_id) VALUES ('ON','summer','h1','ldv','old_bev',2020,'T_D_pkm_ldv_c',2,'fixture')", "static efficiencies"),
])
def test_unsupported_physical_or_scenario_contracts_fail_closed(fixture, sql, match):
    mutate(fixture, sql)
    with pytest.raises(ValueError, match=match):
        diagnostic.screen_database(*fixture)


def test_phev_blends_motorcycles_and_buses_are_included(fixture):
    database, bundle = fixture
    road_series = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["activity_series"]
    with sqlite3.connect(database) as connection:
        insert(connection, "commodity", name="blend", flag="a")
        insert(connection, "technology", tech="blender", flag="p", annual=1)
        for fuel, share in [("ldv", .4), ("fuel", .6)]:
            insert(connection, "efficiency", region="ON", input_comm=fuel, tech="blender", vintage=2025,
                   output_comm="blend", efficiency=1., data_id="fixture")
            insert(connection, "limit_tech_input_split_annual", region="ON", period=2025, input_comm=fuel,
                   tech="blender", operator="e", proportion=share, data_id="fixture")
        connection.execute("UPDATE efficiency SET input_comm='blend',efficiency=2 WHERE tech='old_bev'")
        for tech, category, energy, capacity, efficiency in [
            ("motorcycle", "motorcycles", "ldv", .5, 1.),
            ("bus", "urban_transit", "mhdv", 2., 2.),
        ]:
            output = road_series[category]["commodity"]
            insert(connection, "technology", tech=tech, flag="p", category=category, annual=1)
            insert(connection, "lifetime_tech", region="ON", tech=tech, lifetime=15, data_id="fixture")
            insert(connection, "capacity_to_activity", region="ON", tech=tech, c2a=1., data_id="fixture")
            insert(connection, "existing_capacity", region="ON", tech=tech, vintage=2020, capacity=capacity, data_id="fixture")
            insert(connection, "efficiency", region="ON", input_comm=energy, tech=tech, vintage=2020,
                   output_comm=output, efficiency=efficiency, data_id="fixture")
            insert(connection, "limit_annual_capacity_factor", region="ON", tech_or_group=tech, vintage=2020,
                   output_comm=output, operator="e", factor=1., data_id="fixture")
            connection.execute("UPDATE demand SET demand=? WHERE commodity=?", (capacity, output))
    publication(database, bundle)
    report = diagnostic.screen_database(database, bundle)["results"][0]
    assert report["bounds"]["ldv"]["minimum_vehicle_electricity_PJ"] == pytest.approx(.2 + .5)
    assert report["bounds"]["mhdv"]["minimum_vehicle_electricity_PJ"] == 1
    assert {"cars", "motorcycles", "urban_transit"} <= set(report["road_classes_included"])
    mutate(fixture, "UPDATE efficiency SET efficiency=.9 WHERE tech='blender'")
    with pytest.raises(ValueError, match="nonunity"):
        diagnostic.screen_database(database, bundle)


def test_changed_database_or_config_is_not_relabelled_as_current(fixture):
    database, bundle = fixture
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE demand SET demand=11")
    with pytest.raises(ValueError, match="matching successful native publication"):
        diagnostic.screen_database(database, bundle)
    publication(database, bundle)
    payload = yaml.safe_load(bundle.scenario_path.read_text(encoding="utf-8"))
    payload["ev_chargers"]["ld_home_utilization"] = .12
    bundle.scenario_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    changed = load_config_bundle(bundle.scenario_path, repo_root=bundle.repo_root)
    with pytest.raises(ValueError, match="configuration differs"):
        diagnostic.screen_database(database, changed)


def test_scipy_is_optional_and_missing_dependency_is_actionable(fixture, monkeypatch):
    original = builtins.__import__

    def without_scipy(name, *args, **kwargs):
        if name.startswith("scipy"):
            raise ImportError("not installed")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_scipy)
    with pytest.raises(RuntimeError, match="uv sync --group dev"):
        diagnostic.screen_database(*fixture)


@pytest.mark.parametrize("fault", ["missing", "duplicate", "changed"])
def test_published_rules_require_every_partition_exactly_once(fixture, fault):
    database, bundle = fixture
    path = bundle.scenario.outputs.validation_report
    with open(path, encoding="utf-8") as stream:
        report = json.load(stream)
    records = report["config"]["parameters"]
    selected = next(row for row in records if row["path"].endswith("charging_and_range_representation.yaml"))
    if fault == "missing":
        records.remove(selected)
    elif fault == "duplicate":
        records.append(dict(selected))
    else:
        selected["sha256"] = "0" * 64
    with open(path, "w", encoding="utf-8") as stream:
        json.dump(report, stream)

    with pytest.raises(ValueError, match="publication rules differ"):
        diagnostic.screen_database(database, bundle)


@pytest.mark.parametrize("member", ["rules.yaml", "charging_and_range_representation.yaml"])
def test_controls_changed_while_solving_cannot_publish_a_current_report(fixture, monkeypatch, member):
    original_solve, original_hash = diagnostic._LinearSystem.solve, diagnostic.file_sha256
    rules = next(path for path in harmonization_rule_paths(fixture[1]) if path.name == member)
    changed = False

    def solve(system, objective):
        nonlocal changed
        changed = True
        return original_solve(system, objective)

    monkeypatch.setattr(diagnostic._LinearSystem, "solve", solve)
    monkeypatch.setattr(diagnostic, "file_sha256", lambda path: "changed-during-solve"
                        if changed and path == rules else original_hash(path))
    with pytest.raises(ValueError, match="controls changed during"):
        diagnostic.build_diagnostic(fixture[1], fixture[0], write=True)
    assert not (fixture[0].parent / "report").exists()
