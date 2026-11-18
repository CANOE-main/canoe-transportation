"""Opt-in necessary annual charging/transport feasibility screen, without a Temoa solve."""

from collections import defaultdict
from contextlib import closing
import argparse
import json
from pathlib import Path
import sqlite3

import numpy as np

from parameterization.charger_representation import ChargerRepresentationSettings
from utils import (ConfigBundle, file_sha256, harmonization_rule_fingerprints, harmonization_rule_paths,
                   load_config_bundle, load_harmonization_rules,
                   load_yaml, resolve_artifact_path, resolve_configured_path,
                   resolve_repo_path, write_text_atomic)
from validation.config_models import ScenarioConfig
from validation.sqlite_compare import validate_scenario_comparison_reference
from validation.sqlite_utils import open_sqlite_readonly, quote_identifier


TABLES = ("technology", "commodity", "efficiency", "existing_capacity", "capacity_to_activity",
          "lifetime_tech", "lifetime_process", "lifetime_survival_curve", "time_period", "demand",
          "limit_annual_capacity_factor", "limit_capacity_share", "tech_group_member",
          "limit_tech_input_split_annual", "limit_tech_input_split", "efficiency_variable")
LIMITATIONS = [
    "A feasible annual relaxation does not prove full Temoa or CANOE-main feasibility.",
    "Hourly dispatch, upstream electricity/fuel availability, costs, range/new-capacity shares, growth and emissions are omitted.",
    "Each operating period is screened independently; earlier new builds are not required to persist into later periods.",
    "Only explicit chargers with disabled hourly profiles, equality charger utilization and non-retirable physical equipment are supported.",
    "PHEV routing requires current-period unity-efficiency blenders with exact input fractions; unfamiliar routes are rejected.",
    "Time-varying efficiencies and unlimited-capacity physical processes are unsupported and rejected.",
]


class _LinearSystem:
    def __init__(self, bounds, solver):
        self.bounds, self.solver = bounds, solver
        self.eq, self.beq, self.ub, self.bub = [], [], [], []

    def add(self, coefficients, operator, value):
        vector = np.zeros(len(self.bounds))
        for index, coefficient in coefficients.items():
            vector[index] += coefficient
        if operator == "e":
            self.eq.append(vector)
            self.beq.append(value)
        elif operator in {"ge", "le"}:
            sign = -1 if operator == "ge" else 1
            self.ub.append(sign * vector)
            self.bub.append(sign * value)
        else:
            raise ValueError(f"Unsupported annual operator: {operator}")

    def solve(self, objective):
        result = self.solver(objective, A_eq=self.eq or None, b_eq=self.beq or None,
                             A_ub=self.ub or None, b_ub=self.bub or None,
                             bounds=self.bounds, method="highs")
        if result.status not in {0, 2}:
            raise ValueError(f"Annual diagnostic was not resolved: {result.message}")
        return result


class _Inputs:
    def __init__(self, tables, bundle):
        self.tables = tables
        self.periods = bundle.scenario.periods
        self.existing_years = set(self.periods.existing)
        road_series = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["activity_series"]
        self.classes = set(road_series)
        self.demand_units = {series["commodity"]: series["units"] for series in road_series.values()}
        self.technology = {row["tech"]: row for row in tables["technology"]}
        self.c2a = {(row["region"], row["tech"]): row["c2a"] for row in tables["capacity_to_activity"]}
        self.capacity = {(row["region"], row["tech"], row["vintage"]): row["capacity"] for row in tables["existing_capacity"]}
        self.life = {(row["region"], row["tech"], row["vintage"]): row["lifetime"] for row in tables["lifetime_process"]}
        self.tech_life = {(row["region"], row["tech"]): row["lifetime"] for row in tables["lifetime_tech"]}
        self.curves, self.groups = defaultdict(dict), defaultdict(set)
        for row in tables["lifetime_survival_curve"]:
            self.curves[row["region"], row["tech"], row["vintage"]][row["period"]] = row["fraction"]
        for row in tables["tech_group_member"]:
            self.groups[row["group_name"]].add(row["tech"])

    def fraction(self, region, period, tech, vintage):
        if vintage > period:
            return 0.
        if any(self.technology[tech][key] for key in ("retire", "curtail", "flex", "unlim_cap")):
            raise ValueError(f"Unsupported retirement/curtailment/flexible/unlimited capacity physical process: {tech}")
        key = region, tech, vintage
        lifetime = self.life.get(key, self.tech_life.get((region, tech)))
        if lifetime is None or not np.isfinite(lifetime) or lifetime <= 0:
            raise ValueError(f"Missing or invalid emitted physical lifetime: {key}")
        period_end = self.periods.period_end(period)
        duration = period_end - period
        if key in self.curves:
            values = self.curves[key]
            if any(not np.isfinite(value) or not 0 <= value <= 1 for value in values.values()):
                raise ValueError(f"Invalid physical survival fractions: {key}")
            years = range(period, period_end)
            if any(year not in values and year < max(values) for year in years):
                raise ValueError(f"Incomplete annual physical survival evidence: {key}")
            if values[max(values)] != 0:
                raise ValueError(f"Physical survival evidence needs a zero endpoint: {key}")
            return sum(values.get(year, 0.) for year in years) / duration
        return max(0., min(1., (vintage + lifetime - period) / duration))


def _snapshot(database, bundle):
    if bundle.scenario.ev_chargers.charger_type_classes != "explicit" or bundle.scenario.charging_profiles.travel_behavior_source != "none":
        raise ValueError("Annual charger screening requires an explicit scenario with travel_behavior_source: none")
    if ScenarioConfig.model_validate(load_yaml(bundle.scenario_path)) != bundle.scenario:
        raise ValueError("Scenario bundle differs from its versioned controls; materialize the actual scenario first")
    controls = {"scenario": file_sha256(bundle.scenario_path), "paths": file_sha256(bundle.paths_path),
                "sources": file_sha256(bundle.sources_path)}
    rules = harmonization_rule_fingerprints(bundle)
    digest = file_sha256(database)
    publication_path = resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)
    publication = load_yaml(publication_path)
    if not publication.get("ok") or publication.get("database_sha256") != digest:
        raise ValueError("Database lacks a matching successful native publication report; rebuild the scenario")
    config = publication.get("config", {})
    if any(config.get(key, {}).get("sha256") != value for key, value in controls.items()):
        raise ValueError("Native publication configuration differs from current controls; rebuild before screening")
    for name, digest_rules in rules.items():
        parameters = [row for row in config.get("parameters", [])
                      if str(row["path"]).replace("\\", "/").rsplit("/", 1)[-1] == name]
        if len(parameters) != 1 or parameters[0]["sha256"] != digest_rules:
            raise ValueError("Native publication rules differ from current rules; rebuild before screening")
    validate_scenario_comparison_reference(database)
    with closing(open_sqlite_readonly(database)) as connection:
        connection.row_factory = sqlite3.Row
        tables = {name: [dict(row) for row in connection.execute(f"SELECT * FROM {quote_identifier(name)}")]
                  for name in TABLES}
    expected = {(year, "e") for year in bundle.scenario.periods.existing} | {
        (year, "f") for year in [*bundle.scenario.periods.model, bundle.scenario.periods.end_of_horizon]}
    if {(row["period"], row["flag"]) for row in tables["time_period"]} != expected:
        raise ValueError("Database period axes differ from the actual scenario")
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["region_output_map"]
    regions = {region_map.get(region, region) for region in bundle.scenario.geography.regions}
    if {row["region"] for row in tables["demand"]} != regions:
        raise ValueError("Database demand regions differ from the actual scenario")
    settings = ChargerRepresentationSettings.model_validate(load_harmonization_rules(bundle, "ev_chargers")["production_representation"])
    if {row["tech"] for row in tables["technology"] if row["category"] == "charger"} != set(settings.archetypes.model_dump().values()):
        raise ValueError("Database charger representation differs from configured explicit archetypes")
    return tables, {**controls, **{f"rules:{name}": digest_rules for name, digest_rules in rules.items()}, "database": digest,
                    "publication": file_sha256(publication_path)}, sorted(regions)


def _electricity_coefficients(data, region, period, charged, roads):
    coefficients = {commodity: {commodity: 1.} for commodity in charged}
    efficiencies = [row for row in data.tables["efficiency"] if row["region"] == region and row["vintage"] == period]
    splits = [row for name in ("limit_tech_input_split_annual", "limit_tech_input_split")
              for row in data.tables[name] if row["region"] == region and row["period"] == period]
    blends = []
    for row in efficiencies:
        if row["input_comm"] not in charged or row["output_comm"] in {item["output_comm"] for item in roads}:
            continue
        peers = [item for item in data.tables["efficiency"] if item["region"] == region
                 and item["vintage"] <= period and item["tech"] == row["tech"]]
        inputs = {item["input_comm"] for item in peers}
        selected = [item for item in splits if item["tech"] == row["tech"]]
        if (any(item["efficiency"] != 1 for item in peers) or len({item["output_comm"] for item in peers}) != 1
                or len(selected) != len(inputs) or {item["input_comm"] for item in selected} != inputs
                or any(item["operator"] != "e" or not 0 <= item["proportion"] <= 1 for item in selected)
                or not np.isclose(sum(item["proportion"] for item in selected), 1, rtol=0, atol=1e-12)):
            raise ValueError(f"Unsupported nonunity, nonexact or incomplete PHEV route: {row['tech']}")
        ratio = next(item["proportion"] for item in selected if item["input_comm"] == row["input_comm"])
        if row["input_comm"] in coefficients.get(row["output_comm"], {}) and coefficients[row["output_comm"]][row["input_comm"]] != ratio:
            raise ValueError("Conflicting PHEV producer ratios")
        coefficients.setdefault(row["output_comm"], {})[row["input_comm"]] = ratio
        blends.append({"tech": row["tech"], "output": row["output_comm"], "input": row["input_comm"], "fraction": ratio})
    consumers = [row for row in data.tables["efficiency"] if row["region"] == region and row["vintage"] <= period
                 and row["input_comm"] in coefficients]
    if any(row["output_comm"] not in {item["output_comm"] for item in roads}
           and (row["output_comm"] not in coefficients or row["tech"] not in {item["tech"] for item in blends})
           for row in consumers):
        raise ValueError("Unhandled charged-electricity consumer or intermediate route")
    return coefficients, blends


def _screen_period(data, region, period, solver):
    demand_rows = [row for row in data.tables["demand"] if row["region"] == region
                   and row["period"] == period and row["commodity"] in data.demand_units]
    demands = {row["commodity"]: row["demand"] for row in demand_rows}
    if demands.keys() != data.demand_units.keys() or len(demand_rows) != len(demands):
        raise ValueError(f"Missing or duplicate configured road demand: {region}/{period}")
    if any(row["units"] != data.demand_units[row["commodity"]] for row in demand_rows):
        raise ValueError(f"Road demand units differ from configured services: {region}/{period}")
    physical = [row for row in data.tables["efficiency"] if row["region"] == region
                and data.technology[row["tech"]]["category"] in data.classes | {"charger"}
                and data.fraction(region, period, row["tech"], row["vintage"]) > 0]
    roads = [row for row in physical if row["output_comm"] in demands]
    road_outputs = {row["output_comm"] for row in roads}
    if missing := {service for service, demand in demands.items() if demand > 0} - road_outputs:
        raise ValueError(f"Positive road demand lacks active routes: {region}/{period}/{sorted(missing)}")
    chargers = sorted({(row["tech"], row["vintage"], row["output_comm"]) for row in physical
                       if data.technology[row["tech"]]["category"] == "charger"})
    charged = {key[2] for key in chargers}
    if len(charged) != 2 or any("w" in row["flag"] for row in data.tables["commodity"] if row["name"] in charged):
        raise ValueError("Screen requires two balanced, non-waste charging commodities")
    owners = {key[0] for key in chargers}
    if (any(row["units"] != "PJ" for row in data.tables["commodity"] if row["name"] in charged)
            or any(row["units"] != "PJ/GW" for row in data.tables["capacity_to_activity"] if row["tech"] in owners)
            or any(row["units"] != "GW" for row in data.tables["existing_capacity"] if row["tech"] in owners)):
        raise ValueError("Annual screen requires charger GW capacities, PJ/GW conversions and PJ electricity")
    coefficients, blends = _electricity_coefficients(data, region, period, charged, roads)
    nroad, factors, bounds = len(roads), {}, [(0., None)] * len(roads)
    for tech, vintage, output in chargers:
        capacity = data.capacity.get((region, tech, vintage), 0.) * data.fraction(region, period, tech, vintage)
        bounds.append((capacity, capacity) if vintage in data.existing_years else (0., None))
        rows = [row for row in data.tables["limit_annual_capacity_factor"] if (row["region"], row["tech_or_group"], row["vintage"], row["output_comm"]) == (region, tech, vintage, output)
                and row.get("period") in (None, period)]
        if len(rows) != 1 or rows[0]["operator"] != "e" or not 0 < rows[0]["factor"] < 1:
            raise ValueError("Screen requires one positive equality charger utilization per active process")
        factors[tech, vintage, output] = rows[0]["factor"] * data.c2a[region, tech]
    system = _LinearSystem(bounds, solver)
    for output in sorted(road_outputs):
        system.add({i: 1. for i, row in enumerate(roads) if row["output_comm"] == output}, "e", demands[output])
    for tech, vintage in sorted({(row["tech"], row["vintage"]) for row in roads if row["vintage"] in data.existing_years}):
        capacity = data.capacity.get((region, tech, vintage), 0.) * data.fraction(region, period, tech, vintage)
        factors_road = [row for row in data.tables["limit_annual_capacity_factor"] if (row["region"], row["tech_or_group"], row["vintage"]) == (region, tech, vintage) and row.get("period") in (None, period)]
        if not factors_road:
            raise ValueError(f"Missing existing-road activity factor: {tech}/{vintage}")
        for factor in factors_road:
            indices = {i: 1. for i, row in enumerate(roads) if (row["tech"], row["vintage"], row["output_comm"]) == (tech, vintage, factor["output_comm"])}
            if not indices or not np.isfinite(factor["factor"]) or not 0 <= factor["factor"] <= 1:
                raise ValueError("Road activity factor has no matching service or is invalid")
            system.add(indices, factor["operator"], factor["factor"] * capacity * data.c2a[region, tech])
    energy = {commodity: np.array([coefficients.get(row["input_comm"], {}).get(commodity, 0.) / row["efficiency"] for row in roads] + [0.] * len(chargers)) for commodity in charged}
    road_bounds = {commodity: (system.solve(vector), system.solve(-vector)) for commodity, vector in energy.items()}
    mix_rows = 0
    for row in data.tables["limit_capacity_share"]:
        if row["region"] == region and row["period"] == period:
            sub, sup = data.groups[row["sub_group"]], data.groups[row["super_group"]]
            owners = {key[0] for key in chargers}
            if not sub or not sup or ((sub | sup) & owners and not (sub | sup).issubset(owners)):
                raise ValueError("Empty or unhandled explicit capacity-share group")
            if (sub | sup).issubset(owners):
                if row["operator"] != "e" or not np.isfinite(row["share"]) or not 0 <= row["share"] <= 1:
                    raise ValueError("Explicit capacity mix requires finite exact shares")
                mix_rows += 1
                system.add({nroad + i: float(key[0] in sub) - row["share"] * float(key[0] in sup) for i, key in enumerate(chargers)}, row["operator"], 0.)
    if mix_rows != 6:
        raise ValueError("Screen requires all six explicit available-capacity mix constraints")
    report = {}
    for commodity, vector in energy.items():
        production = np.array([0.] * nroad + [factors[key] if key[2] == commodity else 0. for key in chargers])
        minimum = system.solve(production)
        low, high = road_bounds[commodity]
        report[commodity] = {"existing_fixed_throughput_PJ": sum(lower * production[i] for i, (lower, upper) in enumerate(bounds) if lower == upper),
                             "minimum_charger_throughput_with_mix_PJ": float(minimum.fun) if minimum.success else None,
                             "minimum_vehicle_electricity_PJ": float(low.fun) if low.success else None,
                             "maximum_vehicle_electricity_PJ": float(-high.fun) if high.success else None,
                             "uptake_coupling_lower_bound_PJ": max(0., float(minimum.fun - low.fun)) if minimum.success and low.success else None}
    for commodity, vector in energy.items():
        balance = {i: -value for i, value in enumerate(vector)}
        balance.update({nroad + i: factors[key] if key[2] == commodity else 0. for i, key in enumerate(chargers)})
        system.add(balance, "e", 0.)
    result = system.solve(np.zeros(len(bounds)))
    return {"region": region, "period": period, "road_flow_variables": nroad, "charger_capacity_variables": len(chargers),
            "equality_constraints": len(system.eq), "inequality_constraints": len(system.ub),
            "annual_relaxation_feasible": bool(result.success), "solver_status": result.message,
            "bounds": report, "road_classes_included": sorted({data.technology[row["tech"]]["category"] for row in roads}),
            "phev_blends_included": blends}


def screen_database(database: str | Path, bundle: ConfigBundle) -> dict:
    """Screen a matching published explicit database without changing any input."""
    try:
        from scipy import __version__ as scipy_version
        from scipy.optimize import linprog
    except ImportError as exc:
        raise RuntimeError("Annual charger screening needs development dependencies; run uv sync --group dev") from exc
    database = Path(database)
    tables, hashes, regions = _snapshot(database, bundle)
    if tables["efficiency_variable"]:
        raise ValueError("Annual screen requires static efficiencies; time-varying efficiencies are unsupported")
    data = _Inputs(tables, bundle)
    physical = {(row["region"], row["tech"]) for row in tables["efficiency"]
                if data.technology[row["tech"]]["category"] in data.classes | {"charger"}}
    if physical - data.c2a.keys():
        raise ValueError("Missing physical capacity_to_activity coverage")
    for name, value in [*(('efficiency', row['efficiency']) for row in tables['efficiency']),
                        *(('c2a', value) for value in data.c2a.values())]:
        if not np.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid physical {name}: {value}")
    if any(not np.isfinite(row["capacity"]) or row["capacity"] < 0 for row in tables["existing_capacity"]):
        raise ValueError("Existing capacities must be finite and nonnegative")
    if any(not np.isfinite(row["demand"]) or row["demand"] < 0 for row in tables["demand"]):
        raise ValueError("Demands must be finite and nonnegative")
    results = [_screen_period(data, region, period, linprog) for region in regions for period in bundle.scenario.periods.model]
    current_paths = {"database": database, "scenario": bundle.scenario_path, "paths": bundle.paths_path,
                     "sources": bundle.sources_path,
                     **{f"rules:{path.name}": path for path in harmonization_rule_paths(bundle)},
                     "publication": resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)}
    if any(file_sha256(path) != hashes[key] for key, path in current_paths.items()):
        raise ValueError("Database or controls changed during annual screening; rerun against a completed publication")
    return {"method": "annual-charger-transport-relaxation-v1", "ok": all(row["annual_relaxation_feasible"] for row in results),
            "solver": {"library": "scipy.optimize.linprog", "version": scipy_version, "method": "highs"},
            "full_model_feasibility": "not_assessed", "database": str(database.resolve()), "sha256": hashes,
            "scenario": bundle.scenario.model_dump(mode="json", by_alias=True), "row_counts": {name: len(rows) for name, rows in tables.items()},
            "equations": {"charger_output": "factor * available vintage capacity * capacity_to_activity",
                          "electricity_balance": "charger annual output = BEV/PHEV/motorcycle/bus annual charged-electricity input",
                          "mix": "available capacity of subgroup = source share * parent available capacity",
                          "physical_capacity": "existing capacity * mean annual survival over operating interval, or fixed-lifetime remaining fraction"},
            "limitations": LIMITATIONS, "results": results}


def build_diagnostic(bundle: ConfigBundle, database: str | Path | None = None, *, write: bool = False) -> dict:
    path = Path(database) if database is not None else resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    report = screen_database(path, bundle)
    if write:
        target = resolve_artifact_path(bundle, "charger_feasibility_diagnostic", f"{bundle.scenario.scenario.name}_{bundle.scenario_path.stem}.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        write_text_atomic(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", target)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--database")
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build_diagnostic(load_config_bundle(args.scenario), args.database, write=args.write_report), indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
