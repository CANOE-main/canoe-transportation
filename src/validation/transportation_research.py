"""Independent comparisons of production road parameters and legacy evidence."""

import argparse
import ast
from contextlib import closing
from dataclasses import dataclass, field
import hashlib
import json
import logging
from math import isclose, isfinite
from pathlib import Path
import subprocess
from typing import Any

import pandas as pd
from pydantic import Field

from parameterization.ceud_inputs import read_ceud_input
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames, resolved_road_lifetime_rules,
)
from utils import (
    ConfigBundle, file_sha256, load_config_bundle, load_harmonization_rules,
    resolve_artifact_path, resolve_configured_path, resolve_repo_path,
    write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import MappingModel
from validation.sqlite_utils import open_sqlite_readonly, quote_identifier


LOGGER = logging.getLogger(__name__)


class CeudSeries(MappingModel):
    table_id: int = Field(gt=0)
    raw_series: str = Field(min_length=1)


class TransportationResearchRules(MappingModel):
    reference_route: str
    report_route: str
    reference_sqlite: str
    reference_notebook: str
    legacy_baseline: str
    energy_series: dict[str, CeudSeries]
    notebook_scenario: str = Field(min_length=1)
    comparison_significance_percent: float = Field(ge=0, allow_inf_nan=False)


@dataclass(frozen=True)
class TransportationResearch:
    frames: dict[str, pd.DataFrame]
    metadata: dict[str, Any]
    destination: Path
    retained_exports: dict[str, dict[str, Any]] = field(default_factory=dict)


def _require_unique(frame: pd.DataFrame, keys: list[str], label: str) -> None:
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError(f"{label} has missing or duplicate keys")


def _ceud_value(frame: pd.DataFrame, region: str, year: int, selector: dict,
                unit: str) -> tuple[float, int, str]:
    selected = frame.loc[
        frame.region.eq(region) & frame.year.eq(year)
        & frame.table_id.eq(selector["table_id"])
        & frame.raw_series.eq(selector["raw_series"])
    ]
    if len(selected) != 1 or selected.iloc[0].unit != unit:
        raise ValueError(f"Missing, duplicate or wrong-unit CEUD report target: {region}/{year}/{selector}")
    row = selected.iloc[0]
    value = float(row.value)
    if not isfinite(value) or value < 0:
        raise ValueError("Invalid CEUD report target")
    return value, int(row.get("ceud_observed_year", year)), str(row.get("ceud_value_status", "observed"))


def fleet_activity_energy(
    capacity: pd.DataFrame, c2a: pd.DataFrame, factors: pd.DataFrame,
    efficiencies: pd.DataFrame, ceud: pd.DataFrame, *,
    base_year: int, regions: list[str], road_rules: dict, demand_rules: dict,
    energy_series: dict[str, dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate inserted stock at its class utilization; count blend energy once.

    This is a base-year accounting reconstruction using inserted cohort-average
    efficiencies. It is not dispatch, a future-period retirement calculation or
    independently observed consumption. PHEV blend-production edges are excluded
    by selecting only vehicle edges ending in the class demand commodity.
    """
    owners = {tech: name for name in energy_series
              for tech in road_rules["fuel_technology"][name].values()}
    selected = capacity.loc[capacity.tech.isin(owners)].copy()
    selected["road_class"] = selected.tech.map(owners)
    keys = ["region", "tech", "vintage"]
    _require_unique(selected, keys, "road existing capacity")
    if selected.empty or set(selected.units) != {"k vehicles"}:
        raise ValueError("Road report requires positive vehicle-capacity rows in k vehicles")
    selected["output_comm"] = selected.road_class.map(
        {name: demand_rules["activity_series"][name]["commodity"] for name in energy_series}
    )
    activity = c2a.loc[c2a.tech.isin(owners), ["region", "tech", "c2a"]]
    _require_unique(activity, ["region", "tech"], "road capacity-to-activity")
    factor = factors.loc[factors.tech_or_group.isin(owners) & factors.operator.eq("e")].rename(
        columns={"tech_or_group": "tech"},
    )
    _require_unique(factor, [*keys, "output_comm"], "road equality utilization")
    vehicle_efficiency = efficiencies.loc[
        efficiencies.tech.isin(owners)
        & efficiencies.output_comm.isin(selected.output_comm)
    ]
    _require_unique(vehicle_efficiency, [*keys, "output_comm"], "vehicle service efficiency")
    detail = selected.merge(activity, on=["region", "tech"], how="left", validate="many_to_one")
    detail = detail.merge(factor[[*keys, "output_comm", "factor"]],
                          on=[*keys, "output_comm"], how="left", validate="one_to_one")
    detail = detail.merge(vehicle_efficiency[[*keys, "output_comm", "input_comm", "efficiency"]],
                          on=[*keys, "output_comm"], how="left", validate="one_to_one")
    if detail[["capacity", "c2a", "factor", "efficiency"]].isna().any().any():
        raise ValueError("Road stock lacks a unique c2a, equality factor or service efficiency")
    if (detail.capacity.lt(0).any() or detail.c2a.le(0).any()
            or detail.efficiency.le(0).any() or not detail.factor.between(0, 1).all()
            or not detail[["capacity", "c2a", "factor", "efficiency"]].map(isfinite).all().all()):
        raise ValueError("Road accounting contains inadmissible physical values")
    detail["implied_activity_bn"] = detail.capacity * detail.c2a * detail.factor
    detail["implied_vehicle_input_pj"] = detail.implied_activity_bn / detail.efficiency
    records = []
    for native_region in regions:
        region = road_rules["region_output_map"].get(native_region, native_region)
        for road_class, energy_selector in energy_series.items():
            group = detail.loc[detail.region.eq(region) & detail.road_class.eq(road_class)]
            if group.empty:
                raise ValueError(f"Road accounting lacks {region}/{road_class}")
            stock, source_year, status = _ceud_value(
                ceud, native_region, base_year, road_rules["ceud_stock_series"][road_class], "thousands",
            )
            target_activity, activity_year, activity_status = _ceud_value(
                ceud, native_region, base_year, demand_rules["activity_series"][road_class], "millions",
            )
            target_activity *= float(demand_rules["activity_to_billion_factor"])
            energy, energy_year, energy_status = _ceud_value(
                ceud, native_region, base_year, energy_selector, "PJ",
            )
            represented_stock = float(group.capacity.sum())
            implied_activity = float(group.implied_activity_bn.sum())
            implied_energy = float(group.implied_vehicle_input_pj.sum())
            factors_used = sorted(set(group.factor))
            records.append({
                "region": region, "ceud_region": native_region, "road_class": road_class,
                "base_year": base_year, "ceud_stock_k_vehicles": stock,
                "inserted_stock_k_vehicles": represented_stock,
                "stock_minus_ceud_k_vehicles": represented_stock - stock,
                "stock_relative_difference": represented_stock / stock - 1 if stock else None,
                "ceud_stock_observed_year": source_year, "ceud_stock_status": status,
                "utilization": factors_used[0] if len(factors_used) == 1 else None,
                "utilization_values": "|".join(str(value) for value in factors_used),
                "ceud_activity_bn": target_activity, "implied_activity_bn": implied_activity,
                "activity_residual_bn": implied_activity - target_activity,
                "ceud_activity_observed_year": activity_year, "ceud_activity_status": activity_status,
                "ceud_secondary_energy_pj": energy, "implied_vehicle_input_pj": implied_energy,
                "energy_residual_pj": implied_energy - energy,
                "energy_relative_difference": implied_energy / energy - 1 if energy else None,
                "ceud_energy_observed_year": energy_year, "ceud_energy_status": energy_status,
                "energy_boundary": "vehicle input incl PHEV blend once; excludes upstream charger losses",
                "ceud_boundary_limit": "reported class secondary energy; unsupported fuels and fleet composition differ",
            })
    return detail[[*keys, "road_class", "output_comm", "input_comm", "capacity", "c2a",
                   "factor", "efficiency", "implied_activity_bn", "implied_vehicle_input_pj"]], pd.DataFrame(records)


def inspect_notebook(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Record executable source separately from saved outputs without executing it."""
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells, dependencies, fixed_switches = [], [], []
    write_calls = []
    stored_fixed_output = False
    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell.get("source", []))
        tree = ast.parse(source)
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        for call in calls:
            name = call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")
            if name == "read_excel" and call.args and isinstance(call.args[0], ast.Constant):
                dependency = str(call.args[0].value)
                dependencies.append({"cell_index": index, "path": dependency,
                                     "present_relative_to_notebook": (path.parent / dependency).is_file()})
            if name in {"to_sql", "executemany"}:
                write_calls.append({"cell_index": index, "call": name})
            for keyword in call.keywords:
                if keyword.arg == "fixed_lifetime" and isinstance(keyword.value, ast.Constant):
                    fixed_switches.append({"cell_index": index, "value": keyword.value.value})
                if keyword.arg == "canoe_default_all" and isinstance(keyword.value, ast.Constant):
                    dependency = str(keyword.value.value)
                    dependencies.append({"cell_index": index, "path": dependency,
                                         "present_relative_to_notebook": (path.parent / dependency).is_file()})
        output = "".join("".join(item.get("text", [])) for item in cell.get("outputs", []))
        stored_fixed_output |= "Assumed using fixed lifetime" in output
        if any(isinstance(node, ast.FunctionDef) for node in tree.body) or fixed_switches and fixed_switches[-1]["cell_index"] == index:
            cells.append({"cell_index": index, "locator_basis": "zero_based_ipynb_cells",
                          "source_sha256": hashlib.sha256(source.encode()).hexdigest(), "source": source})
    for dependency in dependencies:
        provided = path.parent / Path(dependency["path"]).name
        located = provided.is_file() and provided.suffix.lower() in {".xlsx", ".xls"}
        dependency["provided_comparison_evidence"] = str(provided) if located else None
        dependency["provided_sha256"] = file_sha256(provided) if located else None
    return pd.DataFrame(cells), {
        "path": str(path), "sha256": file_sha256(path), "dependencies": dependencies,
        "fixed_lifetime_calls": fixed_switches, "stored_output_mentions_fixed_lifetime": stored_fixed_output,
        "source_output_conflict": stored_fixed_output and any(item["value"] is False for item in fixed_switches),
        "sqlite_dataframe_write_calls": write_calls,
        "execution": "source inspected; supplied comparison workbooks identified independently of original notebook paths",
        "missing_dependency_count": sum(
            not item["present_relative_to_notebook"] and item["provided_comparison_evidence"] is None
            for item in dependencies
        ),
    }


def _legacy_annual_factors(connection) -> pd.DataFrame:
    """Normalize the original min/max tables and the newer operator table."""
    keys = ["region", "period", "tech", "output_comm", "operator", "factor"]
    names = {row[0].lower(): row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "limitannualcapacityfactor" in names:
        frame = pd.read_sql_query("SELECT * FROM LimitAnnualCapacityFactor", connection)
    else:
        members = []
        for name, operator in (("minannualcapacityfactor", "ge"), ("maxannualcapacityfactor", "le")):
            frame = pd.read_sql_query(f"SELECT * FROM {quote_identifier(names[name])}", connection)
            members.append(frame.assign(operator=operator))
        frame = pd.concat(members, ignore_index=True)
    if frame.empty:
        return pd.DataFrame(columns=keys)
    if missing := set(keys) - set(frame):
        raise ValueError(f"Legacy annual factors lack comparison keys: {sorted(missing)}")
    return frame[keys]


def compare_reference_state(reference: Path, baseline: Path, *, absolute_tolerance: float,
                            relative_tolerance: float) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compare the supplied legacy state to the prior legacy database on exact keys."""
    specs = {
        "ExistingCapacity": (["region", "tech", "vintage"], "capacity"),
        "Demand": (["region", "period", "commodity"], "demand"),
        "LimitAnnualCapacityFactor": (["region", "period", "tech", "output_comm", "operator"], "factor"),
        "LifetimeTech": (["region", "tech"], "lifetime"),
    }
    records = []
    with closing(open_sqlite_readonly(reference)) as current, closing(open_sqlite_readonly(baseline)) as prior:
        for table, (keys, value) in specs.items():
            frames = (
                [_legacy_annual_factors(connection) for connection in (current, prior)]
                if table == "LimitAnnualCapacityFactor" else
                [pd.read_sql_query(f"SELECT * FROM {quote_identifier(table)}", connection)
                 for connection in (current, prior)]
            )
            column = "commodity" if table == "Demand" else "tech"
            frames = [frame.loc[frame[column].str.startswith("T_", na=False)] for frame in frames]
            if table == "ExistingCapacity":
                frames = [frame.loc[~frame.tech.str.contains("CHRG", regex=False)] for frame in frames]
            for frame in frames:
                _require_unique(frame, keys, table)
            merged = frames[0][[*keys, value]].merge(
                frames[1][[*keys, value]], on=keys, how="outer", indicator=True,
                suffixes=("_reference", "_baseline"), validate="one_to_one",
            )
            common = merged.loc[merged._merge.eq("both")]
            matching = sum(isclose(float(row[0]), float(row[1]), abs_tol=absolute_tolerance,
                                   rel_tol=relative_tolerance)
                           for row in common[[value + "_reference", value + "_baseline"]].itertuples(index=False, name=None))
            records.append({"table": table, "scope": "non_charger_transport" if table == "ExistingCapacity" else "transport",
                            "common_keys": len(common), "matching_values": matching,
                            "different_values": len(common) - matching,
                            "reference_only_keys": int(merged._merge.eq("left_only").sum()),
                            "baseline_only_keys": int(merged._merge.eq("right_only").sum())})
        integrity = current.execute("PRAGMA integrity_check").fetchall()
        foreign_keys = current.execute("PRAGMA foreign_key_check").fetchall()
        survival_rows = current.execute("SELECT count(*) FROM LifetimeSurvivalCurve").fetchone()[0]
        factor_columns = [row[1] for row in current.execute("PRAGMA table_info(LimitAnnualCapacityFactor)")]
    return pd.DataFrame(records), {
        "reference": {"path": str(reference), "sha256": file_sha256(reference)},
        "legacy_baseline": {"path": str(baseline), "sha256": file_sha256(baseline)},
        "integrity": integrity, "foreign_key_violations": len(foreign_keys),
        "survival_rows": survival_rows, "annual_factor_columns": factor_columns,
        "absolute_tolerance": absolute_tolerance, "relative_tolerance": relative_tolerance,
        "interpretation": "supplied database state is compared separately from notebook intent",
    }


def _verify_build(bundle: ConfigBundle, database: Path, report: dict) -> None:
    if not report.get("ok") or report.get("scenario") != bundle.scenario.scenario.name:
        raise ValueError("Research report requires a successful matching scenario build")
    if report.get("database_sha256") != file_sha256(database):
        raise ValueError("Research database differs from its build fingerprint")
    expected = bundle.scenario.periods.model_dump(mode="json")
    if any(report.get("periods", {}).get(key) != value for key, value in expected.items()):
        raise ValueError("Research build has different period controls")
    configuration = report.get("config", {})
    for name, path in (("scenario", bundle.scenario_path), ("paths", bundle.paths_path),
                       ("sources", bundle.sources_path)):
        if configuration.get(name, {}).get("sha256") != file_sha256(path):
            raise ValueError(f"Research build has stale {name} configuration")
    for family in ("parameters", "manual_inputs"):
        evidence = configuration.get(family)
        if not isinstance(evidence, list) or not evidence:
            raise ValueError(f"Research build lacks {family} fingerprints")
        for item in evidence:
            path = resolve_repo_path(bundle.repo_root, item["path"])
            if not path.is_file() or file_sha256(path) != item["sha256"]:
                raise ValueError(f"Research input changed since build: {path}")


def _verify_accepted_survival(transformed: pd.DataFrame, capacity_audit: dict) -> str:
    """Bind the diagnostic schedules to the exact accepted build-time evidence."""
    expected = capacity_audit.get("accepted_survival_sha256")
    if not expected:
        raise ValueError("Research build lacks the accepted survival fingerprint")
    actual = hashlib.sha256(transformed.to_csv(index=False).encode("utf-8")).hexdigest()
    if actual != expected:
        raise ValueError("Research accepted survival evidence changed since build")
    return actual


def road_parameter_comparison(
    database: Path,
    reference: Path,
    baseline: Path,
    *,
    demand_rules: dict,
) -> dict[str, pd.DataFrame]:
    """Compare class/cohort values while retaining each database's original keys."""
    services = {
        spec["commodity"]: name
        for name, spec in demand_rules["activity_series"].items()
        if name
        in {
            "cars",
            "passenger_light_trucks",
            "freight_light_trucks",
            "medium_trucks",
            "heavy_trucks",
        }
    }
    compared = (("current", database), ("reference", reference), ("baseline", baseline))
    owner_frames = []
    for _, path in compared:
        with closing(open_sqlite_readonly(path)) as connection:
            efficiency = pd.read_sql_query("SELECT region, tech, output_comm FROM efficiency", connection)
        owner_frames.append(efficiency.loc[efficiency.output_comm.isin(services)])
    owners = pd.concat(owner_frames, ignore_index=True).drop_duplicates()
    owners["road_class"] = owners.output_comm.map(services)
    _require_unique(owners, ["region", "tech"], "shared road service owners")
    summaries: dict[str, list[pd.DataFrame]] = {
        name: [] for name in ("capacity", "utilization", "demand")
    }
    for label, path in compared:
        with closing(open_sqlite_readonly(path)) as connection:
            names = {
                row[0].lower(): row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }

            def read(name: str) -> pd.DataFrame:
                return pd.read_sql_query(
                    f"SELECT * FROM {quote_identifier(names[name.lower()])}", connection
                )

            capacity = read(
                "existing_capacity"
                if "existing_capacity" in names
                else "existingcapacity"
            )
            capacity = capacity.merge(
                owners[["region", "tech", "road_class"]],
                on=["region", "tech"],
                how="inner",
                validate="many_to_one",
            )
            if (
                not capacity.units.map(lambda value: str(value).strip("() "))
                .isin({"k vehicles", "k units"})
                .all()
            ):
                raise ValueError("Compared vehicle capacity must use thousand vehicles")
            capacity = (
                capacity.groupby(["region", "road_class", "vintage"], as_index=False)
                .capacity.sum()
                .rename(columns={"capacity": label})
            )
            summaries["capacity"].append(capacity)

            if (
                "limit_annual_capacity_factor" in names
                or "limitannualcapacityfactor" in names
            ):
                factor = read(
                    "limit_annual_capacity_factor"
                    if "limit_annual_capacity_factor" in names
                    else "limitannualcapacityfactor"
                )
            else:
                factor = pd.concat(
                    [read("minannualcapacityfactor"), read("maxannualcapacityfactor")],
                    ignore_index=True,
                )
            factor = factor.rename(columns={"tech_or_group": "tech"})
            factor = factor.loc[factor.output_comm.isin(services)].copy()
            factor["road_class"] = factor.output_comm.map(services)
            factor["technology_kind"] = factor.tech.map(
                lambda tech: (
                    "existing"
                    if tech.endswith("_EX")
                    else "new"
                    if tech.endswith("_N")
                    else "unsplit"
                )
            )
            bounds = (
                factor.groupby(["region", "road_class", "technology_kind"])
                .factor.agg(["min", "max"])
                .reset_index()
                .rename(columns={"min": f"{label}_minimum", "max": f"{label}_maximum"})
            )
            summaries["utilization"].append(bounds)

            demand = read("demand")
            demand = demand.loc[demand.commodity.isin(services)].copy()
            demand["road_class"] = demand.commodity.map(services)
            demand = (
                demand.groupby(["region", "road_class", "period"], as_index=False)
                .demand.sum()
                .rename(columns={"demand": label})
            )
            summaries["demand"].append(demand)
    keys = {
        "capacity": ["region", "road_class", "vintage"],
        "utilization": ["region", "road_class", "technology_kind"],
        "demand": ["region", "road_class", "period"],
    }
    result = {}
    for name, members in summaries.items():
        combined = members[0]
        for frame in members[1:]:
            combined = combined.merge(
                frame, on=keys[name], how="outer", validate="one_to_one"
            )
        if name != "utilization":
            combined["relative_percent"] = (combined.current - combined.reference).div(
                combined.reference.where(combined.reference.ne(0))
            ) * 100
        result[f"{name}_comparison"] = combined.sort_values(keys[name]).reset_index(
            drop=True
        )
    return result


def build_research_report(
    bundle: ConfigBundle,
    *,
    database: Path | None = None,
    build_report: Path | None = None,
) -> TransportationResearch:
    """Read a fresh native build and capture its audit evidence before another build."""
    rules = TransportationResearchRules.model_validate(load_harmonization_rules(bundle, "transportation_research"))
    database = database or resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    build_report = build_report or resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)
    report = json.loads(build_report.read_text(encoding="utf-8"))
    _verify_build(bundle, database, report)
    stock_rules = resolved_road_lifetime_rules(bundle)
    demand_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    energy_series = {name: selector.model_dump() for name, selector in rules.energy_series.items()}
    if not set(energy_series) <= set(stock_rules["ceud_sales_series"]):
        raise ValueError("Research classes lack configured sales series")
    frames: dict[str, pd.DataFrame] = {}
    inputs = {}
    annual = report.get("existing_capacity", {}).get("annual_stock_evidence")
    if not annual:
        raise ValueError("Research build lacks annual stock evidence fingerprint")
    evidence_records = {"road_fuel_shares": annual,
                        **report["existing_capacity"].get("research_evidence", {})}
    for name, item in evidence_records.items():
        path = resolve_repo_path(bundle.repo_root, item["path"])
        if not path.is_file() or file_sha256(path) != item["sha256"]:
            raise ValueError(f"Research audit is stale: {path}")
        frames[name] = pd.read_csv(path)
        inputs[name] = {"path": str(path), "sha256": item["sha256"]}
    ceud_rules = load_harmonization_rules(bundle, "ceud_inputs")
    manifest_path = resolve_artifact_path(bundle, "ceud_parameter_inputs", ceud_rules["manifest_file"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_evidence = report["existing_capacity"].get("ceud_parameter_evidence", {})
    if manifest_evidence.get("sha256") != file_sha256(manifest_path):
        raise ValueError("Research CEUD manifest differs from the build fingerprint")
    if (manifest["scenario"] != bundle.scenario.scenario.name
            or manifest["base_year"] != bundle.scenario.periods.base_year
            or manifest["policy"] != bundle.scenario.sources.nrcan_ceud_beyond_observed):
        raise ValueError("Research CEUD evidence belongs to a different scenario")
    for item in manifest["sources"]:
        for path_key, hash_key in (("native_path", "native_sha256"), ("output_path", "output_sha256")):
            path = resolve_repo_path(bundle.repo_root, item[path_key])
            if file_sha256(path) != item[hash_key]:
                raise ValueError(f"Research CEUD input changed: {path}")
    audit_path = resolve_repo_path(bundle.repo_root, manifest["audit_path"])
    if file_sha256(audit_path) != manifest["audit_sha256"]:
        raise ValueError("Research CEUD extension audit is stale")
    frames["ceud_extension_audit"] = pd.read_csv(audit_path)
    ceud = pd.concat([read_ceud_input(bundle, region) for region in bundle.scenario.geography.regions], ignore_index=True)
    with closing(open_sqlite_readonly(database)) as connection:
        tables = {name: pd.read_sql_query(f"SELECT * FROM {quote_identifier(name)}", connection)
                  for name in ("existing_capacity", "capacity_to_activity", "limit_annual_capacity_factor", "efficiency")}
    frames["fleet_cohort_accounting"], frames["stock_activity_energy"] = fleet_activity_energy(
        tables["existing_capacity"], tables["capacity_to_activity"], tables["limit_annual_capacity_factor"],
        tables["efficiency"], ceud, base_year=bundle.scenario.periods.base_year,
        regions=bundle.scenario.geography.regions, road_rules=stock_rules,
        demand_rules=demand_rules, energy_series=energy_series,
    )
    reference_dir = resolve_artifact_path(bundle, rules.reference_route)
    reference = reference_dir / rules.reference_sqlite
    reference_state, reference_metadata = compare_reference_state(
        reference, resolve_repo_path(bundle.repo_root, rules.legacy_baseline),
        absolute_tolerance=bundle.scenario.comparison.absolute_tolerance,
        relative_tolerance=bundle.scenario.comparison.relative_tolerance,
    )
    frames["reference_state"] = reference_state
    frames["notebook_source_cells"], notebook_metadata = inspect_notebook(reference_dir / rules.reference_notebook)
    frames.update(road_parameter_comparison(
        database, reference, resolve_repo_path(bundle.repo_root, rules.legacy_baseline),
        demand_rules=demand_rules,
    ))
    with closing(open_sqlite_readonly(reference)) as connection:
        legacy_stock = pd.read_sql_query("SELECT region, tech, vintage, capacity, units FROM ExistingCapacity", connection)
    owner = {tech: name for name in energy_series for tech in stock_rules["fuel_technology"][name].values()}
    legacy_stock = legacy_stock.loc[legacy_stock.tech.isin(owner)].copy()
    if not legacy_stock.units.map(lambda value: str(value).strip("() ")).isin({"k vehicles", "k units"}).all():
        raise ValueError("Legacy road capacity units are not thousand vehicles")
    legacy_stock["road_class"] = legacy_stock.tech.map(owner)
    legacy_totals = legacy_stock.groupby(["region", "road_class"], as_index=False).capacity.sum().rename(
        columns={"capacity": "supplied_legacy_stock_k_vehicles"},
    )
    frames["stock_activity_energy"] = frames["stock_activity_energy"].merge(
        legacy_totals, on=["region", "road_class"], how="left", validate="one_to_one",
    )
    frames["supplied_legacy_stock"] = legacy_stock
    lifetime_frames = derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
    )
    accepted_survival_sha256 = _verify_accepted_survival(
        lifetime_frames["transformed_curves"], report["existing_capacity"],
    )
    frames["configured_survival_curves"] = lifetime_frames["transformed_curves"]
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=bundle.repo_root, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    sales_rows = frames["stock_activity_energy"].loc[
        frames["stock_activity_energy"].ceud_region.map(bundle.scenario.existing_capacity.method_for).eq("sales_survival")
    ]
    closure = all(isclose(row.implied_activity_bn, row.ceud_activity_bn,
                         abs_tol=bundle.scenario.comparison.absolute_tolerance,
                         rel_tol=bundle.scenario.comparison.relative_tolerance)
                  for row in sales_rows.itertuples(index=False))
    if not closure:
        raise ValueError("Inserted sales scenario does not close its calibrated class activity")
    metadata = {
        "scenario": bundle.scenario.model_dump(mode="json"), "revision": revision,
        "files": {"database": {"path": str(database), "sha256": file_sha256(database)},
                  "build_report": {"path": str(build_report), "sha256": file_sha256(build_report)},
                  "ceud_manifest": {"path": str(manifest_path), "sha256": file_sha256(manifest_path)}, **inputs},
        "build_validation": report["validation"], "build_parameter_support": report["parameter_support"],
        "cohort_audit": report["existing_capacity"], "utilization_audit": report["road_utilization"],
        "ceud_parameter_inputs": manifest, "reference_state": reference_metadata,
        "notebook": notebook_metadata,
        "accepted_survival_sha256": accepted_survival_sha256,
        "sales_activity_closure": {"classes_checked": len(sales_rows), "passes": closure,
                                   "interpretation": "calibration check, not independent validation"},
        "limitations": [
            "The supplied database is the notebook-derived Ontario extract confirmed by the user. Its rows are comparison evidence.",
            "Comparisons retain original period and vintage labels. The current scenario manifest supplies its source-year interpretation.",
            "Utilization comparisons show the full minimum-to-maximum range by class and existing/new technology kind. Reference period rows and current vintage rows have different keys.",
            "Workbook values and the legacy fixed_lifetime execution flag do not parameterize the current backend.",
            "Activity/energy accounting evaluates all inserted base-year cohorts before future retirement; it is not an optimization result.",
            "Vehicle input energy includes PHEV blend energy once, excluding upstream charger losses; CEUD class energy contains a different fuel/fleet boundary.",
            "Current source history omits older cohorts and fuel registration shares proxy sales composition; stock discrepancy is not rescaled away.",
        ],
        "configuration": report["config"],
        "implementation_sha256": file_sha256(Path(__file__)),
        "comparison_significance_percent": rules.comparison_significance_percent,
        "commands": [f"uv run python src/build_transport.py --scenario {bundle.scenario_path}",
                     f"uv run python -m validation.transportation_research --scenario {bundle.scenario_path} --write-report"],
    }
    return TransportationResearch(frames, metadata,
                                  resolve_artifact_path(bundle, rules.report_route, bundle.scenario.scenario.name))


def _load_captured_report(path: Path) -> tuple[dict, dict[str, pd.DataFrame]]:
    """Read only manifested, hash-verified tables inside a report directory."""
    metadata = json.loads(path.read_text(encoding="utf-8"))
    frames = {}
    for filename, record in metadata["exports"].items():
        if Path(filename).name != filename or not filename.endswith(".csv"):
            raise ValueError(f"Invalid comparison export filename: {filename}")
        csv_path = path.parent / filename
        if csv_path.resolve().parent != path.parent.resolve() or not csv_path.is_file():
            raise ValueError(f"Comparison export escapes its report directory: {filename}")
        if file_sha256(csv_path) != record["sha256"]:
            raise ValueError(f"Comparison export changed after capture: {csv_path}")
        frame = pd.read_csv(csv_path)
        if len(frame) != record["rows"]:
            raise ValueError(f"Comparison export row count changed: {csv_path}")
        frames[Path(filename).stem] = frame
    return metadata, frames


def refresh_research_comparisons(
    bundle: ConfigBundle,
    *,
    database: Path | None = None,
    build_report: Path | None = None,
) -> TransportationResearch:
    """Recompute DB comparisons while retaining exact captured native audit bytes."""
    rules = TransportationResearchRules.model_validate(
        load_harmonization_rules(bundle, "transportation_research")
    )
    destination = resolve_artifact_path(bundle, rules.report_route, bundle.scenario.scenario.name)
    manifest_path = destination / "manifest.json"
    previous_hash = file_sha256(manifest_path)
    metadata, _ = _load_captured_report(manifest_path)
    database = database or resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    build_report = build_report or resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)
    report = json.loads(build_report.read_text(encoding="utf-8"))
    _verify_build(bundle, database, report)
    if metadata["scenario"] != bundle.scenario.model_dump(mode="json") or metadata["configuration"] != report["config"]:
        raise ValueError("Captured research report has different scenario or build configuration")
    reference_dir = resolve_artifact_path(bundle, rules.reference_route)
    reference = reference_dir / rules.reference_sqlite
    baseline = resolve_repo_path(bundle.repo_root, rules.legacy_baseline)
    identities = (
        (database, metadata["files"]["database"]),
        (build_report, metadata["files"]["build_report"]),
        (reference, metadata["reference_state"]["reference"]),
        (baseline, metadata["reference_state"]["legacy_baseline"]),
        (reference_dir / rules.reference_notebook, metadata["notebook"]),
    )
    for path, captured in identities:
        if path.resolve() != Path(captured["path"]).resolve() or file_sha256(path) != captured["sha256"]:
            raise ValueError(f"Captured comparison identity changed: {path}")
    for dependency in metadata["notebook"].get("dependencies", []):
        if dependency.get("provided_comparison_evidence"):
            path = Path(dependency["provided_comparison_evidence"])
            if file_sha256(path) != dependency["provided_sha256"]:
                raise ValueError(f"Captured notebook comparison evidence changed: {path}")
    frames = road_parameter_comparison(
        database, reference, baseline,
        demand_rules=load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"],
    )
    frames["reference_state"], metadata["reference_state"] = compare_reference_state(
        reference, baseline,
        absolute_tolerance=bundle.scenario.comparison.absolute_tolerance,
        relative_tolerance=bundle.scenario.comparison.relative_tolerance,
    )
    retained = {
        filename: record for filename, record in metadata.pop("exports").items()
        if Path(filename).stem not in frames
    }
    metadata.setdefault("comparison_refresh", []).append({
        "prior_manifest_sha256": previous_hash,
        "comparison_implementation_sha256": file_sha256(Path(__file__)),
        "refreshed_frames": sorted(frames),
        "command": f"uv run python -m validation.transportation_research --scenario {bundle.scenario_path} --refresh-comparisons --write-report",
        "interpretation": "Direct comparisons refreshed from unchanged databases; captured native audits and their implementation identity retained.",
    })
    return TransportationResearch(frames, metadata, destination, retained)


def write_research_report(result: TransportationResearch) -> Path:
    result.destination.mkdir(parents=True, exist_ok=True)
    path = result.destination / "manifest.json"
    previous = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    if result.retained_exports and result.metadata.get("comparison_refresh"):
        expected = result.metadata["comparison_refresh"][-1]["prior_manifest_sha256"]
        if not path.is_file() or file_sha256(path) != expected:
            raise ValueError("Captured report changed while preparing the comparison refresh")
    filenames = {f"{name}.csv" for name in result.frames} | set(result.retained_exports)
    if any(Path(filename).name != filename for filename in filenames):
        raise ValueError("Comparison export names must be filenames within the report directory")
    for filename, record in result.retained_exports.items():
        candidate = result.destination / filename
        if (candidate.resolve().parent != result.destination.resolve() or not candidate.is_file()
                or file_sha256(candidate) != record["sha256"]):
            raise ValueError(f"Retained comparison export changed after capture: {filename}")
    obsolete = []
    for filename, record in previous.get("exports", {}).items():
        if Path(filename).name != filename or not filename.endswith(".csv"):
            raise ValueError(f"Invalid previously owned comparison export: {filename}")
        if filename in filenames:
            continue
        candidate = result.destination / filename
        if not candidate.exists():
            continue
        if candidate.resolve().parent != result.destination.resolve() or not candidate.is_file():
            raise ValueError(f"Obsolete comparison export escapes its report directory: {filename}")
        if file_sha256(candidate) != record.get("sha256"):
            raise ValueError(f"Obsolete comparison export was modified after capture: {filename}")
        obsolete.append(candidate)
    exports = dict(result.retained_exports)
    for name, frame in result.frames.items():
        filename = f"{name}.csv"
        csv_path = result.destination / filename
        write_dataframe_atomic(frame, csv_path)
        exports[filename] = {"rows": len(frame), "sha256": file_sha256(csv_path)}
    for candidate in obsolete:
        candidate.unlink()
        LOGGER.info("Removed obsolete comparison export %s", candidate.name)
    write_text_atomic(json.dumps({**result.metadata, "exports": exports}, indent=2, sort_keys=True,
                                 allow_nan=False) + "\n", path)
    return path


def load_validation_diagnosis(*, repo_root: Path | None = None) -> dict[str, Any]:
    """Load configured captured comparisons and verify every exported table hash."""
    control = load_config_bundle("config/scenarios/default.yaml", repo_root=repo_root)
    rules = TransportationResearchRules.model_validate(
        load_harmonization_rules(control, "transportation_research")
    )
    selected = load_config_bundle(rules.notebook_scenario, repo_root=control.repo_root)
    report_directory = resolve_artifact_path(
        selected, rules.report_route, selected.scenario.scenario.name,
    )
    charging_rules = load_harmonization_rules(selected, "charging_cost_comparison")
    locations = {
        "fleet": report_directory / "manifest.json",
        "charging": resolve_artifact_path(selected, charging_rules["report_route"], "snapshot.json"),
    }
    result: dict[str, Any] = {"significance_percent": rules.comparison_significance_percent}
    for label, path in locations.items():
        metadata, frames = _load_captured_report(path)
        result[label] = frames
        result[f"{label}_metadata"] = metadata
    required = {"capacity_comparison", "utilization_comparison", "demand_comparison"}
    if required - set(result["fleet"]):
        raise ValueError("The configured fleet comparison predates the direct parameter comparison. Regenerate its report.")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--build-report", type=Path)
    parser.add_argument("--write-report", action="store_true")
    parser.add_argument("--refresh-comparisons", action="store_true",
                        help="Refresh database comparisons from a verified report without replaying mutable native audits")
    args = parser.parse_args()
    builder = refresh_research_comparisons if args.refresh_comparisons else build_research_report
    result = builder(load_config_bundle(args.scenario), database=args.database, build_report=args.build_report)
    if args.write_report:
        print(write_research_report(result))
    else:
        print(result.frames["reference_state" if args.refresh_comparisons else "stock_activity_energy"].to_string(index=False))


if __name__ == "__main__":
    main()
