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
from parameterization.road_stocks_and_demands import derive_road_annual_utilization
from utils import (
    ConfigBundle, file_sha256, load_config_bundle, load_harmonization_rules,
    resolve_artifact_path, resolve_configured_path, resolve_repo_path,
    write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import MappingModel
from validation.sqlite_utils import open_sqlite_readonly, quote_identifier
from validation.legacy_sales_reconstruction import reconstruct_legacy_sales


LOGGER = logging.getLogger(__name__)


class CeudSeries(MappingModel):
    table_id: int = Field(gt=0)
    raw_series: str = Field(min_length=1)


class TransportationResearchRules(MappingModel):
    reference_route: str
    report_route: str
    reference_sqlite: str
    reference_notebook: str
    inherited_sqlite: str = Field(min_length=1)
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
    legacy_stock = _read_vehicle_capacity(reference, {
        demand_rules["activity_series"][name]["commodity"]: name for name in energy_series
    })
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
        "demand_audit": report["demand"],
        "ceud_parameter_inputs": manifest, "reference_state": reference_metadata,
        "notebook": notebook_metadata,
        "accepted_survival_sha256": accepted_survival_sha256,
        "sales_activity_closure": {"classes_checked": len(sales_rows), "passes": closure,
                                   "interpretation": "calibration check, not independent validation"},
        "limitations": [
            "The supplied SQLite and the newer workbooks are separate evidence states; the notebook execution that produced the SQLite is unresolved.",
            "Comparisons retain original period and vintage labels. The current scenario manifest supplies its source-year interpretation.",
            "Utilization comparisons show the full minimum-to-maximum range by class and existing/new technology kind. Reference period rows and current vintage rows have different keys.",
            "The primary legacy comparison replays active fixed_lifetime=False; saved fixed-lifetime messages and disputed SQLite outputs are audit evidence only.",
            "Original Input data/canoe_ALL_cm.sqlite is unavailable; the configured inherited snapshot supports a conditional energy reconstruction, without proving historical identity.",
            "Activity/energy accounting evaluates all inserted base-year cohorts before future retirement; it is not an optimization result.",
            "Vehicle input energy includes PHEV blend energy once, excluding upstream charger losses; CEUD class energy contains a different fuel/fleet boundary.",
            "Raw sales-survival stock is retained for diagnosis; explicit class calibration anchors represented stock to base-year CEUD stock.",
        ],
        "configuration": report["config"],
        "implementation_sha256": file_sha256(Path(__file__)),
        "comparison_significance_percent": rules.comparison_significance_percent,
        "commands": [f"uv run python src/build_transport.py --scenario {bundle.scenario_path}",
                     f"uv run python -m validation.transportation_research --scenario {bundle.scenario_path} --write-report"],
    }
    diagnosis = {"fleet": frames, "fleet_metadata": metadata}
    frames.update(deepen_fleet_diagnosis(frames, metadata))
    frames.update(deepen_reference_cohorts(diagnosis, bundle, rules))
    frames.update(deepen_demand_diagnosis(diagnosis, bundle, rules))
    frames.update(reconstruct_comparison(diagnosis, bundle, rules))
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


def deepen_fleet_diagnosis(
    frames: dict[str, pd.DataFrame], metadata: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Reconcile captured sales, stock and calibration without changing parameters.

    Energy closure is a sensitivity on the same stock, efficiency and C2A. It
    deliberately exposes the resulting activity error rather than claiming that
    one utilization multiplier can independently close two different targets.
    """
    required = {"sales_cohorts", "road_age_cohorts", "fleet_cohort_accounting", "stock_activity_energy"}
    if missing := required - frames.keys():
        raise ValueError(f"Captured fleet report lacks deeper diagnostic evidence: {sorted(missing)}")
    summary = frames["stock_activity_energy"].copy()
    detail = frames["fleet_cohort_accounting"].copy()
    sales = frames["sales_cohorts"].copy()
    ages = frames["road_age_cohorts"]
    calibration_columns = {"stock_calibration_multiplier", "calibrated_surviving_k_vehicles", "stock_calibration_basis"}
    eligibility_columns = {"uncalibrated_surviving_k_vehicles", "stock_calibration_multiplier", "eligibility_basis", "fixed_lifetime_years", "first_model_period", "vintage"}
    if calibration_columns - set(sales) or eligibility_columns - set(ages):
        raise ValueError("Captured fleet evidence predates CEUD-anchored sales calibration; regenerate its report")
    keys = ["region", "road_class"]
    comparison = metadata.get("scenario", {}).get("comparison", {})
    if not {"absolute_tolerance", "relative_tolerance"} <= comparison.keys() or "comparison_significance_percent" not in metadata:
        raise ValueError("Captured fleet report lacks its comparison tolerances or significance threshold")
    absolute_tolerance = float(comparison["absolute_tolerance"])
    relative_tolerance = float(comparison["relative_tolerance"])
    significance = float(metadata["comparison_significance_percent"])
    if any(not isfinite(value) or value < 0 for value in (absolute_tolerance, relative_tolerance, significance)):
        raise ValueError("Captured fleet comparison controls are invalid")
    _require_unique(summary, keys, "fleet closure targets")
    _require_unique(detail, ["region", "tech", "vintage"], "fleet accounting cohorts")
    _require_unique(sales, ["ceud_region", "road_class", "vintage_year"], "annual sales cohorts")
    regional = summary[["ceud_region", "road_class", "region", "base_year", "ceud_stock_k_vehicles"]].rename(
        columns={"ceud_stock_k_vehicles": "expected_ceud_stock_k_vehicles"},
    )
    _require_unique(regional, ["ceud_region", "road_class"], "CEUD regional owners")
    sales = sales.merge(regional, on=["ceud_region", "road_class"], how="left", validate="many_to_one")
    if sales[keys + ["base_year"]].isna().any().any():
        raise ValueError("Annual sales lack a captured regional/class target")

    def require_physical(frame: pd.DataFrame, columns: list[str], label: str) -> None:
        values = frame[columns]
        if values.isna().any().any() or not values.map(isfinite).all().all() or values.lt(0).any().any():
            raise ValueError(f"{label} contains missing, negative or nonfinite values")

    require_physical(sales, ["sales_k_vehicles", "surviving_k_vehicles", "survival_probability", "age", "stock_calibration_multiplier", "calibrated_surviving_k_vehicles"], "Sales reconciliation")
    require_physical(detail, ["capacity", "c2a", "factor", "efficiency", "implied_activity_bn", "implied_vehicle_input_pj"], "Cohort accounting")
    require_physical(summary, ["ceud_stock_k_vehicles", "inserted_stock_k_vehicles", "ceud_activity_bn", "implied_activity_bn", "ceud_secondary_energy_pj", "implied_vehicle_input_pj"], "Fleet targets")
    if (sales.survival_probability.gt(1).any() or not detail.factor.between(0, 1).all()
            or detail.c2a.le(0).any() or detail.efficiency.le(0).any()):
        raise ValueError("Cohort survival, utilization, C2A or efficiency is inadmissible")
    if not all(
        isclose(row.implied_activity_bn, row.capacity * row.c2a * row.factor, rel_tol=1e-10, abs_tol=1e-9)
        and isclose(row.implied_vehicle_input_pj, row.implied_activity_bn / row.efficiency, rel_tol=1e-10, abs_tol=1e-9)
        for row in detail.itertuples()
    ):
        raise ValueError("Captured cohort activity or energy does not satisfy its physical identity")
    accounting = detail.groupby(keys, as_index=False).agg(
        captured_stock=("capacity", "sum"), captured_activity=("implied_activity_bn", "sum"),
        captured_energy=("implied_vehicle_input_pj", "sum"),
    ).merge(summary, on=keys, how="outer", validate="one_to_one", indicator=True)
    if not accounting._merge.eq("both").all() or not all(
        isclose(getattr(row, captured), getattr(row, total), rel_tol=1e-10, abs_tol=1e-9)
        for row in accounting.itertuples()
        for captured, total in (("captured_stock", "inserted_stock_k_vehicles"),
                                ("captured_activity", "implied_activity_bn"),
                                ("captured_energy", "implied_vehicle_input_pj"))
    ):
        raise ValueError("Cohort accounting and captured fleet totals differ")
    if not all(isclose(row.sales_k_vehicles * row.survival_probability, row.surviving_k_vehicles,
                       rel_tol=1e-10, abs_tol=1e-9) for row in sales.itertuples()):
        raise ValueError("Sales times cumulative survival does not reconcile")
    if not sales.stock_calibration_basis.eq("base_year_ceud_stock").all() or not all(
        isclose(row.calibrated_surviving_k_vehicles, row.surviving_k_vehicles * row.stock_calibration_multiplier,
                rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        and isclose(row.ceud_stock_k_vehicles, row.expected_ceud_stock_k_vehicles,
                    rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        for row in sales.itertuples()
    ):
        raise ValueError("Captured stock calibration does not match raw survivors, multiplier or CEUD target")
    if not sales.age.eq(sales.base_year - sales.vintage_year).all():
        raise ValueError("Sales cohort age differs from the captured base year")
    for _, group in sales.groupby(keys):
        starts = group.included_history_start.unique()
        if len(starts) != 1 or set(group.vintage_year) != set(range(int(starts[0]), int(group.base_year.iloc[0]) + 1)):
            raise ValueError("Annual sales coverage is incomplete; missing cohorts are not zero")
        if group.source_history_start.nunique(dropna=False) != 1 or group.older_history_status.nunique(dropna=False) != 1:
            raise ValueError("Annual sales have inconsistent captured history metadata")
        if (group.stock_calibration_multiplier.nunique() != 1
                or not isclose(float(group.calibrated_surviving_k_vehicles.sum()),
                               float(group.expected_ceud_stock_k_vehicles.iloc[0]),
                               rel_tol=relative_tolerance, abs_tol=absolute_tolerance)):
            raise ValueError("Calibrated surviving cohorts do not close their region/class CEUD stock target")
    sales["retired_k_vehicles"] = sales.sales_k_vehicles - sales.surviving_k_vehicles
    eligible = ages.loc[ages.stock_method.eq("sales_survival")].copy()
    _require_unique(eligible, [*keys, "vintage_year"], "annual allocated cohorts")
    require_physical(eligible, ["raw_cohort_k_vehicles", "cohort_k_vehicles"], "Allocated cohorts")
    if not eligible.eligible_first_model_period.isin([True, False]).all():
        raise ValueError("Annual cohort eligibility is missing or invalid")
    sales = sales.merge(
        eligible[[*keys, "vintage_year", "eligible_first_model_period", "raw_cohort_k_vehicles", "cohort_k_vehicles",
                  "uncalibrated_surviving_k_vehicles", "stock_calibration_multiplier", "eligibility_basis",
                  "fixed_lifetime_years", "first_model_period", "vintage"]].rename(
                      columns={"stock_calibration_multiplier": "cohort_stock_calibration_multiplier"},
                  ),
        on=[*keys, "vintage_year"], how="outer", validate="one_to_one", indicator=True,
    )
    if not sales._merge.eq("both").all():
        raise ValueError("Sales and allocated annual cohort coverage differ")
    if not all(
        isclose(row.raw_cohort_k_vehicles, row.calibrated_surviving_k_vehicles, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        and isclose(row.uncalibrated_surviving_k_vehicles, row.surviving_k_vehicles, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        and isclose(row.cohort_stock_calibration_multiplier, row.stock_calibration_multiplier, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        for row in sales.itertuples()
    ):
        raise ValueError("Annual allocation does not start from the captured CEUD-calibrated survivors")
    sales = sales.drop(columns="_merge").rename(columns={"cohort_k_vehicles": "inserted_cohort_k_vehicles"})
    sales["eligibility_redistribution_k_vehicles"] = sales.inserted_cohort_k_vehicles - sales.calibrated_surviving_k_vehicles
    sales["ineligible_survivors_k_vehicles"] = sales.calibrated_surviving_k_vehicles.where(~sales.eligible_first_model_period, 0)
    sales["ineligible_raw_survivors_k_vehicles"] = sales.surviving_k_vehicles.where(~sales.eligible_first_model_period, 0)
    totals = sales.groupby(keys, as_index=False).agg(
        sales_k_vehicles=("sales_k_vehicles", "sum"), retired_k_vehicles=("retired_k_vehicles", "sum"),
        surviving_k_vehicles=("surviving_k_vehicles", "sum"),
        calibrated_surviving_k_vehicles=("calibrated_surviving_k_vehicles", "sum"),
        stock_calibration_multiplier=("stock_calibration_multiplier", "first"),
        ineligible_survivors_k_vehicles=("ineligible_survivors_k_vehicles", "sum"),
        ineligible_raw_survivors_k_vehicles=("ineligible_raw_survivors_k_vehicles", "sum"),
        allocated_stock_k_vehicles=("inserted_cohort_k_vehicles", "sum"),
        history_start=("vintage_year", "min"), history_end=("vintage_year", "max"),
    )
    if not all(isclose(row.allocated_stock_k_vehicles, row.calibrated_surviving_k_vehicles,
                       rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
               for row in totals.itertuples()):
        raise ValueError("Eligibility redistribution does not conserve calibrated class stock")
    history = sales.sort_values("vintage_year").groupby(keys, as_index=False).first()
    history = history[[*keys, "source_history_start", "survival_probability", "older_history_status"]].rename(
        columns={"survival_probability": "oldest_survival_probability"},
    )
    totals = totals.merge(history, on=keys, validate="one_to_one")
    fleet = summary.merge(totals, on=keys, how="outer", validate="one_to_one", indicator=True)
    if not fleet._merge.eq("both").all():
        raise ValueError("Sales reconstruction and fleet target coverage differ")
    fleet = fleet.drop(columns="_merge")
    fleet["allocation_conservation_residual_k_vehicles"] = fleet.allocated_stock_k_vehicles - fleet.calibrated_surviving_k_vehicles
    fleet["survival_to_inserted_residual_k_vehicles"] = fleet.inserted_stock_k_vehicles - fleet.surviving_k_vehicles
    fleet["calibrated_to_inserted_residual_k_vehicles"] = fleet.inserted_stock_k_vehicles - fleet.calibrated_surviving_k_vehicles
    fleet["raw_stock_minus_ceud_k_vehicles"] = fleet.surviving_k_vehicles - fleet.ceud_stock_k_vehicles
    fleet["raw_stock_difference_percent"] = 100 * (fleet.surviving_k_vehicles / fleet.ceud_stock_k_vehicles.where(fleet.ceud_stock_k_vehicles.ne(0)) - 1)
    fleet["raw_stock_significant"] = fleet.raw_stock_difference_percent.abs().ge(significance)
    fleet["calibrated_stock_minus_ceud_k_vehicles"] = fleet.calibrated_surviving_k_vehicles - fleet.ceud_stock_k_vehicles
    fleet["stock_difference_percent"] = 100 * (fleet.inserted_stock_k_vehicles / fleet.ceud_stock_k_vehicles.where(fleet.ceud_stock_k_vehicles.ne(0)) - 1)
    fleet["stock_significant"] = fleet.stock_difference_percent.abs().ge(significance)
    cutoffs = []
    for (region, road_class), group in sales.groupby(keys):
        if group.eligibility_basis.nunique() != 1 or group.first_model_period.nunique() != 1:
            raise ValueError("Captured class eligibility has inconsistent method or operating period")
        basis = str(group.eligibility_basis.iloc[0])
        lifetime = None
        first_period = int(group.first_model_period.iloc[0])
        if basis == "fixed_lifetime":
            if group.fixed_lifetime_years.nunique() != 1 or group.fixed_lifetime_years.isna().any():
                raise ValueError("Captured fixed-lifetime eligibility lacks one class lifetime")
            lifetime = float(group.fixed_lifetime_years.iloc[0])
            expected_eligibility = (group.vintage_year + lifetime > first_period) & (group.vintage + lifetime > first_period)
            if not isfinite(lifetime) or lifetime <= 0 or not expected_eligibility.eq(group.eligible_first_model_period).all():
                raise ValueError("Fixed-lifetime eligibility differs from annual and model-vintage cutoffs")
        elif basis != "accepted_survival_curve_support" or group.fixed_lifetime_years.notna().any():
            raise ValueError("Curve-mode eligibility must not apply a fixed lifetime")
        retained = group.loc[group.eligible_first_model_period]
        first_annual = int(retained.vintage_year.min()) if not retained.empty else None
        first_vintage = int(retained.vintage.min()) if not retained.empty else None
        cutoffs.append({
            "region": region, "road_class": road_class, "eligibility_basis": basis,
            "fixed_lifetime_years": lifetime, "first_model_period": first_period,
            "first_eligible_annual_cohort": first_annual,
            "last_eligible_annual_cohort": int(retained.vintage_year.max()) if not retained.empty else None,
            "first_eligible_model_vintage": first_vintage,
            "eligibility_explanation": (
                f"Fixed lifetime {lifetime:g} years; annual cohort year + lifetime and model vintage + lifetime must both exceed {first_period}. "
                f"First eligible annual cohort {first_annual}, model vintage {first_vintage}."
                if lifetime is not None else
                f"Accepted survival-curve support; fixed median not applied. First eligible annual cohort {first_annual}, model vintage {first_vintage}."
            ),
        })
    fleet = fleet.merge(pd.DataFrame(cutoffs), on=keys, validate="one_to_one")

    def explain_stock(row: pd.Series) -> str:
        label = "significant" if row.raw_stock_significant else f"below {significance:g}% significance"
        gap = f"Raw sales × survival {row.raw_stock_difference_percent:+.1f}% vs CEUD ({label}); stock multiplier {row.stock_calibration_multiplier:.4f}."
        history_note = (
            f"Pre-{int(row.history_start)} sales are not reconstructed"
            if row.older_history_status == "not_reconstructed"
            else f"Captured older-history status: {row.older_history_status}"
        )
        if row.raw_stock_difference_percent > 0:
            history_note += "; omissions cannot explain the raw surplus."
        else:
            history_note += "; their contribution to the raw shortfall is unquantified."
        return (
            f"{gap} CEUD stock closes before and after eligibility redistribution; inserted minus calibrated stock "
            f"{row.calibrated_to_inserted_residual_k_vehicles:+.3g}k. {history_note} Population/survival consistency remains unresolved."
        )

    fleet["stock_explanation"] = fleet.apply(
        explain_stock, axis=1,
    )
    calibration = pd.DataFrame(metadata.get("utilization_audit", {}).get("sales_activity_calibration", []))
    if calibration.empty:
        raise ValueError("Captured report lacks the implemented utilization calibration audit")
    _require_unique(calibration, keys, "utilization calibration audit")
    calibration = calibration[keys + ["prior_mean_utilization", "utilization", "capacity_activity_denominator_bn"]].rename(
        columns={"utilization": "implemented_utilization"},
    )
    calibration = summary.merge(calibration, on=keys, how="outer", validate="one_to_one", indicator=True)
    if not calibration._merge.eq("both").all():
        raise ValueError("Utilization calibration and fleet target coverage differ")
    calibration = calibration.drop(columns="_merge")
    require_physical(calibration, ["prior_mean_utilization", "implemented_utilization", "capacity_activity_denominator_bn"], "Utilization audit")
    if (not calibration.prior_mean_utilization.between(0, 1).all()
            or not calibration.implemented_utilization.between(0, 1).all()
            or calibration.implemented_utilization.le(0).any()
            or calibration.implied_vehicle_input_pj.le(0).any()):
        raise ValueError("Energy sensitivity requires positive implemented utilization and consumption")
    calibration["calibration_percent"] = 100 * (calibration.implemented_utilization / calibration.prior_mean_utilization.where(calibration.prior_mean_utilization.ne(0)) - 1)
    calibration["energy_scale"] = calibration.ceud_secondary_energy_pj / calibration.implied_vehicle_input_pj
    calibration["energy_closing_utilization"] = calibration.implemented_utilization * calibration.energy_scale
    calibration["energy_closure_feasible"] = calibration.energy_closing_utilization.le(1)
    calibration["activity_at_energy_closure_bn"] = calibration.implied_activity_bn * calibration.energy_scale
    calibration["energy_closure_activity_difference_percent"] = 100 * (calibration.activity_at_energy_closure_bn / calibration.ceud_activity_bn.where(calibration.ceud_activity_bn.ne(0)) - 1)
    calibration["prior_energy_pj"] = calibration.implied_vehicle_input_pj * calibration.prior_mean_utilization / calibration.implemented_utilization
    calibration["prior_activity_bn"] = calibration.implied_activity_bn * calibration.prior_mean_utilization / calibration.implemented_utilization
    calibration["energy_difference_percent"] = 100 * (
        calibration.implied_vehicle_input_pj / calibration.ceud_secondary_energy_pj.where(calibration.ceud_secondary_energy_pj.ne(0)) - 1
    )
    calibration["activity_accounting_residual_bn"] = calibration.implied_activity_bn - calibration.ceud_activity_bn
    calibration["energy_significant"] = calibration.energy_difference_percent.abs().ge(significance)

    def explain_energy(row: pd.Series) -> str:
        activity_closes = isclose(row.implied_activity_bn, row.ceud_activity_bn,
                                 rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        closure = "Activity closes" if activity_closes else f"Activity residual {row.activity_accounting_residual_bn:+.4g} bn service-km"
        label = "significant" if row.energy_significant else f"below {significance:g}% significance"
        return (
            f"{closure}; energy {row.energy_difference_percent:+.1f}% vs CEUD ({label}). "
            f"Energy-closing utilization {row.energy_closing_utilization:.5f} would change activity by "
            f"{row.energy_closure_activity_difference_percent:+.1f}% (diagnostic sensitivity). "
            "Fleet/fuel mix, cohort efficiency and the CEUD energy boundary remain unresolved."
        )

    calibration["energy_explanation"] = calibration.apply(explain_energy, axis=1)
    fleet = fleet.merge(calibration[keys + ["energy_explanation", "energy_significant"]], on=keys, validate="one_to_one")
    supported = detail.merge(calibration[keys + ["prior_mean_utilization", "implemented_utilization", "energy_closing_utilization"]],
                             on=keys, how="left", validate="many_to_one")
    if supported.implemented_utilization.isna().any() or not all(
        isclose(row.factor, row.implemented_utilization, rel_tol=1e-10, abs_tol=1e-12)
        for row in supported.itertuples()
    ):
        raise ValueError("Inserted cohort utilization differs from its captured calibration")
    denominators = detail.assign(denominator=detail.capacity * detail.c2a).groupby(keys).denominator.sum()
    if not all(isclose(denominators.loc[(row.region, row.road_class)], row.capacity_activity_denominator_bn,
                       rel_tol=1e-10, abs_tol=1e-9) for row in calibration.itertuples()):
        raise ValueError("Captured utilization denominator differs from inserted capacity × C2A")
    stages = []
    for label, factor_column in (
        ("Before calibration", "prior_mean_utilization"),
        ("Implemented activity calibration", "implemented_utilization"),
        ("Energy closure sensitivity", "energy_closing_utilization"),
    ):
        stage = supported.copy()
        stage["stage"] = label
        stage["annual_utilization"] = stage[factor_column]
        stage["annual_service_per_vehicle"] = stage.c2a * stage.annual_utilization * 1e6
        stage["activity_bn"] = stage.capacity * stage.c2a * stage.annual_utilization
        stage["vehicle_input_pj"] = stage.activity_bn / stage.efficiency
        stage["service_unit"] = stage.road_class.map(
            lambda name: "passenger-km/vehicle/year" if name in {"cars", "passenger_light_trucks"} else "tonne-km/vehicle/year"
        )
        stages.append(stage)
    return {
        "sales_cohort_reconciliation": sales.sort_values([*keys, "vintage_year"]).reset_index(drop=True),
        "fleet_reconciliation": fleet,
        "utilization_calibration": calibration,
        "cohort_utilization_energy": pd.concat(stages, ignore_index=True),
    }


def independent_fleet_comparison(frames: dict[str, pd.DataFrame], *, step: int) -> dict[str, pd.DataFrame]:
    """Compare complete parameterizations; preserve annual lineage and UF controls.

    Backend capacities already include survival and fixed-lifetime allocation.
    Legacy acquisitions receive their own representative-vintage survival exactly
    once. Neither method borrows the other's normalization or efficiencies.
    """
    keys = ["region", "road_class"]
    backend = frames["sales_cohort_reconciliation"].copy()
    backend["evidence"] = "Current backend"
    backend["surviving_k_vehicles"] = backend.inserted_cohort_k_vehicles
    backend["survival_treatment"] = "Annual survival → CEUD scaling → fixed-lifetime allocation; no second retirement"
    legacy = frames["legacy_reconstructed_annual"].copy()
    legacy["evidence"] = "Legacy recipe"
    legacy["survival_treatment"] = "Aggregate acquisitions first; survival at representative model vintage"
    columns = keys + ["evidence", "vintage", "vintage_year", "surviving_k_vehicles", "survival_treatment"]
    annual = pd.concat([backend[columns], legacy[columns]], ignore_index=True)
    if step <= 0 or annual[columns].isna().any().any() or annual.surviving_k_vehicles.lt(0).any():
        raise ValueError("Invalid comparison annual lineage")
    annual["annual_position"] = ((annual.vintage_year - annual.vintage - 1) % step + 1).astype(int)
    annual["cohort_position"] = "Year " + annual.annual_position.astype(str)
    annual["interval"] = annual.vintage.map(lambda value: f"({value}, {value + step}]")
    annual["boundary_exception"] = annual.vintage_year.le(annual.vintage)
    annual.loc[annual.boundary_exception, "interval"] += "; oldest proxy includes lower endpoint"
    available = annual.groupby(keys + ["evidence", "vintage"]).vintage_year
    first_year = available.transform("min").astype(int)
    last_year = available.transform("max").astype(int)
    annual["interval"] += "; available " + first_year.astype(str) + "–" + last_year.astype(str)
    vintages = annual.groupby(keys + ["evidence", "vintage"], as_index=False).surviving_k_vehicles.sum()
    totals = vintages.groupby(keys + ["evidence"], as_index=False).surviving_k_vehicles.sum()
    targets = frames["stock_activity_energy"][keys + ["base_year", "ceud_stock_k_vehicles"]]
    totals = totals.merge(targets, on=keys, how="left", validate="many_to_one")
    totals["difference_percent"] = 100 * (totals.surviving_k_vehicles / totals.ceud_stock_k_vehicles - 1)
    checks = {
        "Current backend": frames["fleet_cohort_accounting"].groupby(keys).capacity.sum(),
        "Legacy recipe": frames["legacy_reconstructed_parameters"].groupby(keys).surviving_k_vehicles.sum(),
    }
    vintage_checks = {
        "Current backend": frames["fleet_cohort_accounting"].groupby(keys + ["vintage"]).capacity.sum(),
        "Legacy recipe": frames["legacy_reconstructed_parameters"].groupby(keys + ["vintage"]).surviving_k_vehicles.sum(),
    }
    for evidence, expected_vintages in vintage_checks.items():
        actual_vintages = vintages.loc[vintages.evidence.eq(evidence)].set_index(keys + ["vintage"]).surviving_k_vehicles
        actual_vintages, expected_vintages = actual_vintages.align(expected_vintages, fill_value=0.)
        if not all(isclose(left, right, rel_tol=1e-10, abs_tol=1e-8)
                   for left, right in zip(actual_vintages, expected_vintages, strict=True)):
            raise ValueError("Annual comparison contributions do not recover parameterized model-vintage stock")
    for row in totals.itertuples():
        if not isclose(row.surviving_k_vehicles, checks[row.evidence].loc[(row.region, row.road_class)],
                       rel_tol=1e-10, abs_tol=1e-8):
            raise ValueError("Annual comparison contributions do not recover parameterized stock")
    # Separate annual-age exclusion from the additional model-vintage screen.
    cutoffs = frames["sales_cohort_reconciliation"].copy()
    cutoffs["annual_lifetime_eligible"] = cutoffs.vintage_year + cutoffs.fixed_lifetime_years > cutoffs.first_model_period
    cutoffs["model_vintage_eligible"] = cutoffs.vintage + cutoffs.fixed_lifetime_years > cutoffs.first_model_period
    cutoffs["coarse_vintage_exclusion_k_vehicles"] = cutoffs.calibrated_surviving_k_vehicles.where(
        cutoffs.annual_lifetime_eligible & ~cutoffs.model_vintage_eligible, 0.)
    cutoffs["all_excluded_k_vehicles"] = cutoffs.calibrated_surviving_k_vehicles.where(
        ~cutoffs.eligible_first_model_period, 0.)
    cutoff_totals = cutoffs.groupby(keys + ["fixed_lifetime_years", "first_model_period"], as_index=False).agg(
        coarse_vintage_exclusion_k_vehicles=("coarse_vintage_exclusion_k_vehicles", "sum"),
        all_excluded_k_vehicles=("all_excluded_k_vehicles", "sum"),
    ).merge(targets, on=keys, validate="one_to_one")
    cutoff_totals["coarse_exclusion_percent"] = 100 * cutoff_totals.coarse_vintage_exclusion_k_vehicles / cutoff_totals.ceud_stock_k_vehicles
    cutoff_totals["total_redistributed_percent"] = 100 * cutoff_totals.all_excluded_k_vehicles / cutoff_totals.ceud_stock_k_vehicles
    # Backend sensitivities retain the same stock, C2A and efficiency rows.
    stages = frames["cohort_utilization_energy"].groupby(keys + ["stage"], as_index=False).agg(
        implied_activity_bn=("activity_bn", "sum"), implied_vehicle_input_pj=("vehicle_input_pj", "sum"),
        utilization=("annual_utilization", "first"),
    ).rename(columns={"stage": "case"})
    labels = {"Before calibration": "Backend before calibration",
              "Implemented activity calibration": "Current backend",
              "Energy closure sensitivity": "Backend energy sensitivity"}
    stages["case"] = stages.case.replace(labels)
    legacy_totals = frames["legacy_reconstructed_totals"].copy()
    legacy_totals["case"] = "Legacy recipe"
    if "utilization" not in legacy_totals:
        legacy_totals = legacy_totals.merge(frames["legacy_reconstructed_utilization"][keys + ["utilization"]],
                                            on=keys, validate="one_to_one")
    cases = pd.concat([stages, legacy_totals[stages.columns]], ignore_index=True)
    target_columns = keys + ["base_year", "ceud_activity_bn", "ceud_secondary_energy_pj",
                             "ceud_activity_observed_year", "ceud_energy_observed_year"]
    cases = cases.merge(frames["stock_activity_energy"][target_columns], on=keys, validate="many_to_one")
    cases["activity_difference_percent"] = 100 * (cases.implied_activity_bn / cases.ceud_activity_bn - 1)
    cases["energy_difference_percent"] = 100 * (cases.implied_vehicle_input_pj / cases.ceud_secondary_energy_pj - 1)
    cases["energy_per_service_pj_per_bn"] = cases.implied_vehicle_input_pj / cases.implied_activity_bn
    cases["ceud_energy_per_service_pj_per_bn"] = cases.ceud_secondary_energy_pj / cases.ceud_activity_bn
    return {"comparison_annual_stock": annual, "comparison_vintage_stock": vintages,
            "comparison_stock_totals": totals, "backend_lifetime_exclusions": cutoff_totals,
            "baseline_activity_energy_cases": cases}


def reconstruct_comparison(result, bundle, rules) -> dict[str, pd.DataFrame]:
    """Capture independent recipe inputs and quantitative backend calibration context."""
    from parameterization.road_utilization import select_baseline_years

    frames, metadata = result["fleet"], result["fleet_metadata"]
    directory = resolve_artifact_path(bundle, rules.reference_route)
    legacy, identity = reconstruct_legacy_sales(
        directory / rules.reference_notebook,
        resolve_repo_path(bundle.repo_root, rules.inherited_sqlite),
        comparison_year=bundle.scenario.periods.base_year,
        regions=bundle.scenario.geography.regions,
    )
    if metadata.get("legacy_reconstruction", identity) != identity:
        raise ValueError("Captured independent legacy reconstruction inputs changed; recapture the report")
    metadata["legacy_reconstruction"] = identity
    raw = legacy["legacy_reconstructed_parameters"]
    expected = frames["reference_recipe_parameters"]
    keys = ["region", "tech", "vintage"]
    check = raw[keys + ["raw_capacity_k_vehicles"]].merge(
        expected[keys + ["capacity_k_vehicles"]], on=keys, how="outer", validate="one_to_one", indicator=True)
    if not check._merge.eq("both").all() or not all(
        isclose(row.raw_capacity_k_vehicles, row.capacity_k_vehicles, rel_tol=1e-12, abs_tol=1e-9)
        for row in check.itertuples()
    ):
        raise ValueError("Executable legacy acquisition recipe differs from the annual workbook audit")
    assembled = {**frames, **legacy}
    comparison = independent_fleet_comparison(assembled, step=bundle.scenario.periods.step)
    service = frames["demand_baseline_service_targets"]
    comparison["baseline_activity_energy_cases"] = comparison["baseline_activity_energy_cases"].merge(
        service[["region", "road_class", "current_base_year_demand_bn", "legacy_calendar_base_year_demand_bn"]],
        on=["region", "road_class"], validate="many_to_one")
    # Recompute the original grey arithmetic mean from the same observed CEUD
    # edition, keeping every year's numerator and denominator available for review.
    ceud = pd.concat([read_ceud_input(bundle, region) for region in bundle.scenario.geography.regions], ignore_index=True)
    observed = ceud.loc[ceud.ceud_value_status.eq("observed")]
    controls = load_harmonization_rules(bundle, "road_utilization")
    years = select_baseline_years(observed, base_year=min(bundle.scenario.periods.base_year, int(observed.year.max())),
                                  observations=controls["baseline_observations"], excluded_years=controls["excluded_years"])
    demand = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    stock = resolved_road_lifetime_rules(bundle)
    classes = set(frames["stock_activity_energy"].road_class)
    detail = frames["fleet_cohort_accounting"]
    c2a = detail.groupby("road_class").c2a
    if c2a.nunique().ne(1).any():
        raise ValueError("Backend class calibration unexpectedly uses multiple C2A factors")
    source_years = derive_road_annual_utilization(
        ceud, regions=bundle.scenario.geography.regions, years=years,
        activity_series={name: demand["activity_series"][name] for name in classes},
        stock_series={name: stock["ceud_stock_series"][name] for name in classes},
        capacity_by_class=c2a.first().to_dict(), region_output_map=demand["region_output_map"],
        activity_to_billion_factor=demand["activity_to_billion_factor"],
    )
    means = source_years.groupby(["region", "road_class"]).utilization.mean()
    calibration = frames["utilization_calibration"].copy()
    for row in calibration.itertuples():
        if not isclose(means.loc[(row.region, row.road_class)], row.prior_mean_utilization, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError("Reconstructed grey utilization differs from captured source-year mean")
    calibration["source_years"] = ", ".join(map(str, years))
    calibration["c2a"] = calibration.road_class.map(c2a.first())
    calibration["stock_denominator_k_vehicles"] = calibration.inserted_stock_k_vehicles
    comparison["backend_utilization_source_years"] = source_years
    comparison["backend_utilization_bridge"] = calibration
    return {**legacy, **comparison}


def deepen_demand_diagnosis(
    result: dict[str, Any], control: ConfigBundle, rules: TransportationResearchRules,
) -> dict[str, pd.DataFrame]:
    """Replay demand and audit its formula, source identity and calendar labels.

    Returns independent recipe, baseline and additive attribution tables. Captured
    database rows remain distinct from the intended notebook calculation.
    """
    metadata, frames = result["fleet_metadata"], result["fleet"]
    reference_dir = resolve_artifact_path(control, rules.reference_route)
    workbook_paths = {}

    def workbook(filename):
        evidence = [item for item in metadata["notebook"]["dependencies"]
                    if Path(item["path"]).name == filename]
        if len(evidence) != 1 or not evidence[0].get("provided_sha256"):
            raise ValueError(f"Missing captured notebook workbook identity: {filename}")
        path = reference_dir / filename
        if file_sha256(path) != evidence[0]["provided_sha256"]:
            raise ValueError(f"Notebook workbook changed after capture: {path}")
        workbook_paths[filename] = path
        return pd.read_excel(path)

    activity = workbook("activity.xlsx")
    growth = workbook("demand_growth_rate.xlsx")
    nodes = [node for source in frames["notebook_source_cells"].source
             for node in ast.parse(source).body]
    functions = {node.name: node for node in nodes if isinstance(node, ast.FunctionDef)}
    if not {"project_demand_v", "organize_activity_ceud", "shift_time_period"} <= functions.keys():
        raise ValueError("Captured notebook lacks the inspected demand recipe functions")
    project = functions["project_demand_v"]
    organize = functions["organize_activity_ceud"]
    shift = functions["shift_time_period"]
    project_notes = [node.value.value for node in ast.walk(project)
                     if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
                     and isinstance(node.value.value, str)
                     and any(isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
                             and target.slice.value == "notes" for target in node.targets)]
    mapping = [node.value for node in ast.walk(organize)
               if isinstance(node, ast.Assign) and any(
                   isinstance(target, ast.Name) and target.id == "map_comm"
                   for target in node.targets)]
    baseline_selector = ast.dump(ast.parse("activity_ceud_organized['period']", mode="eval").body)
    base_years = [node.comparators[0].value for node in ast.walk(project)
                  if isinstance(node, ast.Compare) and len(node.comparators) == 1
                  and isinstance(node.ops[0], ast.Eq)
                  and ast.dump(node.left) == baseline_selector
                  and isinstance(node.comparators[0], ast.Constant)
                  and isinstance(node.comparators[0].value, int)]
    shifts = [node.value.right.value for node in ast.walk(shift)
              if isinstance(node, ast.Assign) and isinstance(node.value, ast.BinOp)
              and isinstance(node.value.op, ast.Sub)
              and isinstance(node.value.right, ast.Constant)
              and any(isinstance(target, ast.Subscript)
                      and isinstance(target.slice, ast.Constant)
                      and target.slice.value == "period" for target in node.targets)]
    ranges = [ast.literal_eval(ast.Tuple(elts=node.args, ctx=ast.Load()))
              for node in ast.walk(project) if isinstance(node, ast.Call)
              and isinstance(node.func, ast.Name) and node.func.id == "range"]
    choices = [keyword.value.value for node in nodes for call in ast.walk(node)
               if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
               and call.func.id == "main_compile_data_transport"
               for keyword in call.keywords if keyword.arg == "growth_rate_type"
               and isinstance(keyword.value, ast.Constant)]
    requested_periods = [ast.literal_eval(ast.Tuple(elts=keyword.value.args, ctx=ast.Load()))
                        for node in nodes for call in ast.walk(node)
                        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                        and call.func.id == "main_compile_data_transport"
                        for keyword in call.keywords if keyword.arg == "range_period"
                        and isinstance(keyword.value, ast.Call)
                        and isinstance(keyword.value.func, ast.Name) and keyword.value.func.id == "range"]
    if not (len(mapping) == len(base_years) == len(shifts) == len(ranges) == len(choices)
            == len(requested_periods) == 1):
        raise ValueError("Supplied notebook demand recipe changed; inspect its AST before replay")
    expected_assignment = ast.dump(ast.parse(
        "demand_update_v['demand'] = demand_update_v['demand'] * demand_update_v['growth_factor']"
    ).body[0])
    demand_writes = [node for node in ast.walk(project) if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Subscript)
                             and isinstance(target.slice, ast.Constant)
                             and target.slice.value == "demand" for target in node.targets)]
    if len(demand_writes) != 1 or ast.dump(demand_writes[0]) != expected_assignment:
        raise ValueError("Supplied notebook no longer multiplies baseline activity by its growth factor")
    conversions = [node.value.right.value for node in ast.walk(organize)
                   if isinstance(node, ast.Assign) and isinstance(node.value, ast.BinOp)
                   and isinstance(node.value.op, ast.Div) and isinstance(node.value.right, ast.Constant)
                   and any(isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
                           and target.slice.value == "demand" for target in node.targets)]
    mode_to_commodity = ast.literal_eval(mapping[0])
    baseline_year, period_shift, growth_column = base_years[0], shifts[0], choices[0]
    source_years = set(range(*ranges[0])) & {year + period_shift for year in range(*requested_periods[0])}
    if not source_years:
        raise ValueError("Notebook demand recipe has no requested projection years")
    growth = growth.set_index("Year")
    if not growth.index.is_unique or growth.index.isna().any():
        raise ValueError("Notebook growth workbook has missing or duplicate years")
    if baseline_year not in growth.index or growth_column not in growth:
        raise ValueError("Notebook growth workbook lacks its explicit baseline or selection")
    if source_years - set(growth.index) or any(
        not isfinite(float(value)) or float(value) <= 0 for value in growth[growth_column]
    ):
        raise ValueError("Notebook growth workbook has missing years or invalid growth factors")
    deflator_columns = [name for name in growth if "Gross Domestic Product Deflator" in str(name)]
    deflator_identity = len(deflator_columns) == 1 and all(
        isclose(float(row[growth_column]), float(row[deflator_columns[0]]) /
                float(growth.loc[baseline_year, deflator_columns[0]]), rel_tol=1e-12)
        for _, row in growth.iterrows()
    )
    # A column label plus an internal ratio is insufficient evidence of economic
    # meaning. Retain the actual Excel formula and independently compare the raw
    # levels against the registered CER source, without downloading or modifying it.
    from openpyxl import load_workbook
    import re
    from fetching.cer_enerfuture import (
        build_requests, module_rules, normalize_component, read_and_validate_source,
    )

    excel = load_workbook(workbook_paths["demand_growth_rate.xlsx"], read_only=True, data_only=False)
    try:
        sheet = excel.active
        headers = [cell.value for cell in next(sheet.iter_rows())]
        year_column = headers.index("Year")
        selected_column = headers.index(growth_column)
        formula_rows = {
            int(row[year_column].value): {
                "workbook_growth_cell": row[selected_column].coordinate,
                "workbook_growth_formula": row[selected_column].value
                if row[selected_column].data_type == "f" else None,
            }
            for row in sheet.iter_rows(min_row=2) if row[year_column].value is not None
        }
        for year, details in formula_rows.items():
            formula = details["workbook_growth_formula"]
            if formula is not None:
                references = re.fullmatch(r"=\$?([A-Z]+)\$?(\d+)/\$?([A-Z]+)\$?(\d+)", formula)
                if references is None:
                    raise ValueError(f"Unsupported workbook growth formula at {details['workbook_growth_cell']}: {formula}")
                numerator_col, numerator_row, denominator_col, denominator_row = references.groups()
                numerator = float(sheet[f"{numerator_col}{numerator_row}"].value)
                denominator = float(sheet[f"{denominator_col}{denominator_row}"].value)
                denominator_year = int(sheet.cell(row=int(denominator_row), column=year_column + 1).value)
                if denominator_year != baseline_year or denominator <= 0 or not isclose(
                    float(growth.loc[year, growth_column]), numerator / denominator,
                    rel_tol=1e-12, abs_tol=1e-12,
                ):
                    raise ValueError("Workbook cached growth index does not reproduce its observation-anchored formula")
    finally:
        excel.close()
    source_audit = pd.DataFrame()
    source_identity = "not supplied to isolated fixture"
    if control is not None:
        cer_rules = module_rules(control)
        request = next(item for item in build_requests(control) if item.component_id == "macro-indicators")
        macro = normalize_component(read_and_validate_source(request), request,
                                    region_labels=[], rules=cer_rules)
        selected_macro = macro.loc[macro.scenario.eq(control.scenario.demand.cer_scenario)]
        source_audit = selected_macro.pivot(index="year", columns="variable_key", values="value")
        if not {"gdp_deflator", "real_gdp"} <= set(source_audit):
            raise ValueError("Registered CER source lacks independent GDP/deflator evidence")
        if set(growth.index) - set(source_audit.index):
            raise ValueError("Registered CER source does not cover every workbook year")
        if deflator_identity and any(
            not isclose(float(row[deflator_columns[0]]), float(source_audit.loc[year, "gdp_deflator"]),
                        rel_tol=1e-12, abs_tol=1e-12)
            for year, row in growth.iterrows()
        ):
            raise ValueError("Workbook deflator levels conflict with the registered CER edition/scenario")
        source_identity = f"CER {request.edition} / {control.scenario.demand.cer_scenario}; {request.url}"
        source_audit["source_path"] = str(request.cache_path)
        source_audit["source_sha256"] = file_sha256(request.cache_path)
    scenario = metadata["scenario"]
    model_base_year = int(scenario["periods"]["base_year"])
    audit = metadata.get("demand_audit", {})
    required_audit = {"ceud_observed_year", "base_year", "base_year_gdp_index",
                      "observed_ceud_baselines", "gdp_index_by_period"}
    if required_audit - set(audit):
        raise ValueError("Captured demand lacks its native observation anchor audit; regenerate the research report")
    anchor = int(audit["ceud_observed_year"])
    base_year_gdp_index = float(audit["base_year_gdp_index"])
    growth_indices = {int(period): float(value) for period, value in audit["gdp_index_by_period"].items()}
    observed_baselines = pd.DataFrame(audit["observed_ceud_baselines"])
    required_baseline = {"region", "service", "commodity", "ceud_observed_year",
                         "baseline_demand", "gdp_indexed_base_year_demand"}
    if (required_baseline - set(observed_baselines) or int(audit["base_year"]) != model_base_year
            or anchor > model_base_year or not isfinite(base_year_gdp_index) or base_year_gdp_index <= 0
            or any(not isfinite(value) or value <= 0 for value in growth_indices.values())):
        raise ValueError("Captured demand observation/base-year/GDP audit is inconsistent")
    _require_unique(observed_baselines, ["region", "service", "commodity"], "observed CEUD demand baselines")
    comparison = scenario.get("comparison", {})
    tolerance = {"abs_tol": float(comparison.get("absolute_tolerance", 1e-9)),
                 "rel_tol": float(comparison.get("relative_tolerance", 1e-6))}
    step = int(scenario["periods"]["step"])
    if scenario["demand"]["future_car_demand"] != "GDP-indexed":
        raise ValueError("Demand attribution requires the captured GDP-indexed method")
    if (anchor not in growth.index or period_shift != step
            or scenario["periods"].get("period_mode") != "prospective"):
        raise ValueError("Demand recipe and backend have different period interpretations")
    demand_rules = load_harmonization_rules(control, "road_stocks_and_demands")["demand"]
    if len(conversions) != 1 or not isclose(1 / conversions[0], demand_rules["activity_to_billion_factor"]):
        raise ValueError("Notebook and backend activity conversions differ")
    service_by_commodity = {spec["commodity"]: name for name, spec
                            in demand_rules["activity_series"].items()}
    activity = activity.assign(commodity=activity.Mode.map(mode_to_commodity))
    baseline_rows, replay_rows = [], []
    for target in frames["stock_activity_energy"].itertuples(index=False):
        selected = activity.loc[activity.Region.eq(target.ceud_region)
                                & activity.commodity.map(service_by_commodity).eq(target.road_class)]
        if len(selected) != 1:
            raise ValueError(f"Missing or duplicate notebook activity: {target.region}/{target.road_class}")
        row = selected.iloc[0]
        if not str(row.Unit).startswith("M "):
            raise ValueError("Notebook activity must use millions before conversion")
        historical = float(row[baseline_year]) * float(demand_rules["activity_to_billion_factor"])
        if not isfinite(historical) or historical <= 0:
            raise ValueError("Notebook activity baseline must be positive")
        observed = observed_baselines.loc[
            observed_baselines.region.eq(target.region) & observed_baselines.service.eq(target.road_class)
            & observed_baselines.commodity.eq(demand_rules["activity_series"][target.road_class]["commodity"])
        ]
        if len(observed) != 1 or int(observed.iloc[0].ceud_observed_year) != anchor:
            raise ValueError(f"Missing or inconsistent native demand anchor: {target.region}/{target.road_class}")
        observed_activity = float(observed.iloc[0].baseline_demand)
        base_year_activity = float(observed.iloc[0].gdp_indexed_base_year_demand)
        if (not isfinite(observed_activity) or observed_activity <= 0 or not isfinite(base_year_activity)
                or not isclose(base_year_activity, observed_activity * base_year_gdp_index, **tolerance)):
            raise ValueError("Captured demand base-year GDP bridge does not reconcile with its native anchor")
        baseline_rows.append({"region": target.region, "road_class": target.road_class,
                              "notebook_baseline_year": baseline_year,
                              "notebook_activity_bn": historical,
                              "current_baseline_year": model_base_year,
                              "current_anchor_year": anchor, "current_observed_year": anchor,
                              "current_baseline_status": "observed",
                              "current_activity_bn": observed_activity,
                              "baseline_difference_bn": observed_activity - historical,
                              "base_year_gdp_index": base_year_gdp_index,
                              "current_base_year_demand_bn": base_year_activity,
                              "observation_to_base_year_growth_bn": base_year_activity - observed_activity,
                              "historical_calibration_target_bn": target.ceud_activity_bn,
                              "historical_calibration_target_status": target.ceud_activity_status,
                              "legacy_calendar_base_year_demand_bn": historical * float(growth.loc[model_base_year, growth_column]),
                              "legacy_calendar_base_year_model_label": model_base_year - period_shift,
                              "legacy_calendar_base_year_emitted": model_base_year - period_shift in range(*requested_periods[0])})
        for source_year in sorted(source_years & set(growth.index)):
            replay_rows.append({"region": target.region, "road_class": target.road_class,
                                "period": source_year - period_shift,
                                "source_year": source_year,
                                "notebook_baseline_year": baseline_year,
                                "notebook_growth_index": float(growth.loc[source_year, growth_column]),
                                "notebook_recipe_demand_bn": historical * float(growth.loc[source_year, growth_column])})
    replay = pd.DataFrame(replay_rows)
    attribution = frames["demand_comparison"].merge(
        replay, on=["region", "road_class", "period"], how="outer", validate="one_to_one",
    ).merge(pd.DataFrame(baseline_rows).drop(columns="notebook_baseline_year"),
            on=["region", "road_class"], validate="many_to_one")
    attribution["current_source_year"] = attribution.period + step
    attribution["replay_at_current_baseline_bn"] = (
        attribution.notebook_growth_index * attribution.current_activity_bn
    )
    attribution["replay_at_current_anchor_bn"] = (
        attribution.replay_at_current_baseline_bn / float(growth.loc[anchor, growth_column])
    )
    attribution["stored_to_recipe_change_bn"] = attribution.notebook_recipe_demand_bn - attribution.reference
    attribution["baseline_change_bn"] = attribution.replay_at_current_baseline_bn - attribution.notebook_recipe_demand_bn
    attribution["anchor_change_bn"] = attribution.replay_at_current_anchor_bn - attribution.replay_at_current_baseline_bn
    # Both growth factors now use the observed CEUD year as denominator. The
    # model base-year bridge is reported separately and never removes its growth.
    attribution["growth_driver_change_bn"] = attribution.current - attribution.replay_at_current_anchor_bn
    attribution["current_growth_index"] = attribution.period.map(growth_indices)
    current = attribution.loc[attribution.current.notna()]
    if (current.current_growth_index.isna().any() or any(
        not isclose(float(row.current), float(row.current_activity_bn * row.current_growth_index), **tolerance)
        for row in current.itertuples(index=False)
    )):
        raise ValueError("Captured demand rows do not reconcile with observation-anchored real GDP indices")
    attribution["current_base_to_projection_growth_index"] = attribution.current_growth_index / base_year_gdp_index
    attribution["replayed_growth_index_at_current_anchor"] = (
        attribution.notebook_growth_index / float(growth.loc[anchor, growth_column])
    )
    attribution["attribution_residual_bn"] = (attribution.current - attribution.reference) - attribution[
        ["stored_to_recipe_change_bn", "baseline_change_bn", "anchor_change_bn", "growth_driver_change_bn"]
    ].sum(axis=1, min_count=4)
    attribution["replay_relative_percent"] = (attribution.current - attribution.notebook_recipe_demand_bn).div(
        attribution.notebook_recipe_demand_bn.where(attribution.notebook_recipe_demand_bn.ne(0))) * 100
    long_rows = []
    for row in attribution.itertuples(index=False):
        for evidence, value, calendar, index in (
            ("Current backend", row.current, row.current_source_year, row.current_growth_index),
            ("Notebook recipe replay", row.notebook_recipe_demand_bn, row.source_year, row.notebook_growth_index),
            ("Stored legacy extract", row.reference, None, None),
        ):
            if pd.notna(value):
                long_rows.append({"region": row.region, "road_class": row.road_class,
                                  "period": row.period, "calendar_year": calendar,
                                  "evidence": evidence, "demand_bn": value,
                                  "unit": demand_rules["activity_series"][row.road_class]["units"],
                                  "growth_index": index})
    demand_state = frames["reference_state"].set_index("table").loc["Demand"]
    growth_audit_rows = []
    for year, row in growth.iterrows():
        source = source_audit.loc[year] if not source_audit.empty else pd.Series(dtype=object)
        source_deflator = source.get("gdp_deflator")
        source_real_gdp = source.get("real_gdp")
        source_deflator_index = (float(source_deflator) / float(source_audit.loc[baseline_year, "gdp_deflator"])
                                 if pd.notna(source_deflator) else None)
        source_real_index = (float(source_real_gdp) / float(source_audit.loc[baseline_year, "real_gdp"])
                            if pd.notna(source_real_gdp) else None)
        growth_audit_rows.append({
            "calendar_year": int(year), "model_label": int(year) - period_shift,
            "compiler_emits_year": int(year) in source_years,
            "observation_anchor_year": baseline_year,
            "workbook_selected_column": growth_column,
            **formula_rows[int(year)],
            "workbook_growth_index": float(row[growth_column]),
            "workbook_native_series": deflator_columns[0] if deflator_identity else "unresolved",
            "workbook_native_value": float(row[deflator_columns[0]]) if deflator_identity else None,
            "workbook_source_statement": row.get("source"),
            "compiler_demand_notes": " | ".join(project_notes),
            "registered_deflator": source_deflator,
            "registered_deflator_index": source_deflator_index,
            "registered_real_gdp": source_real_gdp,
            "registered_real_gdp_index": source_real_index,
            "growth_index_difference": float(row[growth_column]) - source_real_index
            if source_real_index is not None else None,
            "source_identity": source_identity,
            "source_path": source.get("source_path"), "source_sha256": source.get("source_sha256"),
            "interpretation": "price-index ratio, not real output growth" if deflator_identity else "unresolved",
            "compiler_note_conflict": "project_demand_v notes claim projected national GDP growth; selected values are a price deflator"
            if deflator_identity else "series meaning unresolved",
        })
    method = pd.DataFrame([
        {"evidence": "Supplied SQLite", "baseline": "Captured database rows; independent of recipe replay",
         "growth": f"{int(demand_state.matching_values)}/{int(demand_state.common_keys)} shared demand values match original legacy",
         "calendar": "Stored period labels retained; exact source-year mapping unresolved"},
        {"evidence": "Inspected notebook recipe", "baseline": f"CEUD {baseline_year}",
         "growth": ("Workbook GDP-deflator ratio; formula and raw levels verified against registered CER source"
                    if not source_audit.empty else "Workbook GDP-deflator ratio (verified arithmetically)")
         if deflator_identity else f"Workbook {growth_column}; variable identity unresolved",
         "calendar": f"Workbook year = stored period + {period_shift}"},
        {"evidence": "Current backend", "baseline": f"Native CEUD observation {anchor}; stock-tail policy does not set demand",
         "growth": f"Captured CER real GDP / GDP({anchor}); GDP({model_base_year}) / GDP({anchor}) = {base_year_gdp_index:.6f}; emitted rows reconciled",
         "calendar": f"Projection year = start-labelled period + {step}"},
    ])
    return {"demand_recipe_replay": replay, "demand_baseline_comparison": pd.DataFrame(baseline_rows),
            "demand_attribution": attribution.sort_values(["region", "road_class", "period"]),
            "demand_method_comparison": pd.DataFrame(long_rows), "demand_method_context": method,
            "demand_growth_series_audit": pd.DataFrame(growth_audit_rows),
            "demand_baseline_service_targets": pd.DataFrame(baseline_rows)[[
                "region", "road_class", "notebook_baseline_year", "notebook_activity_bn", "current_baseline_year",
                "legacy_calendar_base_year_demand_bn", "legacy_calendar_base_year_model_label",
                "legacy_calendar_base_year_emitted", "current_base_year_demand_bn", "historical_calibration_target_bn",
            ]],
            "demand_base_year_bridge": pd.DataFrame(baseline_rows)[[
                "region", "road_class", "current_anchor_year", "current_baseline_year", "current_activity_bn",
                "base_year_gdp_index", "current_base_year_demand_bn", "observation_to_base_year_growth_bn",
                "historical_calibration_target_bn", "historical_calibration_target_status",
            ]]}


def _literal_reference_stock(result, bundle, rules):
    """Replay only the inspected notebook's raw acquisition-capacity arithmetic.

    Survival, backend stock anchors and backend allocation are intentionally absent:
    the notebook's existing-capacity function does not apply any of them. Returned
    annual contributions explain recipe parameters, never the supplied SQLite.
    """
    metadata = result["fleet_metadata"]["notebook"]
    directory = resolve_artifact_path(bundle, rules.reference_route)
    notebook_path = directory / rules.reference_notebook
    if not metadata.get("sha256") or file_sha256(notebook_path) != metadata["sha256"]:
        raise ValueError("Reference notebook changed since its evidence capture")
    sources = result["fleet"]["notebook_source_cells"]
    if {"source", "source_sha256"} - set(sources):
        raise ValueError("Captured reference recipe lacks its source fingerprints")
    for row in sources.itertuples(index=False):
        if hashlib.sha256(row.source.encode()).hexdigest() != row.source_sha256:
            raise ValueError("Captured reference recipe source fingerprint differs")
    nodes = [node for source in sources.source for node in ast.parse(source).body]
    functions = [node for node in nodes if isinstance(node, ast.FunctionDef)
                 and node.name == "compile_data_existing_capacity"]
    # Fingerprint the complete inspected AST, including product, missing-value,
    # interpolation, native-bin and technology-map semantics. Whitespace and
    # comments are immaterial; a changed recipe requires inspection, not guessing.
    inspected_ast_sha256 = "29d0772947c90ebe6d931b2c2ab8d9652d70dee1692bc7ed06a6e6021844fbec"
    if len(functions) != 1 or hashlib.sha256(
        ast.dump(functions[0], include_attributes=False).encode()
    ).hexdigest() != inspected_ast_sha256:
        raise ValueError("Reference existing-capacity recipe changed; inspect it before replay")
    stock_function = functions[0]
    # Final CSV membership matters as much as the local stock calculation. The
    # inspected main path shifts only old default rows, excludes their five road
    # families, appends new recipe rows and exports them without a later shift.
    path_fingerprints = {
        "main_compile_data_transport": "2c975dd610cea165674604d297fbd7301286179a58adeff0939f70efb3a09d71",
        "shift_time_period": "c55c6f7c94345d78b00c4a0cb97c3a2a44bccfae687143c320ce9b0687230ca3",
        "remove_data_noexca": "f4d94fbd49a22168ad922b5afa954a976d2064e760db3f50c57c9b44f8097538",
    }
    for name, expected in path_fingerprints.items():
        candidates = [node for node in nodes if isinstance(node, ast.FunctionDef) and node.name == name]
        if len(candidates) != 1 or hashlib.sha256(
            ast.dump(candidates[0], include_attributes=False).encode()
        ).hexdigest() != expected:
            raise ValueError(f"Reference final stock-export path changed: {name}; inspect before replay")
    calls = [node for root in nodes for node in ast.walk(root)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
             and node.func.id == "main_compile_data_transport"]
    if len(calls) != 1:
        raise ValueError("Reference notebook does not have one unambiguous main call")
    keywords = {item.arg: item.value for item in calls[0].keywords}
    requested = keywords.get("range_vin")
    if (not isinstance(requested, ast.Call) or not isinstance(requested.func, ast.Attribute)
            or not isinstance(requested.func.value, ast.Name) or requested.func.value.id != "np"
            or requested.func.attr != "arange" or requested.keywords):
        raise ValueError("Reference annual-cohort selection changed")
    arguments = [ast.literal_eval(argument) for argument in requested.args]
    if arguments != [2005, 2026, 1]:
        raise ValueError("Reference annual-cohort support changed; inspect its native bins")
    fixed_flag = keywords.get("fixed_lifetime")
    if not isinstance(fixed_flag, ast.Constant) or not isinstance(fixed_flag.value, bool):
        raise ValueError("Reference fixed-lifetime flag is not explicit")
    requested_years = list(range(*arguments))

    def native_map(name):
        values = [node.value for node in ast.walk(stock_function)
                  if isinstance(node, ast.Assign) and any(
                      isinstance(target, ast.Name) and target.id == name for target in node.targets)]
        if len(values) != 1:
            raise ValueError(f"Reference stock recipe lacks its explicit {name}")
        return ast.literal_eval(values[0])

    mode_prefixes, powertrain_suffixes = native_map("map_mode"), native_map("map_p")
    configured = resolved_road_lifetime_rules(bundle)["fuel_technology"]
    mode_classes = {}
    for mode, prefix in mode_prefixes.items():
        owners = [name for name, mapping in configured.items()
                  if mapping and all(tech.startswith(prefix) for tech in mapping.values())]
        if len(owners) != 1:
            raise ValueError(f"Reference native class cannot be mapped uniquely: {mode}")
        mode_classes[mode] = owners[0]

    identities = []

    def workbook(filename):
        records = [item for item in metadata["dependencies"]
                   if Path(item["path"]).name == filename]
        path = directory / filename
        if len(records) != 1 or not records[0].get("provided_sha256"):
            raise ValueError(f"Captured reference workbook identity is absent: {filename}")
        if file_sha256(path) != records[0]["provided_sha256"]:
            raise ValueError(f"Captured reference workbook changed: {filename}")
        identities.append({"evidence": filename, "method": "Exact captured source bytes",
                           "interpretation": records[0]["provided_sha256"]})
        return pd.read_excel(path)

    size, powertrain = workbook("vehicle_sales_by_size.xlsx"), workbook("vehicle_sales_by_powertrain.xlsx")
    if set(size.Unit) != {"k units"} or size.duplicated(["Region", "Mode"]).any():
        raise ValueError("Reference class-sales keys or thousand-vehicle units are invalid")
    if powertrain.duplicated(["Region", "Mode", "Powertrain"]).any():
        raise ValueError("Reference powertrain-share keys are duplicated")
    # Preserve exactly the literal function's order: multiply, fill missing
    # products with zero, then interpolate each powertrain's product to the tail.
    sizes = size.melt(id_vars=["Region", "Mode", "Unit"], var_name="Year", value_name="total_sales")
    shares = powertrain.melt(id_vars=["Region", "Mode", "Powertrain"],
                            var_name="Year", value_name="market_share")
    for frame, column in ((sizes, "total_sales"), (shares, "market_share")):
        frame["Year"] = pd.to_numeric(frame.Year, errors="raise").astype(int)
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        observed = frame[column].dropna()
        if not observed.map(isfinite).all() or observed.lt(0).any():
            raise ValueError("Reference sales or shares have inadmissible observed values")
    if shares.market_share.dropna().gt(1).any():
        raise ValueError("Reference market shares must be fractions, not percentages")
    products = sizes.merge(shares, on=["Region", "Mode", "Year"], how="left")
    products["sales"] = products.total_sales * products.market_share
    products["observation_status"] = products.sales.map(
        lambda value: "missing product filled with zero" if pd.isna(value) else "observed sales times share"
    )
    products["sales"] = products.sales.fillna(0)
    grid = pd.DataFrame({"Year": requested_years}).merge(
        products[["Region", "Mode", "Unit", "Powertrain"]].drop_duplicates(), how="cross",
    )
    annual = products.merge(grid, on=["Year", "Region", "Mode", "Unit", "Powertrain"], how="right")
    annual = annual.sort_values(["Region", "Mode", "Powertrain", "Year"])
    annual["sales"] = annual.groupby(["Region", "Mode", "Powertrain"], group_keys=False).sales.transform(
        lambda group: group.interpolate(method="linear", limit_direction="forward")
    )
    annual["observation_status"] = annual.observation_status.fillna("forward-extended powertrain product")
    annual["vintage"] = annual.Year.map(
        lambda year: 2000 if year == 2005 else 2005 if year <= 2010 else 2010
        if year <= 2015 else 2015 if year <= 2020 else 2020
    )
    native_keys = ["Region", "Mode", "Powertrain", "vintage", "Unit"]
    parameters = annual.groupby(native_keys, as_index=False).sales.sum()
    parameters = parameters.loc[parameters.sales.gt(0)].copy()

    def label(frame):
        labelled = frame.copy()
        labelled["tech_p"] = labelled.Powertrain.map(powertrain_suffixes)
        labelled.loc[labelled.Mode.eq("Cars") & labelled.Powertrain.eq("Battery electric"), "tech_p"] = "BEV150"
        labelled.loc[labelled.Mode.eq("Light Trucks Passengers") & labelled.Powertrain.eq("Battery electric"), "tech_p"] = "BEV300"
        labelled["tech"] = labelled.Mode.map(mode_prefixes) + labelled.tech_p
        labelled["road_class"] = labelled.Mode.map(mode_classes)
        return labelled.rename(columns={"Region": "region", "Powertrain": "powertrain",
                                         "Year": "vintage_year", "sales": "raw_capacity_k_vehicles"})

    parameters = label(parameters)
    if parameters[["tech", "road_class"]].isna().any().any():
        raise ValueError("Positive reference recipe stock has an unmapped native technology")
    annual = label(annual)
    # The literal groupby drops unmapped/null-powertrain groups. Restrict annual
    # details to the positive native groups actually emitted by that operation.
    annual = annual.merge(parameters[["region", "powertrain", "vintage", "tech"]],
                          on=["region", "powertrain", "vintage", "tech"], how="inner", validate="many_to_one")
    annual = annual.loc[annual.region.isin(bundle.scenario.geography.regions)].copy()
    parameters = parameters.loc[parameters.region.isin(bundle.scenario.geography.regions)].copy()
    parameters = parameters.rename(columns={"raw_capacity_k_vehicles": "capacity_k_vehicles"})
    parameters["units"] = parameters.Unit
    parameters["evidence"] = "Sales variant notebook recipe"
    annual["evidence"] = "Sales variant notebook recipe"
    keys = ["region", "tech", "vintage"]
    if parameters.duplicated(keys).any():
        raise ValueError("Literal reference recipe emits duplicate native stock keys")
    recovered = annual.groupby(keys).raw_capacity_k_vehicles.sum()
    if not all(isclose(recovered.loc[(row.region, row.tech, row.vintage)], row.capacity_k_vehicles,
                       rel_tol=1e-12, abs_tol=1e-9) for row in parameters.itertuples()):
        raise ValueError("Literal annual contributions do not recover emitted recipe stock")
    context = pd.DataFrame([
        {"evidence": "Literal existing-capacity recipe",
         "method": "Raw annual sales × source powertrain fraction; missing products become zero; forward interpolate each product through 2025",
         "interpretation": "No survival, stock normalization or backend fuel allocation is applied to these capacity rows"},
        {"evidence": "Native recipe bins",
         "method": "2005→2000; 2006–10→2005; 2011–15→2010; 2016–20→2015; 2021–25→2020",
         "interpretation": "Annual details decompose the literal recipe only, not supplied SQLite parameters"},
        {"evidence": "Survival flag in source call", "method": f"fixed_lifetime={fixed_flag.value}",
         "interpretation": "Survival is prepared downstream; this flag does not change existing-capacity arithmetic"},
        {"evidence": "Stored output conflict",
         "method": str(metadata.get("source_output_conflict", "not captured")),
         "interpretation": "Source inspection cannot establish which earlier execution produced stored outputs or SQLite rows"},
        {"evidence": "Final existing-capacity CSV path",
         "method": "Shift original defaults; exclude all five original road families; concatenate untouched recipe road rows with remaining non-road defaults",
         "interpretation": "No later road-capacity shift, truncation, retirement or append of original road rows occurs before ExistingCapacity_transport.csv or data_update['ExistingCapacity']"},
        *identities,
    ])
    annual_columns = ["region", "road_class", "powertrain", "vintage_year", "vintage", "tech",
                      "raw_capacity_k_vehicles", "observation_status", "evidence"]
    parameter_columns = ["region", "road_class", "powertrain", "tech", "vintage", "capacity_k_vehicles", "units", "evidence"]
    source_cell = [row.cell_index for row in sources.itertuples(index=False)
                   if any(isinstance(node, ast.FunctionDef) and node.name == "compile_data_existing_capacity"
                          for node in ast.parse(row.source).body)]
    main_cell = [row.cell_index for row in sources.itertuples(index=False)
                 if any(isinstance(node, ast.FunctionDef) and node.name == "main_compile_data_transport"
                        for node in ast.parse(row.source).body)]
    steps = pd.DataFrame([
        {"step": 1, "source_cell": source_cell[0], "operation": "Melt class sales and powertrain shares, then left join region/mode/year",
         "interpretation": "Workbook class sales are thousand vehicles; powertrain shares are fractions"},
        {"step": 2, "source_cell": source_cell[0], "operation": "Multiply sales by share; fill missing products with zero",
         "interpretation": "Preserves literal missing-share handling; no share normalization or substitute weights"},
        {"step": 3, "source_cell": source_cell[0], "operation": "Right join requested 2005–25 support; forward interpolate each powertrain's product",
         "interpretation": "2024–25 retain the 2023 powertrain-level product, rather than a newly allocated class total"},
        {"step": 4, "source_cell": source_cell[0], "operation": "Assign native five-year bins; 2005 alone belongs to vintage 2000",
         "interpretation": "Input columns 2000–04 are excluded by the actual main call's range_vin"},
        {"step": 5, "source_cell": source_cell[0], "operation": "Sum by native region/mode/powertrain/vintage/unit; remove zero totals",
         "interpretation": "Existing capacities contain acquisition volumes without pre-applied survival"},
        {"step": 6, "source_cell": source_cell[0], "operation": "Apply native unsuffixed technology maps, including car BEV150, passenger-LT BEV300 and PHEV35",
         "interpretation": "Separate explicit alias bridges may compare stored SQLite suffixes; no numerical values change"},
        {"step": 7, "source_cell": main_cell[0], "operation": "Replace all five old road families with untouched recipe rows; export ExistingCapacity_transport.csv",
         "interpretation": "The earlier five-year shift acts on old defaults; new road rows undergo no later shift, drop or survival adjustment. The notebook contains no active SQLite insertion path"},
    ])
    return {"reference_recipe_annual": annual[annual_columns].reset_index(drop=True),
            "reference_recipe_parameters": parameters[parameter_columns].reset_index(drop=True),
            "reference_recipe_context": context, "reference_recipe_steps": steps}


def _read_vehicle_capacity(path: Path, services: dict[str, str]) -> pd.DataFrame:
    """Read physical vehicle counts using each SQLite's own service owners."""
    with closing(open_sqlite_readonly(path)) as connection:
        names = {row[0].lower(): row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        capacity_table = names.get("existing_capacity", names.get("existingcapacity"))
        if capacity_table is None or "efficiency" not in names:
            raise ValueError(f"SQLite lacks existing-capacity/service ownership tables: {path}")
        stock = pd.read_sql_query(
            f"SELECT region,tech,vintage,capacity,units FROM {quote_identifier(capacity_table)}", connection,
        )
        owners = pd.read_sql_query(
            f"SELECT DISTINCT region,tech,output_comm FROM {quote_identifier(names['efficiency'])}", connection,
        )
    owners = owners.loc[owners.output_comm.isin(services)].copy()
    owners["road_class"] = owners.output_comm.map(services)
    owners = owners[["region", "tech", "road_class"]].drop_duplicates()
    _require_unique(owners, ["region", "tech"], "SQLite road service ownership")
    stock = stock.merge(owners, on=["region", "tech"], how="inner", validate="many_to_one")
    _require_unique(stock, ["region", "tech", "vintage"], "SQLite vehicle capacity")
    if not stock.units.map(lambda value: str(value).strip("() ")).isin({"k vehicles", "k units"}).all():
        raise ValueError("SQLite vehicle capacity must use thousand vehicles")
    stock["capacity"] = pd.to_numeric(stock.capacity, errors="raise")
    if not stock.capacity.map(isfinite).all() or stock.capacity.lt(0).any():
        raise ValueError("SQLite vehicle capacity must be finite and nonnegative")
    return stock.sort_values(["region", "road_class", "tech", "vintage"]).reset_index(drop=True)


def _stock_powertrains(stock: pd.DataFrame, fuel_technology: dict) -> pd.Series:
    """Group configured powertrains without discarding native range aliases."""
    import re

    def identity(tech):
        return re.sub(r"(?:_EX|_N)$", "", re.sub(r"(BEV|PHEV)\d+(?=_|$)", r"\1", tech))

    owners = {(road_class, identity(tech)): fuel for road_class, fuels in fuel_technology.items()
              for fuel, tech in fuels.items()}
    result = stock.apply(lambda row: owners.get((row.road_class, identity(row.tech))), axis=1)
    if result.isna().any():
        unknown = stock.loc[result.isna(), ["road_class", "tech"]].drop_duplicates().to_dict("records")
        raise ValueError(f"SQLite powertrain lacks an explicit configured identity: {unknown}")
    labels = {"Gasoline": "Gasoline", "Diesel": "Diesel", "Battery electric": "BEV",
              "Hybrid electric": "HEV", "Plug-in hybrid electric": "PHEV",
              "Natural gas": "CNG", "Propane": "LPG"}
    return result.map(lambda value: labels.get(value, "Other"))


def _reference_stock_timing(path: Path, stock: pd.DataFrame) -> pd.DataFrame:
    """Expose stored lifetime evidence without applying a retirement screen."""
    keys = ["region", "tech"]
    represented = stock[[*keys, "road_class"]].drop_duplicates()
    with closing(open_sqlite_readonly(path)) as connection:
        names = {row[0].lower(): row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        table = names.get("lifetime_tech", names.get("lifetimetech"))
        life = (pd.read_sql_query(f"SELECT region,tech,lifetime FROM {quote_identifier(table)}", connection)
                if table else pd.DataFrame(columns=[*keys, "lifetime"]))
        curves = names.get("lifetime_survival_curve", names.get("lifetimesurvivalcurve"))
        curve_rows = connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(curves)}").fetchone()[0] if curves else None
    _require_unique(life, keys, "stored technology lifetimes")
    if not life.lifetime.dropna().map(isfinite).all() or life.lifetime.dropna().le(0).any():
        raise ValueError("Stored technology lifetimes are inadmissible")
    joined = represented.merge(life, on=keys, how="left", validate="one_to_one")
    context = joined.groupby(["region", "road_class"], as_index=False).agg(
        minimum_fixed_lifetime_years=("lifetime", "min"), maximum_fixed_lifetime_years=("lifetime", "max"),
        represented_technologies=("tech", "nunique"), missing_lifetime_technologies=("lifetime", lambda values: values.isna().sum()),
    )
    context["survival_table_rows"] = curve_rows
    context["base_year_status"] = "not established by stored parameters; native vintage labels retained"
    context["interpretation"] = "Stored fixed lifetimes only; capacities are not retired or normalized by this diagnostic"
    return context


def deepen_reference_cohorts(
    result: dict[str, Any], bundle: ConfigBundle, rules: TransportationResearchRules,
) -> dict[str, pd.DataFrame]:
    """Compare actual stored capacity and a separately labelled literal recipe.

    No stock target, survival, fuel share, eligibility or annual allocation from
    the current backend is applied to the supplied reference population.
    """
    frames, metadata = result["fleet"], result["fleet_metadata"]
    if metadata["scenario"]["scenario"]["name"] != bundle.scenario.scenario.name:
        raise ValueError("Reference comparison needs the selected build's scenario")
    if metadata["scenario"]["periods"] != bundle.scenario.periods.model_dump(mode="json"):
        raise ValueError("Reference comparison needs the captured period configuration")
    directory = resolve_artifact_path(bundle, rules.reference_route)
    paths = {"Backend SQLite": Path(metadata["files"]["database"]["path"]),
             "Sales variant SQLite": directory / rules.reference_sqlite}
    identities = {"Backend SQLite": metadata["files"]["database"],
                  "Sales variant SQLite": metadata["reference_state"]["reference"]}
    for evidence, path in paths.items():
        if not path.is_file() or file_sha256(path) != identities[evidence]["sha256"]:
            raise ValueError(f"Captured {evidence} identity changed: {path}")
    notebook = directory / rules.reference_notebook
    if not notebook.is_file() or file_sha256(notebook) != metadata["notebook"]["sha256"]:
        raise ValueError("Captured reference notebook identity changed")
    demand = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    summary = frames["stock_activity_energy"]
    class_keys = ["region", "road_class"]
    _require_unique(summary, class_keys, "CEUD class stock targets")
    services = {item["commodity"]: name for name, item in demand["activity_series"].items()
                if name in set(summary.road_class)}
    fuel_technology = resolved_road_lifetime_rules(bundle)["fuel_technology"]
    stocks = []
    for evidence, path in paths.items():
        stock = _read_vehicle_capacity(path, services)
        stock = stock.merge(summary[class_keys], on=class_keys, how="inner", validate="many_to_one")
        stock["powertrain"] = _stock_powertrains(stock, fuel_technology)
        stock["evidence"] = evidence
        stock = stock.rename(columns={"capacity": "capacity_k_vehicles"})
        # Reconcile to the existing complete class/vintage comparison. The older
        # exact-current-tech capture omitted range-specific BEV and PHEV aliases.
        keys = [*class_keys, "vintage"]
        actual = stock.groupby(keys, as_index=False).capacity_k_vehicles.sum()
        column = "current" if evidence == "Backend SQLite" else "reference"
        expected = frames["capacity_comparison"].loc[frames["capacity_comparison"][column].notna(), keys + [column]]
        check = actual.merge(expected, on=keys, how="outer", validate="one_to_one", indicator=True)
        if not check._merge.eq("both").all() or not all(
            isclose(row.capacity_k_vehicles, getattr(row, column), rel_tol=1e-10, abs_tol=1e-7)
            for row in check.itertuples(index=False)
        ):
            raise ValueError(f"Complete {evidence} population differs from captured capacity comparison")
        stocks.append(stock)
    parameters = pd.concat(stocks, ignore_index=True)[[
        "region", "road_class", "tech", "vintage", "evidence", "capacity_k_vehicles", "powertrain", "units",
    ]]
    totals = parameters.groupby(class_keys + ["evidence"], as_index=False).agg(
        capacity_k_vehicles=("capacity_k_vehicles", "sum"), technology_count=("tech", "nunique"),
        vintage_count=("vintage", "nunique"),
    ).merge(summary[class_keys + ["base_year", "ceud_stock_k_vehicles"]],
            on=class_keys, how="outer", validate="many_to_one")
    totals["stock_gap_k_vehicles"] = totals.capacity_k_vehicles - totals.ceud_stock_k_vehicles
    totals["stock_gap_percent"] = 100 * totals.stock_gap_k_vehicles / totals.ceud_stock_k_vehicles.where(totals.ceud_stock_k_vehicles.ne(0))
    recipe = _literal_reference_stock(result, bundle, rules)
    native_recipe = recipe["reference_recipe_parameters"].copy()
    stored = parameters.loc[parameters.evidence.eq("Sales variant SQLite")].copy()
    for frame in (stored, native_recipe):
        frame["comparison_tech"] = frame.tech.str.replace(r"(?:_EX|_N)$", "", regex=True)
    keys = [*class_keys, "comparison_tech", "vintage"]
    comparison = stored[keys + ["capacity_k_vehicles"]].rename(columns={"capacity_k_vehicles": "stored_capacity_k_vehicles"}).merge(
        native_recipe[keys + ["capacity_k_vehicles"]].rename(columns={"capacity_k_vehicles": "recipe_capacity_k_vehicles"}),
        on=keys, how="outer", validate="one_to_one", indicator="key_status",
    )
    comparison["recipe_minus_stored_k_vehicles"] = comparison.recipe_capacity_k_vehicles - comparison.stored_capacity_k_vehicles
    match_tolerance = {"rel_tol": bundle.scenario.comparison.relative_tolerance,
                       "abs_tol": bundle.scenario.comparison.absolute_tolerance}
    comparison["value_match"] = [
        isclose(left, right, **match_tolerance) if pd.notna(left) and pd.notna(right) else False
        for left, right in zip(comparison.stored_capacity_k_vehicles, comparison.recipe_capacity_k_vehicles, strict=True)
    ]
    recipe_totals = comparison.groupby(class_keys, as_index=False).agg(
        stored_capacity_k_vehicles=("stored_capacity_k_vehicles", lambda values: values.sum(min_count=1)),
        recipe_capacity_k_vehicles=("recipe_capacity_k_vehicles", lambda values: values.sum(min_count=1)),
        common_keys=("key_status", lambda values: values.eq("both").sum()),
        matching_keys=("value_match", "sum"),
        stored_only_keys=("key_status", lambda values: values.eq("left_only").sum()),
        recipe_only_keys=("key_status", lambda values: values.eq("right_only").sum()),
    )
    recipe_totals["recipe_minus_stored_k_vehicles"] = recipe_totals.recipe_capacity_k_vehicles - recipe_totals.stored_capacity_k_vehicles
    recipe_totals["recipe_difference_percent"] = 100 * recipe_totals.recipe_minus_stored_k_vehicles / recipe_totals.stored_capacity_k_vehicles.where(recipe_totals.stored_capacity_k_vehicles.ne(0))
    context = pd.DataFrame([
        {"evidence": "Backend SQLite", "interpretation": "Actual ExistingCapacity rows at native vintage; class ownership from this database's Efficiency outputs"},
        {"evidence": "Sales variant SQLite", "interpretation": "Actual stored ExistingCapacity, including BEV/PHEV range aliases; no normalization or annual decomposition"},
        {"evidence": "Sales variant notebook recipe", "interpretation": "Literal workbook calculation inspected independently; not proof of the supplied SQLite's generation history"},
        {"evidence": "Recipe-to-SQLite key bridge", "interpretation": "Only terminal _EX/_N suffixes are removed for key comparison; values and native vintage labels remain unchanged"},
        {"evidence": "Recipe match tolerance", "interpretation": f"Scenario relative tolerance {match_tolerance['rel_tol']}; absolute tolerance {match_tolerance['abs_tol']} thousand vehicles"},
        {"evidence": "Timing", "interpretation": "A common base-year fleet state is not established for the stored reference; no retirement screen is applied to its capacity values"},
    ])
    return {**recipe, "built_stock_parameters": parameters, "built_stock_totals": totals,
            "reference_recipe_comparison": comparison, "reference_recipe_totals": recipe_totals,
            "reference_replay_context": context,
            "reference_timing_context": _reference_stock_timing(paths["Sales variant SQLite"], stored)}


def harmonized_charging_costs(comparisons: pd.DataFrame) -> pd.DataFrame:
    """Express both cost rates in million target-year CAD/GW on one throughput basis.

    The captured backend rates already use million CAD/GW. The reference rates
    use million CAD/(PJ/year), so multiply by the captured C2A × utilization.
    This retains the report's conditional throughput interpretation and leaves
    absent cost-year bridges missing. Parallel reference technologies are rates,
    not additive investments; only demonstrably equal rates may collapse.
    """
    selected = comparisons.loc[
        comparisons.denominator.eq("PJ/year_at_backend_utilization")
    ].copy()
    keys = ["table", "charger_class", "region", "reference_vintage", "reference_period"]
    rates = ["reference_million_target_cad", "backend_cost_per_gw", "backend_denominator_per_gw",
             "backend_utilization_target", "backend_vintage", "backend_cost_source_year", "target_dollar_year", "scope_status"]
    for _, group in selected.groupby(keys, dropna=False):
        if len(group[rates].drop_duplicates()) != 1:
            raise ValueError("Different charging rates cannot be collapsed across reference technologies")
    selected = selected.drop_duplicates(keys).reset_index(drop=True)
    for name in ("reference_million_target_cad", "backend_cost_per_gw", "backend_denominator_per_gw"):
        values = selected[name].dropna()
        if not values.map(isfinite).all() or values.lt(0).any():
            raise ValueError("Harmonized charging costs contain inadmissible rates")
    valid = (selected.backend_denominator_per_gw.gt(0)
             & selected.backend_utilization_target.between(0, 1, inclusive="right"))
    selected["reference_million_cad_per_gw"] = (
        selected.reference_million_target_cad * selected.backend_denominator_per_gw
    ).where(valid)
    selected["backend_million_cad_per_gw"] = selected.backend_cost_per_gw
    selected["harmonized_relative_percent"] = 100 * (
        selected.backend_million_cad_per_gw / selected.reference_million_cad_per_gw.where(
            selected.reference_million_cad_per_gw.ne(0)
        ) - 1
    )
    selected["harmonization_status"] = selected.reference_million_cad_per_gw.map(
        lambda value: "missing cost-year bridge or utilization" if pd.isna(value) else "conditional effective-output basis"
    )
    return selected


def load_validation_diagnosis(*, repo_root: Path | None = None) -> dict[str, Any]:
    """Bind both captured comparisons to the configured live validation build."""
    control = load_config_bundle("config/scenarios/default.yaml", repo_root=repo_root)
    rules = TransportationResearchRules.model_validate(
        load_harmonization_rules(control, "transportation_research")
    )
    control = load_config_bundle(rules.notebook_scenario, repo_root=control.repo_root)
    report_directory = resolve_artifact_path(
        control, rules.report_route, control.scenario.scenario.name,
    )
    charging_rules = load_harmonization_rules(control, "charging_cost_comparison")
    locations = {
        "fleet": report_directory / "manifest.json",
        "charging": resolve_artifact_path(control, charging_rules["report_route"], "snapshot.json"),
    }
    result: dict[str, Any] = {"significance_percent": rules.comparison_significance_percent,
                              "scenario_path": rules.notebook_scenario}
    for label, path in locations.items():
        metadata, frames = _load_captured_report(path)
        result[label] = frames
        result[f"{label}_metadata"] = metadata
    fleet_metadata = result["fleet_metadata"]
    charging_metadata = result["charging_metadata"]
    if fleet_metadata["scenario"] != control.scenario.model_dump(mode="json"):
        raise ValueError("Fleet capture differs from the live validation scenario; rebuild and recapture")
    if charging_metadata["scenario"] != control.scenario.scenario.name:
        raise ValueError("Charging capture belongs to a different validation scenario")
    database = resolve_configured_path(control, "outputs", "sqlite", control.scenario.outputs.sqlite_name)
    build_path = resolve_repo_path(control.repo_root, control.scenario.outputs.validation_report)
    build = json.loads(build_path.read_text(encoding="utf-8"))
    _verify_build(control, database, build)
    for metadata in (fleet_metadata, charging_metadata):
        for name, path in (("database", database), ("build_report", build_path)):
            captured = metadata["files"][name]
            if (Path(captured["path"]).resolve() != path.resolve()
                    or captured["sha256"] != file_sha256(path)):
                raise ValueError(f"Comparison {name} differs from the live validation build; recapture both reports")
    required = {"capacity_comparison", "utilization_comparison", "demand_comparison"}
    if required - set(result["fleet"]):
        raise ValueError("The configured fleet comparison predates the direct parameter comparison. Regenerate its report.")
    result["fleet"].update(deepen_fleet_diagnosis(result["fleet"], result["fleet_metadata"]))
    result["fleet"].update(deepen_reference_cohorts(result, control, rules))
    result["fleet"].update(deepen_demand_diagnosis(result, control, rules))
    result["fleet"].update(reconstruct_comparison(result, control, rules))
    result["charging"]["harmonized_costs"] = harmonized_charging_costs(result["charging"]["comparisons"])
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
