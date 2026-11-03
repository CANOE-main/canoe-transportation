"""Read-only, unit-aware comparison of supplied charging costs and a built scenario.

Missing reference metadata stays unresolved. Explicit conditional interpretations are
sensitivities, never evidence that the supplied numbers have those units. This report
does not prepare production parameters or modify its reference files or database.
"""

import argparse
from contextlib import closing
from dataclasses import asdict, dataclass
import json
import logging
from math import isclose, isfinite
from pathlib import Path
import subprocess
from typing import Any, Literal, Self

import pandas as pd
from pydantic import Field, model_validator

from parameterization.currency import CerCurrencyConverter
from parameterization.ev_chargers import _manual_value, _period_value
from parameterization.manual_parameters import validate_manual_registry
from utils import (
    ConfigBundle, file_sha256, load_config_bundle, load_harmonization_rules,
    resolve_artifact_path, resolve_configured_path, resolve_input_path,
    resolve_repo_path, write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import MappingModel, ScenarioPeriods
from validation.sqlite_utils import open_sqlite_readonly, quote_identifier


LOGGER = logging.getLogger(__name__)
COST_TABLES = ("cost_invest", "cost_fixed", "cost_variable")


class ReferenceCostBasis(MappingModel):
    name: str = Field(min_length=1)
    currency: Literal["CAD", "USD"]
    dollar_year: int = Field(ge=1900)
    denominator: Literal["GW", "PJ/year_capacity", "PJ/year_at_backend_utilization"] | None = None
    evidence: str = Field(min_length=1)


class ChargerGroup(MappingModel):
    reference_technologies: list[str] = Field(min_length=1)
    existing: str = Field(min_length=1)
    new: str = Field(min_length=1)


class ChargingComparisonRules(MappingModel):
    reference_route: str
    report_route: str
    files: dict[str, str]
    groups: dict[str, ChargerGroup]
    reference_basis: ReferenceCostBasis | None = None
    conditional_bases: list[ReferenceCostBasis] = Field(default_factory=list)

    @model_validator(mode="after")
    def unambiguous_scope(self) -> Self:
        if set(self.files) != set(COST_TABLES):
            raise ValueError("Charging comparison requires all three cost file declarations")
        if not self.groups:
            raise ValueError("Charging comparison requires technology groups")
        refs = [tech for group in self.groups.values() for tech in group.reference_technologies]
        backend = [tech for group in self.groups.values() for tech in (group.existing, group.new)]
        if len(set(refs)) != len(refs) or len(set(backend)) != len(backend):
            raise ValueError("Charger technology groups must be disjoint")
        names = [basis.name for basis in self.conditional_bases]
        if any(basis.denominator is None for basis in self.conditional_bases):
            raise ValueError("Conditional charging cost bases require an explicit denominator")
        if self.reference_basis:
            names.append(self.reference_basis.name)
        if len(names) != len(set(names)):
            raise ValueError("Charging comparison basis names must be unique")
        return self


@dataclass(frozen=True)
class ChargingCostComparison:
    frames: dict[str, pd.DataFrame]
    metadata: dict[str, Any]
    destination: Path


def _validate_cost_frame(frame: pd.DataFrame, table: str) -> pd.DataFrame:
    keys = ["region", "tech", "vintage", *(["period"] if table != "cost_invest" else [])]
    if set([*keys, "cost"]) - set(frame):
        raise ValueError(f"{table} lacks required cost/key columns")
    result = frame.copy()
    if result[keys].isna().any().any() or result[keys].eq("").any().any():
        raise ValueError(f"{table} has blank keys")
    for column in ["vintage", *(["period"] if table != "cost_invest" else [])]:
        numeric = pd.to_numeric(result[column], errors="raise")
        if not numeric.map(isfinite).all() or not numeric.mod(1).eq(0).all():
            raise ValueError(f"{table} has non-integral {column}")
        result[column] = numeric.astype(int)
    result["cost"] = pd.to_numeric(result.cost, errors="raise")
    if not result.cost.map(isfinite).all() or result.cost.lt(0).any():
        raise ValueError(f"{table} has non-finite or negative costs")
    if result.duplicated(keys).any():
        raise ValueError(f"{table} has duplicate cost keys")
    return result


def read_reference_costs(
    directory: Path, rules: ChargingComparisonRules,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Retain original columns and logical CSV row numbers for the mapped subset."""
    owners = {tech for group in rules.groups.values() for tech in group.reference_technologies}
    frames, files = {}, {}
    for table, filename in rules.files.items():
        path = directory / filename
        all_rows = pd.read_csv(path, dtype=str, keep_default_na=False)
        if {"tech", "cost"} - set(all_rows):
            raise ValueError(f"Reference {filename} lacks technology or cost columns")
        selected = all_rows.loc[all_rows.tech.isin(owners)].copy()
        selected["reference_csv_row"] = selected.index + 2
        selected["reference_file"] = filename
        selected["original_cost_text"] = selected["cost"]
        selected["table"] = table
        frames[table] = _validate_cost_frame(selected, table)
        files[table] = {
            "path": str(path), "sha256": file_sha256(path),
            "total_rows": len(all_rows), "selected_rows": len(selected),
            "excluded_rows": len(all_rows) - len(selected), "columns": list(all_rows.columns),
        }
    if frames["cost_invest"].empty:
        raise ValueError("Reference files contain no mapped charger investment rows")
    return frames, files


def _read_backend(database: Path, rules: ChargingComparisonRules) -> dict[str, pd.DataFrame]:
    owners = {tech for group in rules.groups.values() for tech in (group.existing, group.new)}
    frames = {}
    with closing(open_sqlite_readonly(database)) as connection:
        for table in (*COST_TABLES, "capacity_to_activity", "efficiency", "lifetime_tech",
                      "limit_annual_capacity_factor", "existing_capacity"):
            frame = pd.read_sql_query(f"SELECT * FROM {quote_identifier(table)}", connection)
            tech_column = "tech_or_group" if table == "limit_annual_capacity_factor" else "tech"
            selected = frame.loc[frame[tech_column].isin(owners)].copy()
            frames[table] = _validate_cost_frame(selected, table) if table in COST_TABLES else selected
    return frames


def fixed_cost_ratios(reference: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Match costs within the supplied evidence without inferring its money units."""
    fixed = reference["cost_fixed"]
    invest = reference["cost_invest"][["region", "tech", "vintage", "cost"]]
    rows = fixed.merge(invest, on=["region", "tech", "vintage"], how="left",
                       suffixes=("_fixed", "_invest"), validate="many_to_one")
    rows["ratio"] = rows.cost_fixed.div(rows.cost_invest.where(rows.cost_invest.gt(0)))
    rows["status"] = "matched_raw_ratio"
    rows.loc[rows.cost_invest.isna(), "status"] = "investment_absent"
    rows.loc[rows.cost_invest.eq(0), "status"] = "zero_investment_denominator"
    return rows[["region", "tech", "vintage", "period", "cost_fixed", "cost_invest", "ratio", "status"]]


def compare_cost_frames(
    reference: dict[str, pd.DataFrame], backend: dict[str, pd.DataFrame], *,
    rules: ChargingComparisonRules, periods: ScenarioPeriods,
    converter: CerCurrencyConverter, utilization: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare rates by source year, keeping timing and unit uncertainty explicit.

    PJ/year_capacity is full rated annual output (GW * c2a). The utilization basis
    additionally multiplies by the backend's configured annual target; it is a
    throughput sensitivity, not observed dispatch or an inferred reference factor.
    """
    refs = {tech: name for name, group in rules.groups.items() for tech in group.reference_technologies}
    new_by_source = {periods.projection_year(v, legacy_at_end=False): v for v in periods.model}
    if len(new_by_source) != len(periods.model):
        raise ValueError("Backend cost source years do not map uniquely to vintages")
    confirmed = rules.reference_basis is not None and rules.reference_basis.denominator is not None
    bases: list[tuple[str, ReferenceCostBasis | None]] = [
        ("confirmed" if confirmed else "unresolved", rules.reference_basis),
        *[("conditional", basis) for basis in rules.conditional_bases],
    ]
    records = []
    for table in COST_TABLES[:2]:
        current = backend[table]
        expected_unit = f"$M {converter.target_year}CAD / GW"
        if not current.units.eq(expected_unit).all():
            raise ValueError(f"Backend {table} charger units differ from expected {expected_unit}")
        for row in reference[table].to_dict("records"):
            group_name = refs[row["tech"]]
            group = rules.groups[group_name]
            historical = row["vintage"] < reference["cost_invest"].vintage.min()
            tech = group.existing if historical else group.new
            vintage = periods.latest_observed_vintage if historical else new_by_source.get(row["vintage"])
            matched = current.loc[current.region.eq(row["region"]) & current.tech.eq(tech)
                                  & current.vintage.eq(vintage)]
            if table == "cost_fixed":
                matched = matched.loc[matched.period.eq(row["period"])]
            if len(matched) > 1:
                raise ValueError("Ambiguous charging cost rate bridge")
            scope_status = ("historical_rate_bridge" if historical else "source_year_matched") if len(matched) else "backend_key_absent"
            current_cost = float(matched.cost.iloc[0]) if len(matched) else None
            c2a_rows = backend["capacity_to_activity"].loc[
                backend["capacity_to_activity"].region.eq(row["region"])
                & backend["capacity_to_activity"].tech.eq(tech)
            ]
            c2a = float(c2a_rows.c2a.iloc[0]) if len(c2a_rows) == 1 else None
            if current_cost is not None and (c2a is None or not isfinite(c2a) or c2a <= 0):
                raise ValueError(f"Missing positive charger capacity-to-activity: {row['region']}/{tech}")
            target = utilization.get(tech)
            for status, basis in bases:
                denominator = basis.denominator if basis else None
                value, difference, relative, factor, normalized = None, None, None, None, None
                if basis:
                    value = converter.convert(row["cost"], currency=basis.currency,
                                              dollar_year=basis.dollar_year).target_cad_value
                if basis and denominator is not None and current_cost is not None:
                    factor = 1.0 if denominator == "GW" else c2a
                    if denominator == "PJ/year_at_backend_utilization":
                        if target is None or not isfinite(target) or not 0 < target <= 1:
                            raise ValueError(f"Missing positive annual utilization target: {tech}")
                        factor *= target
                    normalized = current_cost / factor
                    difference = normalized - value
                    relative = difference / value * 100 if value else None
                records.append({
                    "table": table, "charger_class": group_name, "region": row["region"],
                    "reference_tech": row["tech"], "reference_vintage": row["vintage"],
                    "reference_period": row.get("period"), "reference_cost": row["cost"],
                    "reference_csv_row": row.get("reference_csv_row"),
                    "backend_tech": tech, "backend_vintage": vintage,
                    "backend_cost_per_gw": current_cost,
                    "backend_cost_source_year": min(periods.model) if historical else row["vintage"],
                    "backend_capacity_to_activity": c2a, "backend_utilization_target": target,
                    "scope_status": scope_status, "basis_status": status,
                    "basis_name": basis.name if basis else "missing_reference_metadata",
                    "basis_evidence": basis.evidence if basis else "Supplied cost files do not declare currency, price year or capacity denominator.",
                    "reference_currency": basis.currency if basis else None,
                    "reference_dollar_year": basis.dollar_year if basis else None,
                    "denominator": denominator, "backend_denominator_per_gw": factor,
                    "reference_million_target_cad": value, "backend_million_target_cad": normalized,
                    "backend_minus_reference": difference, "relative_percent": relative,
                    "target_dollar_year": converter.target_year,
                    "annual_fixed_cost": table == "cost_fixed",
                    "loss_adjustment": "none; same output-capacity boundary is required",
                })
    coverage = []
    regions = sorted({region for frame in reference.values() for region in frame.region})
    for name, group in rules.groups.items():
        for region in regions:
            for side, technologies in (("reference", group.reference_technologies),
                                       ("backend", [group.existing, group.new])):
                frame = reference["cost_variable"] if side == "reference" else backend["cost_variable"]
                selected = frame.loc[frame.region.eq(region) & frame.tech.isin(technologies)]
                coverage.append({"side": side, "region": region, "charger_class": name,
                                 "rows": len(selected), "explicit_zero_rows": int(selected.cost.eq(0).sum()),
                                 "nonzero_rows": int(selected.cost.ne(0).sum()),
                                 "status": "absent" if selected.empty else "present"})
    return pd.DataFrame(records), pd.DataFrame(coverage)


def verify_build_snapshot(bundle: ConfigBundle, database: Path, report: dict[str, Any]) -> None:
    """Allow relocated copies, but require content identity and current inputs."""
    if not report.get("ok") or report.get("scenario") != bundle.scenario.scenario.name:
        raise ValueError("Charging comparison needs a successful matching scenario build report")
    if report.get("database_sha256") != file_sha256(database):
        raise ValueError("Charging comparison database hash does not match its build report")
    expected = bundle.scenario.periods.model_dump(mode="json")
    if any(report.get("periods", {}).get(key) != value for key, value in expected.items()):
        raise ValueError("Charging comparison build has a different period configuration")
    configuration = report.get("config", {})
    for name, path in (("scenario", bundle.scenario_path), ("paths", bundle.paths_path),
                       ("sources", bundle.sources_path)):
        if configuration.get(name, {}).get("sha256") != file_sha256(path):
            raise ValueError(f"Charging comparison build has stale {name} configuration")
    for family in ("parameters", "manual_inputs"):
        evidence = configuration.get(family)
        if not isinstance(evidence, list) or not evidence:
            raise ValueError(f"Charging comparison build lacks {family} fingerprints")
        for record in evidence:
            path = resolve_repo_path(bundle.repo_root, record["path"])
            if not path.is_file() or record.get("sha256") != file_sha256(path):
                raise ValueError(f"Charging comparison input changed since build: {path}")


def _source_cost_components(
    bundle: ConfigBundle, backend: dict[str, pd.DataFrame], converter: CerCurrencyConverter,
    rules: ChargingComparisonRules,
) -> tuple[pd.DataFrame, dict[str, float], dict[str, Any]]:
    charger_rules = load_harmonization_rules(bundle, "ev_chargers")
    manual_rules = load_harmonization_rules(bundle, "manual_parameters")
    _, frames = validate_manual_registry(
        bundle, source_column=manual_rules["source_column"], notes_column=manual_rules["notes_column"],
        selected_files={charger_rules["manual_file"]},
    )
    manual = frames[charger_rules["manual_file"]]
    path = resolve_input_path(bundle, "manual", charger_rules["manual_file"])
    utilization = {}
    records = []
    fixed_ratio = _manual_value(manual, "all", "fixed_to_capex_ratio", "all")
    for category, group in rules.groups.items():
        if category not in charger_rules["technology"]:
            raise ValueError(f"Unknown production charger category {category}")
        mapping = charger_rules["technology"][category]
        if group.existing != mapping["existing"] or group.new != mapping["new"]:
            raise ValueError("Diagnostic charger bridge differs from production technology mappings")
        policy = charger_rules["utilization"][category]
        for kind, tech in (("existing", group.existing), ("new", group.new)):
            utilization[tech] = _manual_value(manual, category, "max_annual_utilization", policy["source_period"][kind])
        kinds = [str(row.parameter).removesuffix("_capex_aggregation") for row in manual.itertuples()
                 if row.category == "charger" and row.sub_category == category
                 and str(row.parameter).endswith("_capex_aggregation")]
        shares = {kind: _manual_value(manual, category, f"{kind}_capex_aggregation", "through_2050") for kind in kinds}
        if not kinds or not isclose(sum(shares.values()), 1, abs_tol=1e-9):
            raise ValueError("Charger cost component shares do not sum to one")
        vintages = [bundle.scenario.periods.latest_observed_vintage, *bundle.scenario.periods.model]
        for vintage in vintages:
            existing = vintage == bundle.scenario.periods.latest_observed_vintage
            source_year = min(bundle.scenario.periods.model) if existing else bundle.scenario.periods.projection_year(vintage, legacy_at_end=False)
            weighted = 0.0
            for kind, share in shares.items():
                row = _period_value(manual, category, f"{kind}_capex_per_GW", source_year)
                if row.unit not in {"M CAD", "M USD"}:
                    raise ValueError("Charger component lacks the native million-currency unit")
                conversion = converter.convert(float(row.value), currency=row.unit.split()[1], dollar_year=int(row.currency_year))
                contribution = share * conversion.target_cad_value
                weighted += contribution
                records.append({"charger_class": category, "backend_vintage": vintage,
                                "source_year": source_year, "charger_type": kind,
                                "count_share": share, **asdict(conversion),
                                "weighted_cost_per_gw": contribution, "source_notes": row.notes})
            tech = group.existing if existing else group.new
            for table, expected in (("cost_invest", weighted), ("cost_fixed", weighted * fixed_ratio)):
                selected = backend[table].loc[backend[table].tech.eq(tech) & backend[table].vintage.eq(vintage)]
                if table == "cost_invest" and existing:
                    if not selected.empty:
                        raise ValueError("Unexpected investment cost for existing charger stock")
                    continue
                if not existing and selected.empty:
                    raise ValueError(f"Missing backend {table} charger cost: {tech}/{vintage}")
                if not all(isclose(value, expected, rel_tol=1e-10, abs_tol=1e-9) for value in selected.cost):
                    raise ValueError(f"Backend {table} differs from current reviewed source calculation: {tech}/{vintage}")
    return pd.DataFrame(records), utilization, {"path": str(path), "sha256": file_sha256(path)}


def prepare_charging_cost_comparison(
    bundle: ConfigBundle, *, database: Path | None = None, build_report: Path | None = None,
) -> ChargingCostComparison:
    rules = ChargingComparisonRules.model_validate(load_harmonization_rules(bundle, "charging_cost_comparison"))
    database = database or resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    build_report = build_report or resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)
    report = json.loads(build_report.read_text(encoding="utf-8"))
    verify_build_snapshot(bundle, database, report)
    reference, identities = read_reference_costs(resolve_artifact_path(bundle, rules.reference_route), rules)
    backend = _read_backend(database, rules)
    cer_rules = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    macro_path = resolve_input_path(bundle, "interim", cer_rules["interim_subdir_template"].format(edition=edition),
                                    cer_rules["components"]["macro-indicators"]["output_file"])
    converter = CerCurrencyConverter(pd.read_csv(macro_path), scenario=bundle.scenario.economics.cer_scenario,
                                     target_year=bundle.scenario.economics.cost_reference_year)
    components, utilization, manual_identity = _source_cost_components(bundle, backend, converter, rules)
    comparisons, coverage = compare_cost_frames(reference, backend, rules=rules, periods=bundle.scenario.periods,
                                               converter=converter, utilization=utilization)
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=bundle.repo_root,
                              capture_output=True, text=True, check=True).stdout.strip()
    frames = {"reference_rows": pd.concat(reference.values(), ignore_index=True),
              "comparisons": comparisons, "fixed_ratios": fixed_cost_ratios(reference),
              "source_components": components, "variable_coverage": coverage,
              **{f"backend_{name}": frame for name, frame in backend.items()}}
    metadata = {
        "scenario": bundle.scenario.scenario.name, "scenario_path": str(bundle.scenario_path),
        "repository_commit": revision, "period_mapping": bundle.scenario.periods.audit(),
        "rules": rules.model_dump(mode="json"), "reference_files": identities,
        "files": {"database": {"path": str(database), "sha256": file_sha256(database)},
                  "build_report": {"path": str(build_report), "sha256": file_sha256(build_report)},
                  "manual_charger_parameters": manual_identity,
                  "cer_macro": {"path": str(macro_path), "sha256": file_sha256(macro_path)}},
        "build_timestamp_utc": report.get("timestamp_utc"), "build_content_verified": True,
        "component_reconciliation": {"relative_tolerance": 1e-10, "absolute_tolerance": 1e-9, "status": "passed"},
        "reference_basis_status": ("confirmed" if rules.reference_basis.denominator is not None else
                                   "currency_confirmed_denominator_unresolved") if rules.reference_basis else "unresolved",
        "implementation_files": {str(path.relative_to(bundle.repo_root)): file_sha256(path) for path in (
            Path(__file__).resolve(), bundle.repo_root / "src/parameterization/ev_chargers.py",
            bundle.repo_root / "src/parameterization/currency.py",
        )},
        "limitations": [
            "Conditional cost interpretations are sensitivities; missing metadata is not inferred from filenames or other researchers' databases.",
            "A confirmed PJ/year capacity denominator permits a dimensional conversion using capacity_to_activity; it does not establish the reference's implicit utilization or loss convention.",
            "Technology bridges compare rates, never add duplicated BEV/PHEV or MDV/HDV rates.",
            "Source-year matching separates the scenario vintage label from the cost projection year.",
            "Historical fixed-cost bridges pool cohorts; they do not establish equal surviving infrastructure stock.",
            "Fixed costs are annual by table semantics, even where the source/backend unit label omits /year.",
            "Throughput normalization uses configured backend utilization, not optimized dispatch; no charger loss, discount or lifetime adjustment is applied.",
            "Production aggregation weights per-GW rates by port-count shares; this is not total fleet expenditure divided by total rated power.",
            "Absent variable-cost entries are not explicit zeros or proof that electricity supply is free.",
        ],
    }
    LOGGER.info("Compared charging costs: %d reference investment, %d fixed, %d variable rows; basis %s",
                len(reference["cost_invest"]), len(reference["cost_fixed"]), len(reference["cost_variable"]),
                metadata["reference_basis_status"])
    return ChargingCostComparison(frames, metadata, resolve_artifact_path(bundle, rules.report_route))


def write_report(result: ChargingCostComparison) -> Path:
    exports = {}
    for name, frame in result.frames.items():
        path = result.destination / f"{name}.csv"
        write_dataframe_atomic(frame, path)
        exports[path.name] = {"rows": len(frame), "sha256": file_sha256(path)}
    write_text_atomic(json.dumps({**result.metadata, "exports": exports}, indent=2,
                                 ensure_ascii=False, allow_nan=False) + "\n", result.destination / "snapshot.json")
    return result.destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--database", type=Path, help="Optional preserved database copy; content hash must match build report")
    parser.add_argument("--build-report", type=Path, help="Optional preserved build report")
    parser.add_argument("--write-report", action="store_true")
    arguments = parser.parse_args()
    result = prepare_charging_cost_comparison(load_config_bundle(arguments.scenario),
                                              database=arguments.database, build_report=arguments.build_report)
    print(result.frames["variable_coverage"].to_string(index=False))
    print(f"Reference cost basis: {result.metadata['reference_basis_status']}")
    if arguments.write_report:
        print(f"Charging comparison evidence: {write_report(result)}")


if __name__ == "__main__":
    main()
