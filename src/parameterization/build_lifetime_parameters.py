"""Build the scenario-selected transport lifetime representation."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

import pandas as pd
from canoe_schema.v4_0 import LifetimeProcess, LifetimeSurvivalCurve, LifetimeTech

from parameterization.charger_representation import select_charger_technologies
from parameterization.offroad_lifetimes import (
    prepare_reviewed_manual_lifetimes,
    prepare_statcan_bus_lifetimes,
)
from parameterization.road_cohort_survival import TEMOA_MIN_INTERIOR_SURVIVAL
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    prepare_fixed_road_lifetimes,
    prepare_road_survival_curve_rows,
    require_tail_derivation,
    resolved_road_lifetime_rules,
)
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
)
from validation.provenance import ResolvedProvenance


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LifetimePreparation:
    fixed_rows: list[LifetimeTech]
    curve_rows: list[LifetimeSurvivalCurve]
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, object]
    process_rows: list[LifetimeProcess]


@dataclass(frozen=True)
class LifetimeEndpoints:
    """Validated retirement endpoints shared by operating-parameter builders."""

    fixed: Mapping[tuple[str, str], float]
    curves: Mapping[tuple[str, str, int], int]

    def end_year(self, region: str, tech: str, vintage: int) -> float:
        """Resolve one process endpoint without applying a default lifetime."""
        owner = (region, tech)
        if owner in self.fixed:
            return vintage + self.fixed[owner]
        key = (region, tech, vintage)
        if key in self.curves:
            return self.curves[key]
        if any(curve_owner[:2] == owner for curve_owner in self.curves):
            raise ValueError(f"Missing lifetime curve for process: {key}")
        raise ValueError(f"Technology has no prepared lifetime: {owner}")

    def operating_periods(
        self, region: str, tech: str, vintage: int, periods: Sequence[int],
    ) -> tuple[int, ...]:
        """Include each period beginning before retirement, including partial periods."""
        end = self.end_year(region, tech, vintage)
        return tuple(period for period in sorted(periods) if vintage <= period < end)


def lifetime_endpoints(
    fixed_rows: Sequence[LifetimeTech], curve_rows: Sequence[LifetimeSurvivalCurve],
) -> LifetimeEndpoints:
    """Resolve fixed lives and complete annual curves once for parameter coverage.

    Curve endpoints belong to individual region/technology/vintage processes.
    Missing vintages must remain errors rather than silently borrowing another
    process's curve or falling back to a model default.
    """
    fixed: dict[tuple[str, str], float] = {}
    for row in fixed_rows:
        key = (row.region, row.tech)
        lifetime = float(row.lifetime) if row.lifetime is not None else float("nan")
        if key in fixed or not isfinite(lifetime) or lifetime <= 0:
            raise ValueError(f"Duplicate or invalid fixed lifetime: {key}")
        fixed[key] = lifetime
    grouped: dict[tuple[str, str, int], list[LifetimeSurvivalCurve]] = {}
    for row in curve_rows:
        key = (row.region, row.tech, row.vintage)
        if key[:2] in fixed:
            raise ValueError(f"Conflicting fixed and survival-curve lifetime: {key[:2]}")
        grouped.setdefault(key, []).append(row)
    curves: dict[tuple[str, str, int], int] = {}
    for key, rows in grouped.items():
        ordered = sorted(rows, key=lambda row: row.period)
        years = [row.period for row in ordered]
        fractions = [float(row.fraction) if row.fraction is not None else float("nan")
                     for row in ordered]
        if (len(years) < 2 or years != list(range(key[2], years[-1] + 1))
                or fractions[0] != 1 or fractions[-1] != 0
                or any(not isfinite(value) or not 0 <= value <= 1 for value in fractions)
                or any(value < TEMOA_MIN_INTERIOR_SURVIVAL for value in fractions[1:-1])
                or any(right > left + 1e-12 for left, right in zip(fractions, fractions[1:]))):
            raise ValueError(f"Invalid or incomplete annual lifetime curve: {key}")
        curves[key] = years[-1]
    return LifetimeEndpoints(MappingProxyType(fixed), MappingProxyType(curves))


def prepare_lifetime_rows(
    bundle: ConfigBundle, *, annual_acquisitions: pd.DataFrame | None = None,
    cohort_provenance_contexts: list[ResolvedProvenance] | None = None,
) -> LifetimePreparation:
    """Resolve accepted road, StatCan, and reviewed manual evidence without SQLite."""
    require_tail_derivation(bundle)
    lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")
    road_rules = load_harmonization_rules(bundle, "road_aggregation")
    assorted_rules = load_harmonization_rules(bundle, "assorted_sources")
    stock_rules = resolved_road_lifetime_rules(bundle)
    master_technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    technology = select_charger_technologies(bundle, master_technology)
    selected_chargers = set(technology.loc[technology.category.eq("charger"), "tech"])
    inactive_chargers = set(master_technology.loc[
        master_technology.category.eq("charger"), "tech",
    ]) - selected_chargers
    frames = derive_accepted_lifetime_frames(
        bundle, rules=lifetime_rules, road_rules=road_rules,
        assorted_rules=assorted_rules,
    )
    manual = pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv"))
    manual_rows, manual_contexts, _ = prepare_reviewed_manual_lifetimes(bundle)
    # The manual helper validates the entire master catalogue. Only the selected
    # scenario representation may contribute charger lifetimes to the payload.
    excluded_manual_rows = sum(row.tech in inactive_chargers for row in manual_rows)
    manual_rows = [row for row in manual_rows if row.tech not in inactive_chargers]
    bus_rows, bus_contexts, bus_audit = prepare_statcan_bus_lifetimes(bundle)
    contexts = [*manual_contexts, *bus_contexts]
    fixed_rows = [*manual_rows, *bus_rows]
    curve_rows: list[LifetimeSurvivalCurve] = []
    process_rows: list[LifetimeProcess] = []
    curve_audit: dict[str, object] = {}
    if bundle.scenario.lifetimes.survival_curves:
        curve_rows, process_rows, curve_contexts, curve_audit = prepare_road_survival_curve_rows(
            bundle, transformed=frames["transformed_curves"], technology=technology,
            annual_acquisitions=annual_acquisitions,
            cohort_provenance_contexts=cohort_provenance_contexts,
        )
        contexts.extend(curve_contexts)
        curve_owners = set(technology.loc[
            technology.category.isin(stock_rules["survival_curve_classes"]), "tech"])
        fixed_rows = [row for row in fixed_rows if row.tech not in curve_owners]
    else:
        road_rows, road_contexts = prepare_fixed_road_lifetimes(
            bundle, medians=frames["medians"], manual=manual, technology=technology,
        )
        fixed_rows.extend(road_rows)
        contexts.extend(road_contexts)
    expected_regions = {
        stock_rules["region_output_map"].get(region, region)
        for region in bundle.scenario.geography.regions
    }
    modeled = set(technology.loc[
        technology["category"].fillna("").ne("") & technology["tech"].ne("T_OFF"), "tech"
    ])
    fixed_keys = {(row.region, row.tech) for row in fixed_rows}
    curve_keys = {(row.region, row.tech) for row in curve_rows}
    if curve_rows:
        curve_categories = set(stock_rules["survival_curve_classes"])
        modeled -= set(technology.loc[technology.category.isin(curve_categories), "tech"])
        expected_keys = {(region, tech) for region in expected_regions for tech in modeled} | curve_keys
    else:
        expected_keys = {(region, tech) for region in expected_regions for tech in modeled}
    if (len(fixed_rows) != len(fixed_keys) or fixed_keys & curve_keys
            or fixed_keys | curve_keys != expected_keys):
        raise ValueError("Every modeled region/technology needs exactly one lifetime representation")
    if not curve_rows and bundle.scenario.lifetimes.survival_curves:
        raise ValueError("Survival mode produced no accepted road curves")
    endpoint_keys = {(row.region, row.tech, row.vintage) for row in process_rows}
    if endpoint_keys != {(row.region, row.tech, row.vintage) for row in curve_rows}:
        raise ValueError("Every annual curve requires one explicit process lifetime endpoint")
    lifetime_endpoints(fixed_rows, curve_rows)
    used_data_ids = {row.data_id for row in [*fixed_rows, *curve_rows, *process_rows]}
    contexts = [context for context in contexts if context.data_id in used_data_ids]
    fixed_rows.sort(key=lambda row: (row.region, row.tech))
    curve_rows.sort(key=lambda row: (row.region, row.tech, row.vintage, row.period))
    audit = {
        "period_mapping": bundle.scenario.periods.audit(),
        "representation": "accepted_road_curves" if curve_rows else "fixed_lifetimes",
        "fixed_rows": len(fixed_rows), "curve_rows": len(curve_rows),
        "process_rows": len(process_rows),
        "fixed_technologies": len({row.tech for row in fixed_rows}),
        "curve_technologies": len({row.tech for row in curve_rows}),
        "regions": sorted(expected_regions),
        "charger_representation": {
            "mode": bundle.scenario.ev_chargers.charger_type_classes,
            "selected_technologies": sorted(selected_chargers),
            "excluded_technologies": sorted(inactive_chargers),
            "excluded_manual_rows": excluded_manual_rows,
        },
        "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
        "road_lifetime_sources": stock_rules["fixed_lifetime_sources"],
        "statcan_canada_fallback_rows": int(bus_audit["canada_fallback"].sum()),
        **curve_audit,
    }
    LOGGER.info("Prepared lifetime_tech=%s and lifetime_survival_curve=%s rows; StatCan Canada fallback rows=%s",
                len(fixed_rows), len(curve_rows), audit["statcan_canada_fallback_rows"])
    LOGGER.info("Charger lifetime representation %s: selected %s technologies; excluded %s technologies (%s manual rows)",
                bundle.scenario.ev_chargers.charger_type_classes, len(selected_chargers),
                len(inactive_chargers), excluded_manual_rows)
    LOGGER.debug("Lifetime audit: %s", audit)
    return LifetimePreparation(fixed_rows, curve_rows, contexts, audit, process_rows)


def build_lifetime_parameter_artifacts(
    scenario_path: str | Path | ConfigBundle, *, annual_acquisitions: pd.DataFrame | None = None,
    cohort_provenance_contexts: list[ResolvedProvenance] | None = None,
) -> Path:
    """Publish deterministic, parameter-ready lifetime rows from registered inputs."""
    bundle = load_config_bundle(scenario_path)
    prepared = prepare_lifetime_rows(bundle, annual_acquisitions=annual_acquisitions,
                                     cohort_provenance_contexts=cohort_provenance_contexts)
    output = resolve_artifact_path(bundle, "lifetime_parameters")
    for filename, rows in (
        ("lifetime_tech.csv", prepared.fixed_rows),
        ("lifetime_survival_curve.csv", prepared.curve_rows),
        ("lifetime_process.csv", prepared.process_rows),
    ):
        frame = pd.DataFrame([row.model_dump(mode="json") for row in rows])
        if frame.empty:
            model = {"lifetime_tech.csv": LifetimeTech, "lifetime_survival_curve.csv": LifetimeSurvivalCurve,
                     "lifetime_process.csv": LifetimeProcess}[filename]
            frame = pd.DataFrame(columns=list(model.model_fields))
        write_dataframe_atomic(frame, output / filename)
    LOGGER.info("Published lifetime parameter tables: %s", output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/default.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    print(build_lifetime_parameter_artifacts(args.scenario))


if __name__ == "__main__":
    main()
