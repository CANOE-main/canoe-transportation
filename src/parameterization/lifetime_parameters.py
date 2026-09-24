"""Prepare the scenario-selected transport lifetime representation."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from canoe_schema.v4_0 import LifetimeSurvivalCurve, LifetimeTech

from parameterization.offroad_lifetimes import (
    prepare_reviewed_manual_lifetimes,
    prepare_statcan_bus_lifetimes,
)
from parameterization.road_lifetimes_survival import (
    _derive_accepted_lifetime_frames,
    prepare_fixed_road_lifetimes,
    prepare_road_survival_curve_rows,
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


def prepare_lifetime_rows(bundle: ConfigBundle) -> LifetimePreparation:
    """Resolve accepted road, StatCan, and reviewed manual evidence without SQLite."""
    lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")
    road_rules = load_harmonization_rules(bundle, "road_aggregation")
    assorted_rules = load_harmonization_rules(bundle, "assorted_sources")
    frames = _derive_accepted_lifetime_frames(
        bundle, rules=lifetime_rules, road_rules=road_rules,
        assorted_rules=assorted_rules,
    )
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    manual = pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv"))
    manual_rows, manual_contexts, _ = prepare_reviewed_manual_lifetimes(bundle)
    bus_rows, bus_contexts, bus_audit = prepare_statcan_bus_lifetimes(bundle)
    contexts = [*manual_contexts, *bus_contexts]
    fixed_rows = [*manual_rows, *bus_rows]
    curve_rows: list[LifetimeSurvivalCurve] = []
    if bundle.scenario.switches.survival_curves:
        curve_rows, curve_contexts = prepare_road_survival_curve_rows(
            bundle, transformed=frames["transformed_curves"], technology=technology,
        )
        contexts.extend(curve_contexts)
        curve_owners = {row.tech for row in curve_rows}
        fixed_rows = [row for row in fixed_rows if row.tech not in curve_owners]
    else:
        road_rows, road_contexts = prepare_fixed_road_lifetimes(
            bundle, medians=frames["medians"], manual=manual, technology=technology,
        )
        fixed_rows.extend(road_rows)
        contexts.extend(road_contexts)
    expected_regions = {
        load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"].get(region, region)
        for region in bundle.scenario.geography.regions
    }
    modeled = set(technology.loc[
        technology["category"].fillna("").ne("") & technology["tech"].ne("T_OFF"), "tech"
    ])
    fixed_keys = {(row.region, row.tech) for row in fixed_rows}
    curve_keys = {(row.region, row.tech) for row in curve_rows}
    expected_keys = {(region, tech) for region in expected_regions for tech in modeled}
    if (len(fixed_rows) != len(fixed_keys) or fixed_keys & curve_keys
            or fixed_keys | curve_keys != expected_keys):
        raise ValueError("Every modeled region/technology needs exactly one lifetime representation")
    if not curve_rows and bundle.scenario.switches.survival_curves:
        raise ValueError("Survival mode produced no accepted road curves")
    used_data_ids = {row.data_id for row in [*fixed_rows, *curve_rows]}
    contexts = [context for context in contexts if context.data_id in used_data_ids]
    fixed_rows.sort(key=lambda row: (row.region, row.tech))
    curve_rows.sort(key=lambda row: (row.region, row.tech, row.vintage, row.period))
    audit = {
        "representation": "accepted_road_curves" if curve_rows else "fixed_lifetimes",
        "fixed_rows": len(fixed_rows), "curve_rows": len(curve_rows),
        "fixed_technologies": len({row.tech for row in fixed_rows}),
        "curve_technologies": len({row.tech for row in curve_rows}),
        "regions": sorted(expected_regions),
        "statcan_canada_fallback_rows": int(bus_audit["canada_fallback"].sum()),
    }
    return LifetimePreparation(fixed_rows, curve_rows, contexts, audit)


def build_lifetime_parameter_artifacts(scenario_path: str | Path) -> Path:
    """Publish deterministic, parameter-ready lifetime rows from registered inputs."""
    bundle = load_config_bundle(scenario_path)
    prepared = prepare_lifetime_rows(bundle)
    output = resolve_artifact_path(bundle, "lifetime_parameters")
    for filename, rows in (
        ("lifetime_tech.csv", prepared.fixed_rows),
        ("lifetime_survival_curve.csv", prepared.curve_rows),
    ):
        frame = pd.DataFrame([row.model_dump(mode="json") for row in rows])
        if frame.empty:
            model = LifetimeTech if filename == "lifetime_tech.csv" else LifetimeSurvivalCurve
            frame = pd.DataFrame(columns=list(model.model_fields))
        write_dataframe_atomic(frame, output / filename)
    LOGGER.info("Published lifetime parameter rows: %s", prepared.audit)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    print(build_lifetime_parameter_artifacts(args.scenario))


if __name__ == "__main__":
    main()
