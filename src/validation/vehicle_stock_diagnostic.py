"""Read the recorded evidence behind the Ontario vehicle-stock notebook."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fetching.vehicle_population import load_history_manifest
from parameterization.road_lifetimes_survival import (
    median_equivalent_lifetimes,
    transform_source_survival_curves,
    validate_accepted_survival_inputs,
)
from utils import (
    ConfigBundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
)


def _validate_history_coverage(manifest: pd.DataFrame, statuses: pd.DataFrame) -> None:
    history_years = set(pd.to_numeric(manifest["year"], errors="raise"))
    status_years = set(pd.to_numeric(statuses["report_year"], errors="raise"))
    if history_years != status_years:
        raise ValueError("Incomplete Ontario registration-status history in diagnostic evidence")


def collect_stock_inputs(bundle: ConfigBundle) -> dict[str, Path]:
    """Resolve the complete notebook inputs after explicit prerequisite preparation.

    Registered history is validated against its original normalized Report A files
    before capture. Notebook readers subsequently use only the recorded snapshots.
    """
    source_manifest = load_history_manifest(bundle)
    ontario = load_harmonization_rules(bundle, "ontario_vehicle_population")
    ceud = load_harmonization_rules(bundle, "nrcan_ceud")
    road = load_harmonization_rules(bundle, "road_aggregation")
    lifetime = load_harmonization_rules(bundle, "road_lifetimes_survival")
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    source_dir = resolve_artifact_path(bundle, "ontario_vehicle_population")
    ceud_dir = resolve_input_path(bundle, "interim", ceud["interim_subdir"])
    road_dir = resolve_artifact_path(bundle, "road_aggregation")
    review_dir = resolve_artifact_path(bundle, "vehicle_mapping_review")
    survival_dir = resolve_artifact_path(bundle, "vehicle_survival_interim")
    validation_dir = resolve_artifact_path(bundle, "lifetime_validation")
    assorted_dir = resolve_input_path(bundle, "interim", assorted["interim_subdir"])
    selected_year = bundle.scenario.existing_capacity.vehicle_population_year
    status_path = source_dir / ontario["long_status_file"]
    _validate_history_coverage(
        source_manifest, pd.read_csv(status_path, usecols=["report_year"], low_memory=False),
    )
    return {
        "coverage": review_dir / road["coverage_file"],
        "unresolved_reason_detail": review_dir / road["unresolved_reason_detail_file"],
        "unresolved_reason_summary": review_dir / road["unresolved_reason_summary_file"],
        "mapped_fleet": road_dir / road["mapped_current_stock_file"],
        "nrcan_ceud": ceud_dir / ceud["region_output_template"].format(region="on"),
        "vehicle_class_evidence": review_dir / road["vehicle_class_evidence_file"],
        "wards_comparison": road_dir / road["wards_comparison_file"],
        "status_long": status_path,
        "report4_distribution": source_dir
        / ontario["reports"][4]["distribution_output_template"].format(year=selected_year),
        "report5_distribution": source_dir
        / ontario["reports"][5]["distribution_output_template"].format(year=selected_year),
        "passenger_cohorts": survival_dir / lifetime["cohort_snapshot_file"],
        "commercial_cohorts": survival_dir / lifetime["commercial_snapshot_file"],
        "raw_key_transitions": survival_dir / lifetime["raw_key_transition_file"],
        "ceud_class_retention": validation_dir / lifetime["ceud_class_retention_file"],
        "ceud_scope_comparison": validation_dir / lifetime["ceud_scope_comparison_file"],
        "transition_mapping_coverage": validation_dir / lifetime["transition_mapping_coverage_file"],
        "mto_survival_decision": validation_dir / lifetime["mto_survival_decision_file"],
        "legacy_survival": validation_dir / lifetime["legacy_survival_curves_file"],
        "mapping_config": resolve_parameter_path(bundle, road["vehicle_size_class_map_file"]),
        "source_manifest": source_dir / ontario["manifest_file"],
        "nhtsa_survival_input": assorted_dir / assorted["nhtsa_cafe"]["output_file"],
        "eia_survival_input": assorted_dir / assorted["eia_nems"]["output_file"],
        "moves5_survival_input": assorted_dir / assorted["epa_moves5"]["output_file"],
    }


def _heavy_truck_comparison(
    bundle: ConfigBundle, frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Apply the existing survival transformation to recorded native schedules."""
    nhtsa = frames["nhtsa_survival_input"]
    eia = frames["eia_survival_input"]
    moves5 = frames["moves5_survival_input"]
    validate_accepted_survival_inputs(
        nhtsa, eia,
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
        moves5=moves5,
    )
    _, transformed = transform_source_survival_curves(nhtsa, eia, moves5)
    options = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["fixed_lifetime_sources"]["heavy_trucks"]["source_options"]
    pairs = {(source, option["curve_source_class"]) for source, option in options.items()}
    selected = transformed.loc[
        pd.MultiIndex.from_frame(transformed[["source_id", "source_class"]]).isin(pairs)
    ].reset_index(drop=True)
    if set(zip(selected.source_id, selected.source_class, strict=True)) != pairs:
        raise ValueError("Heavy-truck survival comparison lacks a recorded source schedule")
    interpolation = str(load_harmonization_rules(bundle, "road_lifetimes_survival")["interpolation"])
    medians = median_equivalent_lifetimes(selected, interpolation=interpolation)
    return selected, medians, interpolation


def load_stock_diagnostic(*, repo_root: Path | None = None) -> dict[str, Any]:
    """Load the latest successful stock evidence without reading mutable ETL inputs."""
    from validation.notebook_evidence import load_notebook_evidence

    recorded = load_notebook_evidence("vehicle_stock", repo_root=repo_root)
    frames = {
        key: pd.read_csv(path, low_memory=False)
        for key, path in recorded.paths.items()
    }
    _validate_history_coverage(frames["source_manifest"], frames["status_long"])
    heavy_curves, heavy_medians, interpolation = _heavy_truck_comparison(recorded.bundle, frames)
    scenario_name = recorded.bundle.scenario.scenario.name
    switches = {
        **recorded.bundle.scenario.lifetimes.model_dump(),
        "vkt_schedules": recorded.bundle.scenario.road_utilization.vkt_schedules,
    }
    return {
        **frames,
        "scenario_switches": pd.DataFrame([
            {"scenario": scenario_name, "switch": switch, "value": value}
            for switch, value in switches.items()
        ]),
        "build_metadata": recorded.metadata,
        "heavy_truck_survival": heavy_curves,
        "heavy_truck_medians": heavy_medians,
        "heavy_truck_median_interpolation": interpolation,
        "heavy_truck_selected_source": recorded.bundle.scenario.lifetimes.heavy_truck_survival_source,
    }
