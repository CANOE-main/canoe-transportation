"""Build road and off-road existing capacity as validated CANOE v4 rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import ExistingCapacity

from parameterization.road_efficiencies import prepare_bus_annual_efficiency_evidence
from parameterization.ceud_inputs import read_ceud_input
from parameterization.vehicle_reference import load_vehicle_reference
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    fixed_existing_lifetimes,
    resolved_road_lifetime_rules,
    require_tail_derivation,
    harmonized_road_survival_schedules,
    resolved_survival_max_age,
    survival_source_component,
)
from parameterization.offroad_lifetimes import prepare_statcan_bus_lifetimes
from parameterization.offroad_stocks_and_demands import build_offroad_existing_capacity
from parameterization.road_stocks_and_demands import (
    accepted_curve_ages,
    distribute_existing_bus_capacity,
    distribute_existing_road_capacity,
    derive_sales_survival_cohorts,
)
from utils import (
    ConfigBundle,
    active_source_keys,
    load_config_bundle,
    load_harmonization_rules,
    file_sha256,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.insertion import (
    cleanup_transport_parameter_batches,
    compact_parameter_notes,
    validate_parameter_rows,
)
from validation.provenance import (
    ResolvedProvenance,
    resolve_composite_provenance,
    resolve_provenance,
    source_id_mapping,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExistingCapacityPreparation:
    """Keep solver parameters separate from vehicles surviving at the reference year."""

    existing_capacity_rows: list[ExistingCapacity]
    physical_stock: pd.DataFrame
    annual_acquisitions: pd.DataFrame
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, Any]


def physical_stock_for_consumers(
    bundle: ConfigBundle, rows: list[ExistingCapacity], physical_stock: pd.DataFrame | None,
) -> pd.DataFrame:
    """Validate the physical vehicle snapshot; fixed-capacity inputs have identical units.

    Curve capacity cannot be interpreted as physical stock. The explicit snapshot is
    required in that mode, including for isolated callers outside the native build.
    """
    vehicles = [row for row in rows if row.units == "k vehicles"]
    if physical_stock is None:
        if bundle.scenario.lifetimes.survival_curves:
            raise ValueError("Curve capacity requires an explicit physical-stock handoff")
        physical_stock = pd.DataFrame([
            {"region": row.region, "tech": row.tech, "vintage": row.vintage,
             "surviving_k_vehicles": row.capacity, "base_year": bundle.scenario.periods.base_year,
             "units": "k vehicles", "data_id": row.data_id}
            for row in vehicles
        ], columns=["region", "tech", "vintage", "surviving_k_vehicles", "base_year", "units", "data_id"])
    required = ["region", "tech", "vintage", "surviving_k_vehicles", "base_year", "units", "data_id"]
    if set(required) - set(physical_stock):
        raise ValueError("Physical-stock handoff is missing its basis, keys or lineage")
    stock = physical_stock[required].copy()
    keys = ["region", "tech", "vintage"]
    if stock[required].isna().any().any() or stock.duplicated(keys).any():
        raise ValueError("Physical-stock handoff has null or duplicate cohort keys")
    if (not stock.units.eq("k vehicles").all()
            or not stock.base_year.eq(bundle.scenario.periods.base_year).all()
            or not stock.surviving_k_vehicles.map(lambda value: isfinite(value) and value >= 0).all()):
        raise ValueError("Physical-stock handoff has invalid units, reference year or vehicle counts")
    expected = {(row.region, row.tech, row.vintage): row for row in vehicles}
    if set(stock[keys].itertuples(index=False, name=None)) != set(expected):
        if not bundle.scenario.lifetimes.survival_curves or not set(expected).issubset(
            set(stock[keys].itertuples(index=False, name=None))
        ):
            raise ValueError("Physical-stock support differs from prepared vehicle parameter keys")
    for row in stock.itertuples(index=False):
        parameter = expected.get((row.region, row.tech, row.vintage))
        if parameter is None:
            continue  # Observed reference-year stock may retire before the first model period.
        if row.data_id != parameter.data_id:
            raise ValueError("Physical-stock lineage differs from its prepared cohort parameter")
        if not bundle.scenario.lifetimes.survival_curves and abs(row.surviving_k_vehicles - parameter.capacity) > 1e-9:
            raise ValueError("Fixed-lifetime capacity and physical stock must agree")
    return stock.reset_index(drop=True)


def _rules_digest(rules: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:16]


def _capacity_handoff_identity(
    bundle: ConfigBundle, rows: list[ExistingCapacity], contexts: list[ResolvedProvenance],
    physical_stock: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Identify the exact prepared batches and scenario, independent of destinations."""
    def digest(value: Any) -> str:
        return hashlib.sha256(json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")).hexdigest()

    return {
        "version": 2,
        "scenario_sha256": digest(bundle.scenario.model_dump(mode="json", exclude={"outputs"})),
        "rows_sha256": digest([row.model_dump(mode="json") for row in rows]),
        "contexts_sha256": digest([context.model_dump(mode="json") for context in contexts]),
        "physical_stock_sha256": digest(physical_stock_for_consumers(bundle, rows, physical_stock).to_dict("records")),
    }


def publish_capacity_handoff(
    bundle: ConfigBundle, *, rows: list[ExistingCapacity], contexts: list[ResolvedProvenance],
    audit: dict[str, Any], artifact_paths: list[Path],
    physical_stock: pd.DataFrame | None = None,
) -> None:
    """Commit a complete capacity family after its individual artifacts are written.

    The producer invalidates the previous integrity marker before any preparation
    writes. A missing marker therefore means the family must be prepared again;
    individual CSV replacements are not a multi-file transaction.
    """
    audit["prepared_handoff"] = {
        **_capacity_handoff_identity(bundle, rows, contexts, physical_stock),
        "artifacts": [
            {
                "path": str(path.relative_to(bundle.repo_root)
                            if path.is_relative_to(bundle.repo_root) else path),
                "sha256": file_sha256(path),
            }
            for path in artifact_paths
        ],
    }
    marker = resolve_artifact_path(bundle, "existing_capacity_validation", "integrity.json")
    write_text_atomic(json.dumps(audit, indent=2) + "\n", marker)
    LOGGER.info("Published complete existing-capacity handoff with %d artifacts", len(artifact_paths))


def validate_capacity_handoff(
    bundle: ConfigBundle, *, rows: list[ExistingCapacity], contexts: list[ResolvedProvenance],
    audit: dict[str, Any],
    physical_stock: pd.DataFrame | None = None,
) -> None:
    """Reject incomplete, replaced or differently scoped capacity evidence."""
    marker = resolve_artifact_path(bundle, "existing_capacity_validation", "integrity.json")
    try:
        committed_text = marker.read_text(encoding="utf-8")
        committed = json.loads(committed_text)
    except (OSError, ValueError) as error:
        raise ValueError("Existing-capacity preparation has no readable completion marker; prepare it again") from error
    # JSON round-trip retains audit semantics (including JSON object-key coercion).
    if json.dumps(committed, sort_keys=True) != json.dumps(json.loads(json.dumps(audit)), sort_keys=True):
        raise ValueError("Existing-capacity completion marker differs from the supplied audit")
    handoff = audit.get("prepared_handoff", {})
    identity = _capacity_handoff_identity(bundle, rows, contexts, physical_stock)
    if any(handoff.get(key) != value for key, value in identity.items()):
        raise ValueError("Existing-capacity handoff differs from the supplied scenario, rows or contexts")
    artifacts = handoff.get("artifacts")
    if not artifacts:
        raise ValueError("Existing-capacity handoff has no completed artifacts")
    for artifact in artifacts:
        path = Path(artifact["path"])
        path = path if path.is_absolute() else bundle.repo_root / path
        try:
            matches = file_sha256(path) == artifact["sha256"]
        except OSError as error:
            raise ValueError(f"Completed existing-capacity artifact is missing: {path}") from error
        if not matches:
            raise ValueError(f"Completed existing-capacity artifact changed: {path}")
    try:
        unchanged = marker.read_text(encoding="utf-8") == committed_text
    except OSError:
        unchanged = False
    if not unchanged:
        raise ValueError("Existing-capacity completion marker changed while checking evidence")


def _component(
    bundle: ConfigBundle, source: str, component: str | int
) -> ResolvedProvenance:
    return resolve_provenance(
        bundle.sources,
        source_key=source,
        component_key=component,
        transformation="existing_capacity_input",
        transformation_version="1",
    )


def _context(
    bundle: ConfigBundle,
    *,
    name: str,
    components: list[tuple[str, str | int]],
    variant: dict[str, Any],
    transformation_version: str = "2",
    transformation: str = "CEUD stock or energy with reviewed age, fuel, and lifetime evidence",
) -> ResolvedProvenance:
    inputs = [_component(bundle, source, component) for source, component in components]
    governing_id = source_id_mapping(bundle.sources)["nrcan_ceud_transport_provincial"]
    governing_quality = next(
        item.data_quality for item in inputs if item.source_id == governing_id
    )
    return resolve_composite_provenance(
        inputs=inputs,
        dataset_key=f"existing_capacity.{name}",
        transformation=transformation,
        transformation_version=transformation_version,
        governing_source_id=governing_id,
        data_quality=governing_quality,
        value_variant=variant,
    )


def validate_capacity_inputs(
    bundle: ConfigBundle, provincial: pd.DataFrame, national: pd.DataFrame
) -> None:
    """Reject incomplete CEUD editions and scenario region mismatches."""
    base_year = bundle.scenario.periods.base_year
    regions = set(bundle.scenario.geography.regions)
    if set(provincial["region"]) != regions:
        raise ValueError(
            f"CEUD regional coverage differs from scenario: {set(provincial['region']) ^ regions}"
        )
    if (
        int(provincial["year"].max()) != base_year
        or int(national["year"].max()) != base_year
    ):
        raise ValueError("CEUD interim tables do not end in the scenario base year")
    if provincial.duplicated(["region", "table_id", "raw_series", "year"]).any():
        raise ValueError("Duplicate provincial CEUD series/year rows")
    if national.duplicated(["table_id", "raw_series", "year"]).any():
        raise ValueError("Duplicate national CEUD series/year rows")


def validate_capacity_outputs(
    *,
    bundle: ConfigBundle,
    road_cohorts: pd.DataFrame,
    road_shares: pd.DataFrame,
    road_capacity: pd.DataFrame,
    bus_cohorts: pd.DataFrame,
    bus_capacity: pd.DataFrame,
    offroad_annual: pd.DataFrame,
    offroad_capacity: pd.DataFrame,
    rows: list[ExistingCapacity],
    cleanup_tolerance: float,
) -> dict[str, Any]:
    """Check conservation, ownership, units, and key coverage before publication."""
    regions = set(bundle.scenario.geography.regions)
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["region_output_map"]
    output_regions = {region_map.get(region, region) for region in regions}
    template = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    technologies = set(template["tech"])
    if not rows or {row.region for row in rows} != output_regions:
        raise ValueError("Existing capacity lacks one or more native output regions")
    if any(row.tech not in technologies for row in rows):
        raise ValueError("Existing capacity references a missing template technology")
    if any(row.vintage not in bundle.scenario.periods.existing for row in rows):
        raise ValueError("Existing capacity references a non-existing vintage period")
    if any(row.capacity is None or row.capacity < 0 for row in rows):
        raise ValueError("Existing capacity has missing or negative capacity")
    if any(
        row.units not in {"k vehicles", "bn passenger-km", "bn tonne-km"}
        for row in rows
    ):
        raise ValueError("Existing capacity has an unsupported unit")
    road_stock = road_cohorts.groupby(["ceud_region", "road_class"])[
        "cohort_k_vehicles"
    ].sum()
    road_source = road_cohorts.groupby(["ceud_region", "road_class"])[
        "stock_k_vehicles"
    ].first()
    if not (road_stock - road_source).abs().le(1e-7).all():
        raise ValueError("Road age cohorts do not conserve their selected stock basis")
    if (
        abs(
            float(road_capacity["capacity"].sum())
            - float(road_shares["solver_capacity_k_vehicles"].sum())
        )
        > 1e-6
    ):
        raise ValueError("Road solver parameters do not reconcile to annual parameter contributions")
    bus_stock = bus_cohorts.groupby(["ceud_region", "road_class"])[
        "capacity"
    ].sum()
    bus_source = bus_cohorts.groupby(["ceud_region", "road_class"])[
        "stock_k_vehicles"
    ].first()
    if not (bus_stock - bus_source).abs().le(1e-7).all():
        raise ValueError("Bus cohorts do not conserve CEUD base-year stock")
    if (
        abs(float(bus_capacity["capacity"].sum()) - float(bus_stock.sum())) > 1e-7
    ):
        raise ValueError("Bus vintage aggregation does not conserve cohort capacity")
    if len(offroad_annual) != len(output_regions) * 6 * (
        bundle.scenario.periods.base_year - min(bundle.scenario.periods.existing) + 1
    ):
        raise ValueError(
            "Off-road annual capacity lacks region/mode/technology/year coverage"
        )
    if set(offroad_capacity["region"]) != output_regions:
        raise ValueError("Off-road capacity lacks native regions")
    if len({(row.region, row.tech, row.vintage) for row in rows}) != len(rows):
        raise ValueError(
            "Existing capacity has duplicate native region/tech/vintage keys"
        )
    expected_keys = {
        (str(record.region), str(record.tech), int(record.vintage))
        for record in pd.concat([road_capacity, bus_capacity, offroad_capacity]).itertuples(
            index=False
        )
        if float(record.capacity) > 0 and float(record.capacity) >= cleanup_tolerance
    }
    actual_keys = {(row.region, row.tech, row.vintage) for row in rows}
    if actual_keys != expected_keys:
        raise ValueError(
            f"Existing capacity technology/vintage coverage differs: missing={len(expected_keys - actual_keys)}, extra={len(actual_keys - expected_keys)}"
        )
    return {
        "regions": sorted(output_regions),
        "road_classes": sorted(road_cohorts["road_class"].unique()),
        "bus_classes": sorted(bus_cohorts["road_class"].unique()),
        "bus_stock_k_vehicles": float(bus_capacity["capacity"].sum()),
        "offroad_modes": sorted(offroad_annual["mode"].unique()),
        "road_stock_k_vehicles": float(road_cohorts["cohort_k_vehicles"].sum()),
        "road_solver_capacity_k_vehicles": float(road_capacity["capacity"].sum()),
        "offroad_capacity_by_unit": offroad_capacity.groupby("units")["capacity"]
        .sum()
        .to_dict(),
        "parameter_rows": len(rows),
        "pending_classes": [],
        "excluded_family": "EV charger capacity",
    }


def build_existing_capacity_artifacts(
    bundle: ConfigBundle,
) -> ExistingCapacityPreparation:
    """Rebuild normalized audit products and parameter-ready v4 rows from cached inputs."""
    tail_settings = require_tail_derivation(bundle)
    completion_marker = resolve_artifact_path(bundle, "existing_capacity_validation", "integrity.json")
    completion_marker.unlink(missing_ok=True)
    LOGGER.info("Invalidated existing-capacity handoff before preparation")
    scenario = bundle.scenario
    selected_sources = {
        region: bundle.scenario.aggregation_sources.source_for("stock_age", region)
        for region in scenario.geography.regions
    }
    available_sources = active_source_keys(bundle)
    if (
        set(selected_sources.values()) - available_sources
        or set(selected_sources.values()) != {"ontario_ministry_transport_vehicle_population"}
    ):
        raise ValueError(
            "Selected regional stock-age source has no implemented adapter"
        )
    lifetime_sources = {
        "canada_energy_policy_simulator",
        "emrg_sfu_cims_model",
    }
    selected_lifetime_rules = resolved_road_lifetime_rules(bundle)["fixed_lifetime_sources"]
    lifetime_sources.update(selector["source_id"] for selector in selected_lifetime_rules.values()
                            if "source_id" in selector)
    if any(selector["kind"] == "ceud_median" for selector in selected_lifetime_rules.values()):
        lifetime_sources.add("nhtsa_cafe_ldv_survival")
    inactive_lifetimes = sorted(lifetime_sources - available_sources)
    if inactive_lifetimes:
        raise ValueError(
            f"Existing-capacity lifetime sources are inactive: {inactive_lifetimes}"
        )
    base_year = scenario.periods.base_year
    regions = list(scenario.geography.regions)
    road_family = load_harmonization_rules(bundle, "road_stocks_and_demands")
    road_rules = resolved_road_lifetime_rules(bundle)
    bus_rules = road_family["bus_existing_capacity"]
    offroad_rules = load_harmonization_rules(bundle, "offroad_stocks_and_demands")[
        "existing_capacity"
    ]
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    statcan_rules = load_harmonization_rules(bundle, "statcan_tables")
    assorted_rules = load_harmonization_rules(bundle, "assorted_sources")
    dashboard_rules = assorted_rules["tc_ev_dashboard"]
    mto_rules = load_harmonization_rules(bundle, "ontario_vehicle_population")
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])
    ceud_files = [
        ceud_dir / ceud_rules["region_output_template"].format(region=region.lower())
        for region in regions
    ]
    national_file = ceud_dir / ceud_rules["region_output_template"].format(
        region="national"
    )
    provincial = pd.concat(
        [read_ceud_input(bundle, region) for region in regions],
        ignore_index=True,
    )
    national = read_ceud_input(bundle, "national")
    validate_capacity_inputs(bundle, provincial, national)
    statcan_dir = resolve_input_path(bundle, "interim", statcan_rules["interim_subdir"])
    lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")
    lifetime_frames = derive_accepted_lifetime_frames(
        bundle, rules=lifetime_rules,
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=assorted_rules,
    )
    survival = harmonized_road_survival_schedules(bundle, lifetime_frames["transformed_curves"])
    report5_file = resolve_artifact_path(
        bundle, "ontario_vehicle_population"
    ) / mto_rules["reports"][5]["distribution_output_template"].format(
        year=scenario.existing_capacity.vehicle_population_year
    )
    ldv_file = statcan_dir / statcan_rules["ldv_history"]["output_file"]
    truck_file = statcan_dir / statcan_rules["tables"]["23-10-0308-01"]["output_file"]
    dashboard_file = (
        resolve_artifact_path(bundle, "tc_ev_dashboard_interim")
        / dashboard_rules["output_file"]
    )
    lifetime_file = resolve_input_path(bundle, "manual", "lifetime_process.csv")
    vehicle_reference = load_vehicle_reference(bundle)
    bus_efficiency, bus_evidence_files = prepare_bus_annual_efficiency_evidence(
        bundle, provincial
    )
    bus_lifetime_rows, _, _ = prepare_statcan_bus_lifetimes(bundle)
    bus_lifetimes = {
        (row.region, row.tech): float(row.lifetime) for row in bus_lifetime_rows
    }
    bus_lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")[
        "bus_lifetimes"
    ]
    bus_lifetime_file = statcan_dir / statcan_rules["tables"][str(
        bus_lifetime_rules["statcan_table_id"]
    )]["output_file"]
    input_digest = hashlib.sha256()
    for path in sorted(
        [
            *ceud_files,
            national_file,
            report5_file,
            ldv_file,
            truck_file,
            dashboard_file,
            lifetime_file,
            *vehicle_reference.paths,
            bus_lifetime_file,
            *bus_evidence_files,
        ],
        key=lambda item: item.name,
    ):
        input_digest.update(path.name.encode("utf-8"))
        input_digest.update(file_sha256(path).encode("ascii"))
    manual_lifetimes = pd.read_csv(lifetime_file)
    for frame in (lifetime_frames["medians"], lifetime_frames["transformed_curves"]):
        input_digest.update(frame.to_csv(index=False).encode("utf-8"))
    input_digest.update(survival.annual.to_csv(index=False).encode("utf-8"))
    for category, source_key, component_key in (
        ("motorcycles", "canada_energy_policy_simulator", "motorcycle_lifetimes"),
    ):
        expected_source = (
            bundle.sources.sources[source_key]
            .component(component_key)
            .adapter["source_selector"]
        )
        selected = manual_lifetimes.loc[manual_lifetimes["category"].eq(category)]
        if (
            len(selected) != 1
            or selected.iloc[0]["source -> data_source"] != expected_source
        ):
            raise ValueError(f"Manual road lifetime source mismatch for {category}")
    medians = lifetime_frames["medians"]
    sales_cohorts = derive_sales_survival_cohorts(
        provincial, survival.annual, regions=regions,
        base_year=base_year, history_start=min(scenario.periods.existing), rules=road_rules,
    )
    road_cohorts, road_shares, road_exclusions, road_capacity = (
        distribute_existing_road_capacity(
            provincial=provincial,
            ldv_age=pd.DataFrame(),
            report5_age=pd.read_csv(report5_file),
            ldv_registrations=pd.read_csv(ldv_file),
            truck_registrations=pd.read_csv(truck_file),
            dashboard=pd.read_csv(dashboard_file),
            regions=regions,
            period_config=scenario.periods,
            vehicle_population_year=scenario.existing_capacity.vehicle_population_year,
            survival_curves=scenario.lifetimes.survival_curves,
            survival_curve_max_age=resolved_survival_max_age(bundle, survival.annual),
            fixed_lifetimes_by_class=fixed_existing_lifetimes(
                medians, manual_lifetimes, road_rules
            ),
            curve_ages_by_class=accepted_curve_ages(survival.annual, road_rules)
            if scenario.lifetimes.survival_curves
            else {},
            rules=road_rules,
            sales_cohorts=sales_cohorts,
            annual_survival=survival.annual,
        )
    )
    bus_cohorts, bus_shares, bus_exclusions, bus_transfers, bus_capacity = (
        distribute_existing_bus_capacity(
            provincial=provincial,
            report5_age=pd.read_csv(report5_file),
            annual_efficiency=bus_efficiency,
            lifetimes=bus_lifetimes,
            regions=regions,
            period_config=scenario.periods,
            vehicle_population_year=scenario.existing_capacity.vehicle_population_year,
            road_rules=road_rules,
            rules=bus_rules,
        )
    )
    cims_component = bundle.sources.sources["emrg_sfu_cims_model"].component(
        "transport_process_lifetimes"
    )
    offroad_annual, offroad_cohorts, offroad_capacity, air_exclusions = (
        build_offroad_existing_capacity(
            provincial=provincial,
            national=national,
            manual_lifetimes=manual_lifetimes,
            source_selector=cims_component.adapter["source_selector"],
            regions=regions,
            period_config=scenario.periods,
            rules=offroad_rules,
        )
    )
    interim_output = resolve_artifact_path(bundle, "existing_capacity_interim")
    processed_output = resolve_artifact_path(bundle, "existing_capacity_processed")
    validation_output = resolve_artifact_path(bundle, "existing_capacity_validation")
    variants = {
        "base_year": base_year,
        "period_mode": scenario.periods.period_mode,
        "periods": scenario.periods.existing,
        "first_model_period": min(scenario.periods.model),
        "survival_curves": scenario.lifetimes.survival_curves,
        "cleanup_tolerance": scenario.existing_capacity.cleanup_tolerance,
        "road_rules_sha256": _rules_digest(road_rules),
        "bus_rules_sha256": _rules_digest(bus_rules),
        "offroad_rules_sha256": _rules_digest(offroad_rules),
        "stock_age_sources": selected_sources,
        "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
        "input_content_digest": input_digest.hexdigest(),
        "road_stock_method": "sales_survival",
        "survival_tail": tail_settings,
        "ceud_beyond_observed": scenario.sources.nrcan_ceud_beyond_observed,
        "ceud_extrapolation": load_harmonization_rules(bundle, "ceud_inputs")["cagr"] if scenario.sources.nrcan_ceud_beyond_observed == "extrapolated" else None,
    }
    rows: list[ExistingCapacity] = []
    contexts: list[ResolvedProvenance] = []
    input_regions = {road_rules["region_output_map"].get(r, r): r for r in regions}
    mapping_rules = load_harmonization_rules(bundle, "road_aggregation")
    mapped_stock = pd.read_csv(
        resolve_artifact_path(bundle, "road_aggregation")
        / mapping_rules["mapped_current_stock_file"], low_memory=False,
    )
    accepted_mapping = mapped_stock.loc[mapped_stock["mapping_accepted"]]
    road_physical = road_shares.groupby(["region", "road_class", "tech", "vintage"], as_index=False).capacity.sum()
    road_context_capacity = road_capacity.copy()
    acquisition_support = road_shares.loc[road_shares.calibrated_acquisitions_k_vehicles.gt(0),
                                         ["region", "road_class", "tech", "vintage"]].drop_duplicates()
    all_context_support = pd.concat([
        acquisition_support,
        road_physical.loc[road_physical.capacity.gt(0), ["region", "road_class", "tech", "vintage"]],
    ]).drop_duplicates()
    solver_keys = pd.MultiIndex.from_frame(road_capacity[["region", "tech", "vintage"]])
    extra_keys = pd.MultiIndex.from_frame(all_context_support[["region", "tech", "vintage"]])
    extra_support = all_context_support.loc[~extra_keys.isin(solver_keys)]
    road_context_capacity = pd.concat([
        road_context_capacity, extra_support.assign(capacity=0.0, units="k vehicles"),
    ], ignore_index=True)
    road_data_ids = {}
    for (road_class, vintage, region), group in road_context_capacity.groupby(
        ["road_class", "vintage", "region"], sort=True
    ):
        ceud_component = {"cars": 21, "motorcycles": 32}.get(road_class, 37)
        mto_component = (
            "A" if road_class in road_rules["statcan_ldv_source_types"] else 5
        )
        components: list[tuple[str, str | int]] = [
            ("nrcan_ceud_transport_provincial", ceud_component),
        ]
        sales_based = road_class in road_rules["ceud_sales_series"]
        if not sales_based:
            components.append((selected_sources[input_regions[region]], mto_component))
        elif (road_class in road_rules["statcan_ldv_source_types"]
              and road_rules["fixed_lifetime_sources"][road_class]["kind"] == "ceud_median"):
            # Report A still supplies class weights for NHTSA survival, not ages.
            components.append((selected_sources[input_regions[region]], "A"))
        if (road_class in road_rules["statcan_ldv_source_types"]
                and (not sales_based or road_rules["fixed_lifetime_sources"][road_class]["kind"] == "ceud_median")):
            mapping_candidates = (
                "nrcan_fuel_consumption_ratings",
                "fueleconomy_gov_vehicle_data",
            )
            target_class = "Car" if road_class == "cars" else "Light Truck"
            evidence = accepted_mapping.loc[
                accepted_mapping.nrcan_ceud_class.eq(target_class), "evidence_source",
            ].fillna("")
            mapping_sources = [
                source for source in mapping_candidates
                if evidence.str.contains(source, regex=False).any()
            ]
            inactive = sorted(set(mapping_sources) - available_sources)
            if inactive:
                raise ValueError(
                    f"LDV mapping evidence sources are inactive: {inactive}"
                )
            components.extend(
                (source_key, component_key)
                for source_key in mapping_sources
                for component_key in bundle.sources.sources[source_key].components
                if source_key != "fueleconomy_gov_vehicle_data" or component_key == "vehicles"
            )
        if road_class in road_rules["survival_curve_classes"]:
            selector = road_rules["fixed_lifetime_sources"][road_class]
            components.append(survival_source_component(
                assorted_rules,
                str(assorted_rules["nhtsa_cafe"]["source_id"]) if selector["kind"] == "ceud_median" else str(selector["source_id"]),
                selector.get("curve_source_class"),
            ))
        elif road_class == "motorcycles":
            components.append(
                ("canada_energy_policy_simulator", "motorcycle_lifetimes")
            )
        shares = road_shares.loc[
            road_shares["road_class"].eq(road_class)
            & road_shares["region"].eq(region)
        ]
        # Retired cohorts contribute to the technology total redistributed into
        # this vintage, so its lineage includes the full reference composition.
        table_ids = sorted(
            {
                table_id
                for packed in shares["source_table_ids"].dropna()
                for table_id in str(packed).split("|")
                if table_id
            }
        )
        components.extend(
            ("statcan_transport_tables", table_id) for table_id in table_ids
        )
        if road_class == "medium_trucks" and shares["dashboard_override"].any():
            components.append(
                ("transport_canada_ev_dashboard", "medium_heavy_ev_market_share")
            )
        context = _context(
            bundle,
            name=f"road.{road_class}.{vintage}.{region}",
            components=components,
            variant={**variants, "sales_stock_calibration": "base_year_ceud_stock"} if sales_based else variants,
            transformation_version=("7" if scenario.lifetimes.survival_curves else "6") if sales_based else "3",
            transformation=(
                "Annual CEUD sales times cumulative survival calibrated to base-year CEUD class stock, registration fuel proxy and "
                + ("sales-weighted survival curves with separate physical stock"
                   if scenario.lifetimes.survival_curves else "stock-preserving fixed-lifetime eligibility redistribution")
                if sales_based else
                "CEUD stock with reviewed age, fuel, and lifetime evidence"
            ),
        )
        contexts.append(context)
        for record in group.itertuples(index=False):
            road_data_ids[record.region, record.tech, int(record.vintage)] = context.data_id
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                    }
                    for record in group.itertuples(index=False)
                ],
                context, bundle=bundle, compact_notes=False,
            )
        )
    for (road_class, region), group in bus_capacity.groupby(["road_class", "region"], sort=True):
        bus_spec = bus_rules["classes"][road_class]
        components: list[tuple[str, str | int]] = [
            ("nrcan_ceud_transport_provincial", bus_spec["stock"]["table_id"]),
            ("nrcan_ceud_transport_provincial", bus_spec["energy_table"]),
            (selected_sources[input_regions[region]], 5),
            ("statcan_transport_tables", str(bus_lifetime_rules["statcan_table_id"])),
            ("nlr_atb_transportation", "vehicles"),
        ]
        if bus_efficiency.loc[
            bus_efficiency.road_class.eq(road_class) & bus_efficiency.region.eq(region),
            "powertrain",
        ].str.startswith("phev").any():
            components.extend([
                ("nlr_atb_transportation", "phev_vehicle_inputs"),
                ("nlr_atb_transportation", "phev_utility_factor_mdhd"),
                ("argonne_rd_greet", "fuel_heating_values"),
            ])
        if road_class == "inter_city_buses":
            components.append(
                ("epri_us_regen_transportation", "intercity_bus_charts")
            )
        context = _context(
            bundle, name=f"bus.{road_class}.{region}", components=components, variant=variants
        )
        contexts.append(context)
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                    }
                    for record in group.itertuples(index=False)
                ],
                context, bundle=bundle, compact_notes=False,
            )
        )
    for mode, group in offroad_capacity.groupby("mode", sort=True):
        selectors = [
            value
            for key, value in offroad_rules["capacity_series"].items()
            if value.get("mode", key) == mode
        ]
        components = [
            ("nrcan_ceud_transport_provincial", selector["provincial_table"])
            for selector in selectors
        ]
        components += [
            ("nrcan_ceud_transport_national", selector["national_table"])
            for selector in selectors
        ]
        components.append(("emrg_sfu_cims_model", "transport_process_lifetimes"))
        context = _context(
            bundle, name=f"offroad.{mode}", components=components, variant=variants
        )
        contexts.append(context)
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                    }
                    for record in group.itertuples(index=False)
                ],
                context, bundle=bundle, compact_notes=False,
            )
        )
    cleaned, removed = cleanup_transport_parameter_batches(
        {"existing_capacity": rows},
        existing_periods=scenario.periods.existing,
        first_model_period=min(scenario.periods.model),
        epsilon=scenario.existing_capacity.cleanup_tolerance,
    )
    # Place sparse notes after pruning, so a discarded vintage cannot carry
    # the only copy of a source treatment's annotation.
    rows = compact_parameter_notes(
        cleaned["existing_capacity"],
        bundle.sources.defaults.database_note_keys.get("existing_capacity", ()),
    )
    removed_capacity = pd.DataFrame(
        [item for item in removed if item["table"] == "existing_capacity"],
        columns=["units", "value"],
    )
    audit = validate_capacity_outputs(
        bundle=bundle,
        road_cohorts=road_cohorts,
        road_shares=road_shares,
        road_capacity=road_capacity,
        bus_cohorts=bus_cohorts,
        bus_capacity=bus_capacity,
        offroad_annual=offroad_annual,
        offroad_capacity=offroad_capacity,
        rows=rows,
        cleanup_tolerance=scenario.existing_capacity.cleanup_tolerance,
    )
    physical_stock = pd.DataFrame([
        {"region": row.region, "tech": row.tech, "vintage": row.vintage,
         "surviving_k_vehicles": row.capacity, "base_year": base_year,
         "units": "k vehicles", "data_id": row.data_id}
        for row in rows if row.units == "k vehicles"
    ])
    if scenario.lifetimes.survival_curves:
        road_technologies = set(road_physical.tech)
        physical_stock = pd.concat([
            physical_stock.loc[~physical_stock.tech.isin(road_technologies)],
            road_physical.loc[road_physical.capacity.gt(0)].assign(
                surviving_k_vehicles=lambda frame: frame.capacity,
                base_year=base_year, units="k vehicles",
                data_id=lambda frame: [road_data_ids[key] for key in frame[["region", "tech", "vintage"]].itertuples(index=False, name=None)],
            ).drop(columns=["capacity", "road_class"]),
        ], ignore_index=True)
    physical_stock = physical_stock_for_consumers(bundle, rows, physical_stock)
    annual_acquisitions = road_shares.loc[
        road_shares.road_class.isin(road_rules["ceud_sales_series"])
        & road_shares.calibrated_acquisitions_k_vehicles.gt(0), [
        "region", "road_class", "tech", "vintage_year", "vintage",
        "raw_acquisitions_k_vehicles", "calibrated_acquisitions_k_vehicles",
    ]].copy()
    annual_acquisitions["data_id"] = [road_data_ids[key] for key in annual_acquisitions[
        ["region", "tech", "vintage"]].itertuples(index=False, name=None)]
    audit["survival_reconciliation"] = survival.audit.to_dict("records")
    audit["vintage_curve_audit"] = road_shares.attrs.get("vintage_curve_audit", [])
    audit["curve_tail_retirements"] = road_shares.attrs.get("curve_tail_retirements", [])
    audit["grouped_tail_reference_loss_k_vehicles"] = float(road_shares.grouped_tail_reference_loss_k_vehicles.sum())
    audit["survival_tail"] = tail_settings
    audit["capacity_basis"] = (
        "installed_capacity_before_survival" if scenario.lifetimes.survival_curves
        else "reference_year_surviving_stock_after_fixed_eligibility"
    )
    audit["physical_stock"] = {"reference_year": base_year, "units": "k vehicles",
                               "total": float(physical_stock.surviving_k_vehicles.sum())}
    audit["cleanup"] = {
        "epsilon": scenario.existing_capacity.cleanup_tolerance,
        "removed_rows": removed,
        "removed_count": len(removed),
        "removed_capacity_by_unit": {
            str(units): float(value)
            for units, value in removed_capacity.groupby("units")["value"].sum().items()
        },
    }
    audit["period_mapping"] = scenario.periods.audit()
    ceud_manifest = resolve_artifact_path(bundle, "ceud_parameter_inputs") / load_harmonization_rules(
        bundle, "ceud_inputs",
    )["manifest_file"]
    if ceud_manifest.exists():
        manifest = json.loads(ceud_manifest.read_text(encoding="utf-8"))
        if (manifest["scenario"] == scenario.scenario.name
                and manifest["base_year"] == base_year
                and manifest["policy"] == scenario.sources.nrcan_ceud_beyond_observed):
            audit["ceud_parameter_evidence"] = {
                "path": str(ceud_manifest), "sha256": file_sha256(ceud_manifest),
            }
    audit["road_stock_method"] = "sales_survival"
    audit["sales_reconstruction"] = [
        {
            "region": region, "road_class": road_class,
            "history_start": int(group.vintage_year.min()),
            "history_end": int(group.vintage_year.max()),
            "observed_stock_k_vehicles": float(group.ceud_stock_k_vehicles.iloc[0]),
            "reconstructed_stock_k_vehicles": float(group.surviving_k_vehicles.sum()),
            "stock_calibration_multiplier": float(group.stock_calibration_multiplier.iloc[0]),
            "calibrated_stock_k_vehicles": float(group.calibrated_surviving_k_vehicles.sum()),
            "stock_calibration_basis": str(group.stock_calibration_basis.iloc[0]),
            "calibration_residual_k_vehicles": float(group.calibrated_surviving_k_vehicles.sum() - group.ceud_stock_k_vehicles.iloc[0]),
            "stock_observed_year": int(group.stock_observed_year.iloc[0]),
            "stock_observation_status": str(group.stock_observation_status.iloc[0]),
            "raw_sales_k_vehicles": float(group.sales_k_vehicles.sum()),
            "oldest_survival_probability": float(group.sort_values("age").survival_probability.iloc[-1]),
            "older_cohorts": "not reconstructed; no assumed sales before registered/configured coverage",
            "fuel_allocation": "existing registration shares proxy sales composition",
            "future_lifetimes": ("annual survival curves weighted by vehicle sales" if scenario.lifetimes.survival_curves
                                 else "fixed; no second cumulative survival multiplication"),
        }
        for (region, road_class), group in sales_cohorts.groupby(["ceud_region", "road_class"])
    ] if not sales_cohorts.empty else []
    audit["road_lifetime_eligibility"] = [
        {
            "region": region, "road_class": road_class,
            "basis": str(group.eligibility_basis.iloc[0]),
            "fixed_lifetime_years": float(group.fixed_lifetime_years.iloc[0]) if group.fixed_lifetime_years.notna().all() else None,
            "first_model_period": int(group.first_model_period.iloc[0]),
            "first_eligible_annual_cohort": int(group.loc[group.eligible_first_model_period, "vintage_year"].min()) if group.eligible_first_model_period.any() else None,
            "last_eligible_annual_cohort": int(group.loc[group.eligible_first_model_period, "vintage_year"].max()) if group.eligible_first_model_period.any() else None,
            "first_eligible_model_vintage": int(group.loc[group.eligible_first_model_period, "vintage"].min()) if group.eligible_first_model_period.any() else None,
            "eligibility_rule": "annual cohort year + lifetime > first period AND model vintage + lifetime > first period" if group.eligibility_basis.eq("fixed_lifetime").all() else "annual vehicle sales determine each vintage's survival curve and retirement year",
        }
        for (region, road_class), group in road_cohorts.groupby(["region", "road_class"])
    ]
    if not sales_cohorts.empty:
        write_dataframe_atomic(sales_cohorts, interim_output / road_rules["sales_cohorts_file"])
        LOGGER.warning(
            "Road fleets use sales from %s through %s, scaled to CEUD stock. "
            "Earlier sales are not estimated; registration shares supply the fuel mix. "
            "Existing capacity contains %s.",
            int(sales_cohorts.vintage_year.min()), base_year,
            "installed vehicles for Temoa to retire" if scenario.lifetimes.survival_curves
            else "surviving vehicles after fixed-lifetime allocation",
        )
    audit["excluded_road_fuel_rows"] = len(road_exclusions)
    audit["excluded_bus_fuel_rows"] = len(bus_exclusions)
    audit["bus_technology_transfers"] = len(bus_transfers)
    audit["bus_transferred_stock_k_vehicles"] = (
        float(bus_transfers["capacity_k_vehicles"].sum())
        if not bus_transfers.empty
        else 0.0
    )
    audit["excluded_bus_fuel_pj"] = (
        {
            str(member): float(value)
            for member, value in bus_exclusions.groupby("fuel_member")[
                "excluded_energy_pj"
            ].sum().items()
        }
        if not bus_exclusions.empty
        else {}
    )
    audit["bus_ineligible_raw_stock_k_vehicles"] = float(
        bus_cohorts.loc[
            ~bus_cohorts["eligible_first_model_period"], "raw_cohort_k_vehicles"
        ].sum()
    )
    audit["excluded_road_fuel_rows_by_reason"] = (
        {
            str(reason): int(count)
            for reason, count in road_exclusions.groupby("exclusion_reason")
            .size()
            .items()
        }
        if not road_exclusions.empty
        else {}
    )
    audit["road_ineligible_raw_stock_k_vehicles"] = float(
        road_cohorts.loc[
            ~road_cohorts["eligible_first_model_period"],
            "raw_cohort_k_vehicles",
        ].sum()
    )
    road_totals = road_shares.groupby(["region", "road_class", "tech"])[
        ["raw_capacity", "capacity"]
    ].sum()
    retired = road_shares.loc[~road_shares["eligible_first_model_period"], "raw_capacity"]
    audit["road_technology_redistribution"] = {
        "composition_basis": "configured_historical_vintage_coverage",
        "redistribution_scope": "same_region_and_technology",
        "checked_technology_totals": len(road_totals),
        "redistributed_stock_k_vehicles": float(retired.sum()),
        "max_conservation_error_k_vehicles": float(
            (road_totals["capacity"] - road_totals["raw_capacity"]).abs().max()
        ),
    }
    audit["offroad_ineligible_raw_capacity_by_unit"] = {
        str(units): float(value)
        for units, value in offroad_cohorts.loc[
            ~offroad_cohorts["eligible_first_model_period"]
        ]
        .groupby("units")["raw_capacity_at_base_year"]
        .sum()
        .items()
    }
    audit["excluded_air_gasoline_rows"] = len(air_exclusions)
    audit["input_content_digest"] = input_digest.hexdigest()
    audit["accepted_survival_sha256"] = hashlib.sha256(
        lifetime_frames["transformed_curves"].to_csv(index=False).encode("utf-8")
    ).hexdigest()
    audit["max_excluded_fuel_source_share"] = (
        [
            {
                "road_class": road_class,
                "fuel_type": fuel_type,
                "max_share": float(share),
            }
            for (road_class, fuel_type), share in road_exclusions.groupby(
                ["road_class", "fuel_type"]
            )["excluded_source_share"]
            .max()
            .items()
        ]
        if not road_exclusions.empty
        else []
    )
    audit["excluded_air_gasoline_pj"] = {
        str(mode): float(value)
        for mode, value in air_exclusions.groupby("mode")["excluded_energy_pj"]
        .sum()
        .items()
    }
    observed_2023 = (
        offroad_annual.loc[offroad_annual["year"].eq(base_year)]
        .groupby("mode")["capacity"]
        .sum()
    )
    surviving_2023 = offroad_cohorts.groupby("mode")["capacity_at_base_year"].sum()
    audit["offroad_surviving_minus_observed_base_year"] = {
        str(mode): float(surviving_2023[mode] - observed_2023[mode])
        for mode in sorted(observed_2023.index)
    }
    audit["offroad_early_retired_capacity_by_mode"] = {
        str(mode): float(value)
        for mode, value in offroad_cohorts.groupby("mode")["early_retired_capacity"]
        .sum()
        .items()
    }
    retained = {(row.region, row.tech, row.vintage) for row in rows}
    road_parameter_ready = road_capacity.loc[
        road_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    bus_parameter_ready = bus_capacity.loc[
        bus_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    offroad_parameter_ready = offroad_capacity.loc[
        offroad_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    artifact_frames = (
        (road_cohorts, interim_output / road_rules["age_cohorts_file"]),
        (road_shares, interim_output / road_rules["fuel_shares_file"]),
        (road_exclusions, validation_output / road_rules["exclusions_file"]),
        (bus_cohorts, interim_output / bus_rules["age_cohorts_file"]),
        (bus_shares, interim_output / bus_rules["activity_shares_file"]),
        (bus_exclusions, validation_output / bus_rules["exclusions_file"]),
        (bus_transfers, validation_output / bus_rules["transfers_file"]),
        (offroad_annual, interim_output / offroad_rules["annual_capacity_file"]),
        (offroad_cohorts, interim_output / offroad_rules["cohort_additions_file"]),
        (air_exclusions, validation_output / "excluded_air_gasoline_energy.csv"),
        (road_parameter_ready, processed_output / road_rules["capacity_file"]),
        (bus_parameter_ready, processed_output / bus_rules["capacity_file"]),
        (offroad_parameter_ready, processed_output / offroad_rules["capacity_file"]),
    )
    for frame, path in artifact_frames:
        write_dataframe_atomic(frame, path)
    write_dataframe_atomic(
        pd.DataFrame([row.model_dump(mode="python") for row in rows]),
        processed_output / "existing_capacity.csv",
    )
    annual_stock_path = interim_output / road_rules["fuel_shares_file"]
    audit["annual_stock_evidence"] = {
        "path": str(annual_stock_path.relative_to(bundle.repo_root)
                    if annual_stock_path.is_relative_to(bundle.repo_root) else annual_stock_path),
        "sha256": file_sha256(annual_stock_path),
    }
    audit["research_evidence"] = {
        name: {"path": str(path), "sha256": file_sha256(path)}
        for name, path in {
            "road_age_cohorts": interim_output / road_rules["age_cohorts_file"],
            **({"sales_cohorts": interim_output / road_rules["sales_cohorts_file"]}
               if not sales_cohorts.empty else {}),
        }.items()
    }
    publish_capacity_handoff(
        bundle, rows=rows, contexts=contexts, audit=audit, physical_stock=physical_stock,
        artifact_paths=[
            *(path for _, path in artifact_frames),
            processed_output / "existing_capacity.csv",
            *([interim_output / road_rules["sales_cohorts_file"]] if not sales_cohorts.empty else []),
        ],
    )
    LOGGER.info(
        "Prepared %s existing-capacity rows across %s regions; removed %s below epsilon=%s; excluded %s road fuel shares",
        len(rows),
        len(audit["regions"]),
        len(removed),
        scenario.existing_capacity.cleanup_tolerance,
        len(road_exclusions),
    )
    LOGGER.info("Redistributed %.3f thousand road vehicles within the same technology; bus transfers=%s (%.3f thousand vehicles)",
                float(retired.sum()), audit["bus_technology_transfers"], audit["bus_transferred_stock_k_vehicles"])
    return ExistingCapacityPreparation(rows, physical_stock, annual_acquisitions, contexts, audit)


def prepare_existing_capacity_rows(
    bundle: ConfigBundle,
) -> ExistingCapacityPreparation:
    """Reusable caller-owned preparation seam for standalone and CANOE-main."""
    return build_existing_capacity_artifacts(bundle)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/default.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    build_existing_capacity_artifacts(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
