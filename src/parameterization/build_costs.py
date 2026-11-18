"""Build validated transport investment and period-by-vintage variable costs offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from functools import cache
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd
from parameterization.ceud_inputs import read_ceud_input
from canoe_schema.v4_0 import CostInvest, CostVariable

from fetching.nlr_atb_autonomie import configured_trajectory
from fetching.epa_omega_baseline import SOURCE as OMEGA, COMPONENT as OMEGA_COMPONENT, build_request as omega_request
from parameterization.ldv_ev_ranges import RangeEvidence, existing_range_weights, module_rules as range_module_rules
from parameterization.currency import CerCurrencyConverter
from parameterization.build_lifetime_parameters import lifetime_endpoints
from parameterization.offroad_capex_opex import (
    aircraft_cost_per_billion_service,
    aircraft_service_evidence,
    manual_offroad_capex,
    offroad_opex_from_capex_ratio,
)
from parameterization.road_capex_opex import (
    LDV_REPAIR_PRICE_INDEX,
    aggregate_purchase_price,
    bean_maintenance_per_mile,
    ldv_repair_msrp_factor,
    ldv_repair_per_mile,
    select_nlr_purchase_prices,
)
from parameterization.road_efficiencies import (
    derive_load_factors,
    interpolate,
)
from parameterization.road_fleet_weights import aggregation_component, load_fleet_aggregation_evidence
from utils import (
    ConfigBundle,
    file_sha256,
    harmonization_rule_paths,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
    write_dataframe_atomic,
)
from validation.insertion import validate_parameter_rows
from validation.provenance import (
    ResolvedProvenance,
    resolve_composite_provenance,
    resolve_provenance,
    source_id_mapping,
)


LOGGER = logging.getLogger(__name__)
ATB = "nlr_atb_transportation"
BEAN = "anl_autonomie_bean"
REGEN = "epri_us_regen_transportation"
GCAM = "jgcri_gcam_motorcycle_inputs"
FAA = "faa_economic_values"
CIMS = "emrg_sfu_cims_model"
OEO = "open_energy_outlook"
CER = "cer_canadas_energy_future"
CEUD = "nrcan_ceud_transport_provincial"


@dataclass(frozen=True)
class CostPreparation:
    invest_rows: list[CostInvest]
    variable_rows: list[CostVariable]
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, Any]


def _read(path: Path, paths: list[Path]) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"Empty cost evidence: {path}")
    paths.append(path)
    return frame


def _cost_trajectory_year(
    frame: pd.DataFrame, year: int, *, year_col: str, endpoint_policy: str,
) -> int:
    """Hold the final cost formulation; never manufacture a historical backfill."""
    if endpoint_policy != "hold_last":
        raise ValueError("Cost trajectory endpoint policy must be hold_last")
    if frame.empty:
        raise ValueError("Missing cost trajectory")
    years = pd.to_numeric(frame[year_col], errors="raise")
    if years.isna().any() or not years.map(isfinite).all():
        raise ValueError("Invalid cost trajectory years")
    return min(year, int(years.max()))


def _cost_input_fingerprints(bundle: ConfigBundle, data_paths: list[Path]) -> list[tuple[str, str]]:
    """Keep data paths root-bound and external controls identified by their role."""
    rule_paths = harmonization_rule_paths(bundle)
    controls = {
        "rules": rule_paths[0],
        **{f"rules:{path.name}": path for path in rule_paths[1:]},
        "conversion": resolve_parameter_path(bundle, "conversion.yaml"),
        "sources": bundle.sources_path,
        "scenario": bundle.scenario_path,
    }
    if all(path.is_relative_to(bundle.repo_root) for path in controls.values()):
        # Preserve existing default-build identities, including path ordering.
        return [(str(path.relative_to(bundle.repo_root)), file_sha256(path))
                for path in sorted(set(data_paths) | set(controls.values()))]
    fingerprints = {
        str(path.relative_to(bundle.repo_root)): file_sha256(path)
        for path in sorted(set(data_paths))
    }
    for role, path in controls.items():
        label = (str(path.relative_to(bundle.repo_root)) if path.is_relative_to(bundle.repo_root)
                 else f"configuration:{role}")
        if label.startswith("configuration:") and label in fingerprints:
            raise ValueError(f"Cost input conflicts with a reserved configuration role: {label}")
        fingerprints[label] = file_sha256(path)
    # Sorting portable identities, rather than external machine paths, makes an
    # identical selected control bundle independent of its storage directory.
    return sorted(fingerprints.items())


def _metadata(bundle: ConfigBundle, source: str, component: str) -> tuple[str, int]:
    adapter = bundle.sources.sources[source].components[component].adapter
    currency = str(adapter["native_cost_currency"])
    year = int(adapter["native_cost_dollar_year"])
    if currency not in {"CAD", "USD"}:
        raise ValueError(f"Invalid source-native currency: {source}/{component}")
    return currency, year


def _region_weights(
    bundle: ConfigBundle, prices: pd.DataFrame, paths: list[Path],
    efficiency: dict,
) -> dict[tuple[str, str], dict[str, float]]:
    fleet = load_fleet_aggregation_evidence(bundle)
    paths.extend(fleet.paths)
    weight_frame = fleet.ldv
    md_classes = sorted(prices.loc[prices.family.eq("mhdv"), "vehicle_class"].unique())
    result: dict[tuple[str, str], dict[str, float]] = {}
    for region in bundle.scenario.geography.regions:
        medium = fleet.medium_weights(region, md_classes)
        heavy = fleet.heavy_weights(region)
        for mode, spec in efficiency["road_classes"].items():
            if spec["pathway"] == "ldv":
                selected = weight_frame.loc[
                    weight_frame.nrcan_ceud_class.eq(spec["weights"])
                ]
                weights = selected.set_index("nlr_atb_class").aggregation_weight.to_dict()
            elif mode == "medium_trucks":
                weights = medium
            elif mode == "heavy_trucks":
                weights = heavy
            elif spec["pathway"] in {"bus", "intercity"}:
                weights = {c: 1 / len(spec["atb_classes"]) for c in spec["atb_classes"]}
            else:
                continue
            if not weights or abs(sum(weights.values()) - 1) > 1e-9:
                raise ValueError(f"Invalid reviewed road weights for {region}/{mode}")
            result[region, mode] = weights
    return result


def _context(
    bundle: ConfigBundle, *, key: str, components: list[tuple[str, str]],
    governing: str, digest: str, version: str,
    governing_component: str | int | None = None,
    extra_inputs: list[ResolvedProvenance] | None = None,
) -> ResolvedProvenance:
    inputs = [
        resolve_provenance(
            bundle.sources, source_key=source, component_key=component,
            transformation="transport_cost_input", transformation_version=version,
        )
        for source, component in sorted(set(components))
    ]
    governing_id = source_id_mapping(bundle.sources)[governing]
    governing_inputs = [
        item for item in inputs
        if item.source_id == governing_id and (
            governing_component is None
            or item.component_key == str(governing_component)
        )
    ]
    if len(governing_inputs) != 1:
        raise ValueError(f"Cost DQ requires one native cost component for {key}")
    return resolve_composite_provenance(
        inputs=[*inputs, *(extra_inputs or [])], dataset_key="transport_cost." + key,
        transformation="Transport cost normalization and currency harmonization",
        transformation_version=version, governing_source_id=governing_id,
        data_quality=governing_inputs[0].data_quality,
        value_variant={"input_digest": digest, "periods": bundle.scenario.periods.model_dump()},
    )


def validate_cost_outputs(
    invest: list[CostInvest], variable: list[CostVariable], *,
    expected_invest: set[tuple[str, str, int]],
    expected_variable: set[tuple[str, int, str, int]],
    reference_year: int,
) -> dict[str, Any]:
    invest_keys = {(r.region, r.tech, r.vintage) for r in invest}
    variable_keys = {(r.region, r.period, r.tech, r.vintage) for r in variable}
    if invest_keys != expected_invest or variable_keys != expected_variable:
        raise ValueError(
            f"Cost coverage mismatch: investment {len(invest_keys)}/{len(expected_invest)}, "
            f"variable {len(variable_keys)}/{len(expected_variable)}"
        )
    if any(r.tech.endswith("_EX") for r in invest):
        raise ValueError("Investment costs apply only to new technologies")
    if len(invest_keys) != len(invest) or len(variable_keys) != len(variable):
        raise ValueError("Duplicate transport cost keys")
    for row in [*invest, *variable]:
        if row.cost is None or not isfinite(row.cost) or row.cost < 0:
            raise ValueError("Invalid transport cost value")
        if row.data_id is None or row.data_source is None:
            raise ValueError("Transport cost lacks source provenance")
        if any(
            getattr(row, field) is None
            for field in ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")
        ):
            raise ValueError("Transport cost lacks complete source data quality")
        if not row.units or f"{reference_year}CAD" not in row.units:
            raise ValueError("Transport cost has wrong currency-year units")
        if isinstance(row, CostVariable) and not row.units.endswith(
            ("/ bn passenger-km", "/ bn tonne-km")
        ):
            raise ValueError("Variable cost must be per service activity")
        if isinstance(row, CostInvest) and not row.units.endswith(
            ("/ k vehicles", "/ bn passenger-km", "/ bn tonne-km")
        ):
            raise ValueError("Investment cost has unsupported denominator")
    return {
        "cost_invest_rows": len(invest), "cost_variable_rows": len(variable),
        "invest_technologies": len({r.tech for r in invest}),
        "variable_technologies": len({r.tech for r in variable}),
        "period_vintage_pairs": len({(r.period, r.vintage) for r in variable}),
    }


def prepare_cost_rows(
    bundle: ConfigBundle, *, existing_capacity_rows: list | None = None,
    existing_capacity_contexts: list[ResolvedProvenance] | None = None,
    fixed_lifetime_rows: list | None = None,
    survival_curve_rows: list | None = None,
    lifetime_provenance_contexts: list[ResolvedProvenance] | None = None,
    range_evidence: RangeEvidence | None = None,
) -> CostPreparation:
    """Build both cost parameters without opening a SQLite connection.

    Caller-owned lifetime batches and motorcycle stock require their matching
    provenance contexts.
    Variable-cost datasets retain the lifetime sources governing their coverage;
    the native cost component continues to govern the final row's source and DQ.
    """
    if (bundle.scenario.lifetimes.survival_curves and existing_capacity_rows is not None
            and (fixed_lifetime_rows is None or survival_curve_rows is None)):
        raise ValueError(
            "Curve-mode costs with supplied capacity require matching prepared lifetime batches"
        )
    supplied_lifetimes = fixed_lifetime_rows is not None or survival_curve_rows is not None
    if supplied_lifetimes and (fixed_lifetime_rows is None or survival_curve_rows is None):
        raise ValueError("Cost lifetime handoff requires matching prepared lifetime batches")
    if supplied_lifetimes and lifetime_provenance_contexts is None:
        raise ValueError("Supplied cost lifetime batches require matching lifetime provenance contexts")
    rules = load_harmonization_rules(bundle, "costs")
    endpoint_policy = rules["trajectory_endpoint_policy"]
    if endpoint_policy != "hold_last":
        raise ValueError("Cost trajectory endpoint policy must be hold_last")
    bean_age_policy = rules["bean_maintenance_age_policy"]
    if bean_age_policy != "linear_throughout_life":
        raise ValueError("BEAN maintenance age policy must be linear_throughout_life")
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    conversions = load_conversion_factors(bundle)
    scenario = bundle.scenario
    historical_weights, range_evidence = existing_range_weights(bundle, evidence=range_evidence)
    if scenario.economics.cost_reference_currency != "CAD":
        raise ValueError("Transport costs require a configured CAD reference currency")
    reference_year = scenario.economics.cost_reference_year
    trajectory = configured_trajectory(bundle, parameter="costs")
    cer_rules = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = scenario.sources.selections[CER].edition
    paths: list[Path] = []
    macro = _read(
        resolve_input_path(
            bundle, "interim", cer_rules["interim_subdir_template"].format(edition=edition),
            cer_rules["components"]["macro-indicators"]["output_file"],
        ), paths,
    )
    converter = CerCurrencyConverter(
        macro, scenario=scenario.economics.cer_scenario, target_year=reference_year,
    )
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    atb_dir = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"])
    atb_vehicles = _read(
        atb_dir / atb_rules["components"]["vehicles"]["output_file"], paths,
    )
    atb_currency, atb_year = _metadata(bundle, ATB, "vehicles")
    prices = select_nlr_purchase_prices(
        atb_vehicles, trajectory=trajectory, efficiency_rules=efficiency,
        rpe_markups=rules["nlr_road_retail_price_equivalent_markup"],
        source_currency=atb_currency, source_dollar_year=atb_year,
    )
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    assorted_dir = resolve_input_path(bundle, "interim", assorted["interim_subdir"])
    gcam = _read(assorted_dir / assorted["jgcri_gcam"]["output_file"], paths)
    regen = _read(assorted_dir / assorted["epri_us_regen"]["output_file"], paths)
    faa_capacity = _read(assorted_dir / assorted["faa"]["capacity_output_file"], paths)
    faa_maintenance = _read(assorted_dir / assorted["faa"]["maintenance_output_file"], paths)
    bean = _read(atb_dir / atb_rules["components"]["anl_bean"]["output_file"], paths)
    burnham_rules = atb_rules["components"]["maintenance_ldv"]
    burnham = {
        name: _read(atb_dir / filename, paths)
        for name, filename in burnham_rules["outputs"].items()
    }
    invest_manual_path = resolve_input_path(bundle, "manual", "cost_invest_multipliers.csv")
    variable_manual_path = resolve_input_path(bundle, "manual", "cost_variable_multipliers.csv")
    manual_invest = _read(invest_manual_path, paths)
    manual_variable = _read(variable_manual_path, paths)
    from parameterization.manual_parameters import validate_manual_registry

    manual_rules = load_harmonization_rules(bundle, "manual_parameters")
    validate_manual_registry(
        bundle, source_column=manual_rules["source_column"],
        notes_column=manual_rules["notes_column"],
        selected_files={invest_manual_path.name, variable_manual_path.name},
    )
    technology = _read(resolve_input_path(bundle, "template", "technology.csv"), paths)
    weights = _region_weights(bundle, prices, paths, efficiency)
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])
    ceud = pd.concat([
        read_ceud_input(bundle, region)
        for region in scenario.geography.regions
    ], ignore_index=True)
    paths.extend(ceud_dir / ceud_rules["region_output_template"].format(region=region.lower())
                 for region in scenario.geography.regions)
    if set(ceud.source_id) != {CEUD}:
        raise ValueError("Wrong CEUD provenance for cost load factors")
    road_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")
    road_demand = road_rules["demand"]
    loads = derive_load_factors(
        ceud, rules=efficiency, activity_rules=road_demand["activity_series"],
        conversions=conversions,
    )
    gcam_currency, gcam_year = _metadata(bundle, GCAM, "canada_motorcycle_inputs")
    regen_currency, regen_year = _metadata(bundle, REGEN, "intercity_bus_charts")
    bean_currency, bean_year = _metadata(bundle, BEAN, "mhdv_maintenance_coefficients")
    faa_currency, faa_year = _metadata(bundle, FAA, "section_4_operating_costs")
    burnham_currency, burnham_year = _metadata(bundle, ATB, "maintenance_ldv")
    price_index_source, price_index_component = LDV_REPAIR_PRICE_INDEX
    usd_price_index = bundle.sources.sources[price_index_source].component(price_index_component).adapter
    repair_msrp_factor = ldv_repair_msrp_factor(
        price_basis=(atb_currency, atb_year), repair_basis=(burnham_currency, burnham_year),
        usd_price_index=usd_price_index["index_by_year"],
    )
    if set(gcam.source_id) != {GCAM} or set(regen.source_id) != {REGEN}:
        raise ValueError("GCAM or REGEN normalized cost source identity changed")
    if set(bean.source_id) != {BEAN} or set(faa_maintenance.source_id) != {FAA}:
        raise ValueError("BEAN or FAA normalized cost source identity changed")
    capacity_preparation = None
    if existing_capacity_rows is None:
        from parameterization.build_existing_capacity import prepare_existing_capacity_rows

        capacity_preparation = prepare_existing_capacity_rows(bundle)
        existing_capacity_rows = capacity_preparation.existing_capacity_rows
        existing_capacity_contexts = capacity_preparation.provenance_contexts
    existing_keys = {
        (r.region, r.tech, r.vintage)
        for r in existing_capacity_rows if r.capacity > 0
    }
    if fixed_lifetime_rows is None or survival_curve_rows is None:
        from parameterization.build_lifetime_parameters import prepare_lifetime_rows

        lifetimes = prepare_lifetime_rows(
            bundle,
            annual_acquisitions=capacity_preparation.annual_acquisitions if capacity_preparation else None,
            cohort_provenance_contexts=capacity_preparation.provenance_contexts if capacity_preparation else None,
        )
        fixed_lifetime_rows = lifetimes.fixed_rows
        survival_curve_rows = lifetimes.curve_rows
        lifetime_provenance_contexts = lifetimes.provenance_contexts
    endpoints = lifetime_endpoints(fixed_lifetime_rows, survival_curve_rows)
    lifetime_contexts: dict[str, ResolvedProvenance] = {}
    for context in lifetime_provenance_contexts or []:
        previous = lifetime_contexts.get(context.data_id)
        if previous is not None and previous != context:
            raise ValueError(f"Conflicting lifetime provenance context: {context.data_id}")
        lifetime_contexts[context.data_id] = context
    fixed_data_ids = {(row.region, row.tech): {row.data_id} for row in fixed_lifetime_rows}
    curve_data_ids: dict[tuple[str, str, int], set[str]] = {}
    for row in survival_curve_rows:
        curve_data_ids.setdefault((row.region, row.tech, row.vintage), set()).add(row.data_id)
    missing_ids = {row.data_id for row in [*fixed_lifetime_rows, *survival_curve_rows]} - lifetime_contexts.keys()
    if missing_ids:
        raise ValueError(f"Cost lifetime rows lack matching provenance contexts: {sorted(map(str, missing_ids))}")

    motorcycle_techs = set(technology.loc[technology.category.eq("motorcycles"), "tech"])
    motorcycle_service: dict[str, dict[str, Any]] = {}
    stock_contexts = {context.data_id: context for context in existing_capacity_contexts or []}
    if motorcycle_techs:
        from parameterization.build_demand import road_base_year_activity

        motorcycle_stock_owners = set(road_rules["existing_capacity"]["fuel_technology"]["motorcycles"].values())
        if any(row.tech in motorcycle_techs | motorcycle_stock_owners for row in survival_curve_rows):
            raise ValueError("Motorcycle maintenance normalization requires physical stock under fixed lifetimes")
        motorcycle_stock = [row for row in existing_capacity_rows
                            if row.tech in motorcycle_stock_owners and row.capacity > 0]
        if not motorcycle_stock:
            from parameterization.build_existing_capacity import prepare_existing_capacity_rows

            # A new-only contribution still needs a fleet reference for the service divisor.
            reference = prepare_existing_capacity_rows(bundle)
            motorcycle_stock = [row for row in reference.existing_capacity_rows
                                if row.tech in motorcycle_stock_owners and row.capacity > 0]
            stock_contexts.update({context.data_id: context for context in reference.provenance_contexts})
        activity, _ = road_base_year_activity(bundle, macro=macro)
        for target in activity.loc[activity.service.eq("motorcycles")].itertuples(index=False):
            # Fixed-lifetime motorcycle capacity is the prepared physical reference-year count.
            stock = [row for row in motorcycle_stock if row.region == target.region]
            count = sum(row.capacity for row in stock)
            service = float(target.projected_base_year_demand)
            if not isfinite(count) or count <= 0 or not isfinite(service) or service <= 0:
                raise ValueError(f"Motorcycle maintenance requires positive calibrated service and stock: {target.region}")
            data_ids = sorted({row.data_id for row in stock})
            if set(data_ids) - stock_contexts.keys():
                raise ValueError(f"Motorcycle maintenance requires prepared stock provenance: {target.region}")
            # Convert the bn pkm/k vehicle ratio (million pkm/vehicle) to thousand pkm/vehicle.
            annual_service = service / count * conversions["service"]["million_service_per_thousand_vehicles"]
            motorcycle_service[target.region] = {
                "service_base_year": target.base_year,
                "service_ceud_observed_year": target.ceud_observed_year,
                "base_year_activity_bn": service,
                "base_year_stock_k_vehicles": count,
                "annual_service_thousand_passenger_km_per_vehicle": annual_service,
                "stock_data_ids": data_ids,
            }

    def process_lifetime_ids(region: str, tech: str, vintage: int) -> set[str]:
        if (region, tech) in fixed_data_ids:
            return fixed_data_ids[region, tech]
        return curve_data_ids[region, tech, vintage]
    def active_period(region: str, period: int, tech: str, vintage: int) -> bool:
        return vintage <= period < endpoints.end_year(region, tech, vintage)
    region_map = efficiency["region_output_map"]
    values: list[dict[str, Any]] = []
    units_vehicle = f"$M {reference_year}CAD / k vehicles"
    def service_unit(mode: str) -> str:
        basis = efficiency["road_classes"].get(mode, {}).get("service")
        if basis is None:
            basis = "freight" if mode in {"freight_air", "freight_rail", "freight_marine"} else "passenger"
        return f"$M {reference_year}CAD / bn {'passenger-km' if basis == 'passenger' else 'tonne-km'}"

    def add(
        *, family: str, region: str, tech: str, vintage: int, mode: str,
        source_value: float, currency: str, dollar_year: int, source_unit: str,
        divisor: float, components: list[tuple[str, str]], governing: str,
        treatment: str, period: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conversion = converter.convert(source_value, currency=currency, dollar_year=dollar_year)
        final = conversion.target_cad_value / divisor
        if not isfinite(final) or final < 0:
            raise ValueError(f"Invalid harmonized transport cost: {tech}/{vintage}")
        values.append({
            "family": family, "region": region, "tech": tech, "vintage": vintage,
            "period": period, "mode": mode, "cost": final,
            "units": units_vehicle if family == "invest" and mode in efficiency["road_classes"] else service_unit(mode),
            "source_value": source_value, "source_unit": source_unit,
            **conversion.__dict__, "normalization_divisor": divisor,
            "components": components + [(CER, "macro-indicators")],
            "governing": governing, "treatment": treatment,
            **(details or {}),
        })

    trajectory_holds: set[tuple[str, int, int]] = set()

    def cost_year(frame: pd.DataFrame, year: int, label: str, *, year_col: str = "source_year") -> int:
        selected_year = _cost_trajectory_year(
            frame, year, year_col=year_col, endpoint_policy=endpoint_policy,
        )
        if selected_year < year:
            trajectory_holds.add((label, year, selected_year))
        return selected_year

    # Physical prices and repair curves repeat across regional/vintage rows.
    # These caches belong to this preparation's freshly read source evidence.
    @cache
    def nlr_purchase_price(
        powertrain: str, year: int, class_weights: tuple[tuple[str, float], ...],
    ) -> tuple[float, float]:
        if (abs(sum(weight for _, weight in class_weights) - 1) > 1e-9
                or any(weight < 0 for _, weight in class_weights)):
            raise ValueError("Road purchase-cost class weights do not sum to one")
        values = []
        for vehicle_class, weight in class_weights:
            selected = prices.loc[
                prices.powertrain.eq(powertrain) & prices.vehicle_class.eq(vehicle_class)
            ]
            selected_year = cost_year(selected, year, f"ATB/{powertrain}/{vehicle_class}")
            msrp, manufacturing = aggregate_purchase_price(
                prices, powertrain=powertrain, year=selected_year, weights={vehicle_class: 1.0},
            )
            values.append((weight * msrp, weight * manufacturing))
        return sum(value[0] for value in values), sum(value[1] for value in values)

    def road_price(mode: str, powertrain: str, year: int, region: str) -> tuple[float, str, int, str, list[tuple[str, str]]]:
        """Native per-vehicle price, retaining the source-specific RPE treatment."""
        if mode == "motorcycles":
            selected = gcam.loc[
                gcam.source_variable.eq("Capital costs (purchase)")
                & gcam.source_technology.eq(efficiency["motorcycle"][powertrain])
            ]
            if set(selected.source_unit) != {"2005$/veh"}:
                raise ValueError("GCAM motorcycle purchase-cost unit changed")
            amount = interpolate(selected, cost_year(selected, year, f"GCAM/purchase/{powertrain}"), "source_value", year_col="source_year")
            return amount, gcam_currency, gcam_year, "GCAM motorcycle purchase price", [(GCAM, "canada_motorcycle_inputs")]
        if mode == "inter_city_buses":
            keys = rules["intercity_purchase_source_keys"]
            source_powertrain = powertrain
            selected = regen.loc[
                regen.metric.eq("purchase_cost")
                & regen.source_technology_key.eq(keys[source_powertrain])
            ]
            if set(selected.native_unit) != {"$"}:
                raise ValueError("REGEN intercity purchase-price unit changed")
            amount = interpolate(selected, cost_year(selected, year, f"REGEN/purchase/{powertrain}"), "source_value", year_col="source_year")
            components = [(REGEN, "intercity_bus_charts")]
            if powertrain == "hev":
                transit_weights = tuple(weights[region, "urban_transit"].items())
                _, diesel_cost = nlr_purchase_price("diesel", year, transit_weights)
                _, hev_cost = nlr_purchase_price("hev", year, transit_weights)
                amount *= hev_cost / diesel_cost
                components.append((ATB, "vehicles"))
            return amount, regen_currency, regen_year, "REGEN intercity purchase price", components
        selected_powertrain = rules["missing_mhdv_purchase_powertrain_proxy"].get(
            powertrain, powertrain
        ) if efficiency["road_classes"][mode]["pathway"] != "ldv" else powertrain
        msrp, manufacturing = nlr_purchase_price(
            selected_powertrain, year, tuple(weights[region, mode].items()),
        )
        family = "ldv" if efficiency["road_classes"][mode]["pathway"] == "ldv" else "mhdv"
        markup = rules["nlr_road_retail_price_equivalent_markup"][family]
        treatment = f"NLR manufacturing price (retail / {markup:g}; {family} RPE)"
        if selected_powertrain != powertrain:
            treatment += f"; reviewed {powertrain}->{selected_powertrain} purchase proxy"
        return manufacturing, atb_currency, atb_year, treatment, [(ATB, "vehicles"), *weight_components(mode, region)]

    @cache
    def weight_components(mode: str, region: str) -> tuple[tuple[str, str | int], ...]:
        pathway = efficiency["road_classes"][mode]["pathway"]
        if pathway == "ldv":
            return (aggregation_component(bundle, "ldv", region),)
        if mode == "medium_trucks":
            return (aggregation_component(bundle, "medium_trucks", region),)
        if mode == "heavy_trucks":
            return (aggregation_component(bundle, "heavy_truck_haul", region),)
        return ()

    @cache
    def load_at(region: str, mode: str) -> float:
        selected = loads.loc[loads.region.eq(region) & loads["mode"].eq(mode)]
        return interpolate(selected, scenario.periods.base_year, "load_factor")

    @cache
    def ldv_repair_msrp(bucket: str, vehicle_class: str, source_year: int) -> float:
        """Reuse the rebased retail input across periods and regional mixtures."""
        selected = prices.loc[
            prices.powertrain.eq(bucket) & prices.vehicle_class.eq(vehicle_class)
        ]
        retail_msrp = interpolate(
            selected, cost_year(selected, source_year, f"ATB/MSRP/{bucket}/{vehicle_class}"), "vehicle_price_usd_2022_per_vehicle",
            year_col="source_year",
        )
        return retail_msrp * repair_msrp_factor

    @cache
    def ldv_maintenance_cost(age: int, msrp: float, vehicle_class: str, powertrain: str) -> float:
        return ldv_repair_per_mile(
            age=age, msrp=msrp, vehicle_class=vehicle_class, powertrain=powertrain,
            baseline=burnham["baseline_repair_cost"],
            class_multipliers=burnham["class_multipliers"],
            powertrain_multipliers=burnham["powertrain_multipliers"],
            maintenance=burnham["maintenance_cost"],
            powertrain_labels=rules["ldv_powertrain_evidence"],
            price_exponent=rules["ldv_repair_price_exponent_per_usd"],
            max_age=rules["ldv_repair_max_age"],
        )["total"]

    @cache
    def mhdv_maintenance_cost(vehicle_class: str, powertrain: str, age: int) -> float:
        return bean_maintenance_per_mile(
            bean, vehicle_class=vehicle_class, powertrain=powertrain, age=age,
        )["source_cost_per_mile"]

    regions = {r: region_map.get(r, r) for r in scenario.geography.regions}
    periods = list(scenario.periods.model)
    for source_region, region in regions.items():
        for item in technology.itertuples(index=False):
            mode = item.category
            if mode not in efficiency["road_classes"] and mode not in {
                "passenger_air", "freight_air", "passenger_rail", "freight_rail", "freight_marine"
            }:
                continue
            powertrain = efficiency["subcategory_aliases"].get(item.sub_category, item.sub_category)
            is_existing = item.tech.endswith("_EX")
            range_mix = historical_weights.get((mode, powertrain), {}) if is_existing else {}
            vintages = (
                [v for v in scenario.periods.existing if (region, item.tech, v) in existing_keys]
                if is_existing else periods
            )
            for vintage in vintages:
                calendar_year = (
                    scenario.periods.historical_years(vintage)[-1]
                    if is_existing and scenario.periods.period_mode == "prospective"
                    else vintage if is_existing
                    else scenario.periods.evaluation_year(vintage)
                )
                source_year = max(calendar_year, int(prices.source_year.min())) if is_existing else calendar_year
                if mode in efficiency["road_classes"]:
                    if not is_existing:
                        price, curr, year, treatment, components = road_price(
                            mode, powertrain, source_year, source_region
                        )
                        rpe_family = (
                            "ldv" if efficiency["road_classes"][mode]["pathway"] == "ldv" else "mhdv"
                        ) if treatment.startswith("NLR") else None
                        applied_rpe = (
                            rules["nlr_road_retail_price_equivalent_markup"][rpe_family]
                            if rpe_family is not None else None
                        )
                        add(
                            family="invest", region=region, tech=item.tech,
                            vintage=vintage, mode=mode, source_value=price,
                            currency=curr, dollar_year=year,
                            source_unit=f"{year} {curr}/vehicle", divisor=1000,
                            components=components, governing=components[0][0],
                            treatment=treatment,
                            details={"purchase_evidence_year": source_year,
                                     "rpe_applied": treatment.startswith("NLR"),
                                     "rpe_family": rpe_family,
                                     "source_retail_price_per_vehicle": (
                                         price * applied_rpe if applied_rpe is not None else price
                                     ),
                                     "rpe_markup": applied_rpe,
                                     "source_raw_unit": (
                                         "$" if mode == "inter_city_buses" else
                                         "2005$/veh" if mode == "motorcycles" else
                                         f"{atb_year} {atb_currency}/vehicle"
                                     ),
                                     "aggregation_weights": json.dumps(
                                         weights[source_region, mode], sort_keys=True
                                     ) if mode not in {"inter_city_buses", "motorcycles"} else None},
                        )
                    for period in periods:
                        if period < vintage or not active_period(region, period, item.tech, vintage):
                            continue
                        age = (
                            scenario.periods.period_end(period) - calendar_year
                            if is_existing and scenario.periods.period_mode == "prospective"
                            else period - vintage
                        )
                        load = load_at(source_region, mode)
                        if mode in {"cars", "passenger_light_trucks", "freight_light_trucks"}:
                            class_costs = []
                            weighted_msrp = 0.0
                            for bucket, range_weight in (range_mix or {powertrain: 1.0}).items():
                                for vehicle_class, class_weight in weights[source_region, mode].items():
                                    weight = range_weight * class_weight
                                    msrp = ldv_repair_msrp(bucket, vehicle_class, source_year)
                                    class_costs.append(weight * ldv_maintenance_cost(
                                        age, msrp, vehicle_class, bucket,
                                    ))
                                    weighted_msrp += weight * msrp
                            cost_per_mile = sum(class_costs)
                            components = [(ATB, "vehicles"), (ATB, "maintenance_ldv"), LDV_REPAIR_PRICE_INDEX,
                                          (CEUD, road_demand["activity_series"][mode]["table_id"]),
                                          *weight_components(mode, source_region)]
                            treatment = f"Burnham repair at {burnham_year} {burnham_currency} MSRP plus maintenance"
                            if range_mix:
                                components.append((OMEGA, OMEGA_COMPONENT))
                                treatment += "; OMEGA range-weighted representative"
                            currency, dollar_year = burnham_currency, burnham_year
                        elif mode == "motorcycles":
                            selected = gcam.loc[
                                gcam.source_variable.eq("Operating costs (maintenance)")
                                & gcam.source_technology.eq(efficiency["motorcycle"][powertrain])
                            ]
                            if set(selected.source_unit) != {"2005$/veh/yr"}:
                                raise ValueError("GCAM motorcycle maintenance-cost unit changed")
                            source_cost_year = cost_year(selected, calendar_year, f"GCAM/maintenance/{powertrain}")
                            amount = interpolate(selected, source_cost_year, "source_value", year_col="source_year")
                            service_basis = motorcycle_service[region]
                            add(
                                family="variable", region=region, tech=item.tech,
                                vintage=vintage, period=period, mode=mode,
                                source_value=amount, currency=gcam_currency,
                                dollar_year=gcam_year, source_unit="2005$/vehicle-year",
                                divisor=service_basis["annual_service_thousand_passenger_km_per_vehicle"],
                                components=[(GCAM, "canada_motorcycle_inputs"), (CEUD, road_demand["activity_series"][mode]["table_id"])],
                                governing=GCAM, treatment="GCAM annual motorcycle maintenance divided by calibrated base-year service per vehicle",
                                details={"source_cost_year": source_cost_year, "cost_evaluation_year": calendar_year,
                                         **service_basis},
                            )
                            continue
                        else:
                            # BEAN uses the same class aggregation as vehicle CAPEX.
                            selected_powertrain = rules["bean_powertrain_evidence"][powertrain]
                            bean_weights = weights[source_region, mode]
                            total = 0.0
                            for vehicle_class, weight in bean_weights.items():
                                if vehicle_class in rules["bean_class_alias"]:
                                    bean_class = rules["bean_class_alias"][vehicle_class]
                                else:
                                    match = re.match(efficiency["atb"]["class_pattern"], vehicle_class)
                                    if match is None:
                                        raise ValueError(f"No BEAN class mapping for {vehicle_class}")
                                    bean_class = rules["bean_gvwr_proxy"][int(match[1])]
                                total += weight * mhdv_maintenance_cost(bean_class, selected_powertrain, age)
                            cost_per_mile = total
                            currency, dollar_year = bean_currency, bean_year
                            components = [(BEAN, "mhdv_maintenance_coefficients"), (CEUD, road_demand["activity_series"][mode]["table_id"]), *weight_components(mode, source_region)]
                            treatment = "BEAN class-weighted age-dependent maintenance"
                        if mode in {"school_buses", "inter_city_buses"}:
                            transit_bean_native_cost_per_mile = cost_per_mile
                            transit_price, transit_currency, transit_year, _, _ = road_price(
                                "urban_transit", powertrain, source_year, source_region
                            )
                            target_price, target_currency, target_year, _, target_components = road_price(
                                mode, powertrain, source_year, source_region
                            )
                            transit_cad = converter.convert(
                                transit_price, currency=transit_currency, dollar_year=transit_year
                            ).target_cad_value
                            target_cad = converter.convert(
                                target_price, currency=target_currency, dollar_year=target_year
                            ).target_cad_value
                            cost_per_mile = converter.convert(
                                cost_per_mile, currency=currency, dollar_year=dollar_year
                            ).target_cad_value * target_cad / transit_cad
                            currency, dollar_year = "CAD", reference_year
                            components.extend(target_components + [(ATB, "vehicles")])
                            treatment = "Transit BEAN OPEX/CAPEX ratio applied to bus purchase price"
                        add(
                            family="variable", region=region, tech=item.tech,
                            vintage=vintage, period=period, mode=mode,
                            source_value=cost_per_mile, currency=currency,
                            dollar_year=dollar_year,
                            source_unit=f"{dollar_year} {currency}/mile",
                            divisor=conversions["length"]["mile_to_km"] * load / 1000,
                            components=components, governing=components[0][0],
                            treatment=treatment,
                            details={"age": age, "load_factor": load,
                                     "maintenance_formula_age": min(age, rules["ldv_repair_max_age"]) if mode in {
                                         "cars", "passenger_light_trucks", "freight_light_trucks"
                                     } else age,
                                     "maintenance_age_endpoint_held": age > rules["ldv_repair_max_age"] if mode in {
                                         "cars", "passenger_light_trucks", "freight_light_trucks"
                                     } else False,
                                     "range_weights": json.dumps(range_mix, sort_keys=True) if range_mix else None,
                                     "purchase_evidence_year": source_year,
                                     "aggregation_weights": json.dumps(
                                         weights[source_region, mode], sort_keys=True
                                     ),
                                     "historical_msrp_proxy": is_existing and calendar_year < int(prices.source_year.min()),
                                     "weighted_repair_msrp": weighted_msrp if mode in {
                                         "cars", "passenger_light_trucks", "freight_light_trucks"
                                     } else None,
                                     **({
                                         "transit_bean_native_cost_per_mile": transit_bean_native_cost_per_mile,
                                         "transit_purchase_native": transit_price,
                                         "transit_purchase_currency": transit_currency,
                                         "transit_purchase_dollar_year": transit_year,
                                         "target_bus_purchase_native": target_price,
                                         "target_bus_purchase_currency": target_currency,
                                         "target_bus_purchase_dollar_year": target_year,
                                     } if mode in {"school_buses", "inter_city_buses"} else {})},
                        )
                else:
                    service_kind = "passenger" if mode == "passenger_air" else "cargo"
                    if mode in {"passenger_air", "freight_air"}:
                        if powertrain == "spk" and rules["spk_aircraft_cost_proxy"] != "jet_fuel":
                            raise ValueError("Unreviewed SPK aircraft cost proxy")
                        evidence = aircraft_service_evidence(
                            faa_capacity, operating_group=service_kind,
                            mile_to_km=conversions["length"]["mile_to_km"],
                            us_short_ton_to_metric_tonne=conversions["mass"]["us_short_ton_to_metric_tonne"],
                            operating_days_per_year=rules["aircraft_operating_days_per_year"],
                        )
                        capex = manual_invest.loc[
                            manual_invest.category.eq(mode)
                            & manual_invest.sub_category.eq("jet_fuel")
                            & manual_invest.parameter.eq("service_unit_capex")
                        ]
                        if len(capex) != 1:
                            raise ValueError(f"Missing aircraft CIMS CAPEX for {mode}")
                        capex = capex.iloc[0]
                        if capex.unit not in {"CAD", "USD"} or pd.isna(capex.currency_year):
                            raise ValueError("Aircraft CAPEX lacks source-native currency-year")
                        native_capex = float(capex.value) / evidence.annual_service_output * 1000
                        invest_components = [(CIMS, "transport_service_output_and_capex"), (FAA, "section_3_capacity")]
                        if not is_existing:
                            capex_treatment = (
                                "CIMS MDO baseline with reviewed manual HFO parity"
                                if powertrain == "hfo" else
                                "CIMS service CAPEX times REGEN relative multiplier"
                            )
                            add(
                                family="invest", region=region, tech=item.tech,
                                vintage=vintage, mode=mode, source_value=native_capex,
                                currency=str(capex.unit), dollar_year=int(capex.currency_year),
                                source_unit=f"{int(capex.currency_year)} {capex.unit} M/bn service-km",
                                divisor=1, components=invest_components, governing=CIMS,
                                treatment="CIMS aircraft CAPEX normalized by FAA All Aircraft annual service output",
                                details={"aircraft_operating_group": service_kind,
                                         "aircraft_capex_per_unit_native": float(capex.value),
                                         "aircraft_capex_native_unit": f"{int(capex.currency_year)} {capex.unit}/aircraft",
                                         "annual_service_output": evidence.annual_service_output,
                                         "operating_days_per_year": evidence.operating_days_per_year,
                                         "daily_block_hours": evidence.daily_block_hours,
                                         "block_speed_mph": evidence.block_speed_mph,
                                         "capacity_native": evidence.capacity_native,
                                         "capacity_metric": evidence.capacity_metric,
                                         "load_factor_percent": evidence.load_factor_percent,
                                         "us_short_ton_to_metric_tonne": evidence.us_short_ton_to_metric_tonne,
                                         "fuel_proxy": powertrain == "spk"},
                            )
                        maintenance = faa_maintenance.loc[
                            faa_maintenance.operating_group.eq(service_kind)
                            & faa_maintenance.metric.eq("maintenance_cost_per_block_hour")
                            & faa_maintenance.aircraft_category.eq("All Aircraft")
                        ]
                        if len(maintenance) != 1 or set(maintenance.unit) != {"USD per block hour"}:
                            raise ValueError("FAA aggregate maintenance cost is missing or ambiguous")
                        native_opex = aircraft_cost_per_billion_service(
                            cost_per_block_hour=float(maintenance.value.iloc[0]),
                            evidence=evidence,
                        )
                        for period in periods:
                            if period < vintage or not active_period(region, period, item.tech, vintage):
                                continue
                            add(
                                family="variable", region=region, tech=item.tech,
                                vintage=vintage, period=period, mode=mode,
                                source_value=native_opex, currency=faa_currency,
                                dollar_year=faa_year,
                                source_unit=f"{faa_year} {faa_currency} M/bn service-km",
                                divisor=1,
                                components=[(FAA, "section_4_operating_costs"), (FAA, "section_3_capacity")],
                                governing=FAA,
                                treatment="FAA All Aircraft maintenance per block-hour and service output",
                                details={"aircraft_operating_group": service_kind,
                                         "maintenance_cost_per_block_hour_native": float(maintenance.value.iloc[0]),
                                         "maintenance_source_unit": "USD per block hour",
                                         "block_speed_mph": evidence.block_speed_mph,
                                         "capacity_native": evidence.capacity_native,
                                         "capacity_metric": evidence.capacity_metric,
                                         "load_factor_percent": evidence.load_factor_percent,
                                         "daily_block_hours": evidence.daily_block_hours,
                                         "us_short_ton_to_metric_tonne": evidence.us_short_ton_to_metric_tonne,
                                         "fuel_proxy": powertrain == "spk"},
                            )
                    else:
                        native = manual_offroad_capex(
                            manual_invest, mode=mode, powertrain=powertrain,
                            year=max(calendar_year, scenario.periods.base_year),
                            period_years=rules["offroad_multiplier_period_year"],
                        )
                        components = [(CIMS, "transport_service_output_and_capex")]
                        if powertrain not in {"diesel", "mdo", "hfo"}:
                            components.append((REGEN, "nonroad_cost_invest_multipliers"))
                        if not is_existing:
                            add(
                                family="invest", region=region, tech=item.tech,
                                vintage=vintage, mode=mode,
                                source_value=native["native_millions_per_billion_service"],
                                currency=native["source_currency"],
                                dollar_year=native["source_dollar_year"],
                                source_unit=f"{native['source_dollar_year']} {native['source_currency']} M/bn service-km",
                                divisor=1,
                                components=components, governing=CIMS,
                                treatment=capex_treatment,
                                details={"baseline_cost_per_vehicle": native["native_cost_per_vehicle"] / native["multiplier"],
                                         "multiplier": native["multiplier"],
                                         "service_output_thousands": native["service_output_thousands"]},
                            )
                        oeo = manual_variable.loc[
                            manual_variable.category.eq(mode)
                            & manual_variable.sub_category.eq("all")
                            & manual_variable.parameter.eq("variable_to_capex_ratio")
                        ]
                        if len(oeo) != 1:
                            raise ValueError(f"Missing class-specific OEO ratio for {mode}")
                        native_opex = offroad_opex_from_capex_ratio(
                            native["native_millions_per_billion_service"],
                            ratio=float(oeo.value.iloc[0]),
                        )
                        for period in periods:
                            if period < vintage or not active_period(region, period, item.tech, vintage):
                                continue
                            add(
                                family="variable", region=region, tech=item.tech,
                                vintage=vintage, period=period, mode=mode,
                                source_value=native_opex,
                                currency=native["source_currency"],
                                dollar_year=native["source_dollar_year"],
                                source_unit=f"{native['source_dollar_year']} {native['source_currency']} M/bn service-km",
                                divisor=1,
                                components=components + [(OEO, "transport_variable_cost_multipliers")],
                                governing=CIMS,
                                treatment="Class-specific OEO OPEX/CAPEX ratio on CIMS service baseline",
                                details={"multiplier": native["multiplier"],
                                         "opex_ratio": float(oeo.value.iloc[0]),
                                         "service_output_thousands": native["service_output_thousands"]},
                            )

    if not values:
        raise ValueError("No transport costs were derived")
    lifetime_signature = sorted([
        (row.region, row.tech, "fixed", row.lifetime, row.data_id)
        for row in fixed_lifetime_rows
    ] + [
        (row.region, row.tech, "curve", row.period, row.vintage, row.fraction, row.data_id)
        for row in survival_curve_rows
    ])
    digest = hashlib.sha256(json.dumps(
        {
            "files": _cost_input_fingerprints(bundle, paths),
            "lifetime_signature": lifetime_signature,
            "motorcycle_service": motorcycle_service,
            "ceud_beyond_observed": scenario.sources.nrcan_ceud_beyond_observed,
            "ceud_extrapolation": load_harmonization_rules(bundle, "ceud_inputs")["cagr"] if scenario.sources.nrcan_ceud_beyond_observed == "extrapolated" else None,
            "base_year": scenario.periods.base_year,
            "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
            "fleet_rules": load_harmonization_rules(bundle, "road_aggregation"),
            "historical_range_rules": range_module_rules(bundle).existing.model_dump(),
            "historical_range_weights": range_evidence.bucket_totals.to_dict("records"),
            "omega_extract_sha256": file_sha256(omega_request(bundle).extract_path),
        },
        sort_keys=True,
    ).encode()).hexdigest()
    grouped: dict[tuple, list[dict[str, Any]]] = {}
    for item in values:
        key = (
            item["family"], item["mode"], item["governing"],
            tuple(sorted(set(tuple(pair) for pair in item["components"]))),
        )
        grouped.setdefault(key, []).append(item)
    contexts: list[ResolvedProvenance] = []
    invest_rows: list[CostInvest] = []
    variable_rows: list[CostVariable] = []
    for (family, mode, governing, components), group in sorted(grouped.items()):
        # Cost evidence governs DQ; supporting prices or service denominators do not.
        if governing == ATB:
            governing_component = "vehicles" if family == "invest" else "maintenance_ldv"
        elif governing == FAA:
            governing_component = "section_4_operating_costs"
        else:
            governing_component = None
        used_lifetime_ids = sorted({
            data_id
            for entry in group if family == "variable"
            for data_id in process_lifetime_ids(entry["region"], entry["tech"], entry["vintage"])
        })
        used_stock_ids = sorted({data_id for entry in group for data_id in entry.get("stock_data_ids", [])})
        context = _context(
            bundle, key=f"{family}.{mode}.{hashlib.sha256(repr(components).encode()).hexdigest()[:8]}",
            components=list(components), governing=governing, digest=digest,
            version=rules["transformation_version"],
            governing_component=governing_component,
            extra_inputs=[lifetime_contexts[data_id] for data_id in used_lifetime_ids]
            + [stock_contexts[data_id] for data_id in used_stock_ids],
        )
        contexts.append(context)
        for entry in group:
            entry["data_id"] = context.data_id
            entry["lifetime_data_ids"] = json.dumps(used_lifetime_ids)
        records = [{
            "region": entry["region"], "tech": entry["tech"],
            "vintage": entry["vintage"], "cost": entry["cost"],
            "units": entry["units"],
            **({"period": entry["period"]} if family == "variable" else {}),
        } for entry in group]
        rows = validate_parameter_rows(
            CostInvest if family == "invest" else CostVariable,
            records, context, bundle=bundle,
        )
        (invest_rows if family == "invest" else variable_rows).extend(rows)
    invest_rows.sort(key=lambda r: (r.region, r.tech, r.vintage))
    variable_rows.sort(key=lambda r: (r.region, r.period, r.tech, r.vintage))
    allowed_modes = set(efficiency["road_classes"]) | {
        "passenger_air", "freight_air", "passenger_rail", "freight_rail", "freight_marine"
    }
    expected_invest = {
        (region, item.tech, vintage)
        for region in regions.values()
        for item in technology.itertuples(index=False)
        if item.category in allowed_modes and item.tech.endswith("_N")
        for vintage in periods
    }
    expected_variable = {
        (region, period, item.tech, vintage)
        for region in regions.values()
        for item in technology.itertuples(index=False)
        if item.category in allowed_modes
        for vintage in (
            [v for v in scenario.periods.existing if (region, item.tech, v) in existing_keys]
            if item.tech.endswith("_EX") else periods
        )
        for period in periods
        if period >= vintage and active_period(region, period, item.tech, vintage)
    }
    audit = validate_cost_outputs(
        invest_rows, variable_rows, expected_invest=expected_invest,
        expected_variable=expected_variable, reference_year=reference_year,
    )
    conversion_groups = []
    for (currency, year), group in pd.DataFrame(values).groupby(
        ["source_currency", "source_dollar_year"], sort=True
    ):
        factors = {
            field: sorted(set(float(entry[field]) for entry in group.to_dict("records")))
            for field in ("cad_per_usd", "source_year_deflator", "target_year_deflator")
        }
        if any(len(levels) != 1 for levels in factors.values()):
            raise ValueError(f"Inconsistent CER conversion for {currency}/{year}")
        summary = {
            "source_currency": currency, "source_dollar_year": int(year),
            "target_currency": "CAD", "target_dollar_year": reference_year,
            "rows": len(group),
            **{field: levels[0] for field, levels in factors.items()},
        }
        conversion_groups.append(summary)
        LOGGER.debug("CER transport cost conversion: %s", summary)
    audit.update({
        "period_mapping": scenario.periods.audit(),
        "input_digest": digest, "atb_scenario": trajectory,
        "motorcycle_maintenance_service_basis": motorcycle_service,
        "trajectory_endpoint_policy": endpoint_policy,
        "maintenance_age_policies": {
            "ldv": {"policy": "hold_last", "max_age": rules["ldv_repair_max_age"]},
            "bean": bean_age_policy,
        },
        "variable_age_endpoint_held_rows": sum(
            bool(item.get("maintenance_age_endpoint_held")) for item in values
        ),
        "trajectory_endpoint_holds": [
            {"trajectory": label, "requested_year": requested, "source_endpoint_year": endpoint}
            for label, requested, endpoint in sorted(trajectory_holds)
        ],
        "atb_selected_purchase_trajectories": prices.groupby(
            ["vehicle_class", "trajectory", "source_trajectory"], as_index=False,
        ).size().rename(columns={"size": "source_rows"}).to_dict("records"),
        "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
        "cer_scenario": scenario.economics.cer_scenario,
        "reference_currency": "CAD", "reference_year": reference_year,
        "currency_year_transformations": sorted({
            (item["source_currency"], item["source_dollar_year"])
            for item in values
        }),
        "currency_conversion_groups": conversion_groups,
        "historical_msrp_proxy_rows": sum(
            bool(item.get("historical_msrp_proxy")) for item in values
        ),
        "variable_period_vintage_pairs_excluded_by_lifetime": sum(
            1
            for region in regions.values()
            for item in technology.itertuples(index=False)
            if item.category in allowed_modes
            for vintage in (
                [v for v in scenario.periods.existing if (region, item.tech, v) in existing_keys]
                if item.tech.endswith("_EX") else periods
            )
            for period in periods
            if period >= vintage and not active_period(region, period, item.tech, vintage)
        ),
        "source_metadata_provisional": {
            source: bundle.sources.sources[source].components[component].adapter.get(
                "cost_metadata_status"
            )
            for source, component in (
                (BEAN, "mhdv_maintenance_coefficients"),
                (GCAM, "canada_motorcycle_inputs"),
                (REGEN, "intercity_bus_charts"),
                (FAA, "section_4_operating_costs"),
            )
            if bundle.sources.sources[source].components[component].adapter.get(
                "cost_metadata_status"
            ) == "provisional_review"
        },
        "source_cost_metadata": [
            {
                "source": source, "component": component,
                "currency": registration.adapter["native_cost_currency"],
                "dollar_year": registration.adapter["native_cost_dollar_year"],
                "status": registration.adapter.get("cost_metadata_status", "source_documented"),
            }
            for source, source_registration in sorted(bundle.sources.sources.items())
            for component, registration in sorted(source_registration.components.items(), key=lambda item: str(item[0]))
            if "native_cost_currency" in registration.adapter
        ],
        "source_components": sorted({
            f"{source.source_key}/{component.component_key}"
            for context in contexts for source in context.contributors for component in source.components
        }),
    })
    interim = resolve_artifact_path(bundle, "costs_interim")
    processed = resolve_artifact_path(bundle, "costs_processed")
    validation = resolve_artifact_path(bundle, "costs_validation")
    evidence = pd.DataFrame(values)
    evidence["cost_metadata_status"] = evidence.components.map(
        lambda pairs: "provisional_review" if any(
            bundle.sources.sources[source].components[component].adapter.get("cost_metadata_status") == "provisional_review"
            for source, component in pairs
        ) else "declared_metadata"
    )
    output_contexts = {context.data_id: context for context in contexts}
    evidence["source_components"] = evidence["data_id"].map(
        lambda data_id: json.dumps(sorted({
            f"{source.source_key}/{component.component_key}"
            for source in output_contexts[data_id].contributors for component in source.components
        }))
    )
    evidence = evidence.rename(columns={"governing": "governing_source_key"}).drop(
        columns=["components"]
    )
    for family, road, filename in (
        ("invest", True, rules["files"]["road_invest"]),
        ("variable", True, rules["files"]["road_variable"]),
        ("invest", False, rules["files"]["offroad_invest"]),
        ("variable", False, rules["files"]["offroad_variable"]),
    ):
        selected = evidence.loc[
            evidence.family.eq(family)
            & evidence["mode"].isin(efficiency["road_classes"]).eq(road)
        ].copy()
        write_dataframe_atomic(selected, interim / filename)
    write_dataframe_atomic(
        evidence[[
            "family", "region", "mode", "tech", "period", "vintage",
            "source_value", "source_unit", "source_currency", "source_dollar_year",
            "cad_per_usd", "source_year_deflator", "target_year_deflator",
            "cad_value", "target_cad_value", "normalization_divisor", "cost",
            "units", "treatment", "governing_source_key", "source_components",
            "cost_metadata_status", "data_id", "lifetime_data_ids",
        ]], interim / rules["files"]["currency_audit"],
    )
    invest_frame = pd.DataFrame([row.model_dump(mode="json") for row in invest_rows])
    variable_frame = pd.DataFrame([row.model_dump(mode="json") for row in variable_rows])
    write_dataframe_atomic(invest_frame, processed / "cost_invest.csv")
    write_dataframe_atomic(variable_frame, processed / "cost_variable.csv")
    road_techs = set(technology.loc[
        technology.category.isin(efficiency["road_classes"]), "tech"
    ])
    for frame, road_file, offroad_file in (
        (invest_frame, rules["files"]["road_invest"], rules["files"]["offroad_invest"]),
        (variable_frame, rules["files"]["road_variable"], rules["files"]["offroad_variable"]),
    ):
        write_dataframe_atomic(
            frame.loc[frame.tech.isin(road_techs)], processed / road_file
        )
        write_dataframe_atomic(
            frame.loc[~frame.tech.isin(road_techs)], processed / offroad_file
        )
    validation.mkdir(parents=True, exist_ok=True)
    (validation / rules["files"]["integrity"]).write_text(
        json.dumps(audit, sort_keys=True, indent=2) + "\n", encoding="utf-8",
    )
    LOGGER.info("Prepared cost_invest=%s and cost_variable=%s rows; converted %s currency/year groups to %s CAD",
                len(invest_rows), len(variable_rows), len(conversion_groups), reference_year)
    LOGGER.info("Rebased LDV retail MSRP from %s to %s USD for repair using the U.S. GDP deflator (factor %.8f)",
                atb_year, burnham_year, repair_msrp_factor)
    LOGGER.info("Cost data handling: excluded %s expired period/vintage pairs; historical MSRP proxy rows=%s",
                audit["variable_period_vintage_pairs_excluded_by_lifetime"], audit["historical_msrp_proxy_rows"])
    if trajectory_holds:
        LOGGER.info("Held %s cost trajectory/year selections at their final source formulation", len(trajectory_holds))
    LOGGER.debug("Transport cost audit: %s", audit)
    return CostPreparation(invest_rows, variable_rows, contexts, audit)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/default.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    prepare_cost_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
