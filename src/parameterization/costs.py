"""Prepare transport investment and period-by-vintage variable costs offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import CostInvest, CostVariable

from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.currency import CerCurrencyConverter
from parameterization.offroad_capex_opex import (
    aircraft_cost_per_billion_service,
    aircraft_service_evidence,
    manual_offroad_capex,
    offroad_opex_from_capex_ratio,
)
from parameterization.road_capex_opex import (
    aggregate_purchase_price,
    bean_maintenance_per_mile,
    ldv_repair_per_mile,
    select_nlr_purchase_prices,
)
from parameterization.road_efficiencies import (
    derive_load_factors,
    interpolate,
    medium_vocation_weights,
)
from parameterization.road_utilization import heavy_haul_atb_weights
from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
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
ATB = "nlr_atb_transportation_2024"
BEAN = "anl_autonomie_bean_2022"
REGEN = "epri_us_regen_2025_transportation"
GCAM = "jgcri_gcam_motorcycle_inputs"
FAA = "faa_economic_values_2024"
CIMS = "emrg_sfu_cims_model"
OEO = "open_energy_outlook_2022"
CER = "cer_canadas_energy_future"
CEUD = "nrcan_ceud_transport_provincial"
MTO = "ontario_ministry_transport_vehicle_population"
STATCAN = "statcan_transport_tables"


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
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    weight_frame = _read(
        resolve_artifact_path(bundle, "road_aggregation", aggregation["nlr_weights_file"]),
        paths,
    )
    if weight_frame.report_year.nunique() != 1:
        raise ValueError("Ambiguous reviewed LDV road aggregation edition")
    ontario = load_harmonization_rules(bundle, "ontario_vehicle_population")
    template = ontario["reports"][4]["distribution_output_template"]
    report_dir = resolve_input_path(bundle, "interim", ontario["interim_subdir"])
    reports = sorted(report_dir.glob(template.format(year="*")))
    if not reports:
        raise ValueError("Missing reviewed MTO Report 4 evidence")
    report4 = _read(reports[-1], paths)
    if set(report4.year) != set(weight_frame.report_year):
        raise ValueError("LDV and MDV weights use different MTO editions")
    md_classes = sorted(prices.loc[prices.family.eq("mhdv"), "vehicle_class"].unique())
    medium = medium_vocation_weights(report4, md_classes, rules=efficiency)
    statcan = load_harmonization_rules(bundle, "statcan_tables")
    freight = _read(
        resolve_input_path(
            bundle, "interim", statcan["interim_subdir"],
            statcan["freight"]["output_file"],
        ), paths,
    )
    utilization = load_harmonization_rules(bundle, "road_stocks_and_demands")["capacity_factor"]
    result: dict[tuple[str, str], dict[str, float]] = {}
    for region in bundle.scenario.geography.regions:
        haul = heavy_haul_atb_weights(
            freight,
            source_region=utilization["freight_source_region_map"].get(region, region),
            rules=utilization,
        )
        haul_share = haul.set_index("nlr_atb_class").aggregation_weight.to_dict()
        heavy = {
            vocation: float(haul_share[utilization["heavy_haul_atb_classes"][haul_class]])
            / len(vocations)
            for haul_class, vocations in efficiency["heavy_vocations"].items()
            for vocation in vocations
        }
        for mode, spec in efficiency["road_classes"].items():
            if spec["pathway"] == "ldv":
                selected = weight_frame.loc[
                    weight_frame.weight_basis.eq(efficiency["rating_weight_basis"])
                    & weight_frame.nrcan_ceud_class.eq(spec["weights"])
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
) -> ResolvedProvenance:
    inputs = [
        resolve_provenance(
            bundle.sources, source_key=source, component_key=component,
            transformation="transport_cost_input", transformation_version=version,
        )
        for source, component in sorted(set(components))
    ]
    governing_id = source_id_mapping(bundle.sources)[governing]
    return resolve_composite_provenance(
        inputs=inputs, dataset_key="transport_cost." + key,
        transformation="Transport cost normalization and currency harmonization",
        transformation_version=version, governing_source_id=governing_id,
        value_variant={"input_digest": digest},
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
    fixed_lifetime_rows: list | None = None,
    survival_curve_rows: list | None = None,
) -> CostPreparation:
    """Build and validate both cost parameters without opening a SQLite connection."""
    rules = load_harmonization_rules(bundle, "costs")
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    conversions = load_conversion_factors(bundle)
    scenario = bundle.scenario
    if (
        scenario.existing_capacity.other_region_vehicle_population_source
        != efficiency["supported_population_source"]
    ):
        raise ValueError("Selected road population source lacks reviewed cost weights")
    if scenario.economics.cost_reference_currency != "CAD":
        raise ValueError("Transport costs require a configured CAD reference currency")
    reference_year = scenario.economics.cost_reference_year
    trajectory = configured_trajectory(bundle)
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
        macro, scenario=scenario.demand.cer_scenario, target_year=reference_year,
    )
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    atb_dir = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"])
    atb_vehicles = _read(
        atb_dir / atb_rules["components"]["vehicles"]["output_file"], paths,
    )
    atb_currency, atb_year = _metadata(bundle, ATB, "vehicles")
    prices = select_nlr_purchase_prices(
        atb_vehicles, trajectory=trajectory, efficiency_rules=efficiency,
        rpe_markup=rules["nlr_road_retail_price_equivalent_markup"],
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
        _read(
            ceud_dir / ceud_rules["region_output_template"].format(region=region.lower()),
            paths,
        )
        for region in scenario.geography.regions
    ], ignore_index=True)
    if set(ceud.source_id) != {CEUD}:
        raise ValueError("Wrong CEUD provenance for cost load factors")
    road_demand = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    loads = derive_load_factors(
        ceud, rules=efficiency, activity_rules=road_demand["activity_series"],
        conversions=conversions,
    )
    gcam_currency, gcam_year = _metadata(bundle, GCAM, "canada_motorcycle_inputs")
    regen_currency, regen_year = _metadata(bundle, REGEN, "intercity_bus_charts")
    bean_currency, bean_year = _metadata(bundle, BEAN, "mhdv_maintenance_coefficients")
    faa_currency, faa_year = _metadata(bundle, FAA, "section_4_operating_costs")
    burnham_currency, burnham_year = _metadata(bundle, ATB, "maintenance_ldv")
    if set(gcam.source_id) != {GCAM} or set(regen.source_id) != {REGEN}:
        raise ValueError("GCAM or REGEN normalized cost source identity changed")
    if set(bean.source_id) != {BEAN} or set(faa_maintenance.source_id) != {FAA}:
        raise ValueError("BEAN or FAA normalized cost source identity changed")
    if existing_capacity_rows is None:
        from parameterization.existing_capacity import prepare_existing_capacity_rows

        existing_capacity_rows, _, _ = prepare_existing_capacity_rows(bundle)
    existing_keys = {
        (r.region, r.tech, r.vintage)
        for r in existing_capacity_rows if r.capacity > 0
    }
    if fixed_lifetime_rows is None or survival_curve_rows is None:
        from parameterization.lifetime_parameters import prepare_lifetime_rows

        lifetimes = prepare_lifetime_rows(bundle)
        fixed_lifetime_rows = lifetimes.fixed_rows
        survival_curve_rows = lifetimes.curve_rows
    fixed_lives = {
        (row.region, row.tech): float(row.lifetime)
        for row in fixed_lifetime_rows
    }
    curve_owners = {(row.region, row.tech) for row in survival_curve_rows}
    curve_keys = {
        (row.region, row.period, row.tech, row.vintage)
        for row in survival_curve_rows if row.fraction is not None and row.fraction > 0
    }
    if set(fixed_lives) & curve_owners:
        raise ValueError("Cost lifetime owners have conflicting representations")
    def active_period(region: str, period: int, tech: str, vintage: int) -> bool:
        owner = (region, tech)
        if owner in curve_owners:
            return (region, period, tech, vintage) in curve_keys
        if owner in fixed_lives:
            return period < vintage + fixed_lives[owner]
        raise ValueError(f"Cost technology has no lifetime: {owner}")
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
            "notes": f"{treatment}; {trajectory}; CER {scenario.demand.cer_scenario}",
            "source_value": source_value, "source_unit": source_unit,
            **conversion.__dict__, "normalization_divisor": divisor,
            "components": components + [(CER, "macro-indicators")],
            "governing": governing, "treatment": treatment,
            **(details or {}),
        })

    def road_price(mode: str, powertrain: str, year: int, region: str) -> tuple[float, str, int, str, list[tuple[str, str]]]:
        """Native per-vehicle price, retaining the source-specific RPE treatment."""
        if mode == "motorcycles":
            selected = gcam.loc[
                gcam.source_variable.eq("Capital costs (purchase)")
                & gcam.source_technology.eq(efficiency["motorcycle"][powertrain])
            ]
            if set(selected.source_unit) != {"2005$/veh"}:
                raise ValueError("GCAM motorcycle purchase-cost unit changed")
            amount = interpolate(selected, year, "source_value", year_col="source_year")
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
            amount = interpolate(selected, year, "source_value", year_col="source_year")
            components = [(REGEN, "intercity_bus_charts")]
            if powertrain == "hev":
                _, diesel_cost = aggregate_purchase_price(
                    prices, powertrain="diesel", year=year,
                    weights=weights[region, "urban_transit"],
                )
                _, hev_cost = aggregate_purchase_price(
                    prices, powertrain="hev", year=year,
                    weights=weights[region, "urban_transit"],
                )
                amount *= hev_cost / diesel_cost
                components.append((ATB, "vehicles"))
            return amount, regen_currency, regen_year, "REGEN intercity purchase price", components
        selected_powertrain = rules["missing_mhdv_purchase_powertrain_proxy"].get(
            powertrain, powertrain
        ) if efficiency["road_classes"][mode]["pathway"] != "ldv" else powertrain
        msrp, manufacturing = aggregate_purchase_price(
            prices, powertrain=selected_powertrain, year=year,
            weights=weights[region, mode],
        )
        treatment = "NLR manufacturing price (RPE / 1.5)"
        if selected_powertrain != powertrain:
            treatment += f"; reviewed {powertrain}->{selected_powertrain} purchase proxy"
        return manufacturing, atb_currency, atb_year, treatment, [(ATB, "vehicles"), *weight_components(mode)]

    def weight_components(mode: str) -> list[tuple[str, str | int]]:
        pathway = efficiency["road_classes"][mode]["pathway"]
        if pathway == "ldv":
            return [(MTO, "A")]
        if mode == "medium_trucks":
            return [(MTO, 4)]
        if mode == "heavy_trucks":
            return [(STATCAN, "23-10-0142-01")]
        return []

    def load_at(region: str, mode: str) -> float:
        selected = loads.loc[loads.region.eq(region) & loads["mode"].eq(mode)]
        return interpolate(selected, scenario.periods.base_year, "load_factor")

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
            vintages = (
                [v for v in scenario.periods.existing if (region, item.tech, v) in existing_keys]
                if is_existing else periods
            )
            for vintage in vintages:
                source_year = max(vintage, int(prices.source_year.min())) if is_existing else vintage
                if mode in efficiency["road_classes"]:
                    if not is_existing:
                        price, curr, year, treatment, components = road_price(
                            mode, powertrain, vintage, source_region
                        )
                        add(
                            family="invest", region=region, tech=item.tech,
                            vintage=vintage, mode=mode, source_value=price,
                            currency=curr, dollar_year=year,
                            source_unit=f"{year} {curr}/vehicle", divisor=1000,
                            components=components, governing=components[0][0],
                            treatment=treatment,
                            details={"purchase_evidence_year": vintage,
                                     "rpe_applied": treatment.startswith("NLR"),
                                     "source_retail_price_per_vehicle": (
                                         price * rules["nlr_road_retail_price_equivalent_markup"]
                                         if treatment.startswith("NLR") else price
                                     ),
                                     "rpe_markup": (
                                         rules["nlr_road_retail_price_equivalent_markup"]
                                         if treatment.startswith("NLR") else None
                                     ),
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
                        age = period - vintage
                        load = load_at(source_region, mode)
                        if mode in {"cars", "passenger_light_trucks", "freight_light_trucks"}:
                            selected = prices.loc[prices.powertrain.eq(powertrain)]
                            class_costs = []
                            weighted_msrp = 0.0
                            for vehicle_class, weight in weights[source_region, mode].items():
                                class_prices = selected.loc[selected.vehicle_class.eq(vehicle_class)]
                                msrp = interpolate(
                                    class_prices, source_year,
                                    "vehicle_price_usd_2022_per_vehicle", year_col="source_year",
                                )
                                result = ldv_repair_per_mile(
                                    age=age, msrp=msrp, vehicle_class=vehicle_class,
                                    powertrain=powertrain,
                                    baseline=burnham["baseline_repair_cost"],
                                    class_multipliers=burnham["class_multipliers"],
                                    powertrain_multipliers=burnham["powertrain_multipliers"],
                                    maintenance=burnham["maintenance_cost"],
                                    powertrain_labels=rules["ldv_powertrain_evidence"],
                                    price_exponent=rules["ldv_repair_price_exponent_per_usd"],
                                    max_age=rules["ldv_repair_max_age"],
                                )
                                class_costs.append(weight * result["total"])
                                weighted_msrp += weight * msrp
                            cost_per_mile = sum(class_costs)
                            components = [(ATB, "vehicles"), (ATB, "maintenance_ldv"), (CEUD, road_demand["activity_series"][mode]["table_id"]), *weight_components(mode)]
                            treatment = "Burnham MSRP repair plus maintenance"
                            currency, dollar_year = burnham_currency, burnham_year
                        elif mode == "motorcycles":
                            selected = gcam.loc[
                                gcam.source_variable.eq("Operating costs (maintenance)")
                                & gcam.source_technology.eq(efficiency["motorcycle"][powertrain])
                            ]
                            if set(selected.source_unit) != {"2005$/veh/yr"}:
                                raise ValueError("GCAM motorcycle maintenance-cost unit changed")
                            amount = interpolate(
                                selected, vintage,
                                "source_value", year_col="source_year",
                            )
                            distance = interpolate(
                                loads.loc[loads.region.eq(source_region) & loads["mode"].eq(mode)],
                                scenario.periods.base_year, "distance_km",
                            )
                            denominator = distance * load / 1000
                            add(
                                family="variable", region=region, tech=item.tech,
                                vintage=vintage, period=period, mode=mode,
                                source_value=amount, currency=gcam_currency,
                                dollar_year=gcam_year, source_unit="2005$/vehicle-year",
                                divisor=denominator,
                                components=[(GCAM, "canada_motorcycle_inputs"), (CEUD, road_demand["activity_series"][mode]["table_id"])],
                                governing=GCAM, treatment="GCAM annual motorcycle maintenance per service-km",
                                details={"source_cost_year": vintage, "load_factor": load, "annual_distance_km": distance},
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
                                total += weight * bean_maintenance_per_mile(
                                    bean, vehicle_class=bean_class,
                                    powertrain=selected_powertrain, age=age,
                                )["source_cost_per_mile"]
                            cost_per_mile = total
                            currency, dollar_year = bean_currency, bean_year
                            components = [(BEAN, "mhdv_maintenance_coefficients"), (CEUD, road_demand["activity_series"][mode]["table_id"]), *weight_components(mode)]
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
                                     "purchase_evidence_year": source_year,
                                     "aggregation_weights": json.dumps(
                                         weights[source_region, mode], sort_keys=True
                                     ),
                                     "historical_msrp_proxy": is_existing and vintage < int(prices.source_year.min()),
                                     "weighted_atb_retail_msrp": weighted_msrp if mode in {
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
                            year=max(vintage, scenario.periods.base_year),
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
    digest_inputs = sorted(set(paths) | {
        bundle.repo_root / "config/parameters/rules.yaml",
        bundle.repo_root / "config/parameters/conversion.yaml",
        bundle.repo_root / "config/sources.yaml", bundle.scenario_path,
    })
    lifetime_signature = sorted([
        (row.region, row.tech, "fixed", row.lifetime, row.data_id)
        for row in fixed_lifetime_rows
    ] + [
        (row.region, row.tech, "curve", row.period, row.vintage, row.fraction, row.data_id)
        for row in survival_curve_rows
    ])
    digest = hashlib.sha256(json.dumps(
        {
            "files": [(str(path.relative_to(bundle.repo_root)), file_sha256(path)) for path in digest_inputs],
            "lifetime_signature": lifetime_signature,
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
        context = _context(
            bundle, key=f"{family}.{mode}.{hashlib.sha256(repr(components).encode()).hexdigest()[:8]}",
            components=list(components), governing=governing, digest=digest,
            version=rules["transformation_version"],
        )
        contexts.append(context)
        records = [{
            "region": entry["region"], "tech": entry["tech"],
            "vintage": entry["vintage"], "cost": entry["cost"],
            "units": entry["units"], "notes": entry["notes"],
            **({"period": entry["period"]} if family == "variable" else {}),
        } for entry in group]
        rows = validate_parameter_rows(
            CostInvest if family == "invest" else CostVariable,
            records, context,
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
        LOGGER.info("CER transport cost conversion: %s", summary)
    audit.update({
        "input_digest": digest, "atb_scenario": trajectory,
        "cer_scenario": scenario.demand.cer_scenario,
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
        },
        "source_components": sorted({
            f"{source}/{component}"
            for item in values for source, component in item["components"]
        }),
    })
    interim = resolve_artifact_path(bundle, "costs_interim")
    processed = resolve_artifact_path(bundle, "costs_processed")
    validation = resolve_artifact_path(bundle, "costs_validation")
    evidence = pd.DataFrame(values)
    evidence["source_components"] = evidence["components"].map(
        lambda pairs: json.dumps(sorted({f"{source}/{component}" for source, component in pairs}))
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
    LOGGER.info("Prepared transport costs: %s", audit)
    return CostPreparation(invest_rows, variable_rows, contexts, audit)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    prepare_cost_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
