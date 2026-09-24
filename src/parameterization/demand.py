"""Prepare CEUD transport service demand indexed by CER real GDP."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from math import isfinite
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import Demand

from fetching.cer_enerfuture import configured_edition, configured_scenario
from parameterization.offroad_stocks_and_demands import derive_offroad_baseline_demand
from parameterization.road_stocks_and_demands import (
    derive_road_baseline_demand,
    extrapolate_passenger_ldv_demand,
)
from utils import (
    ConfigBundle,
    active_source_keys,
    file_sha256,
    load_config_bundle,
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
CEUD_PROVINCIAL = "nrcan_ceud_transport_provincial"
CEUD_NATIONAL = "nrcan_ceud_transport_national"
CER = "cer_canadas_energy_future"
DEMAND_UNITS = {"bn passenger-km", "bn tonne-km"}


def real_gdp_indices(
    macro: pd.DataFrame, *, scenario: str, base_year: int,
    periods: list[int], step: int,
) -> dict[int, float]:
    """Map start-labeled periods to end-year GDP divided by base-year GDP."""
    selected = macro.loc[
        macro["scenario"].eq(scenario)
        & macro["region"].eq("Canada")
        & macro["variable_key"].eq("real_gdp")
    ]
    years = {base_year, *(period + step for period in periods)}
    selected = selected.loc[selected["year"].isin(years)].copy()
    if len(selected) != len(years) or set(selected["year"]) != years:
        raise ValueError(f"CER real GDP lacks base or period-end years for {scenario}")
    if selected.duplicated("year").any() or selected["unit"].nunique() != 1:
        raise ValueError("CER real GDP has duplicate years or inconsistent units")
    selected["value"] = pd.to_numeric(selected["value"], errors="raise")
    if any(not isfinite(value) or value <= 0 for value in selected["value"]):
        raise ValueError("CER real GDP must be finite and positive")
    gdp = dict(zip(selected["year"], selected["value"], strict=True))
    return {period: float(gdp[period + step] / gdp[base_year]) for period in periods}


def validate_demand_outputs(
    rows: list[Demand], *, regions: set[str], commodities: set[str],
    periods: list[int], template_units: dict[str, str],
) -> dict[str, Any]:
    """Check complete native keys, units and values before artifact publication."""
    expected = {(region, period, commodity)
                for region in regions for period in periods for commodity in commodities}
    actual = {(row.region, row.period, row.commodity) for row in rows}
    if len(rows) != len(actual) or actual != expected:
        raise ValueError(f"Demand coverage differs: missing={len(expected - actual)}, extra={len(actual - expected)}")
    if not commodities.issubset(template_units):
        raise ValueError(f"Demand references missing template commodities: {commodities - set(template_units)}")
    expected_template_units = {"bn passenger-km": "billion passenger-km",
                               "bn tonne-km": "billion tonne-km"}
    if any(template_units[row.commodity] != expected_template_units.get(row.units)
           for row in rows):
        raise ValueError("Demand units differ from template commodity units")
    if any(row.units not in DEMAND_UNITS or row.demand is None
           or not isfinite(row.demand) or row.demand < 0 for row in rows):
        raise ValueError("Demand contains invalid units or numerical values")
    if any(row.data_source is None or row.data_id is None or any(
        getattr(row, name) is None
        for name in ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")
    ) for row in rows):
        raise ValueError("Demand has incomplete provenance or data-quality fields")
    return {"parameter_rows": len(rows), "regions": sorted(regions),
            "commodities": sorted(commodities), "periods": periods}


def prepare_demand_rows(
    bundle: ConfigBundle,
) -> tuple[list[Demand], list[ResolvedProvenance], dict[str, Any]]:
    """Rebuild parameter-ready demand from registered normalized evidence offline."""
    scenario = bundle.scenario
    required_sources = {CEUD_PROVINCIAL, CEUD_NATIONAL, CER}
    inactive = sorted(required_sources - active_source_keys(bundle))
    if inactive:
        raise ValueError(f"Demand requires active sources: {inactive}")
    base_year = scenario.periods.base_year
    periods = list(scenario.periods.model)
    step = scenario.periods.step
    regions = list(scenario.geography.regions)
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    road_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    offroad_rules = load_harmonization_rules(bundle, "offroad_stocks_and_demands")["demand"]
    cer_rules = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = configured_edition(bundle)
    selected_scenario = configured_scenario(bundle, cer_rules)
    allowed = bundle.sources.sources[CER].adapter["editions"]["allowed"]
    edition_metadata = allowed.get(edition, allowed.get(str(edition)))
    if edition_metadata is None or selected_scenario not in edition_metadata["scenarios"]:
        raise ValueError(f"CER scenario {selected_scenario!r} is not registered for edition {edition}")
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])
    def ceud_file(region: str):
        return ceud_dir / ceud_rules["region_output_template"].format(region=region.lower())
    ceud_files = [ceud_file(region) for region in regions]
    national_file = ceud_file("national")
    provincial = pd.concat([pd.read_csv(path) for path in ceud_files], ignore_index=True)
    national = pd.read_csv(national_file)
    if set(provincial["region"]) != set(regions) or int(provincial["year"].max()) != base_year:
        raise ValueError("Provincial CEUD coverage or edition differs from scenario")
    if int(national["year"].max()) != base_year:
        raise ValueError("National CEUD edition differs from scenario base year")
    macro_file = resolve_input_path(bundle, "interim", str(cer_rules["interim_subdir_template"]).format(edition=edition)) / cer_rules["components"]["macro-indicators"]["output_file"]
    macro = pd.read_csv(macro_file)
    if set(macro["source_id"]) != {CER} or set(macro["edition"]) != {edition}:
        raise ValueError("CER macro artifact identity differs from selected edition")
    indices = real_gdp_indices(macro, scenario=selected_scenario,
                               base_year=base_year, periods=periods, step=step)
    baseline = pd.concat([
        derive_road_baseline_demand(provincial, regions=regions, base_year=base_year, rules=road_rules),
        derive_offroad_baseline_demand(provincial, national, regions=regions,
                                       base_year=base_year, rules=offroad_rules),
    ], ignore_index=True)
    expected_regions = {road_rules["region_output_map"].get(region, region) for region in regions}
    expected_commodities = {
        selector["commodity"] for selector in road_rules["activity_series"].values()
    } | {selector["commodity"] for selector in offroad_rules["energy_intensity_series"].values()}
    if (len(expected_commodities) != len(road_rules["activity_series"]) + len(offroad_rules["energy_intensity_series"])
            or baseline.duplicated(["region", "commodity"]).any()
            or set(baseline["region"]) != expected_regions
            or set(baseline["commodity"]) != expected_commodities
            or len(baseline) != len(expected_regions) * len(expected_commodities)):
        raise ValueError("CEUD baseline demand has incomplete or duplicate service coverage")
    future_car_demand = scenario.demand.future_car_demand
    car_services = {"cars", "passenger_light_trucks"}
    car_projection: dict[tuple[str, int, str], float] = {}
    car_audit: list[dict[str, Any]] = []
    if future_car_demand == "extrapolated":
        car_projection, car_audit = extrapolate_passenger_ldv_demand(
            provincial, baseline, regions=regions, base_year=base_year,
            periods=periods, step=step, gdp_indices=indices, rules=road_rules,
        )
    elif future_car_demand != "GDP-indexed":
        raise ValueError(f"Unsupported future_car_demand: {future_car_demand}")
    template = pd.read_csv(resolve_input_path(bundle, "template", "commodity.csv"))
    template_units = dict(zip(template["name"], template["units"], strict=True))
    road_rules_for_gdp = {
        key: value for key, value in road_rules.items() if key != "car_extrapolation"
    }
    digest = hashlib.sha256(json.dumps({
        "inputs": [(path.name, file_sha256(path)) for path in [*ceud_files, national_file, macro_file]],
        "road_rules": road_rules_for_gdp, "offroad_rules": offroad_rules,
    }, sort_keys=True).encode()).hexdigest()
    variant = {"base_year": base_year, "periods": periods, "step": step,
               "cer_edition": edition, "cer_scenario": selected_scenario,
               "input_content_digest": digest}
    source_ids = source_id_mapping(bundle.sources)
    cer_context = resolve_provenance(bundle.sources, source_key=CER,
                                     component_key="macro-indicators",
                                     transformation="demand_input", transformation_version="1")
    rows: list[Demand] = []
    contexts: list[ResolvedProvenance] = []
    for family, selectors in (("road", road_rules["activity_series"]),
                              ("offroad", offroad_rules["energy_intensity_series"])):
        for service, selector in selectors.items():
            components = [(CEUD_PROVINCIAL, selector.get("table_id", selector.get("provincial_table")))]
            reallocates_passenger_ldv = (
                future_car_demand == "extrapolated" and family == "road"
                and service in car_services
            )
            if reallocates_passenger_ldv and service == "passenger_light_trucks":
                components.append((
                    CEUD_PROVINCIAL, road_rules["activity_series"]["cars"]["table_id"]
                ))
            if family == "offroad":
                components.append((CEUD_NATIONAL, selector["national_table"]))
            inputs = [resolve_provenance(bundle.sources, source_key=source,
                                         component_key=component,
                                         transformation="demand_input", transformation_version="1")
                      for source, component in components]
            service_variant = (
                {**variant, "future_car_demand": future_car_demand,
                 "car_extrapolation_rules": road_rules["car_extrapolation"]}
                if reallocates_passenger_ldv else variant
            )
            transformation = (
                "CEUD car CAGR and CER GDP-indexed passenger LDV total"
                if reallocates_passenger_ldv else
                "CEUD baseline service activity indexed by CER real GDP"
            )
            context = resolve_composite_provenance(
                inputs=[*inputs, cer_context], dataset_key=f"demand.{family}.{service}",
                transformation=transformation,
                transformation_version="1", governing_source_id=source_ids[CEUD_PROVINCIAL],
                value_variant=service_variant,
            )
            contexts.append(context)
            service_rows = baseline.loc[baseline["service"].eq(service)
                                        & baseline["commodity"].eq(selector["commodity"])]
            records = [
                {"region": item.region, "period": period,
                 "commodity": item.commodity,
                 "demand": (
                     car_projection[item.region, period, service]
                     if reallocates_passenger_ldv else
                     float(item.baseline_demand) * indices[period]
                 ),
                 "units": item.units,
                 "notes": (
                     f"CEUD {base_year} {service}; 2013–{base_year} car CAGR; "
                     f"CER {edition} {selected_scenario} passenger LDV total {period + step}"
                     if reallocates_passenger_ldv else
                     f"CEUD {base_year} {service}; CER {edition} {selected_scenario} GDP {period + step}"
                 )}
                for item in service_rows.itertuples(index=False) for period in periods
            ]
            rows.extend(validate_parameter_rows(Demand, records, context))
    audit = validate_demand_outputs(rows, regions=expected_regions,
                                    commodities=expected_commodities, periods=periods,
                                    template_units=template_units)
    audit.update({"cer_edition": edition, "cer_scenario": selected_scenario,
                  "future_car_demand": future_car_demand,
                  "gdp_index_by_period": indices, "input_content_digest": digest})
    if car_audit:
        audit["car_extrapolation"] = {
            "method": road_rules["car_extrapolation"]["method"],
            "history_start_year": road_rules["car_extrapolation"]["history_start_year"],
            "rows": car_audit,
        }
    write_dataframe_atomic(pd.DataFrame([row.model_dump(mode="python") for row in rows]),
                           resolve_artifact_path(bundle, "demand_processed") / "demand.csv")
    LOGGER.info("Prepared %s demand rows for %s regions and %s periods",
                len(rows), len(expected_regions), len(periods))
    return rows, contexts, audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    prepare_demand_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
