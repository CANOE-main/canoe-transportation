"""Road vehicle-cycle lifetime emissions; shared preparation without SQLite I/O."""

import argparse
from dataclasses import dataclass, field
import hashlib
import json
import logging
from math import isfinite
from typing import Any

from canoe_schema.v4_0 import EmissionEmbodied
import pandas as pd

from fetching.greet_vehicle_cycle import COMPONENT, SOURCE, GreetEvidence, normalize_generated_results
from parameterization.road_fleet_weights import (
    FleetAggregationEvidence, aggregation_component, load_fleet_aggregation_evidence,
)
from utils import (
    ConfigBundle, file_sha256, load_config_bundle, load_conversion_factors,
    load_harmonization_rules, resolve_artifact_path, resolve_input_path,
    write_dataframe_atomic,
)
from utils.files import write_text_atomic
from validation.insertion import validate_parameter_rows
from validation.provenance import (
    ResolvedProvenance, resolve_composite_provenance, resolve_provenance,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmbodiedEmissionPreparation:
    rows: list[EmissionEmbodied]
    provenance_contexts: list[ResolvedProvenance]
    source_evidence: GreetEvidence | None
    aggregation_evidence: pd.DataFrame
    coverage: pd.DataFrame
    audit: dict[str, Any]
    expected_keys: frozenset[tuple[str, str, str, int]] = field(default_factory=frozenset)


def class_weights(
    fleet: FleetAggregationEvidence, region: str, mode: str, *, rules: dict,
) -> list[dict[str, Any]]:
    """Adapt the existing regional evidence; retain native and normalized weights."""
    spec = rules["road_classes"][mode]
    kind = spec["aggregation"]
    if kind == "direct":
        return [{"native_class": spec["source_class"], "vehicle_class": spec["source_class"],
                 "original_weight": 1.0, "aggregation_weight": 1.0}]
    if kind == "pickup_suv":
        selected = fleet.ldv.loc[fleet.ldv.nrcan_ceud_class.eq("Light Truck")]
        mapping = rules["light_truck_classes"]
        selected = selected.loc[selected.nlr_atb_class.isin(mapping)]
        if set(selected.nlr_atb_class) != set(mapping):
            raise ValueError(f"Incomplete GREET pickup/SUV aggregation evidence for {region}")
        weights = selected.set_index("nlr_atb_class").aggregation_weight.to_dict()
    elif kind == "medium_gvwr":
        source, _ = aggregation_component(fleet.bundle, "medium_trucks", region)
        shares = fleet.medium_shares[source]
        mapping = rules["medium_class_map"]
        if not set(shares) <= set(mapping):
            raise ValueError(f"Unreviewed GREET medium-truck classes for {region}: {sorted(set(shares) - set(mapping))}")
        weights = shares
    elif kind == "heavy_haul":
        weights = fleet.heavy_weights(region)
        mapping = rules["heavy_class_map"]
        if not set(weights) <= set(mapping):
            raise ValueError(f"Unmapped GREET heavy-truck haul classes for {region}")
    else:
        raise ValueError(f"Unsupported GREET road aggregation: {kind}")
    if (not weights or any(not isfinite(w) or w <= 0 for w in weights.values())):
        raise ValueError(f"Invalid GREET road aggregation weights: {region}/{mode}")
    total = sum(weights.values())
    return [
        {"native_class": str(native), "vehicle_class": mapping[native],
         "original_weight": weight, "aggregation_weight": weight / total}
        for native, weight in sorted(weights.items())
    ]


def validate_embodied_outputs(
    rows: list[EmissionEmbodied], *, expected_keys: set[tuple[str, str, str, int]],
    technologies: set[str], emission_commodities: dict[str, str], units: dict[str, str],
    capacity_units: str,
) -> dict[str, Any]:
    """Check the full gas/technology/regional/vintage grid and physical units."""
    keys = {(r.region, r.tech, r.emis_comm, r.vintage) for r in rows}
    if len(keys) != len(rows) or keys != expected_keys:
        raise ValueError(f"Embodied emission coverage differs: missing={sorted(expected_keys-keys)[:8]}, unexpected={sorted(keys-expected_keys)[:8]}")
    for row in rows:
        if row.tech not in technologies or row.emis_comm not in emission_commodities:
            raise ValueError(f"Unresolved embodied technology/gas owner: {row.tech}/{row.emis_comm}")
        expected_units = units.get(row.emis_comm)
        if (row.value is None or not isfinite(row.value) or row.value < 0
                or row.units != expected_units
                or expected_units != f"{emission_commodities[row.emis_comm]}/{capacity_units}"):
            raise ValueError(f"Invalid embodied emission value/units: {row}")
        if any(getattr(row, name) is None for name in ("data_id", "data_source", "dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")):
            raise ValueError("Incomplete embodied emission provenance/DQ")
    return {"ok": True, "rows": len(rows), "regions": sorted({r.region for r in rows}),
            "vintages": sorted({r.vintage for r in rows}), "technologies": len({r.tech for r in rows}),
            "emission_commodities": sorted({r.emis_comm for r in rows})}


def prepare_emission_embodied_rows(bundle: ConfigBundle) -> EmbodiedEmissionPreparation:
    """Return the complete validated preparation contract and publish audit artifacts."""
    if not bundle.scenario.embodied_emissions:
        LOGGER.info("Road vehicle-cycle embodied emissions disabled; no inputs required or rows prepared")
        return EmbodiedEmissionPreparation([], [], None, pd.DataFrame(), pd.DataFrame(),
                                          {"enabled": False, "rows": 0})
    rules = load_harmonization_rules(bundle, "road_embodied_emissions")
    # Explicitly reviewed treatment is required; never infer additional solved years.
    if rules.get("vintage_treatment") != "constant_solved_year":
        raise ValueError("GREET embodied-emission vintage treatment requires a reviewed configuration")
    if bundle.scenario.periods.end_of_horizon > rules["constant_through_year"]:
        raise ValueError("GREET constant vehicle-cycle factors exceed the configured reviewed horizon")
    source = normalize_generated_results(bundle)
    glider_mismatches = [a for a in source.audit["lightweight_glider_input_audit"] if not a["ok"]]
    if bundle.scenario.embodied_materials == "lightweight" and glider_mismatches:
        if (rules["lightweight_bev_range_mismatch"] != "allow_equal_glider_inputs"
                or not all(a["glider_inputs_equal"] for a in glider_mismatches)):
            raise ValueError("GREET lightweight BEV glider-range mapping has unequal inputs; a reviewed source correction is required")
        LOGGER.warning("Accepted native lightweight BEV range-selector mismatch: all affected saved glider inputs are identical; recheck future GREET updates")
    fleet = load_fleet_aggregation_evidence(bundle)
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    technology_path = resolve_input_path(bundle, "template", "technology.csv")
    commodity_path = resolve_input_path(bundle, "template", "commodity.csv")
    technology = pd.read_csv(technology_path)
    commodity = pd.read_csv(commodity_path)
    if technology.tech.duplicated().any() or commodity.name.duplicated().any():
        raise ValueError("Duplicate technology or commodity owners in the transport template")
    scaling = rules["gas_scaling"]
    if set(scaling) != set(rules["gas_commodities"]):
        raise ValueError("Incomplete embodied gas scaling configuration")
    conversions = load_conversion_factors(bundle)["mass"]
    factors, units = {}, {}
    for gas, name in rules["gas_commodities"].items():
        spec = scaling[gas]
        selected = commodity.loc[commodity.name.eq(name)]
        if len(selected) != 1 or selected.iloc[0].flag != "e" or selected.iloc[0].units != spec["commodity_units"]:
            raise ValueError(f"Missing or invalid existing emission commodity: {gas}/{name}")
        factor = conversions[spec["conversion_key"]]
        # Exact SI mass scaling on the unchanged thousand-vehicle capacity basis.
        if (rules["source_units"] != "g/vehicle-lifetime" or rules["capacity_units"] != "k vehicles"
                or factor != {"kt": 1e-6, "t": 1e-3}.get(spec["commodity_units"])
                or spec["output_units"] != f"{spec['commodity_units']}/{rules['capacity_units']}"):
            raise ValueError(f"Invalid embodied lifetime mass conversion/units: {gas}")
        factors[gas], units[name] = factor, spec["output_units"]
    if set(source.rows.gas_label) != set(rules["gas_commodities"]) or set(source.rows.source_units) != {rules["source_units"]}:
        raise ValueError("Unexpected GREET gas labels or source units")
    regions = {r: efficiency["region_output_map"].get(r, r) for r in bundle.scenario.geography.regions}
    if len(set(regions.values())) != len(regions):
        raise ValueError("Embodied output regions collide after configured region mapping")
    vintages = bundle.scenario.periods.model
    candidates = technology.loc[technology.category.isin(rules["road_classes"])]
    coverage_records, eligible = [], []
    for tech in candidates.itertuples(index=False):
        reason = "eligible"
        family = rules["road_classes"][tech.category]["family"]
        if tech.tech.endswith(efficiency["existing_suffix"]):
            reason = "existing_vehicle_manufacturing_excluded"
        elif not tech.tech.endswith(rules["new_technology_suffix"]):
            raise ValueError(f"Embodied manufacturing requires a new technology owner: {tech.tech}")
        elif tech.sub_category in rules.get("excluded_powertrains", []):
            reason = "unsupported_powertrain_explicitly_excluded"
        elif tech.sub_category not in rules["powertrain_map"][family]:
            raise ValueError(f"Unsupported GREET embodied powertrain mapping: {tech.tech}/{tech.sub_category}")
        else:
            eligible.append(tech)
        coverage_records.append({"tech": tech.tech, "mode": tech.category, "powertrain": tech.sub_category, "status": reason})
    if not eligible:
        raise ValueError("No eligible road technologies for enabled embodied emissions")
    rows, contexts, contributions = [], [], []
    expected = set()
    # Evidence-derived identity excludes runtime paths and unrelated material choices.
    for region, output_region in regions.items():
        for mode, spec in rules["road_classes"].items():
            mode_techs = [t for t in eligible if t.category == mode]
            if not mode_techs:
                continue
            weights = class_weights(fleet, region, mode, rules=rules)
            if abs(sum(w["aggregation_weight"] for w in weights) - 1) > rules["weight_tolerance"]:
                raise ValueError(f"Embodied aggregation weights do not sum to one: {region}/{mode}")
            materials = bundle.scenario.embodied_materials if spec["family"] == "ldv" else "none"
            selected = source.rows.loc[source.rows.family.eq(spec["family"]) & source.rows.materials.eq(materials)]
            selected = selected.loc[selected.vehicle_class.isin({w["vehicle_class"] for w in weights})]
            native = selected.set_index(["vehicle_class", "powertrain", "gas_label", "range_miles"])
            input_context = resolve_provenance(
                bundle.sources, source_key=SOURCE, component_key=COMPONENT,
                transformation="Validated GREET range-specific vehicle-cycle lifetime gas extraction",
                transformation_version=rules["transformation_version"],
            )
            inputs = [input_context]
            role = {"pickup_suv": "ldv", "medium_gvwr": "medium_trucks", "heavy_haul": "heavy_truck_haul"}.get(spec["aggregation"])
            if role:
                source_key, component = aggregation_component(bundle, role, region)
                inputs.append(resolve_provenance(
                    bundle.sources, source_key=source_key, component_key=component,
                    transformation="Shared regional road fleet aggregation",
                    transformation_version=rules["transformation_version"],
                ))
            variant = {
                "region": region, "mode": mode, "materials": materials,
                "vintages": vintages, "vintage_treatment": rules["vintage_treatment"],
                "constant_through_year": rules["constant_through_year"],
                "gas_scaling": scaling, "conversion_factors": factors, "weights": weights,
                "gas_commodities": rules["gas_commodities"],
                "technology_powertrains": {t.tech: t.sub_category for t in mode_techs},
                "powertrain_map": rules["powertrain_map"][spec["family"]],
                "range_miles": rules["ldv_range_miles"] if spec["family"] == "ldv" else {},
                "saved_evidence_digest": hashlib.sha256(selected.to_csv(index=False).encode()).hexdigest(),
            }
            context = resolve_composite_provenance(
                inputs=inputs, dataset_key=f"road_embodied_emissions.{mode}.{region}",
                transformation="Configured GREET lifetime-gas mass scaling and regional road aggregation",
                transformation_version=rules["transformation_version"],
                governing_source_id=input_context.governing_source_id, value_variant=variant,
            )
            context = context.model_copy(update={"dataset_description": context.dataset_description
                + "; vehicle-cycle evidence: " + json.dumps(selected[[
                    "greet1_file", "greet1_sha256", "greet2_file", "greet2_sha256", "solved_simulation_year", "ldv_vehicle_cohort_year",
                    "worksheet", "cell", "vehicle_class", "powertrain", "materials", "gas_label", "range_miles", "evidence_case_ids",
                ]].to_dict("records"), sort_keys=True, separators=(",", ":"))})
            contexts.append(context)
            records = []
            for tech in mode_techs:
                powertrain = rules["powertrain_map"][spec["family"]][tech.sub_category]
                distance = 0
                if spec["family"] == "ldv" and powertrain in {"phev", "bev"}:
                    if tech.sub_category not in rules["ldv_range_miles"]:
                        raise ValueError(f"Missing GREET LDV range mapping: {tech.tech}")
                    distance = rules["ldv_range_miles"][tech.sub_category]
                for gas, emis_comm in rules["gas_commodities"].items():
                    factor = factors[gas]
                    value = 0.0
                    for weight in weights:
                        key = (weight["vehicle_class"], powertrain, gas, distance)
                        if key not in native.index:
                            raise ValueError(f"Missing GREET embodied class/powertrain/gas: {region}/{mode}/{key}")
                        evidence = native.loc[key].to_dict()
                        term = evidence["source_value"] * factor * weight["aggregation_weight"]
                        value += term
                        contributions.append({
                            **evidence, **weight, "vehicle_class": key[0],
                            "powertrain": powertrain, "gas_label": gas, "range_miles": distance,
                            "scenario_region": region, "region": output_region,
                            "mode": mode, "tech": tech.tech, "emis_comm": emis_comm,
                            "conversion_factor": factor, "weighted_value": term,
                            "units": units[emis_comm], "data_id": context.data_id,
                        })
                    for vintage in vintages:
                        expected.add((output_region, tech.tech, emis_comm, vintage))
                        records.append({
                            "region": output_region, "tech": tech.tech, "emis_comm": emis_comm,
                            "vintage": vintage, "value": value, "units": units[emis_comm],
                            "notes": f"GREET simulation {source.rows.solved_simulation_year.iloc[0]}; {materials}; range {distance} miles (0=not range-specific); vehicle-cycle lifetime total, no EOL credits; constant through {rules['constant_through_year']}",
                        })
            rows.extend(validate_parameter_rows(EmissionEmbodied, records, context))
    rows.sort(key=lambda r: (r.region, r.tech, r.emis_comm, r.vintage))
    audit = validate_embodied_outputs(
        rows, expected_keys=expected, technologies=set(technology.tech),
        emission_commodities=commodity.loc[commodity.flag.eq("e")].set_index("name").units.to_dict(),
        units=units, capacity_units=rules["capacity_units"],
    )
    aggregation, coverage = pd.DataFrame(contributions), pd.DataFrame(coverage_records)
    audit.update({
        "enabled": True, "embodied_materials": bundle.scenario.embodied_materials,
        "source_rows": len(source.rows), "excluded_combined_co2_rows": len(source.exclusions),
        "source_generation": source.audit, "source_cases": source.models.to_dict("records"),
        "vintage_treatment": rules["vintage_treatment"], "period_mapping": bundle.scenario.periods.audit(),
        "constant_through_year": rules["constant_through_year"],
        "conversion_factors": factors, "source_units": rules["source_units"], "output_units": units,
        "capacity_units": rules["capacity_units"], "gas_scaling": scaling,
        "lightweight_bev_range_mismatch": rules["lightweight_bev_range_mismatch"],
        "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
        "coverage_status_counts": coverage.status.value_counts().to_dict(),
        "input_hashes": {p.relative_to(bundle.repo_root).as_posix(): file_sha256(p) for p in (*source.paths, *fleet.paths, technology_path, commodity_path)},
    })
    processed = resolve_artifact_path(bundle, "emission_embodied_processed")
    validation = resolve_artifact_path(bundle, "emission_embodied_validation")
    for frame, key, directory in ((aggregation, "aggregation", processed),
                                  (pd.DataFrame([r.model_dump(mode="json") for r in rows]), "parameters", processed),
                                  (coverage, "coverage", validation)):
        path = directory / rules["files"][key]
        write_dataframe_atomic(frame, path)
        LOGGER.info("Wrote embodied %s artifact: %s (%s rows)", key, path, len(frame))
    write_text_atomic(json.dumps(audit, sort_keys=True, indent=2) + "\n", validation / rules["files"]["integrity"])
    LOGGER.info("Prepared %s embodied rows; lifetime mass factors=%s, units=%s; coverage=%s",
                len(rows), factors, units, audit["coverage_status_counts"])
    return EmbodiedEmissionPreparation(rows, contexts, source, aggregation, coverage, audit, frozenset(expected))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    prepare_emission_embodied_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
