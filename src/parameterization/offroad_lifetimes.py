"""Prepare reviewed fixed transport lifetimes from the manual source registry."""

from __future__ import annotations

from math import isfinite

import pandas as pd
from canoe_schema.v4_0 import LifetimeTech

from parameterization.manual_parameters import (
    resolve_manual_parameters,
    validate_manual_registry,
    validate_technology_selectors,
)
from utils import (
    ConfigBundle,
    file_sha256,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.insertion import validate_parameter_rows
from validation.provenance import ResolvedProvenance, resolve_provenance


MANUAL_FILE = "lifetime_process.csv"


def prepare_reviewed_manual_lifetimes(
    bundle: ConfigBundle,
) -> tuple[list[LifetimeTech], list[ResolvedProvenance], pd.DataFrame]:
    """Resolve reviewed category selectors to technology-owned lifetime rows."""
    rules = load_harmonization_rules(bundle, "manual_parameters")
    registry, frames = validate_manual_registry(
        bundle,
        source_column=str(rules["source_column"]),
        notes_column=str(rules["notes_column"]),
        selected_files={MANUAL_FILE},
    )
    manual = frames[MANUAL_FILE]
    if set(manual["sub_category"]) != {"all"}:
        raise ValueError("Reviewed lifetime selectors must apply to each category")
    technology = validate_technology_selectors(
        pd.read_csv(
            resolve_input_path(bundle, "template", str(rules["technology_template_file"])),
            dtype=str,
            keep_default_na=False,
        ),
        rules=rules,
    )
    resolution, _, findings = resolve_manual_parameters(
        frames, technology, rules=rules
    )
    if not findings.empty or resolution.empty:
        raise ValueError("Reviewed manual lifetimes have unresolved technology selectors")
    if resolution.duplicated("tech").any():
        raise ValueError("Reviewed manual lifetimes resolve conflicting technology owners")
    if set(resolution["manual_row"]) != set(manual.index + 2):
        raise ValueError("A reviewed manual lifetime has no technology owner")

    manual_digest = file_sha256(resolve_input_path(bundle, "manual", MANUAL_FILE))
    rows: list[LifetimeTech] = []
    contexts: list[ResolvedProvenance] = []
    output_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"]
    expected_regions = {output_map.get(region, region) for region in bundle.scenario.geography.regions}
    for registration in registry.itertuples(index=False):
        source = bundle.sources.sources[registration.source_id]
        if source.status != "active":
            raise ValueError(f"Manual lifetime source is inactive: {registration.source_id}")
        component = source.component(registration.component_id)
        if component.units != "years":
            raise ValueError(f"Manual lifetime source has non-year units: {registration.source_id}")
        context = resolve_provenance(
            bundle.sources,
            source_key=registration.source_id,
            component_key=registration.component_id,
            transformation="reviewed_manual_technology_lifetime",
            transformation_version="1",
            value_variant={"manual_sha256": manual_digest},
        )
        selected = resolution.loc[
            resolution["registered_source_id"].eq(registration.source_id)
            & resolution["registered_component_id"].eq(registration.component_id)
        ]
        if selected.empty:
            raise ValueError(f"No manual lifetime technologies for {registration.component_id}")
        records: list[dict[str, object]] = []
        for fields in selected.to_dict("records"):
            lifetime = float(fields["lifetime"])
            if not isfinite(lifetime) or lifetime <= 0:
                raise ValueError(f"Invalid reviewed lifetime for {fields['tech']}")
            for quality, reviewed in context.data_quality.row_fields().items():
                if int(fields[quality]) != reviewed:
                    raise ValueError(
                        f"Reviewed DQ differs from source registry for {fields['tech']}: {quality}"
                    )
            year = int(fields["data_year -> dq_time"])
            if year <= 0:
                raise ValueError(f"Invalid source data year for {fields['tech']}")
            notes = str(fields["notes"]).strip()
            if not notes:
                raise ValueError(f"Missing reviewed lifetime note for {fields['tech']}")
            for region in sorted(expected_regions):
                records.append({
                    "region": region,
                    "tech": str(fields["tech"]),
                    "lifetime": lifetime,
                    "units": "years",
                    "notes": f"{notes} Source data year: {year}.",
                })
        rows.extend(validate_parameter_rows(LifetimeTech, records, context))
        contexts.append(context)
    native_keys = {(row.region, row.tech) for row in rows}
    expected_keys = {
        (region, tech)
        for region in expected_regions
        for tech in resolution["tech"]
    }
    if len(rows) != len(native_keys) or native_keys != expected_keys:
        raise ValueError("Reviewed manual lifetime coverage is incomplete or conflicting")
    rows.sort(key=lambda row: (row.region, row.tech))
    return rows, contexts, resolution


def prepare_statcan_bus_lifetimes(
    bundle: ConfigBundle,
) -> tuple[list[LifetimeTech], list[ResolvedProvenance], pd.DataFrame]:
    """Use latest reported provincial bus life, then the reviewed Canada 2020 fallback."""
    rules = load_harmonization_rules(bundle, "road_lifetimes_survival")["bus_lifetimes"]
    statcan = load_harmonization_rules(bundle, "statcan_tables")
    table_id = str(rules["statcan_table_id"])
    table = statcan["tables"][table_id]
    path = resolve_input_path(bundle, "interim", statcan["interim_subdir"]) / table["output_file"]
    evidence = pd.read_csv(path)
    required = {
        "table_id", "scenario_region", "reference_period",
        "public_transit_assets_average_expected_useful_life", "scaled_value", "units",
    }
    if required - set(evidence) or set(evidence["table_id"]) != {table_id}:
        raise ValueError("StatCan bus evidence has an invalid table contract")
    if set(evidence["units"]) != {"Average years"}:
        raise ValueError("StatCan bus evidence must be in average years")
    keys = ["scenario_region", "reference_period", "public_transit_assets_average_expected_useful_life"]
    if evidence.duplicated(keys).any():
        raise ValueError("StatCan bus evidence has duplicate source cells")
    evidence["reference_period"] = pd.to_numeric(evidence["reference_period"], errors="raise").astype(int)
    evidence["scaled_value"] = pd.to_numeric(evidence["scaled_value"], errors="coerce")
    numeric = evidence.loc[evidence["scaled_value"].notna()].copy()
    if numeric["scaled_value"].map(lambda value: not isfinite(value) or value <= 0).any():
        raise ValueError("StatCan bus useful lives must be finite positive years")
    template = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"), dtype=str).fillna("")
    if template["tech"].duplicated().any():
        raise ValueError("Technology template has duplicate owner IDs")
    bus_categories = {"urban_transit", "school_buses", "inter_city_buses"}
    owners = template.loc[template["category"].isin(bus_categories)].copy()
    powertrains = rules["powertrain_source_member"]
    if owners.empty or set(owners["sub_category"]) - set(powertrains):
        raise ValueError("StatCan bus powertrain mapping does not cover template owners")
    canada_year = int(rules["canada_fallback_year"])
    canada = numeric.loc[numeric["scenario_region"].eq("Canada") & numeric["reference_period"].eq(canada_year)]
    if set(powertrains.values()) - set(canada["public_transit_assets_average_expected_useful_life"]):
        raise ValueError("Canada bus fallback lacks a required powertrain")
    output_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"]
    if len(set(output_map.values())) != len(output_map):
        raise ValueError("Bus region output map has conflicting owners")
    context = resolve_provenance(
        bundle.sources,
        source_key="statcan_transport_tables",
        component_key=table_id,
        transformation="latest_provincial_bus_useful_life_then_canada_2020",
        transformation_version="1",
        value_variant={"normalized_sha256": file_sha256(path), "powertrains": powertrains,
                       "canada_fallback_year": canada_year},
    )
    records: list[dict[str, object]] = []
    audit: list[dict[str, object]] = []
    for region in sorted(bundle.scenario.geography.regions):
        source_region = region
        model_region = output_map.get(region, region)
        for owner in owners.itertuples(index=False):
            member = str(powertrains[owner.sub_category])
            provincial = numeric.loc[
                numeric["scenario_region"].eq(source_region)
                & numeric["public_transit_assets_average_expected_useful_life"].eq(member)
                & numeric["reference_period"].le(canada_year)
            ].sort_values("reference_period")
            selected = provincial.iloc[-1] if not provincial.empty else canada.loc[
                canada["public_transit_assets_average_expected_useful_life"].eq(member)
            ].iloc[0]
            fallback = provincial.empty
            lifetime = float(selected["scaled_value"])
            records.append({
                "region": model_region, "tech": str(owner.tech), "lifetime": lifetime,
                "units": "years",
                "notes": f"StatCan {selected['scenario_region']} {int(selected['reference_period'])} "
                         f"{member}; {'Canada fallback' if fallback else 'latest provincial value'}.",
            })
            audit.append({
                "region": model_region, "tech": str(owner.tech), "source_region": selected["scenario_region"],
                "source_year": int(selected["reference_period"]), "source_member": member,
                "canada_fallback": fallback, "lifetime_years": lifetime,
            })
    rows = validate_parameter_rows(LifetimeTech, records, context)
    expected = {(output_map.get(region, region), tech) for region in bundle.scenario.geography.regions for tech in owners.tech}
    if len(rows) != len(expected) or {(row.region, row.tech) for row in rows} != expected:
        raise ValueError("StatCan bus lifetime owner coverage is incomplete")
    rows.sort(key=lambda row: (row.region, row.tech))
    return rows, [context], pd.DataFrame(audit).sort_values(["region", "tech"]).reset_index(drop=True)
