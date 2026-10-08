"""Build transport efficiency and PHEV input splits independently of SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from canoe_schema.v4_0 import DataSet, Efficiency, LimitTechInputSplit

from fetching.fueleconomy_vehicles import build_request, validate_cache
from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.manual_parameters import validate_manual_registry
from parameterization.ldv_ev_ranges import existing_range_weights, module_rules as range_module_rules
from fetching.epa_omega_baseline import SOURCE as OMEGA, COMPONENT as OMEGA_COMPONENT, build_request as omega_request
from parameterization.offroad_efficiencies import derive_offroad_efficiency
from parameterization.road_efficiencies import (
    RoadEfficiencyEvidence,
    aggregate_atb,
    aggregate_ratings,
    classify_ratings,
    derive_load_factors,
    interpolate,
    select_atb_consumption,
)
from parameterization.road_fleet_weights import aggregation_component, load_fleet_aggregation_evidence
from utils import (
    ConfigBundle,
    active_source_keys,
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
CEUD = "nrcan_ceud_transport_provincial"


@dataclass(frozen=True)
class EfficiencyPreparation:
    efficiency_rows: list[Efficiency]
    split_rows: list[LimitTechInputSplit]
    assumption_dataset: DataSet
    provenance_contexts: list[ResolvedProvenance]
    audit: dict


def phev_blend_relationships(rules: dict) -> dict:
    blends = {key: dict(spec) for key, spec in rules["phev_blends"].items()}
    for category, spec in rules["existing_phev_blends"].items():
        blends[f"existing_{category}"] = {
            **rules["phev_blends"]["ldv35"], **spec, "category": category,
            "archetype": "phev", "existing_representative": True,
        }
    return blends


def technology_relationships(
    technology: pd.DataFrame,
    commodity: pd.DataFrame,
    *,
    rules: dict,
    road_outputs: dict,
    offroad_outputs: dict,
) -> pd.DataFrame:
    """Expand template ownership using explicit configured fuel/service edges."""
    records = []
    blends = phev_blend_relationships(rules)
    blend_techs = {spec["tech"] for spec in blends.values()}
    for item in technology.fillna("").itertuples(index=False):
        if item.category in rules["excluded_categories"] or any(
            re.search(p, item.tech) for p in rules["excluded_technology_patterns"]
        ):
            continue
        if item.tech in blend_techs:
            spec = next(spec for spec in blends.values() if spec["tech"] == item.tech)
            for fuel in ("fuel", "electricity"):
                records.append(
                    {
                        "tech": item.tech,
                        "mode": "blend",
                        "powertrain": fuel,
                        "input_comm": spec[fuel],
                        "output_comm": spec["output"],
                        "units": "PJ/PJ",
                        "kind": "unit",
                    }
                )
            continue
        if item.tech in rules["unit_efficiencies"]:
            spec = rules["unit_efficiencies"][item.tech]
            records.append(
                {
                    "tech": item.tech,
                    "mode": item.category,
                    "powertrain": item.sub_category,
                    "input_comm": spec["input"],
                    "output_comm": spec["output"],
                    "units": spec["units"],
                    "kind": "unit",
                }
            )
            continue
        powertrain = rules["subcategory_aliases"].get(
            item.sub_category, item.sub_category
        )
        if item.category in road_outputs:
            mode = rules["road_classes"][item.category]
            pathway = mode["pathway"]
            output = road_outputs[item.category]
            if powertrain.startswith("phev"):
                blend_key = f"existing_{item.category}" if (
                    powertrain == "phev" and pathway == "ldv" and item.tech.endswith(rules["existing_suffix"])
                ) else next(
                    (
                        key
                        for key, s in blends.items()
                        if s["archetype"] == powertrain and pathway == "ldv"
                    ),
                    None,
                )
                if blend_key is None:
                    blend_key = rules["phev_blend_by_category"][item.category]
                input_comm = blends[blend_key]["output"]
            elif powertrain.startswith("bev"):
                input_comm = rules["electric_inputs"][pathway]
            elif powertrain in {"fcev", "fchev"}:
                input_comm = rules["hydrogen_inputs"][pathway]
            elif powertrain == "hev" and pathway != "ldv":
                input_comm = rules["input_commodities"][
                    rules["missing_powertrain_proxy"]
                ]
            else:
                input_comm = rules["input_commodities"][powertrain]
            kind = "road"
        elif item.category in offroad_outputs:
            output = offroad_outputs[item.category]
            input_comm = rules["input_commodities"][powertrain]
            kind = "offroad"
        else:
            raise ValueError(
                f"Unresolved technology ownership: {item.tech}/{item.category}/{powertrain}"
            )
        records.append(
            {
                "tech": item.tech,
                "mode": item.category,
                "powertrain": powertrain,
                "input_comm": input_comm,
                "output_comm": output["commodity"],
                "units": output["units"] + "/PJ",
                "kind": kind,
            }
        )
    result = pd.DataFrame(records)
    units = commodity.set_index("name").units.to_dict()
    for row in result.itertuples():
        if row.input_comm not in units or row.output_comm not in units:
            raise ValueError(f"Missing template commodity in {row.tech} edge")
        if units[row.input_comm] != "PJ":
            raise ValueError(f"Efficiency input must be PJ: {row.input_comm}")
        expected = {
            "bn passenger-km/PJ": "billion passenger-km",
            "bn tonne-km/PJ": "billion tonne-km",
            "PJ/PJ": "PJ",
        }[row.units]
        if units[row.output_comm] != expected:
            raise ValueError(
                f"Efficiency output units differ from template: {row.tech}"
            )
    if not blend_techs.issubset(set(result.tech)):
        raise ValueError("Missing active PHEV blending technologies")
    return result.sort_values(["tech", "input_comm", "output_comm"])


def phev_split_evidence(
    atb: pd.DataFrame, *, endpoints: list[int], rules: dict, fleet,
) -> pd.DataFrame:
    """Weight component consumption with the selected regional fleet evidence.

    LDV35/50 remain shared car/light-truck blends, so their source-class weights
    use pooled Report A stock counts. Medium and heavy blends use their selected
    regional GVWR/vocation and haul weights. Endpoints retain equal horizon weight.
    """
    records = []
    for blend, spec in rules["phev_blends"].items():
        selected = atb.loc[
            atb.powertrain.eq(spec["archetype"]) & atb.electricity_input_share.notna()
        ]
        if "gvwr" in spec:
            gvwr = selected.vehicle_class.str.extract(
                rules["atb"]["class_pattern"], expand=False
            ).astype(int)
            selected = selected.loc[gvwr.isin(spec["gvwr"])]
            if blend == "mdv":
                selected = selected.loc[
                    ~selected.vehicle_class.str.contains(
                        "|".join(map(re.escape, fleet.rules["medium_trucks"]["excluded_vocations"]))
                    )
                ]
        role = spec["aggregation_role"]
        for region in fleet.bundle.scenario.geography.regions:
            if role == "ldv":
                counts = fleet.ldv.groupby("nlr_atb_class").fit_active_stock.sum()
                weights = (counts / counts.sum()).to_dict()
            elif role == "medium_trucks":
                weights = fleet.medium_weights(region, sorted(selected.vehicle_class.unique()))
            elif role == "heavy_truck_haul":
                weights = fleet.heavy_weights(region)
            else:
                raise ValueError(f"Unsupported PHEV aggregation role: {role}")
            if not set(weights).issubset(set(selected.vehicle_class)):
                raise ValueError(f"Incomplete PHEV fleet-class coverage: {region}/{blend}")
            source, component = aggregation_component(fleet.bundle, role, region)
            for year in endpoints:
                native = []
                for vehicle_class, weight in sorted(weights.items()):
                    history = selected.loc[selected.vehicle_class.eq(vehicle_class)].copy()
                    history["electricity_mj_per_vkm"] = (
                        history.consumption_mj_per_vkm * history.electricity_input_share
                    )
                    total = interpolate(history, year, "consumption_mj_per_vkm")
                    electric = interpolate(history, year, "electricity_mj_per_vkm", allow_zero=True)
                    native.append({
                        "region": region, "blend": blend, "tech": spec["tech"],
                        "vehicle_class": vehicle_class, "year": year, "weight": weight,
                        "electricity_mj_per_vkm": electric, "total_mj_per_vkm": total,
                        "weighted_electricity_mj_per_vkm": weight * electric,
                        "weighted_total_mj_per_vkm": weight * total,
                        "aggregation_source": source, "aggregation_component": str(component),
                    })
                fraction = sum(r["weighted_electricity_mj_per_vkm"] for r in native) / sum(
                    r["weighted_total_mj_per_vkm"] for r in native
                )
                records.extend({**r, "electricity_share": fraction, "fuel_share": 1 - fraction} for r in native)
    result = pd.DataFrame(records)
    if set(result.blend) != set(rules["phev_blends"]):
        raise ValueError("Incomplete PHEV blend evidence")
    return result


def existing_phev_split_evidence(
    atb_audit: pd.DataFrame, *, endpoints: list[int], rules: dict,
    range_weights: dict, regions: list[str],
) -> pd.DataFrame:
    """Keep each category's class and range consumption before its shared blend."""
    records = []
    for category, spec in rules["existing_phev_blends"].items():
        for region in regions:
            for year in endpoints:
                native = []
                for bucket, share in range_weights[category, "phev"].items():
                    selected = atb_audit.loc[
                        atb_audit.region.eq(region) & atb_audit["mode"].eq(category)
                        & atb_audit.powertrain.eq(bucket)
                    ].copy()
                    selected["weighted_electricity"] = selected.weighted_consumption * selected.electricity_input_share
                    annual = selected.groupby("year", as_index=False).agg(
                        total=("weighted_consumption", "sum"), electric=("weighted_electricity", "sum"),
                    )
                    total = interpolate(annual, year, "total")
                    electric = interpolate(annual, year, "electric", allow_zero=True)
                    native.append({
                        "region": region, "blend": f"existing_{category}", "tech": spec["tech"],
                        "range_bucket": bucket, "year": year, "weight": share,
                        "electricity_mj_per_vkm": electric, "total_mj_per_vkm": total,
                        "weighted_electricity_mj_per_vkm": share * electric,
                        "weighted_total_mj_per_vkm": share * total,
                    })
                fraction = sum(r["weighted_electricity_mj_per_vkm"] for r in native) / sum(
                    r["weighted_total_mj_per_vkm"] for r in native
                )
                records.extend({**r, "electricity_share": fraction, "fuel_share": 1 - fraction} for r in native)
    return pd.DataFrame(records)


def validate_efficiency_outputs(
    rows: list[Efficiency],
    splits: list[LimitTechInputSplit],
    *,
    relationships: pd.DataFrame,
    regions: set[str],
    periods: list[int],
    existing_keys: set[tuple],
    rules: dict,
) -> dict:
    actual = {(r.region, r.tech, r.vintage, r.input_comm, r.output_comm) for r in rows}
    expected = set()
    for r in relationships.itertuples():
        for region in regions:
            vintages = (
                [
                    v
                    for reg, tech, v in existing_keys
                    if reg == region and tech == r.tech
                ]
                if r.tech.endswith(rules["existing_suffix"]) and r.kind != "unit"
                else periods
            )
            expected.update(
                (region, r.tech, v, r.input_comm, r.output_comm) for v in vintages
            )
    if len(actual) != len(rows) or actual != expected:
        raise ValueError(
            f"Efficiency key coverage mismatch: missing={len(expected - actual)}, extra={len(actual - expected)}"
        )
    split_frame = pd.DataFrame([r.model_dump() for r in splits])
    expected_splits = {
        (region, period, spec["tech"], spec[role])
        for region in regions
        for period in periods
        for spec in phev_blend_relationships(rules).values()
        for role in ("fuel", "electricity")
    }
    actual_splits = {(r.region, r.period, r.tech, r.input_comm) for r in splits}
    if len(actual_splits) != len(splits) or actual_splits != expected_splits:
        raise ValueError("PHEV split key coverage mismatch")
    if any(
        r.operator != rules["phev_split_operator"] or not 0 <= r.proportion <= 1
        for r in splits
    ):
        raise ValueError("Invalid PHEV split operator/bounds")
    totals = split_frame.groupby(["region", "period", "tech"]).proportion.sum()
    if not np.allclose(totals, 1, rtol=0, atol=rules["tolerances"]["split_sum"]):
        raise ValueError("PHEV energy shares do not sum to one")
    if (split_frame.groupby(["region", "tech", "input_comm"]).proportion.nunique() != 1).any():
        raise ValueError(
            "PHEV regional mean must remain constant across the horizon"
        )
    for row in [*rows, *splits]:
        if row.data_id is None:
            raise ValueError("Untraceable efficiency/split row")
        if row.data_source is not None and any(
            getattr(row, key) is None
            for key in ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")
        ):
            raise ValueError("Incomplete external DQ chain")
    return {
        "efficiency_rows": len(rows),
        "split_rows": len(splits),
        "regions": sorted(regions),
        "technologies": len({r.tech for r in rows}),
        "split_max_sum_error": float(abs(totals - 1).max()),
    }


def prepare_efficiency_rows(
    bundle: ConfigBundle, *, existing_capacity_rows: list | None = None
) -> EfficiencyPreparation:
    """Offline source-to-schema preparation; no connection or transaction is required."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    conversions = load_conversion_factors(bundle)
    scenario = bundle.scenario
    if (
        rules["future_year_rule"] != "interval_end"
        or rules["missing_rating_classes"] != "renormalize_observed"
    ):
        raise ValueError("Unsupported efficiency period/aggregation policy")
    if rules["phev_split_target"] != LimitTechInputSplit.table_name():
        raise ValueError("PHEV split target does not match canoe_schema")
    supported_policies = {
        "unknown_powertrain": "error",
        "historical_aggregation": "mean_annual_service_efficiency",
        "historical_first_bin": "available_source_years_only",
        "future_load_factor": "base_year",
        "missing_annual_rating": "configured_analogue_index",
        "phev_split_method": "regional_consumption_weighted_classes_mean_horizon_endpoints",
    }
    if any(rules[key] != value for key, value in supported_policies.items()):
        raise ValueError("Unsupported efficiency modeling policy; no silent fallback")
    if (rules["intercity"]["multiplier_basis"]
            != "regen_intercity_powertrain_over_diesel_fuel_economy"
            or rules["intercity"]["hev_ratio"] != "atb_transit_hev_over_diesel"):
        raise ValueError("Unsupported intercity efficiency multiplier policy")
    if (
        rules["atb"]["phev_energy_basis"] != "greet_hhv_fuel_plus_electricity"
        or rules["atb"]["interpolation"] != "linear_consumption"
        or rules["atb"]["extrapolation"] != "error"
        or rules["offroad"]["annual_improvement"] != "efficiency_compound_growth"
        or rules["offroad"]["period_method"] != "linear_interpolation_between_endpoints"
    ):
        raise ValueError("Unsupported energy-basis or trajectory interpolation policy")
    if existing_capacity_rows is None:
        from parameterization.build_existing_capacity import prepare_existing_capacity_rows

        existing_capacity_rows, _, _ = prepare_existing_capacity_rows(bundle)
    existing_keys = {
        (r.region, r.tech, r.vintage) for r in existing_capacity_rows if r.capacity > 0
    }
    paths: list[Path] = []

    def read(path: Path, source: str | None = None) -> pd.DataFrame:
        paths.append(path)
        frame = pd.read_csv(path)
        if frame.empty:
            raise ValueError(f"Empty efficiency evidence: {path}")
        if source is not None and set(frame.source_id) != {source}:
            raise ValueError(f"Unexpected evidence source identity: {path}")
        return frame

    technology = read(resolve_input_path(bundle, "template", "technology.csv"))
    range_rules = range_module_rules(bundle)
    historical_weights, range_evidence = existing_range_weights(bundle)
    paths.append(omega_request(bundle).extract_path)
    commodity = read(resolve_input_path(bundle, "template", "commodity.csv"))
    region_template = read(resolve_input_path(bundle, "template", "region.csv"))
    period_template = read(resolve_input_path(bundle, "template", "time_period.csv"))
    expected_regions = {
        rules["region_output_map"].get(region, region)
        for region in scenario.geography.regions
    }
    if not expected_regions.issubset(set(region_template.region)):
        raise ValueError("Efficiency scenario region is not owned by the template")
    for periods, flag in (
        (scenario.periods.existing, "e"),
        (scenario.periods.model, "f"),
    ):
        if not set(periods).issubset(
            set(period_template.loc[period_template.flag.eq(flag), "period"])
        ):
            raise ValueError(
                "Efficiency vintage/period differs from template ownership"
            )
    road_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    offroad_rules = load_harmonization_rules(bundle, "offroad_stocks_and_demands")[
        "demand"
    ]
    relationships = technology_relationships(
        technology,
        commodity,
        rules=rules,
        road_outputs=road_rules["activity_series"],
        offroad_outputs=offroad_rules["energy_intensity_series"],
    )
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    capacity_keys = existing_keys.copy()
    source_only_edges = relationships.loc[
        relationships["mode"].isin(rules["historical_classes_without_capacity"])
        & relationships.tech.str.endswith(rules["existing_suffix"])
    ]
    existing_keys.update(
        (rules["region_output_map"].get(region, region), tech, vintage)
        for region in scenario.geography.regions
        for tech in source_only_edges.tech
        for vintage in scenario.periods.existing
    )
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])

    def ceud_path(region):
        return ceud_dir / ceud_rules["region_output_template"].format(
            region=region.lower()
        )

    provincial = pd.concat(
        [read(ceud_path(region), CEUD) for region in scenario.geography.regions],
        ignore_index=True,
    )
    national = read(ceud_path("national"), "nrcan_ceud_transport_national")
    if (
        int(provincial.year.max()) != scenario.periods.base_year
        or int(national.year.max()) != scenario.periods.base_year
    ):
        raise ValueError("CEUD vintage differs from selected base year")
    loads = derive_load_factors(
        provincial,
        rules=rules,
        activity_rules=road_rules["activity_series"],
        conversions=conversions,
    )
    rating_rules = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")
    ratings = [
        read(
            resolve_input_path(
                bundle, "interim", rating_rules["interim_subdir"], filename
            ),
            "nrcan_fuel_consumption_ratings",
        )
        for filename in rating_rules["outputs"].values()
    ]
    epa_request = build_request(bundle)
    validate_cache(epa_request)
    paths.append(epa_request.cache_path)
    with (
        zipfile.ZipFile(epa_request.cache_path) as archive,
        archive.open(epa_request.archive_member) as member,
    ):
        epa = pd.read_csv(
            member, usecols=rules["ratings"]["evidence_columns"], low_memory=False
        )
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    classified = classify_ratings(
        ratings,
        epa,
        rules=rules,
        conversions=conversions,
        phev_contract=atb_rules["components"]["phev_efficiency"][
            "future_nrcan_phev_contract"
        ],
        range_rules=range_rules,
    )
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    weights_dir = resolve_artifact_path(bundle, "road_aggregation")
    rating_weights = read(weights_dir / aggregation["nrcan_weights_file"])
    fleet = load_fleet_aggregation_evidence(bundle)
    paths.extend(fleet.paths)
    nlr_weights = fleet.ldv
    rating_audit = aggregate_ratings(
        classified, rating_weights, rules=rules, weight_basis=aggregation["ldv_weight_basis"],
    )
    atb_dir = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"])
    vehicle_source = read(
        atb_dir / atb_rules["components"]["vehicles"]["output_file"], ATB
    )
    phev_source = read(
        atb_dir / atb_rules["components"]["phev_efficiency"]["output_file"], ATB
    )
    trajectory = configured_trajectory(bundle)
    aliases = atb_rules["components"]["phev_efficiency"]["reconciliation"][
        "scenario_aliases"
    ]
    phev_source["trajectory"] = phev_source.trajectory.replace(aliases)
    atb = select_atb_consumption(
        vehicle_source,
        phev_source,
        trajectory=trajectory,
        rules=rules,
        conversions=conversions,
    )
    if set(rating_weights.report_year) != set(nlr_weights.report_year):
        raise ValueError("LDV rating and ATB aggregation evidence editions differ")
    md_classes = sorted(atb.loc[atb.family.eq("mhdv"), "vehicle_class"].unique())
    atb_parts = []
    for region in scenario.geography.regions:
        medium_weights = fleet.medium_weights(region, md_classes)
        heavy = fleet.heavy_weights(region)
        for mode, spec in rules["road_classes"].items():
            if spec["pathway"] == "motorcycle":
                continue
            if spec["pathway"] == "ldv":
                selected = nlr_weights.loc[
                    nlr_weights.nrcan_ceud_class.eq(spec["weights"])
                ]
                if (
                    selected.nlr_atb_class.duplicated().any()
                    or selected.report_year.nunique() != 1
                ):
                    raise ValueError("Ambiguous LDV NLR class weights")
                weights = selected.set_index(
                    "nlr_atb_class"
                ).aggregation_weight.to_dict()
            elif mode == "medium_trucks":
                weights = medium_weights
            elif mode == "heavy_trucks":
                weights = heavy
            else:
                weights = {c: 1 / len(spec["atb_classes"]) for c in spec["atb_classes"]}
            selected = atb.loc[
                atb.family.eq("ldv" if spec["pathway"] == "ldv" else "mhdv")
            ]
            part = aggregate_atb(
                selected, weights, tolerance=rules["tolerances"]["weight_sum"]
            )
            part["region"], part["mode"] = region, mode
            atb_parts.append(part)
    atb_audit = pd.concat(atb_parts, ignore_index=True)
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    assorted_dir = resolve_input_path(bundle, "interim", assorted["interim_subdir"])
    gcam = read(
        assorted_dir / assorted["jgcri_gcam"]["output_file"],
        assorted["jgcri_gcam"]["source_id"],
    )
    regen = read(
        assorted_dir / assorted["epri_us_regen"]["output_file"],
        assorted["epri_us_regen"]["source_id"],
    )
    if set(regen.loc[regen.metric.eq("efficiency"), "native_unit"]) != {"mpg-e"}:
        raise ValueError("Unexpected REGEN intercity fuel-economy units")
    manual_rules = load_harmonization_rules(bundle, "manual_parameters")
    _, manual_frames = validate_manual_registry(
        bundle,
        source_column=manual_rules["source_column"],
        notes_column=manual_rules["notes_column"],
        selected_files={rules["offroad"]["manual_file"]},
    )
    manual = manual_frames[rules["offroad"]["manual_file"]]
    paths.append(resolve_input_path(bundle, "manual", rules["offroad"]["manual_file"]))
    evidence = RoadEfficiencyEvidence(
        ceud=provincial,
        loads=loads,
        rating_audit=rating_audit,
        atb_audit=atb_audit,
        gcam=gcam,
        regen=regen,
        base_year=scenario.periods.base_year,
        rules=rules,
    )
    annual_records, payloads = [], []
    offroad_values = {}
    for edge in relationships.itertuples(index=False):
        for source_region in scenario.geography.regions:
            region = rules["region_output_map"].get(source_region, source_region)
            vintages = (
                [
                    v
                    for v in scenario.periods.existing
                    if (region, edge.tech, v) in existing_keys
                ]
                if edge.tech.endswith(rules["existing_suffix"]) and edge.kind != "unit"
                else scenario.periods.model
            )
            for vintage in vintages:
                years = (
                    scenario.periods.historical_years(vintage)
                    if edge.tech.endswith(rules["existing_suffix"]) and edge.kind != "unit"
                    else [scenario.periods.projection_year(vintage, legacy_at_end=True)]
                )
                if (
                    edge.tech.endswith(rules["existing_suffix"]) and edge.kind != "unit"
                    and vintage == scenario.periods.existing[0]
                ):
                    first_source_year = int(
                        provincial.year.min()
                        if edge.kind == "road"
                        else national.year.min()
                    )
                    years = [year for year in years if year >= first_source_year]
                    if not years:
                        raise ValueError(
                            f"No observed years for first vintage: {edge.tech}/{vintage}"
                        )
                if edge.tech in rules["historical_single_year_technologies"]:
                    source_year = rules["historical_single_year_technologies"][
                        edge.tech
                    ]
                    if vintage != scenario.periods.existing_vintage(source_year):
                        raise ValueError(
                            f"Unexpected historical vintage for {edge.tech}: {vintage}"
                        )
                    years = [source_year]
                values = []
                component_values = {}
                range_mix = historical_weights.get((edge.mode, edge.powertrain), {}) if edge.tech.endswith(rules["existing_suffix"]) else {}
                for year in years:
                    if edge.kind == "unit":
                        result = {
                            "efficiency": 1.0,
                            "treatment": "intentional_unit_efficiency",
                        }
                    elif edge.kind == "road":
                        if range_mix:
                            components = {
                                bucket: evidence.derive(source_region, edge.mode, bucket, year)
                                for bucket in range_mix
                            }
                            for bucket, component in components.items():
                                component_values.setdefault(bucket, []).append(component["efficiency"])
                                annual_records.append({
                                    **edge._asdict(), "powertrain": bucket,
                                    "region": region, "ceud_region": source_region,
                                    "vintage": vintage, "source_year": year,
                                    "trajectory": trajectory, "range_weight": range_mix[bucket],
                                    "range_representative_component": True, **component,
                                })
                            result = {"efficiency": 1 / sum(
                                range_mix[bucket] / component["efficiency"] for bucket, component in components.items()
                            ), "treatment": "OMEGA_range_weighted_consumption"}
                        else:
                            result = evidence.derive(
                                source_region, edge.mode, edge.powertrain, year
                            )
                    else:
                        key = (edge.mode, edge.powertrain, year)
                        if key not in offroad_values:
                            offroad_values[key] = derive_offroad_efficiency(
                                national,
                                manual,
                                mode=edge.mode,
                                powertrain=edge.powertrain,
                                year=year,
                                base_year=scenario.periods.base_year,
                                selectors=offroad_rules["energy_intensity_series"],
                                rules=rules["offroad"],
                            )
                        result = offroad_values[key]
                    values.append(result["efficiency"])
                    annual_records.append(
                        {
                            **edge._asdict(),
                            "region": region,
                            "ceud_region": source_region,
                            "vintage": vintage,
                            "source_year": year,
                            "trajectory": trajectory,
                            **result,
                        }
                    )
                payloads.append(
                    {
                        "region": region,
                        "tech": edge.tech,
                        "input_comm": edge.input_comm,
                        "output_comm": edge.output_comm,
                        "vintage": vintage,
                        "efficiency": 1 / sum(
                            share / float(np.mean(component_values[bucket])) for bucket, share in range_mix.items()
                        ) if range_mix else float(np.mean(values)),
                        "units": edge.units,
                        "notes": f"{edge.mode}/{edge.powertrain}; {trajectory}; source years {years[0]}..{years[-1]}",
                        "mode": edge.mode,
                        "kind": edge.kind,
                        "range_aggregated": bool(range_mix),
                    }
                )
    endpoints = [scenario.periods.projection_year(p, legacy_at_end=True) for p in scenario.periods.model]
    split_audit = phev_split_evidence(atb, endpoints=endpoints, rules=rules, fleet=fleet)
    split_audit = pd.concat([split_audit, existing_phev_split_evidence(
        atb_audit, endpoints=endpoints, rules=rules, range_weights=historical_weights,
        regions=scenario.geography.regions,
    )], ignore_index=True)
    digest = hashlib.sha256(
        json.dumps(
            {
                "files": sorted((p.name, file_sha256(p)) for p in set(paths)),
                "rules": rules,
                "road_outputs": road_rules,
                "offroad_outputs": offroad_rules,
                "fleet_rules": fleet.rules,
                "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
                "conversions": conversions,
                "trajectory": trajectory,
                "periods": scenario.periods.model_dump(),
                "existing_keys": sorted(existing_keys),
                "historical_range_rules": range_rules.existing.model_dump(),
                "historical_range_weights": range_evidence.bucket_totals.to_dict("records"),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    variant = {
        "input_digest": digest,
        "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
        "population_year": scenario.existing_capacity.vehicle_population_year,
        "trajectory": trajectory,
    }
    assumption = DataSet(
        data_id="transport-efficiency-structure:" + digest[:16],
        label="Transport unit-efficiency structural relationships",
        version=rules["transformation_version"],
        description="Backend-owned template edges and intentional unit efficiencies; not an external source.",
    )
    contexts, rows = [], []
    input_regions = {rules["region_output_map"].get(r, r): r for r in scenario.geography.regions}
    for (mode, region, range_aggregated), group in pd.DataFrame(payloads).groupby(["mode", "region", "range_aggregated"], sort=True):
        kind = group.kind.iloc[0]
        records = group.drop(columns=["mode", "kind", "range_aggregated"]).to_dict("records")
        if kind == "unit":
            rows.extend(
                Efficiency.model_validate({**record, "data_id": assumption.data_id})
                for record in records
            )
            continue
        components = efficiency_components(
            bundle, mode, rules, road_rules, offroad_rules, region=input_regions[region],
        )
        if range_aggregated:
            components.append((OMEGA, OMEGA_COMPONENT))
        context = efficiency_context(bundle, components, mode, {**variant, "region": region, "historical_range_aggregation": bool(range_aggregated)}, rules)
        contexts.append(context)
        rows.extend(validate_parameter_rows(Efficiency, records, context))
    splits = []
    for blend, spec in phev_blend_relationships(rules).items():
        for region in scenario.geography.regions:
            selected = split_audit.loc[split_audit.blend.eq(blend) & split_audit.region.eq(region)]
            electricity = float(selected.groupby("year").electricity_share.first().mean())
            components = [(ATB, "phev_vehicle_inputs"),
                          (ATB, "phev_utility_factor_ldv" if spec["aggregation_role"] == "ldv" else "phev_utility_factor_mdhd"),
                          ("argonne_rd_greet_2025_rev1", "fuel_heating_values"),
                          aggregation_component(bundle, spec["aggregation_role"], region)]
            if spec.get("existing_representative"):
                components.append((OMEGA, OMEGA_COMPONENT))
            split_context = efficiency_context(
                bundle, components, "phev_splits", {**variant, "region": region, "blend": blend}, rules,
            )
            contexts.append(split_context)
            regional_records = []
            for period in scenario.periods.model:
                for input_comm, proportion in (
                    (spec["electricity"], electricity),
                    (spec["fuel"], 1 - electricity),
                ):
                    regional_records.append(
                        {
                            "region": rules["region_output_map"].get(region, region),
                            "period": period,
                            "tech": spec["tech"],
                            "input_comm": input_comm,
                            "operator": rules["phev_split_operator"],
                            "proportion": proportion,
                            "notes": f"{trajectory}; regional fleet-weighted component consumption; mean endpoint energy share; fixed horizon",
                        }
                    )
            splits.extend(validate_parameter_rows(LimitTechInputSplit, regional_records, split_context))
    rows.sort(key=lambda r: (r.region, r.tech, r.vintage, r.input_comm, r.output_comm))
    splits.sort(key=lambda r: (r.region, r.period, r.tech, r.input_comm))
    regions = {rules["region_output_map"].get(r, r) for r in scenario.geography.regions}
    audit = validate_efficiency_outputs(
        rows,
        splits,
        relationships=relationships,
        regions=regions,
        periods=list(scenario.periods.model),
        existing_keys=existing_keys,
        rules=rules,
    )
    audit.update(variant)
    audit["excluded_technologies"] = sorted(
        set(technology.tech) - set(relationships.tech)
    )
    audit["hybrid_evidence_counts"] = (
        classified.classification_method.value_counts().to_dict()
    )
    audit["rating_index_fallbacks"] = len(evidence.findings)
    audit["historical_source_backed_rows_without_capacity"] = len(
        existing_keys - capacity_keys
    )
    audit["phev_energy_basis"] = rules["atb"]["phev_energy_basis"]
    audit["existing_ev_representatives"] = {
        "rules": range_rules.existing.model_dump(),
        "weights": range_evidence.bucket_totals.to_dict("records"),
        "rating_observations_by_range": classified.loc[classified.powertrain.str.startswith(("bev", "phev"))].groupby("powertrain").size().to_dict(),
        "stock_redistributed_by_range": False,
    }
    selected_phev = phev_source.loc[phev_source.trajectory.eq(trajectory)]
    audit["phev_source_reconciliation"] = {
        "rows": len(selected_phev),
        "outside_source_tolerance": int(
            (~selected_phev.reconciliation_within_tolerance).sum()
        ),
        "maximum_absolute_fuel_economy_difference": float(
            selected_phev.reconciliation_absolute_difference.abs().max()
        ),
        "policy": "diagnostic only; output fuel economy never corrects derived PHEV values",
    }
    audit["omitted_rating_rows"] = int((~classified.included).sum())
    audit["ambiguous_epa_rows"] = int(
        classified.ambiguous_epa_match.fillna(False).sum()
    )
    audit["renormalized_rating_cells"] = int(
        rating_audit.loc[
            abs(rating_audit.observed_weight_sum - 1)
            > rules["tolerances"]["weight_sum"],
            ["ceud_class", "powertrain", "year"],
        ]
        .drop_duplicates()
        .shape[0]
    )
    findings = pd.DataFrame(
        sorted(evidence.findings),
        columns=["region", "mode", "powertrain", "year", "finding"],
    )
    interim, processed, validation = (
        resolve_artifact_path(bundle, "efficiencies_" + layer)
        for layer in ("interim", "processed", "validation")
    )
    for frame, path in (
        (pd.DataFrame(annual_records), interim / rules["files"]["annual"]),
        (loads, interim / rules["files"]["loads"]),
        (classified, interim / rules["files"]["ratings"]),
        (atb_audit, interim / rules["files"]["atb"]),
        (rating_audit, interim / rules["files"]["rating_weights"]),
        (split_audit, interim / rules["files"]["split_evidence"]),
        (relationships, interim / rules["files"]["relationships"]),
        (findings, validation / rules["files"]["findings"]),
        (
            pd.DataFrame([r.model_dump(mode="json") for r in rows]),
            processed / rules["files"]["efficiency"],
        ),
        (
            pd.DataFrame([r.model_dump(mode="json") for r in splits]),
            processed / rules["files"]["splits"],
        ),
    ):
        write_dataframe_atomic(frame, path)
    validation.mkdir(parents=True, exist_ok=True)
    (validation / rules["files"]["integrity"]).write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    LOGGER.info("Prepared efficiencies: %s", audit)
    return EfficiencyPreparation(rows, splits, assumption, contexts, audit)


def efficiency_components(
    bundle: ConfigBundle, mode: str, rules: dict, road: dict, offroad: dict, *, region: str,
) -> list[tuple]:
    if mode not in rules["road_classes"]:
        result = [
            (
                "nrcan_ceud_transport_national",
                offroad["energy_intensity_series"][mode]["national_table"],
            ),
            ("epri_us_regen_2025_transportation", "nonroad_efficiency_multipliers"),
        ]
        if mode == "freight_marine":
            result.append(("argonne_rd_greet_2025_rev1", "marine_hfo_energy_intensity"))
        return result
    spec = rules["road_classes"][mode]
    result = [
        (CEUD, road["activity_series"][mode]["table_id"]),
        (CEUD, spec["stock_table"]),
    ]
    if "intensity_table" in spec:
        result.append((CEUD, spec["intensity_table"]))
    if spec["pathway"] == "motorcycle":
        return result + [("jgcri_gcam_motorcycle_inputs", "canada_motorcycle_inputs")]
    result.extend(
        (ATB, component)
        for component in (
            "vehicles",
            "phev_vehicle_inputs",
            "phev_utility_factor_ldv",
            "phev_utility_factor_mdhd",
        )
    )
    result.append(("argonne_rd_greet_2025_rev1", "fuel_heating_values"))
    if spec["pathway"] == "ldv":
        result.append(aggregation_component(bundle, "ldv", region))
        for source in (
            "nrcan_fuel_consumption_ratings",
            "fueleconomy_gov_vehicle_data",
            "reviewed_mto_make_model_evidence",
        ):
            result.extend(
                (source, component)
                for component in bundle.sources.sources[source].components
            )
    if mode == "medium_trucks":
        result.append(aggregation_component(bundle, "medium_trucks", region))
    if mode == "heavy_trucks":
        result.append(aggregation_component(bundle, "heavy_truck_haul", region))
    if spec["pathway"] == "intercity":
        result.append(("epri_us_regen_2025_transportation", "intercity_bus_charts"))
    return result


def efficiency_context(
    bundle: ConfigBundle, components: list[tuple], mode: str, variant: dict, rules: dict
) -> ResolvedProvenance:
    if inactive := {source for source, _ in components} - active_source_keys(bundle):
        raise ValueError(f"Inactive efficiency sources: {inactive}")
    inputs = [
        resolve_provenance(
            bundle.sources,
            source_key=source,
            component_key=component,
            transformation="efficiency_input",
            transformation_version=rules["transformation_version"],
        )
        for source, component in sorted(
            set(components), key=lambda pair: (pair[0], str(pair[1]))
        )
    ]
    governing = source_id_mapping(bundle.sources)[components[0][0]]
    return resolve_composite_provenance(
        inputs=inputs,
        dataset_key="efficiency." + mode,
        transformation="Configured transport service efficiency and energy-input split",
        transformation_version=rules["transformation_version"],
        governing_source_id=governing,
        value_variant=variant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    prepare_efficiency_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
