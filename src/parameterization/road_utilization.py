"""Prepare road capacity-to-activity and annual utilization without SQLite ownership."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import CapacityToActivity, DataSet, LimitAnnualCapacityFactor

from parameterization.road_stocks_and_demands import (
    aggregate_normalized_mileage_profiles,
    derive_road_annual_utilization,
    flat_road_capacity_factor_records,
    mean_age_utilization_for_period,
    reconcile_road_capacity_to_activity,
)
from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    load_conversion_factors,
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
CEUD = "nrcan_ceud_transport_provincial"


@dataclass(frozen=True)
class RoadUtilizationPreparation:
    assumption_dataset: DataSet
    capacity_to_activity_rows: list[CapacityToActivity]
    flat_factor_rows: list[LimitAnnualCapacityFactor]
    age_factor_artifact: pd.DataFrame | None
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, Any]


def national_medium_atb_weights(
    wards: pd.DataFrame,
    mileage: pd.DataFrame,
    *,
    rules: dict[str, Any],
    max_age: int,
) -> pd.DataFrame:
    """Resolve Wards GVWR shares to equal distinct non-bus ATB freight profiles."""
    selected = wards.loc[
        wards.vehicle_scope.eq("mhdv")
        & wards.nrcan_ceud_class.eq("Medium Trucks")
        & wards.year.eq(int(rules["national_medium_share_year"]))
    ].copy()
    pattern = re.compile(str(rules["medium_atb_class_pattern"]))
    selected["atb_gvwr_class"] = selected.wards_size_class.map(
        lambda value: pattern.match(str(value)).group(0).strip()
        if pattern.match(str(value)) else None
    )
    if (
        selected.empty or selected.atb_gvwr_class.isna().any()
        or selected.atb_gvwr_class.duplicated().any()
        or set(selected.atb_gvwr_class) != set(rules["medium_atb_classes"])
        or abs(float(selected.market_share.sum()) - 1.0) > 1e-8
    ):
        raise ValueError("National medium-truck GVWR shares are incomplete or invalid")
    if rules["medium_within_class_method"] != "equal_distinct_freight_profiles":
        raise ValueError("Unsupported medium-truck within-class treatment")
    records: list[dict[str, Any]] = []
    for item in selected.itertuples(index=False):
        class_mileage = mileage.loc[
            mileage.vehicle_class.str.startswith(item.atb_gvwr_class + " ")
            & mileage.year_index.le(max_age)
        ].copy()
        for excluded in rules["excluded_medium_atb_labels"]:
            class_mileage = class_mileage.loc[
                ~class_mileage.vehicle_class.str.contains(r"\b" + re.escape(excluded) + r"\b")
            ]
        profiles: dict[tuple[float, ...], str] = {}
        for vehicle_class, group in class_mileage.groupby("vehicle_class", sort=True):
            values = group.sort_values("year_index")["vmt_mi"].astype(float).tolist()
            ages = group.sort_values("year_index")["year_index"].astype(int).tolist()
            if ages != list(range(max_age + 1)):
                raise ValueError(f"Incomplete medium-truck ATB ages: {vehicle_class}")
            profiles.setdefault(tuple(values), vehicle_class)
        if not profiles:
            raise ValueError(f"No non-bus ATB freight profile for {item.atb_gvwr_class}")
        for representative in sorted(profiles.values()):
            records.append({
                "nrcan_ceud_class": "Medium Trucks",
                "nlr_atb_class": representative,
                "aggregation_weight": float(item.market_share) / len(profiles),
                "weight_source": "national_wards",
            })
    return pd.DataFrame(records).sort_values("nlr_atb_class").reset_index(drop=True)


def heavy_haul_atb_weights(
    freight: pd.DataFrame,
    *,
    source_region: str,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Use registered tonne-km haul shares for the two ATB heavy truck profiles."""
    selected = freight.loc[freight.scenario_region.eq(source_region)]
    if selected.empty or (pd.to_numeric(selected.tonne_kilometres, errors="raise") < 0).any():
        raise ValueError(f"Invalid freight haul evidence for {source_region}")
    totals = selected.groupby("haul_class").tonne_kilometres.sum()
    mapping = rules["heavy_haul_atb_classes"]
    if set(totals.index) != set(mapping) or not isfinite(float(totals.sum())) or totals.sum() <= 0:
        raise ValueError(f"Incomplete freight haul classes for {source_region}")
    return pd.DataFrame([
        {"nrcan_ceud_class": "Heavy Trucks", "nlr_atb_class": mapping[haul],
         "aggregation_weight": float(totals[haul] / totals.sum()),
         "weight_source": "statcan_tonne_km"}
        for haul in sorted(mapping)
    ])


def prepare_age_profiles(
    bundle: ConfigBundle,
    *,
    rules: dict[str, Any],
    output_regions: dict[str, str],
) -> pd.DataFrame:
    """Build dimensionless age profiles with reviewed class and haul ownership."""
    ceiling = bundle.scenario.switches.survival_curve_max_age
    if ceiling < bundle.scenario.periods.step:
        raise ValueError("Age ceiling must cover one complete model period")
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    mileage_file = resolve_input_path(
        bundle, "interim", atb_rules["interim_subdir"],
        atb_rules["components"]["vmt"]["output_file"],
    )
    mileage = pd.read_csv(mileage_file)
    if set(mileage.source_id) != {"nlr_atb_transportation_2024"}:
        raise ValueError("ATB mileage artifact source identity differs from registry")
    mileage = mileage.loc[mileage.year_index.le(ceiling)].copy()
    conversion = float(load_conversion_factors(bundle)["length"]["mile_to_km"])
    aggregation_rules = load_harmonization_rules(bundle, "road_aggregation")
    ldv_file = resolve_artifact_path(bundle, "road_aggregation") / aggregation_rules["nlr_weights_file"]
    ldv_weights = pd.read_csv(ldv_file)
    ldv_weights = ldv_weights.loc[
        ldv_weights.weight_basis.eq(rules["ldv_weight_basis"])
        & ldv_weights.nrcan_ceud_class.isin(["Car", "Light Truck"])
    ].copy()
    if ldv_weights.empty or ldv_weights.report_year.nunique() != 1:
        raise ValueError("Reviewed LDV road aggregation weights are incomplete")
    ldv_weights = ldv_weights[["nrcan_ceud_class", "nlr_atb_class", "aggregation_weight"]]
    medium_source = bundle.scenario.road_utilization.medium_truck_weight_source
    if medium_source == "national_wards":
        wards = pd.read_csv(resolve_input_path(bundle, "manual", "vehicle_class_market_shares.csv"))
        medium_weights = national_medium_atb_weights(wards, mileage, rules=rules, max_age=ceiling)
    elif medium_source == "ontario_report4":
        raise ValueError("Ontario Report 4 medium-truck weights require an Ontario-only reviewed mapping")
    else:
        raise ValueError(f"Unsupported medium-truck aggregation source: {medium_source}")
    freight_rules = load_harmonization_rules(bundle, "statcan_tables")["freight"]
    freight_file = resolve_input_path(
        bundle, "interim", load_harmonization_rules(bundle, "statcan_tables")["interim_subdir"],
        freight_rules["output_file"],
    )
    freight = pd.read_csv(freight_file)
    if set(freight.table_id) != {"23-10-0142-01"}:
        raise ValueError("Freight haul artifact identity differs from registered table")
    all_profiles: list[pd.DataFrame] = []
    for ceud_region, region in output_regions.items():
        source_region = rules["freight_source_region_map"].get(ceud_region, ceud_region)
        heavy_weights = heavy_haul_atb_weights(freight, source_region=source_region, rules=rules)
        weights = pd.concat([ldv_weights, medium_weights, heavy_weights], ignore_index=True)
        profile = aggregate_normalized_mileage_profiles(mileage, weights, mile_to_km=conversion)
        profile.insert(0, "region", region)
        profile["weight_source"] = profile.nrcan_ceud_class.map({
            "Car": "ontario_mto", "Light Truck": "ontario_mto",
            "Medium Trucks": medium_source, "Heavy Trucks": "statcan_tonne_km",
        })
        all_profiles.append(profile)
    result = pd.concat(all_profiles, ignore_index=True)
    if result.duplicated(["region", "nrcan_ceud_class", "age"]).any():
        raise ValueError("Duplicate region/class/age utilization profile")
    return result.sort_values(["region", "nrcan_ceud_class", "age"]).reset_index(drop=True)


def validate_age_artifact(
    age_rows: pd.DataFrame,
    profiles: pd.DataFrame,
    *,
    regions: set[str],
    periods: list[int],
    max_age: int,
    age_classes: dict[str, str],
) -> dict[str, Any]:
    """Validate the two-dimensional artifact that v4 cannot insert."""
    if age_rows.empty or set(age_rows.region) != regions:
        raise ValueError("Vintage-period road factor region coverage is incomplete")
    keys = ["region", "tech_or_group", "vintage", "period", "output_comm"]
    if age_rows.duplicated(keys).any() or age_rows[keys].isna().any().any():
        raise ValueError("Duplicate or null vintage-period road factor key")
    if (not set(age_rows.period).issubset(periods)
            or not set(age_rows.road_class).issubset(age_classes)):
        raise ValueError("Vintage-period road factor uses an unexpected period or class")
    if ((age_rows.period < age_rows.vintage).any()
            or (age_rows.period + age_rows.period_step - age_rows.vintage > max_age).any()):
        raise ValueError("Vintage-period road factor exceeds configured age ceiling")
    if age_rows.factor.map(lambda value: not isfinite(value) or not 0 <= value <= 1).any():
        raise ValueError("Vintage-period road factor is outside the unit interval")
    expected_profile = {
        (region, road_class, age)
        for region in regions
        for road_class in set(age_classes.values())
        for age in range(max_age + 1)
    }
    profile_keys = set(zip(
        profiles.region, profiles.nrcan_ceud_class, profiles.age, strict=True
    ))
    if len(profiles) != len(expected_profile) or profile_keys != expected_profile:
        raise ValueError("Normalized age profile coverage is incomplete or duplicated")
    peaks = profiles.groupby(["region", "nrcan_ceud_class"]).normalized_utilization.max()
    if not peaks.map(lambda value: abs(value - 1.0) < 1e-10).all():
        raise ValueError("Every region/class age profile must peak at one")
    if profiles.normalized_utilization.map(
        lambda value: not isfinite(value) or not 0 <= value <= 1
    ).any():
        raise ValueError("Normalized age profile is outside the unit interval")
    return {
        "age_factor_rows": len(age_rows),
        "age_profile_rows": len(profiles),
        "age_ceiling": max_age,
        "sqlite_insertion": "blocked_upstream_vintage_plus_period_schema_gap",
    }


def build_age_factor_artifact(
    bundle: ConfigBundle,
    *,
    baseline: pd.DataFrame,
    technology: pd.DataFrame,
    activity: dict[str, Any],
    stock: dict[str, Any],
    rules: dict[str, Any],
    output_regions: dict[str, str],
    input_digest: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[ResolvedProvenance], dict[str, Any]]:
    """Retain technology vintage and model period separately for age utilization."""
    scenario = bundle.scenario
    max_age = scenario.switches.survival_curve_max_age
    profiles = prepare_age_profiles(bundle, rules=rules, output_regions=output_regions)
    age_classes = rules["age_profile_classes"]
    if set(age_classes) | set(rules["flat_only_classes"]) != set(activity):
        raise ValueError("Age and flat-only road classes do not partition road activity")
    age_inputs = [
        resolve_input_path(bundle, "interim", load_harmonization_rules(bundle, "nlr_atb_autonomie")["interim_subdir"],
                           load_harmonization_rules(bundle, "nlr_atb_autonomie")["components"]["vmt"]["output_file"]),
        resolve_artifact_path(bundle, "road_aggregation") /
        load_harmonization_rules(bundle, "road_aggregation")["nlr_weights_file"],
        resolve_input_path(bundle, "manual", "vehicle_class_market_shares.csv"),
        resolve_input_path(bundle, "interim", load_harmonization_rules(bundle, "statcan_tables")["interim_subdir"],
                           load_harmonization_rules(bundle, "statcan_tables")["freight"]["output_file"]),
    ]
    age_digest = hashlib.sha256(json.dumps({
        "baseline": input_digest,
        "age_inputs": [(path.name, file_sha256(path)) for path in age_inputs],
        "mile_to_km": float(load_conversion_factors(bundle)["length"]["mile_to_km"]),
        "max_age": max_age,
        "medium_source": scenario.road_utilization.medium_truck_weight_source,
    }, sort_keys=True).encode()).hexdigest()
    source_ids = source_id_mapping(bundle.sources)
    contexts: list[ResolvedProvenance] = []
    records: list[dict[str, Any]] = []
    for ceud_region, region in sorted(output_regions.items()):
        for road_class, profile_class in age_classes.items():
            atb_component = "vmt_ldv" if road_class in {
                "cars", "passenger_light_trucks", "freight_light_trucks"
            } else "vmt_mdhd"
            components: list[tuple[str, str | int]] = [
                (CEUD, activity[road_class]["table_id"]),
                (CEUD, stock[road_class]["table_id"]),
                ("nlr_atb_transportation_2024", atb_component),
            ]
            if profile_class in {"Car", "Light Truck"}:
                components.append(("ontario_ministry_transport_vehicle_population", "A"))
            elif profile_class == "Medium Trucks":
                components.append(("wards_intelligence_2022_sales_shares", "vehicle_class_market_shares"))
            elif profile_class == "Heavy Trucks":
                components.append(("statcan_transport_tables", "23-10-0142-01"))
            context = resolve_composite_provenance(
                inputs=[resolve_provenance(
                    bundle.sources, source_key=source, component_key=component,
                    transformation="road_age_utilization_input", transformation_version="1",
                ) for source, component in components],
                dataset_key=f"road_utilization.age.{road_class}.{region}",
                transformation="CEUD baseline scaled by normalized ATB mileage and averaged over five annual ages",
                transformation_version="1",
                governing_source_id=source_ids[CEUD],
                value_variant={"age_input_digest": age_digest, "road_class": road_class,
                               "region": region, "max_age": max_age,
                               "mean_window": "period_plus_1_through_period_plus_step"},
            )
            contexts.append(context)
            source_profile = profiles.loc[
                profiles.region.eq(region) & profiles.nrcan_ceud_class.eq(profile_class)
            ][["age", "normalized_utilization"]]
            source_baseline = baseline.loc[
                baseline.region.eq(region) & baseline.road_class.eq(road_class)
            ]
            if len(source_baseline) != 1:
                raise ValueError(f"Missing baseline for {(region, road_class)}")
            value = float(source_baseline.iloc[0].utilization)
            owners = technology.loc[technology.category.eq(road_class)]
            for owner in owners.itertuples(index=False):
                if owner.tech.endswith("_EX"):
                    vintages = scenario.periods.existing
                elif owner.tech.endswith("_N"):
                    vintages = scenario.periods.model
                else:
                    raise ValueError(f"Unknown road technology vintage ownership: {owner.tech}")
                for vintage in vintages:
                    for period in scenario.periods.model:
                        if period < vintage or period + scenario.periods.step - vintage > max_age:
                            continue
                        factor = mean_age_utilization_for_period(
                            source_profile, baseline_utilization=value,
                            vintage=vintage, period=period, step=scenario.periods.step,
                        )
                        records.append({
                            "region": region, "road_class": road_class,
                            "tech_or_group": owner.tech, "vintage": vintage,
                            "period": period, "period_step": scenario.periods.step,
                            "output_comm": activity[road_class]["commodity"],
                            "operator": "e", "factor": factor,
                            "baseline_utilization": value,
                            "age_first_year": period + 1 - vintage,
                            "age_last_year": period + scenario.periods.step - vintage,
                            "unit": "dimensionless",
                            "data_source": context.governing_source_id,
                            "data_id": context.data_id,
                            **context.data_quality.row_fields(),
                        })
    age_rows = pd.DataFrame(records).sort_values(
        ["region", "road_class", "tech_or_group", "vintage", "period"]
    ).reset_index(drop=True)
    audit = validate_age_artifact(
        age_rows, profiles, regions=set(output_regions.values()),
        periods=list(scenario.periods.model), max_age=max_age,
        age_classes=age_classes,
    )
    audit["age_input_digest"] = age_digest
    return age_rows, profiles, contexts, audit


def select_baseline_years(
    provincial: pd.DataFrame,
    *,
    base_year: int,
    observations: int,
    excluded_years: list[int],
) -> list[int]:
    """Take the latest complete-number eligible CEUD years through the base year."""
    if observations <= 0 or len(set(excluded_years)) != len(excluded_years):
        raise ValueError("Invalid baseline utilization year rules")
    years = sorted(
        int(year) for year in pd.to_numeric(provincial["year"], errors="raise").unique()
        if int(year) <= base_year and int(year) not in excluded_years
    )
    selected = years[-observations:]
    if len(selected) != observations or selected[-1] != base_year:
        raise ValueError("CEUD lacks the requested latest eligible baseline years")
    return selected


def _assumption_dataset(manual_file: Path, technology_file: Path) -> DataSet:
    """Identify a versioned manual modeling assumption without external DQ or citation."""
    digest = hashlib.sha256()
    for path in (manual_file, technology_file):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    version = digest.hexdigest()[:16]
    return DataSet(
        data_id=f"canoe-road-capacity-to-activity:{version}",
        label="CANOE reviewed manual road capacity-to-activity assumptions",
        version=version,
        description=(
            "Version-controlled capacity_to_activity.csv values expanded through "
            "the backend technology template; internal modeling assumption."
        ),
    )


def validate_road_utilization(
    annual: pd.DataFrame,
    baseline: pd.DataFrame,
    c2a_rows: list[CapacityToActivity],
    flat_rows: list[LimitAnnualCapacityFactor],
    *,
    regions: set[str],
    road_classes: set[str],
    years: list[int],
    periods: list[int],
    flat_classes: set[str],
    technology: pd.DataFrame,
) -> dict[str, Any]:
    """Check input and schema-supported output coverage before artifact publication."""
    expected_annual = {
        (region, road_class, year)
        for region in regions for road_class in road_classes for year in years
    }
    actual_annual = set(zip(annual.region, annual.road_class, annual.year, strict=True))
    if len(annual) != len(expected_annual) or actual_annual != expected_annual:
        raise ValueError("Annual CEUD utilization coverage is incomplete or duplicated")
    expected_baseline = {
        (region, road_class) for region in regions for road_class in road_classes
    }
    actual_baseline = set(zip(baseline.region, baseline.road_class, strict=True))
    if len(baseline) != len(expected_baseline) or actual_baseline != expected_baseline:
        raise ValueError("Baseline road utilization coverage is incomplete or duplicated")
    if annual["utilization"].map(lambda value: not isfinite(value) or not 0 <= value <= 1).any():
        raise ValueError("Annual road utilization is outside the unit interval")
    if baseline["utilization"].map(lambda value: not isfinite(value) or not 0 <= value <= 1).any():
        raise ValueError("Baseline road utilization is outside the unit interval")
    road_techs = technology.loc[technology.category.isin(road_classes)]
    expected_c2a = {(region, tech) for region in regions for tech in road_techs.tech}
    actual_c2a = {(row.region, row.tech) for row in c2a_rows}
    if len(c2a_rows) != len(expected_c2a) or actual_c2a != expected_c2a:
        raise ValueError("Capacity-to-activity row coverage is incomplete or duplicated")
    flat_techs = road_techs.loc[road_techs.category.isin(flat_classes)]
    expected_flat = {
        (region, tech, period)
        for region in regions for tech in flat_techs.tech for period in periods
    }
    actual_flat = {(row.region, row.tech_or_group, row.vintage) for row in flat_rows}
    if len(flat_rows) != len(expected_flat) or actual_flat != expected_flat:
        raise ValueError("Flat annual factor row coverage is incomplete or duplicated")
    if any(row.dq_cred is not None or row.data_source is not None for row in c2a_rows):
        raise ValueError("Internal capacity-to-activity assumptions must not carry source DQ")
    return {
        "baseline_years": years,
        "annual_rows": len(annual),
        "baseline_rows": len(baseline),
        "capacity_to_activity_rows": len(c2a_rows),
        "flat_factor_rows": len(flat_rows),
    }


def prepare_road_utilization(
    bundle: ConfigBundle,
    *,
    technology: pd.DataFrame | None = None,
) -> RoadUtilizationPreparation:
    """Prepare deterministic road rows and audit artifacts from registered local inputs."""
    scenario = bundle.scenario
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")
    factor_rules = rules["capacity_factor"]
    demand_rules = rules["demand"]
    capacity_rules = rules["existing_capacity"]
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    activity = demand_rules["activity_series"]
    road_classes = set(activity)
    stock = {
        **capacity_rules["ceud_stock_series"],
        **factor_rules["additional_stock_series"],
    }
    if set(stock) != road_classes:
        raise ValueError("CEUD activity and stock selectors do not cover the same road classes")
    manual_file = resolve_input_path(bundle, "manual", factor_rules["manual_file"])
    technology_file = resolve_input_path(bundle, "template", "technology.csv")
    manual = pd.read_csv(manual_file)
    technology = pd.read_csv(technology_file) if technology is None else technology.copy()
    expanded = reconcile_road_capacity_to_activity(
        manual, technology, road_classes=road_classes,
        activity_units={name: selector["units"] for name, selector in activity.items()},
    )
    assumption_dataset = _assumption_dataset(manual_file, technology_file)
    regions = list(scenario.geography.regions)
    output_regions = {demand_rules["region_output_map"].get(region, region) for region in regions}
    c2a_rows = [
        CapacityToActivity.model_validate({
            "region": region,
            "tech": item.tech,
            "c2a": float(item.c2a),
            "units": item.units,
            "notes": item.notes,
            "data_id": assumption_dataset.data_id,
        })
        for region in sorted(output_regions)
        for item in expanded.itertuples(index=False)
    ]
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])
    ceud_files = [
        ceud_dir / ceud_rules["region_output_template"].format(region=region.lower())
        for region in regions
    ]
    provincial = pd.concat([pd.read_csv(path) for path in ceud_files], ignore_index=True)
    if set(provincial.region) != set(regions):
        raise ValueError("CEUD region coverage differs from scenario")
    years = select_baseline_years(
        provincial, base_year=scenario.periods.base_year,
        observations=int(factor_rules["baseline_observations"]),
        excluded_years=list(factor_rules["excluded_years"]),
    )
    c2a_by_class = expanded.drop_duplicates("category").set_index("category")["c2a"].to_dict()
    annual = derive_road_annual_utilization(
        provincial, regions=regions, years=years,
        activity_series=activity, stock_series=stock,
        capacity_by_class=c2a_by_class,
        region_output_map=demand_rules["region_output_map"],
        activity_to_billion_factor=float(demand_rules["activity_to_billion_factor"]),
    )
    baseline = annual.groupby(["region", "road_class"], as_index=False)["utilization"].mean()
    baseline["unit"] = "dimensionless"
    flat_classes = (
        set(factor_rules["flat_only_classes"])
        if scenario.switches.vkt_schedules else road_classes
    )
    flat_payloads = flat_road_capacity_factor_records(
        baseline.loc[baseline.road_class.isin(flat_classes)],
        expanded.loc[expanded.category.isin(flat_classes)],
        periods=list(scenario.periods.model),
        commodity_by_class={name: activity[name]["commodity"] for name in flat_classes},
    )
    input_digest = hashlib.sha256(json.dumps({
        "ceud": [(path.name, file_sha256(path)) for path in ceud_files],
        "manual": file_sha256(manual_file),
        "technology": file_sha256(technology_file),
        "activity_selectors": activity,
        "stock_selectors": stock,
        "activity_to_billion_factor": demand_rules["activity_to_billion_factor"],
        "rules": factor_rules,
        "years": years,
    }, sort_keys=True).encode()).hexdigest()
    contexts: list[ResolvedProvenance] = []
    flat_rows: list[LimitAnnualCapacityFactor] = []
    source_id = source_id_mapping(bundle.sources)[CEUD]
    for road_class in sorted(flat_classes):
        inputs = [
            resolve_provenance(
                bundle.sources, source_key=CEUD, component_key=selector["table_id"],
                transformation="road_utilization_input", transformation_version="1",
            )
            for selector in (activity[road_class], stock[road_class])
        ]
        context = resolve_composite_provenance(
            inputs=inputs,
            dataset_key=f"road_utilization.flat.{road_class}",
            transformation="Mean CEUD annual activity-to-stock ratio divided by manual capacity-to-activity",
            transformation_version="1", governing_source_id=source_id,
            value_variant={"input_digest": input_digest,
                           "manual_assumption_data_id": assumption_dataset.data_id,
                           "vkt_schedules": scenario.switches.vkt_schedules},
        )
        contexts.append(context)
        selected = [
            record for record in flat_payloads
            if record["tech_or_group"] in set(expanded.loc[
                expanded.category.eq(road_class), "tech"
            ])
        ]
        flat_rows.extend(validate_parameter_rows(LimitAnnualCapacityFactor, selected, context))
    age_rows: pd.DataFrame | None = None
    age_profiles: pd.DataFrame | None = None
    age_audit: dict[str, Any] = {}
    if scenario.switches.vkt_schedules:
        age_rows, age_profiles, _age_contexts, age_audit = build_age_factor_artifact(
            bundle, baseline=baseline, technology=expanded,
            activity=activity, stock=stock, rules=factor_rules,
            output_regions={
                region: demand_rules["region_output_map"].get(region, region)
                for region in regions
            }, input_digest=input_digest,
        )
    audit = validate_road_utilization(
        annual, baseline, c2a_rows, flat_rows,
        regions=output_regions, road_classes=road_classes, years=years,
        periods=list(scenario.periods.model), flat_classes=flat_classes,
        technology=expanded,
    )
    audit.update({
        "vkt_schedules": scenario.switches.vkt_schedules,
        "manual_assumption_data_id": assumption_dataset.data_id,
        "input_content_digest": input_digest,
        **age_audit,
    })
    output = resolve_artifact_path(bundle, "road_utilization_processed")
    write_dataframe_atomic(annual, output / factor_rules["annual_audit_file"])
    write_dataframe_atomic(baseline, output / factor_rules["baseline_file"])
    write_dataframe_atomic(
        pd.DataFrame([row.model_dump(mode="python") for row in c2a_rows]),
        output / factor_rules["c2a_file"],
    )
    write_dataframe_atomic(
        pd.DataFrame([row.model_dump(mode="python") for row in flat_rows]),
        output / factor_rules["flat_file"],
    )
    if age_rows is not None and age_profiles is not None:
        write_dataframe_atomic(age_profiles, output / factor_rules["age_profile_file"])
        write_dataframe_atomic(age_rows, output / factor_rules["age_factor_file"])
        gap_path = (
            resolve_artifact_path(bundle, "road_utilization_validation")
            / factor_rules["insertion_gap_file"]
        )
        gap_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = gap_path.with_suffix(gap_path.suffix + ".tmp")
        temporary.write_text(json.dumps({
            "parameter": "limit_annual_capacity_factor",
            "artifact": str(output / factor_rules["age_factor_file"]),
            "required_key": ["region", "tech_or_group", "vintage", "period", "output_comm"],
            "canoe_schema_v4_key": ["region", "tech_or_group", "vintage", "output_comm", "operator", "data_id"],
            "status": "not_inserted",
            "reason": "The authoritative canoe_schema v4 row and DDL have one period-linked vintage column and no second period index.",
            "rows": len(age_rows),
        }, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, gap_path)
    LOGGER.info("Prepared %s road C2A and %s flat annual factor rows", len(c2a_rows), len(flat_rows))
    return RoadUtilizationPreparation(
        assumption_dataset=assumption_dataset,
        capacity_to_activity_rows=c2a_rows,
        flat_factor_rows=flat_rows,
        age_factor_artifact=age_rows,
        provenance_contexts=contexts,
        audit=audit,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    prepare_road_utilization(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
