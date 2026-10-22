"""Build transport efficiency and PHEV input splits independently of SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import zipfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import numpy as np
import pandas as pd
from canoe_schema.v4_0 import DataSet, Efficiency, LimitTechInputSplit

from fetching.fueleconomy_vehicles import build_request, validate_cache
from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.manual_parameters import validate_manual_registry
from parameterization.ldv_ev_ranges import RangeEvidence, existing_range_weights, module_rules as range_module_rules
from fetching.epa_omega_baseline import SOURCE as OMEGA, COMPONENT as OMEGA_COMPONENT, build_request as omega_request
from parameterization.offroad_efficiencies import derive_offroad_consumption
from parameterization.road_efficiencies import (
    RoadEfficiencyEvidence,
    aggregate_atb,
    aggregate_ratings,
    classify_ratings,
    derive_load_factors,
    require_observed_value,
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
)


LOGGER = logging.getLogger(__name__)
ATB = "nlr_atb_transportation"
CEUD = "nrcan_ceud_transport_provincial"


@dataclass(frozen=True)
class EfficiencyPreparation:
    efficiency_rows: list[Efficiency]
    split_rows: list[LimitTechInputSplit]
    assumption_dataset: DataSet
    provenance_contexts: list[ResolvedProvenance]
    audit: dict


def aggregate_consumption_period(observations: list[dict], weights: list[float]) -> dict:
    """Average consumption and load on one support, then convert to service efficiency."""
    if not observations or len(observations) != len(weights):
        raise ValueError("Consumption aggregation requires matching nonempty observations and weights")
    temporal = np.asarray(weights, dtype=float)
    if not np.isfinite(temporal).all() or (temporal < 0).any() or temporal.sum() <= 0:
        raise ValueError("Consumption aggregation weights must be finite, nonnegative and positive in total")
    units = {row["native_unit"] for row in observations}
    if len(units) != 1:
        raise ValueError("Consumption aggregation cannot mix native units")
    native_unit = units.pop()
    if native_unit not in {"MJ/vkm", "MJ/Pkm", "MJ/Tkm", "PJ/PJ"}:
        raise ValueError(f"Unsupported consumption aggregation unit: {native_unit}")
    intensity = np.asarray([row["native_intensity"] for row in observations], dtype=float)
    try:
        loads = np.asarray([
            row["load_factor"] if native_unit == "MJ/vkm" else row.get("load_factor", 1.0)
            for row in observations
        ], dtype=float)
    except KeyError as exc:
        raise ValueError("Vehicle-distance consumption requires an explicit load factor") from exc
    if (not np.isfinite(intensity).all() or (intensity <= 0).any()
            or not np.isfinite(loads).all() or (loads <= 0).any()):
        raise ValueError("Consumption aggregation requires finite positive consumption and load")
    if native_unit != "MJ/vkm" and not np.equal(loads, 1.0).all():
        raise ValueError("Service-intensity observations must use unit load")
    consumption = float(np.average(intensity, weights=temporal))
    load = float(np.average(loads, weights=temporal))
    result = {"native_intensity": consumption, "native_unit": native_unit,
              "load_factor": load, "efficiency": load / consumption}
    has_year = ["source_year" in row for row in observations]
    if any(has_year):
        if not all(has_year):
            raise ValueError("Consumption aggregation source years must cover every observation")
        years = np.asarray([row["source_year"] for row in observations], dtype=float)
        if not np.isfinite(years).all():
            raise ValueError("Consumption aggregation requires finite source years")
        result["weighted_source_year"] = float(np.average(years, weights=temporal))
    return result


def combine_range_observations(
    observations: dict[str, dict[int, dict]], shares: dict[str, float],
    cohort_weights: dict[int, float],
) -> dict:
    """Pool independently observed range periods without inventing common years.

    Each range renormalizes the cohort weights over its own observed years. Its
    sales share then weights its entire period, so a sparse range cannot acquire
    extra influence merely by appearing in a year with no other rated ranges.
    """
    if (set(observations) != set(shares) or not shares
            or any(not np.isfinite(v) or v < 0 for v in shares.values())
            or not np.isclose(sum(shares.values()), 1, rtol=0, atol=1e-9)):
        raise ValueError("Range observations require complete normalized shares")
    if (not cohort_weights or any(not np.isfinite(v) or v < 0 for v in cohort_weights.values())
            or not np.isclose(sum(cohort_weights.values()), 1, rtol=0, atol=1e-9)):
        raise ValueError("Range observations require normalized cohort weights")
    range_weights, coverage = {}, {}
    for bucket, records in observations.items():
        if not records or set(records) - set(cohort_weights):
            raise ValueError(f"Missing or out-of-period range observations: {bucket}")
        coverage[bucket] = sum(cohort_weights[year] for year in records)
        if coverage[bucket] <= 0:
            raise ValueError(f"No positive cohort support for range: {bucket}")
        range_weights[bucket] = {year: cohort_weights[year] / coverage[bucket] for year in records}
    combined, weights = {}, {}
    for year in sorted(set().union(*(set(records) for records in observations.values()))):
        parts = [(records[year], shares[bucket] * range_weights[bucket][year])
                 for bucket, records in observations.items() if year in records]
        weights[year] = sum(weight for _, weight in parts)
        if weights[year] <= 0:
            continue
        result = aggregate_consumption_period([row for row, _ in parts], [weight for _, weight in parts])
        result.pop("efficiency")  # Annual audit stays in consumption space.
        combined[year] = {**result, "treatment": "OMEGA_independent_range_period_consumption"}
    weights = {year: weight for year, weight in weights.items() if year in combined}
    return {"observations": combined, "weights": weights, "range_weights": range_weights,
            "range_coverage": coverage, "cohort_coverage": sum(shares[bucket] * value for bucket, value in coverage.items())}


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
                    total = require_observed_value(history, year, "consumption_mj_per_vkm")
                    electric = require_observed_value(history, year, "electricity_mj_per_vkm", allow_zero=True)
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
                    total = require_observed_value(annual, year, "total")
                    electric = require_observed_value(annual, year, "electric", allow_zero=True)
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


def historical_stock_evidence(
    bundle: ConfigBundle, *, rules: dict, relationships: pd.DataFrame,
    rows: list, contexts: list[ResolvedProvenance], audit: dict,
) -> tuple[Path, pd.DataFrame, dict[tuple, tuple]]:
    """Validate capacity lineage, retaining raw age-distribution counts for weighting."""
    stock_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
    expected = resolve_artifact_path(bundle, "existing_capacity_interim", stock_rules["fuel_shares_file"])
    descriptor = audit.get("annual_stock_evidence", {})
    supplied = Path(descriptor.get("path", ""))
    supplied = supplied if supplied.is_absolute() else bundle.repo_root / supplied
    if supplied.resolve() != expected.resolve():
        raise ValueError("Annual stock handoff does not identify the configured capacity evidence")
    if audit.get("period_mapping") != bundle.scenario.periods.audit():
        raise ValueError("Annual stock handoff period mapping differs from the efficiency scenario")
    digest = file_sha256(expected)
    if descriptor.get("sha256") != digest:
        raise ValueError("Annual stock evidence changed after capacity preparation")
    stock = pd.read_csv(expected)
    if file_sha256(expected) != digest:
        raise ValueError("Annual stock evidence changed while being read")
    keys = ["region", "tech", "vintage"]
    required = {*keys, "road_class", "vintage_year", "raw_capacity", "capacity"}
    if required - set(stock):
        raise ValueError("Annual age-distribution evidence lacks required columns")
    modes = rules["historical_stock_weighted_modes"]
    if not modes or len(set(modes)) != len(modes) or any(
        mode not in rules["road_classes"] or rules["road_classes"][mode]["pathway"] not in {"ldv", "mhdv"}
        for mode in modes
    ):
        raise ValueError("Invalid stock-weighted historical road modes")
    weighted_technologies = relationships.loc[relationships["mode"].isin(modes)].set_index("tech")["mode"].to_dict()
    stock = stock.loc[stock.road_class.isin(modes)].copy()
    for column in ("raw_capacity", "capacity", "vintage", "vintage_year"):
        stock[column] = pd.to_numeric(stock[column], errors="raise")
        if not np.isfinite(stock[column]).all():
            raise ValueError(f"Annual stock evidence has non-finite {column}")
    if stock[["raw_capacity", "capacity"]].lt(0).any().any() or stock[["vintage", "vintage_year"]].mod(1).ne(0).any().any():
        raise ValueError("Annual stock evidence has negative stock or non-integral years")
    if stock.duplicated([*keys, "vintage_year"]).any():
        raise ValueError("Duplicate annual age-distribution cohort")
    expected_vintage = stock.vintage_year.map(bundle.scenario.periods.existing_vintage)
    if not expected_vintage.eq(stock.vintage).all():
        raise ValueError("Annual stock cohorts do not follow the scenario vintage mapping")
    by_id = {context.data_id: context for context in contexts}
    components, selected = {}, []
    tolerance = rules["tolerances"]["weight_sum"]
    for row in rows:
        if row.capacity <= 0 or row.tech not in weighted_technologies:
            continue
        key = (row.region, row.tech, row.vintage)
        cohort = stock.loc[stock.region.eq(row.region) & stock.tech.eq(row.tech) & stock.vintage.eq(row.vintage)]
        if cohort.empty or not cohort.road_class.eq(weighted_technologies[row.tech]).all():
            raise ValueError(f"Missing or mismatched age-distribution support: {key}")
        if not np.isclose(cohort.capacity.sum(), row.capacity, rtol=tolerance, atol=tolerance):
            raise ValueError(f"Annual surviving stock does not reconcile to prepared capacity: {key}")
        cohort = cohort.loc[cohort.raw_capacity.gt(0)].copy()
        if cohort.empty:
            raise ValueError(f"Missing positive raw age-distribution stock: {key}")
        context = by_id.get(row.data_id)
        if context is None:
            raise ValueError(f"Missing capacity provenance for historical efficiency: {key}")
        sources = tuple(sorted({
            (contributor.source_key, component.component_key)
            for contributor in context.contributors for component in contributor.components
        }))
        if not sources:
            raise ValueError(f"Empty capacity provenance for historical efficiency: {key}")
        components[key] = sources
        selected.append(cohort)
    return expected, pd.concat(selected, ignore_index=True) if selected else stock.iloc[0:0], components


def prepare_efficiency_rows(
    bundle: ConfigBundle, *, existing_capacity_rows: list | None = None,
    existing_capacity_contexts: list[ResolvedProvenance] | None = None,
    existing_capacity_audit: dict | None = None,
    range_evidence: RangeEvidence | None = None,
) -> EfficiencyPreparation:
    """Offline source-to-schema preparation; no connection or transaction is required."""
    supplied_capacity = (existing_capacity_rows is not None, existing_capacity_contexts is not None,
                         existing_capacity_audit is not None)
    if any(supplied_capacity) and not all(supplied_capacity):
        raise ValueError("Efficiency preparation requires capacity rows, contexts and audit together")
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
        "historical_aggregation": "age_distribution_stock_weighted_consumption",
        "historical_first_bin": "available_source_years_only",
        "future_load_factor": "base_year",
        "source_point_selection": "previous_observation_clamped",
        "phev_split_method": "regional_consumption_weighted_classes_mean_horizon_endpoints",
    }
    if any(rules[key] != value for key, value in supported_policies.items()):
        raise ValueError("Unsupported efficiency modeling policy; no silent fallback")
    missing_year_policies = {
        "observed_years_only": False,
        "observed_years_with_explicit_period_fallback": True,
    }
    if rules["missing_annual_rating"] not in missing_year_policies:
        raise ValueError("Unsupported annual rating observation policy")
    allow_analogue_when_empty = missing_year_policies[rules["missing_annual_rating"]]
    if (rules["intercity"]["multiplier_basis"]
            != "regen_intercity_powertrain_over_diesel_fuel_economy"
            or rules["intercity"]["hev_ratio"] != "atb_transit_hev_over_diesel"):
        raise ValueError("Unsupported intercity efficiency multiplier policy")
    if (
        rules["atb"]["phev_energy_basis"] != "greet_hhv_fuel_plus_electricity"
        or rules["atb"]["interpolation"] != "exact_source_points"
        or rules["atb"]["extrapolation"] != "error"
        or rules["offroad"]["annual_improvement"] != "efficiency_compound_growth"
        or rules["offroad"]["period_method"] != "previous_endpoint_clamped"
    ):
        raise ValueError("Unsupported energy-basis or trajectory interpolation policy")
    if not any(supplied_capacity):
        from parameterization.build_existing_capacity import prepare_existing_capacity_rows

        existing_capacity_rows, existing_capacity_contexts, existing_capacity_audit = prepare_existing_capacity_rows(bundle)
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
    historical_weights, range_evidence = existing_range_weights(bundle, evidence=range_evidence)
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
    stock_path, annual_stock, stock_components = historical_stock_evidence(
        bundle, rules=rules, relationships=relationships, rows=existing_capacity_rows,
        contexts=existing_capacity_contexts, audit=existing_capacity_audit,
    )
    paths.append(stock_path)
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
    atb_dir = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"])
    phev_source = read(
        atb_dir / atb_rules["components"]["phev_efficiency"]["output_file"], ATB
    )
    utility_factors = {}
    for powertrain, archetype in rules["ldv_archetypes"].items():
        if not powertrain.startswith("phev"):
            continue
        factors = phev_source.loc[
            phev_source.vehicle_weight_category.eq(rules["atb"]["weight_categories"]["ldv"])
            & phev_source.electric_range_mi.eq(archetype["range_mi"]), "fleet_utility_factor"
        ].drop_duplicates()
        if len(factors) != 1 or not np.isfinite(factors.iloc[0]) or not 0 <= factors.iloc[0] <= 1:
            raise ValueError(f"Missing or inconsistent ATB range utility factor: {powertrain}")
        utility_factors[powertrain] = float(factors.iloc[0])
    classified = classify_ratings(
        ratings,
        epa,
        rules=rules,
        conversions=conversions,
        phev_contract=atb_rules["components"]["phev_efficiency"][
            "future_nrcan_phev_contract"
        ],
        range_rules=range_rules,
        uf_by_powertrain=utility_factors,
    )
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    weights_dir = resolve_artifact_path(bundle, "road_aggregation")
    rating_weights = read(weights_dir / aggregation["nrcan_weights_file"])
    fleet = load_fleet_aggregation_evidence(bundle)
    paths.extend(fleet.paths)
    nlr_weights = fleet.ldv
    rating_audit = aggregate_ratings(
        classified, rating_weights, rules=rules, weight_basis=aggregation["ldv_weight_basis"],
        statistic=scenario.efficiencies.rating_statistic,
    )
    vehicle_source = read(
        atb_dir / atb_rules["components"]["vehicles"]["output_file"], ATB
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
        conversions=conversions,
        projection_years=tuple(scenario.periods.historical_years(scenario.periods.latest_observed_vintage)),
        projection_reference_year=scenario.periods.model[0],
        cohort_stock=annual_stock.merge(
            relationships.loc[relationships.tech.isin(annual_stock.tech), ["tech", "powertrain"]].drop_duplicates(),
            on="tech", validate="many_to_one"
        ),
    )
    rating_inputs, baseline_inputs = ldv_rating_input_resolvers(
        evidence, classified, rating_audit, rules=rules,
    )
    component_cache = {}

    def components_for(region, mode, powertrain, year, result):
        key = region, mode, powertrain, year
        if key not in component_cache:
            component_cache[key] = efficiency_components(
                bundle, mode, rules, road_rules, offroad_rules, region=region,
                powertrain=powertrain, year=year, result=result, manual=manual,
                rating_inputs=rating_inputs, baseline_inputs=baseline_inputs,
            )
            if year > scenario.periods.base_year and rules["road_classes"].get(mode, {}).get("pathway") in {"ldv", "mhdv"}:
                anchor = evidence.projection_anchor(region, mode, powertrain)
                for source_year, observation in anchor["observations"].items():
                    component_cache[key].extend(components_for(region, mode, anchor["powertrain"], source_year, observation))
                stock_region = rules["region_output_map"].get(region, region)
                stock_edges = relationships.loc[
                    relationships["mode"].eq(mode) & relationships.powertrain.eq(anchor["stock_powertrain"])
                    & relationships.tech.str.endswith(rules["existing_suffix"]), "tech"
                ]
                for stock_tech in stock_edges:
                    stock_key = (stock_region, stock_tech, scenario.periods.latest_observed_vintage)
                    if stock_key in stock_components:
                        component_cache[key].extend(stock_components[stock_key])
                if not any((stock_region, tech, scenario.periods.latest_observed_vintage) in stock_components for tech in stock_edges):
                    raise ValueError(f"Missing projection-anchor stock lineage: {region}/{mode}/{powertrain}")
        return component_cache[key]

    annual_records, period_records, payloads = [], [], []
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
                if edge.tech in rules["historical_single_year_technologies"]:
                    source_year = rules["historical_single_year_technologies"][
                        edge.tech
                    ]
                    if vintage != scenario.periods.existing_vintage(source_year):
                        raise ValueError(
                            f"Unexpected historical vintage for {edge.tech}: {vintage}"
                        )
                    years = [source_year]
                source_components = []
                historical = edge.tech.endswith(rules["existing_suffix"]) and edge.kind != "unit"
                weighted_stock = historical and edge.mode in rules["historical_stock_weighted_modes"]
                if weighted_stock:
                    cohort = annual_stock.loc[annual_stock.region.eq(region) & annual_stock.tech.eq(edge.tech)
                                              & annual_stock.vintage.eq(vintage)].set_index("vintage_year").raw_capacity
                    if cohort.empty or set(cohort.index) - set(years):
                        raise ValueError(f"Missing raw age-distribution support: {region}/{edge.tech}/{vintage}")
                    years = [year for year in years if year in cohort.index]
                    candidate_weights = {year: float(cohort.loc[year] / cohort.sum()) for year in years}
                    temporal_basis = "age_distribution_cohort_stock"
                else:
                    candidate_weights = {year: 1 / len(years) for year in years}
                    temporal_basis = "equal_annual_observations" if historical else "future_interval_end"
                range_mix = historical_weights.get((edge.mode, edge.powertrain), {}) if edge.tech.endswith(rules["existing_suffix"]) else {}
                first_source_year = int(provincial.year.min() if edge.kind == "road" else national.year.min())
                supported_years = [year for year in years if not historical or year >= first_source_year]
                range_observations = {}
                range_combination = None
                if not supported_years:
                    observations = {}
                elif edge.kind == "unit":
                    observations = {year: {"native_intensity": 1.0, "load_factor": 1.0, "native_unit": "PJ/PJ",
                                           "treatment": "intentional_unit_efficiency"} for year in supported_years}
                elif edge.kind == "road":
                    if range_mix:
                        range_observations = {
                            bucket: evidence.period_observations(
                                source_region, edge.mode, bucket, supported_years,
                                allow_analogue_when_empty=allow_analogue_when_empty,
                            ) for bucket in range_mix
                        }
                        range_combination = combine_range_observations(range_observations, range_mix, candidate_weights)
                        observations = range_combination["observations"]
                    else:
                        observations = evidence.period_observations(
                            source_region, edge.mode, edge.powertrain, supported_years,
                            allow_analogue_when_empty=allow_analogue_when_empty,
                        )
                else:
                    observations = {}
                    for year in supported_years:
                        key = (edge.mode, edge.powertrain, year)
                        if key not in offroad_values:
                            offroad_values[key] = derive_offroad_consumption(
                                national, manual, mode=edge.mode, powertrain=edge.powertrain,
                                year=year, base_year=scenario.periods.base_year,
                                selectors=offroad_rules["energy_intensity_series"], rules=rules["offroad"],
                            )
                        observations[year] = offroad_values[key]
                if not observations:
                    raise ValueError(f"No supported annual consumption observations: {region}/{edge.tech}/{vintage}")
                included_years = sorted(observations)
                coverage = (range_combination["cohort_coverage"] if range_combination is not None
                            else sum(candidate_weights[year] for year in included_years))
                temporal_weights = (range_combination["weights"] if range_combination is not None
                                    else {year: candidate_weights[year] / coverage for year in included_years})
                for year in years:
                    result = observations.get(year)
                    omission = "" if result is not None else (
                        "before_source_coverage" if year not in supported_years else "missing_own_or_anchor_observation"
                    )
                    temporal = {"temporal_weight": temporal_weights.get(year, 0.0), "temporal_weight_basis": temporal_basis,
                                "cohort_stock_k_vehicles": float(cohort.loc[year]) if weighted_stock else None,
                                "cohort_weight": candidate_weights[year], "source_available": result is not None,
                                "included_in_period": result is not None, "omission_reason": omission}
                    common = {**edge._asdict(), "region": region, "ceud_region": source_region,
                              "vintage": vintage, "source_year": year, "trajectory": trajectory, **temporal}
                    for bucket, records in range_observations.items():
                        component = records.get(year)
                        annual_records.append({**common, "powertrain": bucket, "range_weight": range_mix[bucket],
                                               "range_representative_component": True,
                                               "temporal_weight": range_combination["range_weights"][bucket].get(year, 0.0),
                                               "source_available": component is not None, "included_in_period": component is not None,
                                               "omission_reason": "" if component is not None else "missing_own_or_proxy_observation",
                                               **(component or {})})
                        if component is not None:
                            source_components.extend(components_for(source_region, edge.mode, bucket, year, component))
                    annual_records.append({**common, **(result or {})})
                    if result is None:
                        evidence.findings.add((source_region, edge.mode, edge.powertrain, year,
                            f"omitted_annual_consumption={omission};tech={edge.tech};vintage={vintage};cohort_weight={candidate_weights[year]:.17g}"))
                    elif not range_mix and edge.kind != "unit":
                        source_components.extend(components_for(source_region, edge.mode, edge.powertrain, year, result))
                period_result = aggregate_consumption_period(
                    [{**observations[year], "source_year": year} for year in included_years],
                    [temporal_weights[year] for year in included_years],
                )
                period_records.append({
                    **edge._asdict(), "region": region, "ceud_region": source_region, "vintage": vintage,
                    "trajectory": trajectory, **period_result, "temporal_weight_basis": temporal_basis,
                    "source_year_start": min(included_years), "source_year_end": max(included_years),
                    "candidate_year_count": len(years), "included_year_count": len(included_years),
                    "candidate_source_years": "|".join(map(str, years)), "included_source_years": "|".join(map(str, included_years)),
                    "candidate_cohort_stock_k_vehicles": float(cohort.sum()) if weighted_stock else None,
                    "included_cohort_stock_k_vehicles": float(cohort.sum() * coverage) if weighted_stock else None,
                    "cohort_stock_coverage": coverage if weighted_stock else None,
                    "observation_coverage": coverage,
                    "range_source_years": json.dumps({bucket: sorted(records) for bucket, records in range_observations.items()}, sort_keys=True),
                    "range_stock_coverage": json.dumps(range_combination["range_coverage"] if range_combination is not None else {}, sort_keys=True),
                })
                if weighted_stock:
                    source_components.extend(stock_components[region, edge.tech, vintage])
                if range_mix:
                    source_components.append((OMEGA, OMEGA_COMPONENT))
                components = list(dict.fromkeys(
                    (source, str(component)) for source, component in source_components
                ))
                component_signature = (
                    (components[0], *sorted(components[1:])) if components else ()
                )
                payloads.append(
                    {
                        "region": region,
                        "tech": edge.tech,
                        "input_comm": edge.input_comm,
                        "output_comm": edge.output_comm,
                        "vintage": vintage,
                        "efficiency": period_result["efficiency"],
                        "units": edge.units,
                        "mode": edge.mode,
                        "kind": edge.kind,
                        "range_aggregated": bool(range_mix),
                        "components": component_signature,
                    }
                )
    endpoints = [scenario.periods.projection_year(p, legacy_at_end=True) for p in scenario.periods.model]
    split_audit = phev_split_evidence(atb, endpoints=endpoints, rules=rules, fleet=fleet)
    split_audit = pd.concat([split_audit, existing_phev_split_evidence(
        atb_audit, endpoints=endpoints, rules=rules, range_weights=historical_weights,
        regions=scenario.geography.regions,
    )], ignore_index=True)
    if file_sha256(stock_path) != existing_capacity_audit["annual_stock_evidence"]["sha256"]:
        raise ValueError("Annual stock evidence changed during efficiency preparation")
    input_evidence = [
        {"path": str(path.relative_to(bundle.repo_root) if path.is_relative_to(bundle.repo_root) else path),
         "sha256": file_sha256(path)}
        for path in sorted(set(paths))
    ]
    digest = hashlib.sha256(
        json.dumps(
            {
                "files": input_evidence,
                "rules": rules,
                "road_outputs": road_rules,
                "offroad_outputs": offroad_rules,
                "fleet_rules": fleet.rules,
                "rating_source_rules": rating_rules,
                "aggregation_sources": bundle.scenario.aggregation_sources.model_dump(),
                "conversions": conversions,
                "trajectory": trajectory,
                "rating_statistic": scenario.efficiencies.rating_statistic,
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
        "rating_statistic": scenario.efficiencies.rating_statistic,
        "historical_aggregation": rules["historical_aggregation"],
    }
    assumption = DataSet(
        data_id="transport-efficiency-structure:" + digest[:16],
        label="Transport unit-efficiency structural relationships",
        version=rules["transformation_version"],
        description="Backend-owned template edges and intentional unit efficiencies; not an external source.",
    )
    contexts, rows = [], []
    for (mode, region, range_aggregated, components), group in pd.DataFrame(payloads).groupby(
        ["mode", "region", "range_aggregated", "components"], sort=True,
    ):
        kind = group.kind.iloc[0]
        records = group.drop(columns=["mode", "kind", "range_aggregated", "components"]).to_dict("records")
        if kind == "unit":
            rows.extend(
                Efficiency.model_validate({**record, "data_id": assumption.data_id})
                for record in records
            )
            continue
        context = efficiency_context(bundle, components, mode, {**variant, "region": region, "historical_range_aggregation": bool(range_aggregated)}, rules)
        contexts.append(context)
        rows.extend(validate_parameter_rows(Efficiency, records, context, bundle=bundle))
    splits = []
    for blend, spec in phev_blend_relationships(rules).items():
        for region in scenario.geography.regions:
            selected = split_audit.loc[split_audit.blend.eq(blend) & split_audit.region.eq(region)]
            electricity = float(selected.groupby("year").electricity_share.first().mean())
            components = [(ATB, "phev_vehicle_inputs"),
                          (ATB, "phev_utility_factor_ldv" if spec["aggregation_role"] == "ldv" else "phev_utility_factor_mdhd"),
                          ("argonne_rd_greet", "fuel_heating_values"),
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
                        }
                    )
            splits.extend(validate_parameter_rows(LimitTechInputSplit, regional_records, split_context, bundle=bundle))
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
    audit["input_evidence"] = input_evidence
    audit["periods"] = scenario.periods.model_dump(mode="json")
    audit["excluded_technologies"] = sorted(
        set(technology.tech) - set(relationships.tech)
    )
    audit["hybrid_evidence_counts"] = (
        classified.classification_method.value_counts().to_dict()
    )
    audit["rating_index_fallbacks"] = sum(not finding[-1].startswith("omitted_annual_consumption=")
                                           for finding in evidence.findings)
    audit["rating_aggregation"] = "distinct_model_class_weighted"
    audit["annual_stock_evidence"] = dict(existing_capacity_audit["annual_stock_evidence"])
    audit["pev_projection_anchors"] = dict(evidence.pev_projection_anchors)
    audit["ldv_projection_reference_year"] = evidence.projection_reference_year
    audit["rating_outlier_exclusions"] = int(classified.outlier_excluded.sum())
    audit["historical_source_backed_rows_without_capacity"] = len(
        existing_keys - capacity_keys
    )
    period_frame = pd.DataFrame(period_records)
    incomplete = period_frame.loc[
        period_frame.included_year_count.lt(period_frame.candidate_year_count)
        | period_frame.observation_coverage.lt(1 - rules["tolerances"]["weight_sum"])
    ]
    audit["omitted_annual_observations"] = int((period_frame.candidate_year_count - period_frame.included_year_count).sum())
    audit["periods_with_incomplete_observation_coverage"] = len(incomplete)
    audit["incomplete_observation_coverage"] = incomplete[[
        "region", "tech", "input_comm", "output_comm", "vintage", "candidate_source_years", "included_source_years",
        "candidate_cohort_stock_k_vehicles", "included_cohort_stock_k_vehicles", "cohort_stock_coverage", "observation_coverage",
        "range_source_years", "range_stock_coverage",
    ]].replace({np.nan: None}).to_dict("records")
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
        (period_frame, interim / rules["files"]["periods"]),
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
    LOGGER.info("Prepared efficiency=%s and limit_tech_input_split=%s rows (%s technologies)",
                len(rows), len(splits), audit["technologies"])
    LOGGER.info(
        "Efficiency data handling: rating fallbacks=%s, omitted rows=%s, ambiguous EPA rows=%s, renormalized cells=%s",
        audit["rating_index_fallbacks"], audit["omitted_rating_rows"],
        audit["ambiguous_epa_rows"], audit["renormalized_rating_cells"],
    )
    LOGGER.info("Efficiency observation coverage: omitted annual observations=%s across %s periods",
                audit["omitted_annual_observations"], audit["periods_with_incomplete_observation_coverage"])
    LOGGER.debug("Efficiency audit: %s", audit)
    return EfficiencyPreparation(rows, splits, assumption, contexts, audit)


def atb_efficiency_components(powertrain: str, pathway: str, rules: dict) -> list[tuple]:
    """Identify the ATB table and energy basis consumed by one archetype."""
    if powertrain.startswith("phev"):
        components = [(ATB, "phev_vehicle_inputs"), (
            ATB, "phev_utility_factor_ldv" if pathway == "ldv" else "phev_utility_factor_mdhd",
        )]
    else:
        components = [(ATB, "vehicles")]
    if powertrain.startswith("phev") or powertrain in rules["atb"]["fuel_equivalent_hhv_powertrains"]:
        components.append(("argonne_rd_greet", "fuel_heating_values"))
    return components


def ldv_rating_input_resolvers(evidence, classified, rating_audit, *, rules: dict):
    """Trace exact observed rating years and approved baseline inputs."""
    @cache
    def rating_inputs(mode, powertrain, year, column):
        ceud_class = rules["road_classes"][mode]["weights"]
        annual = rating_audit.loc[
            rating_audit.ceud_class.eq(ceud_class)
            & rating_audit.powertrain.eq(powertrain)
            & rating_audit.year.le(evidence.base_year)
        ]
        keys = annual.loc[annual.year.eq(year), ["year", "rating_class"]]
        if keys.empty:
            raise ValueError(f"Missing exact annual rating provenance: {mode}/{powertrain}/{year}")
        selected = classified.loc[classified.included & classified.powertrain.eq(powertrain)].merge(
            keys, on=["year", "rating_class"], validate="many_to_one",
        )
        components = {
            ("nrcan_fuel_consumption_ratings", str(component))
            for component in selected.component.unique()
        }
        if powertrain in {"gasoline", "hev"}:
            classifications = classified.loc[
                classified.included & classified.classification_method.eq("exact_epa_hybrid")
            ].merge(keys, on=["year", "rating_class"], validate="many_to_one")
            if not classifications.empty:
                components.add((rules["ratings"]["evidence_source"], "vehicles"))
        if column == "mj" and selected.native_unit.eq("L/100 km").any():
            components.add(("argonne_rd_greet", "fuel_heating_values"))
        if column == "mj" and powertrain.startswith("phev"):
            components.add(("argonne_rd_greet", "fuel_heating_values"))
            components.add((ATB, "phev_utility_factor_ldv"))
        return tuple(sorted(components))

    @cache
    def baseline_inputs(mode, powertrain, seen=()):
        if powertrain in seen:
            raise ValueError(f"Cyclic/missing LDV rating lineage: {mode}/{powertrain}")
        base = evidence.base_year
        if evidence.rating(mode, powertrain, base, "mj") is not None:
            return rating_inputs(mode, powertrain, base, "mj")
        observed = evidence.ratings.loc[
            evidence.ratings.ceud_class.eq(rules["road_classes"][mode]["weights"])
            & evidence.ratings.powertrain.eq(powertrain) & evidence.ratings.mj.notna()
        ]
        if not observed.empty:
            latest = int(observed.year.max())
            _, analogue = evidence.historical_rating_ratio(mode, powertrain, base, latest)
            return tuple(sorted(set(
                rating_inputs(mode, powertrain, latest, "mj")
                + rating_inputs(mode, analogue, base, "native")
                + rating_inputs(mode, analogue, latest, "native")
            )))
        analogue = rules["ldv_archetypes"][powertrain]["analogue"]
        if analogue in (*seen, powertrain):
            analogue = rules["ldv_incumbent_fallback"]
        return tuple(sorted(set(
            baseline_inputs(mode, analogue, (*seen, powertrain))
            + tuple(atb_efficiency_components(powertrain, "ldv", rules))
            + tuple(atb_efficiency_components(analogue, "ldv", rules))
        )))

    return rating_inputs, baseline_inputs


def efficiency_components(
    bundle: ConfigBundle, mode: str, rules: dict, road: dict, offroad: dict, *,
    region: str, powertrain: str, year: int, result: dict, manual: pd.DataFrame,
    rating_inputs, baseline_inputs,
) -> list[tuple]:
    """Keep lineage specific to the annual pathway that produced the parameter."""
    if mode not in rules["road_classes"]:
        components = [(
            "nrcan_ceud_transport_national",
            offroad["energy_intensity_series"][mode]["national_table"],
        )]
        indices = [int(index) for index in result["manual_rows"].split("|") if index]
        components.extend(manual.loc[
            indices, ["registered_source_id", "registered_component_id"]
        ].drop_duplicates().itertuples(index=False, name=None))
        return components
    spec = rules["road_classes"][mode]
    pathway = spec["pathway"]
    base = bundle.scenario.periods.base_year
    if pathway == "motorcycle":
        components = [(CEUD, spec["intensity_table"])]
        if year > base:
            components.append(("jgcri_gcam_motorcycle_inputs", "canada_motorcycle_inputs"))
        return components
    components = [(CEUD, road["activity_series"][mode]["table_id"]), (CEUD, spec["stock_table"])]
    if "intensity_table" in spec and year <= base:
        components.append((CEUD, spec["intensity_table"]))
    if pathway == "ldv":
        components.append(aggregation_component(bundle, "ldv", region))
        treatment = result["treatment"]
        if treatment == "NRCan_observed_class_weighted":
            components.extend(rating_inputs(mode, powertrain, year, "mj"))
        elif treatment == "NRCan_cohort_anchor_ATB_relative_projection":
            components.extend(atb_efficiency_components(powertrain, pathway, rules))
            anchor = rules["pev_projection_anchors"].get(powertrain.split("_", 1)[0], powertrain)
            if anchor != powertrain:
                components.extend(atb_efficiency_components(anchor, pathway, rules))
            # The caller adds each observed anchor year and its raw-stock lineage.
        else:
            components.extend(baseline_inputs(mode, powertrain))
            if year > base:
                components.extend(atb_efficiency_components(powertrain, pathway, rules))
            else:
                components.extend(rating_inputs(mode, result["analogue"], year, "native"))
                components.extend(rating_inputs(mode, result["analogue"], base, "native"))
    elif pathway == "mhdv":
        spec = rules["mhdv_fleet_baseline"]
        observed = spec["observed_fuels"][mode]
        components.append(("argonne_rd_greet", "fuel_heating_values"))
        if year > base or powertrain not in observed:
            index_powertrain = spec["projection_index_powertrains"].get(powertrain, powertrain)
            components.extend(atb_efficiency_components(index_powertrain, pathway, rules))
            if powertrain not in observed:
                components.extend(atb_efficiency_components(spec["reference_fuel"], pathway, rules))
            components.append(aggregation_component(
                bundle, "medium_trucks" if mode == "medium_trucks" else "heavy_truck_haul", region))
    else:
        atb_powertrain = "diesel" if pathway == "intercity" and powertrain != "hev" else powertrain
        components.extend(atb_efficiency_components(atb_powertrain, pathway, rules))
        if mode == "medium_trucks":
            components.append(aggregation_component(bundle, "medium_trucks", region))
        if mode == "heavy_trucks":
            components.append(aggregation_component(bundle, "heavy_truck_haul", region))
        if pathway == "intercity" and powertrain != "hev":
            components.append(("epri_us_regen_transportation", "intercity_bus_charts"))
    return components


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
    governing_component = next(
        item for item in inputs
        if (item.source_key, item.component_key) == (components[0][0], str(components[0][1]))
    )
    governing = governing_component.governing_source_id
    return resolve_composite_provenance(
        inputs=inputs,
        dataset_key="efficiency." + mode,
        transformation="Configured transport service efficiency and energy-input split",
        transformation_version=rules["transformation_version"],
        governing_source_id=governing,
        data_quality=governing_component.data_quality,
        value_variant=variant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/default.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    prepare_efficiency_rows(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
