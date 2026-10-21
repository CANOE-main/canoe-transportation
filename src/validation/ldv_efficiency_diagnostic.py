"""Recorded road-efficiency sources, aggregation distributions and bridge review.

Loading reads a successful build's evidence snapshot, without ETL or downloads.
Detailed-class recalculations and interpolation between prepared periods are diagnostic.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator

from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.build_efficiencies import aggregate_consumption_period
from parameterization.road_aggregation import nrcan_to_nlr_map
from parameterization.road_efficiencies import RoadEfficiencyEvidence, aggregate_ratings, ceud_series, observed_value, phev_historical_index_mode, positive, rating_class_statistics, select_atb_consumption
from utils import (ConfigBundle, file_sha256, load_conversion_factors, load_harmonization_rules,
                   resolve_artifact_path, resolve_input_path, write_dataframe_atomic, write_text_atomic)


# Presentation groups use existing production mode keys.
VIEW_MODES = {"Cars": "cars", "Light trucks": "passenger_light_trucks",
              "Medium trucks": "medium_trucks", "Heavy trucks": "heavy_trucks"}
COHORT = "Production class aggregate"


@dataclass(frozen=True)
class LDVEfficiencyDiagnostic:
    bundle: ConfigBundle
    frames: dict[str, pd.DataFrame]
    summary: dict[str, Any]
    inputs: dict[str, pd.DataFrame]


def collect_efficiency_inputs(bundle: ConfigBundle) -> dict[str, Path]:
    """Identify the actual data captured by an explicit diagnostic preset build."""
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    paths = {name: resolve_artifact_path(bundle, "efficiencies_interim", efficiency["files"][name])
             for name in ("ratings", "rating_weights", "atb", "annual", "periods", "loads", "relationships")}
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    paths["rating_class_weights"] = resolve_artifact_path(bundle, "road_aggregation", aggregation["nrcan_weights_file"])
    stock = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
    paths["annual_cohort_stock"] = resolve_artifact_path(bundle, "existing_capacity_interim", stock["fuel_shares_file"])
    paths.update({
        "final_efficiency": resolve_artifact_path(bundle, "efficiencies_processed", efficiency["files"]["efficiency"]),
        "final_splits": resolve_artifact_path(bundle, "efficiencies_processed", efficiency["files"]["splits"]),
        "integrity": resolve_artifact_path(bundle, "efficiencies_validation", efficiency["files"]["integrity"]),
    })
    ratings = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")
    paths.update({f"raw_ratings_{component}": resolve_input_path(bundle, "interim", ratings["interim_subdir"], filename)
                  for component, filename in ratings["outputs"].items()})
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    for name, component in (("atb_vehicles", "vehicles"), ("atb_phev", "phev_efficiency")):
        paths[name] = resolve_input_path(bundle, "interim", atb["interim_subdir"], atb["components"][component]["output_file"])
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    ceud = load_harmonization_rules(bundle, "nrcan_ceud")
    paths["ceud"] = resolve_input_path(bundle, "interim", ceud["interim_subdir"], ceud["region_output_template"].format(region=region.lower()))
    ranges = load_harmonization_rules(bundle, "ldv_ev_ranges")
    paths.update({f"omega_{name}": resolve_artifact_path(bundle, "ldv_range_validation", ranges["files"][name]) for name in ("records", "buckets")})
    benchmark = load_harmonization_rules(bundle, "rating_stock_benchmark")
    paths.update({f"benchmark_{name}": resolve_artifact_path(bundle, "rating_stock_benchmark", benchmark["files"][name]) for name in ("matches", "allocations", "weighted")})
    return paths


def _family(powertrain: str) -> str:
    return "bev" if powertrain.startswith("bev") else "phev" if powertrain.startswith("phev") else powertrain


def rating_distributions(bundle: ConfigBundle, classified: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Expose catalogue rows, their equal contribution and unequal model counts."""
    required = {"included", "year", "rating_class", "powertrain", "Make", "Model", "native_consumption", "native_unit", "consumption_mj_per_vkm"}
    if required - set(classified):
        raise ValueError(f"Efficiency diagnostic ratings missing columns: {sorted(required - set(classified))}")
    frame = classified.loc[classified.included].copy()
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    frame["atb_class"] = frame.rating_class.map(nrcan_to_nlr_map(mapping)).fillna("Unresolved source class")
    frame["vehicle_group"] = frame.atb_class.map(mapping["target_to_ceud"]).map({"Car": "Cars", "Light Truck": "Light trucks"}).fillna("Light trucks")
    frame["make_model"] = frame.Make.fillna("") + " / " + frame.Model.fillna("")
    frame["powertrain_family"] = frame.powertrain.map(_family)
    frame["calibration_observation"] = frame.year.le(bundle.scenario.periods.base_year)
    keys = ["year", "rating_class", "powertrain", "native_unit"]
    frame["catalogue_rows_in_cohort"] = frame.groupby(keys).native_consumption.transform("size")
    frame["equal_row_contribution"] = 1 / frame.catalogue_rows_in_cohort
    frame["model_rows_in_cohort"] = frame.groupby([*keys, "make_model"]).native_consumption.transform("size")
    frame["make_model_contribution"] = frame.model_rows_in_cohort / frame.catalogue_rows_in_cohort
    summary = frame.groupby(keys, as_index=False).agg(
        catalogue_rows=("native_consumption", "size"), distinct_models=("make_model", "nunique"),
        equal_row_mean=("native_consumption", "mean"), median=("native_consumption", "median"),
        minimum=("native_consumption", "min"), maximum=("native_consumption", "max"),
        consumption_mj_per_vkm=("consumption_mj_per_vkm", "mean"),
        atb_class=("atb_class", "first"), vehicle_group=("vehicle_group", "first"),
    )
    rules = load_harmonization_rules(bundle, "efficiencies")
    for statistic in ("mean", "median"):
        values = rating_class_statistics(classified, rules=rules, statistic=statistic)
        values = values[keys + ["native_consumption"]].rename(
            columns={"native_consumption": "equal_distinct_model_" + statistic})
        summary = summary.merge(values, on=keys, validate="one_to_one")
    summary["powertrain_family"] = summary.powertrain.map(_family)
    summary["calibration_observation"] = summary.year.le(bundle.scenario.periods.base_year)
    return frame, summary


def _class_rating_audit(bundle: ConfigBundle, audit: pd.DataFrame, atb_class: str) -> pd.DataFrame:
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    selected = audit.loc[audit.rating_class.map(nrcan_to_nlr_map(mapping)).eq(atb_class)].copy()
    selected["weight"] /= selected.groupby(["year", "powertrain"]).weight.transform("sum")
    selected["ceud_class"] = atb_class
    return selected


def representative_consumption(values: dict[str, float], shares: dict[str, float]) -> float:
    """The OMEGA archetype is an arithmetic mean of energy consumption."""
    if not shares or any(not np.isfinite(s) or s < 0 for s in shares.values()) or abs(sum(shares.values()) - 1) > 1e-9 or set(shares) - set(values):
        raise ValueError("Incomplete or unnormalised OMEGA representative consumption")
    return sum(positive(values[bucket], "range consumption") * share for bucket, share in shares.items())


def _range_shares(inputs: dict[str, pd.DataFrame], mode: str, family: str) -> dict[str, float]:
    market = "car" if mode == "cars" else "light_truck"
    selected = inputs["omega_buckets"].loc[inputs["omega_buckets"].market_class.eq(market) & inputs["omega_buckets"].powertrain.str.lower().eq(family)]
    if selected.empty or selected.bucket.duplicated().any():
        raise ValueError(f"Missing or ambiguous OMEGA range evidence: {market}/{family}")
    return selected.set_index("bucket").share_of_all_sales.to_dict()


def _evidence(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], mode: str, atb_class: str | None) -> RoadEfficiencyEvidence:
    rules = deepcopy(load_harmonization_rules(bundle, "efficiencies"))
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    atb = inputs["atb"].loc[inputs["atb"].region.eq(region) & inputs["atb"]["mode"].eq(mode)].copy()
    ratings = inputs["rating_weights"]
    if atb_class is not None:
        atb = atb.loc[atb.vehicle_class.eq(atb_class)].copy()
        atb["weighted_consumption"] = atb.consumption_mj_per_vkm
        if rules["road_classes"][mode]["pathway"] == "ldv":
            ratings = _class_rating_audit(bundle, ratings, atb_class)
            rules["road_classes"][mode]["weights"] = atb_class
    return RoadEfficiencyEvidence(ceud=inputs["ceud"], loads=inputs["loads"], rating_audit=ratings,
        atb_audit=atb, gcam=pd.DataFrame(), regen=pd.DataFrame(), base_year=bundle.scenario.periods.base_year, rules=rules)


def _rating_series(audit: pd.DataFrame) -> pd.DataFrame:
    data = audit.copy()
    data["mj"] = data.weight * data.class_mean_mj_per_vkm
    return data.groupby(["ceud_class", "powertrain", "year"], as_index=False).agg(value=("mj", lambda s: s.sum(min_count=1)))


def phev_rating_components(bundle: ConfigBundle, classified: pd.DataFrame, audit: pd.DataFrame) -> pd.DataFrame:
    """Keep CS and CD operating-mode rates separate while applying recorded class weights."""
    weights = audit.loc[audit.powertrain.str.startswith("phev")].copy()
    columns = ["ceud_class", "powertrain", "year", "component", "native_value", "value",
               "native_unit", "energy_basis", "native_basis", "utility_weighted"]
    if weights.empty:
        return pd.DataFrame(columns=columns)
    contract = load_harmonization_rules(bundle, "nlr_atb_autonomie")["components"]["phev_efficiency"]["future_nrcan_phev_contract"]
    cs_field, cd_field = contract["combined_cs_output_numeric_field"], contract["combined_cd_output_numeric_field"]
    rows = classified.loc[classified.included & classified.powertrain.str.startswith("phev")].copy()
    for field in (cs_field, cd_field):
        if field not in rows or rows[field].isna().any() or not np.isfinite(rows[field]).all() or rows[field].lt(0).any():
            raise ValueError("PHEV component diagnostic requires finite non-negative explicit CS/CD ratings")
    keys = ["year", "rating_class", "powertrain"]
    component_means = rating_class_statistics(
        rows, rules=load_harmonization_rules(bundle, "efficiencies"),
        statistic=bundle.scenario.efficiencies.rating_statistic,
        value_columns=(cs_field, cd_field),
    )[keys + [cs_field, cd_field]]
    weights = weights.merge(component_means, on=keys, how="left", validate="many_to_one")
    if weights[[cs_field, cd_field]].isna().any().any():
        raise ValueError("PHEV component diagnostic is missing a recorded rating class")
    conversions = load_conversion_factors(bundle)
    per_km = conversions["service"]["per_100_km_to_per_km"]
    hhv_fuel = load_harmonization_rules(bundle, "efficiencies")["atb"]["equivalent_gallon_hhv_fuel"]["gge"]
    parts = []
    for component, field, unit, factor, energy_basis, native_basis in (
        ("CS", cs_field, "L/100 km (CS)", conversions["fuel_hhv"][hhv_fuel + "_mj_per_litre"] * per_km,
         "GREET fuel HHV", "Fuel consumed per distance in charge-sustaining operation; no utility weighting"),
        ("CD", cd_field, "Le/100 km (CD)", conversions["energy"]["nrcan_kwh_per_litre_gasoline_equivalent"] * conversions["energy"]["kwh_to_mj"] * per_km,
         "NRCan gasoline-equivalent energy", "Combined equivalent energy per distance in charge-depleting operation; may include liquid fuel; no utility weighting"),
    ):
        values = weights.assign(weighted=weights.weight * weights[field])
        grouped = values.groupby(["ceud_class", "powertrain", "year"], as_index=False).agg(native_value=("weighted", "sum"))
        grouped["value"] = grouped.native_value * factor
        grouped["component"], grouped["native_unit"] = component, unit
        grouped["energy_basis"], grouped["native_basis"] = energy_basis, native_basis
        grouped["utility_weighted"] = False
        parts.append(grouped)
    return pd.concat(parts, ignore_index=True)[columns]


def harmonised_ceud_history(bundle: ConfigBundle, ceud: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Convert parent-fleet fuel evidence; retain its role as an index context."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    road = rules["road_classes"][mode]
    factors = load_conversion_factors(bundle)["fuel_hhv"]
    parts = []
    for powertrain, fuel in rules["historical_index_fuels"][mode].items():
        series = ceud_series(ceud, road["stock_table"], road["cs_prefix"] + "|" + rules["fuel_series"][fuel], "L/100 km")
        series["consumption_index"] = series.value / positive(observed_value(series, bundle.scenario.periods.base_year, "value"), "CEUD base-year consumption")
        series["value"] *= factors[fuel + "_mj_per_litre"] / 100
        series["powertrain"], series["source_fuel"] = powertrain, fuel
        parts.append(series)
    return pd.concat(parts, ignore_index=True)


def _cohort_counts(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], mode: str,
                   powertrain: str, vintage: int) -> pd.Series:
    """Use the recorded age-distribution counts before lifetime redistribution."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    region = rules["region_output_map"].get(region, region)
    family = _family(powertrain)
    relationships = inputs["relationships"]
    candidates = relationships.loc[relationships["mode"].eq(mode)
        & relationships.powertrain.eq(family) & relationships.tech.str.endswith(rules["existing_suffix"]), "tech"]
    prepared = inputs["final_efficiency"]
    retained = prepared.loc[prepared.region.eq(region) & prepared.vintage.eq(vintage)
                            & prepared.tech.isin(candidates), "tech"]
    stock = inputs["annual_cohort_stock"]
    selected = stock.loc[stock.region.eq(region) & stock.tech.isin(retained)
                         & stock.vintage.eq(vintage) & stock.raw_capacity.gt(0)]
    return selected.groupby("vintage_year").raw_capacity.sum().sort_index()


def _observation_summary(observations: list[dict]) -> dict[str, str]:
    """Keep explicit analogue use visible after annual observations are pooled."""
    treatments = sorted({str(r["treatment"]) for r in observations if pd.notna(r.get("treatment"))})
    analogues = sorted({str(r["analogue"]) for r in observations if pd.notna(r.get("analogue"))})
    proxy = any(r.get("treatment") == "NRCan_missing_year_approved_analogue_index"
                or (r.get("treatment") == "ATB_PEV_level_NRCan_shared_anchor_history"
                    and r.get("analogue") != r.get("historical_index_anchor")) for r in observations)
    return {"observation_basis": "Configured analogue for empty observed period" if proxy else "Available source observations / projection",
            "source_treatments": " | ".join(treatments), "analogue": " | ".join(analogues)}


def _bridge_rows(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], *, recorded_statistic: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    spec = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    rules = load_harmonization_rules(bundle, "efficiencies")
    start, end = map(int, spec["bridge_years"])
    base, region = bundle.scenario.periods.base_year, spec["region"]
    output, anchors = [], []
    periods = bundle.scenario.periods
    selected_statistic = bundle.scenario.efficiencies.rating_statistic
    recorded_selection = recorded_statistic is None or recorded_statistic == selected_statistic
    allow_fallback = rules["missing_annual_rating"] == "observed_years_with_explicit_period_fallback"
    first_source_year = int(inputs["ceud"].year.min())
    cohort_counts = {}

    def add(context, stage, series, year, metric, value, units, **extra):
        if pd.notna(value):
            output.append({**context, "stage": stage, "series": series, "year": float(year), "metric": metric, "value": float(value), "units": units, **extra})

    def marker(context, powertrain, consumption, load, year, service_units, **extra):
        for metric, value, units in (("consumption", consumption, "MJ/vkm"),
                                     ("service_efficiency", load / consumption, service_units)):
            add(context | {"powertrain": powertrain}, "Indexed bridge", "Production method", year,
                metric, value, units, load_factor=load, **extra)

    for group, mode in VIEW_MODES.items():
        native = inputs["atb"].loc[inputs["atb"].region.eq(region) & inputs["atb"]["mode"].eq(mode)]
        for atb_class in [None, *sorted(native.vehicle_class.unique())]:
            evidence = _evidence(bundle, inputs, mode, atb_class)
            context = {"vehicle_group": group, "mode": mode, "atb_class": atb_class or COHORT,
                       "resolution": "Actual production aggregation" if atb_class is None else "Diagnostic class recalculation"}
            atb_audit = native if atb_class is None else native.loc[native.vehicle_class.eq(atb_class)]
            history = inputs["rating_weights"] if atb_class is None else _class_rating_audit(bundle, inputs["rating_weights"], atb_class)
            if rules["road_classes"][mode]["pathway"] == "ldv":
                weight_class = rules["road_classes"][mode]["weights"] if atb_class is None else atb_class
                selected_history = history.loc[history.ceud_class.eq(weight_class)]
                for item in _rating_series(selected_history).itertuples(index=False):
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing", "NRCan size-class aggregate", item.year,
                        "consumption", item.value, "MJ/vkm", calibration_observation=item.year <= base,
                        treatment=f"Distinct-model {bundle.scenario.efficiencies.rating_statistic} with recorded MTO class weights")
                components = phev_rating_components(bundle, inputs["ratings"], selected_history)
                for item in components.itertuples(index=False):
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing",
                        "NRCan CS fuel-only aggregate" if item.component == "CS" else "NRCan CD equivalent-energy aggregate",
                        item.year, "consumption", item.value, "MJ/vkm", calibration_observation=item.year <= base,
                        component=item.component, utility_weighted=False, energy_basis=item.energy_basis,
                        native_value=item.native_value, native_unit=item.native_unit, native_basis=item.native_basis,
                        treatment="Operating-mode component rate; not annual utility-weighted PHEV total energy")
            for (powertrain, year), values in atb_audit.groupby(["powertrain", "year"]):
                consumption = values.weighted_consumption.sum() if atb_class is None else values.consumption_mj_per_vkm.iloc[0]
                add(context | {"powertrain": powertrain}, "Harmonised before indexing", "ATB absolute total energy", year,
                    "consumption", consumption, "MJ/vkm", calibration_observation=False,
                    treatment="PHEV utility-weighted fuel HHV plus electricity" if powertrain.startswith("phev") else "Source energy converted with production contract")
            if rules["road_classes"][mode]["pathway"] == "mhdv":
                for item in harmonised_ceud_history(bundle, inputs["ceud"], mode).itertuples(index=False):
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing",
                        f"CEUD {item.source_fuel} coarse fleet (index context)", item.year,
                        "consumption", item.value, "MJ/vkm", source_class="CEUD coarse vehicle group",
                        source_fuel=item.source_fuel, consumption_index=item.consumption_index,
                        calibration_observation=item.year <= base,
                        treatment="Parent-fleet energy is normalised at base year; not an observed ATB-class/powertrain consumption level")
            powertrains = sorted(atb_audit.powertrain.unique())
            historical_powertrains = set(rules["historical_index_fuels"].get(mode, powertrains))
            ldv = rules["road_classes"][mode]["pathway"] == "ldv"
            historical_bins = {v: periods.historical_years(v) for v in periods.existing}
            historical_bins = {v: years for v, years in historical_bins.items() if years and max(years) >= start - periods.step}
            first_year = max(first_source_year, min(min(years) for years in historical_bins.values()))
            if ldv and atb_class is not None:
                first_year = max(first_year, int(history.year.min()))
            annual, base_values, period_values = {}, {}, {}
            loads = inputs["loads"].loc[inputs["loads"].region.eq(region) & inputs["loads"]["mode"].eq(mode)]
            base_load = positive(observed_value(loads, base, "load_factor"), "CEUD base-year load")
            service_units = rules["service_units"][rules["road_classes"][mode]["service"]]
            years_to_show = sorted(set(range(first_year, base + 1)) | set(int(y) for y in atb_audit.year if base < y <= end))
            for year in years_to_show:
                for powertrain in powertrains:
                    if not ldv and year <= base and powertrain not in historical_powertrains:
                        continue
                    result = evidence.derive_consumption(region, mode, powertrain, year, allow_analogue=False)
                    if result is None:
                        continue
                    annual[powertrain, year] = result
                    add(context | {"powertrain": powertrain}, "Annual calculation", "Observed-year consumption calculation", year,
                        "consumption", result["native_intensity"], "MJ/vkm", treatment=result["treatment"],
                        analogue=result["analogue"], consumption_index=result["consumption_index"], load_factor=result["load_factor"],
                        historical_index_anchor=result.get("historical_index_anchor"), calibration_observation=year <= base)

            for powertrain in powertrains:
                # A projected MHDV powertrain may have an ATB base level without any
                # historical CEUD powertrain index. Do not invent its earlier history.
                base_result = (evidence.derive_consumption(region, mode, powertrain, base, allow_analogue=True)
                               if ldv or powertrain in historical_powertrains else None)
                consumption = base_result["native_intensity"] if base_result is not None else evidence.atb_value(region, mode, powertrain, base)
                base_values[powertrain] = consumption
                marker(context, powertrain, consumption, base_load, base, service_units,
                       sample_kind="base_year_anchor", marker_source="Base-year anchor",
                       source_year_start=base, source_year_end=base,
                       treatment="Exact production base-year level; PEV levels come directly from ATB")
                bins = {**historical_bins, **{v: [periods.projection_year(v, legacy_at_end=True)] for v in periods.model
                                             if periods.projection_year(v, legacy_at_end=True) <= end}}
                for vintage, candidate_years in bins.items():
                    historical = vintage in historical_bins
                    if historical:
                        cohort_key = (mode, _family(powertrain), vintage)
                        if cohort_key not in cohort_counts:
                            cohort_counts[cohort_key] = _cohort_counts(bundle, inputs, mode, powertrain, vintage)
                        counts = cohort_counts[cohort_key]
                    else:
                        counts = pd.Series(1.0, index=candidate_years)
                    if counts.empty:
                        continue
                    supported_years = [int(year) for year in counts.index if not historical or year >= first_source_year]
                    observations = {year: annual[powertrain, year] for year in supported_years
                                    if (powertrain, year) in annual}
                    if not observations and supported_years:
                        observations = evidence.period_observations(region, mode, powertrain, supported_years,
                            allow_analogue_when_empty=allow_fallback)
                    if not observations:
                        output.append({**context, "powertrain": powertrain, "stage": "Missing class coverage",
                            "series": "No supported period observations", "year": float(np.average(counts.index, weights=counts)),
                            "metric": "consumption", "value": np.nan, "units": "MJ/vkm", "vintage": vintage,
                            "source_year_start": min(counts.index), "source_year_end": max(counts.index), "cohort_stock_coverage": 0.0,
                            "treatment": "No exact observations or permitted proxy; no interpolated period marker"})
                        continue
                    years = sorted(observations)
                    for year, observation in observations.items():
                        if (powertrain, year) not in annual:
                            annual[powertrain, year] = observation
                            add(context | {"powertrain": powertrain}, "Annual calculation", "Configured analogue consumption calculation", year,
                                "consumption", observation["native_intensity"], "MJ/vkm", treatment=observation["treatment"],
                                analogue=observation.get("analogue"), historical_index_anchor=observation.get("historical_index_anchor"),
                                consumption_index=observation.get("consumption_index"), load_factor=observation["load_factor"],
                                observation_basis="Configured analogue for empty observed period", calibration_observation=False)
                    used_counts = counts.loc[years]
                    result = aggregate_consumption_period(
                        [{**observations[y], "source_year": y} for y in years], used_counts.tolist())
                    consumption, load, coordinate = result["native_intensity"], result["load_factor"], result["weighted_source_year"]
                    coverage = float(used_counts.sum() / counts.sum()) if historical else np.nan
                    observation_summary = _observation_summary(list(observations.values()))
                    period_values[powertrain, vintage] = {**result, "years": years, "cohort_stock_coverage": coverage,
                        "candidate_year_count": len(counts), "included_year_count": len(years), **observation_summary}
                    # Actual aggregate periods are loaded from the prepared CSV below.
                    # Historical LDV ranges are intermediate components of the OMEGA row.
                    range_component = ldv and powertrain.startswith(("bev_", "phev_")) and vintage in historical_bins
                    if atb_class is not None or range_component or not recorded_selection:
                        marker(context, powertrain, consumption, load, coordinate, service_units,
                               vintage=vintage, sample_kind="period", source_year_start=min(years), source_year_end=max(years),
                               marker_source="ATB class recalculation" if atb_class is not None else "Production range component" if range_component and recorded_selection else "Recalculated period",
                               cohort_stock_coverage=coverage, candidate_year_count=len(counts), included_year_count=len(years),
                               **observation_summary,
                               treatment="Age-distribution-weighted mean consumption; service conversion last" if historical else "Future interval-end projection",
                               time_basis="Cohort-stock-weighted observed year; missing source years are excluded and coverage reported" if historical else "Future interval-end source year")
            if not ldv:
                continue
            for family, anchor in rules["pev_historical_anchors"].items():
                shares = _range_shares(inputs, mode, family)
                representative_base = representative_consumption(base_values, shares)
                marker(context, family + "_existing_representative", representative_base, base_load, base, service_units,
                       sample_kind="base_year_anchor", marker_source="Base-year anchor", source_year_start=base, source_year_end=base,
                       treatment="OMEGA-weighted range consumption at the exact ATB base-year levels")
                if atb_class is not None or not recorded_selection:
                    for vintage in historical_bins:
                        if any((bucket, vintage) not in period_values for bucket in shares):
                            continue
                        consumption = representative_consumption(
                            {bucket: period_values[bucket, vintage]["native_intensity"] for bucket in shares}, shares)
                        selected_period = period_values[anchor, vintage]
                        load, years, coordinate = selected_period["load_factor"], selected_period["years"], selected_period["weighted_source_year"]
                        marker(context, family + "_existing_representative", consumption, load, coordinate, service_units,
                               vintage=vintage, sample_kind="period", marker_source="ATB class recalculation" if atb_class is not None else "Recalculated period",
                               source_year_start=min(years), source_year_end=max(years),
                               cohort_stock_coverage=selected_period["cohort_stock_coverage"],
                               candidate_year_count=selected_period["candidate_year_count"], included_year_count=selected_period["included_year_count"],
                               **{key: selected_period[key] for key in ("observation_basis", "source_treatments", "analogue")},
                               treatment="OMEGA arithmetic mean of range period consumptions; service conversion last")
                anchor_atb = evidence.atb_value(region, mode, anchor, base)
                for powertrain in [p for p in powertrains if _family(p) == family]:
                    anchors.append({**context, "powertrain": powertrain, "family": family, "anchor": anchor, "base_year": base,
                        "atb_anchor_base_mj_per_vkm": anchor_atb, "production_base_mj_per_vkm": base_values[powertrain],
                        "existing_representative_base_mj_per_vkm": representative_base, "omega_share": shares.get(powertrain, 0.0)})
    return pd.DataFrame(output), pd.DataFrame(anchors)


def _raw_sources(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], distributions: pd.DataFrame) -> pd.DataFrame:
    rules = load_harmonization_rules(bundle, "efficiencies")
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    contract = atb_rules["components"]["phev_efficiency"]["future_nrcan_phev_contract"]
    statistic = bundle.scenario.efficiencies.rating_statistic
    fields = (contract["combined_cs_output_numeric_field"], contract["combined_cd_output_numeric_field"])
    statistics = rating_class_statistics(distributions, rules=rules, statistic=statistic, value_columns=fields)
    keys = ["rating_class", "powertrain", "year", "native_unit"]
    statistics = statistics.merge(distributions[keys + ["vehicle_group", "atb_class"]].drop_duplicates(),
                                  on=keys, validate="one_to_one")
    rows = []
    for item in statistics.to_dict("records"):
        group, atb_class, rating_class, powertrain, year, unit = (
            item[field] for field in ["vehicle_group", "atb_class", "rating_class", "powertrain", "year", "native_unit"])
        if powertrain.startswith("phev"):
            for component, field, native_unit, basis in (
                ("CS", contract["combined_cs_output_numeric_field"], "L/100 km (CS)", "Charge-sustaining fuel only; no utility weighting"),
                ("CD", contract["combined_cd_output_numeric_field"], "Le/100 km (CD)", "Charge-depleting total equivalent rating; may include liquid fuel; no utility weighting"),
            ):
                rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                    "year": year, "value": item[field], "units": native_unit, "series": f"NRCan {component} distinct-model {statistic}",
                    "native_basis": basis})
        else:
            rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                "year": year, "value": item["native_consumption"], "units": unit, "series": f"NRCan distinct-model {statistic}",
                "native_basis": "Source combined rating"})
    phev = inputs["atb_phev"].copy()
    phev["trajectory"] = phev.trajectory.replace(atb_rules["components"]["phev_efficiency"]["reconciliation"]["scenario_aliases"])
    trajectory = configured_trajectory(bundle)
    selected = select_atb_consumption(inputs["atb_vehicles"], phev, trajectory=trajectory, rules=rules, conversions=load_conversion_factors(bundle))
    memberships = inputs["atb"][["mode", "vehicle_class"]].drop_duplicates()
    for group, mode in VIEW_MODES.items():
        classes = set(memberships.loc[memberships["mode"].eq(mode), "vehicle_class"])
        for item in selected.loc[selected.vehicle_class.isin(classes)].itertuples(index=False):
            rows.append({"vehicle_group": group, "atb_class": item.vehicle_class, "source_class": item.vehicle_class, "powertrain": item.powertrain,
                "year": item.year, "value": item.source_value, "units": item.source_unit, "series": "ATB native input",
                "native_basis": "Source utility-weighted equivalent energy" if item.powertrain.startswith("phev") else "Source equivalent-gallon fuel economy"})
            if item.powertrain.startswith("phev"):
                components = phev.loc[phev.trajectory.eq(trajectory) & phev.year.eq(item.year)
                                      & phev.vehicle_class.eq(item.vehicle_class) & phev.vehicle_detail.eq(item.vehicle_detail)]
                if len(components) != 1:
                    raise ValueError(f"Ambiguous native ATB PHEV component evidence: {item.vehicle_class}/{item.powertrain}/{item.year}")
                component = components.iloc[0]
                for field, unit, label in (
                    ("combined_cs_fuel_economy_mi_per_gallon_equivalent", "mi/" + component.fuel_equivalent_basis + " (CS)", "ATB native charge-sustaining fuel economy"),
                    ("combined_cd_electricity_consumption_wh_per_mi", "Wh/mi (CD)", "ATB native charge-depleting electricity"),
                ):
                    rows.append({"vehicle_group": group, "atb_class": item.vehicle_class, "source_class": item.vehicle_class,
                        "powertrain": item.powertrain, "year": item.year, "value": float(component[field]), "units": unit,
                        "series": label, "native_basis": "Source combined cycle value before utility weighting / model HHV conversion"})
        if rules["road_classes"][mode]["pathway"] == "mhdv":
            road = rules["road_classes"][mode]
            for fuel in set(rules["historical_index_fuels"][mode].values()):
                source = ceud_series(inputs["ceud"], road["stock_table"], road["cs_prefix"] + "|" + rules["fuel_series"][fuel], "L/100 km")
                for item in source.itertuples(index=False):
                    rows.append({"vehicle_group": group, "atb_class": COHORT, "source_class": "CEUD coarse vehicle group", "powertrain": fuel,
                        "year": item.year, "value": item.value, "units": "L/100 km", "series": "NRCan CEUD observed on-road consumption",
                        "native_basis": "Coarse fleet fuel index; not ATB vocation observations"})
    result = pd.DataFrame(rows)
    result["powertrain_family"] = result.powertrain.map(_family)
    return result


def _published_periods(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Join final rows to the consumption/load evidence immediately before conversion."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    relationships = inputs["relationships"].loc[inputs["relationships"]["mode"].isin(VIEW_MODES.values()), ["tech", "mode", "powertrain"]].drop_duplicates()
    selected = inputs["final_efficiency"].merge(relationships, on="tech", validate="many_to_one")
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    selected = selected.loc[selected.region.eq(rules["region_output_map"].get(region, region)) & selected["mode"].isin(VIEW_MODES.values())].copy()
    selected["existing"] = selected.tech.str.endswith(rules["existing_suffix"])
    keys = ["region", "tech", "vintage", "input_comm", "output_comm"]
    fields = ["native_intensity", "native_unit", "load_factor", "weighted_source_year",
              "source_year_start", "source_year_end", "candidate_year_count", "included_year_count", "cohort_stock_coverage"]
    periods = inputs["periods"]
    if set(keys + fields) - set(periods):
        raise ValueError("Rebuild notebook evidence with period consumption before service conversion")
    selected = selected.merge(periods[keys + fields], on=keys, how="left", validate="one_to_one")
    if "annual" in inputs:
        annual = inputs["annual"]
        included = annual.loc[annual.included_in_period.eq(True)]
        summaries = [{**dict(zip(keys, key)), **_observation_summary(group.to_dict("records"))}
                     for key, group in included.groupby(keys)]
        selected = selected.merge(pd.DataFrame(summaries), on=keys, how="left", validate="one_to_one")
    if (not np.isfinite(selected[["native_intensity", "load_factor", "weighted_source_year"]]).all().all()
            or selected.native_unit.isna().any() or selected.native_intensity.le(0).any() or selected.load_factor.le(0).any()
            or not np.allclose(selected.load_factor / selected.native_intensity, selected.efficiency, rtol=1e-10, atol=1e-12)):
        raise ValueError("Prepared service efficiency disagrees with period consumption/load evidence")
    selected["year"] = selected.weighted_source_year
    selected["time_basis"] = np.where(selected.existing, "Cohort-stock-weighted observed consumption; service conversion after all aggregation",
                                     "Future interval-end source year; vintage retained separately")
    selected["vehicle_group"] = selected["mode"].map({mode: group for group, mode in VIEW_MODES.items()})
    representatives = selected.existing & selected["mode"].isin(["cars", "passenger_light_trucks"]) & selected.powertrain.isin(["bev", "phev"])
    selected.loc[representatives, "powertrain"] += "_existing_representative"
    selected["atb_class"], selected["resolution"] = COHORT, "Actual production aggregation"
    selected["stage"], selected["series"], selected["metric"] = "Indexed bridge", "Production method", "service_efficiency"
    selected["sample_kind"], selected["marker_source"] = "period", "Final prepared period CSV"
    selected["value"] = selected.efficiency
    energy = selected.assign(metric="consumption", units="MJ/vkm", value=selected.native_intensity)
    return pd.concat([selected, energy], ignore_index=True)


def interpolate_period_markers(bundle: ConfigBundle, bridge: pd.DataFrame) -> pd.DataFrame:
    """Connect period values and the exact base anchor without refitting annual data.

    PCHIP preserves the shape and bounds between adjacent consumption knots. Service
    efficiency uses the same curve and interpolated load, so both views are consistent.
    These samples are a display aid; the prepared period values remain authoritative.
    """
    spec = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    if spec["period_interpolation"] != "pchip":
        raise ValueError("Unsupported efficiency period interpolation")
    step = positive(spec["interpolation_step_years"], "interpolation step")
    markers = bridge.loc[bridge.stage.eq("Indexed bridge") & bridge.metric.eq("consumption")
                         & bridge.sample_kind.isin(["period", "base_year_anchor"])
                         & bridge.year.le(spec["bridge_years"][1])]
    output = []
    keys = ["vehicle_group", "mode", "atb_class", "powertrain", "resolution"]
    rules = load_harmonization_rules(bundle, "efficiencies")
    for key, group in markers.groupby(keys, sort=True):
        for _, duplicate in group.groupby("year"):
            if any(not np.allclose(duplicate[field], duplicate[field].iloc[0], rtol=1e-10, atol=1e-12)
                   for field in ("value", "load_factor")):
                raise ValueError(f"Conflicting period and base-year coordinates: {key}")
        knots = group.sort_values("year").drop_duplicates("year")
        if len(knots) < 2:
            continue
        x, y = knots.year.to_numpy(dtype=float), knots.value.to_numpy(dtype=float)
        loads = knots.load_factor.to_numpy(dtype=float)
        if not np.isfinite(y).all() or (y <= 0).any() or not np.isfinite(loads).all() or (loads <= 0).any():
            raise ValueError(f"Non-positive or non-finite interpolation knots: {key}")
        samples = np.unique(np.concatenate([x, np.arange(x[0], x[-1], step)]))
        consumption = PchipInterpolator(x, y, extrapolate=False)(samples)
        load = np.interp(samples, x, loads)
        context = dict(zip(keys, key, strict=True))
        units = rules["service_units"][rules["road_classes"][context["mode"]]["service"]]
        for metric, values, unit in (("consumption", consumption, "MJ/vkm"), ("service_efficiency", load / consumption, units)):
            output.extend({**context, "stage": "Diagnostic interpolation", "series": "Diagnostic PCHIP", "sample_kind": "curve",
                           "year": float(year), "metric": metric, "value": float(value), "units": unit, "load_factor": float(factor),
                           "treatment": "PCHIP through period markers and exact base-year anchor; no annual fitting or extrapolation"}
                          for year, value, factor in zip(samples, values, load, strict=True))
    return pd.concat([bridge, pd.DataFrame(output)], ignore_index=True)


def _benchmark(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pieces = []
    for name, series in (("rating_weights", f"Promoted distinct-model {bundle.scenario.efficiencies.rating_statistic} / class weights"), ("benchmark_weighted", "Diagnostic MTO weighting / production fallback")):
        for item in _rating_series(inputs[name]).itertuples(index=False):
            support = inputs[name].loc[inputs[name].ceud_class.eq(item.ceud_class)
                                       & inputs[name].powertrain.eq(item.powertrain) & inputs[name].year.eq(item.year)]
            pieces.append({"ceud_class": item.ceud_class, "year": item.year, "powertrain": item.powertrain, "series": series,
                           "value": item.value, "units": "MJ/vkm", "powertrain_family": _family(item.powertrain),
                           "aggregation_method": " | ".join(sorted(support.aggregation_method.unique())),
                           "cohort_matched_stock": float(support.cohort_matched_stock.max()),
                           "calibration_observation": item.year <= bundle.scenario.periods.base_year})
    return pd.DataFrame(pieces)


def build_diagnostic_frames(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], *, recorded_statistic: str | None = None) -> tuple[dict, dict]:
    """Explain recorded production values and their source distributions."""
    rules = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    phev_index = phev_historical_index_mode(efficiency_rules)
    distribution, distribution_summary = rating_distributions(bundle, inputs["ratings"])
    bridge, anchors = _bridge_rows(bundle, inputs, recorded_statistic=recorded_statistic)
    if recorded_statistic is None or recorded_statistic == bundle.scenario.efficiencies.rating_statistic:
        bridge = pd.concat([bridge, _published_periods(bundle, inputs)], ignore_index=True)
    bridge = interpolate_period_markers(bundle, bridge)
    bridge["powertrain_family"] = bridge.powertrain.map(_family)
    coverage = inputs["benchmark_matches"].groupby("match_status").mto_fit_active_stock.sum()
    frames = {"raw_sources": _raw_sources(bundle, inputs, distribution), "rating_distribution": distribution,
        "rating_summary": distribution_summary, "bridge": bridge, "anchor_comparison": anchors,
        "omega_shares": inputs["omega_buckets"].copy(), "mto_benchmark": _benchmark(bundle, inputs)}
    summary = {
        "region": rules["region"], "base_year": bundle.scenario.periods.base_year, "bridge_years": rules["bridge_years"],
        "anchor_powertrains": efficiency_rules["pev_historical_anchors"], "vehicle_groups": list(VIEW_MODES),
        "minimum_model_year": rules["minimum_model_year"], "distribution_year_step": rules["distribution_year_step"],
        "distribution_consumption_domains": rules["distribution_consumption_domains"],
        "distribution_cd_consumption_domains": rules["distribution_cd_consumption_domains"],
        "period_interpolation": rules["period_interpolation"],
        "population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        "atb_trajectory": bundle.scenario.efficiencies.atb_trajectory,
        "rating_statistic": bundle.scenario.efficiencies.rating_statistic,
        "recorded_rating_statistic": recorded_statistic or bundle.scenario.efficiencies.rating_statistic,
        "historical_index_fuels": load_harmonization_rules(bundle, "efficiencies")["historical_index_fuels"],
        "available_rating_years": sorted(int(y) for y in distribution.year.unique()),
        "benchmark_stock_dispositions": {str(key): float(value) for key, value in coverage.items()},
        "omega_model_years": sorted(int(y) for y in inputs["omega_records"].model_year.unique()),
        "phev_historical_index": phev_index,
        "limitations": [
            "MTO make/model/year keys identify reviewed vehicle classes; powertrain and PEV range are unobserved. Exact-key weighting remains diagnostic only.",
            "Catalogue rows are rated variants, not equal registered stocks. Production first averages variants per exact make/model, then uses the configured distinct-model mean or median and MTO size-class weights.",
            f"BEV300/PHEV35 supply shared historical indices; PHEV uses NRCan {phev_index}. Each range retains its own ATB total-energy level at and after the base year.",
            "PHEV CS fuel HHV and CD gasoline-equivalent curves are operating-mode rates before utility weighting; neither is NRCan annual total energy or a measured electrical-input share.",
            "Production averages consumption using raw annual cohort stock from the age distribution, before lifetime eligibility or redistribution. Occupancy/payload uses the same included-year weights; service conversion follows period and OMEGA consumption aggregation.",
            "Graph 4 alone uses diagnostic PCHIP between markers. Production uses observed source points and the explicit recorded source/proxy policies, without fitting missing NRCan years.",
            "The bridge statistic selector recomputes consumption with the production helpers from recorded inputs. Recalculated period markers are comparisons; only the recorded statistic carries final prepared CSV markers.",
            "Detailed ATB-class trajectories inherit parent-mode loads, cohort counts and broad car/light-truck OMEGA range shares; they are not independently observed fleet cohorts.",
            "Partially observed periods use available annual values and expose cohort-stock coverage. Zero-observation periods use only a permitted explicit proxy or remain missing; no rating gaps are interpolated.",
            "Medium/heavy history applies coarse CEUD fuel indices to ATB vocation levels; it does not observe fine-class cohort efficiencies.",
            "NRCan model years after the base year are source observations excluded from historical calibration, not a statistical holdout test.",
            "OMEGA shares describe US sales in the recorded model year, not Ontario survivors. Existing representatives retain sales-weighted consumption.",
            "Naive bootstrapping of unresolved survivor-stock matches cannot recover missing powertrains, population representation or systematic matching bias. No population-equivalent bootstrap is implemented.",
        ],
    }
    return frames, summary


def load_diagnostic(*, repo_root: Path | None = None) -> LDVEfficiencyDiagnostic:
    """Load the latest successful efficiency evidence snapshot without selecting a scenario."""
    from validation.notebook_evidence import load_notebook_evidence

    recorded = load_notebook_evidence("efficiency", repo_root=repo_root)
    inputs = {name: pd.read_csv(path, low_memory=False) for name, path in recorded.paths.items()
              if path.name.lower().endswith((".csv", ".csv.gz"))}
    frames, summary = build_diagnostic_frames(recorded.bundle, inputs)
    summary["build"] = recorded.metadata
    summary["inputs"] = {name: {"path": str(path), "sha256": file_sha256(path)} for name, path in recorded.paths.items()}
    return LDVEfficiencyDiagnostic(recorded.bundle, frames, summary, inputs)


def select_rating_statistic(diagnostic: LDVEfficiencyDiagnostic, statistic: str) -> LDVEfficiencyDiagnostic:
    """Recompute a bridge comparison from captured inputs, without ETL or publication."""
    if statistic not in {"mean", "median"}:
        raise ValueError(f"Unsupported rating statistic: {statistic!r}")
    if statistic == diagnostic.summary["rating_statistic"]:
        return diagnostic
    if "rating_class_weights" not in diagnostic.inputs:
        raise ValueError("Rebuild notebook evidence to capture the production NRCan class weights")
    efficiencies = diagnostic.bundle.scenario.efficiencies.model_copy(update={"rating_statistic": statistic})
    bundle = replace(diagnostic.bundle, scenario=diagnostic.bundle.scenario.model_copy(update={"efficiencies": efficiencies}))
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    inputs = {**diagnostic.inputs, "rating_weights": aggregate_ratings(
        diagnostic.inputs["ratings"], diagnostic.inputs["rating_class_weights"],
        rules=load_harmonization_rules(bundle, "efficiencies"), weight_basis=aggregation["ldv_weight_basis"], statistic=statistic)}
    recorded = diagnostic.summary["recorded_rating_statistic"]
    frames, summary = build_diagnostic_frames(bundle, inputs, recorded_statistic=recorded)
    summary.update({key: diagnostic.summary[key] for key in ("build", "inputs") if key in diagnostic.summary})
    return LDVEfficiencyDiagnostic(bundle, frames, summary, inputs)


def write_report(diagnostic: LDVEfficiencyDiagnostic) -> Path:
    """Export review tables explicitly; never a production ETL input."""
    directory = resolve_artifact_path(diagnostic.bundle, "ldv_efficiency_report")
    files = load_harmonization_rules(diagnostic.bundle, "ldv_efficiency_diagnostic")["files"]
    for name, frame in diagnostic.frames.items():
        write_dataframe_atomic(frame, directory / files[name])
    summary = {**diagnostic.summary, "exports": {name: {"file": files[name], "rows": len(frame), "sha256": file_sha256(directory / files[name])} for name, frame in diagnostic.frames.items()}}
    write_text_atomic(json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n", directory / files["summary"])
    return directory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    diagnostic = load_diagnostic()
    print(json.dumps(diagnostic.summary, indent=2, ensure_ascii=False, allow_nan=False))
    if args.write_report:
        print(f"Diagnostic report: {write_report(diagnostic)}")


if __name__ == "__main__":
    main()
