"""Recorded road-efficiency sources, aggregation distributions and bridge review.

Loading reads a successful build's evidence snapshot, without ETL or downloads.
Detailed-class recalculations and optional statistic comparisons are diagnostic.
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

from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.build_efficiencies import aggregate_consumption_period, combine_range_observations
from parameterization.road_aggregation import nrcan_to_nlr_map
from parameterization.road_efficiencies import ProjectionAnchorUnavailable, RoadEfficiencyEvidence, aggregate_ratings, ceud_series, observed_value, positive, rating_class_statistics, select_atb_consumption
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


def rating_filter_evidence(bundle: ConfigBundle, classified: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare the same native cohorts before and after production outlier screening."""
    required = {"included_before_filter", "outlier_excluded", "filter_reason", "outlier_status"}
    if required - set(classified):
        raise ValueError("Rebuild notebook evidence to capture NRCan statistical-filter decisions")
    rules = load_harmonization_rules(bundle, "efficiencies")
    keys = ["year", "rating_class", "powertrain", "native_unit"]
    audit = classified.copy()
    audit["powertrain_family"] = audit.powertrain.map(_family)
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    audit["atb_class"] = audit.rating_class.map(nrcan_to_nlr_map(mapping))
    audit["vehicle_group"] = audit.atb_class.map(mapping["target_to_ceud"]).map({"Car": "Cars", "Light Truck": "Light trucks"})
    before = audit.assign(included=audit.included_before_filter)

    def statistics(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
        mean = rating_class_statistics(frame, rules=rules, statistic="mean")
        median = rating_class_statistics(frame, rules=rules, statistic="median")
        result = mean[keys + ["native_consumption", "consumption_mj_per_vkm", "distinct_models", "observations"]].rename(columns={
            "native_consumption": prefix + "distinct_model_mean", "consumption_mj_per_vkm": prefix + "consumption_mj_per_vkm",
            "distinct_models": prefix + "distinct_models", "observations": prefix + "catalogue_rows"})
        return result.merge(median[keys + ["native_consumption"]].rename(columns={"native_consumption": prefix + "distinct_model_median"}),
                            on=keys, validate="one_to_one")

    baseline = statistics(before, "before_")
    impact = baseline.merge(statistics(audit, "after_"), on=keys, how="left", validate="one_to_one")
    impact[["after_distinct_models", "after_catalogue_rows"]] = impact[["after_distinct_models", "after_catalogue_rows"]].fillna(0)
    impact["powertrain_family"] = impact.powertrain.map(_family)
    impact["atb_class"] = impact.rating_class.map(nrcan_to_nlr_map(mapping))
    impact["vehicle_group"] = impact.atb_class.map(mapping["target_to_ceud"]).map({"Car": "Cars", "Light Truck": "Light trucks"})
    return audit, impact


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
        conversions=load_conversion_factors(bundle),
        atb_audit=atb, gcam=pd.DataFrame(), regen=pd.DataFrame(), base_year=bundle.scenario.periods.base_year, rules=rules,
        projection_years=tuple(bundle.scenario.periods.historical_years(bundle.scenario.periods.latest_observed_vintage)),
        projection_reference_year=bundle.scenario.periods.model[0],
        cohort_stock=inputs["annual_cohort_stock"].merge(
            inputs["relationships"].loc[inputs["relationships"].tech.isin(inputs["annual_cohort_stock"].tech),
                                        ["tech", "powertrain"]].drop_duplicates(), on="tech", validate="many_to_one"))


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
    for field in (cs_field, cd_field, "cd_electricity_kwh_per_100km", "cd_fuel_l_per_100km"):
        if field not in rows or rows[field].isna().any() or not np.isfinite(rows[field]).all() or rows[field].lt(0).any():
            raise ValueError("PHEV component diagnostic requires finite non-negative explicit CS/CD ratings")
    rules = load_harmonization_rules(bundle, "efficiencies")
    conversions = load_conversion_factors(bundle)
    per_km = conversions["service"]["per_100_km_to_per_km"]
    fuel = rows[rules["ratings"]["fields"]["phev_fuel"]].map(rules["ratings"]["fuel_codes"])
    hhv = fuel.map({name: conversions["fuel_hhv"][name + "_mj_per_litre"] for name in ("gasoline", "diesel")})
    if hhv.isna().any():
        raise ValueError("PHEV component diagnostic requires a supported liquid fuel")
    rows["cs_mj_per_vkm"] = rows[cs_field] * hhv * per_km
    rows["cd_mj_per_vkm"] = (rows.cd_electricity_kwh_per_100km * conversions["energy"]["kwh_to_mj"]
                              + rows.cd_fuel_l_per_100km * hhv) * per_km
    keys = ["year", "rating_class", "powertrain"]
    fields = (cs_field, cd_field, "cs_mj_per_vkm", "cd_mj_per_vkm")
    component_means = rating_class_statistics(
        rows, rules=rules,
        statistic=bundle.scenario.efficiencies.rating_statistic,
        value_columns=fields,
    )[keys + list(fields)]
    weights = weights.merge(component_means, on=keys, how="left", validate="many_to_one")
    if weights[[cs_field, cd_field]].isna().any().any():
        raise ValueError("PHEV component diagnostic is missing a recorded rating class")
    parts = []
    for component, field, energy_field, unit, energy_basis, native_basis in (
        ("CS", cs_field, "cs_mj_per_vkm", "L/100 km (CS)",
         "GREET fuel HHV", "Fuel consumed per distance in charge-sustaining operation; no utility weighting"),
        ("CD", cd_field, "cd_mj_per_vkm", "Le/100 km (CD)",
         "Electricity plus GREET liquid-fuel HHV", "CD electricity plus explicit liquid fuel; native Le retained for source comparison; no utility weighting"),
    ):
        values = weights.assign(weighted=weights.weight * weights[field], energy=weights.weight * weights[energy_field])
        grouped = values.groupby(["ceud_class", "powertrain", "year"], as_index=False).agg(native_value=("weighted", "sum"), value=("energy", "sum"))
        grouped["component"], grouped["native_unit"] = component, unit
        grouped["energy_basis"], grouped["native_basis"] = energy_basis, native_basis
        grouped["utility_weighted"] = False
        parts.append(grouped)
    return pd.concat(parts, ignore_index=True)[columns]


def harmonised_ceud_history(bundle: ConfigBundle, ceud: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Convert observed parent-fleet fuel volumes with fuel-specific heating values."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    road = rules["road_classes"][mode]
    factors = load_conversion_factors(bundle)["fuel_hhv"]
    parts = []
    for fuel in rules["mhdv_fleet_baseline"]["observed_fuels"][mode]:
        series = ceud_series(ceud, road["stock_table"], road["cs_prefix"] + "|" + rules["fuel_series"][fuel], "L/100 km")
        series["consumption_index"] = series.value / positive(observed_value(series, bundle.scenario.periods.base_year, "value"), "CEUD base-year consumption")
        series["value"] *= factors[fuel + "_mj_per_litre"] / 100
        series["powertrain"], series["source_fuel"] = fuel, fuel
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
    proxy = any(r.get("treatment") == "NRCan_missing_year_approved_analogue_index" for r in observations)
    truck_proxy = any(r.get("treatment") == "CEUD_diesel_fleet_ATB_relative_powertrain" for r in observations)
    fleet = any(r.get("treatment") == "CEUD_observed_fleet_consumption" for r in observations)
    basis = ("Configured analogue for empty observed period" if proxy else
             "Diesel fleet with relative ATB technology proxy" if truck_proxy else
             "Observed CEUD fleet series used as a vintage proxy" if fleet else
             "Available source observations / projection")
    return {"observation_basis": basis,
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
                        "NRCan CS fuel-only aggregate" if item.component == "CS" else "NRCan CD component-energy aggregate",
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
                        f"CEUD {item.source_fuel} observed fleet", item.year,
                        "consumption", item.value, "MJ/vkm", source_class="CEUD coarse vehicle group",
                        source_fuel=item.source_fuel, consumption_index=item.consumption_index,
                        calibration_observation=item.year <= base,
                        treatment="Observed parent-fleet fuel volume converted with its fuel-specific HHV; fleet series proxies vintages")
            powertrains = sorted(atb_audit.powertrain.unique())
            ldv = rules["road_classes"][mode]["pathway"] == "ldv"
            historical_powertrains = set(powertrains) if ldv else set(inputs["annual"].loc[
                inputs["annual"]["mode"].eq(mode) & inputs["annual"].source_year.le(base), "powertrain"])
            historical_bins = {v: periods.historical_years(v) for v in periods.existing}
            historical_bins = {v: years for v, years in historical_bins.items() if years and max(years) >= start - periods.step}
            first_year = max(first_source_year, min(min(years) for years in historical_bins.values()))
            if ldv and atb_class is not None:
                first_year = max(first_year, int(history.year.min()))
            annual, period_values = {}, {}
            service_units = rules["service_units"][rules["road_classes"][mode]["service"]]
            future_vintages = {periods.projection_year(v, legacy_at_end=True): v for v in periods.model}
            reference_year = min(periods.model)

            for year in range(first_year, base + 1):
                for powertrain in powertrains:
                    if not ldv and powertrain not in historical_powertrains:
                        continue
                    result = evidence.derive_consumption(region, mode, powertrain, year, allow_analogue=False)
                    if result is None:
                        continue
                    annual[powertrain, year] = result
                    derived = result["treatment"] == "CEUD_diesel_fleet_ATB_relative_powertrain"
                    add(context | {"powertrain": powertrain}, "Annual calculation",
                        "Diesel-fleet relative technology calculation" if derived else "Observed-year consumption calculation", year,
                        "consumption", result["native_intensity"], "MJ/vkm", treatment=result["treatment"],
                        analogue=result.get("analogue"), consumption_index=result.get("consumption_index"),
                        load_factor=result["load_factor"], historical_index_anchor=result.get("historical_index_anchor"),
                        observation_basis=_observation_summary([result])["observation_basis"], calibration_observation=not derived)

            for powertrain in powertrains:
                # Retain each range's own historical observations. A proxy may fill
                # an entirely empty period only through the production policy.
                if ldv or powertrain in historical_powertrains:
                    for vintage, candidate_years in historical_bins.items():
                        cohort_key = (mode, _family(powertrain), vintage)
                        if cohort_key not in cohort_counts:
                            cohort_counts[cohort_key] = _cohort_counts(bundle, inputs, mode, powertrain, vintage)
                        counts = cohort_counts[cohort_key]
                        if counts.empty:
                            continue
                        supported_years = [int(year) for year in counts.index if year >= first_source_year]
                        observations = {year: annual[powertrain, year] for year in supported_years if (powertrain, year) in annual}
                        if not observations and supported_years:
                            observations = evidence.period_observations(region, mode, powertrain, supported_years,
                                allow_analogue_when_empty=allow_fallback)
                        if not observations:
                            output.append({**context, "powertrain": powertrain, "stage": "Missing class coverage",
                                "series": "No supported period observations", "year": float(max(counts.index)),
                                "metric": "consumption", "value": np.nan, "units": "MJ/vkm", "vintage": vintage,
                                "source_year_start": min(counts.index), "source_year_end": max(counts.index),
                                "cohort_stock_coverage": 0.0,
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
                            [{**observations[year], "source_year": year} for year in years], used_counts.tolist())
                        coverage = float(used_counts.sum() / counts.sum())
                        observation_summary = _observation_summary(list(observations.values()))
                        period_values[powertrain, vintage] = {**result, "years": years, "observations": observations,
                            "cohort_stock_coverage": coverage, "candidate_year_count": len(counts),
                            "included_year_count": len(years), **observation_summary}
                        range_component = ldv and powertrain.startswith(("bev_", "phev_"))
                        if atb_class is not None or range_component or not recorded_selection:
                            marker(context, powertrain, result["native_intensity"], result["load_factor"], max(years), service_units,
                                vintage=vintage, sample_kind="period", source_year_start=min(years), source_year_end=max(years),
                                source_years=" | ".join(map(str, years)), weighted_source_year=result["weighted_source_year"],
                                marker_source="ATB class recalculation" if atb_class is not None else "Production range component" if range_component and recorded_selection else "Recalculated period",
                                cohort_stock_coverage=coverage, candidate_year_count=len(counts), included_year_count=len(years),
                                **observation_summary, treatment="Cohort-stock-weighted observed consumption; service conversion last",
                                time_basis="Last included source year; consumption uses cohort-stock weights")

                try:
                    anchor = evidence.projection_anchor(region, mode, powertrain)
                except ProjectionAnchorUnavailable:
                    if atb_class is None:
                        raise
                    output.append({**context, "powertrain": powertrain, "stage": "Missing class coverage",
                        "series": "No supported projection anchor", "year": float(reference_year),
                        "metric": "consumption", "value": np.nan, "units": "MJ/vkm", "cohort_stock_coverage": 0.0,
                        "treatment": "No observed latest-period anchor or permitted proxy; no future trajectory"})
                    continue
                anchor_powertrain = anchor["powertrain"]
                anchor_years = sorted(anchor["included_years"])
                reference_year = int(anchor["atb_reference_year"])
                atb_anchor = evidence.atb_value(region, mode, anchor_powertrain, reference_year)
                ratio = evidence.atb_value(region, mode, powertrain, reference_year) / positive(atb_anchor, "ATB reference anchor") if ldv else 1.0
                reference_consumption = anchor["native_intensity"] * ratio
                anchor_basis = _observation_summary(list(anchor["observations"].values()))
                anchors.append({**context, **anchor_basis, "powertrain": powertrain, "anchor_powertrain": anchor_powertrain,
                    "source_year_start": min(anchor_years), "source_year_end": max(anchor_years),
                    "source_years": " | ".join(map(str, anchor_years)), "stock_powertrain": anchor["stock_powertrain"],
                    "stock_proxy": anchor["stock_proxy"],
                    "cohort_stock_coverage": anchor["cohort_stock_coverage"], "anchor_consumption_mj_per_vkm": anchor["native_intensity"],
                    "atb_reference_year": reference_year, "range_ratio_at_reference": ratio,
                    "parameter_reference_consumption_mj_per_vkm": reference_consumption})
                marker(context, powertrain, reference_consumption, anchor["load_factor"], reference_year, service_units,
                    **anchor_basis, stock_powertrain=anchor["stock_powertrain"], stock_proxy=anchor["stock_proxy"],
                    sample_kind="base_year_anchor", marker_source="NRCan-cohort projection anchor" if ldv else "CEUD-cohort projection anchor",
                    source_year_start=min(anchor_years), source_year_end=max(anchor_years),
                    source_years=" | ".join(map(str, anchor_years)), projection_anchor_powertrain=anchor_powertrain,
                    cohort_stock_coverage=anchor["cohort_stock_coverage"], candidate_year_count=len(anchor["candidate_years"]),
                    included_year_count=len(anchor_years),
                    treatment="Latest historical cohort consumption; future ATB ratios start at the reference year",
                    time_basis="ATB projection reference year; tooltip source bounds identify the historical anchor")

                future_years = sorted(set(int(year) for year in atb_audit.year if reference_year <= year <= end) | {reference_year})
                for year in future_years:
                    result = evidence.derive_consumption(region, mode, powertrain, year, allow_analogue=False)
                    if result is None:
                        continue
                    add(context | {"powertrain": powertrain}, "Annual calculation", "ATB-relative projection", year,
                        "consumption", result["native_intensity"], "MJ/vkm", treatment=result["treatment"],
                        analogue=result.get("analogue"), consumption_index=result.get("consumption_index"),
                        load_factor=result["load_factor"], calibration_observation=False)
                    # The recorded production aggregate uses final prepared CSV
                    # markers at model-supported source years. Keep the explicit
                    # first ATB reference point even when it is not a model row.
                    if year == reference_year:
                        continue
                    if atb_class is not None or not recorded_selection or year == reference_year:
                        marker(context, powertrain, result["native_intensity"], result["load_factor"], year, service_units,
                            vintage=future_vintages.get(year), sample_kind="period", source_year_start=year, source_year_end=year,
                            source_years=str(year), marker_source="ATB class recalculation" if atb_class is not None else "Recalculated period" if not recorded_selection else "ATB source projection",
                            treatment=result["treatment"], time_basis="ATB source projection year")

            if not ldv:
                continue
            for family in rules["pev_projection_anchors"]:
                shares = _range_shares(inputs, mode, family)
                if atb_class is None and recorded_selection:
                    continue
                for vintage in historical_bins:
                    if any((bucket, vintage) not in period_values for bucket in shares):
                        continue
                    counts = cohort_counts[mode, family, vintage]
                    candidate_weights = (counts / counts.sum()).to_dict()
                    observed_ranges = {bucket: period_values[bucket, vintage]["observations"] for bucket in shares}
                    combined = combine_range_observations(observed_ranges, shares, candidate_weights)
                    observations, weights = combined["observations"], combined["weights"]
                    years = sorted(observations)
                    result = aggregate_consumption_period(
                        [{**observations[year], "source_year": year} for year in years], [weights[year] for year in years])
                    observation_summary = _observation_summary([row for records in observed_ranges.values() for row in records.values()])
                    marker(context, family + "_existing_representative", result["native_intensity"], result["load_factor"], max(years), service_units,
                        vintage=vintage, sample_kind="period", marker_source="ATB class recalculation" if atb_class is not None else "Recalculated period",
                        source_year_start=min(years), source_year_end=max(years), source_years=" | ".join(map(str, years)),
                        weighted_source_year=result["weighted_source_year"], cohort_stock_coverage=combined["cohort_coverage"],
                        candidate_year_count=len(counts), included_year_count=len(years),
                        range_coverage=json.dumps(combined["range_coverage"], sort_keys=True), **observation_summary,
                        treatment="OMEGA-weighted independently observed range periods; service conversion last",
                        time_basis="Last included source year; each range normalizes its own supported cohort years")
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
            for fuel in rules["mhdv_fleet_baseline"]["observed_fuels"][mode]:
                source = ceud_series(inputs["ceud"], road["stock_table"], road["cs_prefix"] + "|" + rules["fuel_series"][fuel], "L/100 km")
                for item in source.itertuples(index=False):
                    rows.append({"vehicle_group": group, "atb_class": COHORT, "source_class": "CEUD coarse vehicle group", "powertrain": fuel,
                        "year": item.year, "value": item.value, "units": "L/100 km", "series": "NRCan CEUD observed on-road consumption",
                        "native_basis": "Observed parent-fleet fuel volume; not ATB vocation observations"})
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
    fields += [field for field in ("range_source_years", "range_stock_coverage") if field in periods]
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
    selected["year"] = selected.source_year_end
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
    filter_audit, filter_impact = rating_filter_evidence(bundle, inputs["ratings"])
    filtered_distribution, _ = rating_distributions(bundle, inputs["ratings"])
    distribution, distribution_summary = rating_distributions(
        bundle, inputs["ratings"].assign(production_included=inputs["ratings"].included,
                                        included=inputs["ratings"].included_before_filter))
    bridge, anchors = _bridge_rows(bundle, inputs, recorded_statistic=recorded_statistic)
    if recorded_statistic is None or recorded_statistic == bundle.scenario.efficiencies.rating_statistic:
        bridge = pd.concat([bridge, _published_periods(bundle, inputs)], ignore_index=True)
    bridge["powertrain_family"] = bridge.powertrain.map(_family)
    coverage = inputs["benchmark_matches"].groupby("match_status").mto_fit_active_stock.sum()
    frames = {"raw_sources": _raw_sources(bundle, inputs, distribution),
        "filtered_sources": _raw_sources(bundle, inputs, filtered_distribution), "rating_distribution": distribution,
        "rating_summary": distribution_summary, "bridge": bridge, "projection_anchors": anchors,
        "rating_filter_audit": filter_audit, "rating_filter_impact": filter_impact,
        "omega_shares": inputs["omega_buckets"].copy(), "mto_benchmark": _benchmark(bundle, inputs)}
    summary = {
        "region": rules["region"], "base_year": bundle.scenario.periods.base_year, "bridge_years": rules["bridge_years"],
        "anchor_powertrains": efficiency_rules["pev_projection_anchors"], "vehicle_groups": list(VIEW_MODES),
        "atb_reference_year": bundle.scenario.periods.model[0],
        "rating_filter_rules": efficiency_rules["ratings"]["outlier_filter"],
        "minimum_model_year": rules["minimum_model_year"], "distribution_year_step": rules["distribution_year_step"],
        "distribution_consumption_domains": rules["distribution_consumption_domains"],
        "population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        "atb_trajectory": bundle.scenario.efficiencies.atb_trajectory,
        "rating_statistic": bundle.scenario.efficiencies.rating_statistic,
        "recorded_rating_statistic": recorded_statistic or bundle.scenario.efficiencies.rating_statistic,
        "mhdv_fleet_baseline": efficiency_rules["mhdv_fleet_baseline"],
        "available_rating_years": sorted(int(y) for y in distribution.year.unique()),
        "benchmark_stock_dispositions": {str(key): float(value) for key, value in coverage.items()},
        "omega_model_years": sorted(int(y) for y in inputs["omega_records"].model_year.unique()),
        "phev_consumption_method": "NRCan CD/CS components weighted by ATB range-bucket utility factors",
        "limitations": [
            "MTO make/model/year keys identify reviewed vehicle classes; powertrain and PEV range are unobserved. Exact-key weighting remains diagnostic only.",
            "Catalogue rows are rated variants, not equal registered stocks. Production first averages variants per exact make/model, then uses the configured distinct-model mean or median and MTO size-class weights.",
            "Each historical PEV range uses its own observed NRCan total consumption before OMEGA aggregation; ranges can have different observed-year support. BEV300/PHEV35 anchor future levels only.",
            "PHEV total energy combines explicit NRCan CD electricity/fuel and CS fuel with ATB's assigned range-bucket distance utility factor. The U.S. travel-share transfer is an assumption, not a Canadian charging observation or a correction for differing test procedures.",
            "The latest existing cohort anchors LDV and MD/HD projection levels and loads. ATB's first model-year point is the ratio denominator; later NRCan observations remain visible but do not recalibrate this recorded build.",
            "Symmetric Tukey fences screen distinct-model total energy within exact annual class/range cohorts with at least five models and positive IQR. Sparse/zero-spread cohorts are retained. Statistical exclusions do not establish measurement errors, sales rarity or a performance designation.",
            "Production averages consumption using raw annual cohort stock from the age distribution, before lifetime eligibility or redistribution. Occupancy/payload uses the same included-year weights; service conversion follows period and OMEGA consumption aggregation.",
            "The bridge statistic selector recomputes consumption with the production helpers from recorded inputs. Recalculated period markers are comparisons; only the recorded statistic carries final prepared CSV markers.",
            "Detailed ATB-class trajectories inherit parent-mode loads, cohort counts and broad car/light-truck OMEGA range shares; they are not independently observed fleet cohorts.",
            "Partially observed periods use available annual values and expose cohort-stock coverage. Zero-observation periods use only a permitted explicit proxy or remain missing; no rating gaps are interpolated.",
            "Medium/heavy incumbents use fuel-specific CEUD fleet consumption as a vintage proxy. Emerging powertrains use diesel-relative ATB ratios. Future cohort anchors use ATB relative improvements; gasoline follows diesel rates. These are not observed fine-class model-year efficiencies.",
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
