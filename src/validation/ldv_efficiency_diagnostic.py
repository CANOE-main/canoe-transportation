"""Recorded road-efficiency sources, aggregation distributions and bridge review.

Loading reads a successful build's evidence snapshot, without ETL or downloads.
All class recalculations and alternative PEV anchors are diagnostic only.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.road_aggregation import nrcan_to_nlr_map
from parameterization.road_efficiencies import RoadEfficiencyEvidence, ceud_series, interpolate, phev_historical_index_mode, positive, select_atb_consumption
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
             for name in ("ratings", "rating_weights", "atb", "annual", "loads", "relationships")}
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
    models = frame.groupby([*keys, "make_model"], as_index=False).native_consumption.mean()
    model_means = models.groupby(keys, as_index=False).native_consumption.mean().rename(columns={"native_consumption": "equal_distinct_model_mean"})
    summary = summary.merge(model_means, on=keys, validate="one_to_one")
    summary["powertrain_family"] = summary.powertrain.map(_family)
    summary["calibration_observation"] = summary.year.le(bundle.scenario.periods.base_year)
    return frame, summary


def _class_rating_audit(bundle: ConfigBundle, audit: pd.DataFrame, atb_class: str) -> pd.DataFrame:
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    selected = audit.loc[audit.rating_class.map(nrcan_to_nlr_map(mapping)).eq(atb_class)].copy()
    selected["weight"] /= selected.groupby(["year", "powertrain"]).weight.transform("sum")
    selected["ceud_class"] = atb_class
    return selected


def sole_anchor_consumption(anchor_baseline: float, target_atb: float, anchor_atb_base: float) -> float:
    """A shared Canadian level adjustment preserves ATB's target-range ratio."""
    return positive(anchor_baseline, "anchor baseline") * positive(target_atb, "target ATB consumption") / positive(anchor_atb_base, "anchor ATB baseline")


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
    component_means = rows.groupby(keys, as_index=False)[[cs_field, cd_field]].mean()
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


def phev_component_backcast(components: pd.DataFrame, *, component: str, base_year: int, anchor_consumption: float) -> pd.DataFrame:
    """Compare an operating-mode index at observed years with an observed base-year cohort."""
    if component not in {"CS", "CD"}:
        raise ValueError(f"Unsupported PHEV comparison component: {component!r}")
    selected = components.loc[components.component.eq(component) & components.year.le(base_year)].sort_values("year").copy()
    if selected.duplicated("year").any():
        raise ValueError("PHEV comparison backcast requires one class/powertrain cohort per year")
    anchor = selected.loc[selected.year.eq(base_year), "value"]
    if len(anchor) != 1 or anchor.iloc[0] <= 0:
        return selected.iloc[0:0].assign(consumption_index=pd.Series(dtype=float), index_segment=pd.Series(dtype=int))
    selected["consumption_index"] = selected.value / anchor.iloc[0]
    selected["value"] = positive(anchor_consumption, "ATB PHEV total-energy anchor") * selected.consumption_index
    selected["index_segment"] = selected.year.diff().ne(1).cumsum()
    return selected


def harmonised_ceud_history(bundle: ConfigBundle, ceud: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Convert parent-fleet fuel evidence; retain its role as an index context."""
    rules = load_harmonization_rules(bundle, "efficiencies")
    road = rules["road_classes"][mode]
    factors = load_conversion_factors(bundle)["fuel_hhv"]
    parts = []
    for powertrain, fuel in rules["historical_index_fuels"][mode].items():
        series = ceud_series(ceud, road["stock_table"], road["cs_prefix"] + "|" + rules["fuel_series"][fuel], "L/100 km")
        series["consumption_index"] = series.value / interpolate(series, bundle.scenario.periods.base_year, "value")
        series["value"] *= factors[fuel + "_mj_per_litre"] / 100
        series["powertrain"], series["source_fuel"] = powertrain, fuel
        parts.append(series)
    return pd.concat(parts, ignore_index=True)


def _bridge_rows(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    spec = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    rules = load_harmonization_rules(bundle, "efficiencies")
    start, end = map(int, spec["bridge_years"])
    base, region = bundle.scenario.periods.base_year, spec["region"]
    phev_index = phev_historical_index_mode(rules)
    comparison_index = "CS" if phev_index == "CD" else "CD"
    output, anchors = [], []

    def add(context, stage, series, year, metric, value, units, **extra):
        if pd.notna(value):
            output.append({**context, "stage": stage, "series": series, "year": int(year), "metric": metric, "value": float(value), "units": units, **extra})

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
                        treatment="Catalogue size-class means with recorded MTO class weights")
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
            first_year = max(start, int(history.year.min())) if atb_class is not None and rules["road_classes"][mode]["pathway"] == "ldv" else start
            consumption_by_year = {}
            service_units = rules["service_units"][rules["road_classes"][mode]["service"]]
            for year in range(first_year, end + 1):
                for powertrain in powertrains:
                    if (rules["road_classes"][mode]["pathway"] == "mhdv"
                            and year <= base and powertrain not in historical_powertrains):
                        continue
                    result = evidence.derive(region, mode, powertrain, year)
                    consumption_by_year.setdefault(year, {})[powertrain] = result["native_intensity"]
                    for metric, value, units in (("consumption", result["native_intensity"], "MJ/vkm"), ("service_efficiency", result["efficiency"], service_units)):
                        add(context | {"powertrain": powertrain}, "Indexed bridge", "Production method", year, metric, value, units,
                            treatment=result["treatment"], analogue=result["analogue"], consumption_index=result["consumption_index"], load_factor=result["load_factor"],
                            index_basis=(f"NRCan {phev_index} / approved analogue" if powertrain.startswith("phev") and mode in {"cars", "passenger_light_trucks"} and year <= base
                                         else "Direct ATB utility-weighted total energy" if powertrain.startswith("phev") and year > base else result["treatment"]),
                            calibration_observation=year <= base,
                            historical_basis=("NRCan class evidence / approved analogue" if mode in {"cars", "passenger_light_trucks"}
                                              else "Coarse CEUD fuel index; no observed ATB-class cohort efficiency" if year <= base
                                              else "Direct ATB projection; no observed fine-class Canadian calibration"))
            if mode not in {"cars", "passenger_light_trucks"}:
                continue
            loads = inputs["loads"].loc[inputs["loads"].region.eq(region) & inputs["loads"]["mode"].eq(mode)]
            for powertrain, cohort in components.groupby("powertrain"):
                alternative = phev_component_backcast(cohort, component=comparison_index, base_year=base, anchor_consumption=evidence.atb_value(region, mode, powertrain, base))
                for item in alternative.loc[alternative.year.ge(start)].itertuples(index=False):
                    if item.value <= 0:
                        continue
                    load = interpolate(loads, item.year, "load_factor")
                    for metric, value, units in (("consumption", item.value, "MJ/vkm"), ("service_efficiency", load / item.value, service_units)):
                        add(context | {"powertrain": powertrain}, "Indexed bridge",
                            "Previous NRCan CS backcast" if comparison_index == "CS" else "NRCan CD backcast comparison",
                            item.year, metric, value, units,
                            treatment=f"ATB total(base) × NRCan {comparison_index} mean(year) / NRCan {comparison_index} mean(base); diagnostic only",
                            index_basis=f"NRCan {comparison_index} operating-mode consumption ratio", consumption_index=item.consumption_index,
                            index_support=f"Observed {comparison_index} cohorts only; no analogue, interpolation or extrapolation",
                            index_segment=int(item.index_segment), load_factor=load, calibration_observation=False)
            for family, anchor in spec["anchor_powertrains"].items():
                shares = _range_shares(inputs, mode, family)
                for year, values in consumption_by_year.items():
                    if year <= base:
                        representative = representative_consumption(values, shares)
                        load = interpolate(loads, year, "load_factor")
                        common = context | {"powertrain": family + "_existing_representative"}
                        add(common, "Indexed bridge", "Existing OMEGA representative", year, "consumption", representative, "MJ/vkm", treatment="OMEGA sales-weighted consumption; unchanged")
                        add(common, "Indexed bridge", "Existing OMEGA representative", year, "service_efficiency", load / representative, service_units, treatment="Harmonic mean of range service efficiencies; unchanged")
                baseline, analogue = (evidence.ldv_baseline(region, mode, anchor) if family == "bev" else (evidence.atb_value(region, mode, anchor, base), anchor))
                anchor_atb = evidence.atb_value(region, mode, anchor, base)
                representative_base = representative_consumption(consumption_by_year[base], shares)
                for powertrain in [p for p in powertrains if _family(p) == family]:
                    for year in range(max(base, start), end + 1):
                        consumption = sole_anchor_consumption(baseline, evidence.atb_value(region, mode, powertrain, year), anchor_atb)
                        common = context | {"powertrain": powertrain}
                        add(common, "Indexed bridge", f"Exploratory sole {anchor} anchor", year, "consumption", consumption, "MJ/vkm", treatment="anchor baseline × ATB target(year) / ATB anchor(base)", analogue=analogue, anchor=anchor)
                        add(common, "Indexed bridge", f"Exploratory sole {anchor} anchor", year, "service_efficiency", interpolate(loads, base, "load_factor") / consumption, service_units, treatment="Diagnostic alternative; no production promotion", anchor=anchor)
                    candidate_base = sole_anchor_consumption(baseline, evidence.atb_value(region, mode, powertrain, base), anchor_atb)
                    anchors.append({**context, "powertrain": powertrain, "family": family, "anchor": anchor, "base_year": base,
                        "anchor_baseline_mj_per_vkm": baseline, "anchor_baseline_analogue": analogue, "atb_anchor_base_mj_per_vkm": anchor_atb,
                        "shared_scale": baseline / anchor_atb, "production_base_mj_per_vkm": consumption_by_year[base][powertrain],
                        "candidate_base_mj_per_vkm": candidate_base, "existing_representative_base_mj_per_vkm": representative_base,
                        "candidate_discontinuity_vs_existing_representative": candidate_base / representative_base - 1,
                        "candidate_change_vs_production": candidate_base / consumption_by_year[base][powertrain] - 1, "omega_share": shares.get(powertrain, 0.0)})
    return pd.DataFrame(output), pd.DataFrame(anchors)


def _raw_sources(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], distributions: pd.DataFrame) -> pd.DataFrame:
    rules = load_harmonization_rules(bundle, "efficiencies")
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    contract = atb_rules["components"]["phev_efficiency"]["future_nrcan_phev_contract"]
    rows = []
    for keys, values in distributions.groupby(["vehicle_group", "atb_class", "rating_class", "powertrain", "year", "native_unit"]):
        group, atb_class, rating_class, powertrain, year, unit = keys
        if powertrain.startswith("phev"):
            for component, field, native_unit, basis in (
                ("CS", contract["combined_cs_output_numeric_field"], "L/100 km (CS)", "Charge-sustaining fuel only; no utility weighting"),
                ("CD", contract["combined_cd_output_numeric_field"], "Le/100 km (CD)", "Charge-depleting total equivalent rating; may include liquid fuel; no utility weighting"),
            ):
                rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                    "year": year, "value": values[field].mean(), "units": native_unit, "series": f"NRCan {component} equal catalogue-row mean",
                    "native_basis": basis})
        else:
            rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                "year": year, "value": values.native_consumption.mean(), "units": unit, "series": "NRCan equal catalogue-row mean",
                "native_basis": "Source combined rating"})
    phev = inputs["atb_phev"].copy()
    phev["trajectory"] = phev.trajectory.replace(atb_rules["components"]["phev_efficiency"]["reconciliation"]["scenario_aliases"])
    selected = select_atb_consumption(inputs["atb_vehicles"], phev, trajectory=configured_trajectory(bundle), rules=rules, conversions=load_conversion_factors(bundle))
    memberships = inputs["atb"][["mode", "vehicle_class"]].drop_duplicates()
    for group, mode in VIEW_MODES.items():
        classes = set(memberships.loc[memberships["mode"].eq(mode), "vehicle_class"])
        for item in selected.loc[selected.vehicle_class.isin(classes)].itertuples(index=False):
            rows.append({"vehicle_group": group, "atb_class": item.vehicle_class, "source_class": item.vehicle_class, "powertrain": item.powertrain,
                "year": item.year, "value": item.source_value, "units": item.source_unit, "series": "ATB native input",
                "native_basis": "Source utility-weighted equivalent energy" if item.powertrain.startswith("phev") else "Source equivalent-gallon fuel economy"})
            if item.powertrain.startswith("phev"):
                components = phev.loc[phev.trajectory.eq(configured_trajectory(bundle)) & phev.year.eq(item.year)
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
    rules = load_harmonization_rules(bundle, "efficiencies")
    relationships = inputs["relationships"].loc[inputs["relationships"]["mode"].isin(VIEW_MODES.values()), ["tech", "mode", "powertrain"]].drop_duplicates()
    selected = inputs["final_efficiency"].merge(relationships, on="tech", validate="many_to_one")
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    selected = selected.loc[selected.region.eq(rules["region_output_map"].get(region, region)) & selected["mode"].isin(VIEW_MODES.values())].copy()
    selected["existing"] = selected.tech.str.endswith(rules["existing_suffix"])
    selected["year"] = [max(bundle.scenario.periods.historical_years(int(v))) if existing else bundle.scenario.periods.projection_year(int(v), legacy_at_end=True)
                        for v, existing in zip(selected.vintage, selected.existing, strict=True)]
    selected["time_basis"] = np.where(selected.existing, "Mean annual service efficiency; plotted at last source year", "Future interval-end source year; vintage retained separately")
    selected["vehicle_group"] = selected["mode"].map({mode: group for group, mode in VIEW_MODES.items()})
    selected["atb_class"], selected["resolution"] = COHORT, "Actual production aggregation"
    selected["stage"], selected["series"], selected["metric"] = "Indexed bridge", "Final prepared period CSV", "service_efficiency"
    selected["value"] = selected.efficiency
    return selected


def _benchmark(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pieces = []
    for name, series in (("rating_weights", "Promoted catalogue / class weights"), ("benchmark_weighted", "Diagnostic MTO weighting / catalogue fallback")):
        for item in _rating_series(inputs[name]).itertuples(index=False):
            support = inputs[name].loc[inputs[name].ceud_class.eq(item.ceud_class)
                                       & inputs[name].powertrain.eq(item.powertrain) & inputs[name].year.eq(item.year)]
            pieces.append({"ceud_class": item.ceud_class, "year": item.year, "powertrain": item.powertrain, "series": series,
                           "value": item.value, "units": "MJ/vkm", "powertrain_family": _family(item.powertrain),
                           "aggregation_method": " | ".join(sorted(support.aggregation_method.unique())),
                           "cohort_matched_stock": float(support.cohort_matched_stock.max()),
                           "calibration_observation": item.year <= bundle.scenario.periods.base_year})
    return pd.DataFrame(pieces)


def build_diagnostic_frames(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> tuple[dict, dict]:
    """Calculate review alternatives using the recorded production contracts."""
    rules = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    phev_index = phev_historical_index_mode(load_harmonization_rules(bundle, "efficiencies"))
    comparison_index = "CS" if phev_index == "CD" else "CD"
    distribution, distribution_summary = rating_distributions(bundle, inputs["ratings"])
    bridge, anchors = _bridge_rows(bundle, inputs)
    bridge = pd.concat([bridge, _published_periods(bundle, inputs)], ignore_index=True)
    bridge["powertrain_family"] = bridge.powertrain.map(_family)
    coverage = inputs["benchmark_matches"].groupby("match_status").mto_fit_active_stock.sum()
    frames = {"raw_sources": _raw_sources(bundle, inputs, distribution), "rating_distribution": distribution,
        "rating_summary": distribution_summary, "bridge": bridge, "anchor_comparison": anchors,
        "omega_shares": inputs["omega_buckets"].copy(), "mto_benchmark": _benchmark(bundle, inputs)}
    summary = {
        "region": rules["region"], "base_year": bundle.scenario.periods.base_year, "bridge_years": rules["bridge_years"],
        "anchor_powertrains": rules["anchor_powertrains"], "vehicle_groups": list(VIEW_MODES),
        "population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        "atb_trajectory": bundle.scenario.efficiencies.atb_trajectory,
        "historical_index_fuels": load_harmonization_rules(bundle, "efficiencies")["historical_index_fuels"],
        "available_rating_years": sorted(int(y) for y in distribution.year.unique()),
        "benchmark_stock_dispositions": {str(key): float(value) for key, value in coverage.items()},
        "omega_model_years": sorted(int(y) for y in inputs["omega_records"].model_year.unique()),
        "phev_historical_index": phev_index,
        "phev_index_comparison_coverage": [
            {"vehicle_group": group, "atb_class": atb_class, "powertrain": powertrain,
             "component": comparison_index,
             "base_year_observed": bool(values.year.eq(bundle.scenario.periods.base_year).any()),
             "observed_history_years": sorted(int(year) for year in values.loc[values.year.le(bundle.scenario.periods.base_year), "year"]),
             "unsupported_bridge_years": sorted(set(range(int(rules["bridge_years"][0]), bundle.scenario.periods.base_year + 1)) - set(values.year))}
            for (group, atb_class, powertrain), values in bridge.loc[bridge.series.eq("NRCan CS fuel-only aggregate" if comparison_index == "CS" else "NRCan CD equivalent-energy aggregate")].groupby(["vehicle_group", "atb_class", "powertrain"])
        ],
        "limitations": [
            "MTO make/model/year keys identify reviewed vehicle classes; powertrain and PEV range are unobserved. Exact-key weighting remains diagnostic only.",
            "Catalogue rows are rated variants, not equal registered stocks. Equal-row and equal-distinct-model means are exposed; only the former is promoted.",
            f"This recorded build uses NRCan PHEV {phev_index} consumption for historical indices, not total-energy levels. PHEV levels use utility-weighted ATB fuel HHV plus electricity.",
            "PHEV CS fuel HHV and CD gasoline-equivalent curves are operating-mode rates before utility weighting; neither is NRCan annual total energy or a measured electrical-input share.",
            f"The diagnostic {comparison_index} comparison uses ATB total(base) times the observed {comparison_index} mean(year)/{comparison_index} mean(base), requires an observed base-year cohort and omits unsupported years. The recorded build's {phev_index}/analogue history remains authoritative for its production curves.",
            "Detailed ATB-class trajectories are diagnostic recalculations. Final CSV values retain actual production aggregation and vintage timing.",
            "Detailed classes inherit parent-mode CEUD loads and broad car/light-truck OMEGA range shares; these are not class-specific observations.",
            "Medium/heavy history applies coarse CEUD fuel indices to ATB vocation levels; it does not observe fine-class cohort efficiencies.",
            "Post-base-year NRCan ratings are holdouts and never calibrate the recorded production bridge.",
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
