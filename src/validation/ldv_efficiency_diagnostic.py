"""Recorded road-efficiency sources, aggregation distributions and bridge review.

Loading reads a successful build's evidence snapshot, without ETL or downloads.
Detailed-class recalculations are diagnostic; displayed composition follows production.
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
from parameterization.build_efficiencies import aggregate_consumption_period, combine_range_observations
from parameterization.ceud_inputs import extend_ceud_to_base_year
from parameterization.road_aggregation import nrcan_to_nlr_map
from parameterization.road_efficiencies import ProjectionAnchorUnavailable, RoadEfficiencyEvidence, ceud_series, observed_value, positive, rating_class_statistics, select_atb_consumption
from parameterization.vehicle_reference import load_vehicle_reference, reference_mto_mapping
from utils import (ConfigBundle, file_sha256, load_conversion_factors, load_harmonization_rules, load_yaml,
                   resolve_artifact_path, resolve_input_path, resolve_repo_path, write_dataframe_atomic, write_text_atomic)
from validation.config_models import PathsConfig
from validation.ldv_composition_benchmark import build_composition_benchmark, collect_composition_benchmark_inputs


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


def composition_view_settings(bundle: ConfigBundle) -> dict:
    """Read live display choices only; captured evidence and model rules stay immutable."""
    paths = PathsConfig.model_validate(load_yaml(bundle.repo_root / "config/paths.yaml"))
    rules_path = resolve_repo_path(bundle.repo_root, paths.config.parameters) / "rules.yaml"
    rules = load_yaml(rules_path)["parameterization"]["ldv_efficiency_diagnostic"]
    return {"years": rules["composition_years"], "top_size_classes": rules["composition_top_size_classes"],
        "distribution_years": list(range(rules["minimum_model_year"], rules["distribution_year_end"] + 1, rules["distribution_year_step"])),
        "class_order": rules["size_class_order"],
        "grid_plot_width": rules["grid_plot_width"], "line_plot_width": rules["line_plot_width"],
        "heatmap_plot_width": rules["heatmap_plot_width"],
        "native_year_start": rules["native_year_start"], "bridge_years": rules["bridge_years"]}


def supported_display_years(frame: pd.DataFrame, years: list[int]) -> list[int]:
    """Trim unsupported ends, retaining missing observations inside a panel's span."""
    available = pd.to_numeric(frame.year, errors="coerce")
    available = available.loc[np.isfinite(available)]
    return [year for year in years if not available.empty and available.min() <= year <= available.max()]


def composition_class_columns(diagnostic: LDVEfficiencyDiagnostic, vehicle_group: str, view: dict) -> pd.DataFrame:
    """Arrange every captured class by nominal size/body; a limit remains opt-in."""
    weights = diagnostic.inputs["rating_class_weights"]
    ceud_class = {"Cars": "Car", "Light trucks": "Light Truck"}[vehicle_group]
    pooled = weights.loc[weights.weight_basis.eq("all_vintages") & weights.nrcan_ceud_class.eq(ceud_class)]
    latest = pooled.loc[pooled.report_year.eq(pooled.report_year.max())]
    if latest.nrcan_vehicle_class.duplicated().any():
        raise ValueError("Duplicate captured MTO class shares in the latest stock snapshot")
    selected = latest.sort_values(["aggregation_weight", "nrcan_vehicle_class"], ascending=[False, True]).copy()
    limit = view["top_size_classes"]
    if limit is not None:
        selected = selected.head(limit).copy()
    else:
        # A historical catalogue body (e.g. unsplit SUVs) can have no current
        # stock-share row. Keep its column without inventing a stock weight.
        distribution = diagnostic.frames["rating_distribution"]
        available = set(distribution.loc[distribution.vehicle_group.eq(vehicle_group), "rating_class"])
        missing = sorted(available - set(selected.nrcan_vehicle_class))
        if missing:
            extra = pd.DataFrame({"nrcan_vehicle_class": missing, "report_year": latest.report_year.max(),
                                  "nrcan_ceud_class": ceud_class, "weight_basis": "all_vintages"})
            selected = pd.concat([selected, extra], ignore_index=True)
    order = {name: index for index, name in enumerate(view["class_order"][vehicle_group])}
    return selected.assign(display_order=selected.nrcan_vehicle_class.map(order).fillna(len(order))).sort_values(
        ["display_order", "nrcan_vehicle_class"], kind="stable").drop(columns="display_order")


def practical_axis(values: pd.Series) -> tuple[list[float], list[float]]:
    """Four evenly spaced, readable ticks covering one powertrain's full support."""
    finite = pd.to_numeric(values, errors="coerce")
    finite = finite.loc[np.isfinite(finite)]
    if finite.empty:
        raise ValueError("A plotted axis requires finite observations")
    lower, upper = float(finite.min()), float(finite.max())
    if lower == upper:
        padding = max(abs(lower) * .05, .01)
        lower, upper = max(0., lower - padding), upper + padding
    exponent = int(np.floor(np.log10((upper - lower) / 3)))
    candidates = []
    for power in range(exponent - 1, exponent + 3):
        for multiplier in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8):
            step = multiplier * 10. ** power
            for origin_step in (step, step / 2):
                start = np.floor(lower / origin_step) * origin_step
                # Native values with an integer-scale span keep integer ticks;
                # smaller MJ/vkm ranges still get readable decimal increments.
                if lower >= 1 and step >= 1 and not np.isclose(start, round(start)):
                    continue
                if start + 3 * step >= upper - step * 1e-10:
                    candidates.append((step, start))
    step, start = min(candidates)
    ticks = [float(f"{start + index * step:.12g}") for index in range(4)]
    # Small mark clearance is distinct from the readable tick extent.
    return [max(0., ticks[0] - step * .03), ticks[-1] + step * .03], ticks


def nrcan_anchor_endpoints(diagnostic: LDVEfficiencyDiagnostic, frame: pd.DataFrame) -> pd.DataFrame:
    """Identify observed trajectory endpoints supporting the recorded cohort anchor.

    The labelled source observation and the stock-weighted multi-year anchor are
    different quantities. Configured analogues and PHEV CD-only curves are excluded.
    """
    result = frame.copy()
    result["nrcan_anchor_window"] = ""
    result["nrcan_anchor_consumption"] = np.nan
    anchors = diagnostic.frames["projection_anchors"]
    for (group, atb_class), panel in result.groupby(["vehicle_group", "atb_class"], dropna=False):
        recorded = anchors.loc[anchors.vehicle_group.eq(group) & anchors.atb_class.eq(atb_class)]
        recorded = recorded.loc[recorded.source_treatments.eq("NRCan_observed_class_weighted")].drop_duplicates("anchor_powertrain")
        for item in recorded.itertuples(index=False):
            years = {int(year.strip()) for year in str(item.source_years).split("|")}
            eligible = panel.loc[panel.powertrain.eq(item.anchor_powertrain)
                & panel.series.eq("NRCan size-class aggregate") & panel.year.isin(years)]
            if eligible.empty:
                continue
            index = eligible.year.idxmax()
            window = str(item.source_year_start) if item.source_year_start == item.source_year_end else f"{item.source_year_start}–{item.source_year_end}"
            result.loc[index, "nrcan_anchor_window"] = window
            result.loc[index, "nrcan_anchor_consumption"] = item.anchor_consumption_mj_per_vkm
    return result


def composition_heatmap_data(diagnostic: LDVEfficiencyDiagnostic, vehicle_group: str, family: str, *,
                             atb_class: str = COHORT, years: tuple[int, int] | None = None) -> pd.DataFrame:
    """Compare common total-energy contracts, without mixing native PHEV CD rates."""
    frame = diagnostic.frames["composition_comparison"]
    selected = frame.loc[frame.vehicle_group.eq(vehicle_group) & frame.powertrain_family.eq(family)].copy()
    if years is not None:
        selected = selected.loc[selected.year.between(*years)].copy()
    if atb_class != COHORT:
        mapping = diagnostic.frames["rating_summary"][["rating_class", "atb_class"]].drop_duplicates()
        selected = selected.merge(mapping, on="rating_class", how="left", validate="many_to_one")
        selected = selected.loc[selected.atb_class.eq(atb_class)].copy()
    if selected.duplicated(["year", "rating_class", "powertrain"]).any():
        raise ValueError("Duplicate class/year/powertrain consumption comparison")
    selected["delta_mj_per_vkm"] = selected.catalogue_mj_per_vkm - selected.production_mj_per_vkm
    selected["delta_fraction"] = selected.delta_mj_per_vkm / selected.catalogue_mj_per_vkm.where(selected.catalogue_mj_per_vkm.ne(0))
    return selected


def collect_efficiency_inputs(bundle: ConfigBundle) -> dict[str, Path]:
    """Identify the actual data captured by an explicit diagnostic preset build."""
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    paths = {name: resolve_artifact_path(bundle, "efficiencies_interim", efficiency["files"][name])
             for name in ("ratings", "rating_weights", "atb", "annual", "periods", "loads", "relationships",
                          "composition_observations", "composition_variants", "composition_matches", "composition_coverage", "catalogue_rating_weights")}
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
    paths.update(collect_composition_benchmark_inputs(bundle))
    return paths


def regional_ratings(bundle: ConfigBundle, frame: pd.DataFrame) -> pd.DataFrame:
    """Select the recorded recipient region before pooling annual cohorts."""
    if "region" not in frame:
        return frame.copy()
    region = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"]
    return frame.loc[frame.region.eq(region)].copy()


def distribution_metric_rows(frame: pd.DataFrame, metric: str) -> tuple[pd.DataFrame, int]:
    """Expose native displacement/power without inventing absent engine values."""
    metrics = {
        "Consumption": ("native_consumption", None),
        "Engine size": ("Engine size (L)", "Engine displacement (L)"),
        "Motor power": ("Motor (kW)", "Electric motor power (kW)"),
    }
    if metric not in metrics:
        raise ValueError(f"Unsupported NRCan distribution metric: {metric!r}")
    field, unit = metrics[metric]
    output = frame.copy()
    values = pd.to_numeric(output.get(field, pd.Series(np.nan, index=output.index)), errors="coerce")
    valid = values.notna() & np.isfinite(values) & values.gt(0)
    omitted = int((~valid).sum())
    output = output.loc[valid].copy()
    output["native_consumption"] = values.loc[valid]
    if unit is not None:
        output["native_unit"] = unit
    return output, omitted


def _family(powertrain: str) -> str:
    return "bev" if powertrain.startswith("bev") else "phev" if powertrain.startswith("phev") else powertrain


def rating_distributions(bundle: ConfigBundle, classified: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Expose native trim rows and their canonical family-median hierarchy."""
    from parameterization.rating_composition import catalogue_family_labels, distinct_catalogue_variants

    required = {"included", "year", "rating_class", "powertrain", "Make", "Model", "native_consumption", "native_unit", "consumption_mj_per_vkm"}
    if required - set(classified):
        raise ValueError(f"Efficiency diagnostic ratings missing columns: {sorted(required - set(classified))}")
    rules = load_harmonization_rules(bundle, "efficiencies")
    frame = catalogue_family_labels(classified.loc[classified.included], rules["ratings"]["composition"])
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    frame["atb_class"] = frame.rating_class.map(nrcan_to_nlr_map(mapping)).fillna("Unresolved source class")
    frame["vehicle_group"] = frame.atb_class.map(mapping["target_to_ceud"]).map({"Car": "Cars", "Light Truck": "Light trucks"}).fillna("Light trucks")
    frame["make_model"] = frame.family_make + " / " + frame.family_model
    frame["powertrain_family"] = frame.powertrain.map(_family)
    if {"component", "source_row"} <= set(frame):
        variants = distinct_catalogue_variants(classified, rules["ratings"]["composition"])
        frame = frame.merge(variants[["component", "source_row", "rating_variant_id"]],
                            on=["component", "source_row"], validate="one_to_one")
    frame["calibration_observation"] = frame.year.le(min(bundle.scenario.periods.base_year, rules["projection_reference_year"]))
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
    for statistic in ("mean", "median"):
        values = rating_class_statistics(classified, rules=rules, statistic=statistic)
        values = values[keys + ["native_consumption"]].rename(
            columns={"native_consumption": "equal_distinct_model_" + statistic})
        summary = summary.merge(values, on=keys, validate="one_to_one")
    summary["powertrain_family"] = summary.powertrain.map(_family)
    summary["calibration_observation"] = summary.year.le(min(bundle.scenario.periods.base_year, rules["projection_reference_year"]))
    return frame, summary


def _joined(values: pd.Series) -> str:
    return " | ".join(sorted({str(value) for value in values if pd.notna(value) and str(value)}))


def composition_model_evidence(bundle: ConfigBundle, classified: pd.DataFrame) -> pd.DataFrame:
    """One median of distinct trims per canonical make/model family and cohort."""
    from parameterization.rating_composition import distinct_catalogue_variants

    rows, _ = rating_distributions(bundle, classified)
    rules = load_harmonization_rules(bundle, "efficiencies")
    variants = distinct_catalogue_variants(classified, rules["ratings"]["composition"])
    variants = variants.drop_duplicates("rating_variant_id").copy()
    keys = ["year", "rating_class", "powertrain", "native_unit", "family_make", "family_model"]
    rows["rating_row_id"] = (rows.component.astype(str) + ":" + rows.source_row.astype(str)
        if {"component", "source_row"} <= set(rows) else "")
    aggregations = {
        "family_median_mj_per_vkm": ("consumption_mj_per_vkm", "median"),
        "model_min_mj_per_vkm": ("consumption_mj_per_vkm", "min"),
        "model_max_mj_per_vkm": ("consumption_mj_per_vkm", "max"),
        "model_native_consumption": ("native_consumption", "median"),
        "distinct_trims": ("rating_variant_id", "nunique"),
    }
    for column in ("Engine size (L)", "Motor (kW)", "range_km"):
        if column in variants:
            aggregations[column] = (column, "median")
    result = variants.groupby(keys, dropna=False, as_index=False, sort=True).agg(**aggregations)
    audit = rows.groupby(keys, dropna=False, as_index=False, sort=True).agg(
        catalogue_rows=("native_consumption", "size"), rating_row_ids=("rating_row_id", _joined),
        native_model_labels=("Model", _joined), vehicle_group=("vehicle_group", "first"), atb_class=("atb_class", "first"))
    result = result.merge(audit, on=keys, validate="one_to_one")
    result["Make"], result["Model"] = result.family_make, result.family_model
    result["make_model"] = result.family_make + " / " + result.family_model
    result["powertrain_family"] = result.powertrain.map(_family)
    result["calibration_observation"] = result.year.le(min(bundle.scenario.periods.base_year, rules["projection_reference_year"]))
    return result


def composition_family_evidence(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Reconcile each selected family's mass and its compatible rated variants."""
    variants = regional_ratings(bundle, inputs["composition_variants"])
    columns = ["region", "year", "rating_class", "powertrain", "family_make", "family_model", "family_label",
               "family_count", "family_weight", "family_median_mj_per_vkm", "family_native_consumption",
               "native_unit", "variant_count", "geography", "source_kind", "empirical_observation_ids",
               "production_mj_per_vkm", "production_native_consumption", "aggregation_method"]
    if variants.empty:
        return pd.DataFrame(columns=columns)
    required = {"year", "rating_class", "powertrain", "empirical_family_make", "empirical_family_model",
                "empirical_observation_id", "allocated_count", "variant_weight", "consumption_mj_per_vkm",
                "native_consumption", "native_unit", "component", "source_row"}
    if missing := required - set(variants):
        raise ValueError(f"Recorded composition variants are missing columns: {sorted(missing)}")
    observations = regional_ratings(bundle, inputs["composition_observations"])
    metadata = [field for field in ("empirical_observation_id", "province", "source_kind", "pool_geography",
                                    "pool_provinces", "range_assignment", "identity_evidence_methods",
                                    "identity_support_reason", "composition_basis", "source_model_year_min",
                                    "source_model_year_max", "source_model_years") if field in observations]
    variants = variants.drop(columns=[field for field in metadata if field != "empirical_observation_id" and field in variants])
    variants = variants.merge(observations[metadata].drop_duplicates("empirical_observation_id"),
                              on="empirical_observation_id", how="left", validate="many_to_one")
    variants["family_make"], variants["family_model"] = variants.empirical_family_make, variants.empirical_family_model
    keys = ["year", "rating_class", "powertrain", "family_make", "family_model", "native_unit"]
    records = []
    for key, group in variants.groupby(keys, dropna=False, sort=True):
        weight = float(group.variant_weight.sum())
        if not np.isfinite(weight) or weight <= 0:
            raise ValueError("Selected empirical families require positive finite contribution")
        item = dict(zip(keys, key, strict=True))
        native_weights = group.variant_weight
        if "native_median_weight" in group:
            contributions = group.groupby("empirical_observation_id").variant_weight.sum()
            native_weights = group.empirical_observation_id.map(contributions) * group.native_median_weight
        item.update(region=_joined(group.region) if "region" in group else load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")["region"],
            family_label=f"{item['family_make']} / {item['family_model']}",
            family_count=float(group.allocated_count.sum()), family_weight=weight,
            family_median_mj_per_vkm=float((group.consumption_mj_per_vkm * group.variant_weight).sum(min_count=1) / weight),
            family_native_consumption=float((group.native_consumption * native_weights).sum(min_count=1) / weight),
            variant_count=int(group.rating_variant_id.nunique()) if "rating_variant_id" in group else len(group[["component", "source_row"]].drop_duplicates()),
            geography=_joined(group.province) if "province" in group else "",
            source_kind=_joined(group.source_kind) if "source_kind" in group else "",
            empirical_observation_ids=_joined(group.empirical_observation_id))
        for field in ("pool_geography", "pool_provinces", "range_assignment", "catalogue_powertrain",
                      "identity_evidence_methods", "identity_support_reason", "composition_basis", "source_model_years"):
            if field in group:
                item[field] = _joined(group[field])
        for field in ("source_model_year_min", "source_model_year_max"):
            if field in group:
                item[field] = group[field].min() if field.endswith("min") else group[field].max()
        records.append(item)
    result = pd.DataFrame(records)
    audit = regional_ratings(bundle, inputs["rating_weights"])
    fields = [field for field in ("year", "rating_class", "powertrain", "native_class_mean", "class_mean_mj_per_vkm", "aggregation_method",
              "weight", "identity_supported_resolved_share", "minimum_resolved_count_share", "identity_coverage_met") if field in audit]
    audit = audit[fields].rename(columns={"native_class_mean": "production_native_consumption", "class_mean_mj_per_vkm": "production_mj_per_vkm"})
    return result.merge(audit, on=["year", "rating_class", "powertrain"], how="left", validate="many_to_one")


def composition_detail_frames(diagnostic: LDVEfficiencyDiagnostic, kind: str, *,
                              year: int | None = None, rating_class: str | None = None,
                              powertrain: str | None = None) -> dict[str, pd.DataFrame]:
    """Filter recorded source pools and exact matching rows without recomputation."""
    if kind not in {"izev", "mto"}:
        raise ValueError(f"Unknown composition source: {kind!r}")
    observations = diagnostic.frames["composition_observations"]
    observations = observations.loc[observations.source_kind.eq(kind)].copy()
    if year is not None:
        observations = observations.loc[observations.year.eq(year)]
    unresolved = observations.loc[~observations.match_status.fillna("").str.startswith("resolved")].copy()
    if rating_class is not None:
        compatible_class = observations.get("candidate_rating_classes", pd.Series("", index=observations.index)).fillna("").map(
            lambda value: rating_class in [part.strip() for part in value.split("|")]).astype(bool)
        observations = observations.loc[observations.rating_class.eq(rating_class) | compatible_class]
    if powertrain is not None:
        def in_powertrain_context(frame: pd.DataFrame) -> pd.Series:
            assigned = frame.resolved_powertrain.fillna("")
            candidates = frame.candidate_powertrains.fillna("")
            compatible = candidates.eq("") | candidates.map(
                lambda value: powertrain in [part.strip() for part in value.split("|")]).astype(bool)
            if kind == "izev":
                compatible &= frame.observed_powertrain.eq(_family(powertrain))
            return assigned.eq(powertrain) | (assigned.eq("") & compatible)

        observations = observations.loc[in_powertrain_context(observations)]
        unresolved = unresolved.loc[in_powertrain_context(unresolved)]
    observation_ids = set(observations.empirical_observation_id)
    matches = diagnostic.frames["composition_matches"]
    matches = matches.loc[matches.empirical_observation_id.isin(observation_ids)].copy()
    allocations = diagnostic.frames["composition_variants"]
    individual = []
    for observation_id, group in allocations.loc[allocations.empirical_observation_id.isin(observation_ids)].groupby("empirical_observation_id"):
        weight = group.variant_weight.sum()
        if weight <= 0:
            continue
        native_weights = group.native_median_weight * weight if "native_median_weight" in group else group.variant_weight
        individual.append({"empirical_observation_id": observation_id,
            "selected_family_native_consumption": float((group.native_consumption * native_weights).sum(min_count=1) / weight),
            "selected_family_mj_per_vkm": float((group.consumption_mj_per_vkm * group.variant_weight).sum(min_count=1) / weight),
            "selected_family_weight": float(weight)})
    if individual:
        values = pd.DataFrame(individual)
        observations = observations.merge(values, on="empirical_observation_id", how="left", validate="one_to_one")
        matches = matches.merge(values, on="empirical_observation_id", how="left", validate="many_to_one")
    aggregate = regional_ratings(diagnostic.bundle, diagnostic.inputs["rating_weights"])
    aggregate = aggregate[["year", "rating_class", "powertrain", "native_class_mean", "class_mean_mj_per_vkm"]].rename(columns={
        "powertrain": "resolved_powertrain", "native_class_mean": "aggregate_native_consumption", "class_mean_mj_per_vkm": "aggregate_mj_per_vkm"})
    observations = observations.merge(aggregate, on=["year", "rating_class", "resolved_powertrain"], how="left", validate="many_to_one")
    matches = matches.merge(observations[["empirical_observation_id", "aggregate_native_consumption", "aggregate_mj_per_vkm"]],
                             on="empirical_observation_id", how="left", validate="many_to_one")
    families = diagnostic.frames["composition_families"]
    families = families.loc[families.source_kind.eq(kind)].copy()
    coverage = diagnostic.frames["composition_coverage"].copy()
    coverage = coverage.loc[coverage.powertrain.str.startswith(("bev", "phev")) if kind == "izev" else coverage.powertrain.eq("gasoline")]
    class_values = regional_ratings(diagnostic.bundle, diagnostic.inputs["rating_weights"])
    class_fields = [field for field in ("year", "rating_class", "powertrain", "native_class_mean", "class_mean_mj_per_vkm", "weight", "native_unit") if field in class_values]
    class_values = class_values[class_fields].rename(columns={"native_class_mean": "production_native_consumption",
        "class_mean_mj_per_vkm": "production_mj_per_vkm", "weight": "aggregation_weight"})
    coverage = coverage.drop(columns=[field for field in class_values if field not in {"year", "rating_class", "powertrain"} and field in coverage])
    coverage = coverage.merge(class_values, on=["year", "rating_class", "powertrain"], how="left", validate="many_to_one")
    if "catalogue_rating_weights" not in diagnostic.inputs:
        raise ValueError("Composition details require the recorded catalogue-median baseline")
    catalogue = regional_ratings(diagnostic.bundle, diagnostic.inputs["catalogue_rating_weights"])
    catalogue_fields = [field for field in ("year", "rating_class", "powertrain", "native_class_mean", "class_mean_mj_per_vkm",
                        "observations", "distinct_models") if field in catalogue]
    catalogue = catalogue[catalogue_fields].rename(columns={"native_class_mean": "catalogue_native_consumption",
        "class_mean_mj_per_vkm": "catalogue_mj_per_vkm", "observations": "catalogue_rows", "distinct_models": "catalogue_models"})
    coverage = coverage.merge(catalogue, on=["year", "rating_class", "powertrain"], how="left", validate="many_to_one")
    if {"year", "rating_class", "powertrain", "Make", "Model"}.issubset(allocations):
        identity = (["family_make", "family_model"] if {"family_make", "family_model"} <= set(allocations)
                    else ["Make", "Model"])
        model_counts = allocations.loc[allocations.variant_weight.gt(0), ["year", "rating_class", "powertrain", *identity]].drop_duplicates()
        model_counts = model_counts.groupby(["year", "rating_class", "powertrain"]).size().rename("weighted_catalogue_models")
        coverage = coverage.merge(model_counts, on=["year", "rating_class", "powertrain"], how="left", validate="many_to_one")
        coverage["weighted_catalogue_models"] = coverage.weighted_catalogue_models.fillna(0).astype(int)
    for field, selected in (("year", year), ("rating_class", rating_class), ("powertrain", powertrain)):
        if selected is not None:
            families = families.loc[families[field].eq(selected)]
            coverage = coverage.loc[coverage[field].eq(selected)]
    return {"observations": observations, "matches": matches, "families": families, "coverage": coverage,
            "unresolved": unresolved}


def _mto_mapping_year_review(diagnostic: LDVEfficiencyDiagnostic) -> pd.DataFrame:
    """Expose every positive MTO observation excluded before ratings matching.

    Other-year identities and mixed-scope keys provide review context only. The
    native rows remain distinct, including passenger/commercial observations with
    the same make/model/year; counts are never deduplicated by visible labels.
    """
    mapped = getattr(diagnostic, "inputs", {}).get("benchmark_mto_mapped", pd.DataFrame())
    if mapped.empty:
        return pd.DataFrame()
    rules = load_harmonization_rules(diagnostic.bundle, "road_aggregation")
    crosswalk = reference_mto_mapping(
        load_vehicle_reference(diagnostic.bundle), rules,
        load_harmonization_rules(diagnostic.bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"],
    )
    reviewed = crosswalk.loc[crosswalk.entry_type.eq("mto_crosswalk")
        & crosswalk.mapping_status.isin(rules["accepted_mapping_statuses"])].copy()
    keys = ["mto_make_code", "mto_model_code"]
    for field in keys:
        reviewed[field] = reviewed[field].astype(str).str.strip().str.upper()
    groups = {key: group for key, group in reviewed.groupby(keys, sort=False)}
    pending = mapped.loc[~mapped.mapping_accepted.eq(True)
                         & pd.to_numeric(mapped.FIT_ACTIVE, errors="raise").gt(0)].copy()
    records = []
    for index, row in pending.iterrows():
        key = (str(row.MAKE).strip().upper(), str(row.MODEL).strip().upper())
        family = groups.get(key)
        outcome = str(row.mapping_outcome)
        status = "missing_reviewed_mapping_identity"
        reason = "No reviewed mapping establishes this MTO make–model key's identity and scope."
        current = pd.DataFrame()
        if outcome == "mapped_non_ldv":
            status = "mapped_non_ldv"
            reason = "The applicable reviewed mapping identifies a vehicle outside the light-duty estimator."
        elif outcome == "out_of_scope":
            status = "outside_mapping_scope"
            reason = "The source model year is outside the configured reviewed-mapping scope."
        elif family is not None:
            applicable = family.model_year_from.le(row.MODEL_YEAR) & family.model_year_to.ge(row.MODEL_YEAR)
            if not applicable.any():
                status = "missing_applicable_reviewed_mapping_year"
                reason = "Reviewed key exists, but no reviewed interval covers this vehicle model year."
            else:
                current = family.loc[applicable]
                scopes = set(current.vehicle_scope)
                if "identity_unresolved" in scopes:
                    status = "unresolved_mapping_identity"
                    reason = "Reviewed evidence does not establish this source key's canonical identity; no model, class or propulsion allocation is accepted."
                elif scopes == {"ldv_class_unresolved"}:
                    status = "unresolved_reviewed_size_class"
                    reason = "Reviewed evidence establishes the light-duty family, but not a defensible fine size class."
                elif "scope_unresolved" in scopes:
                    status = "unresolved_reviewed_vehicle_scope"
                    reason = "Reviewed evidence identifies the broad vehicle family, but not its light-duty or numbered-truck scope."
                else:
                    status = "unresolved_mapping_identity"
                    reason = "A reviewed interval exists, but this captured observation has no accepted mapping; reconcile its mapping outcome."
                notes = _joined(current.review_notes) if "review_notes" in current else ""
                if notes and status in {"unresolved_mapping_identity", "unresolved_reviewed_size_class", "unresolved_reviewed_vehicle_scope"}:
                    reason += " " + notes
        record = row.to_dict()
        record.update(empirical_observation_id=f"mto_mapping:{row.report_year}:{index}",
            source_kind="mto", province="ON", year=int(row.MODEL_YEAR), source_count=float(row.FIT_ACTIVE),
            composition_basis="observed_model_year_stock", source_model_years=str(int(row.MODEL_YEAR)),
            raw_make=row.MAKE, raw_model=row.MODEL, family_make="", family_model="", rating_class="", resolved_powertrain="",
            selected=False, match_status=status, selection_status="excluded_before_rating_matching", match_reason=reason,
            candidate_families="", candidate_rating_classes="", candidate_powertrains="", reviewed_mapping_year_intervals="", reviewed_identity="",
            identity_exclusion_reason=reason,
            identity_support_reason="Other reviewed intervals are context only; no target-year class or propulsion is inferred.")
        if family is not None:
            record.update(candidate_families=_joined(family.canonical_make.fillna("").astype(str) + " / " + family.canonical_model.fillna("").astype(str)),
                candidate_rating_classes=_joined(family.nrcan_vehicle_class),
                reviewed_mapping_year_intervals=_joined(family.model_year_from.map(lambda value: str(int(value)))
                    + "–" + family.model_year_to.map(lambda value: str(int(value)))),
                reviewed_mapping_scopes=_joined(family.vehicle_scope))
        if not current.empty and status in {"unresolved_reviewed_size_class", "unresolved_reviewed_vehicle_scope"}:
            record.update(family_make=_joined(current.canonical_make), family_model=_joined(current.canonical_model),
                reviewed_identity=_joined(current.canonical_make.fillna("").astype(str) + " / " + current.canonical_model.fillna("").astype(str)),
                identity_support_reason="The applicable reviewed interval supports this family identity; its size class or vehicle scope remains unresolved.")
        records.append(record)
    return pd.DataFrame(records)


def composition_review_cases(diagnostic: LDVEfficiencyDiagnostic) -> pd.DataFrame:
    """Explain captured applications; source exposure is summarized separately."""
    rows = diagnostic.frames["composition_observations"].copy()
    status = rows.get("match_status", pd.Series("", index=rows.index, dtype=str)).fillna("")
    rows["match_status"] = status
    resolved = status.str.startswith("resolved")
    variants = pd.to_numeric(rows.get("matched_variant_count", pd.Series(0, index=rows.index)), errors="coerce")
    review = rows.loc[~resolved | variants.gt(1)].copy()
    unresolved = ~review.match_status.fillna("").str.startswith("resolved")
    review["review_kind"] = np.where(unresolved, "Unresolved identity / configuration", "Retained family-median representative")
    review["recommended_handling"] = np.where(unresolved,
        "Review identity/class/configuration; retain current selection and cohort-median fallback policies.",
        "Leave as is: one source count uses the median of compatible distinct trims; range uses its own median.")
    if "selected" in review:
        review.loc[~unresolved & ~review.selected.eq(True), "recommended_handling"] = (
            "Retain recorded selection policy; the family median is available but contributes no empirical weight here.")
    missing_years = _mto_mapping_year_review(diagnostic)
    if not missing_years.empty:
        review = pd.concat([review, missing_years], ignore_index=True)
    if review.empty:
        return review
    ratings = getattr(diagnostic, "inputs", {}).get("ratings", pd.DataFrame())
    first_rating_year = pd.to_numeric(ratings.get("year", pd.Series(dtype=float)), errors="coerce").min()
    explanations = [_composition_case_explanation(row, first_rating_year) for row in review.to_dict("records")]
    review["review_kind"] = [item[0] for item in explanations]
    review["review_reason"] = [item[1] for item in explanations]
    review["recommended_handling"] = [item[2] for item in explanations]
    # Regional recipients repeat the same source observation; preserve those
    # applications here and remove repetition only in the exposure summary.
    return review.sort_values("source_count", ascending=False, kind="stable")


def _composition_case_explanation(row: dict, first_rating_year: float) -> tuple[str, str, str]:
    """Use production findings without treating a missing rating as nonexistence."""
    status = str(row.get("match_status", ""))
    specific = row.get("match_reason", row.get("matching_reason", ""))
    specific = str(specific) if pd.notna(specific) else ""
    if status.startswith("resolved"):
        selected = row.get("selected", False)
        handling = ("Leave as is: each source count uses its compatible family's median consumption and median range."
                    if pd.notna(selected) and bool(selected) else
                    "Retain the recorded selection policy; this available family median has no empirical weight here.")
        return "Retained family-median representative", specific or "Family identity is supported; trim composition is unspecified.", handling
    reasons = {
        "outside_supported_program_window": ("Outside the configured iZEV model-year window.", "Retain the configured program window."),
        "unsupported_transaction_type": ("The transaction is outside the configured eligible purchase/lease types.", "Retain the configured transaction eligibility."),
        "unsupported_observed_powertrain": ("The observed propulsion is outside the BEV/PHEV acquisition estimator.", "Retain the propulsion exclusion."),
        "missing_applicable_reviewed_mapping_year": ("No applicable reviewed mapping interval covers this MTO model year.", "Review year-specific identity evidence; do not extend another interval automatically."),
        "missing_reviewed_mapping_identity": ("No reviewed mapping establishes this MTO make–model key's identity and scope.", "Review the raw manufacturer/model code and year evidence before assigning an identity or class."),
        "unresolved_reviewed_size_class": ("Reviewed evidence establishes the light-duty family, but not a defensible fine size class.", "Review the applicable model-year/body size-class evidence; retain the supported family identity without allocating a class."),
        "unresolved_reviewed_vehicle_scope": ("Reviewed evidence identifies a broad family without establishing light-duty or numbered-truck scope.", "Review numbered-family, body and GVWR evidence; similar consumption cannot establish truck scope."),
        "mapped_non_ldv": ("The applicable reviewed mapping identifies a vehicle outside the light-duty estimator.", "Retain the reviewed non-light-duty exclusion."),
        "outside_mapping_scope": ("The source model year is outside the configured reviewed-mapping scope.", "Retain the configured mapping scope; source observations remain intact."),
        "unresolved_mapping_identity": ("The mapping does not establish one supported make–model family and scope.", "Review the mapping's identity evidence and scope before allocation."),
        "ambiguous_model_family": ("More than one make–model family remains compatible with the observation.", "Resolve family identity using year and scope evidence; trim similarity alone is insufficient."),
        "ambiguous_rating_class": ("Eligible configurations retain conflicting size/body classes.", "Review the target-year body/class evidence before choosing a representative."),
        "conflicting_reviewed_rating_class": ("The eligible target-year family class conflicts with the authoritative mapped interval.", "Review the mapping interval and target-year body/class evidence; do not override the reviewed class automatically."),
        "ambiguous_powertrain_evidence": ("Available configuration evidence does not distinguish conventional gasoline from hybrid propulsion.", "Review year/configuration HEV evidence; fuel code alone does not decide propulsion."),
        "unresolved_propulsion_evidence": ("Target-year ratings exist, but complementary configuration evidence does not establish conventional gasoline.", "Review year/configuration HEV evidence and source identity correspondence; X/Z fuel codes alone do not establish propulsion."),
        "ambiguous_powertrain_or_range": ("Compatible candidates retain conflicting propulsion or range evidence.", "Review explicit propulsion/configuration evidence before allocation."),
        "conflicting_or_unverified_configuration": ("The explicit configuration conflicts with available target-year ratings.", "Review the observation's explicit configuration against the captured ratings."),
        "material_consumption_dispersion": ("Compatible configurations vary too widely for the configured family representative.", "Retain the catalogue fallback unless evidence supports a narrower configuration scope."),
        "family_consumption_heterogeneity": ("Compatible configurations vary too widely for the configured family representative.", "Retain the catalogue fallback unless evidence supports a narrower configuration scope."),
        "ineligible_for_estimator": ("The supported vehicle is ineligible for this estimator's propulsion/fuel scope.", "Retain the estimator exclusion."),
        "ineligible_observed_propulsion": ("The supported vehicle is ineligible for this estimator's propulsion/fuel scope.", "Retain the estimator exclusion."),
        "target_year_rating_unavailable": ("The family is supported, but no eligible target-year NRCan rating is available.", "Retain the catalogue fallback; a missing rating does not establish model nonexistence."),
        "unverified_model_family": ("Name similarity finds possible families without establishing identity.", "Review manufacturer, year and scope evidence before assigning the family."),
        "incompatible_model_year": ("Identity evidence places the model/configuration outside this target model year.", "Review the MTO identity/year assignment; preserve the source observation."),
    }
    reason, handling = reasons.get(status, (
        "No eligible target-year NRCan rating matches this observation; catalogue absence alone does not establish nonexistence.",
        "Check the captured target-year catalogue, family identity and estimator eligibility; retain the fallback."))
    if specific:
        reason = specific
    elif status == "unresolved_mapping_identity" and pd.notna(row.get("identity_exclusion_reason")) and row.get("identity_exclusion_reason"):
        reason = str(row["identity_exclusion_reason"])
    elif status == "conflicting_or_unverified_configuration" and row.get("configuration_status"):
        reason += " " + str(row["configuration_status"]).replace("_", " ") + "."
    category = "Actionable matching issue"
    if status in {"outside_supported_program_window", "unsupported_transaction_type", "unsupported_observed_powertrain", "ineligible_for_estimator", "ineligible_observed_propulsion", "mapped_non_ldv", "outside_mapping_scope"}:
        category = "Expected exclusion"
    elif row.get("composition_basis") == "historical_pooled_model_year_window":
        category = "Historical proxy limitation"
        handling = "Retain only target-year-compatible proxy families; this does not diagnose the family's own-year iZEV matching."
    elif (status in {"unmatched_ratings", "target_year_rating_unavailable"}
          and pd.notna(first_rating_year) and row.get("year", first_rating_year) < first_rating_year):
        category = "Historical catalogue limitation"
        reason = f"Target MY precedes the captured NRCan catalogue (first MY {int(first_rating_year)})."
        handling = "Retain the historical coverage limitation and configured fallback."
    return category, reason, handling


def composition_review_summary(review: pd.DataFrame) -> pd.DataFrame:
    """Rank source-family issues without adding repeated target/pool applications.

    Own-year rows identify disjoint source-year counts. Historical proxies are a
    separate population: a pooled count is counted once across its target years,
    never added to own-year counts. Identical native transaction rows have already
    contributed their full counts to each captured source observation.
    """
    if review.empty:
        return review.copy()
    rows = review.copy()
    defaults = {"raw_make": "", "raw_model": "", "province": "", "family_make": "", "family_model": "",
                "observed_powertrain": "", "composition_basis": "", "source_model_years": ""}
    for field, default in defaults.items():
        rows[field] = rows.get(field, pd.Series(default, index=rows.index)).fillna(default)
    rows["exposure_basis"] = np.where(rows.composition_basis.eq("historical_pooled_model_year_window"),
        "Pooled source window; separate from own-year counts", "Own-year source observations")
    keys = ["review_kind", "source_kind", "province", "raw_make", "raw_model", "observed_powertrain", "exposure_basis"]
    records = []
    for key, group in rows.groupby(keys, sort=False, dropna=False):
        item = dict(zip(keys, key, strict=True))
        applications = group.drop_duplicates(["empirical_observation_id", "year"])
        pooled = applications.composition_basis.eq("historical_pooled_model_year_window")
        native = applications.loc[~pooled].drop_duplicates("empirical_observation_id")
        exposure = native.source_count.sum() if not native.empty else applications.source_count.max()
        item.update(source_count_exposure=float(exposure), target_model_years=_review_year_intervals(group.year),
            source_model_years=_joined(group.source_model_years), target_applications=len(applications),
            review_reason=_joined(group.review_reason), recommended_handling=_joined(group.recommended_handling))
        for field in ("reviewed_identity", "candidate_families", "candidate_rating_classes", "reviewed_mapping_year_intervals", "reviewed_mapping_scopes"):
            if field in group:
                item[field] = _joined(group[field].dropna())
        records.append(item)
    return pd.DataFrame(records).sort_values("source_count_exposure", ascending=False, kind="stable").reset_index(drop=True)


def _review_year_intervals(years: pd.Series) -> str:
    ordered = sorted({int(year) for year in years if pd.notna(year)})
    intervals = []
    for year in ordered:
        if intervals and year == intervals[-1][1] + 1:
            intervals[-1][1] = year
        else:
            intervals.append([year, year])
    return ", ".join(str(first) if first == last else f"{first}–{last}" for first, last in intervals)


def _class_rating_audit(bundle: ConfigBundle, audit: pd.DataFrame, atb_class: str) -> pd.DataFrame:
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    audit = regional_ratings(bundle, audit)
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
    ratings = regional_ratings(bundle, inputs["rating_weights"])
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
        projection_reference_year=rules["projection_reference_year"],
        cohort_stock=inputs["annual_cohort_stock"].merge(
            inputs["relationships"].loc[inputs["relationships"].tech.isin(inputs["annual_cohort_stock"].tech),
                                        ["tech", "powertrain"]].drop_duplicates(), on="tech", validate="many_to_one"))


def _rating_series(audit: pd.DataFrame) -> pd.DataFrame:
    data = audit.copy()
    data["mj"] = data.weight * data.class_mean_mj_per_vkm
    return data.groupby(["ceud_class", "powertrain", "year"], as_index=False).agg(value=("mj", lambda s: s.sum(min_count=1)))


def _empirical_class_values(bundle: ConfigBundle, rows: pd.DataFrame, fields: tuple[str, ...],
                            variants: pd.DataFrame | None = None) -> pd.DataFrame:
    """Apply empirical median-support coefficients, with family/cohort medians elsewhere."""
    keys = ["year", "rating_class", "powertrain"]
    rules = load_harmonization_rules(bundle, "efficiencies")
    fallback = rating_class_statistics(rows, rules=rules,
        statistic="median", value_columns=fields)[keys + list(fields)]
    if variants is None or variants.empty:
        return fallback
    variants = regional_ratings(bundle, variants)
    allocations = variants.loc[variants.powertrain.isin(rows.powertrain.unique())].copy()
    if allocations.empty:
        return fallback
    # Source-row allocation remains authoritative even when the same family
    # description matched several variants or several descriptions share a row.
    links = ["component", "source_row"]
    if missing := {*keys, *links, "variant_weight"} - set(allocations):
        raise ValueError(f"Empirical variant evidence is missing columns: {sorted(missing)}")
    allocations = allocations[[*keys, *links, "variant_weight"]].merge(
        rows[links + list(fields)], on=links, how="left", validate="many_to_one", indicator=True)
    if allocations._merge.ne("both").any():
        raise ValueError("Empirical component allocation references a missing NRCan source row")
    sums = allocations.groupby(keys).variant_weight.sum()
    if not np.allclose(sums, 1, rtol=0, atol=1e-9):
        raise ValueError("Empirical component variant weights must sum to one per class cohort")
    for field in fields:
        allocations[field] *= allocations.variant_weight
    empirical = allocations.groupby(keys, as_index=False)[list(fields)].sum(min_count=1)
    fallback = fallback.merge(empirical[keys].assign(empirical=True), on=keys, how="left")
    return pd.concat([fallback.loc[fallback.empirical.isna()].drop(columns="empirical"), empirical], ignore_index=True)


def phev_rating_components(bundle: ConfigBundle, classified: pd.DataFrame, audit: pd.DataFrame,
                           variants: pd.DataFrame | None = None) -> pd.DataFrame:
    """Keep CS and CD operating-mode rates separate while applying recorded class weights."""
    audit = regional_ratings(bundle, audit)
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
    component_means = _empirical_class_values(bundle, rows, fields, variants)
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
    table = ceud.loc[ceud.table_id.eq(road["stock_table"])]
    parameter_view = table
    if bundle.scenario.periods.base_year > table.year.max():
        parameter_view, _ = extend_ceud_to_base_year(
            table, base_year=bundle.scenario.periods.base_year,
            policy=bundle.scenario.sources.nrcan_ceud_beyond_observed,
            cagr_rules=load_harmonization_rules(bundle, "ceud_inputs")["cagr"],
        )
    parts = []
    for fuel in rules["mhdv_fleet_baseline"]["observed_fuels"][mode]:
        label = road["cs_prefix"] + "|" + rules["fuel_series"][fuel]
        series = ceud_series(ceud, road["stock_table"], label, "L/100 km")
        parameter_series = ceud_series(parameter_view, road["stock_table"], label, "L/100 km")
        baseline = observed_value(parameter_series, bundle.scenario.periods.base_year, "value")
        if baseline is None:
            raise ValueError(f"CEUD consumption has no supported {bundle.scenario.periods.base_year} baseline for {mode}/{fuel}")
        series["consumption_index"] = series.value / positive(baseline, "CEUD base-year consumption")
        series["index_reference_year"] = bundle.scenario.periods.base_year
        series["index_reference_status"] = (
            "observed" if series.year.eq(bundle.scenario.periods.base_year).any()
            else bundle.scenario.sources.nrcan_ceud_beyond_observed
        )
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


def _bridge_rows(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], *, recorded_statistic: str | None = None,
                 bridge_years: list[int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    spec = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    rules = load_harmonization_rules(bundle, "efficiencies")
    start, end = map(int, bridge_years if bridge_years is not None else spec["bridge_years"])
    base, region = bundle.scenario.periods.base_year, spec["region"]
    output, anchors = [], []
    periods = bundle.scenario.periods
    recorded_selection = recorded_statistic is None
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
        ceud_history = (harmonised_ceud_history(bundle, inputs["ceud"], mode)
                        if rules["road_classes"][mode]["pathway"] == "mhdv" else None)
        for atb_class in [None, *sorted(native.vehicle_class.unique())]:
            evidence = _evidence(bundle, inputs, mode, atb_class)
            context = {"vehicle_group": group, "mode": mode, "atb_class": atb_class or COHORT,
                       "resolution": "Actual production aggregation" if atb_class is None else "Diagnostic class recalculation"}
            atb_audit = native if atb_class is None else native.loc[native.vehicle_class.eq(atb_class)]
            history = regional_ratings(bundle, inputs["rating_weights"]) if atb_class is None else _class_rating_audit(bundle, inputs["rating_weights"], atb_class)
            if rules["road_classes"][mode]["pathway"] == "ldv":
                weight_class = rules["road_classes"][mode]["weights"] if atb_class is None else atb_class
                selected_history = history.loc[history.ceud_class.eq(weight_class)]
                for item in _rating_series(selected_history).itertuples(index=False):
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing", "NRCan size-class aggregate", item.year,
                        "consumption", item.value, "MJ/vkm", calibration_observation=item.year <= evidence.observation_end_year,
                        treatment="Recorded count-weighted family medians or hierarchical family/cohort median fallback, with MTO size-class weights")
                components = phev_rating_components(bundle, inputs["ratings"], selected_history, inputs.get("composition_variants"))
                for item in components.itertuples(index=False):
                    if item.component != "CD":
                        continue
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing",
                        "NRCan CD component-energy aggregate",
                        item.year, "consumption", item.value, "MJ/vkm", calibration_observation=item.year <= evidence.observation_end_year,
                        component=item.component, utility_weighted=False, energy_basis=item.energy_basis,
                        native_value=item.native_value, native_unit=item.native_unit, native_basis=item.native_basis,
                        treatment="Operating-mode component rate; not annual utility-weighted PHEV total energy")
            for (powertrain, year), values in atb_audit.groupby(["powertrain", "year"]):
                consumption = values.weighted_consumption.sum() if atb_class is None else values.consumption_mj_per_vkm.iloc[0]
                add(context | {"powertrain": powertrain}, "Harmonised before indexing", "ATB absolute total energy", year,
                    "consumption", consumption, "MJ/vkm", calibration_observation=False,
                    treatment="PHEV utility-weighted fuel HHV plus electricity" if powertrain.startswith("phev") else "Source energy converted with production contract")
            if rules["road_classes"][mode]["pathway"] == "mhdv":
                for item in ceud_history.itertuples(index=False):
                    add(context | {"powertrain": item.powertrain}, "Harmonised before indexing",
                        f"CEUD {item.source_fuel} observed fleet", item.year,
                        "consumption", item.value, "MJ/vkm", source_class="CEUD coarse vehicle group",
                        source_fuel=item.source_fuel, consumption_index=item.consumption_index,
                        index_reference_year=item.index_reference_year,
                        index_reference_status=item.index_reference_status,
                        calibration_observation=item.year <= evidence.observation_end_year,
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
            reference_year = evidence.projection_reference_year

            for year in range(first_year, base + 1):
                for powertrain in powertrains:
                    if not ldv and powertrain not in historical_powertrains:
                        continue
                    try:
                        result = evidence.derive_consumption(region, mode, powertrain, year, allow_analogue=False)
                    except ProjectionAnchorUnavailable:
                        if atb_class is None:
                            raise
                        # Detailed classes can lack an observed anchor. The
                        # missing-coverage row below records this same absence.
                        continue
                    if result is None:
                        continue
                    annual[powertrain, year] = result
                    derived = result["treatment"] == "CEUD_diesel_fleet_ATB_relative_powertrain"
                    projected = year > evidence.observation_end_year
                    add(context | {"powertrain": powertrain}, "Annual calculation",
                        "ATB-relative projection" if projected else "Diesel-fleet relative technology calculation" if derived else "Observed-year consumption calculation", year,
                        "consumption", result["native_intensity"], "MJ/vkm", treatment=result["treatment"],
                        analogue=result.get("analogue"), consumption_index=result.get("consumption_index"),
                        load_factor=result["load_factor"], historical_index_anchor=result.get("historical_index_anchor"),
                        observation_basis=_observation_summary([result])["observation_basis"],
                        calibration_observation=not derived and year <= evidence.observation_end_year)

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
                                vintage=vintage, existing=True, sample_kind="period", source_year_start=min(years), source_year_end=max(years),
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

                future_years = sorted(set(int(year) for year in atb_audit.year if reference_year < year <= end)
                                      | set(range(reference_year + 1, base + 1)))
                for year in future_years:
                    result = evidence.derive_consumption(region, mode, powertrain, year, allow_analogue=False)
                    if result is None:
                        continue
                    if (powertrain, year) not in annual:
                        add(context | {"powertrain": powertrain}, "Annual calculation", "ATB-relative projection", year,
                            "consumption", result["native_intensity"], "MJ/vkm", treatment=result["treatment"],
                            analogue=result.get("analogue"), consumption_index=result.get("consumption_index"),
                            load_factor=result["load_factor"], calibration_observation=False)
                    # The recorded production aggregate uses final prepared CSV
                    # markers at model-supported source years. Keep the explicit
                    # first ATB reference point even when it is not a model row.
                    if year > base and (atb_class is not None or not recorded_selection):
                        marker(context, powertrain, result["native_intensity"], result["load_factor"], year, service_units,
                            vintage=future_vintages.get(year), existing=False, sample_kind="period", source_year_start=year, source_year_end=year,
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
                        vintage=vintage, existing=True, sample_kind="period", marker_source="ATB class recalculation" if atb_class is not None else "Recalculated period",
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
    statistics = rating_class_statistics(distributions, rules=rules, statistic="median")
    keys = ["rating_class", "powertrain", "year", "native_unit"]
    statistics = statistics.merge(distributions[keys + ["vehicle_group", "atb_class"]].drop_duplicates(),
                                  on=keys, validate="one_to_one")
    rows = []
    for item in statistics.to_dict("records"):
        group, atb_class, rating_class, powertrain, year, unit = (
            item[field] for field in ["vehicle_group", "atb_class", "rating_class", "powertrain", "year", "native_unit"])
        if powertrain.startswith("phev"):
            rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                "year": year, "value": item["native_consumption"], "units": "Le/100 km (CD)",
                "series": "NRCan CD family-median fallback",
                "native_basis": "Median CD equivalent rate across distinct-trim family medians; no utility weighting"})
        else:
            rows.append({"vehicle_group": group, "atb_class": atb_class, "source_class": rating_class, "powertrain": powertrain,
                "year": year, "value": item["native_consumption"], "units": unit, "series": "NRCan family-median fallback",
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


def production_native_sources(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame],
                               raw: pd.DataFrame) -> pd.DataFrame:
    """Show production class estimates using their recorded aggregation basis."""
    weights = regional_ratings(bundle, inputs["rating_weights"])
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    atb_map = nrcan_to_nlr_map(mapping)
    keys = ["year", "rating_class", "powertrain"]
    output = []
    for item in weights.drop_duplicates(keys).to_dict("records"):
        atb_class = atb_map.get(item["rating_class"], "Unresolved source class")
        context = {"vehicle_group": {"Car": "Cars", "Light Truck": "Light trucks"}.get(item["ceud_class"], item["ceud_class"]),
                   "atb_class": atb_class, "source_class": item["rating_class"], "powertrain": item["powertrain"],
                   "year": item["year"], "aggregation_method": item.get("aggregation_method", "recorded_class_estimate")}
        if item["powertrain"].startswith("phev"):
            output.append({**context, "series": "NRCan production CD", "value": item["native_class_mean"], "units": "Le/100 km (CD)",
                           "native_basis": "Count-weighted family native medians or hierarchical fallback; no utility weighting"})
        else:
            output.append({**context, "series": "NRCan production class estimate", "value": item["native_class_mean"],
                           "units": item["native_unit"], "native_basis": "Recorded empirical composition or explicit catalogue fallback"})
    # CEUD and external model inputs retain their existing source treatment.
    retained = raw.loc[~raw.series.str.startswith("NRCan family-median") & ~raw.series.str.startswith("NRCan CD family-median")]
    result = pd.concat([pd.DataFrame(output), retained], ignore_index=True)
    result["powertrain_family"] = result.powertrain.map(_family)
    return result


def composition_comparison(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compare the current hierarchical fallback stage with the production estimate."""
    keys = ["ceud_class", "year", "rating_class", "powertrain", "native_unit"]
    production = regional_ratings(bundle, inputs["rating_weights"])
    fallback = regional_ratings(bundle, inputs["catalogue_rating_weights"])
    selected = production.rename(columns={"native_class_mean": "production_native_consumption",
        "class_mean_mj_per_vkm": "production_mj_per_vkm"})
    catalogue = fallback[keys + ["native_class_mean", "class_mean_mj_per_vkm"]].rename(columns={
        "native_class_mean": "catalogue_native_consumption", "class_mean_mj_per_vkm": "catalogue_mj_per_vkm"})
    result = selected.merge(catalogue, on=keys, how="outer", validate="one_to_one")
    result["consumption_change_fraction"] = result.production_mj_per_vkm / result.catalogue_mj_per_vkm - 1
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


def production_trajectory_data(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Separate prepared periods, historical range references and ATB indexing."""
    periods = frame.loc[frame.sample_kind.eq("period") & frame.vintage.notna()].copy()
    if "existing" not in periods or periods.existing.isna().any():
        raise ValueError("Bridge period plotting requires existing/future classification")
    actual = periods.atb_class.eq(COHORT)
    range_history = (periods.existing.eq(True) & periods.powertrain.str.startswith(("bev_", "phev_"))
                     & ~periods.powertrain.str.endswith("_existing_representative"))
    reference_rows = range_history & (~actual | periods.marker_source.eq("Production range component"))
    references = periods.loc[reference_rows].copy()
    periods = periods.loc[~reference_rows & (~actual | periods.marker_source.eq("Final prepared period CSV"))].copy()
    anchors = frame.loc[frame.sample_kind.eq("base_year_anchor")].copy()
    contexts = ["vehicle_group", "atb_class", "series", "metric", "units"]
    references["trajectory_powertrain"] = references.powertrain
    references["trajectory_id"] = (references.groupby(contexts, dropna=False, sort=False).ngroup().astype(str)
                                    + "/reference/" + references.powertrain)
    solid, indexing = [], []
    for group_index, (key, group) in enumerate(periods.groupby(contexts, dropna=False, sort=False)):
        group = group.assign(trajectory_powertrain=group.powertrain,
                             trajectory_id=str(group_index) + "/period/" + group.powertrain)
        solid.append(group)
        matching = anchors
        for field, value in zip(contexts, key, strict=True):
            matching = matching.loc[matching[field].eq(value)]
        for anchor_index, anchor in matching.iterrows():
            candidates = group.loc[group.powertrain.eq(anchor.powertrain) & group.existing.eq(False)
                                   & group.year.gt(anchor.year)]
            if candidates.empty:
                continue
            target = candidates.loc[candidates.year.eq(candidates.year.min())]
            if len(target) != 1:
                raise ValueError(f"Ambiguous next production period for ATB indexing: {key}/{anchor.powertrain}")
            segment = pd.concat([anchor.to_frame().T, target], ignore_index=True)
            segment["trajectory_powertrain"] = anchor.powertrain
            segment["trajectory_id"] = f"{group_index}/index/{anchor_index}/{anchor.powertrain}"
            indexing.append(segment)
    empty = periods.assign(trajectory_powertrain=pd.Series(dtype=str), trajectory_id=pd.Series(dtype=str)).iloc[:0]
    return {"periods": periods, "anchors": anchors, "reference": references,
            "solid": pd.concat(solid, ignore_index=True) if solid else empty,
            "indexing": pd.concat(indexing, ignore_index=True) if indexing else empty.copy()}


def distinct_catalogue_reference(production: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    """Omit a median reference trajectory when all its plotted points coincide."""
    if reference.empty:
        return reference.copy()
    path = ["vehicle_group", "powertrain", "metric"]
    if "atb_class" in reference:
        path.append("atb_class")
    point = [*path, "sample_kind", "year"]
    if "vintage" in reference and "vintage" in production:
        point.append("vintage")
    actual = production.loc[production.stage.eq("Indexed bridge")
        & production.sample_kind.isin(["period", "base_year_anchor"]), point + ["value"]].drop_duplicates()
    if actual.duplicated(point).any():
        raise ValueError("Conflicting production markers at the same source-period coordinate")
    retained = []
    for _, group in reference.groupby(path, dropna=False, sort=False):
        plotted = group.loc[group.stage.eq("Indexed bridge")
            & group.sample_kind.isin(["period", "base_year_anchor"]), point + ["value"]]
        compared = plotted.merge(actual, on=point, how="left", suffixes=("_reference", "_production"),
                                 indicator=True, validate="many_to_one")
        duplicate = (not compared.empty and compared._merge.eq("both").all()
            and np.allclose(compared.value_reference, compared.value_production, rtol=1e-12, atol=1e-12))
        if not duplicate:
            retained.append(group)
    return pd.concat(retained, ignore_index=True) if retained else reference.iloc[:0].copy()


def build_diagnostic_frames(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame], *, recorded_statistic: str | None = None,
                            bridge_years: list[int] | None = None) -> tuple[dict, dict]:
    """Explain recorded production values without rerunning source preparation."""
    required = {"composition_observations", "composition_variants", "composition_matches", "composition_coverage", "catalogue_rating_weights"}
    if required - set(inputs):
        raise ValueError("Rebuild notebook evidence to capture empirical NRCan composition, compatible matches and the hierarchical cohort fallback")
    benchmark = build_composition_benchmark(bundle, inputs)
    rules = load_harmonization_rules(bundle, "ldv_efficiency_diagnostic")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    distribution, distribution_summary = rating_distributions(bundle, inputs["ratings"])
    bridge, anchors = _bridge_rows(bundle, inputs, recorded_statistic=recorded_statistic, bridge_years=bridge_years)
    if recorded_statistic is None:
        bridge = pd.concat([bridge, _published_periods(bundle, inputs)], ignore_index=True)
    bridge["powertrain_family"] = bridge.powertrain.map(_family)
    # Retain the configured audit surface without generating an alternative
    # historical/future estimator alongside the current production method.
    catalogue_bridge = bridge.iloc[:0].copy()
    raw_sources = _raw_sources(bundle, inputs, distribution)
    models = composition_model_evidence(bundle, inputs["ratings"])
    comparison = composition_comparison(bundle, inputs)
    model_keys = ["year", "rating_class", "powertrain", "native_unit"]
    ranges = models.groupby(model_keys, as_index=False).agg(
        catalogue_model_min=("family_median_mj_per_vkm", "min"), catalogue_model_max=("family_median_mj_per_vkm", "max"),
        catalogue_models=("family_median_mj_per_vkm", "size"), vehicle_group=("vehicle_group", "first"))
    comparison = comparison.merge(ranges, on=model_keys, how="left", validate="many_to_one")
    mapping = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    inferred_group = comparison.rating_class.map(nrcan_to_nlr_map(mapping)).map(mapping["target_to_ceud"]).map({"Car": "Cars", "Light Truck": "Light trucks"})
    comparison["vehicle_group"] = comparison.vehicle_group.fillna(inferred_group)
    frames = {"raw_sources": raw_sources, "production_sources": production_native_sources(bundle, inputs, raw_sources),
        "rating_distribution": distribution, "rating_summary": distribution_summary,
        "bridge": bridge, "projection_anchors": anchors, "omega_shares": inputs["omega_buckets"].copy(),
        "composition_observations": regional_ratings(bundle, inputs["composition_observations"]),
        "composition_variants": regional_ratings(bundle, inputs["composition_variants"]),
        "composition_matches": regional_ratings(bundle, inputs["composition_matches"]),
        "composition_coverage": regional_ratings(bundle, inputs["composition_coverage"]),
        "composition_models": models, "composition_families": composition_family_evidence(bundle, inputs),
        "composition_comparison": comparison, "catalogue_bridge": catalogue_bridge,
        "composition_benchmark": benchmark.comparison,
        "composition_benchmark_coverage": benchmark.coverage,
        "composition_benchmark_timing": benchmark.timing,
        "composition_benchmark_observations": benchmark.observations}
    summary = {
        "region": rules["region"], "base_year": bundle.scenario.periods.base_year,
        "bridge_years": bridge_years if bridge_years is not None else rules["bridge_years"],
        "anchor_powertrains": efficiency_rules["pev_projection_anchors"], "vehicle_groups": list(VIEW_MODES),
        "atb_reference_year": min(bundle.scenario.periods.base_year, efficiency_rules["projection_reference_year"]),
        "composition_rules": efficiency_rules["ratings"]["composition"],
        "composition_years": rules["composition_years"],
        "benchmark_calendar_years": load_harmonization_rules(bundle, "ldv_composition_benchmark")["comparison_calendar_years"],
        "minimum_model_year": rules["minimum_model_year"], "distribution_year_step": rules["distribution_year_step"],
        "distribution_consumption_domains": rules["distribution_consumption_domains"],
        "population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        "atb_trajectory": bundle.scenario.efficiencies.atb_trajectory,
        "production_methods": sorted(inputs["rating_weights"].aggregation_method.unique()),
        "mhdv_fleet_baseline": efficiency_rules["mhdv_fleet_baseline"],
        "available_rating_years": sorted(int(y) for y in distribution.year.unique()),
        "omega_model_years": sorted(int(y) for y in inputs["omega_records"].model_year.unique()),
        "phev_consumption_method": "UF * (CD electricity plus CD fuel energy) + (1 - UF) * CS fuel energy; ATB range-bucket UF",
        "limitations": [
            "Gasoline composition uses MTO families with applicable reviewed identities and positive documentary evidence. Eligible families must cover the configured share of gasoline-compatible family counts in each model-year/size-class cohort; otherwise the class uses its hierarchical catalogue median. Compatible gasoline trims exclude hybrids and alternative fuels, but MTO counts do not observe fuel or trim. This threshold is a coverage policy, not a confidence probability. Coverage of the whole mapped all-fuel class is separate.",
            "iZEV records cover eligible purchases and leases, including program exceptions for demonstrators and repeat incentive requests. These are acquisition records, not identifiable unique vehicles. The supported model-year window and regional/national pooling threshold are recorded in configuration. Earlier target vintages may use that whole window as an explicit composition proxy, paired only with target-year Ratings; those reused records are not observed historical registrations and must not be summed across target vintages. Incentives do not cover the full market; price, vehicle and program eligibility select the observed acquisitions. Request year differs from vehicle model year, so later requests make the composition an ex-post proxy. Identical administrative rows are retained because the public data provide no VIN for vehicle-level deduplication.",
            "Every rated subclass lacking empirical support uses a median across canonical make/model family medians of distinct eligible trims. Original outer class weights remain; only genuinely absent rated classes require renormalization. Each selected source description contributes its count once using its compatible family's median consumption. A clear PEV family uses median eligible-trim range for bucket allocation; this is a representative-rating assumption, not observed trim composition. Contradictory identity, class or explicit configuration remains unresolved and visible.",
            "Count weights stand in for vehicle activity under equal annual distance within each CEUD class: a common VKT factor cancels from the weighted consumption mean. This is a modelling assumption, not evidence that the observed families have equal real-world use.",
            "Each PHEV row combines UF times CD electricity-plus-fuel energy with (1 minus UF) times CS fuel energy, using the ATB range-bucket UF. Family/cohort aggregation follows. Independent operating-mode values remain in tables; CS is not added unweighted to CD. The transferred travel share and laboratory ratings are not measured Canadian consumption.",
            "Existing-period consumption and loads use raw annual cohort counts before lifetime redistribution; service efficiency is calculated last. OMEGA combines supported historical range periods and the recorded cohort anchors future ATB ratios. Historical rating gaps retain explicit existing analogue policies. Annual years after the aligned 2023 reference use bounded ATB consumption interpolation.",
            "Catalogue family medians and the hierarchical cohort median in the grids/heatmap are stages of the current production method. Future plots show only recorded production values and anchors. Detailed ATB classes inherit parent loads and cohort weights. Smoother curves and high matched shares alone do not establish representativeness.",
            "The final StatCan comparison is validation only. Registered stock, new registrations, MTO fit-active survivors and incentivized acquisitions have different populations and reference periods. Calendar request years are not model years. MTO does not observe powertrain: a catalogue-resolved subset cannot establish the full fleet's fuel composition, and non-overlapping years remain labelled.",
            "Medium/heavy truck incumbents use their fuel-specific CEUD fleet cohort through the aligned ATB reference year. Later source observations remain visible in the source views; projected annual values use that cohort and relative ATB consumption changes.",
        ],
    }
    return frames, summary


def load_diagnostic(*, repo_root: Path | None = None) -> LDVEfficiencyDiagnostic:
    """Load the latest successful efficiency evidence snapshot without selecting a scenario."""
    from validation.notebook_evidence import load_notebook_evidence, prepare_command

    recorded = load_notebook_evidence("efficiency", repo_root=repo_root)
    policy = load_harmonization_rules(recorded.bundle, "efficiencies")["ratings"]["composition"]
    family_policy = policy.get("family_matching", {})
    if (policy.get("variant_allocation") != "family_median" or policy.get("family_statistic") != "median"
            or not {"class_resolution", "consumption_dispersion"}.issubset(family_policy)):
        raise ValueError("Recorded efficiency evidence predates the production family-median matching method. "
                         "Rebuild the evidence explicitly, then reload this notebook: "
                         + prepare_command(recorded.bundle.repo_root))
    reference = load_vehicle_reference(recorded.bundle)
    inputs = {name: pd.read_csv(path, low_memory=False) for name, path in recorded.paths.items()
              if path.name.lower().endswith((".csv", ".csv.gz"))}
    view = composition_view_settings(recorded.bundle)
    frames, summary = build_diagnostic_frames(recorded.bundle, inputs, bridge_years=view["bridge_years"])
    summary["build"] = recorded.metadata
    summary["inputs"] = {name: {"path": str(path), "sha256": file_sha256(path)} for name, path in recorded.paths.items()}
    canonical, links = reference.canonical, reference.links
    linked_ids = set(links.loc[links.source.eq("ontario_ministry_transport_vehicle_population"), "vehicle_id"].dropna())
    summary["vehicle_reference"] = {
        "canonical_identities": canonical.vehicle_id.nunique(), "reviewed_scope_rows": len(canonical),
        "source_link_intervals": len(links),
        "identities_without_mto_link": int(canonical.loc[~canonical.vehicle_id.isin(linked_ids), "vehicle_id"].nunique()),
        "unlinked_identity_intervals": int(links.vehicle_id.isna().sum()),
        "configuration_unresolved_intervals": int(links.link_status.eq("configuration_unresolved").sum()),
    }
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
