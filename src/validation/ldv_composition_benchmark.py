"""Validation-only comparisons of independently counted LDV populations.

New registrations and iZEV requests are acquisition flows. MTO FIT_ACTIVE and
StatCan annual registrations are stock snapshots. Their time bases, selection
coverage and denominators stay explicit; this module supplies no production
weights and never infers a complete MTO powertrain distribution.
"""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from parameterization.rating_composition import canonical_family
from parameterization.road_aggregation import (
    ceud_class_for_nlr,
    normalize_vehicle_text,
    nrcan_to_nlr_map,
)
from utils import ConfigBundle, load_harmonization_rules, resolve_artifact_path, resolve_input_path


@dataclass(frozen=True)
class CompositionBenchmark:
    comparison: pd.DataFrame
    coverage: pd.DataFrame
    timing: pd.DataFrame
    observations: pd.DataFrame


def collect_composition_benchmark_inputs(bundle: ConfigBundle) -> dict[str, Path]:
    """Locate prepared inputs for immutable notebook capture; never prepare them."""
    spec = load_harmonization_rules(bundle, "ldv_composition_benchmark")
    statcan = load_harmonization_rules(bundle, "statcan_tables")
    directory = resolve_input_path(bundle, "interim", statcan["interim_subdir"])
    paths = {
        "benchmark_statcan_new": directory / spec["new_registrations_file"],
        "benchmark_statcan_stock": directory / spec["stock_file"],
        "benchmark_mto_passenger_cohorts": resolve_artifact_path(bundle, "vehicle_survival_interim", spec["mto_passenger_file"]),
        "benchmark_mto_commercial_cohorts": resolve_artifact_path(bundle, "vehicle_survival_interim", spec["mto_commercial_file"]),
        "benchmark_mto_mapped": resolve_artifact_path(bundle, "road_aggregation", spec["mto_mapped_file"]),
        "benchmark_izev_records": resolve_artifact_path(bundle, "transport_canada_izev", spec["izev_file"]),
    }
    if missing := [key for key, path in paths.items() if not path.is_file()]:
        raise FileNotFoundError(f"Prepare the diagnostic benchmark inputs before capture: {missing}")
    return paths


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    if missing := columns - set(frame):
        raise ValueError(f"{label} missing columns: {sorted(missing)}")


def _counts(values: pd.Series, label: str, *, missing: bool = False) -> pd.Series:
    result = pd.to_numeric(values, errors="raise")
    valid = result.dropna() if missing else result
    if valid.isna().any() or not np.isfinite(valid).all() or valid.lt(0).any() or valid.mod(1).ne(0).any():
        raise ValueError(f"{label} requires nonnegative integer counts")
    return result


def _class_groups(bundle: ConfigBundle) -> dict[str, str]:
    source = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"]
    display = {"Car": "Cars", "Light Truck": "Light trucks"}
    result = {label: display[ceud_class_for_nlr(target, source)] for label, target in nrcan_to_nlr_map(source).items()}
    # Historical MTO cohort snapshots already use the five accepted NLR classes.
    # NRCan-labelled catalogue evidence and NLR-labelled cohort counts share the
    # same established CEUD rollup, rather than a new benchmark crosswalk.
    for target, ceud in source["target_to_ceud"].items():
        if target in result and result[target] != display[ceud]:
            raise ValueError("Benchmark source classes conflict across established class layers")
        result[target] = display[ceud]
    historical = load_harmonization_rules(bundle, "efficiencies")["historical_suv"]
    children = {result[label] for label in historical["combined_weight_classes"]}
    if len(children) != 1:
        raise ValueError("Historical SUV class does not resolve to one established vehicle group")
    result[historical["source_label"]] = children.pop()
    return result


def _statcan(frame: pd.DataFrame, spec: dict, source: str, regions: list[str], geography: dict) -> pd.DataFrame:
    """Keep published totals and missing/suppressed cells distinct from zeros."""
    _require(frame, {"scenario_region", "geography", "vehicle_type", "fuel_type", "scaled_value"}, source)
    year_column = "reference_year" if source == "statcan_new" else "reference_period"
    _require(frame, {year_column}, source)
    rows = frame.loc[frame.scenario_region.isin(regions)].copy()
    rows["year"] = pd.to_numeric(rows[year_column], errors="raise")
    if rows.year.isna().any() or rows.year.mod(1).ne(0).any():
        raise ValueError("StatCan benchmark requires complete annual reference years")
    rows["count"] = _counts(rows.scaled_value, source, missing=True)
    rows["body"] = rows.vehicle_type.astype(str).str.split(", weighing ", n=1).str[0]
    rows["vehicle_group"] = rows.body.map(spec["body_map"])
    rows["powertrain"] = rows.fuel_type.map(spec["fuel_map"])
    rows = rows.loc[rows.vehicle_group.notna() & rows.powertrain.notna()]
    keys = ["scenario_region", "year", "body", "powertrain"]
    if rows.duplicated(keys).any():
        raise ValueError(f"{source} has duplicate or overlapping geographic/count series")
    output = []
    for (region, year), annual in rows.groupby(["scenario_region", "year"], sort=True):
        for group in dict.fromkeys(spec["body_map"].values()):
            expected = {body for body, mapped in spec["body_map"].items() if mapped == group}
            expected_fuels = set(spec["fuel_map"].values()) - ({"all"} if source == "statcan_new" else set())
            for powertrain in sorted(expected_fuels):
                selected = annual.loc[annual.vehicle_group.eq(group) & annual.powertrain.eq(powertrain)]
                complete = set(selected.body) == expected and selected["count"].notna().all()
                native_geography = " | ".join(sorted(selected.geography.unique()))
                expected_geography = geography[region]["label"]
                proxy = complete and native_geography != expected_geography
                output.append({
                    "region": region, "year": int(year), "vehicle_group": group,
                    "powertrain": powertrain, "source": source,
                    "count": float(selected["count"].sum()) if complete else np.nan,
                    "native_vehicle_class": " | ".join(sorted(selected.vehicle_type.unique())),
                    "native_powertrain": " | ".join(sorted(selected.fuel_type.unique())),
                    "source_geography": native_geography,
                    "source_table": " | ".join(sorted(selected.get("source_table_id", selected.get("table_id", pd.Series(dtype=str))).dropna().astype(str).unique())),
                    "population": "New registrations" if source == "statcan_new" else "Registered stock",
                    "time_basis": "Registration calendar year" if source == "statcan_new" else "Annual registration stock year",
                    "status": "geography_proxy" if proxy else "reported" if complete else "missing_or_suppressed_body_fuel_cells",
                })
    return pd.DataFrame(output)


def _class_lookup(observations: pd.DataFrame, groups: dict[str, str]) -> dict[tuple, tuple[str, str | None]]:
    """Resolve only the common broad group, preserving every candidate class."""
    _require(observations, {"source_kind", "year", "family_make", "family_model", "observed_powertrain", "candidate_rating_classes"}, "Recorded model-class evidence")
    rows = observations.loc[observations.source_kind.eq("izev")]
    candidates: dict[tuple, set[str]] = {}
    for row in rows.itertuples(index=False):
        key = int(row.year), normalize_vehicle_text(row.family_make), normalize_vehicle_text(row.family_model), row.observed_powertrain
        classes = {part.strip() for part in str(row.candidate_rating_classes).split("|")
                   if pd.notna(row.candidate_rating_classes) and part.strip()}
        candidates.setdefault(key, set()).update(classes)
    result = {}
    for key, classes in candidates.items():
        mapped = {groups.get(label) for label in classes}
        broad = next(iter(mapped)) if len(mapped) == 1 and None not in mapped else None
        result[key] = " | ".join(sorted(classes)), broad
    return result


def _izev(bundle: ConfigBundle, frame: pd.DataFrame, matched: pd.DataFrame, spec: dict,
          groups: dict[str, str], regions: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _require(frame, {"model_year", "request_year", "province", "make", "model", "powertrain", "count", "Purchase or Lease"}, "iZEV benchmark")
    policy = load_harmonization_rules(bundle, "efficiencies")["ratings"]["composition"]
    aliases = {**policy, "aliases": [*policy.get("aliases", []), *policy.get("izev_aliases", [])]}
    lookup = _class_lookup(matched, groups)
    accepted = frame["Purchase or Lease"].isin(spec["incentive_transaction_types"])
    raw = frame.loc[accepted & frame.powertrain.isin(["bev", "phev"])].copy()
    raw["count"] = _counts(raw["count"], "iZEV benchmark")
    raw["year"] = pd.to_numeric(raw.request_year, errors="raise")
    raw["model_year"] = pd.to_numeric(raw.model_year, errors="raise")
    if raw[["year", "model_year"]].isna().any().any() or raw[["year", "model_year"]].mod(1).ne(0).any().any():
        raise ValueError("iZEV benchmark needs distinct integer request and model years")
    # Resolve hundreds of source labels once, rather than per transaction.
    labels = {(make, model): canonical_family(make, model, aliases)
              for make, model in raw[["make", "model"]].drop_duplicates().itertuples(index=False, name=None)}
    keys = [(int(year), *map(normalize_vehicle_text, labels[make, model]), powertrain)
            for year, make, model, powertrain in raw[["model_year", "make", "model", "powertrain"]].itertuples(index=False, name=None)]
    resolved_classes = [lookup.get(key, ("", None)) for key in keys]
    raw["rating_class"] = [classes for classes, _ in resolved_classes]
    raw["vehicle_group"] = [broad for _, broad in resolved_classes]
    outputs, coverage, timing, native_observations = [], [], [], []
    for region in regions:
        provinces = policy.get("province_map", {}).get(region, [region])
        provinces = [provinces] if isinstance(provinces, str) else provinces
        local = raw.loc[raw.province.isin(provinces)]
        for (year, powertrain), group in local.groupby(["year", "powertrain"]):
            total, resolved = group["count"].sum(), group.loc[group.vehicle_group.notna(), "count"].sum()
            coverage.append({"region": region, "year": int(year), "source": "izev", "powertrain": powertrain,
                             "total_count": total, "mapped_count": resolved, "coverage_share": resolved / total if total else np.nan,
                             "denominator_basis": "All iZEV PEV request records, purchases plus leases",
                             "status": "class_mapping_partial" if resolved < total else "class_mapping_complete",
                             "note": "All candidate NRCan classes resolve to one known Cars/Light trucks group; independent of production selection, assigned range and incentive model-year window"})
        for (year, model_year, powertrain), group in local.groupby(["year", "model_year", "powertrain"]):
            timing.append({"region": region, "request_year": int(year), "model_year": int(model_year),
                           "powertrain": powertrain, "count": group["count"].sum(), "year_lag": int(year - model_year),
                           "time_basis": "Request received year minus vehicle model year; purchase/delivery date unavailable"})
        detail_keys = ["year", "model_year", "province", "make", "model", "powertrain", "Purchase or Lease", "rating_class", "vehicle_group"]
        for key, selected in local.groupby(detail_keys, dropna=False, sort=True):
            year, model_year, province, make, model, powertrain, transaction, rating_class, group = key
            resolved = pd.notna(group)
            native_observations.append({
                "region": region, "year": int(year), "model_year": int(model_year), "vehicle_group": group if resolved else "",
                "powertrain": powertrain, "source": "izev", "count": selected["count"].sum(),
                "source_make": make, "source_model": model,
                "native_vehicle_class": "Not observed by iZEV", "mapped_rating_classes": rating_class,
                "native_powertrain": powertrain.upper(), "source_geography": province, "transaction_types": transaction,
                "population": "iZEV new-vehicle acquisition request records, including leases",
                "time_basis": "Incentive request calendar year; registration date unavailable",
                "status": "mapped_class_subset" if resolved else "unmatched_or_ambiguous_class",
            })
        for (year, group, powertrain), subset in local.loc[local.vehicle_group.notna()].groupby(["year", "vehicle_group", "powertrain"]):
            outputs.append({"region": region, "year": int(year), "vehicle_group": group, "powertrain": powertrain,
                            "source": "izev", "count": subset["count"].sum(),
                            "native_vehicle_class": "Not observed by iZEV", "mapped_rating_classes": " | ".join(sorted(subset.rating_class.unique())),
                            "native_powertrain": powertrain, "source_geography": " | ".join(sorted(subset.province.unique())),
                            "transaction_types": " | ".join(sorted(subset["Purchase or Lease"].unique())),
                            "population": "iZEV new-vehicle acquisition request records, including leases",
                            "time_basis": "Incentive request calendar year; registration date unavailable", "status": "mapped_class_subset"})
    return pd.DataFrame(outputs), pd.DataFrame(coverage), pd.DataFrame(timing), pd.DataFrame(native_observations)


def _mto_history(passenger: pd.DataFrame, commercial: pd.DataFrame, groups: dict[str, str],
                 regions: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "ON" not in regions:
        return pd.DataFrame(), pd.DataFrame()
    _columns = {"population_group", "report_year", "model_year", "cohort_class", "stock_status", "cohort_count"}
    _require(passenger, _columns, "MTO passenger cohorts")
    _require(commercial, _columns, "MTO commercial cohorts")
    for frame, expected in ((passenger, "mapped_passenger"), (commercial, "mapped_commercial")):
        if set(frame.population_group) - {expected}:
            raise ValueError("MTO benchmark inputs must preserve the producer's disjoint Report A passenger/commercial partitions")
    raw = pd.concat([passenger, commercial], ignore_index=True)
    raw = raw.loc[raw.stock_status.eq("FIT_ACTIVE")].copy()
    keys = ["population_group", "report_year", "model_year", "cohort_class", "stock_status"]
    if raw.duplicated(keys).any():
        raise ValueError("MTO benchmark cohort snapshots overlap")
    raw["count"] = _counts(raw.cohort_count, "MTO cohort benchmark")
    raw["vehicle_group"] = raw.cohort_class.map(groups)
    output, coverage = [], []
    for year, rows in raw.groupby("report_year"):
        total, mapped = rows["count"].sum(), rows.loc[rows.vehicle_group.notna(), "count"].sum()
        coverage.append({"region": "ON", "year": int(year), "source": "mto_stock", "powertrain": "all",
                         "total_count": total, "mapped_count": mapped, "coverage_share": mapped / total if total else np.nan,
                         "model_year_min": rows.model_year.min(), "model_year_max": rows.model_year.max(),
                         "denominator_basis": "Available mapped FIT_ACTIVE passenger and commercial cohorts, not all MTO records",
                         "status": "available_mapped_cohort_subset", "note": "Vehicle model-year support is retained; propulsion is not observed"})
        for group, selected in rows.loc[rows.vehicle_group.notna()].groupby("vehicle_group"):
            output.append({"region": "ON", "year": int(year), "vehicle_group": group, "powertrain": "all", "source": "mto_stock",
                           "count": selected["count"].sum(), "native_vehicle_class": " | ".join(sorted(selected.cohort_class.unique())),
                           "native_powertrain": "Unobserved", "source_geography": "Ontario", "population": "Available mapped surviving FIT_ACTIVE cohorts",
                           "time_basis": "MTO report-year stock snapshot", "status": "available_mapped_cohort_subset"})
    return pd.DataFrame(output), pd.DataFrame(coverage)


def _mto_subset(mapped: pd.DataFrame, observations: pd.DataFrame, groups: dict[str, str], regions: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "ON" not in regions:
        return pd.DataFrame(), pd.DataFrame()
    _require(mapped, {"report_year", "mapping_accepted", "nrcan_ceud_class", "nrcan_vehicle_class", "FIT_ACTIVE"}, "MTO current mapped stock")
    _require(observations, {"source_kind", "resolved_powertrain", "source_count", "rating_class", "candidate_rating_classes", "candidate_powertrains", "empirical_observation_id", "match_status"}, "MTO catalogue compatibility")
    if not mapped.mapping_accepted.isin([True, False]).all():
        raise ValueError("MTO benchmark mapping_accepted requires complete boolean flags")
    _counts(mapped.FIT_ACTIVE, "MTO current benchmark")
    years = mapped.report_year.dropna().unique()
    if len(years) != 1:
        raise ValueError("MTO current benchmark needs one report-year snapshot")
    display = {"Car": "Cars", "Light Truck": "Light trucks"}
    class_groups = mapped.loc[mapped.mapping_accepted & mapped.nrcan_ceud_class.isin(display)]
    denominators = class_groups.assign(vehicle_group=class_groups.nrcan_ceud_class.map(display)).groupby("vehicle_group").FIT_ACTIVE.sum()
    source = observations.loc[observations.source_kind.eq("mto")].copy()
    source["source_count"] = _counts(source.source_count, "MTO catalogue subset")
    # One family may occur in several requested regions; count its Ontario evidence once.
    source = source.drop_duplicates("empirical_observation_id")
    single = source.candidate_rating_classes.fillna("").ne("") & ~source.candidate_rating_classes.fillna("").str.contains("|", regex=False)
    single &= source.candidate_powertrains.fillna("").ne("") & ~source.candidate_powertrains.fillna("").str.contains("|", regex=False)
    single &= source.resolved_powertrain.fillna("").ne("") & ~source.match_status.isin(["unresolved_mapping_identity", "ambiguous_powertrain_evidence"])
    source = source.loc[single]
    source["vehicle_group"] = source.rating_class.map(groups)
    source["powertrain"] = source.resolved_powertrain.str.split("_").str[0]
    output, coverage = [], []
    for group, selected in source.loc[source.vehicle_group.notna()].groupby("vehicle_group"):
        total = float(denominators.get(group, 0))
        subset = float(selected.source_count.sum())
        coverage.append({"region": "ON", "year": int(years[0]), "source": "mto_catalogue_subset", "vehicle_group": group,
                         "total_count": total, "mapped_count": subset, "coverage_share": subset / total if total else np.nan,
                         "denominator_basis": "Current mapped FIT_ACTIVE stock in this vehicle group",
                         "status": "catalogue_resolved_subset_only", "note": "Compatible catalogue propulsion is not independently observed MTO fuel; never a full fleet distribution"})
        for powertrain, cells in selected.groupby("powertrain"):
            output.append({"region": "ON", "year": int(years[0]), "vehicle_group": group, "powertrain": powertrain,
                           "source": "mto_catalogue_subset", "count": cells.source_count.sum(),
                           "native_vehicle_class": " | ".join(sorted(cells.rating_class.unique())), "native_powertrain": "Unobserved",
                           "source_geography": "Ontario", "population": "MTO families with single compatible catalogue propulsion",
                           "time_basis": "Current MTO report-year stock snapshot", "status": "catalogue_resolved_subset_only"})
    return pd.DataFrame(output), pd.DataFrame(coverage)


def _shares(rows: pd.DataFrame, scope: str, denominator_keys: list[str], basis: str, note: str) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    result = rows.copy()
    totals = result.groupby(denominator_keys, dropna=False)["count"].transform(lambda x: x.sum(min_count=len(x)))
    result["denominator"] = totals
    result["share"] = result["count"] / totals.where(totals.gt(0))
    result["comparison_scope"], result["denominator_basis"], result["comparability_note"] = scope, basis, note
    return result


def _acquisition_grid_coverage(new: pd.DataFrame, incentive: pd.DataFrame, raw_coverage: pd.DataFrame,
                               regions: list[str], years: list[int]) -> tuple[pd.DataFrame, dict[str, set[tuple]]]:
    """Expose wholly absent source years without inventing zero observations."""
    available = {source: set(frame[["region", "year"]].itertuples(index=False, name=None)) if not frame.empty else set()
                 for source, frame in (("statcan_new", new), ("izev", incentive))}
    raw = ({key: (group.total_count.sum(), group.mapped_count.sum())
            for key, group in raw_coverage.groupby(["region", "year"])} if not raw_coverage.empty else {})
    records = []
    for region in regions:
        for year in years:
            for source in ("statcan_new", "izev"):
                if (region, year) in available[source]:
                    continue
                total, mapped = raw.get((region, year), (np.nan, np.nan)) if source == "izev" else (np.nan, np.nan)
                status = ("missing_reference_region_year" if source == "statcan_new" else
                          "no_mapped_incentive_region_year" if pd.notna(total) else "no_recorded_incentive_region_year")
                records.append({"region": region, "year": year, "source": source, "powertrain": "all",
                                "total_count": total, "mapped_count": mapped,
                                "coverage_share": mapped / total if pd.notna(total) and total > 0 else np.nan,
                                "denominator_basis": "Configured acquisition comparison region/calendar-year grid",
                                "status": status,
                                "note": "No comparable source counts are available for this region-year; source absence is not a zero population or a paired comparison"})
    return pd.DataFrame(records), available


def build_composition_benchmark(bundle: ConfigBundle, inputs: dict[str, pd.DataFrame]) -> CompositionBenchmark:
    """Compare common classes and populations without importing benchmark weights."""
    required = {"benchmark_statcan_new", "benchmark_statcan_stock", "benchmark_mto_passenger_cohorts",
                "benchmark_mto_commercial_cohorts", "benchmark_mto_mapped", "benchmark_izev_records", "composition_observations"}
    if missing := required - set(inputs):
        raise ValueError(f"Rebuild notebook evidence to capture composition benchmarks: {sorted(missing)}")
    spec = load_harmonization_rules(bundle, "ldv_composition_benchmark")
    if set(spec["incentive_transaction_types"]) != {"Purchase", "Lease"}:
        raise ValueError("Acquisition benchmark requires both initial purchases and leases")
    if set(spec["body_map"].values()) != {"Cars", "Light trucks"}:
        raise ValueError("Composition benchmarks require the common Cars/Light trucks grain")
    calendar_years = spec["comparison_calendar_years"]
    if (not isinstance(calendar_years, list) or not calendar_years
            or any(isinstance(year, bool) or not isinstance(year, int) for year in calendar_years)
            or calendar_years != sorted(set(calendar_years))):
        raise ValueError("Acquisition comparison calendar years must be configured, sorted distinct integers")
    regions = list(bundle.scenario.geography.regions)
    groups = _class_groups(bundle)
    geography = load_harmonization_rules(bundle, "statcan_tables")["geography"]
    new = _statcan(inputs["benchmark_statcan_new"], spec, "statcan_new", regions, geography)
    stock = _statcan(inputs["benchmark_statcan_stock"], spec, "statcan_stock", regions, geography)
    incentive, incentive_coverage, timing, incentive_observations = _izev(bundle, inputs["benchmark_izev_records"], inputs["composition_observations"], spec, groups, regions)
    mto, mto_coverage = _mto_history(inputs["benchmark_mto_passenger_cohorts"], inputs["benchmark_mto_commercial_cohorts"], groups, regions)
    subset, subset_coverage = _mto_subset(inputs["benchmark_mto_mapped"], inputs["composition_observations"], groups, regions)
    observations = pd.concat([frame for frame in (new, stock, incentive_observations, mto, subset) if not frame.empty], ignore_index=True)
    gaps, available = _acquisition_grid_coverage(new, incentive, incentive_coverage, regions, calendar_years)
    coverage = pd.concat([frame for frame in (incentive_coverage, mto_coverage, subset_coverage, gaps) if not frame.empty], ignore_index=True)
    observations["in_acquisition_comparison"] = (observations.source.isin(["statcan_new", "izev"])
                                                 & observations.year.isin(calendar_years)
                                                 & observations.vehicle_group.isin(["Cars", "Light trucks"]))
    coverage["in_acquisition_comparison"] = coverage.source.eq("izev") & coverage.year.isin(calendar_years)
    if not timing.empty:
        timing["in_comparison_calendar_years"] = timing.request_year.isin(calendar_years)
    comparison = []
    if not stock.empty:
        reference = stock.loc[stock.powertrain.ne("all")]
        comparison.append(_shares(reference, "statcan_stock_powertrain_reference", ["region", "year", "vehicle_group", "source"],
                                  "All reported fuel categories within vehicle group", "StatCan stock reference only; no complete independently observed MTO powertrain distribution"))
        totals = stock.loc[stock.powertrain.eq("all")]
        pairs = set(totals[["region", "year"]].itertuples(index=False, name=None))
        historical = mto.loc[[key in pairs for key in mto[["region", "year"]].itertuples(index=False, name=None)]] if not mto.empty else mto
        combined = pd.concat([totals, historical], ignore_index=True)
        comparison.append(_shares(combined, "stock_vehicle_group_share", ["region", "year", "source"],
                                  "Cars plus light trucks within the same stock source and report year", "MTO counts cover available mapped cohorts; StatCan covers registered stock. Shared broad classes do not establish identical coverage or observation dates."))
    acquisition = pd.concat([new.loc[new.powertrain.isin(["bev", "phev"])] if not new.empty else new, incentive], ignore_index=True)
    if not acquisition.empty:
        acquisition = acquisition.loc[acquisition.year.isin(calendar_years)]
        acquisition["counterpart_source_available"] = [
            (region, year) in available["statcan_new" if source == "izev" else "izev"]
            for region, year, source in acquisition[["region", "year", "source"]].itertuples(index=False, name=None)
        ]
        unpaired = ~acquisition.counterpart_source_available
        acquisition.loc[unpaired & acquisition.source.eq("izev"), "status"] = "unpaired_missing_statcan_reference"
        acquisition.loc[unpaired & acquisition.source.eq("statcan_new"), "status"] = "unpaired_no_mapped_incentive_counts"
    if not acquisition.empty:
        comparison.append(_shares(acquisition, "new_pev_powertrain_share", ["region", "year", "vehicle_group", "source"],
                                  "BEV plus PHEV acquisitions within vehicle group", "Eligible iZEV requests include purchases and leases; request year may lag registration. iZEV class shares are conditional on mapped descriptions, not all new registrations."))
        comparison.append(_shares(acquisition, "new_pev_vehicle_group_share", ["region", "year", "powertrain", "source"],
                                  "Cars plus light trucks within BEV or PHEV acquisitions", "Request calendar years and registration years are displayed separately from vehicle model years; eligibility and class mapping limit iZEV coverage."))
    if not subset.empty:
        comparison.append(_shares(subset, "mto_catalogue_subset_powertrain", ["region", "year", "vehicle_group", "source"],
                                  "MTO families with one compatible catalogue propulsion only", "Coverage-only conditional subset. Propulsion is not observed in MTO. No same-year stock benchmark is implied."))
    return CompositionBenchmark(pd.concat(comparison, ignore_index=True) if comparison else pd.DataFrame(), coverage, timing, observations)
