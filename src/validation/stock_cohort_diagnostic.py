"""Read-only comparisons of production sales cohorts and deprecated age allocation.

The production owners perform survival, calibration and allocation. This module
loads registered evidence without publication and prepares notebook comparisons.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fetching import statcan_tables
from parameterization.ceud_inputs import ceud_input_path, read_ceud_input
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    fixed_existing_lifetimes,
    resolved_road_lifetime_rules,
)
from parameterization.road_stocks_and_demands import (
    derive_ldv_age_distributions,
    derive_sales_survival_cohorts,
    distribute_existing_road_capacity,
)
from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
)


COHORT_KEYS = ["profile", "region", "road_class", "vintage", "vintage_year"]
GROUP_KEYS = ["profile", "region", "road_class"]


def _read_fuel_evidence(bundle: ConfigBundle) -> tuple[pd.DataFrame, pd.DataFrame, list[Path], list[str]]:
    """Use the registered adapters on three cached tables, without fetching/writing."""
    rules = load_harmonization_rules(bundle, "statcan_tables")
    history_rules = rules["ldv_history"]
    required = {str(history_rules["archived_table"]), str(history_rules["current_table"]), "23-10-0308-01"}
    requests = [request for request in statcan_tables.build_requests(bundle) if request.table_id in required]
    if {request.table_id for request in requests} != required:
        raise ValueError("Stock-cohort diagnosis requires the three registered fuel tables")
    multipliers = {
        str(key).lower(): float(value)
        for key, value in load_conversion_factors(bundle)["statcan"]["scalar_factor_multipliers"].items()
    }
    normalized: dict[str, pd.DataFrame] = {}
    paths, warnings = [], []
    for request in requests:
        statcan_tables.validate_source(request)
        statcan_tables.validate_metadata_contract(statcan_tables.read_cached_metadata(request), request)
        regions = list(bundle.scenario.geography.regions)
        if request.normalizer in {"ldv_archived", "ldv_current"} and set(regions).intersection(
            history_rules["canada_proxy_regions"]
        ):
            regions.append("Canada")
        if "BCT" in regions and request.table_rules.get("include_bc_for_bct"):
            regions.append("BC")
        selected, member, _ = statcan_tables.read_selected_table(
            request, regions=list(dict.fromkeys(regions)), geography_rules=rules["geography"],
            chunksize=int(rules["read_chunksize"]), provenance_columns=rules["provenance_columns"],
        )
        normalized[request.table_id] = statcan_tables.normalize_table(
            selected, request=request, source_member=member, scalar_multipliers=multipliers,
        )
        paths.extend([request.archive_cache_path, request.metadata_cache_path])
    history, _ = statcan_tables.build_ldv_history(
        normalized[str(history_rules["archived_table"])], normalized[str(history_rules["current_table"])],
        rules=rules, warnings=warnings,
    )
    return history, normalized["23-10-0308-01"], paths, warnings


def summarize_annual_ledgers(annual: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Keep additions, physical stock and redistribution as distinct quantities."""
    values = [
        "sales_k_vehicles", "surviving_k_vehicles", "calibrated_surviving_k_vehicles",
        "calibrated_acquisitions_k_vehicles", "fixed_allocated_k_vehicles", "deprecated_mto_k_vehicles",
        "deprecated_mto_physical_k_vehicles",
    ]
    required = {*COHORT_KEYS, *values, "stock_calibration_multiplier", "ceud_stock_k_vehicles"}
    if required - set(annual):
        raise ValueError(f"Incomplete diagnostic annual ledger: {sorted(required - set(annual))}")
    if annual.duplicated(COHORT_KEYS).any() or annual[list(required)].isna().any().any():
        raise ValueError("Diagnostic annual ledger has missing or duplicate cohorts")
    if not np.isfinite(annual[values]).all().all() or annual[values].lt(0).any().any():
        raise ValueError("Diagnostic quantities must be finite and nonnegative")
    if not np.allclose(annual.calibrated_surviving_k_vehicles,
                       annual.surviving_k_vehicles * annual.stock_calibration_multiplier):
        raise ValueError("Physical survivors do not reconcile to stock calibration")
    if not np.allclose(annual.calibrated_acquisitions_k_vehicles,
                       annual.sales_k_vehicles * annual.stock_calibration_multiplier):
        raise ValueError("Acquisitions do not retain the same class calibration")
    vintage = annual.groupby(GROUP_KEYS + ["vintage"], as_index=False)[values].sum().sort_values(
        GROUP_KEYS + ["vintage"]
    )
    vintage["redistributed_k_vehicles"] = (
        vintage.fixed_allocated_k_vehicles - vintage.calibrated_surviving_k_vehicles
    )
    vintage["cumulative_redistribution_k_vehicles"] = vintage.groupby(GROUP_KEYS).redistributed_k_vehicles.cumsum()
    for column in ["sales_k_vehicles", "calibrated_acquisitions_k_vehicles"]:
        vintage[f"cumulative_{column}"] = vintage.groupby(GROUP_KEYS)[column].cumsum()
    calibration = annual.groupby(GROUP_KEYS, as_index=False).agg(
        raw_sales_k_vehicles=("sales_k_vehicles", "sum"),
        raw_survivors_k_vehicles=("surviving_k_vehicles", "sum"),
        calibrated_acquisitions_k_vehicles=("calibrated_acquisitions_k_vehicles", "sum"),
        calibrated_survivors_k_vehicles=("calibrated_surviving_k_vehicles", "sum"),
        ceud_stock_k_vehicles=("ceud_stock_k_vehicles", "first"),
        multiplier=("stock_calibration_multiplier", "first"),
        factor_count=("stock_calibration_multiplier", "nunique"),
        target_count=("ceud_stock_k_vehicles", "nunique"),
        history_start=("vintage_year", "min"), history_end=("vintage_year", "max"),
    )
    if not calibration.factor_count.eq(1).all() or not calibration.target_count.eq(1).all():
        raise ValueError("Calibration must have one target and multiplier per region/class")
    calibration["stock_residual_k_vehicles"] = calibration.calibrated_survivors_k_vehicles - calibration.ceud_stock_k_vehicles
    if not np.allclose(calibration.stock_residual_k_vehicles, 0., atol=1e-8):
        raise ValueError("Diagnostic physical stock does not recover its CEUD target")
    calibration["adjustment_percent"] = 100 * (calibration.multiplier - 1)
    allocated = annual.groupby(GROUP_KEYS).fixed_allocated_k_vehicles.sum()
    targets = calibration.set_index(GROUP_KEYS).ceud_stock_k_vehicles
    if not np.allclose(allocated.reindex(targets.index), targets, atol=1e-8):
        raise ValueError("Fixed redistribution does not conserve calibrated class stock")
    mto_physical = annual.groupby(GROUP_KEYS).deprecated_mto_physical_k_vehicles.sum()
    if not np.allclose(mto_physical.reindex(targets.index), targets, atol=1e-8):
        raise ValueError("MTO physical comparison does not recover the same class stock target")
    return {"vintage": vintage.reset_index(drop=True), "calibration": calibration.drop(columns=["factor_count", "target_count"])}


def comparison_lineage(annual: pd.DataFrame, *, physical_survivors: bool) -> pd.DataFrame:
    """Compare methods while explicitly retaining their different input meanings."""
    selected = annual[COHORT_KEYS + [
        "curve_solver_capacity_k_vehicles", "calibrated_surviving_k_vehicles", "deprecated_mto_physical_k_vehicles",
    ]].copy()
    curve_column = "calibrated_surviving_k_vehicles" if physical_survivors else "curve_solver_capacity_k_vehicles"
    sales = selected[COHORT_KEYS + [curve_column]].rename(columns={curve_column: "vehicles_k"})
    sales["method"] = "Sales cohorts"
    sales["basis"] = "Physical survivors" if physical_survivors else "Installed-equivalent acquisitions"
    deprecated = selected[COHORT_KEYS + ["deprecated_mto_physical_k_vehicles"]].rename(
        columns={"deprecated_mto_physical_k_vehicles": "vehicles_k"}
    )
    deprecated["method"], deprecated["basis"] = "Deprecated MTO age allocation", "Physical survivors before fixed redistribution"
    return pd.concat([sales, deprecated], ignore_index=True)


def historical_retention(
    acquisitions: pd.DataFrame, schedule: pd.DataFrame, *, years: list[int], allow_unknown: bool = False,
) -> pd.DataFrame:
    """Account for historical cohorts at future dates; never recalibrate to future stock.

    This is direct cohort accounting using the production owner's reconciled
    annual schedule. There are no future sales, demand fits or solver claims.
    """
    rows = []
    for road_class, curve in schedule.groupby("road_class"):
        survival = curve.set_index("age").survival_probability.sort_index()
        if survival.index.duplicated().any() or (not allow_unknown and survival.iloc[-1] != 0):
            raise ValueError("Production retention requires an explicit terminal zero")
        source = acquisitions.loc[acquisitions.road_class.eq(road_class)]
        for year in years:
            ages = year - source.vintage_year
            if ages.lt(0).any():
                raise ValueError("Historical projection cannot precede an acquisition")
            fractions = ages.map(survival)
            if survival.iloc[-1] == 0:
                fractions = fractions.mask(ages.gt(survival.index.max()), 0.)
            fractions = fractions.mask(source.calibrated_acquisitions_k_vehicles.eq(0), 0.)
            if fractions.isna().any() and not allow_unknown:
                raise ValueError("Production schedule has missing annual ages")
            retained = source.assign(retained_k_vehicles=source.calibrated_acquisitions_k_vehicles * fractions)
            totals = retained.groupby("region", as_index=False).agg(
                retained_k_vehicles=("retained_k_vehicles", lambda values: values.sum() if values.notna().all() else np.nan),
                complete_support=("retained_k_vehicles", lambda values: values.notna().all()),
            )
            totals["road_class"], totals["calendar_year"] = road_class, year
            rows.append(totals)
    return pd.concat(rows, ignore_index=True)


def retirement_projection(
    acquisitions: pd.DataFrame, native: pd.DataFrame, production: pd.DataFrame,
    tail_losses: pd.DataFrame, *, years: list[int],
) -> pd.DataFrame:
    """Compare native evidence with production, including its exact tail-loss audit."""
    keys = ["region", "road_class", "calendar_year"]
    result = historical_retention(acquisitions, production, years=years).rename(
        columns={"retained_k_vehicles": "source_survivors_k_vehicles"}).drop(columns="complete_support")
    original = historical_retention(acquisitions, native, years=years, allow_unknown=True).rename(
        columns={"retained_k_vehicles": "native_survivors_k_vehicles", "complete_support": "native_complete"})
    losses = tail_losses.rename(columns={"period": "calendar_year"}).groupby(keys, as_index=False).removed_k_vehicles.sum()
    result = result.merge(original, on=keys, validate="one_to_one").merge(losses, on=keys, how="left", validate="one_to_one")
    result["removed_k_vehicles"] = result.removed_k_vehicles.astype(float).fillna(0.)
    result["retained_k_vehicles"] = result.source_survivors_k_vehicles - result.removed_k_vehicles
    if result.retained_k_vehicles.lt(-1e-9).any() or result.removed_k_vehicles.lt(0).any():
        raise ValueError("Production tail losses exceed available historical stock")
    result["retained_k_vehicles"] = result.retained_k_vehicles.clip(lower=0.)
    result["native_difference_k_vehicles"] = result.retained_k_vehicles - result.native_survivors_k_vehicles
    return result


def reference_survival_comparison(sales: pd.DataFrame, native: pd.DataFrame) -> pd.DataFrame:
    """Expose reference calibration changes without treating calibration as validation."""
    joined = sales.merge(native[["road_class", "age", "survival_probability"]].rename(
        columns={"survival_probability": "native_survival_probability"}),
        on=["road_class", "age"], how="left", validate="many_to_one")
    joined["native_surviving_k_vehicles"] = (joined.sales_k_vehicles * joined.native_survival_probability).mask(
        joined.sales_k_vehicles.eq(0), 0.)
    rows = []
    for (region, road_class), group in joined.groupby(["ceud_region", "road_class"]):
        complete = group.native_surviving_k_vehicles.notna().all()
        native_total = group.native_surviving_k_vehicles.sum() if complete else np.nan
        target = float(group.ceud_stock_k_vehicles.iloc[0])
        native_multiplier = target / native_total if complete and native_total > 0 else np.nan
        difference = group.calibrated_surviving_k_vehicles - group.native_surviving_k_vehicles * native_multiplier
        rows.append({"region": region, "road_class": road_class, "native_complete": bool(complete),
                     "native_raw_survivors_k_vehicles": native_total,
                     "production_raw_survivors_k_vehicles": group.surviving_k_vehicles.sum(),
                     "native_multiplier": native_multiplier,
                     "production_multiplier": group.stock_calibration_multiplier.iloc[0],
                     "max_cohort_stock_difference_k_vehicles": difference.abs().max() if complete else np.nan})
    return pd.DataFrame(rows)


def _profile_bundle(bundle: ConfigBundle, profile: str) -> ConfigBundle:
    lifetimes = bundle.scenario.lifetimes.model_copy(update={
        "road_lifetime_survival_source": profile,
    })
    return replace(bundle, scenario=bundle.scenario.model_copy(update={"lifetimes": lifetimes}))


def _five_class_rules(bundle: ConfigBundle) -> dict[str, Any]:
    rules = resolved_road_lifetime_rules(bundle)
    classes = set(rules["ceud_sales_series"])
    return {**rules, "ceud_stock_series": {key: value for key, value in rules["ceud_stock_series"].items() if key in classes},
            "fixed_lifetime_sources": {key: value for key, value in rules["fixed_lifetime_sources"].items() if key in classes}}


def load_stock_cohort_diagnosis(
    scenario_path: str | Path = "config/scenarios/default.yaml", *, repo_root: Path | None = None,
) -> dict[str, Any]:
    """Recompute diagnostics from registered evidence; never build, fetch or publish."""
    from parameterization.road_cohort_survival import build_composite_cohorts
    from parameterization.road_lifetimes_survival import (
        harmonized_road_survival_schedules,
        require_tail_derivation,
        resolved_survival_max_age,
        selected_road_survival_schedules,
    )

    bundle = load_config_bundle(scenario_path, repo_root=repo_root)
    tail_settings = require_tail_derivation(bundle)
    if bundle.scenario.sources.nrcan_ceud_beyond_observed != "constant":
        raise ValueError("Stock/cohort diagnosis requires the constant CEUD observation-tail policy")
    road_family = load_harmonization_rules(bundle, "road_stocks_and_demands")
    lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")
    road_rules = load_harmonization_rules(bundle, "road_aggregation")
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    regions = list(bundle.scenario.geography.regions)
    provincial = pd.concat([read_ceud_input(bundle, region) for region in regions], ignore_index=True)
    edition = bundle.scenario.sources.selections["nrcan_ceud_transport_provincial"].year
    if not provincial.ceud_source_endpoint_year.eq(edition).all():
        raise ValueError("Diagnostic CEUD source endpoint differs from the selected registered edition")
    source_paths = [ceud_input_path(bundle, region) for region in regions]
    source_paths.extend(resolve_input_path(bundle, "interim", assorted["interim_subdir"], assorted[key]["output_file"])
                        for key in ["nhtsa_cafe", "eia_nems", "epa_moves5"])
    ldv, trucks, fuel_paths, warnings = _read_fuel_evidence(bundle)
    source_paths.extend(fuel_paths)
    mapped_path = resolve_artifact_path(bundle, "road_aggregation") / road_rules["mapped_current_stock_file"]
    mapped = pd.read_csv(mapped_path, low_memory=False)
    mto_rules = load_harmonization_rules(bundle, "ontario_vehicle_population")
    report5_path = resolve_artifact_path(bundle, "ontario_vehicle_population") / mto_rules["reports"][5][
        "distribution_output_template"].format(year=bundle.scenario.existing_capacity.vehicle_population_year)
    report5 = pd.read_csv(report5_path)
    dashboard_path = resolve_artifact_path(bundle, "tc_ev_dashboard_interim") / assorted["tc_ev_dashboard"]["output_file"]
    dashboard = pd.read_csv(dashboard_path)
    source_paths.extend([mapped_path, report5_path, dashboard_path,
                         resolve_artifact_path(bundle, "road_aggregation", road_rules["nlr_weights_file"])])
    annual_frames, composition_frames, production_audits, production_curves, projections = [], [], [], [], []
    vintage_audits, reference_comparisons = [], []
    profile_frames = {}
    for profile in road_family["existing_capacity"]["survival_source_profiles"]:
        selected_bundle = _profile_bundle(bundle, profile)
        rules = _five_class_rules(selected_bundle)
        frames = derive_accepted_lifetime_frames(
            selected_bundle, rules=lifetime_rules, road_rules=road_rules, assorted_rules=assorted,
        )
        selected = selected_road_survival_schedules(selected_bundle, frames["transformed_curves"])
        profile_frames[profile] = selected.assign(profile=profile)
        harmonized = harmonized_road_survival_schedules(selected_bundle, frames["transformed_curves"])
        maximum_age = resolved_survival_max_age(selected_bundle, harmonized.annual)
        production_curves.append(harmonized.annual.assign(profile=profile))
        production_audits.append(harmonized.audit.assign(profile=profile))
        lifetimes = fixed_existing_lifetimes(frames["medians"], pd.DataFrame(), rules)
        sales = derive_sales_survival_cohorts(
            provincial, harmonized.annual, regions=regions,
            base_year=bundle.scenario.periods.base_year,
            history_start=min(bundle.scenario.periods.existing), rules=rules,
        )
        reference_comparisons.append(reference_survival_comparison(sales, selected).assign(profile=profile))
        ages, _ = derive_ldv_age_distributions(
            mapped, survival_curves=False, maximum_age=maximum_age,
            median_lifetimes={}, historical_review_minimum_model_year=int(
                road_family["ontario_report_a"]["historical_review_minimum_model_year"]),
            period_config=bundle.scenario.periods,
        )
        common = dict(
            provincial=provincial, ldv_age=ages, report5_age=report5,
            ldv_registrations=ldv, truck_registrations=trucks, dashboard=dashboard,
            regions=regions, period_config=bundle.scenario.periods,
            vehicle_population_year=bundle.scenario.existing_capacity.vehicle_population_year,
            survival_curves=False, survival_curve_max_age=maximum_age,
            fixed_lifetimes_by_class=lifetimes, curve_ages_by_class={}, rules=rules,
        )
        off_cohorts, off_shares, _, _ = distribute_existing_road_capacity(**common, sales_cohorts=sales)
        _, old_shares, _, _ = distribute_existing_road_capacity(**common, cohort_method="stock_age")
        group = ["ceud_region", "road_class", "vintage", "vintage_year"]
        off = off_shares.groupby(group, as_index=False).capacity.sum().rename(columns={"capacity": "fixed_allocated_k_vehicles"})
        old = old_shares.groupby(group, as_index=False)[["capacity", "raw_capacity"]].sum().rename(columns={
            "capacity": "deprecated_mto_k_vehicles", "raw_capacity": "deprecated_mto_physical_k_vehicles"})
        annual = sales.merge(off_cohorts[group].drop_duplicates(), on=["ceud_region", "road_class", "vintage_year"],
                             how="left", validate="one_to_one").merge(off, on=group, how="left", validate="one_to_one")
        annual = annual.merge(old, on=group, how="outer", validate="one_to_one")
        # Deprecated ages may include a source-year not represented in sales. Keep
        # those rows visible; their sales quantities are explicitly zero.
        for column in ["sales_k_vehicles", "surviving_k_vehicles", "calibrated_surviving_k_vehicles",
                       "calibrated_acquisitions_k_vehicles", "fixed_allocated_k_vehicles", "deprecated_mto_k_vehicles",
                       "deprecated_mto_physical_k_vehicles"]:
            annual[column] = annual[column].fillna(0.)
        for column in ["stock_calibration_multiplier", "ceud_stock_k_vehicles"]:
            annual[column] = annual.groupby(["ceud_region", "road_class"])[column].transform(lambda values: values.ffill().bfill())
        annual["profile"], annual["region"] = profile, annual.ceud_region
        for method, shares, quantity in [("Sales fixed mode", off_shares, "capacity"),
                                         ("Sales curve-mode survivors", off_shares, "raw_capacity"),
                                         ("Deprecated MTO age allocation", old_shares, "raw_capacity")]:
            composition = shares.groupby(["ceud_region", "road_class", "tech"], as_index=False)[quantity].sum()
            composition["profile"], composition["method"] = profile, method
            composition_frames.append(composition.rename(columns={"ceud_region": "region", quantity: "physical_stock_k_vehicles"}))
        combined = build_composite_cohorts(
            off_shares[["region", "road_class", "tech", "vintage_year", "vintage", "calibrated_acquisitions_k_vehicles"]],
            harmonized.annual, reference_year=bundle.scenario.periods.base_year,
            first_model_period=bundle.scenario.periods.model[0],
        )
        vintage_audits.append(combined.audit.assign(profile=profile))
        solver_annual = combined.physical_stock.groupby(
            ["region", "road_class", "vintage", "vintage_year"], as_index=False,
        ).installed_equivalent_k_vehicles.sum().rename(
            columns={"installed_equivalent_k_vehicles": "curve_solver_capacity_k_vehicles"})
        output_to_ceud = {value: key for key, value in rules["region_output_map"].items()}
        solver_annual["region"] = solver_annual.region.replace(output_to_ceud)
        annual = annual.merge(solver_annual, on=["region", "road_class", "vintage", "vintage_year"],
                              how="left", validate="one_to_one")
        if annual.curve_solver_capacity_k_vehicles.isna().any():
            raise ValueError("Production curve calculation lacks an annual installation basis")
        tail_losses = combined.tail_losses.assign(region=lambda frame: frame.region.replace(output_to_ceud))
        projected = retirement_projection(
            annual, selected, harmonized.annual, tail_losses,
            years=list(range(bundle.scenario.periods.base_year,
                             bundle.scenario.periods.model[-1] + bundle.scenario.periods.step + 1)),
        )
        projections.append(projected.assign(profile=profile))
        annual_frames.append(annual)
    annual = pd.concat(annual_frames, ignore_index=True).sort_values(COHORT_KEYS).reset_index(drop=True)
    result = {"annual": annual, **summarize_annual_ledgers(annual),
              "composition": pd.concat(composition_frames, ignore_index=True),
              "source_curves": pd.concat(profile_frames.values(), ignore_index=True),
              "production_survival_audit": pd.concat(production_audits, ignore_index=True),
              "production_curves": pd.concat(production_curves, ignore_index=True),
              "historical_projection": pd.concat(projections, ignore_index=True),
              "vintage_survival_audit": pd.concat(vintage_audits, ignore_index=True),
              "reference_survival_comparison": pd.concat(reference_comparisons, ignore_index=True)}
    result["metadata"] = {
        "scenario_path": str(bundle.scenario_path), "scenario": bundle.scenario.model_dump(mode="json"),
        "reference_year": bundle.scenario.periods.base_year, "regions": regions,
        "selected_profile": bundle.scenario.lifetimes.road_lifetime_survival_source,
        "warnings": warnings, "files": [{"path": str(path), "sha256": file_sha256(path)} for path in sorted(set(source_paths))],
        "survival_treatment": tail_settings,
        "tail_survival_threshold": tail_settings["threshold"],
    }
    return result
