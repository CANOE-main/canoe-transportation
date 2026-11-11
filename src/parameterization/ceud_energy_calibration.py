"""Optional class-level liquid-energy calibration; source observations stay intact.

The fitted target excludes fuels not represented by the existing vehicle mix.
One scalar is transferred to every powertrain in a supported class. That transfer
preserves relative trajectories and shared PHEV blends; it is not evidence that
electric or hydrogen consumption has been independently calibrated.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


KEYS = ["region", "tech", "vintage"]


@dataclass(frozen=True)
class EnergyCalibration:
    summary: pd.DataFrame
    cohorts: pd.DataFrame
    trajectories: pd.DataFrame
    annual: pd.DataFrame
    periods: pd.DataFrame
    payloads: list[dict]


def validate_emitted_calibration(frames: dict[str, pd.DataFrame], *,
                                c2a: pd.DataFrame | None = None,
                                factors: pd.DataFrame | None = None) -> dict:
    """Check fitted targets against final contribution/SQLite parameter values."""
    summary, cohorts, efficiencies, splits = (frames[name] for name in ("summary", "cohorts", "efficiency", "splits"))
    if "liquid_target_basis" in summary:
        aggregate = summary.loc[summary.liquid_target_basis.eq("reported_diesel_total")]
        if (not aggregate.status.eq("calibrate").all()
                or not np.array_equal(aggregate.ceud_liquid_target_pj, aggregate.ceud_total_energy_pj)
                or not aggregate.excluded_reported_energy_pj.eq(0).all()
                or not aggregate.other_or_unresolved_energy_pj.eq(0).all()):
            raise ValueError("Emitted CEUD diesel-total boundary differs from its reported aggregate")
    keys = [*KEYS, "input_comm", "output_comm"]
    actual = cohorts.merge(efficiencies[keys + ["efficiency"]], on=keys, how="left", validate="one_to_one")
    if actual.efficiency.isna().any() or not np.allclose(actual.efficiency, actual.calibrated_efficiency, rtol=1e-10, atol=1e-12):
        raise ValueError("CEUD calibration differs from emitted vehicle efficiency rows")
    if (c2a is None) != (factors is None):
        raise ValueError("Final CEUD calibration requires both activity parameter tables")
    if c2a is not None:
        actual = actual.merge(c2a[["region", "tech", "c2a"]].rename(columns={"c2a": "emitted_c2a"}),
                              on=["region", "tech"], how="left", validate="many_to_one")
        equality = factors.loc[factors.operator.eq("e")].rename(columns={"tech_or_group": "tech", "factor": "emitted_utilization"})
        actual = actual.merge(equality[[*KEYS, "output_comm", "emitted_utilization"]],
                              on=[*KEYS, "output_comm"], how="left", validate="one_to_one")
        emitted_activity = actual.surviving_k_vehicles * actual.emitted_c2a * actual.emitted_utilization
        if not np.allclose(emitted_activity, actual.activity_bn, rtol=1e-10, atol=1e-9):
            raise ValueError("Final CEUD calibration activity changed after fitting")
        actual["activity_bn"] = emitted_activity
    for row in cohorts.loc[cohorts.phev_blend_tech.notna()].itertuples():
        liquid = splits.loc[splits.region.eq(row.region) & splits.tech.eq(row.phev_blend_tech)
                            & splits.input_comm.eq(row.phev_liquid_input)]
        if liquid.empty or not np.allclose(liquid.proportion, row.liquid_energy_share, rtol=0, atol=1e-12):
            raise ValueError("CEUD calibration used a different PHEV liquid share from emitted splits")
    if not np.allclose(splits.groupby(["region", "period", "tech"]).proportion.sum(), 1, rtol=0, atol=1e-12):
        raise ValueError("Emitted PHEV input splits do not conserve energy")
    actual["emitted_liquid_pj"] = actual.activity_bn / actual.efficiency * actual.liquid_energy_share
    totals = actual.groupby(["region", "road_class"]).emitted_liquid_pj.sum()
    if any(not np.isclose(totals.loc[(row.region, row.road_class)], row.calibrated_liquid_energy_pj,
                          rtol=1e-10, atol=1e-9) for row in summary.itertuples()):
        raise ValueError("CEUD liquid totals differ from emitted efficiency accounting")
    fitted = summary.loc[summary.status.eq("calibrate")]
    if not np.allclose(fitted.calibrated_liquid_energy_pj.to_numpy(dtype=float),
                       fitted.ceud_liquid_target_pj.to_numpy(dtype=float), rtol=1e-9, atol=1e-8):
        raise ValueError("Emitted CEUD liquid targets do not close")
    residuals = [abs(totals.loc[(row.region, row.road_class)] - row.ceud_liquid_target_pj) for row in fitted.itertuples()]
    return {"cohort_rows": len(actual), "fitted_region_classes": len(fitted),
            "maximum_liquid_residual_pj": max(residuals, default=0.),
            "activity_unchanged": True, "phev_splits_preserved": True}


def _unique(frame: pd.DataFrame, keys: list[str], name: str) -> None:
    if frame.empty or frame.duplicated(keys).any():
        raise ValueError(f"Missing or duplicate CEUD calibration {name}")


def _target(ceud: pd.DataFrame, region: str, year: int, table: int,
            series: str, unit: str) -> pd.Series:
    selected = ceud.loc[ceud.region.eq(region) & ceud.year.eq(year)
                        & ceud.table_id.eq(table) & ceud.raw_series.eq(series)]
    if len(selected) != 1 or selected.iloc[0].unit != unit:
        raise ValueError(f"Missing, duplicate or wrong-unit CEUD calibration target: {region}/{year}/{series}")
    row = selected.iloc[0]
    if (not np.isfinite(row.value) or row.value < 0
            or row.source_id != "nrcan_ceud_transport_provincial"
            or row.ceud_value_status not in {"observed", "constant", "extrapolated"}):
        raise ValueError(f"Invalid CEUD calibration target: {region}/{series}")
    return row


def calibrate_consumption(
    *, annual: pd.DataFrame, periods: pd.DataFrame, payloads: list[dict],
    physical_stock: pd.DataFrame, c2a: pd.DataFrame, factors: pd.DataFrame,
    split_audit: pd.DataFrame, ceud: pd.DataFrame, relationships: pd.DataFrame,
    activity_targets: pd.DataFrame,
    base_year: int, source_edition: int, regions: list[str],
    calibration_rules: dict, stock_rules: dict, demand_rules: dict,
    efficiency_rules: dict,
) -> EnergyCalibration:
    """Fit comparable liquid energy on physical stock at unchanged class activity.

Scaling native consumption by a class-constant k commutes with the existing
linear temporal/range means and constant-load service conversion. Both annual
and period evidence retain the unadjusted values; final row efficiency is
load/(k*consumption), before any new-vehicle representative aggregation.
"""
    policies = {
        "method": "class_liquid_energy_ratio",
        "transfer": "uniform_all_powertrains_and_years",
        "phev_split": "preserve_emitted_horizon_mean",
        "electricity_target": "unobserved",
        "energy_boundary": "vehicle_input_without_additional_charger_losses",
    }
    if any(calibration_rules.get(key) != value for key, value in policies.items()):
        raise ValueError("Unsupported CEUD energy-calibration policy")
    if not isinstance(source_edition, int) or source_edition <= 0:
        raise ValueError("CEUD energy calibration requires an explicit source edition")
    atol, rtol = (float(calibration_rules["tolerances"][key]) for key in ("absolute", "relative"))
    resolution = float(calibration_rules["reported_energy_resolution_pj"])
    if not np.isfinite(resolution) or resolution <= 0:
        raise ValueError("CEUD reported energy resolution must be finite and positive")
    classes = calibration_rules["classes"]
    _unique(activity_targets, ["region", "road_class"], "projected activity targets")
    expected_targets = {(stock_rules["region_output_map"].get(region, region), name)
                        for region in regions for name in classes}
    selected_targets = activity_targets.loc[activity_targets.road_class.isin(classes)]
    if (set(selected_targets[["region", "road_class"]].itertuples(index=False, name=None)) != expected_targets
            or not selected_targets.base_year.eq(base_year).all()
            or not np.isfinite(selected_targets.target_activity_bn).all()
            or selected_targets.target_activity_bn.le(0).any()):
        raise ValueError("CEUD energy calibration lacks complete projected base-year activity targets")
    targets = selected_targets.set_index(["region", "road_class"]).target_activity_bn
    owner = {tech: name for name in classes for tech in stock_rules["fuel_technology"][name].values()}
    selected = physical_stock.loc[physical_stock.tech.isin(owner)].copy()
    selected["road_class"] = selected.tech.map(owner)
    _unique(selected, KEYS, "physical-stock cohorts")
    if set(selected.units) != {"k vehicles"} or not selected.base_year.eq(base_year).all():
        raise ValueError("CEUD calibration requires base-year physical stock in k vehicles")
    service = {name: demand_rules["activity_series"][name]["commodity"] for name in classes}
    selected["output_comm"] = selected.road_class.map(service)
    activity = c2a.loc[c2a.tech.isin(owner), ["region", "tech", "c2a"]]
    _unique(activity, ["region", "tech"], "capacity-to-activity")
    uf = factors.loc[factors.tech_or_group.isin(owner) & factors.operator.eq("e")].rename(
        columns={"tech_or_group": "tech", "factor": "annual_utilization"})
    _unique(uf, [*KEYS, "output_comm"], "flat equality utilization")
    efficiency = pd.DataFrame(payloads)
    efficiency = efficiency.loc[efficiency.tech.isin(owner) & efficiency.output_comm.isin(service.values())]
    _unique(efficiency, [*KEYS, "output_comm"], "vehicle service efficiencies")
    detail = selected.merge(activity, on=["region", "tech"], how="left", validate="many_to_one")
    detail = detail.merge(uf[[*KEYS, "output_comm", "annual_utilization"]],
                          on=[*KEYS, "output_comm"], how="left", validate="one_to_one")
    detail = detail.merge(efficiency[[*KEYS, "output_comm", "input_comm", "efficiency", "units"]].rename(
        columns={"efficiency": "unadjusted_efficiency", "units": "efficiency_units"}),
        on=[*KEYS, "output_comm"], how="left", validate="one_to_one")
    detail = detail.merge(relationships.loc[relationships.tech.isin(owner), ["tech", "powertrain"]].drop_duplicates(),
                          on="tech", how="left", validate="many_to_one")
    numeric = ["surviving_k_vehicles", "c2a", "annual_utilization", "unadjusted_efficiency"]
    if (detail[numeric].isna().any().any() or not np.isfinite(detail[numeric]).all().all()
            or detail[numeric].le(0).any().any() or detail.annual_utilization.gt(1).any()):
        raise ValueError("CEUD calibration requires complete positive physical activity and efficiency")
    expected_units = detail.road_class.map({name: demand_rules["activity_series"][name]["units"] + "/PJ" for name in classes})
    if not detail.efficiency_units.eq(expected_units).all():
        raise ValueError("CEUD calibration service units differ from vehicle efficiencies")
    blend_rules = {**efficiency_rules["phev_blends"], **{
        "existing_" + name: {**efficiency_rules["phev_blends"]["ldv35"], **spec}
        for name, spec in efficiency_rules["existing_phev_blends"].items()}}
    blend_by_output = {spec["output"]: name for name, spec in blend_rules.items()}
    shares = split_audit.groupby(["region", "blend", "year"]).electricity_share.first().groupby(
        ["region", "blend"]).mean()
    output_to_source = {stock_rules["region_output_map"].get(region, region): region for region in regions}
    liquid, blend_techs, blend_fuels = [], [], []
    for row in detail.itertuples():
        blend_techs.append(None)
        blend_fuels.append(None)
        if row.powertrain in calibration_rules["liquid_powertrains"]:
            liquid.append(1.0)
        elif row.powertrain.startswith("phev"):
            key = (output_to_source[row.region], blend_by_output.get(row.input_comm))
            if key not in shares or not 0 <= shares.loc[key] <= 1:
                raise ValueError(f"Missing emitted PHEV energy split for calibration: {key}")
            liquid.append(1 - float(shares.loc[key]))
            blend_techs[-1] = blend_rules[key[1]]["tech"]
            blend_fuels[-1] = blend_rules[key[1]]["fuel"]
        elif row.powertrain.startswith("bev"):
            liquid.append(0.0)
        else:
            raise ValueError(f"Unsupported existing fuel in CEUD calibration: {row.powertrain}")
    detail["liquid_energy_share"] = liquid
    detail["phev_blend_tech"] = blend_techs
    detail["phev_liquid_input"] = blend_fuels
    detail["activity_bn"] = detail.surviving_k_vehicles * detail.c2a * detail.annual_utilization
    detail["unadjusted_vehicle_energy_pj"] = detail.activity_bn / detail.unadjusted_efficiency
    detail["unadjusted_liquid_energy_pj"] = detail.unadjusted_vehicle_energy_pj * detail.liquid_energy_share
    detail["unadjusted_electric_energy_pj"] = detail.unadjusted_vehicle_energy_pj * (1 - detail.liquid_energy_share)
    summaries = []
    multipliers = {}
    for native_region in regions:
        region = stock_rules["region_output_map"].get(native_region, native_region)
        for name, spec in classes.items():
            target_basis = spec.get("liquid_target_basis", "reported_fuel_components")
            liquid_series, excluded_series = spec["liquid_series"], spec["excluded_series"]
            if (len(set(liquid_series)) != len(liquid_series)
                    or len(set(excluded_series)) != len(excluded_series)
                    or set(liquid_series).intersection(excluded_series)):
                raise ValueError(f"Duplicate or overlapping CEUD energy selectors: {name}")
            if target_basis == "reported_diesel_total":
                if (spec["status"] != "calibrate" or liquid_series != [spec["total_series"]]
                        or excluded_series):
                    raise ValueError(f"CEUD diesel-total boundary must select only the class total: {name}")
            elif target_basis != "reported_fuel_components" or spec["total_series"] in [*liquid_series, *excluded_series]:
                raise ValueError(f"Unsupported or mixed CEUD liquid target boundary: {name}")
            group = detail.loc[detail.region.eq(region) & detail.road_class.eq(name)]
            if group.empty:
                raise ValueError(f"Missing CEUD calibration class: {region}/{name}")
            if (target_basis == "reported_diesel_total"
                    and not group.input_comm.eq(efficiency_rules["input_commodities"]["diesel"]).all()):
                raise ValueError(f"CEUD diesel-total boundary requires diesel-only existing energy inputs: {region}/{name}")
            stock_spec = stock_rules["ceud_stock_series"][name]
            activity_spec = demand_rules["activity_series"][name]
            stock = _target(ceud, native_region, base_year, stock_spec["table_id"], stock_spec["raw_series"], "thousands")
            activity_target = _target(ceud, native_region, base_year, activity_spec["table_id"], activity_spec["raw_series"], "millions")
            total = _target(ceud, native_region, base_year, spec["table_id"], spec["total_series"], "PJ")
            fuel_rows = ([total] if target_basis == "reported_diesel_total" else [
                _target(ceud, native_region, base_year, spec["table_id"], series, "PJ") for series in liquid_series])
            excluded = [_target(ceud, native_region, base_year, spec["table_id"], series, "PJ") for series in excluded_series]
            target_rows = [stock, activity_target, total, *([] if target_basis == "reported_diesel_total" else fuel_rows), *excluded]
            identities = {(int(row.ceud_observed_year), str(row.ceud_value_status)) for row in target_rows}
            if len(identities) != 1 or any(int(row.ceud_observed_year) > min(base_year, source_edition) for row in target_rows):
                raise ValueError(f"Incompatible CEUD calibration observation years or statuses: {region}/{name}")
            observed_year, observation_status = next(iter(identities))
            ceud_activity = float(activity_target.value) * demand_rules["activity_to_billion_factor"]
            target_activity = float(targets.loc[(region, name)])
            actual_activity = float(group.activity_bn.sum())
            if not np.isclose(actual_activity, target_activity, rtol=rtol, atol=atol):
                raise ValueError(f"CEUD energy calibration requires unchanged calibrated activity: {region}/{name}")
            target = float(sum(row.value for row in fuel_rows)) if fuel_rows else None
            excluded_energy = float(sum(row.value for row in excluded))
            reported_residual = float(total.value) - (target or 0.) - excluded_energy
            rounding_allowance = (0. if target_basis == "reported_diesel_total"
                                  else (len(fuel_rows) + len(excluded) + 1) * resolution / 2)
            unresolved = 0. if abs(reported_residual) <= rounding_allowance + atol else reported_residual
            unadjusted = float(group.unadjusted_liquid_energy_pj.sum())
            if spec["status"] == "calibrate":
                if target is None or target <= 0 or unadjusted <= 0 or not np.isfinite(unadjusted):
                    raise ValueError(f"Nonpositive CEUD liquid energy target or denominator: {region}/{name}")
                if target > float(total.value) + (len(fuel_rows) + 1) * resolution / 2 + atol:
                    raise ValueError(f"CEUD covered fuels exceed class total: {region}/{name}")
                if reported_residual < -rounding_allowance - atol:
                    raise ValueError(f"CEUD selected fuel components exceed class total: {region}/{name}")
                multiplier = target / unadjusted
            elif spec["status"] == "unsupported_fuel_boundary" and not fuel_rows:
                multiplier = 1.0
            else:
                raise ValueError(f"Unsupported CEUD calibration class status: {name}")
            if not np.isfinite(multiplier) or multiplier <= 0:
                raise ValueError(f"Nonphysical CEUD calibration factor: {region}/{name}")
            multipliers[region, name] = multiplier
            summaries.append({
                "region": region, "ceud_region": native_region, "road_class": name,
                "base_year": base_year, "source_edition": source_edition,
                "observed_year": observed_year, "observation_status": observation_status,
                "status": spec["status"], "ceud_stock_k_vehicles": float(stock.value),
                "liquid_target_basis": target_basis,
                "physical_stock_k_vehicles": float(group.surviving_k_vehicles.sum()),
                "ceud_activity_bn": ceud_activity, "target_activity_bn": target_activity,
                "activity_target_basis": str(selected_targets.loc[
                    selected_targets.region.eq(region) & selected_targets.road_class.eq(name)
                ].iloc[0].get("target_basis", "projected_base_year_demand")),
                "activity_bn": actual_activity,
                "activity_residual_bn": actual_activity - target_activity,
                "ceud_total_energy_pj": float(total.value), "ceud_liquid_target_pj": target,
                "excluded_reported_energy_pj": excluded_energy,
                "reported_energy_balance_residual_pj": reported_residual,
                "source_rounding_allowance_pj": rounding_allowance,
                "other_or_unresolved_energy_pj": unresolved,
                "unadjusted_liquid_energy_pj": unadjusted,
                "calibrated_liquid_energy_pj": unadjusted * multiplier,
                "liquid_residual_pj": unadjusted * multiplier - target if target is not None else None,
                "consumption_multiplier": multiplier,
                "unadjusted_electric_energy_pj": float(group.unadjusted_electric_energy_pj.sum()),
                "calibrated_electric_energy_pj": float(group.unadjusted_electric_energy_pj.sum()) * multiplier,
                "electricity_target_status": ("zero_within_confirmed_diesel_total_not_fitted"
                                              if target_basis == "reported_diesel_total" else "unobserved_not_zero"),
                "full_class_residual_pj": float(group.unadjusted_vehicle_energy_pj.sum()) * multiplier - float(total.value),
                "boundary": calibration_rules["energy_boundary"],
            })
    detail["consumption_multiplier"] = [multipliers[region, name] for region, name in detail[["region", "road_class"]].itertuples(index=False, name=None)]
    for fuel in ("vehicle", "liquid", "electric"):
        detail[f"calibrated_{fuel}_energy_pj"] = detail[f"unadjusted_{fuel}_energy_pj"] * detail.consumption_multiplier
    detail["calibrated_efficiency"] = detail.unadjusted_efficiency / detail.consumption_multiplier

    def scale(frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy(deep=True)
        result["ceud_consumption_multiplier"] = [multipliers.get((region, mode), 1.0)
                                                   for region, mode in result[["region", "mode"]].itertuples(index=False, name=None)]
        result["unadjusted_native_intensity"] = result.native_intensity
        result["native_intensity"] = result.native_intensity * result.ceud_consumption_multiplier
        if "efficiency" in result:
            result["unadjusted_efficiency"] = result.efficiency
            result["efficiency"] = result.load_factor / result.native_intensity
        return result

    scaled_annual, scaled_periods = scale(annual), scale(periods)
    scaled_payloads = [{**row, "efficiency": row["efficiency"] / multipliers.get((row["region"], row["mode"]), 1.0)} for row in payloads]
    trajectory_columns = [*KEYS, "mode", "powertrain", "input_comm", "output_comm", "units",
                          "native_unit", "load_factor", "unadjusted_native_intensity", "native_intensity",
                          "unadjusted_efficiency", "efficiency", "ceud_consumption_multiplier"]
    trajectories = scaled_periods.loc[scaled_periods["mode"].isin(classes), trajectory_columns].copy()
    summary = pd.DataFrame(summaries)
    fitted = summary.loc[summary.status.eq("calibrate")]
    if not np.allclose(fitted.calibrated_liquid_energy_pj.to_numpy(dtype=float),
                       fitted.ceud_liquid_target_pj.to_numpy(dtype=float), rtol=rtol, atol=atol):
        raise ValueError("CEUD liquid-energy calibration did not close")
    return EnergyCalibration(summary, detail, trajectories, scaled_annual, scaled_periods, scaled_payloads)
