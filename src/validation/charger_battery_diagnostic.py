"""Read-only truck battery and activity evidence for charger-duration proposals."""

from dataclasses import dataclass
from math import isfinite

import pandas as pd

from fetching.cer_enerfuture import configured_edition, configured_scenario
from fetching.nlr_atb_autonomie import ATB_SOURCE_ID, configured_trajectory
from parameterization.build_demand import real_gdp_indices, road_base_year_activity
from parameterization.ceud_inputs import ceud_input_path
from parameterization.road_fleet_weights import load_fleet_aggregation_evidence
from utils import (
    file_sha256, harmonization_rule_paths, load_harmonization_rules, resolve_input_path,
)


TRUCK_CLASSES = ("medium_trucks", "heavy_trucks")


@dataclass(frozen=True)
class ChargerBatteryEvidence:
    class_batteries: pd.DataFrame
    activity_weights: pd.DataFrame
    pooled_battery: pd.DataFrame
    component_audit: pd.DataFrame
    metadata: dict


def select_battery_rows(vehicles: pd.DataFrame, *, years: list[int], trajectory: str,
                        metric_key: str, weight_category: str, vehicle_detail: str) -> pd.DataFrame:
    """Require actual usable-battery source points, without interpolation or holds."""
    required = {"year", "trajectory", "metric_key", "vehicle_weight_category",
                "vehicle_class", "vehicle_detail", "value", "unit", "source_id"}
    if required - set(vehicles):
        raise ValueError(f"ATB battery evidence lacks columns: {sorted(required - set(vehicles))}")
    selected = vehicles.loc[
        vehicles.trajectory.eq(trajectory) & vehicles.metric_key.eq(metric_key)
        & vehicles.vehicle_weight_category.eq(weight_category)
        & vehicles.vehicle_detail.str.strip().eq(vehicle_detail)
        & vehicles.year.isin(years)
    ].copy()
    if selected.empty or set(selected.year) != set(years):
        raise ValueError("ATB usable-battery evidence lacks an exact requested source year")
    if selected.duplicated(["year", "vehicle_class"]).any():
        raise ValueError("ATB usable-battery evidence has duplicate class/year rows")
    if set(selected.unit) != {"kWh"} or set(selected.source_id) != {ATB_SOURCE_ID}:
        raise ValueError("ATB usable-battery evidence must retain its registered source and kWh units")
    selected["value"] = pd.to_numeric(selected.value, errors="raise")
    if selected.value.map(lambda value: not isfinite(value) or value <= 0).any():
        raise ValueError("ATB usable-battery capacities must be finite and positive")
    return selected.sort_values(["year", "vehicle_class"]).reset_index(drop=True)


def aggregate_battery_classes(selected: pd.DataFrame, weights: dict,
                             *, years: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the owning regional GVWR/vocation weights to battery kWh per vehicle."""
    batteries, audit = [], []
    for (region, road_class), class_weights in sorted(weights.items()):
        if (road_class not in TRUCK_CLASSES or not class_weights
                or any(not isfinite(value) or value < 0 for value in class_weights.values())
                or abs(sum(class_weights.values()) - 1) > 1e-8):
            raise ValueError("Truck battery class weights must be finite, nonnegative and sum to one")
        for year in years:
            rows = selected.loc[selected.year.eq(year) & selected.vehicle_class.isin(class_weights)].copy()
            if set(rows.vehicle_class) != set(class_weights) or rows.vehicle_class.duplicated().any():
                raise ValueError(f"ATB battery vocation coverage is incomplete: {region}/{road_class}/{year}")
            rows["class_weight"] = rows.vehicle_class.map(class_weights)
            rows["weighted_battery_kwh"] = rows.value * rows.class_weight
            batteries.append({"year": year, "region": region, "road_class": road_class,
                              "battery_kwh": float(rows.weighted_battery_kwh.sum())})
            rows = rows.rename(columns={"value": "battery_kwh"})
            rows["region"], rows["road_class"] = region, road_class
            columns = ["year", "region", "road_class", "vehicle_class", "battery_kwh",
                       "class_weight", "weighted_battery_kwh", "trajectory", "unit"]
            columns.extend(name for name in ("source_member", "reference") if name in rows)
            audit.append(rows[columns])
    if not batteries:
        raise ValueError("No regional truck battery class weights are available")
    return pd.DataFrame(batteries), pd.concat(audit, ignore_index=True)


def pool_batteries_by_activity(regional_batteries: pd.DataFrame, activity: pd.DataFrame):
    """Form a service-activity-weighted proxy, not a vehicle/session-count mean."""
    keys = ["year", "region", "road_class"]
    if (regional_batteries.empty or activity.empty
            or regional_batteries.duplicated(keys).any() or activity.duplicated(keys).any()):
        raise ValueError("Battery/activity evidence must have unique nonempty year/region/class rows")
    if set(activity.units) != {"bn tonne-km"}:
        raise ValueError("Truck battery activity weights require compatible bn tonne-km units")
    for frame, column in ((regional_batteries, "battery_kwh"), (activity, "activity_bn")):
        if pd.to_numeric(frame[column], errors="raise").map(lambda value: not isfinite(value) or value <= 0).any():
            raise ValueError("Battery sizes and truck activity must be finite and positive")
    joined = regional_batteries.merge(activity, on=keys, how="outer", validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all():
        raise ValueError("Battery and activity evidence do not cover the same region/class/source years")
    for _, rows in joined.groupby("year"):
        if set(rows.road_class) != set(TRUCK_CLASSES):
            raise ValueError("Battery activity pooling requires both medium and heavy trucks")
        counts = rows.groupby("region").road_class.nunique()
        if not counts.eq(2).all():
            raise ValueError("Battery activity pooling lacks a truck class in a region")
    joined["activity_weighted_battery"] = joined.activity_bn * joined.battery_kwh
    classes = joined.groupby(["year", "road_class"], as_index=False).agg(
        activity_bn=("activity_bn", "sum"), weighted_battery=("activity_weighted_battery", "sum"),
        region_count=("region", "nunique"),
    )
    classes["battery_kwh"] = classes.weighted_battery / classes.activity_bn
    weights = classes[["year", "road_class", "activity_bn"]].copy()
    weights["activity_share"] = weights.activity_bn / weights.groupby("year").activity_bn.transform("sum")
    weights["units"] = "bn tonne-km"
    pooled = classes.groupby("year", as_index=False).agg(
        weighted_battery=("weighted_battery", "sum"), activity_bn=("activity_bn", "sum"),
    )
    pooled["battery_kwh"] = pooled.weighted_battery / pooled.activity_bn
    pooled["unit"] = "kWh usable battery"
    return (classes[["year", "road_class", "battery_kwh", "region_count"]], weights,
            pooled[["year", "battery_kwh", "unit"]])


def load_battery_evidence(bundle, *, years: list[int]) -> ChargerBatteryEvidence:
    """Read registered inputs and reuse fleet/activity owners without writing ETL."""
    if (not years or any(type(year) is not int for year in years)
            or years != sorted(set(years)) or min(years) < bundle.scenario.periods.base_year):
        raise ValueError("Battery preview years must be unique increasing integers from the scenario base year")
    command = f'uv run python src/build_transport.py --scenario "{bundle.scenario_path.as_posix()}"'
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    cer_rules = load_harmonization_rules(bundle, "cer_enerfuture")
    atb_path = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"],
                                  atb_rules["components"]["vehicles"]["output_file"])
    edition = configured_edition(bundle)
    macro_path = resolve_input_path(bundle, "interim", cer_rules["interim_subdir_template"].format(edition=edition),
                                    cer_rules["components"]["macro-indicators"]["output_file"])
    for path in (atb_path, macro_path):
        if not path.is_file():
            raise FileNotFoundError(f"Missing prepared battery/activity evidence: {path}. Prepare with: {command}")
    trajectory = configured_trajectory(bundle)
    selected = select_battery_rows(
        pd.read_csv(atb_path), years=years, trajectory=trajectory,
        metric_key=atb_rules["components"]["vehicles"]["metric_keys"]["Useable Energy (kWh)"],
        weight_category=efficiency_rules["atb"]["weight_categories"]["mhdv"],
        vehicle_detail=efficiency_rules["mhdv_archetypes"]["bev"]["detail"],
    )
    try:
        fleet = load_fleet_aggregation_evidence(bundle, include_ldv=False)
        macro = pd.read_csv(macro_path)
        baseline, activity_metadata = road_base_year_activity(bundle, macro=macro)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"{error}. Prepare with: {command}") from error
    weights = {}
    source_regions = list(bundle.scenario.geography.regions)
    classes = sorted(selected.vehicle_class.unique())
    for region in source_regions:
        weights[region, "medium_trucks"] = fleet.medium_weights(region, classes)
        try:
            weights[region, "heavy_trucks"] = fleet.heavy_weights(region)
        except ValueError as error:
            if not str(error).startswith("Incomplete freight haul evidence for "):
                raise
            raise ValueError(f"{error}. Prepared freight coverage does not support the configured regions. "
                             f"Prepare with: {command}") from error
    regional_batteries, components = aggregate_battery_classes(selected, weights, years=years)
    base_year = bundle.scenario.periods.base_year
    cer_scenario = configured_scenario(bundle, cer_rules)
    growth = real_gdp_indices(macro, scenario=cer_scenario, anchor_year=base_year,
                              evaluation_years=years)
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["region_output_map"]
    expected = {(region_map.get(region, region), road_class) for region in source_regions for road_class in TRUCK_CLASSES}
    selected_baseline = baseline.loc[baseline.service.isin(TRUCK_CLASSES)].copy()
    if (selected_baseline.duplicated(["region", "service"]).any()
            or set(zip(selected_baseline.region, selected_baseline.service, strict=True)) != expected):
        raise ValueError("Base-year truck activity lacks the configured region/class coverage")
    source_region_map = {region_map.get(region, region): region for region in source_regions}
    activity = pd.DataFrame([
        {"year": year, "region": source_region_map[row.region], "road_class": row.service,
         "activity_bn": row.projected_base_year_demand * growth[year], "units": row.units}
        for year in years for row in selected_baseline.itertuples(index=False)
    ])
    class_batteries, activity_weights, pooled = pool_batteries_by_activity(regional_batteries, activity)
    paths = {atb_path, macro_path, *fleet.paths,
             *(ceud_input_path(bundle, region) for region in source_regions)}
    metadata = {
        "diagnostic_only": True, "production_adoption": False,
        "trajectory": trajectory, "years": years, "regions": source_regions,
        "battery_basis": "ATB usable battery kWh, exact source points; no battery range is selected or extrapolated",
        "class_weight_basis": "Configured regional GVWR/vocation weights reused from road_fleet_weights",
        "aggregation_population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        "activity_weight_basis": "Projected total truck service in bn tonne-km; not BEV, vehicle-count or charging-session shares",
        "pooling_basis": "Activity-weighted battery proxy; payload and distance differences mean it is not the mean battery of a charging vehicle",
        "activity_method": "Owning CEUD-to-base-year demand projection, then CER real GDP at calendar/source years",
        "activity": activity_metadata, "cer_edition": edition, "cer_scenario": cer_scenario,
        "files": [{"path": str(path), "sha256": file_sha256(path)} for path in sorted(paths)],
        "configuration": [{"path": str(path), "sha256": file_sha256(path)} for path in (
            bundle.scenario_path, bundle.paths_path, bundle.sources_path, *harmonization_rule_paths(bundle))],
        "prepare_command": command,
    }
    return ChargerBatteryEvidence(class_batteries, activity_weights, pooled, components, metadata)
