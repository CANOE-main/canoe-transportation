"""Source-to-archetype charger calculations shared by production and diagnostics."""

from dataclasses import dataclass
import math
import re
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from parameterization.currency import CerCurrencyConverter
from utils import ConfigBundle, load_harmonization_rules


class ChargerArchetypes(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ldv_public_l2: str = Field(min_length=1)
    ldv_public_fast: str = Field(min_length=1)
    ldv_private: str = Field(min_length=1)
    mhdv_overnight: str = Field(min_length=1)
    mhdv_fast: str = Field(min_length=1)


class ChargerGroups(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ldv_public: str = Field(min_length=1)
    ldv_all: str = Field(min_length=1)
    mhdv_all: str = Field(min_length=1)


class ChargerRepresentationSettings(BaseModel):
    """Accepted modelling choices, validated before source I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_year_policy: Literal["linear_between_points_then_hold"]
    count_hold_after_year: int = Field(ge=2025, strict=True)
    ldv_scenario: str = Field(min_length=1)
    mhdv_scenario: str = Field(min_length=1)
    workplace_partition: Literal["source_public_private_halves"]
    capacity_basis: Literal["matched_cost_power"]
    public_dcfc_utilization_basis: Literal["reported_port_utilization"]
    public_overnight_cost_policy: Literal["linear_per_kw_from_50kw_cost"]
    private_l2_cost_policy: Literal["public_l2_proxy"]
    mhdv_operating_days: int = Field(gt=0, le=365, strict=True)
    mhdv_mix_scope: Literal["national_mix_each_province"]
    negligible_public_l1: Literal[True]
    stock_basis: Literal["prepared_physical_bev_stock"]
    archetypes: ChargerArchetypes
    groups: ChargerGroups

    @model_validator(mode="after")
    def distinct_members(self):
        names = [*self.archetypes.model_dump().values(), *self.groups.model_dump().values()]
        if len(names) != len(set(names)):
            raise ValueError("Charger archetypes and parent groups require distinct identities")
        return self


@dataclass(frozen=True)
class ChargerRepresentation:
    parameters: pd.DataFrame
    components: pd.DataFrame
    composition: pd.DataFrame


def select_charger_technologies(bundle: ConfigBundle, technology: pd.DataFrame) -> pd.DataFrame:
    """Select one charger representation while preserving every non-charger row."""
    rules = load_harmonization_rules(bundle, "ev_chargers")
    if bundle.scenario.ev_chargers.charger_type_classes == "explicit":
        selected = set(ChargerRepresentationSettings.model_validate(
            rules["production_representation"]).archetypes.model_dump().values())
    else:
        selected = {mapping[kind] for mapping in rules["technology"].values()
                    for kind in ("existing", "new")}
    chargers = technology.loc[technology.category.eq("charger")]
    if chargers.tech.duplicated().any() or selected - set(chargers.tech):
        raise ValueError(f"Selected charger technologies are missing or duplicated: {sorted(selected - set(chargers.tech))}")
    return technology.loc[~technology.category.eq("charger") | technology.tech.isin(selected)].copy()


def technology_mappings(bundle: ConfigBundle) -> dict[str, dict[str, str]]:
    rules = load_harmonization_rules(bundle, "ev_chargers")
    if bundle.scenario.ev_chargers.charger_type_classes == "aggregated":
        return {category: {**mapping, "vehicle_class": category}
                for category, mapping in rules["technology"].items()}
    settings = ChargerRepresentationSettings.model_validate(rules["production_representation"])
    return {key: {"existing": tech, "new": tech,
                  "output": rules["technology"]["ldv" if key.startswith("ldv_") else "mhdv"]["output"],
                  "vehicle_class": "ldv" if key.startswith("ldv_") else "mhdv"}
            for key, tech in settings.archetypes.model_dump().items()}


def capacity_share_targets(parameters, settings, regions, period_years):
    """Convert source port composition to period-wide available-GW share targets."""
    a, g = settings.archetypes, settings.groups
    # The schema references group IDs on both sides of each share, while Temoa
    # requires technology and group names to be disjoint.
    singleton = {tech: f"{tech}_GROUP" for tech in a.model_dump().values()}
    parents = set(g.model_dump().values())
    if set(singleton.values()) & (singleton.keys() | parents):
        raise ValueError("Charger singleton group names conflict with a technology or parent group")
    groups = {**{name: [tech] for tech, name in singleton.items()},
              g.ldv_public: [a.ldv_public_l2, a.ldv_public_fast],
              g.ldv_all: [a.ldv_public_l2, a.ldv_public_fast, a.ldv_private],
              g.mhdv_all: [a.mhdv_overnight, a.mhdv_fast]}
    definitions = [(singleton[a.ldv_public_l2], g.ldv_public), (singleton[a.ldv_public_fast], g.ldv_public),
                   (g.ldv_public, g.ldv_all), (singleton[a.ldv_private], g.ldv_all),
                   (singleton[a.mhdv_overnight], g.mhdv_all), (singleton[a.mhdv_fast], g.mhdv_all)]
    records = []
    for period, source_year in sorted(period_years.items()):
        rows = pd.DataFrame([
            {"archetype": archetype,
             "port_count": source_value(trajectory.rename(columns={"port_count": "value"}), source_year),
             "rated_power_kw": source_value(trajectory.rename(columns={"rated_power_kw": "value"}), source_year)}
            for archetype, trajectory in parameters.groupby("archetype", sort=True)
        ]).set_index("archetype")
        power = rows.port_count * rows.rated_power_kw
        for sub, super_group in definitions:
            share = float(power.loc[groups[sub]].sum() / power.loc[groups[super_group]].sum())
            port_share = float(rows.loc[groups[sub], "port_count"].sum() / rows.loc[groups[super_group], "port_count"].sum())
            if not math.isfinite(share) or not 0 <= share <= 1 or not math.isfinite(port_share) or not 0 <= port_share <= 1:
                raise ValueError("Charger capacity/port shares must be finite fractions")
            records.extend({"region": region, "period": int(period), "source_year": int(source_year),
                            "sub_group": sub, "super_group": super_group, "operator": "e",
                            "share": share, "port_share": port_share, "capacity_units": "GW"}
                           for region in regions)
    return pd.DataFrame(records), groups


def source_value(frame: pd.DataFrame, year: int, **selectors) -> float:
    """Interpolate a registered trajectory and hold only after its last point."""
    return source_value_and_years(frame, year, **selectors)[0]


def source_value_and_years(frame: pd.DataFrame, year: int, **selectors) -> tuple[float, tuple[int, ...]]:
    """Return the evaluated value and the exact native years supporting it."""
    rows = frame
    for key, value in selectors.items():
        rows = rows.loc[rows[key].eq(value)]
    rows = rows.sort_values("year")
    if rows.empty or rows.year.duplicated().any():
        raise ValueError(f"Missing or duplicate charger source trajectory: {selectors}")
    if year < int(rows.year.min()):
        raise ValueError(f"Charger year precedes registered trajectory: {year}/{selectors}")
    values = pd.to_numeric(rows.value, errors="raise").to_numpy(float)
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError(f"Invalid charger source values: {selectors}")
    years = rows.year.to_numpy(int)
    right = years.searchsorted(year)
    if right == len(years):
        points = (int(years[-1]),)
    elif year == years[right]:
        points = (int(years[right]),)
    else:
        points = (int(years[right - 1]), int(years[right]))
    return float(np.interp(year, years, values)), points


def aggregate_components(components: list[dict]) -> dict:
    """Conserve ports, expenditure, rated capacity and annual charging energy."""
    for item in components:
        for key in ("ports", "power_kw", "cost_cad_per_port", "utilization"):
            value = item[key]
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"Invalid charger component: {key}")
        if item["power_kw"] <= 0 or item["cost_cad_per_port"] <= 0 or item["utilization"] > 1:
            raise ValueError("Invalid charger component power, cost or utilization")
    ports = sum(item["ports"] for item in components)
    power = sum(item["ports"] * item["power_kw"] for item in components)
    expense = sum(item["ports"] * item["cost_cad_per_port"] for item in components)
    energy = sum(item["ports"] * item["power_kw"] * item["utilization"] for item in components)
    if not all(math.isfinite(value) and value > 0 for value in (ports, power, expense)):
        raise ValueError("Charger aggregate requires positive finite ports, power and expenditure")
    return {"port_count": ports, "rated_power_kw": power / ports,
            "capex_mcad_per_gw": expense / power, "annual_utilization": energy / power}


def composition_components(frames: dict[str, pd.DataFrame], settings: ChargerRepresentationSettings,
                           years: list[int]) -> pd.DataFrame:
    """Use disjoint native components; never add a printed subtotal to its parts."""
    a = settings.archetypes
    assignments = {
        a.ldv_public_l2: ["L2 - community", "L2 - workplace"],
        a.ldv_public_fast: ["DCFC - community", "DCFC - corridor + rural", "DCFC - taxis + rideshare"],
        a.ldv_private: ["L1 - home", "L2 - home", "L2 - work"],
    }
    records = []
    for year in years:
        source_year = min(year, settings.count_hold_after_year)
        light = frames["ldv"].loc[frames["ldv"].scenario.eq(settings.ldv_scenario)]
        for archetype, labels in assignments.items():
            for label in labels:
                ports, native_years = source_value_and_years(light, source_year, charger=label)
                records.append({"year": year, "source_year": source_year, "vehicle_class": "LDV",
                                "archetype": archetype, "component": label, "table": 6,
                                "ports": ports, "count_native_years": native_years})
        freight = frames["mhdv"].loc[frames["mhdv"].scenario.eq(settings.mhdv_scenario)]
        for location in ("Public", "Private"):
            for kind in ("Overnight", "FC", "UFC"):
                ports, native_years = source_value_and_years(freight, source_year, location=location, charger=kind)
                records.append({"year": year, "source_year": source_year, "vehicle_class": "MHDV",
                                "archetype": a.mhdv_overnight if kind == "Overnight" else a.mhdv_fast,
                                "component": f"{location} {kind}", "table": 38,
                                "ports": ports, "count_native_years": native_years})
    result = pd.DataFrame(records)
    result["population_ports"] = result.groupby(["vehicle_class", "year"]).ports.transform("sum")
    result["archetype_ports"] = result.groupby(["archetype", "year"]).ports.transform("sum")
    if result.population_ports.le(0).any() or result.archetype_ports.le(0).any():
        raise ValueError("Charger source composition requires positive populations")
    result["population_share"] = result.ports / result.population_ports
    result["within_archetype_share"] = result.ports / result.archetype_ports
    return result


def _source_cost(costs: pd.DataFrame, converter: CerCurrencyConverter, year: int,
                 vehicle_class: str, charger: str) -> tuple[float, float, str, int, tuple[int, ...]]:
    rows = costs.loc[costs.vehicle_class.eq(vehicle_class) & costs.charger.eq(charger)]
    units = rows.unit.drop_duplicates()
    if len(units) != 1 or not (match := re.fullmatch(r"([A-Z]{3})(\d{4})/port", units.iloc[0])):
        raise ValueError("Charger source cost requires one native currency and dollar year")
    value, native_years = source_value_and_years(rows, year)
    converted = converter.convert(value, currency=match[1], dollar_year=int(match[2]))
    return converted.target_cad_value, value, match[1], int(match[2]), native_years


def build_representation(
    frames: dict[str, pd.DataFrame], settings: ChargerRepresentationSettings, years: list[int],
    converter: CerCurrencyConverter, *, l1_input: pd.Series, fixed_om_fraction: float,
    ld_home_utilization: float,
) -> ChargerRepresentation:
    """Build the five physical archetypes without I/O or diagnostic dependencies."""
    if not years or years != sorted(set(years)):
        raise ValueError("Charger source years must be unique and increasing")
    if not math.isfinite(ld_home_utilization) or not 0 < ld_home_utilization < 1:
        raise ValueError("Home charger utilization must be in (0, 1)")
    if not math.isfinite(fixed_om_fraction) or not 0 <= fixed_om_fraction <= 1:
        raise ValueError("Charger fixed O&M fraction must be in [0, 1]")
    l1_cost = converter.convert(float(l1_input.value), currency=l1_input.unit,
                                dollar_year=int(l1_input.currency_year)).target_cad_value
    a = settings.archetypes
    composition = composition_components(frames, settings, years)
    components = []
    for year in years:
        selected = composition.loc[composition.year.eq(year)]
        ldv = frames["ldv"].loc[frames["ldv"].scenario.eq(settings.ldv_scenario)]
        private_count = selected.loc[selected.archetype.eq(a.ldv_private), "ports"].sum()
        published_private = source_value(ldv, min(year, settings.count_hold_after_year), charger="Total Private")
        if not math.isclose(private_count, published_private, abs_tol=1e-8):
            raise ValueError("Private charger components differ from Table 6 Total Private")
        costs = frames["costs"]
        l2 = _source_cost(costs, converter, year, "LDV", "L2")
        dcfc = _source_cost(costs, converter, year, "LDV", "DCFC")
        fast = costs.loc[costs.vehicle_class.eq("LDV") & costs.charger.eq("DCFC")]
        fc_power, fc_power_years = source_value_and_years(
            fast.rename(columns={"value": "cost", "rated_power_kw": "value"}), year,
        )
        for row in selected.loc[selected.vehicle_class.eq("LDV")].to_dict("records"):
            label = row["component"]
            home = label in {"L1 - home", "L2 - home"}
            if row["archetype"] == a.ldv_public_fast:
                power, power_years = fc_power, fc_power_years
                cost, native, currency, dollar_year, cost_years = dcfc
                utilization, utilization_years = source_value_and_years(frames["utilization"], year, charger="Public - DCFC")
                power_table, uf_basis = 12, "Dunsky Table 31 reported port utilization"
            else:
                power_label = ("Home - L1" if label == "L1 - home" else "Home - L2" if label == "L2 - home"
                               else "Workplace" if label == "L2 - work" else "Public - L2")
                power, power_years = source_value_and_years(frames["power"], year, charger=power_label)
                cost, native, currency, dollar_year, cost_years = (
                    (l1_cost, float(l1_input.value), l1_input.unit, int(l1_input.currency_year), ())
                    if label == "L1 - home" else l2
                )
                utilization, utilization_years = (ld_home_utilization, ()) if home else source_value_and_years(
                    frames["utilization"], year, charger="Workplace" if label == "L2 - work" else "Public - L2")
                power_table = 30
                uf_basis = "Scenario home-utilization proxy; residential L2 telemetry extended to home L1" if home else "Dunsky Table 31"
            components.append({**row, "power_kw": power, "cost_cad_per_port": cost,
                               "utilization": utilization, "native_cost_per_port": native,
                               "native_currency": currency, "native_dollar_year": dollar_year,
                               "cost_native_years": cost_years, "power_native_years": power_years,
                               "utilization_native_years": utilization_years,
                               "power_source_table": power_table, "utilization_basis": uf_basis,
                               "cost_basis": "manual residential L1 source" if label == "L1 - home" else "Dunsky Table 12; private L2 uses public-L2 price proxy"})
        for row in selected.loc[selected.vehicle_class.eq("MHDV")].to_dict("records"):
            location, kind = row["component"].split(" ", 1)
            schedule_name, cost_name = {"Overnight": ("Overnight (CCS)", "Overnight"),
                                        "FC": ("Fast (CCS)", "Fast Charge"),
                                        "UFC": ("Ultrafast (MCS)", "Ultra-Fast Charge")}[kind]
            schedule = frames["operating"].loc[frames["operating"].location.eq(location)
                                                       & frames["operating"].charger.eq(schedule_name)]
            if len(schedule) != 1:
                raise ValueError(f"MHDV operating schedule is missing or duplicate: {location}/{kind}")
            schedule = schedule.iloc[0]
            occupied = float(schedule.session_hours) * float(schedule.vehicles_per_day)
            if not 0 < occupied <= 24:
                raise ValueError("MHDV charging duration and daily throughput exceed available hours")
            cost, native, currency, dollar_year, cost_years = _source_cost(costs, converter, year, "MHDV", cost_name)
            if location == "Public" and kind == "Overnight":
                rated = costs.loc[costs.vehicle_class.eq("MHDV") & costs.charger.eq("Overnight"), "rated_power_kw"].drop_duplicates()
                if len(rated) != 1 or rated.iloc[0] <= 0:
                    raise ValueError("Public overnight cost proxy requires one positive source cost rating")
                cost *= float(schedule.power_kw) / float(rated.iloc[0])
            components.append({**row, "power_kw": float(schedule.power_kw), "cost_cad_per_port": cost,
                               "utilization": occupied * settings.mhdv_operating_days / 8760,
                               "native_cost_per_port": native, "native_currency": currency,
                               "native_dollar_year": dollar_year, "session_hours": float(schedule.session_hours),
                               "cost_native_years": cost_years, "power_native_years": (), "utilization_native_years": (),
                               "vehicles_per_day": float(schedule.vehicles_per_day),
                               "operating_days": settings.mhdv_operating_days, "occupied_hours_per_day": occupied,
                               "power_source_table": 19,
                               "utilization_basis": "Table 19 charging hours/session × vehicles/operating day × operating days/year / 8760",
                               "cost_basis": "public 100kW overnight: linear per kW from Table 21 private 50kW cost" if location == "Public" and kind == "Overnight" else "Dunsky Table 21 matched cost/rating"})
    component_frame = pd.DataFrame(components)
    parameters = []
    for (year, vehicle_class, archetype), rows in component_frame.groupby(["year", "vehicle_class", "archetype"], sort=True):
        combined = aggregate_components(rows.to_dict("records"))
        population = component_frame.loc[component_frame.year.eq(year) & component_frame.vehicle_class.eq(vehicle_class)]
        parameters.append({"year": int(year), "vehicle_class": vehicle_class, "archetype": archetype,
                           "count_source_year": min(year, settings.count_hold_after_year), **combined,
                           "port_share": combined["port_count"] / population.ports.sum(),
                           "capacity_share": float((rows.ports * rows.power_kw).sum() / (population.ports * population.power_kw).sum()),
                           "fixed_om_mcad_per_gw_year": combined["capex_mcad_per_gw"] * fixed_om_fraction})
    return ChargerRepresentation(pd.DataFrame(parameters), component_frame, composition)
