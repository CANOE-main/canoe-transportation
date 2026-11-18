"""Read-only production charger parameters with explicit research alternatives."""

from dataclasses import dataclass
import math
from typing import Annotated

import numpy as np
import pandas as pd
from pydantic import Field, ValidationInfo, field_validator, model_validator

from parameterization.charger_representation import (
    ChargerRepresentationSettings, aggregate_components, build_representation,
    capacity_share_targets, source_value,
)
from parameterization.currency import CerCurrencyConverter
from parameterization.ev_chargers import prepare_charger_utilization_targets
from validation.charger_battery_diagnostic import load_battery_evidence
from validation.charger_stock_diagnostic import load_stock_evidence
from utils import (
    file_sha256, harmonization_rule_fingerprints, load_config_bundle, load_harmonization_rules, load_yaml,
    resolve_artifact_path, resolve_input_path, resolve_parameter_path,
)


Fraction = Annotated[float, Field(gt=0, le=1)]


class ChargerDiagnosticSettings(ChargerRepresentationSettings):
    """Combine separately owned production settings and diagnostic selections."""

    years: list[Annotated[int, Field(ge=2025)]] = Field(min_length=1)
    private_home_utilization_reference: Annotated[float, Field(gt=0, lt=1)]
    private_home_utilization_cases: list[Fraction] = Field(min_length=1)
    mhdv_recharge_fractions: list[Fraction] = Field(min_length=1)
    stock_review_report: str | None
    fixed_om_fraction: Fraction

    @field_validator("private_home_utilization_cases", mode="after")
    @classmethod
    def include_scenario_reference(cls, values: list[float], info: ValidationInfo) -> list[float]:
        """Expand display choices using the already validated scenario reference."""
        reference = info.data.get("private_home_utilization_reference")
        return sorted(set(values) | ({reference} if reference is not None else set()))

    @model_validator(mode="after")
    def coherent_diagnostic_choices(self):
        if self.years != sorted(set(self.years)):
            raise ValueError("Diagnostic years must be unique and increasing")
        if self.mhdv_recharge_fractions != sorted(set(self.mhdv_recharge_fractions)):
            raise ValueError("Recharge fractions must be unique and increasing")
        return self


@dataclass(frozen=True)
class ChargerRepresentationDiagnostic:
    parameters: pd.DataFrame
    constraints: pd.DataFrame
    existing_capacity: pd.DataFrame
    utilization_cases: pd.DataFrame
    audit: pd.DataFrame
    metadata: dict
    composition: pd.DataFrame
    ldv_utilization: pd.DataFrame
    battery: object
    utilization_components: pd.DataFrame
    stock_evidence: object


def mhdv_utilization_components(components: pd.DataFrame, settings: ChargerDiagnosticSettings,
                               batteries: pd.DataFrame) -> pd.DataFrame:
    """Derive hours first, then apply daily throughput and operating days."""
    for field in ("ports", "power_kw", "session_hours", "vehicles_per_day"):
        values = pd.to_numeric(components[field], errors="raise")
        if not np.isfinite(values).all() or (values.lt(0) if field == "ports" else values.le(0)).any():
            raise ValueError(f"Invalid MHDV charging component: {field}")
    if batteries.duplicated("year").any():
        raise ValueError("Battery estimate must have one value per calendar year")
    merged = components.merge(batteries[["year", "battery_kwh"]], on="year", how="left", validate="many_to_one")
    if merged.battery_kwh.isna().any() or not np.isfinite(merged.battery_kwh).all() or merged.battery_kwh.le(0).any():
        raise ValueError("Every charger year needs a positive finite ATB battery estimate")
    records = []
    for row in merged.to_dict("records"):
        cases = [("table19_duration", "Table 19 duration", np.nan, row["session_hours"])]
        cases += [(f"battery_recharge_{fraction:g}", f"{fraction:.0%} battery recharge", fraction,
                   fraction * row["battery_kwh"] / row["power_kw"])
                  for fraction in settings.mhdv_recharge_fractions]
        for case, label, fraction, hours in cases:
            occupied = hours * row["vehicles_per_day"]
            records.append({**row, "case": case, "case_label": label, "recharge_fraction": fraction,
                "charging_hours": hours, "occupied_hours_per_day": occupied,
                "exceeds_reference_duration": hours > row["session_hours"] + 1e-12,
                "operating_days": settings.mhdv_operating_days,
                "annual_utilization": occupied * settings.mhdv_operating_days / 8760,
                "feasible_daily_schedule": occupied <= 24,
                "status": "assumed constant rated-power charging; no taper or downtime"})
    return pd.DataFrame(records)


def mhdv_utilization_cases(components: pd.DataFrame) -> pd.DataFrame:
    """Pool time-based component utilizations by their rated-capacity shares."""
    records = []
    for (year, archetype, case), rows in components.groupby(["year", "archetype", "case"], sort=True):
        capacity = rows.ports * rows.power_kw
        if capacity.sum() <= 0:
            raise ValueError("MHDV utilization requires positive rated capacity")
        records.append({"year": int(year), "archetype": archetype, "case": case,
            "case_label": rows.case_label.iloc[0], "operating_days": rows.operating_days.iloc[0],
            "battery_kwh": rows.battery_kwh.iloc[0], "recharge_fraction": rows.recharge_fraction.iloc[0],
            "annual_utilization": float((capacity * rows.annual_utilization).sum() / capacity.sum()),
            "feasible_daily_schedule": bool(rows.feasible_daily_schedule.all()),
            "status": "time-based duration scenario, not measured annual utilization"})
    return pd.DataFrame(records)


def _converter(diagnostic, bundle):
    rules = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    path = resolve_input_path(bundle, "interim", rules["interim_subdir_template"].format(edition=edition),
                              rules["components"]["macro-indicators"]["output_file"])
    return CerCurrencyConverter(pd.read_csv(path), scenario=bundle.scenario.economics.cer_scenario,
                                target_year=bundle.scenario.economics.cost_reference_year)


def _existing_capacity(parameters, evidence, bundle, settings):
    columns = ["region", "vehicle_class", "archetype", "ports", "rated_power_kw",
               "capacity_gw", "stock_reference_year", "power_source_year",
               "stock_observation_year", "status"]
    rules = load_harmonization_rules(bundle, "ev_chargers")
    stock = evidence.fleet_counts
    afdc_rules = load_harmonization_rules(bundle, "afdc")
    public_path = resolve_artifact_path(bundle, "afdc_interim", afdc_rules["province_output_file"])
    public = pd.read_csv(public_path)
    first_year = min(settings.years)
    rows = parameters.loc[parameters.year.eq(first_year)].set_index("archetype")
    records, calculations = [], []
    for (region, vehicle_class), fleet in stock.groupby(["region", "vehicle_class"], sort=True):
        complete = fleet.bev_vehicles.notna().all()
        vehicles = float(fleet.known_bev_vehicles.sum(min_count=1))
        ratios = bundle.scenario.ev_chargers.evs_per_port
        ratio_by_class = {"cars": ratios[0], "light_trucks": ratios[0],
                          "medium_trucks": ratios[1], "heavy_trucks": ratios[2]}
        total = float((fleet.known_bev_vehicles / fleet.road_class.map(ratio_by_class)).sum(min_count=1))
        ratio = vehicles / total if pd.notna(total) and total > 0 else np.nan
        if pd.notna(total) and (not math.isfinite(total) or total < 0):
            raise ValueError("Existing vehicle/port estimate is invalid")
        status = "Point-year registration proxy" if complete else "Lower bound; some registrations are unpublished"
        if pd.isna(total):
            status = "Missing baseline-year registrations; stock estimate unavailable"
        if vehicle_class == "LDV":
            observed = public.loc[public.source_region.isin(rules["public_charger_region_map"][region])]
            if observed.empty:
                raise ValueError(f"Missing AFDC public port coverage for {region}")
            all_public = float(observed.public_chargers.sum())
            private = total - all_public if complete else np.nan
            if pd.notna(private) and private < 0:
                raise ValueError(f"Observed public ports exceed inferred total ports in {region}")
            counts = {settings.archetypes.ldv_public_l2: float(observed.level_2_ports.sum()),
                      settings.archetypes.ldv_public_fast: float(observed.dc_fast_ports.sum()),
                      settings.archetypes.ldv_private: private}
            ignored_l1 = float(observed.level_1_ports.sum())
        else:
            names = [settings.archetypes.mhdv_overnight, settings.archetypes.mhdv_fast]
            shares = rows.loc[names, "port_count"] / rows.loc[names, "port_count"].sum()
            counts = {name: total * float(shares[name]) for name in names}
            ignored_l1 = 0.0
        calculations.append({"region": region, "vehicle_class": vehicle_class,
            "baseline_year": bundle.scenario.periods.base_year,
            "known_bev_vehicles": vehicles, "vehicles_per_port": ratio,
            "total_inferred_ports": total, "ignored_public_l1_ports": ignored_l1, "status": status})
        for name, ports in counts.items():
            rating = float(rows.loc[name, "rated_power_kw"])
            records.append({"region": region, "vehicle_class": vehicle_class,
                            "archetype": name, "ports": ports, "rated_power_kw": rating,
                            "capacity_gw": ports * rating / 1_000_000,
                            "stock_reference_year": bundle.scenario.periods.base_year, "power_source_year": first_year,
                            "stock_observation_year": fleet.source_year.iloc[0], "status": status})
        if complete and not math.isclose(sum(counts.values()) + ignored_l1, total, abs_tol=1e-8, rel_tol=1e-12):
            raise ValueError("Explicit existing-port totals do not reconcile with the vehicle ratio")
    return pd.DataFrame(records, columns=columns), {"stock_status": "Research alternative: direct point-year registrations; production uses prepared physical BEV stock.",
                                   "stock_regions": sorted(stock.region.unique().tolist()),
                                   "stock_calculations": calculations, "stock_source": evidence.metadata,
                                   "afdc_snapshot_dates": sorted(public.as_of.dropna().unique().tolist()) if "as_of" in public else [],
                                   "afdc_public_path": str(public_path), "afdc_public_sha256": file_sha256(public_path)}


def build_diagnostic(diagnostic, bundle=None) -> ChargerRepresentationDiagnostic:
    """Read the production core; calculate only labelled diagnostic alternatives."""
    if bundle is None:
        bundle = load_config_bundle(diagnostic.metadata["configured_notebook_scenario"])
    path = resolve_parameter_path(bundle, "charger_infrastructure_diagnosis.yaml")
    config = load_yaml(path)
    rules = load_harmonization_rules(bundle, "ev_chargers")
    manual = pd.read_csv(resolve_input_path(bundle, "manual", rules["manual_file"]))
    fixed = manual.loc[manual.sub_category.eq("all") & manual.parameter.eq("fixed_to_capex_ratio")]
    if len(fixed) != 1:
        raise ValueError("Diagnostic requires the one registered charger fixed O&M ratio")
    settings = ChargerDiagnosticSettings.model_validate({
        **rules["production_representation"], **config["representation_diagnostic"],
        "private_home_utilization_reference": bundle.scenario.ev_chargers.ld_home_utilization,
        "fixed_om_fraction": float(fixed.value.iloc[0]),
    })
    converter = _converter(diagnostic, bundle)
    l1 = manual.loc[manual.sub_category.eq("ldv") & manual.parameter.eq("L1_capex")]
    if len(l1) != 1:
        raise ValueError("Diagnostic requires the one registered residential L1 cost")
    frames = {
        "ldv": diagnostic.counts.loc[diagnostic.counts.table.eq(6)],
        "mhdv": diagnostic.counts.loc[diagnostic.counts.table.eq(38)],
        "power": diagnostic.power, "costs": diagnostic.costs,
        "utilization": diagnostic.utilization, "operating": diagnostic.mhd_utilization,
    }
    core = build_representation(
        frames, settings, settings.years, converter, l1_input=l1.iloc[0],
        fixed_om_fraction=settings.fixed_om_fraction,
        ld_home_utilization=bundle.scenario.ev_chargers.ld_home_utilization,
    )
    parameters = core.parameters.copy()
    components = core.components.copy()
    battery = load_battery_evidence(bundle, years=settings.years)
    stock_evidence = load_stock_evidence(bundle, review_report=settings.stock_review_report)
    mhdv = components.loc[components.vehicle_class.eq("MHDV")].copy()
    mhdv[["location", "charger"]] = mhdv.component.str.split(" ", n=1, expand=True)
    utilization_components = mhdv_utilization_components(mhdv, settings, battery.pooled_battery)
    utilization = mhdv_utilization_cases(utilization_components)
    for fraction in settings.mhdv_recharge_fractions:
        selected = utilization.loc[utilization["case"].eq(f"battery_recharge_{fraction:g}"),
                                   ["year", "archetype", "annual_utilization"]]
        parameters = parameters.merge(selected.rename(columns={
            "annual_utilization": f"recharge_{fraction:g}_utilization",
        }), on=["year", "archetype"], how="left", validate="one_to_one")
    ldv_utilization, audit = [], []
    names = settings.archetypes
    labels = {"L1 - home": "Home L1", "L2 - home": "Home L2", "L2 - work": "Private workplace L2"}
    for (year, archetype), rows in components.groupby(["year", "archetype"], sort=True):
        mask = parameters.year.eq(year) & parameters.archetype.eq(archetype)
        parameters.loc[mask, "cost_status"] = "; ".join(rows.cost_basis.drop_duplicates())
        parameters.loc[mask, "utilization_status"] = "; ".join(rows.utilization_basis.drop_duplicates())
        parameters.loc[mask, "installed_nameplate_kw"] = (
            source_value(diagnostic.power, year, charger="Public - DCFC")
            if archetype == names.ldv_public_fast else parameters.loc[mask, "rated_power_kw"].item()
        )
        parameters.loc[mask, "source_annual_utilization"] = (
            parameters.loc[mask, "annual_utilization"].item()
            if archetype in {names.ldv_public_l2, names.ldv_public_fast} else np.nan
        )
        if rows.vehicle_class.iloc[0] != "LDV":
            continue
        if archetype == names.ldv_private:
            for row in rows.to_dict("records"):
                ldv_utilization.append({**row, "annual_utilization": row["utilization"],
                    "component": labels[row["component"]],
                    "basis": "Empirical scenario home-utilization proxy" if row["component"].endswith("home") else "Dunsky Table 31"})
            for factor in settings.private_home_utilization_cases:
                pieces = [dict(row, utilization=factor if row["component"].endswith("home") else row["utilization"])
                          for row in rows.to_dict("records")]
                audit.append({"kind": "private_home_utilization_sensitivity", "year": year,
                              "archetype": archetype, "home_utilization": factor, **aggregate_components(pieces)})
        else:
            ldv_utilization.append({"year": year, "archetype": archetype,
                "component": "Public L2" if archetype == names.ldv_public_l2 else "Public fast",
                "ports": rows.ports.sum(), "power_kw": rows.power_kw.iloc[0],
                "annual_utilization": rows.utilization.iloc[0], "basis": "Dunsky Table 31"})
    regions = sorted({load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"].get(r, r)
                      for r in bundle.scenario.geography.regions})
    period_years = {period: bundle.scenario.periods.evaluation_year(period)
                    for period in bundle.scenario.periods.model}
    constraints, groups = capacity_share_targets(parameters, settings, regions, period_years)
    existing, stock_metadata = _existing_capacity(parameters, stock_evidence, bundle, settings)
    configured_targets = []
    if bundle.scenario.ev_chargers.charger_type_classes == "aggregated":
        targets, _ = prepare_charger_utilization_targets(bundle, manual=manual)
        configured_targets = targets[["region", "tech_or_group", "vintage", "target_utilization"]].to_dict("records")
    metadata = {
        "read_only": True, "production_adoption": True,
        "configured_representation": bundle.scenario.ev_chargers.charger_type_classes,
        "parameter_basis": "Five physical archetypes from the shared production core; aggregated mode pools their investment-cost basis and retains separate utilization/stock-rating policies",
        "stock_alternative_adopted": False, "type_specific_hourly_profiles": "pending",
        "configured_aggregate_utilization_targets": configured_targets,
        "utilization_policy": ("Aggregated mode retains its reviewed technology-kind annual utilization; the displayed physical-archetype utilization is the explicit-mode calculation"
                               if configured_targets else "Explicit-mode annual utilization is the physical-archetype calculation displayed here"),
        "settings": settings.model_dump(), "baseline_year": bundle.scenario.periods.base_year,
        "config_sha256": file_sha256(path), "source_sha256": diagnostic.metadata["source_sha256"],
        "production_rules_fingerprints": harmonization_rule_fingerprints(bundle),
        "cost_dollar_year": bundle.scenario.economics.cost_reference_year,
        "constraint_regions": regions, "constraint_period_source_years": period_years,
        "group_members": groups,
        "limitations": [
            "The configured production representation is reported separately from the five physical archetypes used to derive it.",
            "Home utilization is an empirical scenario proxy; residential L2 telemetry is extended to home L1, not a national measurement.",
            "MHDV production uses Table 19 session duration and throughput; battery recharge cases remain research sensitivities without taper or downtime.",
            "The point-year registration stock illustration is an alternative; production uses its prepared physical BEV stock handoff.",
            "National MHDV composition is transferred to provinces; source bus coverage differs from the selected registration classes.",
            "Type-specific hourly charging profiles and joint solver feasibility require separate evidence; an aggregate hourly profile does not establish them.",
        ], **stock_metadata,
    }
    return ChargerRepresentationDiagnostic(
        parameters, constraints, existing, utilization,
        pd.concat([pd.DataFrame(audit), components.assign(kind="production_source_component")], ignore_index=True),
        metadata, core.composition, pd.DataFrame(ldv_utilization), battery, utilization_components, stock_evidence,
    )
