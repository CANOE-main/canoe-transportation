"""Read-only comparison of published charger requirements and production inputs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import re

import altair as alt
import numpy as np
import pandas as pd

from fetching.dunsky_charger_inputs import infrastructure_frames, load_evidence
from parameterization.currency import CerCurrencyConverter
from parameterization.ev_chargers import load_charger_inputs
from parameterization.charger_representation import ChargerRepresentationSettings, build_representation
from utils import (file_sha256, find_repo_root, load_config_bundle, load_harmonization_rules,
                   load_yaml, resolve_artifact_path, resolve_input_path, resolve_parameter_path,
                   write_dataframe_atomic, write_text_atomic)
from validation.notebook_figures import identified_chart


@dataclass(frozen=True)
class ChargerInfrastructureDiagnostic:
    counts: pd.DataFrame
    shares: pd.DataFrame
    power: pd.DataFrame
    utilization: pd.DataFrame
    costs: pd.DataFrame
    cost_comparison: pd.DataFrame
    manual_comparison: pd.DataFrame
    anomalies: pd.DataFrame
    mhd_utilization: pd.DataFrame
    vintage_years: pd.DataFrame
    afdc_counts: pd.DataFrame
    afdc_stations: pd.DataFrame
    metadata: dict
    source_tables: dict


def number(value: str, *, dash_zero: bool = False) -> float:
    if value == "-" and dash_zero:
        return 0.0
    match = re.fullmatch(r"\$?(-?[\d,]+(?:\.\d+)?)(%)?(?: \((\d+) kW average\))?", value)
    if not match:
        raise ValueError(f"Unrecognized source numeric cell: {value!r}")
    return float(match[1].replace(",", "")) / (100 if match[2] else 1)


def annual_rows(evidence: dict, table: int, dimensions: list[str], unit: str, *,
                zero_dash_cells: set[tuple[str, int]] | None = None) -> pd.DataFrame:
    native = evidence["tables"][str(table)]
    matrix = native["matrix"]
    years = [int(v) for v in matrix[native["header_rows"] - 1][len(dimensions):]]
    records = []
    for row_index, row in enumerate(matrix[native["header_rows"]:], native["header_rows"] + 1):
        for column, year in enumerate(years, len(dimensions)):
            labels = dict(zip(dimensions, row[:len(dimensions)], strict=True))
            dash_zero = (labels.get("charger"), year) in (zero_dash_cells or set())
            records.append({**labels,
                            "year": year, "value": number(row[column], dash_zero=dash_zero),
                            "raw_value": row[column], "unit": unit, "table": table,
                            "source_row": row_index, "source_column": column + 1,
                            "evidence_kind": native["evidence_kind"],
                            "source_url": evidence["url"] + "#" + native["id"]})
    return pd.DataFrame(records)


def extract_frames(evidence: dict, settings: dict) -> dict[str, pd.DataFrame]:
    frames = infrastructure_frames(evidence)
    regional = []
    table = evidence["tables"]["44"]
    for row in table["matrix"][2:]:
        for col in range(1, 17):
            regional.append({"geography": row[0], "charger": table["matrix"][0][col],
                             "year": int(table["matrix"][1][col]), "value": number(row[col]),
                             "raw_value": row[col], "unit": "ports", "table": 44})
    return {**frames, "regional": pd.DataFrame(regional)}


def projected_compositions(frames: dict) -> pd.DataFrame:
    """Construct explicit diagnostic populations without summing published subtotals."""
    ldv = frames["ldv"]
    # Workplace appears in both published public and private totals. Count it once.
    selections = {
        "All ports, workplace once (diagnostic)": {
            "L1 - home": "L1", "L2 - home": "L2", "Total - L2": "L2", "Total - DCFC": "DCFC"},
        "Public per report (includes workplace)": {"Total - L2": "L2", "Total - DCFC": "DCFC"},
    }
    records = []
    for population, mapping in selections.items():
        selected = ldv.loc[ldv.charger.isin(mapping)].copy()
        selected["charger"] = selected.charger.map(mapping)
        selected = selected.groupby(["scenario", "year", "charger"], as_index=False).value.sum()
        records.append(selected.assign(vehicle_class="LDV", population=population, basis="port count", table=6))
    mhdv = frames["mhdv"].loc[frames["mhdv"].charger.isin(["Overnight", "FC", "UFC"])]
    for population in ("Public", "Private", "Public + Private"):
        selected = mhdv if population == "Public + Private" else mhdv.loc[mhdv.location.eq(population)]
        selected = selected.groupby(["scenario", "year", "charger"], as_index=False).value.sum()
        records.append(selected.assign(vehicle_class="MHDV", population=population, basis="port count", table=38))
    result = pd.concat(records, ignore_index=True).rename(columns={"value": "ports"})
    if (result.ports < 0).any():
        raise ValueError("Negative counts cannot support diagnostic shares")
    totals = result.groupby(["vehicle_class", "scenario", "population", "year"]).ports.transform("sum")
    if (totals <= 0).any():
        raise ValueError("A diagnostic composition has no ports")
    result["port_share"] = result.ports / totals
    result["population_ports"] = totals
    return result


def source_anomalies(frames: dict) -> pd.DataFrame:
    findings = []
    mhdv = frames["mhdv"]
    for (scenario, location, year), rows in mhdv.groupby(["scenario", "location", "year"]):
        if location == "Total Ports":
            published = rows.value.item()
            population = mhdv.loc[mhdv.scenario.eq(scenario) & mhdv.year.eq(year)]
            children = population.loc[population.charger.isin(["Overnight", "FC", "UFC"]), "value"].sum()
            subtotals = population.loc[population.charger.eq("Total"), "value"].sum()
            for basis, value in (("component sum", children), ("published subtotal sum", subtotals)):
                if published != value:
                    findings.append({"table": 38, "scope": f"{scenario}; Total Ports; {year}",
                                     "issue": f"Published grand total differs from {basis}", "published": published,
                                     "component_sum": value, "difference": published - value})
            continue
        total = rows.loc[rows.charger.eq("Total"), "value"].item()
        children = rows.loc[rows.charger.isin(["Overnight", "FC", "UFC"]), "value"].sum()
        if total != children:
            findings.append({"table": 38, "scope": f"{scenario}; {location}; {year}",
                             "issue": "Published subtotal differs from component sum", "published": total,
                             "component_sum": children, "difference": total - children})
    for row in frames["regional"].loc[frames["regional"].value.lt(0)].itertuples():
        findings.append({"table": 44, "scope": f"{row.geography}; {row.charger}; {row.year}",
                         "issue": "Negative published port count; excluded from share calculations",
                         "published": row.value, "component_sum": np.nan, "difference": np.nan})
    return pd.DataFrame(findings)


def manual_reconciliation(manual: pd.DataFrame, frames: dict) -> pd.DataFrame:
    """Keep every manual row and note, identifying direct matches versus derivations."""
    rows = []
    for row in manual.to_dict("records"):
        values, table, status = [], "", "Reviewed assumption or external evidence; no direct report cell"
        parameter, category, period = row["parameter"], row["sub_category"], str(row["period"])
        if parameter.endswith("_capex") and parameter != "L1_capex":
            table = 12 if category == "ldv" else 21
            kind = {"50kW": "Overnight", "350kW": "Fast Charge", "2MW": "Ultra-Fast Charge"}.get(parameter[:-6], parameter[:-6])
            selected = frames["costs"].loc[frames["costs"].vehicle_class.eq(category.upper()) & frames["costs"].charger.eq(kind)]
            selected = selected.loc[selected.year.eq(int(period))] if period.isdigit() else selected.loc[selected.year.ge(2030 if category == "ldv" else 2040)]
            values = selected.value.tolist()
        elif parameter.endswith("_rated_power"):
            if category == "mhdv":
                table = 21
                kind = {"50kW": "Overnight", "350kW": "Fast Charge", "2MW": "Ultra-Fast Charge"}[parameter[:-12]]
                values = frames["costs"].loc[frames["costs"].charger.eq(kind), "rated_power_kw"].unique().tolist()
            elif parameter == "DCFC_rated_power":
                table = 30
                selected = frames["power"].loc[frames["power"].charger.eq("Public - DCFC")]
                selected = selected.loc[selected.year.eq(int(period))] if period.isdigit() else selected.loc[selected.year.ge(2040)]
                values = selected.value.tolist()
                status = "Manual values match Table 12 cost-rating pairs; cited Table 30 installed-fleet ratings differ"
            else:
                table = 30
                kind = "Home - L1" if parameter == "L1_rated_power" else "Public - L2"
                selected = frames["power"].loc[frames["power"].charger.eq(kind)]
                selected = selected.loc[selected.year.eq(int(period))] if period.isdigit() else selected.loc[selected.year.ge(2035)]
                values = selected.value.tolist()
        elif parameter == "max_annual_utilization":
            table = 31 if category == "ldv" else "19, 44"
            status = "Aggregate reviewed override; report is type-specific and omits residential annual UF" if category == "ldv" else "Derived override; count mix, public/private power and energy weighting require reconciliation"
        elif parameter.endswith("capex_aggregation") or parameter.startswith("existing_") and category == "mhdv":
            table = 6 if category == "ldv" else 38
            status = "Fixed modeled composition; published projected composition varies by year and population"
        if values and "cited Table 30" not in status:
            status = "Matches extracted source cells within reported horizon" if all(abs(v - float(row["value"])) < 1e-10 for v in values) else "Differs from extracted source cells"
        rows.append({"vehicle_class": category.upper(), "parameter": parameter, "period": period,
                     "manual_value": row["value"], "unit": row["unit"], "source_table": str(table),
                     "published_values": ", ".join(f"{v:g}" for v in dict.fromkeys(values)),
                     "assessment": status, "manual_note": row["notes"],
                     "manual_source": row["source -> data_source"]})
    return pd.DataFrame(rows)


def mhd_utilization(evidence: dict, settings: dict, operating=None) -> pd.DataFrame:
    """Keep the six-day schedule as a labelled review reference, not production."""
    result = (infrastructure_frames(evidence)["operating"] if operating is None else operating).copy()
    result["service_hours_per_operating_day"] = result.session_hours * result.vehicles_per_day
    result["occupancy_proxy_6_day"] = result.service_hours_per_operating_day / 24 * settings["comparison"]["operating_days_per_week"] / 7
    result["basis"] = "Six-day-per-week review reference; production uses configured operating days/year"
    return result



def compare_costs(bundle, frames: dict, shares: pd.DataFrame, manual: pd.DataFrame, settings: dict):
    """Compare adopted physical components with explicitly held-count alternatives."""
    cer = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    path = resolve_input_path(bundle, "interim", cer["interim_subdir_template"].format(edition=edition),
                              cer["components"]["macro-indicators"]["output_file"])
    converter = CerCurrencyConverter(pd.read_csv(path), scenario=bundle.scenario.economics.cer_scenario,
                                     target_year=bundle.scenario.economics.cost_reference_year)
    production = ChargerRepresentationSettings.model_validate(load_harmonization_rules(bundle, "ev_chargers")["production_representation"])
    l1 = manual.loc[manual.sub_category.eq("ldv") & manual.parameter.eq("L1_capex")]
    fixed = manual.loc[manual.sub_category.eq("all") & manual.parameter.eq("fixed_to_capex_ratio")]
    if len(l1) != 1 or len(fixed) != 1:
        raise ValueError("Charger review requires registered residential L1 cost and fixed O&M ratio")
    years = settings["comparison"]["report_years"]
    core = build_representation(frames, production, years, converter, l1_input=l1.iloc[0],
                                fixed_om_fraction=float(fixed.value.iloc[0]),
                                ld_home_utilization=bundle.scenario.ev_chargers.ld_home_utilization)
    records = []
    for category, population in core.components.groupby("vehicle_class", sort=True):
        initial = previous = None
        for year in years:
            components = population.loc[population.year.eq(year)].set_index(["archetype", "component"]).sort_index()
            counts = components.ports
            if initial is None:
                initial = counts.copy()
            costs, power = components.cost_cad_per_port, components.power_kw
            current = float((counts * costs).sum() / (counts * power).sum())
            alternatives = [("Production physical component mix", counts,
                             "Adopted source-core expenditure divided by rated power"),
                            (f"{years[0]} component counts held (research alternative)", initial,
                             "Baseline component counts held; current-year cost and power retained")]
            if previous is not None:
                alternatives.append(("Net additions without replacements (research alternative)", counts - previous,
                                     "Port differences omit retirements, replacements and vintage power changes"))
            for basis, weights, boundary in alternatives:
                usable = bool(weights.notna().all() and weights.ge(0).all() and weights.sum() > 0)
                records.append({"vehicle_class": category, "source_year": year, "mix_basis": basis,
                    "cost_mcad_per_gw": float((weights * costs).sum() / (weights * power).sum()) if usable else np.nan,
                    "fixed_method_cost": current,
                    "mean_rated_power_kw": float((weights * power).sum() / weights.sum()) if usable else np.nan,
                    "usable_mix": usable,
                    "boundary": boundary if usable else "Not an investment mix: negative component additions or no positive additions"})
            previous = counts.copy()
    return pd.DataFrame(records), path



def build_diagnostic(bundle, *, write: bool = False) -> ChargerInfrastructureDiagnostic:
    from fetching.afdc_charger_stations import build_request, normalize_station_inventory
    evidence, path = load_evidence(bundle)
    settings = load_yaml(resolve_parameter_path(bundle, "charger_infrastructure_diagnosis.yaml"))
    frames = extract_frames(evidence, settings)
    shares = projected_compositions(frames)
    manual_path = resolve_input_path(bundle, "manual", settings["comparison"]["manual_file"])
    manual = pd.read_csv(manual_path, keep_default_na=False, dtype={"currency_year": str, "period": str})
    production, _ = load_charger_inputs(bundle, manual)
    cost_comparison, currency_path = compare_costs(bundle, frames, shares, production, settings)
    vintage_years = pd.DataFrame([{"vintage": v, "source_year": bundle.scenario.periods.evaluation_year(v),
                                  "within_report_count_horizon": bundle.scenario.periods.evaluation_year(v) <= 2040}
                                 for v in bundle.scenario.periods.model])
    afdc_records, afdc_identities = [], {}
    for component in settings["comparison"]["afdc_components"]:
        request = build_request(bundle, component)
        stations = normalize_station_inventory(request.cache_path.read_bytes(), request)
        afdc_records.append(stations.assign(component=component))
        afdc_identities[component] = {"sha256": request.expected_sha256, "bytes": request.expected_bytes,
                                     "date": request.snapshot_date.isoformat(), "url": request.url,
                                     "query": request.query.model_dump(),
                                     "data_quality": bundle.sources.resolved_data_quality("afdc_alternative_fueling_stations", component).row_fields()}
    afdc_stations = pd.concat(afdc_records, ignore_index=True)
    afdc_counts = afdc_stations.groupby(["as_of", "access_code"], as_index=False).agg(
        station_locations=("station_id", "size"), charging_ports=("charging_ports", "sum"),
        level_1_ports=("level_1_ports", "sum"), level_2_ports=("level_2_ports", "sum"),
        dc_fast_ports=("dc_fast_ports", "sum"))
    result = ChargerInfrastructureDiagnostic(
        counts=pd.concat([frames["ldv"], frames["mhdv"]], ignore_index=True), shares=shares,
        power=frames["power"], utilization=frames["utilization"], costs=frames["costs"],
        cost_comparison=cost_comparison, manual_comparison=manual_reconciliation(manual, frames),
        anomalies=source_anomalies(frames), mhd_utilization=mhd_utilization(evidence, settings, frames["operating"]),
        vintage_years=vintage_years, afdc_counts=afdc_counts, afdc_stations=afdc_stations, metadata={
            "source_key": evidence["source_key"], "component_key": evidence["component_key"],
            "publication_date": evidence["publication_date"], "url": evidence["url"],
            "source_sha256": evidence["source_sha256"], "source_path": str(path),
            "data_quality": evidence["data_quality"], "manual_sha256": file_sha256(manual_path),
            "report_context": evidence["context"],
            "configured_notebook_scenario": settings["scenario"],
            "currency_path": str(currency_path),
            "currency_edition": bundle.scenario.sources.selections["cer_canadas_energy_future"].edition,
            "currency_scenario": bundle.scenario.economics.cer_scenario,
            "currency_sha256": file_sha256(currency_path),
            "diagnostic_config_sha256": file_sha256(resolve_parameter_path(bundle, "charger_infrastructure_diagnosis.yaml")),
            "source_config_sha256": file_sha256(resolve_parameter_path(bundle, "dunsky_charger_inputs.yaml")),
            "cost_dollar_year": bundle.scenario.economics.cost_reference_year,
            "production_utilization": {"home_empirical_scenario_proxy": bundle.scenario.ev_chargers.ld_home_utilization,
                                       "mhdv_basis": "Table 19 durations and throughput × production operating days/year"},
            "fetched_dunsky_inputs_adopted": len(production) - len(manual),
            "production_adoption": True,
            "configured_charger_type_classes": bundle.scenario.ev_chargers.charger_type_classes,
            "afdc_inventories": afdc_identities,
        }, source_tables=evidence["tables"],
    )
    if write:
        out = resolve_artifact_path(bundle, "charger_infrastructure_diagnostic")
        for name in ("counts", "shares", "power", "utilization", "costs", "cost_comparison", "manual_comparison", "anomalies", "mhd_utilization", "vintage_years", "afdc_counts", "afdc_stations"):
            write_dataframe_atomic(getattr(result, name), out / f"{name}.csv")
        write_text_atomic(json.dumps(result.metadata, indent=2) + "\n", out / "manifest.json", newline="\n")
    return result


def load_diagnostic() -> ChargerInfrastructureDiagnostic:
    root = find_repo_root()
    settings = load_yaml(root / "config/parameters/charger_infrastructure_diagnosis.yaml")
    return build_diagnostic(load_config_bundle(root / settings["scenario"]))


def composition_chart(diagnostic, vehicle_class, scenario, population):
    selected = diagnostic.shares.loc[diagnostic.shares.vehicle_class.eq(vehicle_class)
                                    & diagnostic.shares.scenario.eq(scenario)
                                    & diagnostic.shares.population.eq(population)]
    chart = alt.Chart(selected).mark_bar().encode(
        x=alt.X("year:O", title="Projection year"), y=alt.Y("port_share:Q", title="Share of projected ports", axis=alt.Axis(format="%")),
        color=alt.Color("charger:N", title="Port type"),
        tooltip=["charger:N", "year:O", alt.Tooltip("ports:Q", format=",.0f"), alt.Tooltip("port_share:Q", format=".2%")],
    ).properties(width=650, height=270)
    return identified_chart(chart, "charger-infrastructure-composition", f"{vehicle_class}: {population}")


def cost_chart(diagnostic, vehicle_class):
    chart = alt.Chart(diagnostic.cost_comparison.loc[diagnostic.cost_comparison.vehicle_class.eq(vehicle_class)]).mark_line(point=True).encode(
        x=alt.X("source_year:O", title="Cost source year"),
        y=alt.Y("cost_mcad_per_gw:Q", title=f"Million CAD{diagnostic.metadata['cost_dollar_year']}/GW", scale=alt.Scale(zero=False)),
        color=alt.Color("mix_basis:N", title="Composition basis", legend=alt.Legend(orient="bottom", labelLimit=500)),
        tooltip=["source_year:O", "mix_basis:N", alt.Tooltip("cost_mcad_per_gw:Q", format=".2f")],
    ).properties(width=650, height=270)
    return identified_chart(chart, "charger-infrastructure-cost", f"{vehicle_class}: mix sensitivity with production costs and power")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/validation_baseline.yaml")
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    result = build_diagnostic(load_config_bundle(args.scenario), write=args.write_report)
    print(json.dumps({"source_sha256": result.metadata["source_sha256"], "count_rows": len(result.counts),
                      "manual_rows": len(result.manual_comparison), "anomaly_rows": len(result.anomalies)}))


if __name__ == "__main__":
    main()
