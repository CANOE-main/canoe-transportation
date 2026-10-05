"""Prepare EV charging infrastructure rows from reviewed inputs, without SQLite I/O."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import CostFixed, CostInvest, Efficiency, ExistingCapacity, LimitAnnualCapacityFactor

from parameterization.currency import CerCurrencyConverter
from parameterization.manual_parameters import validate_manual_registry
from utils import (
    ConfigBundle, file_sha256, load_config_bundle, load_harmonization_rules,
    resolve_artifact_path, resolve_input_path, write_dataframe_atomic,
)
from validation.insertion import validate_parameter_rows
from validation.provenance import (
    ResolvedProvenance, resolve_composite_provenance, resolve_provenance,
    source_id_mapping,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChargerPreparation:
    capacity_rows: list[ExistingCapacity]
    invest_rows: list[CostInvest]
    fixed_rows: list[CostFixed]
    efficiency_rows: list[Efficiency]
    utilization: pd.DataFrame
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, Any]


def _manual_value(manual: pd.DataFrame, category: str, parameter: str, period: str) -> float:
    selected = manual.loc[
        manual.category.eq("charger") & manual.sub_category.eq(category)
        & manual.parameter.eq(parameter) & manual.period.eq(period)
    ]
    if len(selected) != 1:
        raise ValueError(f"Expected one reviewed charger value: {category}/{parameter}/{period}")
    value = float(selected.iloc[0].value)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid reviewed charger value: {category}/{parameter}/{period}")
    return value


def _period_value(manual: pd.DataFrame, category: str, parameter: str, year: int) -> pd.Series:
    selected = manual.loc[
        manual.category.eq("charger") & manual.sub_category.eq(category)
        & manual.parameter.eq(parameter)
    ]
    exact = selected.loc[selected.period.eq(str(year))]
    if len(exact) == 1:
        return exact.iloc[0]
    all_rows = selected.loc[selected.period.eq("all")]
    if len(all_rows) == 1:
        return all_rows.iloc[0]
    previous = [int(item) for item in selected.period if str(item).isdigit()]
    through = selected.loc[selected.period.eq("through_2050")]
    if len(through) == 1 and previous and year > max(previous) and year <= 2050:
        return through.iloc[0]
    remainder = selected.loc[selected.period.eq("remainder")]
    if len(remainder) == 1 and not exact.empty:
        raise ValueError(f"Ambiguous charger period selector: {category}/{parameter}/{year}")
    if len(remainder) == 1:
        return remainder.iloc[0]
    raise ValueError(f"Missing charger period selector: {category}/{parameter}/{year}")


def _context(
    bundle: ConfigBundle, components: list[tuple[str, str]], *, name: str,
    digest: str, governing: str | None = None,
    additional: list[ResolvedProvenance] | None = None,
) -> ResolvedProvenance:
    inputs = [
        resolve_provenance(
            bundle.sources, source_key=source, component_key=component,
            transformation="reviewed_ev_charger_input", transformation_version="1",
        )
        for source, component in components
    ]
    inputs.extend(additional or [])
    governing_source = governing or components[0][0]
    governing_id = source_id_mapping(bundle.sources)[governing_source]
    quality = next(item.data_quality for item in inputs if item.source_id == governing_id)
    return resolve_composite_provenance(
        inputs=inputs, dataset_key=f"ev_chargers.{name}",
        transformation="reviewed EV charger parameterization",
        transformation_version="1", governing_source_id=governing_id,
        data_quality=quality, value_variant={"input_digest": digest, "periods": bundle.scenario.periods.model_dump()},
    )


def _validate_shares(manual: pd.DataFrame) -> None:
    groups = (
        ("ldv", "existing_private", ("L1", "L2"), "existing"),
        ("mhdv", "existing", ("50kW", "350kW"), "existing"),
        ("ldv", "cost", ("L1", "L2", "DCFC"), "through_2050"),
        ("mhdv", "cost", ("50kW", "350kW", "2MW"), "through_2050"),
    )
    for category, usage, types, period in groups:
        names = (
            [f"existing_private_{kind}_share" for kind in types]
            if usage == "existing_private" else
            [f"existing_{kind}_share" for kind in types]
            if usage == "existing" else
            [f"{kind}_cost_aggregation" for kind in types]
        )
        values = [_manual_value(manual, category, name, period) for name in names]
        if any(value > 1 for value in values) or not math.isclose(sum(values), 1, abs_tol=1e-9):
            raise ValueError(f"Charger {category}/{usage} shares do not sum to one")


def prepare_ev_charger_rows(
    bundle: ConfigBundle, *, existing_capacity_rows: list[ExistingCapacity],
    existing_capacity_contexts: list[ResolvedProvenance],
) -> ChargerPreparation:
    """Reuse prepared vehicle stock and return validated charger products."""
    rules = load_harmonization_rules(bundle, "ev_chargers")
    manual_rules = load_harmonization_rules(bundle, "manual_parameters")
    _, frames = validate_manual_registry(
        bundle, source_column=manual_rules["source_column"],
        notes_column=manual_rules["notes_column"],
        selected_files={rules["manual_file"]},
    )
    manual = frames[rules["manual_file"]]
    _validate_shares(manual)
    template = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"),
                           dtype=str, keep_default_na=False)
    commodity = pd.read_csv(resolve_input_path(bundle, "template", "commodity.csv"),
                            dtype=str, keep_default_na=False)
    technology = template.set_index("tech")
    if not technology.index.is_unique:
        raise ValueError("Duplicate technology template IDs")
    for category, mapping in rules["technology"].items():
        for kind in ("existing", "new"):
            tech = mapping[kind]
            if tech not in technology.index or (
                technology.loc[tech, "category"] != "charger"
                or technology.loc[tech, "sub_category"] != category
            ):
                raise ValueError(f"Charger technology mapping changed: {category}/{kind}")
        if mapping["output"] not in set(commodity.name):
            raise ValueError(f"Unknown charger output commodity: {mapping['output']}")
    if rules["input_commodity"] not in set(commodity.name):
        raise ValueError("Unknown charger input commodity")

    dashboard = load_harmonization_rules(bundle, "assorted_sources")["tc_ev_dashboard"]
    source_dir = resolve_artifact_path(bundle, "tc_ev_dashboard_interim")
    charger_source = dashboard["public_chargers"]
    province_path = source_dir / charger_source["province_output_file"]
    type_path = source_dir / charger_source["type_output_file"]
    manual_path = resolve_input_path(bundle, "manual", rules["manual_file"])
    vehicle_path = resolve_artifact_path(bundle, "existing_capacity_processed") / "existing_capacity.csv"
    provincial = pd.read_csv(province_path)
    types = pd.read_csv(type_path)
    if (
        len(provincial) != 13 or set(provincial.source_region) != set(charger_source["source_regions"])
        or provincial.source_region.duplicated().any()
        or set(types.charger_type) != {"L2", "DCFC"}
        or types.charger_type.duplicated().any()
        or set(provincial.units) != {"ports"} or set(types.units) != {"ports"}
        or set(provincial.as_of) != {charger_source["as_of"]}
        or set(types.as_of) != {charger_source["as_of"]}
        or set(provincial.source_year) != {bundle.scenario.sources.selections[dashboard["source_id"]].year}
        or set(types.source_year) != set(provincial.source_year)
    ):
        raise ValueError("Normalized TC public charger coverage or edition changed")
    if any(types.public_chargers < 0) or any(provincial.public_chargers < 0):
        raise ValueError("Negative TC public charger count")
    national = int(types.public_chargers.sum())
    if (int(provincial.public_chargers.sum()) != national
            or not math.isclose(float(types.national_share.sum()), 1, abs_tol=1e-9)
            or any(abs(types.national_share - types.public_chargers / national) > 1e-12)):
        raise ValueError("TC public charger national totals or type shares differ")
    region_map = rules["public_charger_region_map"]
    if set(sum((list(labels) for labels in region_map.values()), [])) != set(provincial.source_region):
        raise ValueError("TC provinces/territories do not map to template regions")
    model_regions = set(pd.read_csv(resolve_input_path(bundle, "template", "region.csv")).region)
    if set(region_map) != model_regions:
        raise ValueError("Charger region mapping does not cover the template")
    public_by_region = {
        region: int(provincial.loc[provincial.source_region.isin(labels), "public_chargers"].sum())
        for region, labels in region_map.items()
    }
    if sum(public_by_region.values()) != national:
        raise ValueError("TC public chargers were lost during regional harmonization")

    stock_class_map = rules["stock_classes"]
    stock_data_ids: set[str] = set()
    stock_by_region: dict[tuple[str, str], float] = {}
    for row in existing_capacity_rows:
        if row.tech not in technology.index:
            raise ValueError(f"Vehicle capacity technology is absent from template: {row.tech}")
        item = technology.loc[row.tech]
        matched = [category for category, classes in stock_class_map.items()
                   if item["category"] in classes and item["sub_category"].startswith("bev")]
        if not matched:
            continue
        if row.units != "k vehicles" or row.capacity < 0:
            raise ValueError(f"BEV stock has invalid units or capacity: {row.tech}")
        key = (row.region, matched[0])
        stock_by_region[key] = stock_by_region.get(key, 0.0) + row.capacity * 1000
        stock_data_ids.add(row.data_id)
    expected_stock = {(region, category) for region in model_regions for category in stock_class_map}
    missing_stock = sorted(expected_stock - set(stock_by_region))
    for key in missing_stock:
        stock_by_region[key] = 0.0
    if missing_stock:
        LOGGER.info("Zero BEV stock, omitting historical charger rows: %s", missing_stock)
    vehicle_contexts = {item.data_id: item for item in existing_capacity_contexts}
    if not stock_data_ids <= set(vehicle_contexts):
        raise ValueError("BEV stock provenance contexts are missing")
    paths = [manual_path, province_path, type_path, vehicle_path]
    digest = hashlib.sha256(json.dumps(
        {"files": [(path.name, file_sha256(path)) for path in paths],
         "scenario": bundle.scenario.ev_chargers.model_dump(),
         "rules": rules, "stock": sorted((key, value) for key, value in stock_by_region.items())},
        sort_keys=True,
    ).encode()).hexdigest()
    dunsky = ("dunsky_ev_charging_infrastructure_2024", "charger_shares_and_utilization")
    tc = ("transport_canada_ev_dashboard", "public_light_duty_chargers")
    probe = ("pollution_probe_ev_charging_survey_2024", "private_charger_shares")
    capacity_context = _context(
        bundle, [tc, dunsky, probe], name="existing_capacity", digest=digest,
        governing=tc[0], additional=[vehicle_contexts[key] for key in sorted(stock_data_ids)],
    )
    port_records = []
    capacity_records = []
    for region in sorted(model_regions):
        for category in ("ldv", "mhdv"):
            evs = stock_by_region[region, category]
            ratio = (bundle.scenario.ev_chargers.ld_evs_per_port if category == "ldv"
                     else bundle.scenario.ev_chargers.mhd_evs_per_port)
            required_ports = evs / ratio
            port_counts: dict[str, float]
            public = None
            if category == "ldv":
                public = public_by_region[region]
                private = required_ports - public
                if private < 0:
                    raise ValueError(f"Observed public ports exceed required LD ports in {region}")
                type_shares = types.set_index("charger_type").national_share.to_dict()
                port_counts = {
                    "L1": private * _manual_value(manual, "ldv", "existing_private_L1_share", "existing"),
                    "L2": private * _manual_value(manual, "ldv", "existing_private_L2_share", "existing")
                          + public * type_shares["L2"],
                    "DCFC": public * type_shares["DCFC"],
                }
            else:
                private = None
                port_counts = {
                    kind: required_ports * _manual_value(manual, "mhdv", f"existing_{kind}_share", "existing")
                    for kind in ("50kW", "350kW")
                }
            if not math.isclose(sum(port_counts.values()), required_ports, rel_tol=1e-12, abs_tol=1e-8):
                raise ValueError(f"Charger port reconciliation failed: {region}/{category}")
            power_kw = {
                kind: _manual_value(manual, category, f"existing_{kind}_power", "existing")
                for kind in port_counts
            }
            for kind, count in port_counts.items():
                selected = manual.loc[
                    manual.category.eq("charger") & manual.sub_category.eq(category)
                    & manual.parameter.eq(f"existing_{kind}_power")
                ]
                if len(selected) != 1 or selected.iloc[0].unit != "kW":
                    raise ValueError(f"Charger power unit must be kW: {category}/{kind}")
            gw = sum(port_counts[kind] * power_kw[kind] for kind in port_counts) / 1_000_000
            if not math.isfinite(gw) or gw < 0:
                raise ValueError(f"Invalid charger GW capacity: {region}/{category}")
            port_records.append({
                "region": region, "charger_class": category, "bev_vehicles": evs,
                "ev_per_port": ratio, "required_ports": required_ports,
                "observed_public_ports": public, "derived_private_ports": private,
                **{f"{kind}_ports": count for kind, count in port_counts.items()},
                "capacity_gw": gw, "bev_stock_year": bundle.scenario.periods.base_year,
                "public_charger_as_of": charger_source["as_of"],
            })
            if gw > 0:
                capacity_records.append({
                    "region": region, "tech": rules["technology"][category]["existing"],
                    "vintage": bundle.scenario.periods.latest_observed_vintage, "capacity": gw,
                    "units": "GW", "notes": f"{category} BEV stock and {charger_source['as_of']} public ports; EV/port {ratio:g}",
                })
    capacity_rows = validate_parameter_rows(ExistingCapacity, capacity_records, capacity_context)

    cer = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    macro_path = resolve_input_path(
        bundle, "interim", cer["interim_subdir_template"].format(edition=edition),
        cer["components"]["macro-indicators"]["output_file"],
    )
    converter = CerCurrencyConverter(
        pd.read_csv(macro_path), scenario=bundle.scenario.economics.cer_scenario,
        target_year=bundle.scenario.economics.cost_reference_year,
    )
    cost_digest = hashlib.sha256((digest + file_sha256(macro_path)).encode()).hexdigest()
    nlr = ("nlr_alternative_fueling_infrastructure_2024", "residential_l1_cost")
    oeo = ("open_energy_outlook_2022", "charger_fixed_cost_ratio")
    cer_macro = ("cer_canadas_energy_future", "macro-indicators")
    invest_contexts = {
        "ldv": _context(bundle, [dunsky, nlr, cer_macro], name="ldv_cost_invest", digest=cost_digest),
        "mhdv": _context(bundle, [dunsky, cer_macro], name="mhdv_cost_invest", digest=cost_digest),
    }
    fixed_contexts = {
        category: _context(bundle, [dunsky, *([nlr] if category == "ldv" else []), oeo, cer_macro],
                           name=f"{category}_cost_fixed", digest=cost_digest)
        for category in stock_class_map
    }
    if (bundle.scenario.economics.cost_reference_currency != "CAD"
            or bundle.scenario.economics.cost_reference_year != 2020):
        raise ValueError("Charger cost units require the reviewed 2020 CAD scenario")
    cost_types = {"ldv": ("L1", "L2", "DCFC"), "mhdv": ("50kW", "350kW", "2MW")}
    invest_rows, fixed_rows, cost_audit = [], [], []
    fixed_ratio = _manual_value(manual, "all", "fixed_to_capex_ratio", "all")
    if not 0 <= fixed_ratio <= 1:
        raise ValueError("Invalid reviewed charger fixed cost ratio")
    for category, kinds in cost_types.items():
        existing_vintage = bundle.scenario.periods.latest_observed_vintage
        for vintage in [existing_vintage, *bundle.scenario.periods.model]:
            source_year = (
                bundle.scenario.periods.model[0] if vintage == existing_vintage
                else bundle.scenario.periods.projection_year(vintage, legacy_at_end=False)
            )
            weighted = 0.0
            for kind in kinds:
                share = _manual_value(manual, category, f"{kind}_cost_aggregation", "through_2050")
                selected = _period_value(manual, category, f"{kind}_cost_per_GW", source_year)
                if selected.unit not in {"M CAD", "M USD"} or not str(selected.currency_year).isdigit():
                    raise ValueError(f"Charger cost currency contract changed: {category}/{kind}")
                converted = converter.convert(
                    float(selected.value), currency=selected.unit.split()[1],
                    dollar_year=int(selected.currency_year),
                )
                weighted += share * converted.target_cad_value
                cost_audit.append({
                    "charger_class": category, "vintage": vintage, "source_year": source_year,
                    "charger_type": kind, "count_share": share,
                    "source_cost_per_gw": float(selected.value), "source_unit": selected.unit,
                    "source_dollar_year": int(selected.currency_year),
                    "cad_2020_cost_per_gw": converted.target_cad_value,
                    "weighted_cad_2020_cost_per_gw": share * converted.target_cad_value,
                })
            if not math.isfinite(weighted) or weighted <= 0:
                raise ValueError(f"Invalid aggregated charger cost: {category}/{vintage}")
            for region in sorted(model_regions):
                if vintage == existing_vintage and stock_by_region[region, category] == 0:
                    continue
                tech = rules["technology"][category]["existing" if vintage == existing_vintage else "new"]
                base_record = {
                    "region": region, "tech": tech, "vintage": vintage,
                    "units": "$M 2020CAD / GW",
                    "notes": f"Reviewed charger type count shares; {source_year} cost assumptions",
                }
                if vintage != existing_vintage:
                    invest_rows.extend(validate_parameter_rows(CostInvest, [
                        {**base_record, "cost": weighted}
                    ], invest_contexts[category]))
                periods = [period for period in bundle.scenario.periods.model if period >= vintage]
                fixed_rows.extend(validate_parameter_rows(CostFixed, [
                    {**base_record, "period": period, "cost": weighted * fixed_ratio,
                     "notes": f"Annual fixed charger cost = {fixed_ratio:g} × investment cost"}
                    for period in periods
                ], fixed_contexts[category]))

    efficiency_contexts = {
        "ldv": _context(bundle, [("icct_ev_charge_2024", "ld_charger_efficiency")],
                        name="ldv_efficiency", digest=digest),
        "mhdv": _context(bundle, [("icct_hdv_charge_2024", "mhd_charger_efficiency")],
                         name="mhdv_efficiency", digest=digest),
    }
    efficiency_rows = []
    utilization_context = _context(bundle, [dunsky], name="utilization", digest=digest)
    utilization_records = []
    for category in ("ldv", "mhdv"):
        efficiency = _manual_value(manual, category, "efficiency", "all")
        if not 0 < efficiency <= 1:
            raise ValueError(f"Invalid charger efficiency: {category}")
        mapping = rules["technology"][category]
        for region in sorted(model_regions):
            for kind, vintages in (("existing", [bundle.scenario.periods.latest_observed_vintage]),
                                   ("new", bundle.scenario.periods.model)):
                if kind == "existing" and stock_by_region[region, category] == 0:
                    continue
                tech = mapping[kind]
                efficiency_rows.extend(validate_parameter_rows(Efficiency, [
                    {"region": region, "tech": tech, "vintage": vintage,
                     "input_comm": rules["input_commodity"], "output_comm": mapping["output"],
                     "efficiency": efficiency, "units": "PJ/PJ",
                     "notes": f"Reviewed {category} charging efficiency"}
                    for vintage in vintages
                ], efficiency_contexts[category]))
                for period in bundle.scenario.periods.model:
                    if kind == "new" and period not in vintages:
                        continue
                    source_year = bundle.scenario.periods.projection_year(period, legacy_at_end=False)
                    row = _period_value(manual, category, "max_annual_utilization", source_year)
                    factor = float(row.value)
                    if not 0 < factor < 1:
                        raise ValueError(f"Invalid charger utilization: {category}/{period}")
                    utilization_records.append({
                        "region": region, "period": period, "source_year": source_year, "tech": tech,
                        "output_comm": mapping["output"], "operator": "≤",
                        "factor": factor, "units": "fraction",
                        "notes": f"Reviewed {category} annual charging utilization",
                        **utilization_context.parameter_fields(),
                    })
    utilization = pd.DataFrame(utilization_records).sort_values(
        ["region", "period", "tech"], kind="stable"
    ).reset_index(drop=True)
    if utilization.duplicated(["region", "period", "tech", "output_comm", "operator"]).any():
        raise ValueError("Duplicate charger utilization keys")
    if "period" in LimitAnnualCapacityFactor.model_fields:
        raise ValueError("Charger utilization schema changed; review period-row insertion")

    contexts = [capacity_context, *invest_contexts.values(), *fixed_contexts.values(),
                *efficiency_contexts.values()]
    output_dir = resolve_artifact_path(bundle, "ev_chargers_processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    frames_to_write = {
        rules["capacity_output_file"]: pd.DataFrame([row.model_dump() for row in capacity_rows]),
        rules["port_audit_file"]: pd.DataFrame(port_records),
        rules["invest_output_file"]: pd.DataFrame([row.model_dump() for row in invest_rows]),
        rules["fixed_output_file"]: pd.DataFrame([row.model_dump() for row in fixed_rows]),
        rules["utilization_output_file"]: utilization,
        rules["efficiency_output_file"]: pd.DataFrame([row.model_dump() for row in efficiency_rows]),
    }
    for filename, frame in frames_to_write.items():
        write_dataframe_atomic(frame, output_dir / filename)
    audit = {
        "period_mapping": bundle.scenario.periods.audit(),
        "capacity_rows": len(capacity_rows), "invest_rows": len(invest_rows),
        "fixed_rows": len(fixed_rows), "efficiency_rows": len(efficiency_rows),
        "existing_investment_rows_excluded": len(capacity_rows),
        "utilization_rows": len(utilization), "utilization_schema_inserted": False,
        "utilization_provenance": {
            "data_id": utilization_context.data_id,
            "contributors": [item.source_key for item in utilization_context.contributors],
        },
        "tc_public_ports": national, "tc_type_counts": types.set_index("charger_type").public_chargers.to_dict(),
        "tc_as_of": charger_source["as_of"], "bev_stock_year": bundle.scenario.periods.base_year,
        "zero_bev_stock_skipped": [list(key) for key in missing_stock],
        "port_reconciliation": port_records, "cost_conversion": cost_audit,
        "input_sha256": {path.name: file_sha256(path) for path in [*paths, macro_path]},
    }
    validation_dir = resolve_artifact_path(bundle, "ev_chargers_validation")
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / rules["validation_file"]).write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    LOGGER.info(
        "EV charger rows: capacity=%d invest=%d fixed=%d efficiency=%d utilization_artifact=%d; public_ports=%d",
        len(capacity_rows), len(invest_rows), len(fixed_rows), len(efficiency_rows),
        len(utilization), national,
    )
    LOGGER.info("Excluded %d existing charger investment rows; retained their annual fixed-cost basis", len(capacity_rows))
    return ChargerPreparation(capacity_rows, invest_rows, fixed_rows, efficiency_rows,
                              utilization, contexts, audit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare reviewed EV charger parameters")
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    args = parser.parse_args()
    from parameterization.build_existing_capacity import prepare_existing_capacity_rows

    bundle = load_config_bundle(args.scenario)
    vehicle_rows, vehicle_contexts, _ = prepare_existing_capacity_rows(bundle)
    result = prepare_ev_charger_rows(
        bundle, existing_capacity_rows=vehicle_rows,
        existing_capacity_contexts=vehicle_contexts,
    )
    print(json.dumps({key: value for key, value in result.audit.items()
                      if key.endswith("_rows") or key in {"tc_public_ports", "tc_as_of"}},
                     indent=2))


if __name__ == "__main__":
    main()
