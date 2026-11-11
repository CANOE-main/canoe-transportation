"""Prepare EV charging and H2 refueling rows from reviewed inputs, without SQLite I/O."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import CapacityToActivity, CostFixed, CostInvest, DataSet, Efficiency, ExistingCapacity, LifetimeTech, LimitAnnualCapacityFactor

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


def _public_charger_totals(
    provincial: pd.DataFrame, types: pd.DataFrame, *, precedence: str,
) -> tuple[int, int]:
    """Validate native observations while keeping geographic totals and type mix distinct."""
    if precedence not in {"require_equal", "provincial"}:
        raise ValueError(f"Unknown TC public charger national_total_precedence: {precedence!r}")
    validated_counts = []
    for frame in (provincial, types):
        counts = pd.to_numeric(frame.public_chargers, errors="coerce")
        if counts.isna().any() or any(not math.isfinite(value) or value != int(value) for value in counts):
            raise ValueError("TC public charger counts must be finite integers")
        if any(counts < 0):
            raise ValueError("Negative TC public charger count")
        validated_counts.append(counts)
    provincial_total = int(validated_counts[0].sum())
    chart_total = int(validated_counts[1].sum())
    if chart_total <= 0:
        raise ValueError("TC public charger type total must be positive")
    shares = pd.to_numeric(types.national_share, errors="coerce")
    if (shares.isna().any() or any(not math.isfinite(value) or value < 0 for value in shares)
            or not math.isclose(float(shares.sum()), 1, abs_tol=1e-9)
            or any(abs(shares - validated_counts[1] / chart_total) > 1e-12)):
        raise ValueError("TC public charger type shares differ from the published type counts")
    if precedence == "require_equal" and provincial_total != chart_total:
        raise ValueError("TC public charger national totals or type shares differ")
    if provincial_total != chart_total:
        LOGGER.warning(
            "TC public charger discrepancy: provincial=%d, type chart=%d, difference=%+d; "
            "using provincial counts and the published type mix",
            provincial_total, chart_total, provincial_total - chart_total,
        )
    return provincial_total, chart_total


@dataclass(frozen=True)
class ChargerPreparation:
    capacity_rows: list[ExistingCapacity]
    invest_rows: list[CostInvest]
    fixed_rows: list[CostFixed]
    efficiency_rows: list[Efficiency]
    utilization_rows: list[LimitAnnualCapacityFactor]
    utilization: pd.DataFrame
    provenance_contexts: list[ResolvedProvenance]
    audit: dict[str, Any]
    assumption_dataset: DataSet
    capacity_to_activity_rows: list[CapacityToActivity]


def _prepared_stock_identity(
    rows: pd.DataFrame, contexts: list[ResolvedProvenance],
) -> tuple[dict[str, Any], dict[str, ResolvedProvenance]]:
    """Identify consumed BEV rows and lineage, independently of published CSVs.

    Preserve row order because it is the order of floating-point accumulation.
    Context order and identical repeated contexts do not affect resolution. Unused
    contexts and rows excluded by the caller's region/technology selection are not
    dependencies. The version names this serialization contract, not a source edition.
    """
    keys = list(rows[["region", "tech", "vintage"]].itertuples(index=False, name=None))
    if len(set(keys)) != len(keys):
        raise ValueError("Duplicate prepared BEV stock keys")
    data_ids = set(rows.data_id)
    selected: dict[str, ResolvedProvenance] = {}
    for context in contexts:
        if context.data_id not in data_ids:
            continue
        if context.data_id in selected and selected[context.data_id] != context:
            raise ValueError(f"Conflicting BEV stock provenance context: {context.data_id}")
        selected[context.data_id] = context
    if data_ids - set(selected):
        raise ValueError("BEV stock provenance contexts are missing")
    payload = {
        "contract": "supplied_bev_physical_stock_v2",
        "rows": rows.to_dict("records"),
        "contexts": [selected[key].model_dump(mode="json") for key in sorted(selected)],
    }
    identity = {
        "contract": payload["contract"],
        "sha256": hashlib.sha256(json.dumps(
            payload, sort_keys=True, allow_nan=False, separators=(",", ":"),
        ).encode("utf-8")).hexdigest(),
        "row_count": len(rows),
        "data_ids": sorted(data_ids),
    }
    return identity, selected


def prepare_charger_capacity_to_activity(
    bundle: ConfigBundle, *, technology: pd.DataFrame, regions: set[str],
) -> tuple[DataSet, list[CapacityToActivity]]:
    """Register internal charger accounting and expand the physical conversion."""
    rules = load_harmonization_rules(bundle, "ev_chargers")
    accounting = _charger_electricity_accounting(rules)
    manual_path = resolve_input_path(bundle, "manual", rules["capacity_to_activity_file"])
    template_path = resolve_input_path(bundle, "template", "technology.csv")
    required = {"category", "sub_category", "c2a", "units", "notes"}
    manual = pd.read_csv(manual_path, usecols=lambda column: column in required)
    if required - set(manual):
        raise ValueError("Charger capacity-to-activity input lacks required columns")
    selected = manual.loc[manual.category.eq("charger")]
    if (len(selected) != 1 or not selected.sub_category.eq("all").all()
            or selected[list(required)].isna().any().any()):
        raise ValueError("Expected one reviewed charger/all capacity-to-activity value")
    record = selected.iloc[0]
    value = float(record.c2a)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Charger capacity-to-activity must be finite and positive")
    if record.units != rules["capacity_to_activity_units"]:
        raise ValueError("Charger capacity-to-activity units differ from configured units")
    if not str(record.notes).strip():
        raise ValueError("Charger capacity-to-activity lacks reviewed notes")
    owners = {
        mapping[kind] for mapping in rules["technology"].values()
        for kind in ("existing", "new")
    }
    template_owners = technology.loc[technology.category.eq("charger"), "tech"]
    if template_owners.duplicated().any() or set(template_owners) != owners:
        raise ValueError("Charger capacity-to-activity does not cover template technologies")
    digest = hashlib.sha256()
    for path in (manual_path, template_path):
        digest.update(path.name.encode("utf-8") + b"\0" + path.read_bytes() + b"\0")
    digest.update(json.dumps({
        "electricity_accounting": accounting,
        "technology": rules["technology"],
        "input_commodity": rules["input_commodity"],
    }, sort_keys=True, allow_nan=False).encode("utf-8"))
    version = digest.hexdigest()[:16]
    dataset = DataSet(
        data_id=f"canoe-charger-internal-assumptions:{version}", version=version,
        label="CANOE internal charger accounting and capacity conversion",
        description="Version-controlled capacity_to_activity.csv physical energy/power conversion and configured unity electricity transfer: vehicle electricity consumption includes charging losses. Backend accounting assumptions, not external source measurements.",
    )
    rows = [
        CapacityToActivity(region=region, tech=tech, c2a=value, units=record.units,
                           notes=record.notes, data_id=dataset.data_id)
        for region in sorted(regions) for tech in sorted(owners)
    ]
    LOGGER.info("Prepared %d charger capacity_to_activity rows: %s %s per year",
                len(rows), value, record.units)
    return dataset, rows


def _charger_electricity_accounting(rules: dict) -> dict:
    """Keep charging losses in vehicle consumption, once, across the shared edge."""
    accounting = rules.get("electricity_accounting", {})
    if (not isinstance(accounting, dict)
            or accounting.get("vehicle_energy_basis") != "grid_electricity_including_charging_losses"):
        raise ValueError("Unsupported charger vehicle electricity energy basis")
    value = accounting.get("charger_transfer_efficiency")
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value != 1.0):
        raise ValueError("Charging-inclusive vehicle electricity requires unity charger transfer efficiency")
    return accounting


def _manual_value(
    manual: pd.DataFrame, category: str, parameter: str, period: str, *,
    infrastructure_category: str = "charger",
) -> float:
    selected = manual.loc[
        manual.category.eq(infrastructure_category) & manual.sub_category.eq(category)
        & manual.parameter.eq(parameter) & manual.period.eq(period)
    ]
    if len(selected) != 1:
        raise ValueError(f"Expected one reviewed {infrastructure_category} value: {category}/{parameter}/{period}")
    value = float(selected.iloc[0].value)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid reviewed {infrastructure_category} value: {category}/{parameter}/{period}")
    return value


def _period_value(
    manual: pd.DataFrame, category: str, parameter: str, year: int, *,
    infrastructure_category: str = "charger",
) -> pd.Series:
    selected = manual.loc[
        manual.category.eq(infrastructure_category) & manual.sub_category.eq(category)
        & manual.parameter.eq(parameter)
    ]
    if selected.period.duplicated().any():
        raise ValueError(f"Duplicate charger period selector: {category}/{parameter}")
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


def _rated_power(manual: pd.DataFrame, category: str, kind: str, source_year: int) -> pd.Series:
    """Select the rated power for the same calendar year as its source cost."""
    row = _period_value(manual, category, f"{kind}_rated_power", source_year)
    value = float(row.value)
    if row.unit != "kW" or not math.isfinite(value) or value <= 0:
        raise ValueError(f"Charger rated power must be positive finite kW: {category}/{kind}/{source_year}")
    return row


def charger_cost_components(
    manual: pd.DataFrame, category: str, source_year: int,
    converter: CerCurrencyConverter,
) -> tuple[float, list[dict[str, Any]]]:
    """Divide expenditure per representative port by its rated kW.

    Count shares define the equipment mix. Convert each native per-port cost to
    target-year CAD before summing expenditure. CAD/kW is numerically equal to
    million CAD/GW; no additional million factor is applied. Power shares are
    derived audit evidence, never independent modelling assumptions.
    """
    cost_types = {"ldv": ("L1", "L2", "DCFC"), "mhdv": ("50kW", "350kW", "2MW")}
    if category not in cost_types:
        raise ValueError(f"Unknown charger cost category: {category}")
    records = []
    for kind in cost_types[category]:
        share = _manual_value(manual, category, f"{kind}_capex_aggregation", "through_2050")
        if share > 1:
            raise ValueError(f"Invalid charger cost count share: {category}/{kind}")
        cost = _period_value(manual, category, f"{kind}_capex", source_year)
        power = _rated_power(manual, category, kind, source_year)
        if cost.unit not in {"CAD", "USD"} or not str(cost.currency_year).isdigit():
            raise ValueError(f"Charger capex requires native CAD/USD per port and dollar year: {category}/{kind}")
        value = float(cost.value)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Charger capex must be positive and finite: {category}/{kind}")
        converted = converter.convert(value, currency=cost.unit, dollar_year=int(cost.currency_year))
        kw = float(power.value)
        records.append({
            "charger_class": category, "source_year": source_year, "charger_type": kind,
            "count_share": share, "rated_power_kw": kw,
            "cost_source_period": str(cost.period), "power_source_period": str(power.period),
            "source_cost_per_port": value, "source_unit": cost.unit,
            "source_dollar_year": int(cost.currency_year),
            "cad_per_usd": converted.cad_per_usd,
            "source_year_deflator": converted.source_year_deflator,
            "target_year_deflator": converted.target_year_deflator,
            "target_dollar_year": converted.target_dollar_year,
            "cad_2020_cost_per_port": converted.target_cad_value,
            "weighted_cad_2020_cost_per_port": share * converted.target_cad_value,
            "weighted_rated_power_kw": share * kw,
            "cad_2020_cost_per_gw": converted.target_cad_value / kw,
            "aggregation_basis": "total_expenditure_per_rated_power",
        })
    if not math.isclose(sum(row["count_share"] for row in records), 1, abs_tol=1e-9):
        raise ValueError(f"Charger {category} cost count shares do not sum to one")
    mean_power = sum(row["weighted_rated_power_kw"] for row in records)
    mean_expense = sum(row["weighted_cad_2020_cost_per_port"] for row in records)
    total = mean_expense / mean_power
    if not math.isfinite(total) or total <= 0:
        raise ValueError(f"Invalid aggregated charger cost: {category}/{source_year}")
    for row in records:
        row["power_share"] = row["weighted_rated_power_kw"] / mean_power
        row["weighted_cad_2020_cost_per_gw"] = row["power_share"] * row["cad_2020_cost_per_gw"]
        row["mean_rated_power_kw"] = mean_power
        row["mean_cad_2020_cost_per_port"] = mean_expense
    if not math.isclose(sum(row["weighted_cad_2020_cost_per_gw"] for row in records), total,
                        rel_tol=1e-12, abs_tol=1e-9):
        raise ValueError(f"Charger expenditure/power reconciliation failed: {category}/{source_year}")
    return total, records


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
        transformation="reviewed transport charging and refueling parameterization",
        transformation_version=load_harmonization_rules(bundle, "ev_chargers")["transformation_version"], governing_source_id=governing_id,
        data_quality=quality, value_variant={"input_digest": digest, "periods": bundle.scenario.periods.model_dump(),
                                           "charger_rules": load_harmonization_rules(bundle, "ev_chargers")},
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
            [f"{kind}_capex_aggregation" for kind in types]
        )
        values = [_manual_value(manual, category, name, period) for name in names]
        if any(value > 1 for value in values) or not math.isclose(sum(values), 1, abs_tol=1e-9):
            raise ValueError(f"Charger {category}/{usage} shares do not sum to one")


def prepare_charger_utilization_targets(
    bundle: ConfigBundle, *, manual: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, ResolvedProvenance]:
    """Select reviewed utilization by technology kind for profiles and annual limits.

    The source's legacy ``max_annual_utilization`` name is preserved. Its reviewed
    average utilization is the profile mean. Source-period choices apply to every
    vintage; configured slack applies only to annual lower bounds, not equalities.
    """
    rules = load_harmonization_rules(bundle, "ev_chargers")
    if rules["utilization_time_basis"] != "technology_kind":
        raise ValueError("Charger utilization requires technology-kind source selection")
    policies = rules["utilization"]
    if set(policies) != set(rules["technology"]):
        raise ValueError("Charger utilization policies must cover each technology category")
    for category, policy in policies.items():
        for annual_policy in (policy, policy.get("without_profile", policy)):
            multiplier = float(annual_policy["floor_multiplier"])
            if not math.isfinite(multiplier) or not 0 < multiplier <= 1:
                raise ValueError("Charger utilization floor multiplier must be in (0, 1]")
            if annual_policy["operator"] not in {"ge", "e"}:
                raise ValueError(f"Unsupported charger annual utilization operator: {category}")
            if annual_policy["operator"] == "e" and multiplier != 1:
                raise ValueError("Charger utilization equality requires multiplier one")
        if set(policy["source_period"]) != {"existing", "new"}:
            raise ValueError(f"Charger utilization source selectors must cover existing/new: {category}")
    profiles_enabled = bundle.scenario.charging_profiles.travel_behavior_source != "none"
    if not profiles_enabled:
        policies = {
            category: {**policy, **policy.get("without_profile", {})}
            for category, policy in policies.items()
        }
    if manual is None:
        manual_rules = load_harmonization_rules(bundle, "manual_parameters")
        _, frames = validate_manual_registry(
            bundle, source_column=manual_rules["source_column"],
            notes_column=manual_rules["notes_column"],
            selected_files={rules["manual_file"]},
        )
        manual = frames[rules["manual_file"]]
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["region_output_map"]
    regions = sorted(region_map.get(r, r) for r in bundle.scenario.geography.regions)
    records = []
    for category, mapping in rules["technology"].items():
        policy = policies[category]
        multiplier = float(policy["floor_multiplier"])
        for kind, vintages in (("existing", [bundle.scenario.periods.latest_observed_vintage]),
                               ("new", bundle.scenario.periods.model)):
            source_period = str(policy["source_period"][kind])
            target = _manual_value(manual, category, "max_annual_utilization", source_period)
            if not 0 < target < 1:
                raise ValueError(f"Invalid charger utilization: {category}/{kind}")
            for vintage in vintages:
                records.extend({
                    "region": region, "vintage": vintage, "source_period": source_period,
                    "tech_or_group": mapping[kind], "output_comm": mapping["output"],
                    "operator": policy["operator"],
                    "target_utilization": target, "floor_multiplier": multiplier,
                    "factor": target * multiplier, "units": "fraction",
                } for region in regions)
    targets = pd.DataFrame(records)
    if targets.duplicated(["region", "tech_or_group", "vintage"]).any():
        raise ValueError("Duplicate charger utilization target keys")
    digest = hashlib.sha256(json.dumps({
        "manual_sha256": file_sha256(resolve_input_path(bundle, "manual", rules["manual_file"])),
        "selected_policies": policies,
    }, sort_keys=True).encode()).hexdigest()
    context = _context(
        bundle, [("dunsky_ev_charging_infrastructure", "charger_shares_and_utilization")],
        name="utilization", digest=digest,
    )
    LOGGER.info("Charger utilization annual limits use %s; LDV hourly profiles enabled=%s",
                {category: policy["operator"] for category, policy in policies.items()}, profiles_enabled)
    return targets, context


def _prepare_hydrogen_refueling(
    bundle: ConfigBundle, *, manual: pd.DataFrame, rules: dict,
    technology: pd.DataFrame, commodities: set[str], regions: set[str],
    converter: CerCurrencyConverter, fixed_lives: dict[tuple[str, str], float],
    digest: str,
) -> tuple[list[CostInvest], list[CostFixed], list[Efficiency], list[ResolvedProvenance], dict]:
    """Expand the reviewed station rates; capacity is annual hydrogen throughput."""
    hydrogen = rules["hydrogen_refueling"]
    if (hydrogen["capacity_units"] != "PJ/year"
            or hydrogen["cost_units"] != "$M 2020CAD / (PJ/year)"
            or hydrogen["fixed_cost_units"] != "$M 2020CAD / (PJ/year) / year"):
        raise ValueError("H2 refueling requires the reviewed PJ/year capacity and CAD2020 cost basis")
    mappings = hydrogen["technology"]
    declared = technology.loc[technology.category.eq("h2_refuel")]
    if set(declared.index) != {mapping["tech"] for mapping in mappings.values()}:
        raise ValueError("H2 refueling mappings do not cover template technologies")
    if hydrogen["input_commodity"] not in commodities:
        raise ValueError("Unknown H2 refueling input commodity")
    efficiency = _manual_value(
        manual, "all", "efficiency", "all", infrastructure_category="h2_refuel",
    )
    if not 0 < efficiency <= 1:
        raise ValueError("H2 refueling efficiency must be in (0, 1]")
    efficiency_context = _context(
        bundle, [("iea_future_of_hydrogen", "hydrogen_refueling_efficiency")],
        name="hydrogen_refueling.efficiency", digest=digest,
    )
    costs = [("open_energy_outlook", "hydrogen_refueling_costs"),
             ("cer_canadas_energy_future", "macro-indicators")]
    invest_context = _context(bundle, costs, name="hydrogen_refueling.cost_invest", digest=digest)
    fixed_context = _context(
        bundle, [*costs, ("argonne_hdsam", "hydrogen_dispenser_lifetimes")],
        name="hydrogen_refueling.cost_fixed", digest=digest,
    )
    invest_rows, fixed_rows, efficiency_rows, conversions = [], [], [], []
    excluded_pairs = 0
    for category, mapping in mappings.items():
        tech = mapping["tech"]
        if technology.loc[tech, "sub_category"] != category or mapping["output"] not in commodities:
            raise ValueError(f"H2 refueling template mapping changed: {category}")
        if str(technology.loc[tech, "annual"]) != "1":
            raise ValueError(f"H2 refueling technology must remain annual: {tech}")
        for vintage in bundle.scenario.periods.model:
            rates = {}
            for parameter in ("capex_per_PJ_year", "fixed_cost_per_PJ_year"):
                selected = _period_value(
                    manual, category, parameter, vintage, infrastructure_category="h2_refuel",
                )
                if selected.unit not in {"M CAD", "M USD"} or not str(selected.currency_year).isdigit():
                    raise ValueError(f"H2 refueling cost currency contract changed: {category}/{parameter}")
                converted = converter.convert(
                    float(selected.value), currency=selected.unit.split()[1],
                    dollar_year=int(selected.currency_year),
                )
                rates[parameter] = converted.target_cad_value
                conversions.append({"station_class": category, "vintage": vintage,
                                    "parameter": parameter, **vars(converted)})
            for region in sorted(regions):
                base = {"region": region, "tech": tech, "vintage": vintage}
                invest_rows.extend(validate_parameter_rows(CostInvest, [{
                    **base, "cost": rates["capex_per_PJ_year"], "units": hydrogen["cost_units"],
                }], invest_context, bundle=bundle))
                candidates = [period for period in bundle.scenario.periods.model if period >= vintage]
                periods = [period for period in candidates if period < vintage + fixed_lives[region, tech]]
                excluded_pairs += len(candidates) - len(periods)
                fixed_rows.extend(validate_parameter_rows(CostFixed, [{
                    **base, "period": period, "cost": rates["fixed_cost_per_PJ_year"],
                    "units": hydrogen["fixed_cost_units"],
                } for period in periods], fixed_context, bundle=bundle))
                efficiency_rows.extend(validate_parameter_rows(Efficiency, [{
                    **base, "input_comm": hydrogen["input_commodity"],
                    "output_comm": mapping["output"], "efficiency": efficiency, "units": "PJ/PJ",
                }], efficiency_context, bundle=bundle))
    audit = {
        "capacity_units": hydrogen["capacity_units"], "capacity_to_activity": 1.0,
        "capacity_basis": "effective annual hydrogen throughput; model default capacity_to_activity=1",
        "existing_capacity_rows": 0,
        "invest_rows": len(invest_rows), "fixed_rows": len(fixed_rows),
        "efficiency_rows": len(efficiency_rows), "cost_conversion": conversions,
        "fixed_period_vintage_pairs_excluded_by_lifetime": excluded_pairs,
    }
    return invest_rows, fixed_rows, efficiency_rows, [invest_context, fixed_context, efficiency_context], audit


def prepare_ev_charger_rows(
    bundle: ConfigBundle, *, existing_capacity_rows: list[ExistingCapacity],
    existing_capacity_contexts: list[ResolvedProvenance],
    physical_stock: pd.DataFrame | None = None,
    fixed_lifetime_rows: list[LifetimeTech] | None = None,
) -> ChargerPreparation:
    """Reuse prepared stock/lifetimes and return charger and H2 station products."""
    from parameterization.build_existing_capacity import physical_stock_for_consumers

    physical_stock = physical_stock_for_consumers(bundle, existing_capacity_rows, physical_stock)
    rules = load_harmonization_rules(bundle, "ev_chargers")
    electricity_accounting = _charger_electricity_accounting(rules)
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
    charger_as_of = bundle.sources.sources[dashboard["source_id"]].component(
        charger_source["component_id"]
    ).adapter.get("reporting_date", charger_source["as_of"])
    total_precedence = charger_source.get("national_total_precedence", "require_equal")
    province_path = source_dir / charger_source["province_output_file"]
    type_path = source_dir / charger_source["type_output_file"]
    manual_path = resolve_input_path(bundle, "manual", rules["manual_file"])
    provincial = pd.read_csv(province_path)
    types = pd.read_csv(type_path)
    if (
        len(provincial) != 13 or set(provincial.source_region) != set(charger_source["source_regions"])
        or provincial.source_region.duplicated().any()
        or set(types.charger_type) != {"L2", "DCFC"}
        or types.charger_type.duplicated().any()
        or set(provincial.units) != {"ports"} or set(types.units) != {"ports"}
        or set(provincial.as_of) != {charger_as_of}
        or set(types.as_of) != {charger_as_of}
        or set(provincial.source_year) != {bundle.scenario.sources.selections[dashboard["source_id"]].year}
        or set(types.source_year) != set(provincial.source_year)
    ):
        raise ValueError("Normalized TC public charger coverage or edition changed")
    national, chart_total = _public_charger_totals(
        provincial, types, precedence=total_precedence,
    )
    region_map = rules["public_charger_region_map"]
    if set(sum((list(labels) for labels in region_map.values()), [])) != set(provincial.source_region):
        raise ValueError("TC provinces/territories do not map to template regions")
    template_regions = set(pd.read_csv(resolve_input_path(bundle, "template", "region.csv")).region)
    if set(region_map) != template_regions:
        raise ValueError("Charger region mapping does not cover the template")
    output_region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["region_output_map"]
    model_regions = {output_region_map.get(region, region) for region in bundle.scenario.geography.regions}
    if model_regions - template_regions:
        raise ValueError(f"Charger scenario regions are absent from template: {sorted(model_regions - template_regions)}")
    if fixed_lifetime_rows is None:
        from parameterization.offroad_lifetimes import prepare_reviewed_manual_lifetimes

        fixed_lifetime_rows, _, _ = prepare_reviewed_manual_lifetimes(bundle)
    infrastructure_techs = set(template.loc[template.category.isin(["charger", "h2_refuel"]), "tech"])
    fixed_lives = {}
    for row in fixed_lifetime_rows:
        key = (row.region, row.tech)
        if row.region not in model_regions or row.tech not in infrastructure_techs:
            continue
        if key in fixed_lives or not math.isfinite(row.lifetime) or row.lifetime <= 0:
            raise ValueError(f"Invalid or duplicate infrastructure lifetime: {key}")
        fixed_lives[key] = float(row.lifetime)
    missing_lives = {(region, tech) for region in model_regions for tech in infrastructure_techs} - set(fixed_lives)
    if missing_lives:
        raise ValueError(f"Infrastructure technology has no fixed lifetime: {sorted(missing_lives)}")
    assumption_dataset, c2a_rows = prepare_charger_capacity_to_activity(
        bundle, technology=template, regions=model_regions,
    )
    LOGGER.debug("Preparing charger parameters for selected regions: %s", sorted(model_regions))
    public_by_region = {
        region: int(provincial.loc[provincial.source_region.isin(labels), "public_chargers"].sum())
        for region, labels in region_map.items()
    }
    if sum(public_by_region.values()) != national:
        raise ValueError("TC public chargers were lost during regional harmonization")

    stock_class_map = rules["stock_classes"]
    stock_data_ids: dict[tuple[str, str], set[str]] = {}
    stock_by_region: dict[tuple[str, str], float] = {}
    consumed_stock = []
    excluded_rows = int((~physical_stock.region.isin(model_regions)).sum())
    if excluded_rows:
        LOGGER.info("Skipped %d vehicle capacity rows outside selected charger regions", excluded_rows)
    for row in physical_stock.itertuples(index=False):
        if row.region not in model_regions:
            continue
        if row.tech not in technology.index:
            raise ValueError(f"Vehicle capacity technology is absent from template: {row.tech}")
        item = technology.loc[row.tech]
        matched = [category for category, classes in stock_class_map.items()
                   if item["category"] in classes and item["sub_category"].startswith("bev")]
        if not matched:
            continue
        if row.units != "k vehicles" or not math.isfinite(row.surviving_k_vehicles) or row.surviving_k_vehicles < 0:
            raise ValueError(f"BEV stock has invalid units or capacity: {row.tech}")
        consumed_stock.append(row._asdict())
        key = (row.region, matched[0])
        stock_by_region[key] = stock_by_region.get(key, 0.0) + row.surviving_k_vehicles * 1000
        stock_data_ids.setdefault(key, set()).add(row.data_id)
    expected_stock = {(region, category) for region in model_regions for category in stock_class_map}
    missing_stock = sorted(expected_stock - set(stock_by_region))
    for key in missing_stock:
        stock_by_region[key] = 0.0
    if missing_stock:
        LOGGER.info("Zero BEV stock, omitting historical charger rows: %s", missing_stock)
    stock_identity, vehicle_contexts = _prepared_stock_identity(
        pd.DataFrame(consumed_stock, columns=physical_stock.columns), existing_capacity_contexts,
    )
    paths = [manual_path, province_path, type_path]
    digest = hashlib.sha256(json.dumps(
        {"files": [(path.name, file_sha256(path)) for path in paths],
         "prepared_stock": stock_identity,
         "scenario": bundle.scenario.ev_chargers.model_dump(),
         "tc_total_precedence": total_precedence,
         "rules": rules, "stock": sorted((key, value) for key, value in stock_by_region.items()),
         "fixed_lifetimes": sorted((key, value) for key, value in fixed_lives.items())},
        sort_keys=True,
    ).encode()).hexdigest()
    dunsky = ("dunsky_ev_charging_infrastructure", "charger_shares_and_utilization")
    tc = ("transport_canada_ev_dashboard", "public_light_duty_chargers")
    probe = ("pollution_probe_ev_charging_survey", "private_charger_shares")
    capacity_contexts = []
    port_records = []
    capacity_rows = []
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
            # Existing ratings use the same earliest model-year proxy as costs.
            power_source_year = min(bundle.scenario.periods.model)
            power_rows = {kind: _rated_power(manual, category, kind, power_source_year)
                          for kind in port_counts}
            power_kw = {kind: float(row.value) for kind, row in power_rows.items()}
            gw = sum(port_counts[kind] * power_kw[kind] for kind in port_counts) / 1_000_000
            if not math.isfinite(gw) or gw < 0:
                raise ValueError(f"Invalid charger GW capacity: {region}/{category}")
            port_records.append({
                "region": region, "charger_class": category, "bev_vehicles": evs,
                "ev_per_port": ratio, "required_ports": required_ports,
                "observed_public_ports": public, "derived_private_ports": private,
                **{f"{kind}_ports": count for kind, count in port_counts.items()},
                **{f"{kind}_rated_power_kw": kw for kind, kw in power_kw.items()},
                "rated_power_source_year": power_source_year,
                **{f"{kind}_rated_power_source_period": str(row.period) for kind, row in power_rows.items()},
                "capacity_gw": gw, "bev_stock_year": bundle.scenario.periods.base_year,
                "public_charger_as_of": charger_as_of,
            })
            if gw > 0:
                context = _context(
                    bundle, [tc, dunsky, probe] if category == "ldv" else [dunsky],
                    name=f"existing_capacity.{region}.{category}", digest=digest,
                    additional=[vehicle_contexts[key] for key in sorted(
                        stock_data_ids[region, category]
                    )],
                )
                capacity_contexts.append(context)
                capacity_rows.extend(validate_parameter_rows(ExistingCapacity, [{
                    "region": region, "tech": rules["technology"][category]["existing"],
                    "vintage": bundle.scenario.periods.latest_observed_vintage, "capacity": gw,
                    "units": "GW",
                }], context, bundle=bundle))

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
    nlr = ("nlr_alternative_fueling_infrastructure", "residential_l1_cost")
    oeo = ("open_energy_outlook", "charger_fixed_cost_ratio")
    cer_macro = ("cer_canadas_energy_future", "macro-indicators")
    invest_contexts = {
        "ldv": _context(bundle, [dunsky, nlr, cer_macro], name="ldv_cost_invest", digest=cost_digest),
        "mhdv": _context(bundle, [dunsky, cer_macro], name="mhdv_cost_invest", digest=cost_digest),
    }
    fixed_contexts = {
        category: _context(bundle, [dunsky, *([nlr] if category == "ldv" else []), oeo, cer_macro,
                                    ("open_energy_outlook", "transport_process_lifetimes")],
                           name=f"{category}_cost_fixed", digest=cost_digest)
        for category in stock_class_map
    }
    if (bundle.scenario.economics.cost_reference_currency != "CAD"
            or bundle.scenario.economics.cost_reference_year != 2020):
        raise ValueError("Charger cost units require the reviewed 2020 CAD scenario")
    invest_rows, fixed_rows, cost_audit = [], [], []
    excluded_fixed_pairs = 0
    fixed_ratio = _manual_value(manual, "all", "fixed_to_capex_ratio", "all")
    if not 0 <= fixed_ratio <= 1:
        raise ValueError("Invalid reviewed charger fixed cost ratio")
    for category in stock_class_map:
        existing_vintage = bundle.scenario.periods.latest_observed_vintage
        for vintage in [existing_vintage, *bundle.scenario.periods.model]:
            source_year = (
                bundle.scenario.periods.model[0] if vintage == existing_vintage
                else bundle.scenario.periods.projection_year(vintage, legacy_at_end=False)
            )
            weighted, components = charger_cost_components(manual, category, source_year, converter)
            cost_audit.extend({"vintage": vintage, **record} for record in components)
            for region in sorted(model_regions):
                if vintage == existing_vintage and stock_by_region[region, category] == 0:
                    continue
                tech = rules["technology"][category]["existing" if vintage == existing_vintage else "new"]
                base_record = {
                    "region": region, "tech": tech, "vintage": vintage,
                    "units": "$M 2020CAD / GW",
                }
                if vintage != existing_vintage:
                    invest_rows.extend(validate_parameter_rows(CostInvest, [
                        {**base_record, "cost": weighted}
                    ], invest_contexts[category], bundle=bundle))
                candidates = [period for period in bundle.scenario.periods.model if period >= vintage]
                periods = [period for period in candidates if period < vintage + fixed_lives[region, tech]]
                excluded_fixed_pairs += len(candidates) - len(periods)
                fixed_rows.extend(validate_parameter_rows(CostFixed, [
                    {**base_record, "period": period, "cost": weighted * fixed_ratio}
                    for period in periods
                ], fixed_contexts[category], bundle=bundle))

    efficiency_rows = []
    unapplied_efficiencies = []
    utilization_targets, utilization_context = prepare_charger_utilization_targets(bundle, manual=manual)
    utilization_records = []
    for category in ("ldv", "mhdv"):
        reviewed_efficiency = _manual_value(manual, category, "efficiency", "all")
        if not 0 < reviewed_efficiency <= 1:
            raise ValueError(f"Invalid charger efficiency: {category}")
        source_row = manual.loc[
            manual.category.eq("charger") & manual.sub_category.eq(category)
            & manual.parameter.eq("efficiency") & manual.period.eq("all")
        ].iloc[0]
        unapplied_efficiencies.append({
            "charger_class": category, "reviewed_efficiency": reviewed_efficiency,
            "source": source_row[manual_rules["source_column"]],
        })
        mapping = rules["technology"][category]
        for region in sorted(model_regions):
            for kind, vintages in (("existing", [bundle.scenario.periods.latest_observed_vintage]),
                                   ("new", bundle.scenario.periods.model)):
                if kind == "existing" and stock_by_region[region, category] == 0:
                    continue
                tech = mapping[kind]
                efficiency_rows.extend(
                    Efficiency.model_validate({
                        "region": region, "tech": tech, "vintage": vintage,
                        "input_comm": rules["input_commodity"], "output_comm": mapping["output"],
                        "efficiency": electricity_accounting["charger_transfer_efficiency"],
                        "units": "PJ/PJ", "data_id": assumption_dataset.data_id,
                        "notes": "Unity accounting transfer; charging losses are already included in vehicle electricity consumption.",
                    })
                    for vintage in vintages
                )
                for vintage in vintages:
                    selected = utilization_targets.loc[
                        utilization_targets.region.eq(region) & utilization_targets.tech_or_group.eq(tech)
                        & utilization_targets.vintage.eq(vintage)
                    ]
                    if len(selected) != 1:
                        raise ValueError(f"Missing charger utilization target: {region}/{tech}/{vintage}")
                    utilization_records.append(selected.iloc[0].to_dict())
    utilization_rows = validate_parameter_rows(
        LimitAnnualCapacityFactor,
        [{key: value for key, value in record.items()
          if key not in {"source_period", "units", "target_utilization", "floor_multiplier"}}
         for record in utilization_records], utilization_context, bundle=bundle,
    )
    utilization_records = [
        {**row.model_dump(), **{key: record[key] for key in (
            "source_period", "units", "target_utilization", "floor_multiplier",
        )}}
        for row, record in zip(utilization_rows, utilization_records, strict=True)
    ]
    utilization = pd.DataFrame(utilization_records).sort_values(
        ["region", "vintage", "tech_or_group"], kind="stable"
    ).reset_index(drop=True)
    if utilization.duplicated(["region", "vintage", "tech_or_group", "output_comm", "operator"]).any():
        raise ValueError("Duplicate charger utilization keys")

    h2_invest, h2_fixed, h2_efficiency, h2_contexts, h2_audit = _prepare_hydrogen_refueling(
        bundle, manual=manual, rules=rules, technology=technology,
        commodities=set(commodity.name), regions=model_regions, converter=converter,
        fixed_lives=fixed_lives, digest=cost_digest,
    )
    invest_rows.extend(h2_invest)
    fixed_rows.extend(h2_fixed)
    efficiency_rows.extend(h2_efficiency)
    contexts = [*capacity_contexts, *invest_contexts.values(), *fixed_contexts.values(),
                utilization_context, *h2_contexts]
    output_dir = resolve_artifact_path(bundle, "ev_chargers_processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    frames_to_write = {
        rules["capacity_to_activity_output_file"]: pd.DataFrame([row.model_dump() for row in c2a_rows]),
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
        "capacity_to_activity_rows": len(c2a_rows),
        "capacity_to_activity": {
            "value": c2a_rows[0].c2a, "units": rules["capacity_to_activity_units"],
            "technologies": sorted({row.tech for row in c2a_rows}),
            "regions": sorted(model_regions),
            "manual_assumption_data_id": assumption_dataset.data_id,
            "input_sha256": file_sha256(resolve_input_path(bundle, "manual", rules["capacity_to_activity_file"])),
        },
        "capacity_rows": len(capacity_rows), "invest_rows": len(invest_rows),
        "fixed_rows": len(fixed_rows), "efficiency_rows": len(efficiency_rows),
        "electricity_accounting": {
            **electricity_accounting,
            "internal_assumption_data_id": assumption_dataset.data_id,
            "reviewed_efficiencies_not_applied": unapplied_efficiencies,
            "reason": "Vehicle electricity consumption includes charging losses; an additional charger loss would count them twice.",
        },
        "existing_investment_rows_excluded": len(capacity_rows),
        "utilization_rows": len(utilization_rows), "utilization_schema_supported": True,
        "utilization_time_basis": rules["utilization_time_basis"],
        "utilization_policy": rules["utilization"],
        "utilization_profile_enabled": bundle.scenario.charging_profiles.travel_behavior_source != "none",
        "utilization_provenance": {
            "data_id": utilization_context.data_id,
            "contributors": [item.source_key for item in utilization_context.contributors],
        },
        "tc_public_ports": national, "tc_type_counts": types.set_index("charger_type").public_chargers.to_dict(),
        "tc_type_total": chart_total,
        "tc_total_reconciliation": {
            "policy": total_precedence, "authoritative_total": national,
            "provincial_total": national, "type_total": chart_total,
            "difference": national - chart_total, "discrepancy": national != chart_total,
            "type_mix_basis": "published type counts divided by their own sum",
        },
        "tc_as_of": charger_as_of, "bev_stock_year": bundle.scenario.periods.base_year,
        "zero_bev_stock_skipped": [list(key) for key in missing_stock],
        "port_reconciliation": port_records, "cost_conversion": cost_audit,
        "fixed_period_vintage_pairs_excluded_by_lifetime": excluded_fixed_pairs,
        "hydrogen_refueling": h2_audit,
        "prepared_stock": stock_identity,
        "input_sha256": {path.name: file_sha256(path) for path in [*paths, macro_path]},
    }
    validation_dir = resolve_artifact_path(bundle, "ev_chargers_validation")
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / rules["validation_file"]).write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    LOGGER.info(
        "EV charger rows: capacity=%d invest=%d fixed=%d efficiency=%d annual_utilization=%d; public_ports=%d",
        len(capacity_rows), len(invest_rows), len(fixed_rows), len(efficiency_rows),
        len(utilization), national,
    )
    LOGGER.info("Excluded %d existing charger investment rows; retained their annual fixed-cost basis", len(capacity_rows))
    LOGGER.info("Normalized charger capex: total converted CAD per representative port / total rated kW = million 2020CAD/GW; existing ratings use source year %d", min(bundle.scenario.periods.model))
    LOGGER.info("Applied unity charger electricity transfer: losses are included in vehicle consumption; retained %d reviewed loss factors as unapplied evidence", len(unapplied_efficiencies))
    LOGGER.info("Excluded fixed-cost pairs at/after retirement: chargers=%d H2=%d",
                excluded_fixed_pairs, h2_audit["fixed_period_vintage_pairs_excluded_by_lifetime"])
    LOGGER.info("H2 refueling rows: invest=%d fixed=%d efficiency=%d; no existing station stock input",
                len(h2_invest), len(h2_fixed), len(h2_efficiency))
    return ChargerPreparation(capacity_rows, invest_rows, fixed_rows, efficiency_rows,
                              utilization_rows, utilization, contexts, audit,
                              assumption_dataset, c2a_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare reviewed EV charger and H2 refueling parameters")
    parser.add_argument("--scenario", default="config/scenarios/default.yaml")
    args = parser.parse_args()
    from parameterization.build_existing_capacity import prepare_existing_capacity_rows

    bundle = load_config_bundle(args.scenario)
    vehicles = prepare_existing_capacity_rows(bundle)
    result = prepare_ev_charger_rows(
        bundle, existing_capacity_rows=vehicles.existing_capacity_rows,
        existing_capacity_contexts=vehicles.provenance_contexts,
        physical_stock=vehicles.physical_stock,
    )
    print(json.dumps({key: value for key, value in result.audit.items()
                      if key.endswith("_rows") or key in {"tc_public_ports", "tc_as_of"}},
                     indent=2))


if __name__ == "__main__":
    main()
