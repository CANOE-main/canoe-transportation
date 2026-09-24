"""Off-road capital and variable cost normalization from reviewed evidence."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd

from parameterization.road_efficiencies import interpolate


@dataclass(frozen=True)
class AircraftServiceEvidence:
    operating_group: str
    source_tables: tuple[str, str]
    block_speed_mph: float
    capacity_native: float
    capacity_unit: str
    load_factor_percent: float
    daily_block_hours: float
    capacity_metric: float
    capacity_metric_unit: str
    mile_to_km: float
    us_short_ton_to_metric_tonne: float | None
    operating_days_per_year: int
    annual_service_output: float
    annual_service_unit: str


def aircraft_service_evidence(
    capacity: pd.DataFrame,
    *,
    operating_group: str,
    mile_to_km: float,
    us_short_ton_to_metric_tonne: float,
    operating_days_per_year: int,
) -> AircraftServiceEvidence:
    """Normalize FAA All Aircraft factors to annual passenger/tonne-km capacity."""
    if operating_group not in {"passenger", "cargo"}:
        raise ValueError(f"Unsupported FAA operating group {operating_group!r}")
    if (
        not isfinite(mile_to_km) or mile_to_km <= 0
        or not isfinite(us_short_ton_to_metric_tonne)
        or us_short_ton_to_metric_tonne <= 0
        or operating_days_per_year <= 0
    ):
        raise ValueError("FAA service-output conversion factors must be positive")
    required = {"operating_group", "source_table", "aircraft_category", "metric", "value", "unit"}
    if not required <= set(capacity):
        raise ValueError(f"FAA capacity columns missing: {sorted(required - set(capacity))}")
    selected = capacity.loc[
        capacity["operating_group"].eq(operating_group)
        & capacity["aircraft_category"].eq("All Aircraft")
    ]
    metrics = {
        "average_block_speed": ("miles per hour", "Table 3-6" if operating_group == "passenger" else "Table 3-9"),
        "average_daily_utilization": ("block hours per aircraft-day", "Table 3-7" if operating_group == "passenger" else "Table 3-10"),
        ("passenger_capacity" if operating_group == "passenger" else "cargo_capacity"): (
            "seats" if operating_group == "passenger" else "source-labelled tons",
            "Table 3-6" if operating_group == "passenger" else "Table 3-9",
        ),
        ("passenger_load_factor" if operating_group == "passenger" else "cargo_load_factor"): (
            "percent", "Table 3-6" if operating_group == "passenger" else "Table 3-9"
        ),
    }
    values: dict[str, float] = {}
    for metric, (unit, table) in metrics.items():
        rows = selected.loc[selected["metric"].eq(metric)]
        if len(rows) != 1 or rows.iloc[0]["unit"] != unit or rows.iloc[0]["source_table"] != table:
            raise ValueError(f"FAA {operating_group} {metric} is missing or ambiguous")
        value = float(rows.iloc[0]["value"])
        if not isfinite(value) or value <= 0:
            raise ValueError(f"FAA {operating_group} {metric} must be positive")
        values[metric] = value
    load = values[f"{operating_group if operating_group == 'passenger' else 'cargo'}_load_factor"]
    if load > 100:
        raise ValueError("FAA aircraft load factor exceeds 100 percent")
    capacity_key = "passenger_capacity" if operating_group == "passenger" else "cargo_capacity"
    capacity_native = values[capacity_key]
    ton_factor = us_short_ton_to_metric_tonne if operating_group == "cargo" else None
    capacity_metric = capacity_native * (ton_factor or 1.0)
    annual = (
        operating_days_per_year
        * values["average_daily_utilization"]
        * values["average_block_speed"]
        * mile_to_km
        * capacity_metric
        * load / 100.0
    )
    return AircraftServiceEvidence(
        operating_group=operating_group,
        source_tables=(metrics["average_block_speed"][1], metrics["average_daily_utilization"][1]),
        block_speed_mph=values["average_block_speed"],
        capacity_native=capacity_native,
        capacity_unit=metrics[capacity_key][0],
        load_factor_percent=load,
        daily_block_hours=values["average_daily_utilization"],
        capacity_metric=capacity_metric,
        capacity_metric_unit="seats" if operating_group == "passenger" else "tonnes",
        mile_to_km=mile_to_km,
        us_short_ton_to_metric_tonne=ton_factor,
        operating_days_per_year=operating_days_per_year,
        annual_service_output=annual,
        annual_service_unit="passenger-km/year" if operating_group == "passenger" else "tonne-km/year",
    )


def aircraft_cost_per_billion_service(
    *,
    cost_per_block_hour: float,
    evidence: AircraftServiceEvidence,
) -> float:
    """FAA maintenance cost per hour to source-currency millions/bn service."""
    if not isfinite(cost_per_block_hour) or cost_per_block_hour < 0:
        raise ValueError("FAA maintenance cost must be finite and non-negative")
    service_per_block_hour = (
        evidence.block_speed_mph
        * evidence.mile_to_km
        * evidence.capacity_metric
        * evidence.load_factor_percent / 100.0
    )
    return cost_per_block_hour / service_per_block_hour * 1000.0


def manual_offroad_capex(
    assumptions: pd.DataFrame, *, mode: str, powertrain: str, year: int,
    period_years: dict[str, int],
) -> dict[str, float | int | str]:
    """Keep CIMS incumbent cost and output paired; apply REGEN relative factors."""
    incumbent = {"passenger_rail": "diesel", "freight_rail": "diesel", "freight_marine": "mdo"}[mode]
    selected = assumptions.loc[assumptions.category.eq(mode)]
    def one(sub: str, parameter: str) -> pd.Series:
        rows = selected.loc[
            selected.sub_category.eq(sub) & selected.parameter.eq(parameter)
        ]
        if len(rows) != 1:
            raise ValueError(f"Missing/ambiguous CIMS {mode}/{sub}/{parameter}")
        return rows.iloc[0]
    capex = one(incumbent, "service_unit_capex")
    output = one(incumbent, "service_output")
    if capex.unit not in {"CAD", "USD"} or pd.isna(capex.currency_year):
        raise ValueError("CIMS off-road CAPEX lacks native currency/year")
    if output.unit != ("k_pkm" if mode == "passenger_rail" else "k_tkm"):
        raise ValueError("CIMS off-road service-output unit changed")
    if powertrain == incumbent:
        factor = 1.0
    else:
        multiplier = selected.loc[
            selected.sub_category.eq(powertrain)
            & selected.parameter.eq(f"{incumbent}_relative_cost_invest")
        ]
        if multiplier.empty or multiplier.period.duplicated().any():
            raise ValueError(f"Missing REGEN CAPEX multiplier for {mode}/{powertrain}")
        if set(multiplier.period) == {"all"}:
            factor = float(multiplier.value.iloc[0])
        elif set(multiplier.period) == set(period_years):
            trajectory = multiplier.assign(
                year=multiplier.period.map(period_years)
            )
            factor = interpolate(
                trajectory, max(year, min(period_years.values())), "value"
            )
        else:
            raise ValueError(f"Incomplete REGEN CAPEX multiplier for {mode}/{powertrain}")
    if not isfinite(factor) or factor <= 0 or capex.value <= 0 or output.value <= 0:
        raise ValueError("Invalid off-road CAPEX baseline/multiplier/service output")
    native_per_unit = float(capex.value) * factor
    return {
        "native_cost_per_vehicle": native_per_unit,
        "source_currency": str(capex.unit),
        "source_dollar_year": int(capex.currency_year),
        "service_output_thousands": float(output.value),
        "source_unit": f"{capex.unit}/vehicle",
        "multiplier": factor,
        # One million CAD per billion km is numerically CAD per thousand km.
        "native_millions_per_billion_service": native_per_unit / float(output.value),
    }


def offroad_opex_from_capex_ratio(
    capex_millions_per_billion_service: float, *, ratio: float
) -> float:
    if not isfinite(capex_millions_per_billion_service) or capex_millions_per_billion_service <= 0:
        raise ValueError("Invalid off-road CAPEX service cost")
    if not isfinite(ratio) or ratio < 0:
        raise ValueError("Invalid OEO CAPEX-to-OPEX ratio")
    return capex_millions_per_billion_service * ratio
