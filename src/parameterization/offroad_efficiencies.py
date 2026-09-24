"""CEUD off-road service efficiencies and reviewed manual improvement factors."""

from __future__ import annotations

import pandas as pd

from parameterization.road_efficiencies import ceud_series, interpolate, positive


def manual_ratio(rows: pd.DataFrame, year: int, *, rules: dict) -> float:
    """Interpret reviewed ratios individually, without changing the manual input."""
    if rows.empty:
        raise ValueError("Missing expected off-road multiplier")
    if rows.period.eq("all").any():
        if len(rows) != 1:
            raise ValueError("Overlapping all-years and period-specific multipliers")
        return positive(rows.iloc[0].value, "manual multiplier")
    selected = rows.copy()
    selected["year"] = selected.period.map(rules["period_upper_year"])
    if selected.year.isna().any() or selected.year.duplicated().any():
        raise ValueError("Unknown or ambiguous manual multiplier period")
    # Endpoint values apply before/after the documented interval; interpolation is explicit.
    bounded_year = max(int(selected.year.min()), min(year, int(selected.year.max())))
    return interpolate(selected, bounded_year, "value")


def derive_offroad_efficiency(
    national: pd.DataFrame,
    manual: pd.DataFrame,
    *,
    mode: str,
    powertrain: str,
    year: int,
    base_year: int,
    selectors: dict,
    rules: dict,
) -> dict:
    spec = selectors[mode]
    native_unit = "MJ/Pkm" if spec["units"] == "bn passenger-km" else "MJ/Tkm"
    history = ceud_series(
        national, spec["national_table"], spec["intensity_series"], native_unit
    )
    source_year = min(year, base_year)
    intensity = interpolate(history, source_year, "value")
    rows = manual.loc[manual.category.eq(mode)]
    incumbent = rules["incumbent"][mode]
    ratio, interpretation = 1.0, "incumbent"
    used = []
    if (
        powertrain != incumbent
        and powertrain not in rules["equal_incumbent_powertrains"]
    ):
        relative = rows.loc[
            rows.sub_category.eq(powertrain)
            & rows.parameter.ne("annual_improvement_rate")
        ]
        ratio = manual_ratio(relative, year, rules=rules)
        parameters = set(relative.parameter)
        if len(parameters) != 1:
            raise ValueError(f"Ambiguous relative multiplier: {mode}/{powertrain}")
        interpretation = rules["ratio_interpretation"][mode][parameters.pop()]
        used.extend(relative.index.tolist())
    growth = 1.0
    rate = 0.0
    if year > base_year:
        rates = rows.loc[rows.parameter.eq("annual_improvement_rate")]
        specific = rates.loc[rates.sub_category.eq(powertrain)]
        if specific.empty:
            specific = rates.loc[rates.sub_category.isin(["all", "remainder"])]
        if len(specific) != 1:
            raise ValueError(
                f"Missing or ambiguous improvement rate: {mode}/{powertrain}"
            )
        rate = float(specific.iloc[0].value)
        if not 0 <= rate < 1:
            raise ValueError("Invalid annual efficiency improvement rate")
        growth = (1 + rate) ** (year - base_year)
        used.extend(specific.index.tolist())
    if interpretation == "consumption_ratio":
        factor = 1 / ratio
    elif interpretation in {"efficiency_ratio", "incumbent"}:
        factor = ratio
    else:
        raise ValueError(f"Unknown multiplier interpretation: {interpretation}")
    return {
        "efficiency": growth * factor / intensity,
        "native_intensity": intensity,
        "native_unit": native_unit,
        "ceud_source_year": source_year,
        "relative_multiplier": ratio,
        "multiplier_interpretation": interpretation,
        "annual_improvement_rate": rate,
        "development_factor": growth,
        "manual_rows": "|".join(map(str, sorted(set(used)))),
    }
