"""CEUD off-road energy consumption and reviewed manual improvement factors."""

from __future__ import annotations

import pandas as pd

from parameterization.road_efficiencies import ceud_series, positive, require_observed_value, source_point


def _manual_source_rows(rows: pd.DataFrame, year: int, *, rules: dict) -> pd.DataFrame:
    if rules["period_method"] != "previous_endpoint_clamped":
        raise ValueError("Unsupported manual consumption source-point selection")
    if rows.empty:
        raise ValueError("Missing expected off-road multiplier")
    if rows.period.eq("all").any():
        if len(rows) != 1:
            raise ValueError("Overlapping all-years and period-specific multipliers")
        return rows
    selected = rows.assign(year=rows.period.map(rules["period_upper_year"]))
    if selected.year.isna().any() or selected.year.duplicated().any():
        raise ValueError("Unknown or ambiguous manual multiplier period")
    _, source_year = source_point(selected, year, "value")
    return selected.loc[selected.year.eq(source_year)]


def manual_ratio(
    rows: pd.DataFrame, year: int, *, rules: dict,
    interpretation: str = "consumption_ratio",
) -> float:
    """Convert the selected reviewed endpoint to a consumption ratio."""
    if interpretation not in {"consumption_ratio", "efficiency_ratio"}:
        raise ValueError(f"Unknown multiplier interpretation: {interpretation}")
    selected = _manual_source_rows(rows, year, rules=rules)
    ratio = positive(selected.iloc[0].value, "manual multiplier")
    if interpretation == "efficiency_ratio":
        ratio = 1 / ratio
    return ratio


def derive_offroad_consumption(
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
    intensity = require_observed_value(history, source_year, "value")
    rows = manual.loc[manual.category.eq(mode)]
    incumbent = rules["incumbent"][mode]
    ratio, interpretation = 1.0, "incumbent"
    manual_source_year = None
    used = []
    if (
        powertrain != incumbent
        and powertrain not in rules["equal_incumbent_powertrains"]
    ):
        relative = rows.loc[
            rows.sub_category.eq(powertrain)
            & rows.parameter.ne("annual_improvement_rate")
        ]
        parameters = set(relative.parameter)
        if len(parameters) != 1:
            raise ValueError(f"Ambiguous relative multiplier: {mode}/{powertrain}")
        interpretation = rules["ratio_interpretation"][mode][parameters.pop()]
        relative = _manual_source_rows(relative, year, rules=rules)
        ratio = manual_ratio(relative, year, rules=rules, interpretation=interpretation)
        if "year" in relative:
            manual_source_year = int(relative.iloc[0].year)
        used.extend(relative.index.tolist())
    development = 1.0
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
        development = (1 + rate) ** -(year - base_year)
        used.extend(specific.index.tolist())
    return {
        "native_intensity": positive(intensity * ratio * development, "off-road energy consumption"),
        "native_unit": native_unit,
        "load_factor": 1.0,
        "ceud_native_intensity": intensity,
        "ceud_source_year": source_year,
        "relative_consumption_multiplier": ratio,
        "source_multiplier_interpretation": interpretation,
        "manual_source_year": manual_source_year,
        "annual_improvement_rate": rate,
        "consumption_development_factor": development,
        "manual_rows": "|".join(map(str, sorted(set(used)))),
    }
