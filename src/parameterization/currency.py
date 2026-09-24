"""Shared CER-backed currency and dollar-year harmonization for transport costs."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd


@dataclass(frozen=True)
class CurrencyConversion:
    source_value: float
    source_currency: str
    source_dollar_year: int
    cad_per_usd: float
    source_year_deflator: float
    target_year_deflator: float
    cad_value: float
    target_cad_value: float
    target_dollar_year: int
    cer_scenario: str


class CerCurrencyConverter:
    """Convert source-labelled money through same-year FX and CAD GDP deflators."""

    def __init__(
        self,
        macro: pd.DataFrame,
        *,
        scenario: str,
        target_year: int,
    ) -> None:
        required = {"scenario", "region", "variable_key", "year", "value", "unit"}
        if not required <= set(macro):
            raise ValueError(f"CER macro columns missing: {sorted(required - set(macro))}")
        selected = macro.loc[
            macro["scenario"].eq(scenario)
            & macro["region"].eq("Canada")
            & macro["variable_key"].isin(("cad_per_usd", "gdp_deflator"))
        ].copy()
        if selected.empty or selected.duplicated(["variable_key", "year"]).any():
            raise ValueError("CER macro currency series are missing or ambiguous")
        expected_units = {"cad_per_usd": "C$/US$"}
        for key, unit in expected_units.items():
            if set(selected.loc[selected.variable_key.eq(key), "unit"]) != {unit}:
                raise ValueError(f"CER {key} has unexpected units")
        deflator_units = set(selected.loc[selected.variable_key.eq("gdp_deflator"), "unit"])
        if len(deflator_units) != 1 or not next(iter(deflator_units)).endswith("=100"):
            raise ValueError("CER GDP deflator has unexpected index units")
        self.series = {
            key: {int(row.year): float(row.value) for row in group.itertuples()}
            for key, group in selected.groupby("variable_key")
        }
        if set(self.series) != {"cad_per_usd", "gdp_deflator"}:
            raise ValueError("CER macro exchange and deflator series are required")
        if set(self.series["cad_per_usd"]) != set(self.series["gdp_deflator"]):
            raise ValueError("CER macro exchange and deflator year coverage differs")
        if target_year not in self.series["gdp_deflator"]:
            raise ValueError(f"CER GDP deflator lacks target year {target_year}")
        if any(
            not isfinite(value) or value <= 0
            for series in self.series.values()
            for value in series.values()
        ):
            raise ValueError("CER macro conversion values must be finite and positive")
        self.scenario = scenario
        self.target_year = target_year

    def convert(
        self, value: float, *, currency: str, dollar_year: int
    ) -> CurrencyConversion:
        """Return auditable native, same-year CAD, and target-year CAD values."""
        if not isfinite(value) or value < 0:
            raise ValueError("Source cost must be finite and non-negative")
        if currency not in {"CAD", "USD"}:
            raise ValueError(f"Unsupported or unresolved source currency {currency!r}")
        if dollar_year not in self.series["gdp_deflator"]:
            raise ValueError(f"CER macro indicators lack source dollar year {dollar_year}")
        fx = self.series["cad_per_usd"][dollar_year] if currency == "USD" else 1.0
        source_deflator = self.series["gdp_deflator"][dollar_year]
        target_deflator = self.series["gdp_deflator"][self.target_year]
        cad_value = value * fx
        target_cad_value = cad_value * target_deflator / source_deflator
        if not isfinite(target_cad_value):
            raise ValueError("Non-finite currency-year conversion result")
        return CurrencyConversion(
            source_value=value,
            source_currency=currency,
            source_dollar_year=dollar_year,
            cad_per_usd=fx,
            source_year_deflator=source_deflator,
            target_year_deflator=target_deflator,
            cad_value=cad_value,
            target_cad_value=target_cad_value,
            target_dollar_year=self.target_year,
            cer_scenario=self.scenario,
        )
