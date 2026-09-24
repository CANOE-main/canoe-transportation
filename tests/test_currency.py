"""Focused tests for the cost families' shared currency-year seam."""

import pandas as pd
import pytest

from parameterization.currency import CerCurrencyConverter


def macro() -> pd.DataFrame:
    return pd.DataFrame(
        [
            (scenario, "Canada", key, year, value, unit)
            for scenario, rates in (
                ("Current Measures", (1.3, 1.2, 1.4)),
                ("Higher Scenario", (1.1, 1.0, 1.2)),
            )
            for key, values, unit in (
                ("cad_per_usd", rates, "C$/US$"),
                ("gdp_deflator", (80.0, 100.0, 120.0), "2017=100"),
            )
            for year, value in zip((2015, 2020, 2022), values, strict=True)
        ],
        columns=["scenario", "region", "variable_key", "year", "value", "unit"],
    )


def test_usd_and_cad_use_one_auditable_conversion_path() -> None:
    converter = CerCurrencyConverter(macro(), scenario="Current Measures", target_year=2020)
    usd = converter.convert(100.0, currency="USD", dollar_year=2022)
    cad = converter.convert(100.0, currency="CAD", dollar_year=2015)
    assert usd.cad_value == pytest.approx(140.0)
    assert usd.target_cad_value == pytest.approx(140.0 * 100 / 120)
    assert (usd.source_currency, usd.source_dollar_year, usd.target_dollar_year) == (
        "USD", 2022, 2020
    )
    assert cad.cad_per_usd == 1.0
    assert cad.target_cad_value == pytest.approx(125.0)
    assert CerCurrencyConverter(macro(), scenario="Higher Scenario", target_year=2020).convert(
        100.0, currency="USD", dollar_year=2022
    ).target_cad_value == pytest.approx(100.0)


def test_missing_currency_year_and_ambiguous_macro_fail() -> None:
    converter = CerCurrencyConverter(macro(), scenario="Current Measures", target_year=2020)
    with pytest.raises(ValueError, match="unresolved source currency"):
        converter.convert(1.0, currency="$", dollar_year=2022)
    with pytest.raises(ValueError, match="lack source dollar year"):
        converter.convert(1.0, currency="USD", dollar_year=2023)
    with pytest.raises(ValueError, match="missing or ambiguous"):
        CerCurrencyConverter(
            pd.concat([macro(), macro().iloc[[0]]]), scenario="Current Measures", target_year=2020
        )
