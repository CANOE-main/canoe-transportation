from pathlib import Path

import pandas as pd
import pytest

from parameterization.offroad_capex_opex import (
    manual_offroad_capex, offroad_opex_from_capex_ratio,
)


def test_reviewed_capex_baseline_and_multiplier_stay_related() -> None:
    source = pd.read_csv(
        Path(__file__).resolve().parents[1]
        / "inputs/0_manual_params/cost_invest_multipliers.csv"
    )
    period_years = {"through_2035": 2035, "through_2050": 2050}
    diesel = manual_offroad_capex(
        source, mode="freight_rail", powertrain="diesel", year=2040,
        period_years=period_years,
    )
    lng = manual_offroad_capex(
        source, mode="freight_rail", powertrain="lng", year=2040,
        period_years=period_years,
    )
    assert diesel["source_currency"] == "CAD"
    assert diesel["source_dollar_year"] == 2015
    assert diesel["native_cost_per_vehicle"] == pytest.approx(2893901)
    assert diesel["source_unit"] == "CAD/vehicle"
    assert diesel["native_millions_per_billion_service"] == pytest.approx(
        2893901 / 79735
    )
    assert lng["multiplier"] == pytest.approx(1.57 + (1.55 - 1.57) * 5 / 15)
    early = manual_offroad_capex(
        source, mode="freight_rail", powertrain="lng", year=2025,
        period_years=period_years,
    )
    assert early["multiplier"] == pytest.approx(1.57)
    assert lng["native_millions_per_billion_service"] == pytest.approx(
        diesel["native_millions_per_billion_service"] * lng["multiplier"]
    )
    assert offroad_opex_from_capex_ratio(100, ratio=0.06) == pytest.approx(6)
    ratios = pd.read_csv(
        Path(__file__).resolve().parents[1]
        / "inputs/0_manual_params/cost_variable_multipliers.csv"
    ).set_index("category").value.to_dict()
    assert ratios == {
        "passenger_rail": 0.10, "freight_rail": 0.06, "freight_marine": 0.05
    }
