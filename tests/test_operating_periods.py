"""Operating parameters share complete fixed/curve retirement coverage."""

import pytest
from canoe_schema.v4_0 import LifetimeSurvivalCurve, LifetimeTech

from parameterization.build_lifetime_parameters import lifetime_endpoints


def annual_curve(*, vintage=2020, end=2037, tech="T_CAR_EX", region="ON"):
    return [LifetimeSurvivalCurve(
        region=region, tech=tech, vintage=vintage, period=year,
        fraction=(end - year) / (end - vintage), data_id="test-lifetime",
    ) for year in range(vintage, end + 1)]


def test_fixed_lifetime_includes_terminal_partial_period_and_rejects_later_periods():
    endpoints = lifetime_endpoints([
        LifetimeTech(region="ON", tech="T_CAR_EX", lifetime=17, data_id="test-lifetime"),
        LifetimeTech(region="AB", tech="T_CAR_EX", lifetime=20, data_id="test-lifetime"),
    ], [])
    periods = [2015, 2020, 2025, 2030, 2035, 2040, 2045]
    assert endpoints.end_year("ON", "T_CAR_EX", 2020) == 2037
    assert endpoints.operating_periods("ON", "T_CAR_EX", 2020, periods) == (
        2020, 2025, 2030, 2035,
    )
    assert endpoints.operating_periods("AB", "T_CAR_EX", 2020, periods) == (
        2020, 2025, 2030, 2035,
    )
    assert endpoints.end_year("ON", "T_CAR_EX", 2025) == 2042


def test_curve_lifetime_uses_process_endpoint_and_covers_horizon_when_still_operating():
    endpoints = lifetime_endpoints([], [
        *annual_curve(end=2037),
        *annual_curve(vintage=2025, end=2082, tech="T_CAR_N"),
    ])
    periods = [2025, 2030, 2035, 2040, 2045, 2050]
    assert endpoints.end_year("ON", "T_CAR_EX", 2020) == 2037
    assert endpoints.operating_periods("ON", "T_CAR_EX", 2020, periods) == (
        2025, 2030, 2035,
    )
    assert endpoints.operating_periods("ON", "T_CAR_N", 2025, periods) == tuple(periods)
    with pytest.raises(ValueError, match="Missing lifetime curve for process"):
        endpoints.operating_periods("ON", "T_CAR_EX", 2015, periods)
    with pytest.raises(ValueError, match="no prepared lifetime"):
        endpoints.operating_periods("QC", "T_CAR_EX", 2020, periods)


@pytest.mark.parametrize("lifetime", [0, -1, float("nan"), float("inf"), None])
def test_invalid_fixed_lifetime_is_rejected(lifetime):
    row = LifetimeTech(region="ON", tech="T_CAR_EX", lifetime=17, data_id="test-lifetime")
    with pytest.raises(ValueError, match="invalid fixed lifetime"):
        lifetime_endpoints([row.model_copy(update={"lifetime": lifetime})], [])


def test_duplicate_or_conflicting_lifetime_owners_are_rejected():
    row = LifetimeTech(region="ON", tech="T_CAR_EX", lifetime=17, data_id="test-lifetime")
    with pytest.raises(ValueError, match="Duplicate or invalid fixed lifetime"):
        lifetime_endpoints([row, row], [])
    with pytest.raises(ValueError, match="Conflicting fixed and survival-curve"):
        lifetime_endpoints([row], annual_curve())


@pytest.mark.parametrize("mutation", [
    "gap", "duplicate", "missing_zero", "missing_one", "interior_zero",
    "increasing", "nonfinite", "missing_fraction", "below_temoa_minimum",
])
def test_incomplete_or_invalid_curve_cannot_silently_shorten_operating_coverage(mutation):
    rows = annual_curve()
    if mutation == "gap":
        rows.pop(5)
    elif mutation == "duplicate":
        rows.append(rows[5])
    elif mutation == "missing_zero":
        rows.pop()
    elif mutation == "missing_one":
        rows.pop(0)
    else:
        values = {
            "interior_zero": 0,
            "increasing": 1,
            "nonfinite": float("nan"),
            "missing_fraction": None,
            "below_temoa_minimum": 0.00001,
        }
        index = -2 if mutation == "below_temoa_minimum" else 5
        rows[index] = rows[index].model_copy(update={"fraction": values[mutation]})
    with pytest.raises(ValueError, match="Invalid or incomplete annual lifetime curve"):
        lifetime_endpoints([], rows)
