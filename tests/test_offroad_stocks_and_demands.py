import pandas as pd
import pytest

from parameterization.offroad_stocks_and_demands import (
    estimate_demand_unit_capacity,
    estimate_linear_cohort_additions,
    redistribute_eligible_offroad_cohorts,
)


def test_offroad_energy_to_capacity_uses_same_year_and_keeps_marine_fuels() -> None:
    energy = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "freight_marine",
                "tech": "MDO",
                "year": 2023,
                "energy_pj": 2.0,
            },
            {
                "region": "ON",
                "mode": "freight_marine",
                "tech": "HFO",
                "year": 2023,
                "energy_pj": 3.0,
            },
        ]
    )
    intensity = pd.DataFrame(
        [
            {
                "mode": "freight_marine",
                "year": 2023,
                "intensity_mj_per_km": 0.5,
                "units": "bn tonne-km",
            }
        ]
    )

    result = estimate_demand_unit_capacity(energy, intensity)

    assert dict(zip(result.tech, result.capacity, strict=True)) == {
        "HFO": 6.0,
        "MDO": 4.0,
    }
    assert set(result.units) == {"bn tonne-km"}
    with pytest.raises(ValueError, match="Missing same-year national"):
        estimate_demand_unit_capacity(
            energy,
            intensity.assign(year=2022),
        )


def test_linear_cohort_retirement_conserves_constant_capacity() -> None:
    annual = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "freight_rail",
                "tech": "DIESEL",
                "year": year,
                "capacity": 100.0,
                "units": "bn tonne-km",
            }
            for year in (2021, 2022, 2023)
        ]
    )

    result = estimate_linear_cohort_additions(
        annual, lifetimes_by_mode={"freight_rail": 2}, base_year=2023
    )

    assert result.capacity_additions.tolist() == pytest.approx([100.0, 50.0, 75.0])
    assert result.capacity_at_base_year.sum() == pytest.approx(100.0)
    assert result.residual_before_additions.tolist() == pytest.approx([0.0, 50.0, 25.0])
    with pytest.raises(ValueError, match="contiguous"):
        estimate_linear_cohort_additions(
            annual.loc[annual.year.ne(2022)],
            lifetimes_by_mode={"freight_rail": 2},
            base_year=2023,
        )


def test_annual_reconciliation_retires_excess_and_restores_recent_additions() -> None:
    annual = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "freight_marine",
                "tech": "MARINE_POOL",
                "year": year,
                "capacity": capacity,
                "units": "bn tonne-km",
            }
            for year, capacity in ((2021, 100.0), (2022, 50.0), (2023, 60.0))
        ]
    )

    reconciled = estimate_linear_cohort_additions(
        annual,
        lifetimes_by_mode={"freight_marine": 10},
        base_year=2023,
        reconcile_annual_capacity=True,
    )

    assert reconciled.early_retired_capacity.tolist() == pytest.approx([0.0, 40.0, 0.0])
    assert reconciled.capacity_additions.tolist() == pytest.approx(
        [100.0, 0.0, 15.555555555555557]
    )
    assert reconciled.capacity_at_base_year.sum() == pytest.approx(60.0)


def test_first_model_period_redistributes_retired_cohort_without_losing_proxy() -> None:
    cohorts = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "freight_rail",
                "tech": "RAIL",
                "vintage_year": year,
                "lifetime_years": 25,
                "capacity_at_base_year": value,
            }
            for year, value in ((2000, 20.0), (2005, 30.0), (2023, 50.0))
        ]
    )
    proxy = pd.DataFrame(
        [{"region": "ON", "mode": "freight_rail", "tech": "RAIL", "capacity": 100.0}]
    )

    result = redistribute_eligible_offroad_cohorts(
        cohorts, proxy, first_model_period=2025
    )

    assert result.eligible_first_model_period.tolist() == [False, True, True]
    assert result.capacity_at_base_year.tolist() == pytest.approx([0.0, 37.5, 62.5])
    assert result.raw_capacity_at_base_year.tolist() == [20.0, 30.0, 50.0]
