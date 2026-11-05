"""Sales survivors, mixed stock methods, allocation, and activity closure contracts."""

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from parameterization.road_stocks_and_demands import (
    derive_sales_survival_cohorts,
    distribute_existing_road_capacity,
)
from parameterization.road_utilization import calibrate_sales_activity
from utils import load_config_bundle
from validation.config_models import (
    ScenarioConfig,
    ScenarioExistingCapacity,
    ScenarioPeriods,
)


@pytest.fixture
def sales_inputs():
    rules = {
        "ceud_sales_series": {"cars": {"table_id": 21, "raw_series": "Sales|Cars"}},
        "ceud_stock_series": {"cars": {"table_id": 21, "raw_series": "Stock|Cars"}},
        "fixed_lifetime_sources": {"cars": {"kind": "ceud_median", "target_class": "Car"}},
        "survival_curve_classes": ["cars"],
        "first_model_period_rule": "positive_surviving_capacity",
        "ineligible_cohort_redistribution": "proportional_to_eligible_capacity",
        "historical_fuel_types": ["Gasoline", "Diesel"],
        "statcan_ldv_source_types": {"cars": ["Passenger cars"]},
        "report5_age_classes": {}, "region_output_map": {}, "share_region_proxy": {},
        "fuel_technology": {"cars": {
            "Gasoline": "T_LDV_C_GSL_EX", "Diesel": "T_LDV_C_DSL_EX",
            "Battery electric": "T_LDV_C_BEV_EX",
        }},
    }
    provincial = pd.DataFrame([
        {
            "region": region, "year": year, "table_id": 21, "raw_series": "Sales|Cars",
            "unit": "thousands", "value": sales,
            "ceud_value_status": "constant" if year == 2023 else "observed",
        }
        for region in ("ON", "QC")
        for year, sales in ((2021, 10.0), (2022, 20.0), (2023, 30.0))
    ] + [
        {
            "region": region, "year": 2023, "table_id": 21, "raw_series": "Stock|Cars",
            "unit": "thousands", "value": stock, "ceud_value_status": "observed",
        }
        for region, stock in (("ON", 999.0), ("QC", 100.0))
    ])
    curves = pd.DataFrame({
        "source_id": "ontario_report_a_weighted_nhtsa_cafe", "source_class": "Car",
        "age": [0, 1, 2], "survival_probability": [1.0, 0.8, 0.5],
    })
    return {"provincial": provincial, "transformed_curves": curves,
            "regions": ["ON"], "base_year": 2023, "history_start": 2020, "rules": rules}


def test_sales_are_survived_once_with_age_zero_and_explicit_missing_older_history(sales_inputs):
    cohorts = derive_sales_survival_cohorts(**sales_inputs)
    assert cohorts.vintage_year.tolist() == [2021, 2022, 2023]
    assert cohorts.age.tolist() == [2, 1, 0]
    assert cohorts.surviving_k_vehicles.tolist() == pytest.approx([5.0, 16.0, 30.0])
    assert cohorts.surviving_k_vehicles.sum() == pytest.approx(51.0)
    assert set(cohorts.ceud_stock_k_vehicles) == {999.0}
    assert set(cohorts.source_history_start) == {2021}
    assert set(cohorts.included_history_start) == {2021}
    assert set(cohorts.older_history_status) == {"not_reconstructed"}
    assert set(cohorts.survival_source_id) == {"ontario_report_a_weighted_nhtsa_cafe"}
    assert set(cohorts.survival_source_class) == {"Car"}
    assert cohorts.observation_status.tolist() == ["observed", "observed", "constant"]


def test_ceud_comparison_stock_never_rescales_sales_survivors(sales_inputs):
    original = derive_sales_survival_cohorts(**sales_inputs)
    provincial = sales_inputs["provincial"].copy()
    provincial.loc[provincial.raw_series.eq("Stock|Cars"), "value"] *= 10
    changed = derive_sales_survival_cohorts(**(sales_inputs | {"provincial": provincial}))
    pd.testing.assert_series_equal(original.surviving_k_vehicles, changed.surviving_k_vehicles)
    assert set(changed.ceud_stock_k_vehicles) == {9990.0}


def test_history_start_and_base_year_bound_sales_without_inventing_missing_years(sales_inputs):
    future = sales_inputs["provincial"].iloc[[0]].assign(year=2024, value=10000.0)
    cohorts = derive_sales_survival_cohorts(**(sales_inputs | {
        "provincial": pd.concat([sales_inputs["provincial"], future], ignore_index=True),
        "history_start": 2022,
    }))
    assert cohorts.vintage_year.tolist() == [2022, 2023]
    assert cohorts.surviving_k_vehicles.sum() == pytest.approx(46.0)
    assert set(cohorts.source_history_start) == {2021}
    assert set(cohorts.included_history_start) == {2022}


@pytest.mark.parametrize("missing_year", [2022, 2023])
def test_interior_or_base_year_sales_gap_fails(sales_inputs, missing_year):
    source = sales_inputs["provincial"]
    source = source.loc[~(source.raw_series.eq("Sales|Cars") & source.year.eq(missing_year))]
    with pytest.raises(ValueError, match="Incomplete CEUD annual sales history"):
        derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source}))


@pytest.mark.parametrize("invalid", [-1.0, np.nan, np.inf])
def test_invalid_sales_are_not_zero_filled(sales_inputs, invalid):
    source = sales_inputs["provincial"].copy()
    source.loc[source.region.eq("ON") & source.year.eq(2022), "value"] = invalid
    with pytest.raises(ValueError, match="Invalid CEUD sales"):
        derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source}))


def test_missing_sales_and_wrong_unit_fail(sales_inputs):
    source = sales_inputs["provincial"]
    with pytest.raises(ValueError, match="CEUD sales/stock"):
        derive_sales_survival_cohorts(**(sales_inputs | {
            "provincial": source.loc[~source.raw_series.eq("Sales|Cars")],
        }))
    source = source.copy()
    source.loc[source.raw_series.eq("Sales|Cars"), "unit"] = "vehicles"
    with pytest.raises(ValueError, match="CEUD sales/stock"):
        derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source}))


def test_insufficient_survival_coverage_fails(sales_inputs):
    with pytest.raises(ValueError, match="exceeds accepted survival coverage"):
        derive_sales_survival_cohorts(**(sales_inputs | {
            "transformed_curves": sales_inputs["transformed_curves"].iloc[:2],
        }))


@pytest.mark.parametrize("survival", [[0.9, 0.8, 0.5], [1.0, 0.7, 0.8], [1.0, 0.8, -0.1]])
def test_survival_is_cumulative_bounded_and_starts_at_one(sales_inputs, survival):
    curves = sales_inputs["transformed_curves"].assign(survival_probability=survival)
    with pytest.raises(ValueError, match="Invalid cumulative sales survival"):
        derive_sales_survival_cohorts(**(sales_inputs | {"transformed_curves": curves}))


def allocation_inputs(sales_inputs, *, regions=("ON",), lifetime=4):
    registrations = pd.DataFrame([
        {
            "scenario_region": region, "reference_year": year, "vehicle_type": "Passenger cars",
            "fuel_type": fuel, "scaled_value": value, "source_table_id": "test_registration",
        }
        for region in regions
        for year, counts in (
            (2021, {"Gasoline": 90, "Diesel": 10, "Battery electric": 0}),
            (2022, {"Gasoline": 50, "Diesel": 50, "Battery electric": 0}),
            (2023, {"Gasoline": 25, "Diesel": 25, "Battery electric": 50}),
        )
        for fuel, value in counts.items()
    ])
    return {
        "provincial": sales_inputs["provincial"],
        "ldv_age": pd.DataFrame({
            "report_year": [2025, 2025], "nrcan_ceud_class": ["Car", "Car"],
            "age": [0, 2], "fit_active_stock": [50.0, 50.0],
        }),
        "report5_age": pd.DataFrame(), "ldv_registrations": registrations,
        "truck_registrations": pd.DataFrame(),
        "dashboard": pd.DataFrame(columns=["source_region", "ytd_market_share"]),
        "regions": list(regions),
        "period_config": ScenarioPeriods(
            period_mode="prospective", base_year=2023, existing=[2020, 2021, 2022],
            model=[2025], step=1,
        ),
        "vehicle_population_year": 2025, "survival_curves": False, "survival_curve_max_age": 5,
        "fixed_lifetimes_by_class": {"cars": lifetime}, "curve_ages_by_class": {},
        "rules": sales_inputs["rules"],
    }


def test_sales_allocation_needs_no_population_ages_and_preserves_technology_totals(sales_inputs):
    sales = derive_sales_survival_cohorts(**sales_inputs)
    expected = {"T_LDV_C_GSL_EX": 20.0, "T_LDV_C_DSL_EX": 16.0, "T_LDV_C_BEV_EX": 15.0}
    for lifetime in (4, 10):
        args = allocation_inputs(sales_inputs, lifetime=lifetime)
        cohorts, shares, _, capacity = distribute_existing_road_capacity(**(args | {
            "sales_cohorts": sales, "ldv_age": pd.DataFrame(),
        }))
        assert capacity.groupby("tech").capacity.sum().to_dict() == pytest.approx(expected)
        assert capacity.capacity.sum() == pytest.approx(51.0)
        assert cohorts.raw_cohort_k_vehicles.sum() == pytest.approx(51.0)
        assert set(cohorts.stock_method) == {"sales_survival"}
        assert set(cohorts.age_source) == {"CEUD sales times cumulative survival"}
        assert shares.groupby("tech").raw_capacity.sum().to_dict() == pytest.approx(expected)
        if lifetime == 4:
            assert set(capacity.vintage) == {2022}
            assert not shares.loc[~shares.eligible_first_model_period, "capacity"].any()
            assert (shares.redistribution_factor > 1).any()
        else:
            assert set(capacity.vintage) == {2020, 2021, 2022}


def test_mixed_regional_methods_preserve_unselected_stock_results(sales_inputs):
    args = allocation_inputs(sales_inputs, regions=("ON", "QC"), lifetime=10)
    _, _, _, stock = distribute_existing_road_capacity(**args)
    sales = derive_sales_survival_cohorts(**sales_inputs)
    cohorts, _, _, mixed = distribute_existing_road_capacity(**(args | {"sales_cohorts": sales}))
    pd.testing.assert_frame_equal(
        mixed.loc[mixed.region.eq("QC")].reset_index(drop=True),
        stock.loc[stock.region.eq("QC")].reset_index(drop=True),
    )
    assert mixed.groupby("region").capacity.sum().to_dict() == pytest.approx({"ON": 51, "QC": 100})
    assert cohorts.groupby("region").stock_method.first().to_dict() == {
        "ON": "sales_survival", "QC": "stock_age",
    }


@pytest.fixture
def calibration_inputs():
    return {
        "baseline": pd.DataFrame({
            "region": ["ON", "QC"], "road_class": ["cars", "cars"],
            "utilization": [0.1, 0.2], "unit": ["dimensionless", "dimensionless"],
        }),
        "annual": pd.DataFrame({
            "region": ["ON"], "road_class": ["cars"], "year": [2023],
            "activity_bn": [30.0], "stock_k_vehicles": [999.0],
        }),
        "capacity": pd.DataFrame({
            "region": ["ON", "ON"], "tech": ["gas", "electric"],
            "vintage": [2020, 2022], "capacity": [10.0, 20.0],
        }),
        "technology": pd.DataFrame({
            "tech": ["gas", "electric"], "category": ["cars", "cars"], "c2a": [1.0, 2.0],
        }),
        "selected_pairs": {("ON", "cars")}, "base_year": 2023,
    }


def test_activity_closes_on_retained_stock_without_mutating_stock_or_other_regions(calibration_inputs):
    capacity_before = calibration_inputs["capacity"].copy(deep=True)
    baseline_before = calibration_inputs["baseline"].copy(deep=True)
    baseline, audit = calibrate_sales_activity(**calibration_inputs)
    pd.testing.assert_frame_equal(calibration_inputs["capacity"], capacity_before)
    pd.testing.assert_frame_equal(calibration_inputs["baseline"], baseline_before)
    assert baseline.set_index("region").utilization.to_dict() == pytest.approx({"ON": 0.6, "QC": 0.2})
    assert audit.iloc[0].retained_stock_k_vehicles == 30.0
    assert audit.iloc[0].reported_stock_k_vehicles == 999.0
    assert audit.iloc[0].capacity_activity_denominator_bn == 50.0
    assert audit.iloc[0].modelled_activity_bn == pytest.approx(30.0)
    assert audit.iloc[0].activity_residual_bn == pytest.approx(0.0)


def test_infeasible_activity_calibration_is_rejected(calibration_inputs):
    annual = calibration_inputs["annual"].assign(activity_bn=51.0)
    with pytest.raises(ValueError, match="Infeasible sales activity calibration"):
        calibrate_sales_activity(**(calibration_inputs | {"annual": annual}))


def test_zero_calibration_denominator_is_rejected(calibration_inputs):
    capacity = calibration_inputs["capacity"].assign(capacity=0.0)
    with pytest.raises(ValueError, match="Invalid activity calibration denominator"):
        calibrate_sales_activity(**(calibration_inputs | {"capacity": capacity}))


@pytest.mark.parametrize("target", [-1.0, np.inf, np.nan])
def test_nonphysical_calibration_target_is_rejected(calibration_inputs, target):
    annual = calibration_inputs["annual"].assign(activity_bn=target)
    with pytest.raises(ValueError, match="Invalid activity calibration denominator/target"):
        calibrate_sales_activity(**(calibration_inputs | {"annual": annual}))


def test_method_selection_defaults_region_overrides_and_bc_alias():
    model = ScenarioExistingCapacity(vehicle_population_year=2025, cleanup_tolerance=0.001)
    assert model.method_for("ON") == "stock_age"
    selected = model.model_copy(update={"road_stock_method": {
        "ON": "sales_survival", "BC": "sales_survival", "other": "stock_age",
    }})
    assert [selected.method_for(region) for region in ("ON", "BCT", "QC")] == [
        "sales_survival", "sales_survival", "stock_age",
    ]
    everywhere = model.model_copy(update={"road_stock_method": {"other": "sales_survival"}})
    assert all(everywhere.method_for(region) == "sales_survival" for region in ("ON", "BCT", "QC"))


@pytest.mark.parametrize("selection", [{}, {"XX": "sales_survival", "other": "stock_age"}])
def test_invalid_method_region_configuration_fails(selection):
    with pytest.raises(ValidationError):
        ScenarioExistingCapacity(
            vehicle_population_year=2025, cleanup_tolerance=0.001, road_stock_method=selection,
        )


def test_sales_scenario_rejects_double_retirement_and_age_vkt_only_when_selected():
    payload = load_config_bundle("config/scenarios/default.yaml").scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON"]
    payload["lifetimes"]["survival_curves"] = False
    payload["road_utilization"]["vkt_schedules"] = False
    payload["existing_capacity"]["road_stock_method"] = {"ON": "sales_survival", "other": "stock_age"}
    ScenarioConfig.model_validate(payload)
    curves = deepcopy(payload)
    curves["lifetimes"]["survival_curves"] = True
    with pytest.raises(ValidationError, match="requires fixed future lifetimes"):
        ScenarioConfig.model_validate(curves)
    schedules = deepcopy(payload)
    schedules["road_utilization"]["vkt_schedules"] = True
    with pytest.raises(ValidationError, match="requires flat road utilization"):
        ScenarioConfig.model_validate(schedules)
    curves["geography"]["regions"] = ["QC"]
    ScenarioConfig.model_validate(curves)


def test_selected_regions_all_require_stock_methods_even_after_sales_region():
    payload = load_config_bundle("config/scenarios/default.yaml").scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON", "QC"]
    payload["existing_capacity"]["road_stock_method"] = {"ON": "sales_survival"}
    with pytest.raises(ValueError, match="QC"):
        ScenarioConfig.model_validate(payload)


def test_prospective_base_year_label_retains_2030_future_endpoint():
    payload = load_config_bundle("config/scenarios/default.yaml").scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON"]
    payload["periods"] = {
        "period_mode": "prospective", "base_year": 2025,
        "existing": [2000, 2005, 2010, 2015, 2020],
        "model": [2025, 2030, 2035, 2040, 2045], "step": 5,
    }
    payload["sources"]["nrcan_ceud_beyond_observed"] = "constant"
    payload["lifetimes"]["survival_curves"] = False
    payload["road_utilization"]["vkt_schedules"] = False
    payload["existing_capacity"]["road_stock_method"] = {"ON": "sales_survival"}
    scenario = ScenarioConfig.model_validate(payload)
    assert scenario.periods.model[0] == scenario.periods.base_year == 2025
    assert scenario.periods.historical_years(2020) == list(range(2021, 2026))
    assert scenario.periods.projection_year(2025, legacy_at_end=True) == 2030
