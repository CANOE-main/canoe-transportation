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
    assert cohorts.calibrated_surviving_k_vehicles.sum() == pytest.approx(999.0)
    assert cohorts.calibrated_acquisitions_k_vehicles.tolist() == pytest.approx(
        cohorts.sales_k_vehicles * 999. / 51.)
    assert set(cohorts.stock_calibration_multiplier) == {999.0 / 51.0}
    assert set(cohorts.stock_calibration_basis) == {"base_year_ceud_stock"}
    assert set(cohorts.ceud_stock_k_vehicles) == {999.0}
    assert set(cohorts.source_history_start) == {2021}
    assert set(cohorts.included_history_start) == {2021}
    assert set(cohorts.older_history_status) == {"not_reconstructed"}
    assert set(cohorts.survival_source_id) == {"ontario_report_a_weighted_nhtsa_cafe"}
    assert set(cohorts.survival_source_class) == {"Car"}
    assert cohorts.observation_status.tolist() == ["observed", "observed", "constant"]


def test_ceud_stock_scales_modelled_cohorts_while_raw_sales_survivors_stay_unchanged(sales_inputs):
    original = derive_sales_survival_cohorts(**sales_inputs)
    provincial = sales_inputs["provincial"].copy()
    provincial.loc[provincial.raw_series.eq("Stock|Cars"), "value"] *= 10
    changed = derive_sales_survival_cohorts(**(sales_inputs | {"provincial": provincial}))
    pd.testing.assert_series_equal(original.surviving_k_vehicles, changed.surviving_k_vehicles)
    pd.testing.assert_series_equal(original.sales_k_vehicles, changed.sales_k_vehicles)
    assert changed.calibrated_surviving_k_vehicles.tolist() == pytest.approx(original.calibrated_surviving_k_vehicles * 10)
    assert changed.stock_calibration_multiplier.tolist() == pytest.approx(original.stock_calibration_multiplier * 10)
    assert set(changed.ceud_stock_k_vehicles) == {9990.0}


def test_history_start_and_base_year_bound_sales_without_inventing_missing_years(sales_inputs):
    future = sales_inputs["provincial"].iloc[[0]].assign(year=2024, value=10000.0)
    cohorts = derive_sales_survival_cohorts(**(sales_inputs | {
        "provincial": pd.concat([sales_inputs["provincial"], future], ignore_index=True),
        "history_start": 2022,
    }))
    assert cohorts.vintage_year.tolist() == [2022, 2023]
    assert cohorts.surviving_k_vehicles.sum() == pytest.approx(46.0)
    assert cohorts.calibrated_surviving_k_vehicles.sum() == pytest.approx(999.0)
    assert set(cohorts.source_history_start) == {2021}
    assert set(cohorts.included_history_start) == {2022}


def test_sales_older_than_an_explicit_zero_endpoint_have_no_survivors(sales_inputs):
    curves = sales_inputs["transformed_curves"].iloc[:2].copy()
    curves["survival_probability"] = [1.0, 0.0]
    cohorts = derive_sales_survival_cohorts(**(sales_inputs | {"transformed_curves": curves}))
    assert cohorts.survival_probability.tolist() == [0.0, 0.0, 1.0]
    assert cohorts.surviving_k_vehicles.tolist() == [0.0, 0.0, 30.0]
    assert cohorts.calibrated_surviving_k_vehicles.sum() == pytest.approx(999.0)
    assert cohorts.sales_k_vehicles.tolist() == [10.0, 20.0, 30.0]


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
    multiplier = 999. / 51.
    expected = {"T_LDV_C_GSL_EX": 20.0 * multiplier, "T_LDV_C_DSL_EX": 16.0 * multiplier, "T_LDV_C_BEV_EX": 15.0 * multiplier}
    for lifetime in (4, 10):
        args = allocation_inputs(sales_inputs, lifetime=lifetime)
        cohorts, shares, _, capacity = distribute_existing_road_capacity(**(args | {
            "sales_cohorts": sales, "ldv_age": pd.DataFrame(),
        }))
        assert capacity.groupby("tech").capacity.sum().to_dict() == pytest.approx(expected)
        assert capacity.capacity.sum() == pytest.approx(999.0)
        assert cohorts.raw_cohort_k_vehicles.sum() == pytest.approx(999.0)
        assert cohorts.uncalibrated_surviving_k_vehicles.sum() == pytest.approx(51.0)
        assert set(cohorts.stock_calibration_multiplier) == {multiplier}
        assert set(cohorts.stock_method) == {"sales_survival"}
        assert set(cohorts.age_source) == {"CEUD sales times cumulative survival"}
        assert shares.groupby("tech").raw_capacity.sum().to_dict() == pytest.approx(expected)
        if lifetime == 4:
            assert set(capacity.vintage) == {2022}
            assert not shares.loc[~shares.eligible_first_model_period, "capacity"].any()
            assert (shares.redistribution_factor > 1).any()
        else:
            assert set(capacity.vintage) == {2020, 2021, 2022}


def test_partial_regional_sales_cannot_fall_back_to_population_ages(sales_inputs):
    args = allocation_inputs(sales_inputs, regions=("ON", "QC"), lifetime=10)
    sales = derive_sales_survival_cohorts(**sales_inputs)
    with pytest.raises(ValueError, match="complete regional car/truck sales coverage"):
        distribute_existing_road_capacity(**(args | {"sales_cohorts": sales}))
    _, _, _, stock = distribute_existing_road_capacity(**(args | {"cohort_method": "stock_age"}))
    assert stock.groupby("region").capacity.sum().to_dict() == pytest.approx({"ON": 999, "QC": 100})


def test_each_region_gets_its_own_reported_stock_calibration(sales_inputs, caplog):
    with caplog.at_level("INFO"):
        sales = derive_sales_survival_cohorts(**(sales_inputs | {"regions": ["ON", "QC"]}))
    assert sales.groupby("ceud_region").surviving_k_vehicles.sum().to_dict() == {"ON": 51., "QC": 51.}
    assert sales.groupby("ceud_region").calibrated_surviving_k_vehicles.sum().to_dict() == pytest.approx({"ON": 999., "QC": 100.})
    assert sales.groupby("ceud_region").stock_calibration_multiplier.first().to_dict() == pytest.approx({"ON": 999 / 51, "QC": 100 / 51})
    assert "raw survivors 51" in caplog.text
    assert "CEUD target 999" in caplog.text
    assert "CEUD target 100" in caplog.text


def test_each_road_class_gets_its_own_reported_stock_calibration(sales_inputs):
    rules = deepcopy(sales_inputs["rules"])
    for key in ("ceud_sales_series", "ceud_stock_series", "fixed_lifetime_sources"):
        rules[key]["passenger_light_trucks"] = deepcopy(rules[key]["cars"])
    rules["ceud_stock_series"]["passenger_light_trucks"]["raw_series"] = "Stock|LightTrucks"
    light_stock = sales_inputs["provincial"].loc[sales_inputs["provincial"].raw_series.eq("Stock|Cars")].assign(
        raw_series="Stock|LightTrucks", value=75.,
    )
    source = pd.concat([sales_inputs["provincial"], light_stock], ignore_index=True)
    sales = derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source, "rules": rules}))
    assert sales.groupby("road_class").calibrated_surviving_k_vehicles.sum().to_dict() == pytest.approx({"cars": 999., "passenger_light_trucks": 75.})
    assert sales.groupby("road_class").surviving_k_vehicles.sum().to_dict() == {"cars": 51., "passenger_light_trucks": 51.}


def test_stock_calibration_never_falls_back_when_surviving_sales_are_zero(sales_inputs):
    source = sales_inputs["provincial"].copy()
    source.loc[source.raw_series.eq("Sales|Cars"), "value"] = 0
    with pytest.raises(ValueError, match="No positive surviving sales"):
        derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source}))


def test_zero_reported_stock_is_an_explicit_zero_multiplier(sales_inputs):
    source = sales_inputs["provincial"].copy()
    source.loc[source.raw_series.eq("Stock|Cars"), "value"] = 0
    sales = derive_sales_survival_cohorts(**(sales_inputs | {"provincial": source}))
    assert sales.surviving_k_vehicles.sum() == 51
    assert set(sales.stock_calibration_multiplier) == {0}
    assert sales.calibrated_surviving_k_vehicles.sum() == 0
    args = allocation_inputs(sales_inputs) | {"provincial": source, "sales_cohorts": sales}
    cohorts, shares, _, capacity = distribute_existing_road_capacity(**args)
    assert cohorts.cohort_k_vehicles.sum() == 0
    assert shares.capacity.sum() == 0
    assert capacity.empty


def test_allocator_rejects_stale_sales_stock_anchor(sales_inputs):
    sales = derive_sales_survival_cohorts(**sales_inputs)
    sales["calibrated_surviving_k_vehicles"] *= .9
    with pytest.raises(ValueError, match="preserve their CEUD stock calibration"):
        distribute_existing_road_capacity(**(allocation_inputs(sales_inputs) | {"sales_cohorts": sales}))


def test_composite_bins_keep_annual_survival_and_separate_solver_from_physical_stock(sales_inputs):
    inputs = deepcopy(sales_inputs)
    inputs["transformed_curves"] = pd.DataFrame({
        "source_id": "ontario_report_a_weighted_nhtsa_cafe", "source_class": "Car",
        "road_class": "cars", "age": range(7),
        "survival_probability": [1., .8, .5, .3, .2, .1, 0.],
    })
    sales = derive_sales_survival_cohorts(**inputs)
    args = allocation_inputs(inputs, lifetime=10)
    args.update({
        "sales_cohorts": sales, "survival_curves": True,
        "curve_ages_by_class": {"cars": set(range(7))},
        "annual_survival": inputs["transformed_curves"],
        "period_config": ScenarioPeriods(period_mode="prospective", base_year=2023,
                                         existing=[2020], model=[2023], step=5),
    })
    cohorts, shares, _, parameters = distribute_existing_road_capacity(**args)
    assert cohorts.cohort_k_vehicles.sum() == pytest.approx(999.)
    assert shares.capacity.sum() == pytest.approx(999.)
    assert parameters.capacity.sum() == pytest.approx(60. * 999. / 51.)
    assert parameters.capacity.sum() > shares.capacity.sum()
    assert shares.solver_capacity_k_vehicles.sum() == pytest.approx(parameters.capacity.sum())
    assert set(shares.capacity_basis) == {"installed_capacity_before_survival"}
    # Historical fuel proxies stay at each actual year, including its distinct survival.
    expected = {"T_LDV_C_GSL_EX": 20. * 999. / 51.,
                "T_LDV_C_DSL_EX": 16. * 999. / 51., "T_LDV_C_BEV_EX": 15. * 999. / 51.}
    assert shares.groupby("tech").capacity.sum().to_dict() == pytest.approx(expected)
    altered = inputs["transformed_curves"].copy()
    altered.loc[altered.age.eq(2), "survival_probability"] = .4
    with pytest.raises(ValueError, match="calibrated annual physical stock"):
        distribute_existing_road_capacity(**(args | {"annual_survival": altered}))


@pytest.mark.parametrize("old_sales,expected_loss", [(100.0, 0.0), (10000.0, 999.0)])
def test_grouped_tail_retirement_is_recorded_without_recalibrating_physical_stock(sales_inputs, old_sales, expected_loss):
    inputs = deepcopy(sales_inputs)
    source = inputs["provincial"]
    for year, value in ((2021, old_sales), (2022, 1.0), (2023, 0.0)):
        source.loc[source.raw_series.eq("Sales|Cars") & source.year.eq(year), "value"] = value
    inputs["transformed_curves"] = inputs["transformed_curves"].assign(
        road_class="cars", survival_probability=[1.0, 0.2, 0.0],
    )
    sales = derive_sales_survival_cohorts(**inputs)
    args = allocation_inputs(inputs, lifetime=10) | {
        "sales_cohorts": sales, "survival_curves": True,
        "curve_ages_by_class": {"cars": {0, 1}}, "annual_survival": inputs["transformed_curves"],
        "period_config": ScenarioPeriods(period_mode="prospective", base_year=2023,
                                         existing=[2020], model=[2023], step=5),
    }
    _, shares, _, _ = distribute_existing_road_capacity(**args)
    assert sales.stock_calibration_multiplier.unique().tolist() == pytest.approx([999.0 / 0.2])
    assert shares.capacity.sum() == pytest.approx(999.0)
    loss = shares.grouped_tail_reference_loss_k_vehicles.sum()
    # Keep the ~0.002 grouped fraction even though it is below the selected
    # source-tail threshold. Only the more extreme mix falls below Temoa's floor.
    assert loss == pytest.approx(expected_loss)
    assert shares.solver_reference_stock_k_vehicles.sum() + loss == pytest.approx(999.0)
    summary = pd.DataFrame(shares.attrs["vintage_curve_audit"])
    annual_loss = pd.DataFrame(shares.attrs["curve_tail_retirements"])
    assert summary.removed_reference_stock_k_vehicles.sum() == pytest.approx(loss)
    if expected_loss == 0:
        assert annual_loss.empty
        assert summary.removed_stock_years_k_vehicles.sum() == 0
        return
    assert annual_loss.loc[annual_loss.period.eq(2023), "removed_k_vehicles"].sum() == pytest.approx(loss)
    assert annual_loss.removed_k_vehicles.sum() == pytest.approx(summary.removed_stock_years_k_vehicles.sum())


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


def test_production_has_one_sales_method_for_all_regions():
    model = ScenarioExistingCapacity(vehicle_population_year=2025, cleanup_tolerance=0.001)
    assert set(model.model_dump()) == {"vehicle_population_year", "cleanup_tolerance"}


@pytest.mark.parametrize("selection", [{}, {"ON": "sales_survival", "other": "stock_age"}])
def test_retired_method_selector_is_rejected(selection):
    with pytest.raises(ValidationError):
        ScenarioExistingCapacity(
            vehicle_population_year=2025, cleanup_tolerance=0.001, road_stock_method=selection,
        )


@pytest.mark.parametrize("threshold", [0.005, 0.02])
def test_curves_use_the_selected_production_tail_derivation(threshold):
    from parameterization.road_lifetimes_survival import require_tail_derivation
    from dataclasses import replace

    bundle = load_config_bundle("config/scenarios/default.yaml")
    payload = bundle.scenario.model_dump(mode="python")
    payload["lifetimes"]["survival_curves"] = True
    payload["lifetimes"]["tail_survival_threshold"] = threshold
    curves = ScenarioConfig.model_validate(payload)
    settings = require_tail_derivation(replace(bundle, scenario=curves))
    assert settings["tail_derivation"] == "constant_retirement"
    assert settings["threshold"] == threshold


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
    scenario = ScenarioConfig.model_validate(payload)
    assert scenario.periods.model[0] == scenario.periods.base_year == 2025
    assert scenario.periods.historical_years(2020) == list(range(2021, 2026))
    assert scenario.periods.projection_year(2025, legacy_at_end=True) == 2030
