"""A solver installation basis cannot silently become the physical fleet."""

from dataclasses import replace

from canoe_schema.v4_0 import ExistingCapacity
import pandas as pd
import pytest

from parameterization.build_existing_capacity import physical_stock_for_consumers
from utils import load_config_bundle


def fleet(curves):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    scenario = bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": curves}),
    })
    bundle = replace(bundle, scenario=scenario)
    rows = [ExistingCapacity(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020,
                             capacity=100.0, units="k vehicles", data_id="cohort")]
    physical = pd.DataFrame([{
        "region": "ON", "tech": "T_LDV_C_BEV_EX", "vintage": 2020,
        "surviving_k_vehicles": 60., "base_year": scenario.periods.base_year,
        "units": "k vehicles", "data_id": "cohort",
    }])
    return bundle, rows, physical


def test_curve_parameter_requires_explicit_survivors():
    bundle, rows, physical = fleet(True)
    with pytest.raises(ValueError, match="explicit physical-stock"):
        physical_stock_for_consumers(bundle, rows, None)
    actual = physical_stock_for_consumers(bundle, rows, physical)
    assert actual.surviving_k_vehicles.sum() == 60.
    assert rows[0].capacity == 100.


def test_fixed_capacity_has_the_same_physical_basis():
    bundle, rows, physical = fleet(False)
    assert physical_stock_for_consumers(bundle, rows, None).surviving_k_vehicles.sum() == 100.
    with pytest.raises(ValueError, match="must agree"):
        physical_stock_for_consumers(bundle, rows, physical)


@pytest.mark.parametrize("column,value,message", [
    ("base_year", 1900, "reference year"),
    ("units", "vehicles", "units"),
    ("data_id", "different", "lineage"),
    ("surviving_k_vehicles", float("nan"), "null"),
    ("surviving_k_vehicles", -1, "vehicle counts"),
])
def test_invalid_or_stale_physical_stock_is_rejected(column, value, message):
    bundle, rows, physical = fleet(True)
    physical.loc[0, column] = value
    with pytest.raises(ValueError, match=message):
        physical_stock_for_consumers(bundle, rows, physical)


def test_reference_stock_can_precede_solver_support_without_disappearing():
    bundle, rows, physical = fleet(True)
    retired_before_first_period = physical.assign(vintage=2000, surviving_k_vehicles=5.)
    actual = physical_stock_for_consumers(bundle, rows, pd.concat([physical, retired_before_first_period]))
    assert actual.surviving_k_vehicles.sum() == 65.

