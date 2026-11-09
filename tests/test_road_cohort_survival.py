"""Annual source reconciliation, composite conservation and explicit tail limits."""

import pandas as pd
import pytest

from parameterization.road_cohort_survival import (
    build_composite_cohorts,
    reconcile_survival_schedules,
)


def schedule(values):
    return pd.DataFrame({"road_class": "cars", "source_id": "source", "source_class": "Cars",
                         "age": range(len(values)), "survival_probability": values})


def acquisitions(years=(2011, 2015), vintage=2010, amounts=(100.0, 200.0)):
    return pd.DataFrame({"region": "ON", "road_class": "cars", "tech": "car_EX",
                         "vintage_year": years, "vintage": vintage,
                         "calibrated_acquisitions_k_vehicles": amounts})


def reconcile(source, *, cap=None, threshold=0.001):
    return reconcile_survival_schedules(source, tail_derivation="constant_retirement",
                                        threshold=threshold, hard_zero_age=cap)


def test_derived_tail_preserves_native_precision_and_keeps_threshold_point():
    source = schedule([1.0, 0.7, 0.49])
    original = source.copy(deep=True)
    result = reconcile(source)
    pd.testing.assert_frame_equal(source, original)
    assert result.annual.survival_probability.iloc[:3].tolist() == source.survival_probability.tolist()
    assert result.annual.survival_probability.iloc[3] == pytest.approx(0.343)
    assert 0 < result.annual.survival_probability.iloc[-2] <= 0.001
    assert result.annual.survival_probability.iloc[-3] > 0.001
    assert result.annual.survival_probability.iloc[-1] == 0
    assert result.audit.terminal_retirement_rate.item() == pytest.approx(0.3)
    assert result.audit.removed_source_survival_years.item() == 0
    assert result.audit.added_tail_survival_years.item() > 0


def test_hard_zero_cap_is_optional_and_can_extend_beyond_source_support():
    source = schedule([1.0, 0.8, 0.6, 0.3])
    result = reconcile(source, cap=2)
    assert result.annual.survival_probability.tolist() == [1.0, 0.8, 0.0]
    assert result.annual.source_survival_probability.tolist() == [1.0, 0.8, 0.6]
    assert result.audit.removed_source_survival_years.item() == pytest.approx(0.9)
    assert result.audit.hard_zero_age.item() == 2
    extended = reconcile(source, cap=5)
    assert extended.annual.survival_probability.tolist() == [1.0, 0.8, 0.6, 0.3, 0.15, 0.0]
    assert extended.audit.removed_source_survival_years.item() == 0
    for invalid in (0, -1, 2.5, True):
        with pytest.raises(ValueError, match="hard zero age"):
            reconcile(source, cap=invalid)


def test_existing_native_tail_below_target_is_preserved_to_its_last_known_age():
    source = schedule([1.0, 0.1, 0.0008, 0.0004])
    result = reconcile(source)
    assert result.annual.survival_probability.tolist() == [1.0, 0.1, 0.0008, 0.0004, 0.0]
    assert result.audit.removed_source_survival_years.item() == 0


def test_long_tail_has_no_candidate_extension_guard_and_flat_tail_requires_cap():
    result = reconcile(schedule([1.0, 0.99]))
    assert result.audit.terminal_age.item() > 100
    assert result.annual.survival_probability.iloc[-2] <= 0.001
    with pytest.raises(ValueError, match="finite survival tail"):
        reconcile(schedule([1.0, 1.0]))
    assert reconcile(schedule([1.0, 1.0]), cap=3).annual.survival_probability.tolist() == [1, 1, 1, 0]


def test_numerical_floor_ends_derived_tail_without_clamping_native_values():
    result = reconcile(schedule([1.0, 0.0011]))
    assert result.annual.survival_probability.tolist() == [1.0, 0.0011, 0.0]
    assert result.annual.treatment.iloc[-1] == "temoa_zero_endpoint"
    with pytest.raises(ValueError, match="Native positive survival"):
        reconcile(schedule([1.0, 0.00001]))
    assert reconcile(schedule([1.0, 0.00001]), cap=1).annual.survival_probability.tolist() == [1, 0]


@pytest.mark.parametrize("vintage", [2010, 2015])
def test_composite_preserves_each_annual_survivor_total_for_left_and_right_labels(vintage):
    source = schedule([1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0])
    result = build_composite_cohorts(acquisitions(vintage=vintage), source,
                                    reference_year=2015, first_model_period=2020)
    installed = result.capacity.capacity.item()
    assert installed == pytest.approx(300 if vintage == 2010 else 260)
    assert result.physical_stock.installed_equivalent_k_vehicles.sum() == pytest.approx(installed)
    assert result.physical_stock.physical_stock_k_vehicles.sum() == pytest.approx(260)
    lookup = source.set_index("age").survival_probability.to_dict()
    for row in result.curves.itertuples(index=False):
        expected = sum(amount * lookup.get(max(0, row.period - year), 0)
                       for year, amount in ((2011, 100), (2015, 200)))
        assert installed * row.fraction == pytest.approx(expected)
    mean = result.curves.loc[result.curves.period.between(2020, 2024), "fraction"].mean() * installed
    assert mean == pytest.approx((110 + 80 + 60 + 40 + 20) / 5)
    assert result.lifetimes.lifetime.item() + vintage == 2025
    assert result.audit.temoa_compatible.item()


def test_retired_bins_are_audited_without_reappearing_as_solver_capacity():
    result = build_composite_cohorts(acquisitions(years=(2000, 2001), amounts=(5, 7)),
                                    schedule([1.0, 0.5, 0.0]),
                                    reference_year=2015, first_model_period=2020)
    assert result.capacity.empty and result.curves.empty and result.lifetimes.empty
    assert result.physical_stock.calibrated_acquisitions_k_vehicles.sum() == 12
    assert result.physical_stock.physical_stock_k_vehicles.sum() == 0
    assert result.audit.status.item() == "retired_before_model"


def test_grouped_tail_keeps_physical_stock_and_reports_exact_early_retirement():
    result = build_composite_cohorts(
        acquisitions(years=(2011, 2015), amounts=(99999, 1)),
        schedule([1.0, 0.5, 0.0]), reference_year=2015, first_model_period=2016)
    assert result.capacity.empty
    assert result.physical_stock.physical_stock_k_vehicles.sum() == 1
    assert result.physical_stock.solver_reference_stock_k_vehicles.sum() == 0
    assert result.physical_stock.grouped_tail_reference_loss_k_vehicles.sum() == 1
    assert result.audit.removed_reference_stock_k_vehicles.item() == 1
    assert result.audit.removed_first_model_stock_k_vehicles.item() == 0.5
    assert result.audit.removed_stock_years_k_vehicles.item() == result.tail_losses.removed_k_vehicles.sum()
    assert result.audit.temoa_compatible.item()


def test_grouped_fraction_below_temoa_floor_uses_zero_in_that_year():
    result = build_composite_cohorts(
        acquisitions(years=(2011, 2012), amounts=(99999, 1)),
        schedule([1.0, 0.5, 0.0]), reference_year=2012, first_model_period=2012)
    assert result.curves.period.max() == 2013
    assert result.curves.fraction.tolist() == [1.0, 1.0, 0.500005, 0.0]
    assert result.audit.removed_reference_stock_k_vehicles.item() == 0
    assert result.tail_losses.period.min() == 2013


def test_grouping_preserves_known_source_tail_below_selected_derivation_threshold():
    source = reconcile(schedule([1.0, 0.1, 0.0008, 0.0004]), threshold=0.005)
    result = build_composite_cohorts(
        acquisitions(years=(2000,), vintage=2000, amounts=(100,)), source.annual,
        reference_year=2001, first_model_period=2001)
    assert result.curves.fraction.tolist() == [1.0, 0.1, 0.0008, 0.0004, 0.0]
    assert result.curves.period.max() == 2004
    assert result.tail_losses.empty
    assert result.audit.required_minimum_survival.item() == 0.0001
    assert result.audit.removed_stock_years_k_vehicles.item() == 0


def test_grouping_preserves_plateau_at_exact_temoa_minimum():
    result = build_composite_cohorts(
        acquisitions(years=(2000,), vintage=2000, amounts=(100,)),
        schedule([1.0, 0.001, 0.0001, 0.0001, 0.0]),
        reference_year=2001, first_model_period=2001)
    assert result.curves.fraction.tolist() == [1.0, 0.001, 0.0001, 0.0001, 0.0]
    assert result.curves.period.max() == 2004
    assert result.tail_losses.empty
    assert result.audit.minimum_interior_fraction.item() == 0.0001


def test_missing_source_year_and_existing_future_overlap_fail():
    with pytest.raises(ValueError, match="complete, unique"):
        reconcile(schedule([1.0, 0.5, 0.0]).drop(index=1))
    with pytest.raises(ValueError, match="Historical model vintages"):
        build_composite_cohorts(acquisitions(vintage=2020), schedule([1.0, 0.5, 0.0]),
                                reference_year=2015, first_model_period=2020)
