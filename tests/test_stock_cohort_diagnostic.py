"""Diagnostic quantity boundaries and independent region/class reconciliation."""

import pandas as pd
import pytest

from validation.stock_cohort_diagnostic import (
    cohort_lineage,
    comparison_lineage,
    stock_age_comparison,
    historical_retention,
    reference_survival_comparison,
    retirement_projection,
    summarize_annual_ledgers,
)


@pytest.fixture
def annual():
    rows = []
    for profile in ["moves", "nhtsa_nems"]:
        for region, scale in [("ON", 1.), ("QC", 2.)]:
            for vintage, year, sales, survivors, allocated in [
                (2000, 2001, 100., 50., 0.),
                (2005, 2006, 200., 160., 252.),
            ]:
                rows.append({
                    "profile": profile, "region": region, "road_class": "cars",
                    "vintage": vintage, "vintage_year": year,
                    "sales_k_vehicles": sales * scale, "surviving_k_vehicles": survivors * scale,
                    "calibrated_surviving_k_vehicles": survivors * scale * 1.2,
                    "calibrated_acquisitions_k_vehicles": sales * scale * 1.2,
                    "curve_solver_capacity_k_vehicles": sales * scale * 1.2,
                    "fixed_allocated_k_vehicles": allocated * scale,
                    "stock_calibration_multiplier": 1.2, "ceud_stock_k_vehicles": 252. * scale,
                })
    return pd.DataFrame(rows)


def test_redistribution_is_cumulative_within_each_region_and_source(annual):
    original = annual.copy(deep=True)
    result = summarize_annual_ledgers(annual)
    vintage = result["vintage"]
    groups = ["profile", "region", "road_class"]
    assert vintage.groupby(groups).cumulative_redistribution_k_vehicles.last().eq(0).all()
    assert vintage.loc[vintage.region.eq("ON")].redistributed_k_vehicles.tolist() == [-60., 60., -60., 60.]
    assert result["calibration"].stock_residual_k_vehicles.abs().max() < 1e-12
    assert result["calibration"].multiplier.eq(1.2).all()
    assert vintage.groupby(groups).cumulative_sales_k_vehicles.last().tolist() == [300., 600., 300., 600.]
    pd.testing.assert_frame_equal(annual, original)


def test_paired_native_inputs_never_alias_survivors_as_acquisitions(annual):
    selected = annual.loc[annual.profile.eq("moves") & annual.region.eq("ON")]
    native = cohort_lineage(selected)
    native_totals = native.groupby("method").vehicles_k.sum()
    assert native_totals["Installed-equivalent acquisitions"] == 360.
    assert native_totals["Physical survivors"] == 252.
    assert native.loc[native.method.eq("Installed-equivalent acquisitions"), "basis"].eq("Installed-equivalent acquisitions").all()
    assert native.loc[native.method.eq("Physical survivors"), "vehicles_k"].tolist() == [60., 192.]


def test_right_labelled_bins_use_the_kernel_installation_basis(annual):
    annual["curve_solver_capacity_k_vehicles"] = annual.calibrated_acquisitions_k_vehicles * .9
    native = cohort_lineage(annual)
    assert native.loc[native.method.eq("Installed-equivalent acquisitions"), "vehicles_k"].sum() == pytest.approx(
        annual.curve_solver_capacity_k_vehicles.sum())


@pytest.mark.parametrize("column", ["calibrated_acquisitions_k_vehicles", "calibrated_surviving_k_vehicles", "ceud_stock_k_vehicles"])
def test_inconsistent_calibration_cannot_be_presented_as_closed(annual, column):
    annual.loc[0, column] += 1.
    with pytest.raises(ValueError, match="calibration|Calibration|target"):
        summarize_annual_ledgers(annual)


def test_projection_preserves_original_cohorts_without_future_recalibration():
    acquisitions = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "vintage_year": 2000, "calibrated_acquisitions_k_vehicles": 100.},
        {"region": "ON", "road_class": "cars", "vintage_year": 2002, "calibrated_acquisitions_k_vehicles": 200.},
    ])
    curve = pd.DataFrame({"road_class": ["cars"] * 5, "age": range(5), "survival_probability": [1., .8, .5, .2, 0.]})
    result = historical_retention(acquisitions, curve, years=[2002, 2004, 2006]).set_index("calendar_year")
    assert result.loc[2002, "retained_k_vehicles"] == 250.
    assert result.loc[2004, "retained_k_vehicles"] == 100.
    assert result.loc[2006, "retained_k_vehicles"] == 0.


def test_unknown_tail_is_not_silently_filled_with_zero():
    acquisitions = pd.DataFrame([{"region": "ON", "road_class": "cars", "vintage_year": 2000,
                                  "calibrated_acquisitions_k_vehicles": 100.}])
    curve = pd.DataFrame({"road_class": ["cars", "cars"], "age": [0, 1], "survival_probability": [1., .5]})
    with pytest.raises(ValueError, match="terminal zero"):
        historical_retention(acquisitions, curve, years=[2003])


def test_historical_projection_uses_the_configured_half_percent_schedule():
    from parameterization.road_cohort_survival import reconcile_survival_schedules

    native = pd.DataFrame({"road_class": ["cars"] * 2, "age": [0, 1], "survival_probability": [1., .4]})
    schedule = reconcile_survival_schedules(native, tail_derivation="constant_retirement", threshold=.005).annual
    acquisitions = pd.DataFrame([{"region": "ON", "road_class": "cars", "vintage_year": 2000,
                                  "calibrated_acquisitions_k_vehicles": 100.}])
    result = historical_retention(acquisitions, schedule, years=[2006, 2007]).set_index("calendar_year")
    assert result.loc[2006, "retained_k_vehicles"] == pytest.approx(.4096)
    assert result.loc[2007, "retained_k_vehicles"] == 0.


def test_model_vintage_does_not_apply_the_source_threshold_a_second_time():
    from parameterization.road_cohort_survival import build_composite_cohorts, reconcile_survival_schedules

    native = pd.DataFrame({"road_class": ["cars"] * 4, "age": [0, 1, 2, 3],
                           "survival_probability": [1., .1, .0008, .0004]})
    schedule = reconcile_survival_schedules(native, tail_derivation="constant_retirement", threshold=.005).annual
    acquisitions = pd.DataFrame([{"region": "ON", "road_class": "cars", "tech": "car", "vintage": 2000,
                                  "vintage_year": 2000, "calibrated_acquisitions_k_vehicles": 100.}])
    combined = build_composite_cohorts(acquisitions, schedule, reference_year=2000, first_model_period=2001)
    result = retirement_projection(acquisitions, native, schedule, combined.tail_losses, years=[2002, 2003])
    assert result.retained_k_vehicles.tolist() == pytest.approx([.08, .04])
    assert result.native_difference_k_vehicles.eq(0).all()
    assert result.removed_k_vehicles.eq(0).all()


def test_native_support_remains_unknown_and_production_tail_losses_remain_explicit():
    acquisitions = pd.DataFrame([{"region": "ON", "road_class": "cars", "vintage_year": 2000,
                                  "calibrated_acquisitions_k_vehicles": 100.}])
    native = pd.DataFrame({"road_class": ["cars"] * 2, "age": [0, 1], "survival_probability": [1., .5]})
    production = pd.DataFrame({"road_class": ["cars"] * 4, "age": [0, 1, 2, 3], "survival_probability": [1., .5, .25, 0.]})
    losses = pd.DataFrame([{"region": "ON", "road_class": "cars", "period": 2002, "removed_k_vehicles": 5.}])
    result = retirement_projection(acquisitions, native, production, losses, years=[2001, 2002]).set_index("calendar_year")
    assert result.loc[2001, "native_difference_k_vehicles"] == 0.
    assert result.loc[2002, "source_survivors_k_vehicles"] == 25.
    assert result.loc[2002, "retained_k_vehicles"] == 20.
    assert result.loc[2002, "removed_k_vehicles"] == 5.
    assert not result.loc[2002, "native_complete"]
    assert pd.isna(result.loc[2002, "native_survivors_k_vehicles"])
    assert pd.isna(result.loc[2002, "native_difference_k_vehicles"])


def test_reference_calibration_comparison_exposes_shape_changes_despite_equal_stock():
    sales = pd.DataFrame({"ceud_region": ["ON"] * 2, "road_class": ["cars"] * 2, "age": [1, 0],
                          "sales_k_vehicles": [100., 100.], "surviving_k_vehicles": [50., 100.],
                          "calibrated_surviving_k_vehicles": [50., 100.], "stock_calibration_multiplier": [1., 1.],
                          "ceud_stock_k_vehicles": [150., 150.]})
    native = pd.DataFrame({"road_class": ["cars"] * 2, "age": [0, 1], "survival_probability": [1., .25]})
    result = reference_survival_comparison(sales, native).iloc[0]
    assert result.native_raw_survivors_k_vehicles == 125.
    assert result.production_raw_survivors_k_vehicles == 150.
    assert result.native_multiplier == 1.2
    assert result.production_multiplier == 1.
    assert result.max_cohort_stock_difference_k_vehicles == 20.


def test_restored_stock_age_comparison_keeps_survivors_distinct(annual):
    from validation.config_models import ScenarioPeriods

    periods = ScenarioPeriods(period_mode="prospective", base_year=2025,
                              existing=[2000, 2005, 2010, 2015, 2020],
                              model=[2025, 2030], step=5)
    mapped = pd.DataFrame({"mapping_accepted": [True, True, True, False],
                           "report_year": 2025, "MODEL_YEAR": [1999, 2005, 2020, 2020],
                           "FIT_ACTIVE": [900., 1., 3., 900.],
                           "nrcan_ceud_class": "Car"})
    report5 = pd.DataFrame({"VEHICLE_CLASS": ["COMMERCIAL"], "year": [2025],
                            "AGE": [5], "AGE_DIST": [1.]})
    selected = annual.loc[annual.profile.eq("moves") & annual.region.eq("ON")]
    targets = selected[["region", "road_class", "ceud_stock_k_vehicles"]].drop_duplicates()
    stock = stock_age_comparison(mapped, report5, targets, periods=periods, report_year=2025)
    assert stock.vehicles_k.tolist() == [189., 63.]
    # Out-of-history and unresolved rows cannot consume the observed-stock total.
    assert stock.vehicles_k.sum() == 252.
    lineage = comparison_lineage(selected, stock, physical_survivors=False)
    assert lineage.groupby("method").vehicles_k.sum().to_dict() == {
        "Sales cohorts": 360., "Deprecated MTO age allocation": 252.}
    physical = comparison_lineage(selected, stock, physical_survivors=True)
    assert physical.groupby("method").vehicles_k.sum().eq(252.).all()
    # The chart may load every province, but an Ontario selection must retain
    # only Ontario's stock-age comparator and its own class-stock denominator.
    all_regions = pd.concat([stock, stock.assign(region="QC", vehicles_k=stock.vehicles_k * 100)])
    regional = comparison_lineage(selected, all_regions, physical_survivors=True)
    assert set(regional.region) == {"ON"}
    assert regional.groupby("method").vehicles_k.sum().eq(252.).all()
