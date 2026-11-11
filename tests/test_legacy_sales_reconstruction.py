"""Independent legacy workbook reconstruction, representative survival and accounting."""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from utils import file_sha256
from validation.legacy_sales_reconstruction import (
    _inspected_recipe, _recipe_functions, _validate_legacy_physical, reconstruct_legacy_sales,
)


ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "legacy_backend/legacy_variant_sales_cohorts"
NOTEBOOK = DIRECTORY / "Update data_20260618_fixed lifetime (1).ipynb"
INHERITED = DIRECTORY / "canoe_ON_16d.sqlite"


@pytest.fixture(scope="module")
def replay():
    paths = [NOTEBOOK, INHERITED, *DIRECTORY.glob("*.xlsx")]
    before = {path: file_sha256(path) for path in paths}
    frames, metadata = reconstruct_legacy_sales(NOTEBOOK, INHERITED, comparison_year=2025, regions=["ON"])
    assert before == {path: file_sha256(path) for path in paths}
    return frames, metadata


def test_active_recipe_and_input_substitution_are_explicit(replay):
    frames, metadata = replay
    assert metadata["active_arguments"]["fixed_lifetime"] == "False"
    assert metadata["active_arguments"]["growth_rate_type"] == "'gdp'"
    assert metadata["calibration_year"] == 2023
    assert metadata["comparison_year"] == 2025
    assert not metadata["original_inherited_input_available"]
    assert not metadata["original_input_identity_matches"]
    assert "historical equivalence" in metadata["evidence_gap"]
    assert metadata["inherited_database"]["sha256"] == file_sha256(INHERITED)
    assert metadata["reconstruction_implementation_sha256"] == file_sha256(
        ROOT / "src/validation/legacy_sales_reconstruction.py")
    assert len(metadata["workbooks"]) == 5
    assert "rounded workbook" in frames["legacy_reconstruction_context"].set_index("stage").loc["Survival", "interpretation"]


def test_partial_bins_and_annual_lineage_recover_every_native_parameter(replay):
    frames, _ = replay
    annual = frames["legacy_reconstructed_annual"]
    parameters = frames["legacy_reconstructed_parameters"]
    keys = ["region", "tech", "vintage"]
    actual = annual.groupby(keys)[["raw_capacity_k_vehicles", "surviving_k_vehicles"]].sum().sort_index()
    expected = parameters.set_index(keys)[actual.columns].sort_index()
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=1e-12, atol=1e-9)
    assert set(annual.loc[annual.vintage.eq(2000), "vintage_year"]) <= {2005}
    assert set(annual.loc[annual.vintage.eq(2005), "vintage_year"]) == set(range(2006, 2011))
    assert set(annual.loc[annual.vintage.eq(2010), "vintage_year"]) == set(range(2011, 2016))
    assert set(annual.loc[annual.vintage.eq(2020), "vintage_year"]) == set(range(2021, 2026))
    assert set(annual.annual_position) == {1, 2, 3, 4, 5}
    assert annual.loc[annual.vintage_year.gt(2023), "observation_status"].eq("2023 product held constant").all()
    # Missing car powertrain shares make those native products zero; other classes retain2005.
    assert annual.loc[annual.vintage_year.eq(2005) & annual.road_class.eq("cars"), "raw_capacity_k_vehicles"].sum() == 0
    assert annual.loc[annual.vintage_year.eq(2005), "raw_capacity_k_vehicles"].sum() > 0
    assert annual.raw_capacity_k_vehicles.sum() == pytest.approx(13604.481700, abs=1e-5)


def test_native_partial_bin_sums_available_years_and_preserves_exact_endpoints():
    functions, _ = _inspected_recipe(NOTEBOOK)
    namespace, captures = _recipe_functions(functions)
    volumes = {2010: 10., 2011: 11., 2013: 2., 2014: 3., 2015: 4., 2016: 16.}
    sales = pd.DataFrame([{"Region": "ON", "Mode": "Cars", "Unit": "k units", **volumes}])
    shares = pd.DataFrame([
        {"Region": "ON", "Mode": "Cars", "Powertrain": fuel,
         **{year: fraction for year in volumes}}
        for fuel, fraction in [("Gasoline", .75), ("Battery electric", .25)]
    ])
    parameters, _ = namespace["compile_data_existing_capacity"](sales, shares, np.arange(2013, 2016))
    annual = captures["annual"]
    assert set(annual.Year) == {2013, 2014, 2015}
    assert set(annual.vintage) == {2010}
    assert parameters.vintage.eq(2010).all()
    for fuel, fraction in [("T_LDV_C_GSL", .75), ("T_LDV_C_BEV150", .25)]:
        actual = parameters.loc[parameters.tech.eq(fuel), "capacity"].item()
        assert actual == pytest.approx(sum(volumes[year] * fraction for year in [2013, 2014, 2015]))
    assert parameters.capacity.sum() == pytest.approx(sum(volumes[year] for year in [2013, 2014, 2015]))
    # The native stock compiler has only raw acquisition arithmetic at this stage.
    assert "fraction" not in annual
    namespace["compile_data_existing_capacity"](sales, shares, [2010, 2011, 2015, 2016])
    endpoints = captures["annual"][["Year", "vintage"]].drop_duplicates().set_index("Year").vintage.to_dict()
    assert endpoints == {2010: 2005., 2011: 2010., 2015: 2010., 2016: 2015.}


def test_survival_is_applied_after_aggregation_at_representative_vintage_once(replay):
    frames, _ = replay
    annual = frames["legacy_reconstructed_annual"]
    curves = pd.read_excel(DIRECTORY / "survival_curve_transport.xlsx").set_index("Age")
    cars = annual.loc[annual.road_class.eq("cars") & annual.vintage.eq(2010)]
    assert set(cars.vintage_year) == set(range(2011, 2016))
    assert cars.baseline_survival_fraction.eq(round(curves.loc[15, "Cars"], 2)).all()
    assert not all(round(curves.loc[2025 - row.vintage_year, "Cars"], 2) == row.baseline_survival_fraction
                   for row in cars.itertuples())
    np.testing.assert_allclose(annual.surviving_k_vehicles,
                               annual.raw_capacity_k_vehicles * annual.baseline_survival_fraction)
    totals = frames["legacy_reconstructed_totals"].set_index("road_class")
    assert totals.loc["cars", "surviving_k_vehicles"] == pytest.approx(2504.501640, abs=1e-6)
    assert totals.loc["heavy_trucks", "surviving_k_vehicles"] == pytest.approx(173.030930, abs=1e-6)
    assert totals.surviving_k_vehicles.sum() < totals.raw_capacity_k_vehicles.sum()


def test_uf_calibrates_2023_activity_with_own_stock_and_c2a_and_is_bounded(replay):
    frames, _ = replay
    calibration = frames["legacy_reconstructed_utilization"]
    detail = frames["legacy_reconstructed_parameters"]
    np.testing.assert_allclose(calibration.utilization * calibration.capacity_activity_denominator_bn,
                               calibration.calibration_activity_bn, rtol=1e-12, atol=1e-9)
    np.testing.assert_allclose(calibration.lower_bound, calibration.utilization * .975)
    np.testing.assert_allclose(calibration.upper_bound, calibration.utilization * 1.025)
    for row in calibration.itertuples():
        group = detail.loc[detail.region.eq(row.region) & detail.road_class.eq(row.road_class)]
        assert row.capacity_activity_denominator_bn == pytest.approx(
            (group.raw_capacity_k_vehicles * group.calibration_survival_fraction * group.c2a).sum())
        assert group.implied_activity_bn.sum() < row.calibration_activity_bn
    assert calibration.set_index("road_class").loc["cars", "c2a"] == 1.3


def test_service_energy_use_own_parameters_and_reconcile_class_totals(replay):
    frames, _ = replay
    detail = frames["legacy_reconstructed_energy_detail"]
    np.testing.assert_allclose(detail.implied_activity_bn,
                               detail.surviving_k_vehicles * detail.c2a * detail.utilization)
    np.testing.assert_allclose(detail.implied_vehicle_input_pj,
                               detail.implied_activity_bn / detail.efficiency)
    actual = detail.groupby(["region", "road_class"])[[
        "surviving_k_vehicles", "implied_activity_bn", "implied_vehicle_input_pj",
    ]].sum().sort_index()
    expected = frames["legacy_reconstructed_totals"].set_index(["region", "road_class"])[actual.columns].sort_index()
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.loc[("ON", "cars"), "implied_vehicle_input_pj"] == pytest.approx(138.106239, abs=1e-6)
    # One service-production edge per process; upstream blend production is not summed again.
    assert not detail.duplicated(["region", "tech", "vintage"]).any()
    assert detail.output_comm.str.startswith("T_D_").all()


@pytest.mark.parametrize("defect", ["fixed_lifetime", "survival_body", "capacity_body"])
def test_executable_drift_requires_new_inspection(tmp_path, defect):
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    for cell in notebook["cells"]:
        source = "".join(cell.get("source", []))
        if defect == "fixed_lifetime" and "main_compile_data_transport(canoe_default_all =" in source:
            source = source.replace("fixed_lifetime = False", "fixed_lifetime = True")
        elif defect == "survival_body" and source.startswith("def compile_data_survival_lifetime_transport"):
            source = source.replace(".round(2)", ".round(3)")
        elif defect == "capacity_body" and source.startswith("def compile_data_existing_capacity"):
            source = source.replace("2006 <= y <= 2010", "2006 <= y < 2010")
        cell["source"] = source.splitlines(keepends=True)
    altered = tmp_path / "altered.ipynb"
    altered.write_text(json.dumps(notebook), encoding="utf-8")
    with pytest.raises(ValueError, match="changed"):
        _inspected_recipe(altered)


@pytest.fixture
def physical_parameters():
    return pd.DataFrame([{
        "units": "k units", "raw_capacity_k_vehicles": 10., "surviving_k_vehicles": 5.,
        "c2a": 1.3, "utilization": .02, "efficiency": .5,
        "calibration_survival_fraction": .6, "baseline_survival_fraction": .5,
        "lower_bound": .0195, "upper_bound": .0205,
    }])


@pytest.mark.parametrize("column,value", [
    ("units", "vehicles"), ("calibration_survival_fraction", 1.1),
    ("baseline_survival_fraction", -.1), ("baseline_survival_fraction", np.inf),
    ("c2a", 0.), ("efficiency", -.1), ("lower_bound", np.nan),
    ("upper_bound", np.inf), ("lower_bound", .025), ("upper_bound", .01),
])
def test_physical_support_rejects_wrong_units_survival_and_bounds(physical_parameters, column, value):
    physical_parameters.loc[0, column] = value
    with pytest.raises(ValueError, match="Legacy|legacy"):
        _validate_legacy_physical(physical_parameters)


def test_native_uf_is_not_capped_by_diagnostic_validation(physical_parameters):
    physical_parameters[["utilization", "lower_bound", "upper_bound"]] *= 100
    original = physical_parameters.copy(deep=True)
    _validate_legacy_physical(physical_parameters)
    pd.testing.assert_frame_equal(physical_parameters, original)
