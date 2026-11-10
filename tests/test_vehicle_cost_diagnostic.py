"""Vehicle cost evidence preserves money, empirical inputs and range denominators."""

from dataclasses import replace
import json
from math import exp

import pandas as pd
import pytest

from parameterization.currency import CerCurrencyConverter
from utils import file_sha256, load_config_bundle
from validation.vehicle_cost_diagnostic import (
    FIGURES, VehicleCostDiagnostic, investment_trajectory_chart, load_diagnostic,
    maintenance_age_chart, range_cost_chart, range_cost_comparison,
    reconcile_emitted_costs, service_maintenance_chart, source_cost_frames, source_price_chart,
    source_step_audit, write_diagnostic,
)


@pytest.fixture
def source_frames():
    prices = pd.DataFrame([
        {"family": "ldv", "vehicle_class": "Compact", "powertrain": powertrain, "source_year": year,
         "source_currency": "USD", "source_dollar_year": 2022, "source_unit": "2022 USD/vehicle",
         "vehicle_price_usd_2022_per_vehicle": price, "manufacturing_cost_usd_2022_per_vehicle": price / 1.5}
        for powertrain, price in (("gasoline", 30000), ("bev_150_miles", 45000)) for year in (2025, 2030)
    ])
    burnham = {
        "baseline": pd.DataFrame({"year_index": [0, 14], "baseline_repair_cost($/mi)": [.01, .10]}),
        "class_multipliers": pd.DataFrame({"vehicle_class": ["Compact"], "class_multiplier": [1.0]}),
        "powertrain_multipliers": pd.DataFrame({"vehicle_powertrain": ["Gasoline", "Battery Electric"], "powertrain_multiplier": [1., .5]}),
        "maintenance": pd.DataFrame({"vehicle_powertrain": ["Gasoline", "Battery Electric"], "maintenance_cost($/mi)": [.05, .02]}),
    }
    bean = pd.DataFrame({"vehicle_class": ["BoxMedium 4"] * 3, "vehicle_powertrain": ["Conv"] * 3,
                         "coefficient_key": ["coefficient_a", "coefficient_b", "maintenance_powertrain_multiplier"],
                         "value": [.09, .2618, 1.]})
    macro = pd.DataFrame([
        {"scenario": "Current Measures", "region": "Canada", "variable_key": key, "year": year, "value": value, "unit": unit}
        for year, fx, deflator in [(2020, 1.3, 100.), (2022, 1.4, 120.)]
        for key, value, unit in (("cad_per_usd", fx, "C$/US$"), ("gdp_deflator", deflator, "2020=100"))
    ])
    converter = CerCurrencyConverter(macro, scenario="Current Measures", target_year=2020)
    rules = {"ldv_powertrain_evidence": {"gasoline": "Gasoline", "bev_150_miles": "Battery Electric"},
             "ldv_repair_price_exponent_per_usd": .00002, "ldv_repair_max_age": 14}
    return source_cost_frames(prices, burnham=burnham, bean=bean, converter=converter, rules=rules,
                              ages=[0, 14, 15], burnham_money=("USD", 2020), bean_money=("USD", 2020))


def test_source_curve_preserves_retail_msrp_and_money_basis(source_frames):
    prices, costs = source_frames
    row = prices.loc[prices.powertrain.eq("gasoline")].iloc[0]
    assert row.manufacturing_native == 20000
    assert row.manufacturing_cad == pytest.approx(20000 * 1.4 * 100 / 120)
    repair = costs.loc[costs.powertrain.eq("gasoline") & costs.age.eq(14) & costs.component.eq("repair")].iloc[0]
    assert repair.cost_native == pytest.approx(.10 * exp(.00002 * 30000))
    assert repair.cost_cad == pytest.approx(repair.cost_native * 1.3)
    assert repair.msrp_dollar_year == 2022
    assert repair.source_dollar_year == 2020
    assert repair.denominator == "vehicle-mile"


def test_terminal_age_is_a_hold_and_bean_domain_is_not_invented(source_frames):
    _, costs = source_frames
    ldv = costs.loc[costs.family.eq("ldv") & costs.component.eq("total") & costs.source_year.eq(2025)]
    by_age = ldv.pivot(index="powertrain", columns="age", values="cost_native")
    pd.testing.assert_series_equal(by_age[14], by_age[15], check_names=False)
    assert ldv.loc[ldv.age.eq(15), "source_support"].str.contains("held").all()
    heavy = costs.loc[costs.family.eq("mhdv")]
    assert heavy.source_year.isna().all()
    assert heavy.source_support.str.contains("not registered").all()
    assert heavy.loc[heavy.age.eq(14), "cost_native"].iloc[0] == pytest.approx(.09 * 14 + .2618)


@pytest.fixture
def range_inputs():
    costs = pd.DataFrame([
        {"region": "ON", "mode": "cars", "vintage": year, "powertrain": powertrain,
         "cost": value, "units": "$M 2020CAD / k vehicles", "purchase_evidence_year": year + 5}
        for year in (2025, 2030) for powertrain, value in (("bev_150_miles", 20.), ("bev_400_miles", 50.))
    ])
    shares = pd.DataFrame({"market_class": ["car", "car"], "powertrain": ["BEV", "BEV"],
                           "bucket": ["bev_150_miles", "bev_400_miles"], "share_of_all_sales": [.25, .75]})
    return costs, shares


def test_range_mix_preserves_cost_envelope_and_full_denominator(range_inputs):
    costs, shares = range_inputs
    result = range_cost_comparison(costs, shares, {"cars": "car"})
    representative = result.loc[result.powertrain.eq("bev_representative")]
    assert representative.cost.eq(42.5).all()
    assert representative.representation.str.contains("not an additional emitted row").all()
    with pytest.raises(ValueError, match="complete"):
        range_cost_comparison(costs.iloc[1:], shares, {"cars": "car"})
    bad = costs.copy()
    bad.loc[0, "units"] = "$M 2020CAD / bn passenger-km"
    with pytest.raises(ValueError, match="matching units"):
        range_cost_comparison(bad, shares, {"cars": "car"})


@pytest.fixture
def diagnostic(source_frames, range_inputs):
    prices, maintenance = source_frames
    investments, shares = range_inputs
    investments = investments.assign(tech=investments.powertrain + "_N")
    service = pd.DataFrame([
        {"region": "ON", "mode": "cars", "tech": "gasoline_N", "powertrain": "gasoline",
         "vintage": 2025, "period": 2025 + age, "age": age, "cost": 100. + age,
         "units": "$M 2020CAD / bn passenger-km"} for age in (0, 5)
    ])
    return VehicleCostDiagnostic({
        "source_investment": prices, "source_maintenance": maintenance,
        "investment": investments, "maintenance": service,
        "range_comparison": range_cost_comparison(investments, shares, {"cars": "car"}),
    }, {"trajectory": "Mid", "repair_terminal_age": 14, "inputs": {},
        "scenario": {"BEV_PHEV_range_representation": {"mode": "new_capacity_shares"}}})


def test_figures_have_visible_stable_id_context_and_units(diagnostic):
    figures = [source_price_chart(diagnostic), investment_trajectory_chart(diagnostic),
               maintenance_age_chart(diagnostic), service_maintenance_chart(diagnostic), range_cost_chart(diagnostic)]
    for expected_id, chart in zip(FIGURES, figures, strict=True):
        spec = chart.to_dict(validate=True)
        assert spec["usermeta"]["figure_id"] == expected_id
        assert f"Figure {expected_id}" in spec["title"]["subtitle"]
        assert len(spec["title"]["subtitle"]) >= 3
        assert "CAD" in spec["encoding"]["y"]["title"]
        assert spec["encoding"]["y"]["scale"]["zero"] is True
        assert spec["encoding"]["detail"]["field"] == "series"


def test_source_steps_do_not_bridge_classes_or_powertrains(source_frames):
    prices, _ = source_frames
    steps = source_step_audit(prices)
    assert len(steps) == 2
    assert steps.relative_change.eq(0).all()
    assert steps.previous_source_year.eq(2025).all()


def test_emitted_reconciliation_rejects_changed_values_and_unexplained_coverage():
    rows = pd.DataFrame({"region": ["ON"], "tech": ["T_LDV_C_GSL_N"], "vintage": [2025],
                         "mode": ["cars"], "cost": [30.], "units": ["$M 2020CAD / k vehicles"]})
    assert reconcile_emitted_costs(rows, rows) == 1
    with pytest.raises(ValueError, match="disagrees"):
        reconcile_emitted_costs(rows, rows.assign(cost=31.))
    with pytest.raises(ValueError, match="missing emitted"):
        reconcile_emitted_costs(pd.concat([rows, rows.assign(tech="T_LDV_C_HEV_N")]), rows)


def test_recorded_capture_is_self_contained_and_rejects_tampering(tmp_path, diagnostic):
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml")
    routes = dict(bundle.paths.artifacts)
    routes["vehicle_cost_diagnostic"] = routes["vehicle_cost_diagnostic"].model_copy(update={"path": str(tmp_path / "costs")})
    isolated = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}))
    directory = write_diagnostic(diagnostic, isolated)
    loaded = load_diagnostic(directory=directory)
    assert loaded.metadata["capture_id"].startswith("validation_baseline-")
    pd.testing.assert_frame_equal(loaded.frames["investment"], diagnostic.frames["investment"], check_dtype=False)
    manifest = json.loads((directory / "manifest.json").read_text())
    frame = directory / manifest["frames"]["investment"]["path"]
    frame.write_text(frame.read_text() + "\n")
    with pytest.raises(ValueError, match="frame hash changed"):
        load_diagnostic(directory=directory)


def test_loader_rejects_escaping_frame_path(tmp_path):
    outside = tmp_path / "outside.csv"
    outside.write_text("value\n1\n")
    directory = tmp_path / "capture"
    directory.mkdir()
    (directory / "manifest.json").write_text(json.dumps({"inputs": {}, "frames": {
        "invalid": {"path": "../outside.csv", "sha256": file_sha256(outside), "rows": 1}}}))
    with pytest.raises(ValueError, match="escapes"):
        load_diagnostic(directory=directory)
