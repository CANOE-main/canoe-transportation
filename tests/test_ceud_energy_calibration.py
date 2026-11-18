"""Comparable-energy fitting, preserved representation and rejected boundaries."""

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from parameterization.ceud_energy_calibration import calibrate_consumption
from utils import load_config_bundle, load_harmonization_rules
from validation.config_models import ScenarioConfig


def test_calibration_comparison_requires_matching_partition_fingerprints():
    from validation.ceud_calibration_diagnostic import _require_matching_configuration

    paths = {"path": "config/paths.yaml", "sha256": "paths"}
    sources = {"path": "config/sources.yaml", "sha256": "sources"}
    rules = {"path": "config/parameters/rules.yaml", "sha256": "index"}
    member = {"path": "config/parameters/source_normalization.yaml", "sha256": "member"}
    publication = {"paths": paths, "sources": sources, "parameters": [rules, member]}
    capture = {
        "scenario": {"path": "config/scenario.yaml", "sha256": "calibrated"},
        "paths": paths,
        "sources": sources,
        "rules": rules,
        "rules:source_normalization.yaml": member,
    }

    _require_matching_configuration(publication, capture)
    with pytest.raises(ValueError, match="parameter/source_normalization.yaml"):
        _require_matching_configuration(publication, {
            **capture, "rules:source_normalization.yaml": {**member, "sha256": "changed"},
        })
    del capture["rules:source_normalization.yaml"]
    with pytest.raises(ValueError, match="parameter/source_normalization.yaml"):
        _require_matching_configuration(publication, capture)

@pytest.fixture
def calibration_inputs():
    bundle = load_config_bundle("config/scenarios/validation_energy_calibration.yaml")
    rules = deepcopy(load_harmonization_rules(bundle, "ceud_energy_calibration"))
    rules["classes"] = {"cars": rules["classes"]["cars"]}
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")
    efficiencies = load_harmonization_rules(bundle, "efficiencies")
    blend = efficiencies["existing_phev_blends"]["cars"]
    techs = ["T_LDV_C_GSL_EX", "T_LDV_C_GSL_PHEV_EX", "T_LDV_C_BEV_EX"]
    powertrains = ["gasoline", "phev", "bev"]
    inputs = ["T_gsl", blend["output"], "T_elc_ldv"]
    output = road["demand"]["activity_series"]["cars"]["commodity"]
    periods = pd.DataFrame([
        {"region": "ON", "tech": tech, "vintage": 2020, "mode": "cars", "kind": "road",
         "powertrain": powertrain, "input_comm": fuel, "output_comm": output,
         "units": "bn passenger-km/PJ", "native_unit": "MJ/vkm", "load_factor": 1.,
         "native_intensity": 5., "efficiency": .2, "components": ()}
        for tech, powertrain, fuel in zip(techs, powertrains, inputs, strict=True)
    ])
    future = periods.iloc[[0]].assign(tech="T_LDV_C_GSL_N", vintage=2030, native_intensity=3., efficiency=1/3)
    periods = pd.concat([periods, future], ignore_index=True)
    physical = pd.DataFrame({"region": "ON", "tech": techs, "vintage": 2020,
                             "surviving_k_vehicles": [5., 3., 2.], "base_year": 2025, "units": "k vehicles"})
    c2a = pd.DataFrame({"region": "ON", "tech": techs, "c2a": .1})
    factors = pd.DataFrame({"region": "ON", "tech_or_group": techs, "vintage": 2020,
                            "output_comm": output, "operator": "e", "factor": .5})
    spec = rules["classes"]["cars"]
    selectors = [
        (road["existing_capacity"]["ceud_stock_series"]["cars"]["table_id"], road["existing_capacity"]["ceud_stock_series"]["cars"]["raw_series"], "thousands", 10.),
        (20, road["demand"]["activity_series"]["cars"]["raw_series"], "millions", 500.),
        (20, spec["total_series"], "PJ", 3.6),
        (20, spec["liquid_series"][0], "PJ", 3.),
        (20, spec["liquid_series"][1], "PJ", .4),
        (20, spec["excluded_series"][0], "PJ", .05),
        (20, spec["excluded_series"][1], "PJ", .15),
    ]
    ceud = pd.DataFrame([
        {"region": "ON", "year": 2025, "table_id": table, "raw_series": series, "unit": unit,
         "value": value, "source_id": "nrcan_ceud_transport_provincial",
         "ceud_observed_year": 2023, "ceud_value_status": "constant"}
        for table, series, unit, value in selectors
    ])
    split = pd.DataFrame({"region": ["ON", "ON"], "blend": ["existing_cars"] * 2,
                          "year": [2030, 2035], "electricity_share": [.3, .5]})
    return dict(annual=periods.copy(), periods=periods, payloads=periods.to_dict("records"),
                physical_stock=physical, c2a=c2a, factors=factors, split_audit=split, ceud=ceud,
                activity_targets=pd.DataFrame([{"region": "ON", "road_class": "cars",
                                                "base_year": 2025, "target_activity_bn": .5}]),
                relationships=periods[["tech", "powertrain"]], base_year=2025, source_edition=2023,
                regions=["ON"], calibration_rules=rules, stock_rules=road["existing_capacity"],
                demand_rules=road["demand"], efficiency_rules=efficiencies)


def test_fit_uses_physical_activity_and_emitted_phev_liquid_fraction(calibration_inputs):
    inputs = calibration_inputs
    originals = {key: value.copy(deep=True) for key, value in inputs.items() if isinstance(value, pd.DataFrame)}
    result = calibrate_consumption(**inputs)
    row = result.summary.iloc[0]
    # 5*.1*.5/.2 + 3*.1*.5/.2*(1 - mean(.3,.5)) = 1.7 PJ.
    assert row.unadjusted_liquid_energy_pj == pytest.approx(1.7)
    assert row.consumption_multiplier == pytest.approx(2.)
    assert row.calibrated_liquid_energy_pj == pytest.approx(3.4)
    assert row.liquid_residual_pj == pytest.approx(0.)
    assert row.activity_bn == pytest.approx(.5)
    assert row.excluded_reported_energy_pj == pytest.approx(.2)
    assert row.electricity_target_status == "unobserved_not_zero"
    assert row.calibrated_electric_energy_pj == pytest.approx(row.unadjusted_electric_energy_pj * 2)
    assert row.full_class_residual_pj != pytest.approx(0.)
    assert np.allclose(result.periods.efficiency, result.periods.load_factor / result.periods.native_intensity)
    for key, original in originals.items():
        pd.testing.assert_frame_equal(inputs[key], original)


def test_unrelated_two_input_blends_do_not_duplicate_vehicle_relationships(calibration_inputs):
    calibration_inputs["relationships"] = pd.concat([
        calibration_inputs["relationships"],
        pd.DataFrame({"tech": ["unit_blend", "unit_blend"], "powertrain": ["fuel", "electricity"]}),
    ], ignore_index=True)
    result = calibrate_consumption(**calibration_inputs)
    assert result.summary.consumption_multiplier.iloc[0] == pytest.approx(2.)


def test_reported_source_rounding_is_separate_from_fitted_closure(calibration_inputs):
    calibration_inputs["ceud"].loc[2, "value"] -= 1e-6
    result = calibrate_consumption(**calibration_inputs)
    row = result.summary.iloc[0]
    assert row.reported_energy_balance_residual_pj == pytest.approx(-1e-6)
    assert row.source_rounding_allowance_pj == pytest.approx(2.5e-6)
    assert row.other_or_unresolved_energy_pj == 0
    assert abs(row.liquid_residual_pj) < 1e-12
    calibration_inputs["ceud"].loc[2, "value"] -= 1e-4
    with pytest.raises(ValueError, match="components exceed class total"):
        calibrate_consumption(**calibration_inputs)


def test_uniform_factor_preserves_trajectories_components_and_range_means(calibration_inputs):
    result = calibrate_consumption(**calibration_inputs)
    assert np.allclose(result.annual.native_intensity / result.annual.unadjusted_native_intensity, 2)
    assert np.allclose(result.trajectories.efficiency / result.trajectories.unadjusted_efficiency, .5)
    assert result.periods.iloc[-1].native_intensity / result.periods.iloc[0].native_intensity == pytest.approx(3/5)
    phev = result.cohorts.loc[result.cohorts.powertrain.eq("phev")].iloc[0]
    assert phev.calibrated_liquid_energy_pj / phev.calibrated_vehicle_energy_pj == pytest.approx(.6)
    assert phev.calibrated_electric_energy_pj / phev.calibrated_vehicle_energy_pj == pytest.approx(.4)
    # A common k commutes with cohort/range means; occupancy/payload is unchanged.
    for weights in ([.2, .3, .5], [.7, .2, .1]):
        assert np.average(np.array([4., 6., 8.]) * 2, weights=weights) == pytest.approx(2 * np.average([4., 6., 8.], weights=weights))


@pytest.mark.parametrize("defect", ["target_missing", "duplicate", "units", "year", "status", "zero", "activity", "stock_basis", "split", "efficiency", "fuel_sum"])
def test_incompatible_boundary_fails_instead_of_fitting(calibration_inputs, defect):
    inputs = calibration_inputs
    ceud = inputs["ceud"]
    if defect == "target_missing":
        inputs["ceud"] = ceud.drop(index=3)
    elif defect == "duplicate":
        inputs["ceud"] = pd.concat([ceud, ceud.iloc[[3]]], ignore_index=True)
    elif defect == "units":
        ceud.loc[3, "unit"] = "TJ"
    elif defect == "year":
        ceud.loc[3, "ceud_observed_year"] = 2022
    elif defect == "status":
        ceud.loc[3, "ceud_value_status"] = "observed"
    elif defect == "zero":
        ceud.loc[[3, 4], "value"] = 0
    elif defect == "activity":
        inputs["factors"]["factor"] = .4
    elif defect == "stock_basis":
        inputs["physical_stock"]["base_year"] = 2020
    elif defect == "split":
        inputs["split_audit"]["blend"] = "unknown"
    elif defect == "fuel_sum":
        ceud.loc[5, "value"] = 1.
    else:
        inputs["payloads"][0]["efficiency"] = float("nan")
    with pytest.raises(ValueError):
        calibrate_consumption(**inputs)


def test_unsupported_class_stays_unadjusted_with_visible_missing_target(calibration_inputs):
    rules = calibration_inputs["calibration_rules"]["classes"]["cars"]
    rules.update(status="unsupported_fuel_boundary", liquid_series=[], excluded_series=[])
    result = calibrate_consumption(**calibration_inputs)
    assert result.summary.consumption_multiplier.iloc[0] == 1.
    assert result.summary.ceud_liquid_target_pj.isna().all()
    assert result.summary.liquid_residual_pj.isna().all()
    assert np.allclose(result.periods.efficiency, calibration_inputs["periods"].efficiency)


@pytest.fixture
def heavy_calibration_inputs(calibration_inputs):
    inputs = calibration_inputs
    bundle = load_config_bundle("config/scenarios/validation_energy_calibration.yaml")
    heavy = deepcopy(load_harmonization_rules(bundle, "ceud_energy_calibration")["classes"]["heavy_trucks"])
    inputs["calibration_rules"]["classes"] = {"heavy_trucks": heavy}
    existing = inputs["stock_rules"]["fuel_technology"]["heavy_trucks"]["Diesel"]
    activity = inputs["demand_rules"]["activity_series"]["heavy_trucks"]
    output = activity["commodity"]
    original = inputs["periods"].iloc[0].to_dict()
    rows = pd.DataFrame([
        {**original, "mode": "heavy_trucks", "tech": tech, "vintage": vintage,
         "powertrain": powertrain, "input_comm": fuel, "output_comm": output,
         "units": "bn tonne-km/PJ", "native_intensity": consumption, "efficiency": 1 / consumption}
        for tech, vintage, powertrain, fuel, consumption in [
            (existing, 2020, "diesel", "T_dsl", 5.),
            ("T_HDV_T_DSL_N", 2030, "diesel", "T_dsl", 3.),
            ("T_HDV_T_BEV_N", 2030, "bev", "T_elc_mhdv", 2.),
        ]
    ])
    inputs.update(
        annual=rows.copy(), periods=rows, payloads=rows.to_dict("records"),
        physical_stock=pd.DataFrame([{"region": "ON", "tech": existing, "vintage": 2020,
                                      "surviving_k_vehicles": 10., "base_year": 2025, "units": "k vehicles"}]),
        c2a=pd.DataFrame([{"region": "ON", "tech": existing, "c2a": .1}]),
        factors=pd.DataFrame([{"region": "ON", "tech_or_group": existing, "vintage": 2020,
                               "output_comm": output, "operator": "e", "factor": .5}]),
        activity_targets=pd.DataFrame([{"region": "ON", "road_class": "heavy_trucks",
                                        "base_year": 2025, "target_activity_bn": .5}]),
        relationships=rows[["tech", "powertrain"]],
    )
    stock = inputs["stock_rules"]["ceud_stock_series"]["heavy_trucks"]
    selectors = [(stock["table_id"], stock["raw_series"], "thousands", 10.),
                 (activity["table_id"], activity["raw_series"], "millions", 500.),
                 (heavy["table_id"], heavy["total_series"], "PJ", 3.6)]
    inputs["ceud"] = pd.DataFrame([
        {"region": "ON", "year": 2025, "table_id": table, "raw_series": series, "unit": unit,
         "value": value, "source_id": "nrcan_ceud_transport_provincial",
         "ceud_observed_year": 2023, "ceud_value_status": "constant"}
        for table, series, unit, value in selectors
    ])
    return inputs


@pytest.mark.parametrize("native_region,output_region", [("ON", "ON"), ("BCT", "BCT"), ("PE", "PEI")])
def test_heavy_truck_fits_reviewed_diesel_total_once_and_preserves_activity(
    heavy_calibration_inputs, native_region, output_region,
):
    inputs = heavy_calibration_inputs
    inputs["regions"] = [native_region]
    inputs["ceud"]["region"] = native_region
    for key in ("annual", "periods", "physical_stock", "c2a", "factors", "activity_targets"):
        inputs[key]["region"] = output_region
    inputs["payloads"] = inputs["periods"].to_dict("records")
    result = calibrate_consumption(**inputs)
    summary = result.summary.iloc[0]
    assert summary.ceud_region == native_region
    assert summary.region == output_region
    assert summary.status == "calibrate"
    assert summary.liquid_target_basis == "reported_diesel_total"
    assert summary.ceud_liquid_target_pj == pytest.approx(3.6)
    assert summary.ceud_liquid_target_pj == summary.ceud_total_energy_pj
    assert summary.unadjusted_liquid_energy_pj == pytest.approx(2.5)
    assert summary.consumption_multiplier == pytest.approx(1.44)
    assert summary.calibrated_liquid_energy_pj == pytest.approx(3.6)
    assert summary.reported_energy_balance_residual_pj == 0
    assert summary.source_rounding_allowance_pj == 0
    assert summary.other_or_unresolved_energy_pj == 0
    assert summary.excluded_reported_energy_pj == 0
    assert summary.full_class_residual_pj == pytest.approx(0)
    assert summary.electricity_target_status == "zero_within_confirmed_diesel_total_not_fitted"
    assert summary.activity_bn == pytest.approx(.5)
    assert summary.activity_residual_bn == pytest.approx(0)
    assert np.allclose(result.annual.native_intensity / result.annual.unadjusted_native_intensity, 1.44)
    assert np.allclose(result.trajectories.efficiency / result.trajectories.unadjusted_efficiency, 1 / 1.44)
    future = result.trajectories.loc[result.trajectories.vintage.eq(2030)]
    assert set(future.powertrain) == {"diesel", "bev"}


@pytest.mark.parametrize("defect", ["missing", "duplicate", "unit", "zero", "wrong_year", "mixed", "repeated", "excluded", "unconfirmed"])
def test_heavy_diesel_total_boundary_rejects_invalid_or_overlapping_targets(heavy_calibration_inputs, defect):
    inputs = heavy_calibration_inputs
    rules = inputs["calibration_rules"]["classes"]["heavy_trucks"]
    if defect == "missing":
        inputs["ceud"] = inputs["ceud"].iloc[:2]
    elif defect == "duplicate":
        inputs["ceud"] = pd.concat([inputs["ceud"], inputs["ceud"].iloc[[2]]], ignore_index=True)
    elif defect == "unit":
        inputs["ceud"].loc[2, "unit"] = "TJ"
    elif defect == "zero":
        inputs["ceud"].loc[2, "value"] = 0
    elif defect == "wrong_year":
        inputs["ceud"].loc[2, "ceud_observed_year"] = 2022
    elif defect == "mixed":
        rules["liquid_series"].append("separate diesel component")
    elif defect == "repeated":
        rules["liquid_series"] *= 2
    elif defect == "excluded":
        rules["excluded_series"] = ["another fuel"]
    else:
        del rules["liquid_target_basis"]
    with pytest.raises(ValueError):
        calibrate_consumption(**inputs)


@pytest.mark.parametrize("powertrain,fuel", [("gasoline", "T_gsl"), ("hev", "T_gsl"), ("bev", "T_elc_mhdv")])
def test_confirmed_diesel_total_rejects_non_diesel_existing_inputs(heavy_calibration_inputs, powertrain, fuel):
    inputs = heavy_calibration_inputs
    existing = inputs["physical_stock"].tech.iloc[0]
    inputs["relationships"].loc[inputs["relationships"].tech.eq(existing), "powertrain"] = powertrain
    inputs["payloads"][0]["input_comm"] = fuel
    with pytest.raises(ValueError, match="diesel-only existing energy inputs"):
        calibrate_consumption(**inputs)


def test_confirmed_diesel_total_accepts_diesel_hybrid_energy_inputs(heavy_calibration_inputs):
    inputs = heavy_calibration_inputs
    existing = inputs["physical_stock"].tech.iloc[0]
    inputs["relationships"].loc[inputs["relationships"].tech.eq(existing), "powertrain"] = "hev"
    result = calibrate_consumption(**inputs)
    assert result.summary.calibrated_liquid_energy_pj.iloc[0] == pytest.approx(3.6)


def test_emitted_heavy_calibration_closes_the_reported_total(heavy_calibration_inputs):
    from parameterization.ceud_energy_calibration import validate_emitted_calibration

    result = calibrate_consumption(**heavy_calibration_inputs)
    frames = {"summary": result.summary, "cohorts": result.cohorts,
              "efficiency": pd.DataFrame(result.payloads),
              "splits": pd.DataFrame(columns=["region", "period", "tech", "input_comm", "proportion"])}
    audit = validate_emitted_calibration(frames, c2a=heavy_calibration_inputs["c2a"],
                                         factors=heavy_calibration_inputs["factors"])
    assert audit["fitted_region_classes"] == 1
    assert audit["activity_unchanged"]
    assert audit["maximum_liquid_residual_pj"] == pytest.approx(0)
    frames["summary"].loc[:, "ceud_total_energy_pj"] = 5.
    with pytest.raises(ValueError, match="diesel-total boundary"):
        validate_emitted_calibration(frames)


def test_scenario_opt_in_requires_flat_emitted_activity():
    scenario = load_config_bundle("config/scenarios/validation_energy_calibration.yaml").scenario.model_dump()
    scenario["road_utilization"]["vkt_schedules"] = True
    with pytest.raises(ValueError, match="flat annual utilization"):
        ScenarioConfig.model_validate(scenario)
    scenario["road_utilization"]["vkt_schedules"] = False
    scenario["lifetimes"]["survival_curves"] = True
    with pytest.raises(ValueError, match="reference-only curve cohorts"):
        ScenarioConfig.model_validate(scenario)
    scenario["lifetimes"]["survival_curves"] = False
    scenario["efficiencies"]["ceud_energy_calibration"] = "yes"
    with pytest.raises(ValueError):
        ScenarioConfig.model_validate(scenario)
    del scenario["efficiencies"]["ceud_energy_calibration"]
    with pytest.raises(ValueError, match="ceud_energy_calibration"):
        ScenarioConfig.model_validate(scenario)


def test_energy_fit_preserves_projected_activity_instead_of_held_ceud(calibration_inputs):
    calibration_inputs["factors"]["factor"] *= 1.1
    calibration_inputs["activity_targets"]["target_activity_bn"] *= 1.1
    result = calibrate_consumption(**calibration_inputs)
    row = result.summary.iloc[0]
    assert row.ceud_activity_bn == pytest.approx(.5)
    assert row.target_activity_bn == pytest.approx(.55)
    assert row.activity_bn == pytest.approx(.55)
    assert row.activity_residual_bn == pytest.approx(0.)
    assert row.calibrated_liquid_energy_pj == pytest.approx(3.4)


@pytest.mark.parametrize("defect", ["missing", "duplicate", "wrong_year", "nonfinite"])
def test_energy_fit_rejects_incomplete_projected_activity_contract(calibration_inputs, defect):
    targets = calibration_inputs["activity_targets"]
    if defect == "missing":
        calibration_inputs["activity_targets"] = targets.iloc[:0]
    elif defect == "duplicate":
        calibration_inputs["activity_targets"] = pd.concat([targets, targets], ignore_index=True)
    elif defect == "wrong_year":
        targets["base_year"] = 2020
    else:
        targets["target_activity_bn"] = float("nan")
    with pytest.raises(ValueError, match="projected"):
        calibrate_consumption(**calibration_inputs)


def test_read_only_diagnostic_checks_emitted_rows_and_identified_charts(calibration_inputs):
    from validation.ceud_calibration_diagnostic import (
        CEUDCalibrationDiagnostic, calibration_charts, validate_calibration_evidence,
    )

    result = calibrate_consumption(**calibration_inputs)
    blend = {**calibration_inputs["efficiency_rules"]["phev_blends"]["ldv35"],
             **calibration_inputs["efficiency_rules"]["existing_phev_blends"]["cars"]}
    frames = {"summary": result.summary, "cohorts": result.cohorts,
              "trajectories": result.trajectories, "efficiency": pd.DataFrame(result.payloads),
              "splits": pd.DataFrame([{"region": "ON", "period": 2025, "tech": blend["tech"],
                                        "input_comm": blend[fuel], "proportion": share}
                                       for fuel, share in [("fuel", .6), ("electricity", .4)]])}
    validate_calibration_evidence(frames, c2a=calibration_inputs["c2a"], factors=calibration_inputs["factors"])
    changed = calibration_inputs["factors"].copy()
    changed["factor"] *= .9
    with pytest.raises(ValueError, match="activity changed after fitting"):
        validate_calibration_evidence(frames, c2a=calibration_inputs["c2a"], factors=changed)
    charts = calibration_charts(CEUDCalibrationDiagnostic(frames, {}, {}), region="ON")
    assert set(charts) == {"ceud-cal-1", "ceud-cal-4"}
    for identity, chart in charts.items():
        spec = chart.to_dict(validate=True)
        assert spec["usermeta"]["figure_id"] == identity
        assert f"Figure {identity}" in spec["title"]["subtitle"]
    frames["splits"].loc[:, "proportion"] = [.5, .5]
    with pytest.raises(ValueError, match="different PHEV liquid share"):
        validate_calibration_evidence(frames)


def test_final_comparison_checks_representatives_future_and_unchanged_edges(calibration_inputs):
    from validation.ceud_calibration_diagnostic import CEUDCalibrationDiagnostic, compare_efficiency_frames

    result = calibrate_consumption(**calibration_inputs)
    before = pd.DataFrame(calibration_inputs["payloads"])
    after = pd.DataFrame(result.payloads)
    for frame in (before, after):
        frame.loc[len(frame)] = frame.iloc[-1].to_dict() | {"tech": "T_LDV_C_BEV_REP_N"}
        frame.loc[len(frame)] = frame.iloc[-1].to_dict() | {
            "tech": "charger", "input_comm": "grid", "output_comm": "charging", "efficiency": .9, "units": "PJ/PJ"}
    diagnostic = CEUDCalibrationDiagnostic({"trajectories": result.trajectories, "summary": result.summary}, {}, {})
    report = compare_efficiency_frames(before, after, diagnostic)
    assert report["changed_rows"] == 5
    assert report["representative_rows"] == 1
    assert report["new_vintage_rows"] == 2
    assert report["maximum_factor_relative_error"] == pytest.approx(0.)
    altered = after.copy()
    altered.loc[altered.tech.eq("T_LDV_C_BEV_REP_N"), "efficiency"] *= 1.1
    with pytest.raises(ValueError, match="do not match fitted class factors"):
        compare_efficiency_frames(before, altered, diagnostic)
    after.loc[after.tech.eq("charger"), "efficiency"] *= .9
    with pytest.raises(ValueError, match="do not match fitted class factors"):
        compare_efficiency_frames(before, after, diagnostic)


def test_base_year_anchor_heatmaps_do_not_mix_historical_or_future_vintages():
    from validation.ceud_calibration_diagnostic import CEUDCalibrationDiagnostic, calibration_anchor_charts

    summary = pd.DataFrame([
        {"region": region, "road_class": mode, "base_year": 2025, "status": status}
        for region in ["ON", "QC"]
        for mode, status in [("cars", "calibrate"), ("heavy_trucks", "unsupported_fuel_boundary")]
    ])
    trajectories = pd.DataFrame([
        {"region": row.region, "mode": row.road_class, "vintage": vintage, "powertrain": fuel,
         "unadjusted_efficiency": 2., "efficiency": 2.2 if row.status == "calibrate" else 2.,
         "units": "bn passenger-km/PJ" if row.road_class == "cars" else "bn tonne-km/PJ"}
        for row in summary.itertuples() for vintage in [2020, 2025, 2030] for fuel in ["diesel", "bev"]
    ])
    charts = calibration_anchor_charts(CEUDCalibrationDiagnostic(
        {"summary": summary, "trajectories": trajectories}, {}, {}))
    assert set(charts) == {"ceud-cal-5-ldv", "ceud-cal-5-mhdv"}
    for identity, chart in charts.items():
        spec = chart.to_dict(validate=True)
        assert spec["usermeta"]["figure_id"] == identity
        data = [row for rows in spec["datasets"].values() for row in rows]
        assert {row["vintage"] for row in data} == {2025}
        assert {row["region"] for row in data} == {"ON", "QC"}
        assert all(row["cell_label"] == "pending" for row in data) if identity.endswith("mhdv") else \
               all(row["efficiency_change"] == pytest.approx(.1) for row in data)


def test_regional_capture_requires_each_region_class_and_base_year_anchor():
    from validation.ceud_calibration_diagnostic import CLASS_LABELS, require_regional_coverage

    regions = ["AB", "BCT", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK"]
    summary = pd.DataFrame([{"region": region, "road_class": mode, "base_year": 2025}
                            for region in regions for mode in CLASS_LABELS])
    trajectories = summary.rename(columns={"road_class": "mode", "base_year": "vintage"})
    frames = {"summary": summary, "trajectories": trajectories}
    require_regional_coverage(frames, expected_regions=regions)
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_regional_coverage({**frames, "summary": summary.loc[summary.region.eq("ON")]}, expected_regions=regions)
    with pytest.raises(ValueError, match="anchors are incomplete"):
        require_regional_coverage({**frames, "trajectories": trajectories.iloc[:-1]}, expected_regions=regions)


@pytest.mark.parametrize("regions", [[], "ON", [False], ["AB", False, "QC"], [""], [" ON"], ["ON", "ON"], [None]])
def test_regional_expected_regions_rejects_yaml_booleans_and_invalid_codes_before_frame_access(regions):
    from validation.ceud_calibration_diagnostic import require_regional_coverage

    with pytest.raises(ValueError, match="nonempty list of distinct region-code strings"):
        require_regional_coverage({}, expected_regions=regions)


def test_owning_regional_rules_cover_all_ten_provinces_with_string_ontario_code():
    bundle = load_config_bundle("config/scenarios/validation_energy_calibration_regions.yaml")
    rules = load_harmonization_rules(bundle, "notebook_diagnostics")
    regions = rules["ceud_calibration_regions"]["expected_regions"]
    assert all(isinstance(region, str) for region in regions)
    assert regions == ["AB", "BCT", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK"]
    assert len(regions) == len(set(regions)) == 10
    assert set(bundle.scenario.geography.regions) == set(regions)


def test_regional_coverage_respects_registered_newfoundland_and_pei_model_aliases():
    from validation.ceud_calibration_diagnostic import CLASS_LABELS, require_regional_coverage

    bundle = load_config_bundle("config/scenarios/validation_energy_calibration_regions.yaml")
    regions = load_harmonization_rules(bundle, "notebook_diagnostics")["ceud_calibration_regions"]["expected_regions"]
    mapping = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"]
    summary = pd.DataFrame([{"region": mapping.get(region, region), "road_class": mode, "base_year": 2025}
                            for region in regions for mode in CLASS_LABELS])
    assert {"NLLAB", "PEI"} <= set(summary.region)
    trajectories = summary.rename(columns={"road_class": "mode", "base_year": "vintage"})
    require_regional_coverage({"summary": summary, "trajectories": trajectories},
                              expected_regions=regions, region_output_map=mapping)


def test_heatmap_groups_share_powertrain_axes_and_keep_all_ten_regions():
    from validation.ceud_calibration_diagnostic import CLASS_LABELS, CEUDCalibrationDiagnostic, calibration_anchor_charts

    regions = ["AB", "BCT", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK"]
    summary = pd.DataFrame([{"region": region, "road_class": mode, "base_year": 2025, "status": "calibrate"}
                            for region in regions for mode in CLASS_LABELS])
    trajectories = pd.DataFrame([{"region": row.region, "mode": row.road_class, "vintage": 2025,
        "powertrain": fuel, "unadjusted_efficiency": 2., "efficiency": 2.2, "units": "service/PJ"}
        for row in summary.itertuples() for fuel in ["bev", "gasoline" if row.road_class in
            {"cars", "passenger_light_trucks", "freight_light_trucks"} else "diesel"]])
    charts = calibration_anchor_charts(CEUDCalibrationDiagnostic(
        {"summary": summary, "trajectories": trajectories}, {"expected_regions": regions}, {}))
    for identity, chart in charts.items():
        spec = chart.to_dict(validate=True)
        data = [row for records in spec["datasets"].values() for row in records]
        assert {row["region"] for row in data} == set(regions)
        assert spec["spec"]["layer"][0]["encoding"]["y"]["sort"] == regions
        assert len({row["vehicle_class"] for row in data}) == (3 if identity.endswith("ldv") and not identity.endswith("mhdv") else 2)
        assert {row["powertrain"] for row in data} == ({"bev", "gasoline"} if identity.endswith("-ldv") else {"bev", "diesel"})


def test_combined_fit_retains_excluded_energy_and_secondary_multiplier():
    from validation.ceud_calibration_diagnostic import _energy_fit_chart

    chart = _energy_fit_chart(pd.DataFrame([{"vehicle_class": "Cars", "status": "calibrate",
        "unadjusted_liquid_energy_pj": 120., "ceud_liquid_target_pj": 100.,
        "calibrated_liquid_energy_pj": 100., "excluded_reported_energy_pj": 4.,
        "consumption_multiplier": 100 / 120}]))
    spec = chart.to_dict(validate=True)
    data = [row for records in spec["datasets"].values() for row in records]
    excluded = [row for row in data if "excluded" in row]
    assert len(excluded) == 1
    assert (excluded[0]["lower"], excluded[0]["upper"]) == (100., 104.)
    assert any(row.get("consumption_multiplier") == pytest.approx(100 / 120) for row in data
               if "consumption_multiplier" in row)
    assert spec["resolve"]["scale"]["y"] == "independent"
    assert spec["layer"][1]["layer"][0]["encoding"]["y"]["axis"]["orient"] == "right"


def test_incumbent_trajectory_uses_source_year_and_calibration_dash_for_all_classes():
    from validation.ceud_calibration_diagnostic import CLASS_LABELS, CEUDCalibrationDiagnostic, calibration_charts

    rows = [{"region": "ON", "mode": mode, "powertrain": "gasoline" if mode in
        {"cars", "passenger_light_trucks", "freight_light_trucks"} else "diesel",
        "source_year": year, "native_unit": "MJ/vkm", "unadjusted_native_intensity": 3.,
        "native_intensity": 2.7}
        for mode in CLASS_LABELS for year in [2000, 2025, 2050]]
    summary = pd.DataFrame([{"region": "ON", "road_class": mode, "status": "calibrate",
        "unadjusted_liquid_energy_pj": 30., "ceud_liquid_target_pj": 27.,
        "calibrated_liquid_energy_pj": 27., "consumption_multiplier": .9} for mode in CLASS_LABELS])
    charts = calibration_charts(CEUDCalibrationDiagnostic({"summary": summary,
        "source_trajectories": pd.DataFrame(rows)}, {"trajectory_years": [2000, 2050]}, {}),
        region="ON", include_anchors=False)
    spec = charts["ceud-cal-4"].to_dict(validate=True)
    data = [row for records in spec["datasets"].values() for row in records]
    assert {row["vehicle_class"] for row in data} == set(CLASS_LABELS.values())
    assert {row["source_year"] for row in data} == {2000, 2025, 2050}
    assert spec["layer"][0]["encoding"]["x"]["scale"]["domain"] == [2000, 2050]
    assert spec["layer"][0]["encoding"]["strokeDash"]["field"] == "basis"
    assert "cohort" not in str(spec)
