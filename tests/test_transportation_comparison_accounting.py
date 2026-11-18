"""Independent arithmetic oracles for diagnostic cohort and energy comparisons."""

import pandas as pd
import pytest

from validation.transportation_research import independent_fleet_comparison


@pytest.fixture
def comparison_frames():
    """Two independently parameterized fleets, with deliberately unequal stocks."""
    # The backend has already survived, scaled and redistributed stock. The
    # 2009 annual lifetime endpoint fails; 2010 fails only its model-vintage test.
    backend = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "vintage_year": year,
         "vintage": vintage, "fixed_lifetime_years": 16., "first_model_period": 2025,
         "eligible_first_model_period": eligible,
         "calibrated_surviving_k_vehicles": calibrated,
         "inserted_cohort_k_vehicles": allocated,
         "survival_probability": .5}
        for year, vintage, calibrated, allocated, eligible in (
            (2009, 2005, 10., 0., False), (2010, 2005, 20., 0., False),
            (2011, 2010, 30., 40., True), (2013, 2010, 2., 4., True),
            (2014, 2010, 3., 6., True), (2015, 2010, 5., 10., True),
            (2016, 2015, 10., 20., True),
        )
    ])
    # The legacy model applies survival to representative vintage acquisitions.
    # These are its resulting annual contributions, not backend-scaled values.
    legacy = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "vintage_year": year,
         "vintage": vintage, "raw_capacity_k_vehicles": raw,
         "surviving_k_vehicles": surviving, "survival_probability": probability}
        for year, vintage, raw, surviving, probability in (
            (2013, 2010, 2., 1., .5), (2014, 2010, 6., 3., .5),
            (2015, 2010, 10., 5., .5), (2016, 2015, 8., 6., .75),
        )
    ])
    parameters = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "vintage": vintage,
         "capacity": capacity, "c2a": 2., "efficiency": efficiency}
        for vintage, capacity, efficiency in ((2010, 60., 4.), (2015, 20., 2.))
    ])
    # Independent hand-calculated physical totals: at factor .5 the backend
    # produces 60+20=80 bn passenger-km, requiring 15+10=25 PJ.
    stages = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "stage": stage,
         "vintage": vintage, "annual_utilization": uf,
         "capacity": capacity, "c2a": 2., "efficiency": efficiency,
         "activity_bn": activity, "vehicle_input_pj": energy}
        for stage, uf, values in (
            ("Before calibration", .25, ((2010, 60., 4., 30., 7.5), (2015, 20., 2., 10., 5.))),
            ("Implemented activity calibration", .5, ((2010, 60., 4., 60., 15.), (2015, 20., 2., 20., 10.))),
            ("Energy closure sensitivity", .4, ((2010, 60., 4., 48., 12.), (2015, 20., 2., 16., 8.))),
        )
        for vintage, capacity, efficiency, activity, energy in values
    ])
    return {
        "sales_cohort_reconciliation": backend,
        "legacy_reconstructed_annual": legacy,
        "fleet_cohort_accounting": parameters,
        "legacy_reconstructed_parameters": pd.DataFrame([
            {"region": "ON", "road_class": "cars", "vintage": vintage,
             "raw_capacity_k_vehicles": raw, "surviving_k_vehicles": surviving,
             "c2a": c2a, "efficiency": efficiency}
            for vintage, raw, surviving, c2a, efficiency in (
                (2010, 18., 9., 3., 6.), (2015, 8., 6., 1., 3.),
            )
        ]),
        "stock_activity_energy": pd.DataFrame([{
            "region": "ON", "road_class": "cars", "base_year": 2025,
            "ceud_stock_k_vehicles": 80., "ceud_activity_bn": 80.,
            "ceud_secondary_energy_pj": 20., "ceud_activity_observed_year": 2023,
            "ceud_energy_observed_year": 2023,
        }]),
        "cohort_utilization_energy": stages,
        "legacy_reconstructed_totals": pd.DataFrame([{
            "region": "ON", "road_class": "cars", "utilization": .4,
            "implied_activity_bn": 13.2, "implied_vehicle_input_pj": 2.6,
        }]),
    }


def test_independent_annual_vintage_and_class_stocks_keep_own_survival(comparison_frames):
    originals = {name: frame.copy(deep=True) for name, frame in comparison_frames.items()}
    result = independent_fleet_comparison(comparison_frames, step=5)
    vintages = result["comparison_vintage_stock"].set_index(["evidence", "vintage"])
    assert vintages.loc[("Current backend", 2010), "surviving_k_vehicles"] == 60.
    assert vintages.loc[("Current backend", 2015), "surviving_k_vehicles"] == 20.
    assert vintages.loc[("Legacy recipe", 2010), "surviving_k_vehicles"] == 9.
    assert vintages.loc[("Legacy recipe", 2015), "surviving_k_vehicles"] == 6.
    totals = result["comparison_stock_totals"].set_index("evidence")
    assert totals.loc["Current backend", "surviving_k_vehicles"] == 80.
    assert totals.loc["Legacy recipe", "surviving_k_vehicles"] == 15.
    assert totals.loc["Legacy recipe", "difference_percent"] == -81.25
    # The backend's .5 survival column and legacy's .5/.75 probabilities must
    # never be multiplied into these already parameterized capacities again.
    assert result["comparison_annual_stock"].surviving_k_vehicles.sum() == 95.
    for name, original in originals.items():
        pd.testing.assert_frame_equal(comparison_frames[name], original)


def test_partial_2013_through_2015_bin_sums_available_annual_contributions(comparison_frames):
    result = independent_fleet_comparison(comparison_frames, step=5)
    annual = result["comparison_annual_stock"]
    partial = annual.loc[annual.vintage_year.between(2013, 2015)]
    assert set(partial.vintage) == {2010}
    assert partial.groupby("evidence").surviving_k_vehicles.sum().to_dict() == {
        "Current backend": 20., "Legacy recipe": 9.,
    }
    assert partial.groupby("vintage_year").annual_position.first().to_dict() == {
        2013: 3, 2014: 4, 2015: 5,
    }
    assert set(partial.interval.str.split(";").str[0]) == {"(2010, 2015]"}
    assert partial.loc[partial.evidence.eq("Legacy recipe"), "interval"].str.contains(
        "available 2013", regex=False,
    ).all()
    assert not partial.boundary_exception.any()
    assert set(annual.loc[annual.vintage_year.eq(2016), "vintage"]) == {2015}
    assert set(annual.loc[annual.vintage_year.eq(2016), "annual_position"]) == {1}


@pytest.mark.parametrize("lifetime,extra_count", [(16., 1), (17., 2), (20., 4)])
def test_strict_annual_and_model_vintage_lifetime_boundaries(
    comparison_frames, lifetime, extra_count,
):
    years = [2005, 2006, 2008, 2009, 2010, 2011]
    backend = comparison_frames["sales_cohort_reconciliation"].iloc[:1].copy()
    backend = pd.concat([backend] * len(years), ignore_index=True)
    backend["vintage_year"] = years
    backend["vintage"] = [2000, 2005, 2005, 2005, 2005, 2010]
    backend["fixed_lifetime_years"] = lifetime
    backend["calibrated_surviving_k_vehicles"] = 1.
    backend["inserted_cohort_k_vehicles"] = [0., 0., 0., 0., 0., 6.]
    backend["eligible_first_model_period"] = [False, False, False, False, False, True]
    comparison_frames["sales_cohort_reconciliation"] = backend
    comparison_frames["fleet_cohort_accounting"] = pd.DataFrame([{
        "region": "ON", "road_class": "cars", "vintage": 2010, "capacity": 6.,
    }])
    comparison_frames["stock_activity_energy"]["ceud_stock_k_vehicles"] = 6.
    result = independent_fleet_comparison(comparison_frames, step=5)
    cutoff = result["backend_lifetime_exclusions"].iloc[0]
    # The exact annual endpoint (2009/2008/2005)+L=2025 is excluded.
    # Vintage2005+20=2025 also fails; all classes first retain annual2011.
    assert cutoff.coarse_vintage_exclusion_k_vehicles == extra_count
    assert cutoff.all_excluded_k_vehicles == 5.
    assert cutoff.coarse_exclusion_percent == pytest.approx(100 * extra_count / 6)
    assert cutoff.total_redistributed_percent == pytest.approx(100 * 5 / 6)
    retained = result["comparison_annual_stock"].query(
        "evidence == 'Current backend' and surviving_k_vehicles > 0"
    )
    assert retained.vintage_year.tolist() == [2011]
    assert retained.vintage.tolist() == [2010]


def test_common_backend_uf_scales_activity_and_energy_but_legacy_stays_independent(comparison_frames):
    result = independent_fleet_comparison(comparison_frames, step=5)
    cases = result["baseline_activity_energy_cases"].set_index("case")
    assert cases.implied_activity_bn.to_dict() == {
        "Backend before calibration": 40., "Current backend": 80.,
        "Backend energy sensitivity": 64., "Legacy recipe": 13.2,
    }
    assert cases.implied_vehicle_input_pj.to_dict() == {
        "Backend before calibration": 12.5, "Current backend": 25.,
        "Backend energy sensitivity": 20., "Legacy recipe": 2.6,
    }
    backend = cases.loc[["Backend before calibration", "Current backend", "Backend energy sensitivity"]]
    assert backend.energy_per_service_pj_per_bn.tolist() == [.3125] * 3
    assert cases.loc["Legacy recipe", "energy_per_service_pj_per_bn"] == pytest.approx(2.6 / 13.2)
    assert cases.loc["Current backend", "activity_difference_percent"] == 0.
    assert cases.loc["Current backend", "energy_difference_percent"] == 25.
    assert cases.loc["Backend energy sensitivity", "energy_difference_percent"] == 0.
    assert cases.loc["Backend energy sensitivity", "activity_difference_percent"] == pytest.approx(-20.)
    assert set(cases.base_year) == {2025}
    assert set(cases.ceud_activity_observed_year) == set(cases.ceud_energy_observed_year) == {2023}


@pytest.mark.parametrize("build", ["backend", "legacy"])
def test_nonreconciling_annual_class_detail_is_rejected(comparison_frames, build):
    frame = "sales_cohort_reconciliation" if build == "backend" else "legacy_reconstructed_annual"
    column = "inserted_cohort_k_vehicles" if build == "backend" else "surviving_k_vehicles"
    comparison_frames[frame].loc[0, column] += 1.
    with pytest.raises(ValueError, match="Annual comparison contributions"):
        independent_fleet_comparison(comparison_frames, step=5)


@pytest.mark.parametrize("build", ["backend", "legacy"])
def test_wrong_vintage_detail_is_rejected_even_when_class_totals_match(comparison_frames, build):
    frame = "sales_cohort_reconciliation" if build == "backend" else "legacy_reconstructed_annual"
    comparison_frames[frame].loc[comparison_frames[frame].vintage_year.eq(2013), "vintage"] = 2015
    with pytest.raises(ValueError, match="Annual comparison contributions"):
        independent_fleet_comparison(comparison_frames, step=5)


@pytest.fixture
def comparison_with_cleanup(comparison_frames):
    """One tiny technology-vintage is removed, with its two annual origins kept."""
    frames = comparison_frames
    backend = frames["sales_cohort_reconciliation"]
    shares = backend[["region", "road_class", "vintage", "vintage_year", "inserted_cohort_k_vehicles"]].rename(
        columns={"inserted_cohort_k_vehicles": "capacity"},
    ).assign(tech="gas")
    bev = shares.loc[shares.vintage_year.isin([2013, 2014])].copy()
    bev["tech"] = "bev"
    bev["capacity"] = [.0001, .0003]
    shares.loc[shares.vintage_year.isin([2013, 2014]), "capacity"] -= bev.capacity.to_numpy()
    frames["road_fuel_shares"] = pd.concat([shares, bev], ignore_index=True)
    frames["fleet_cohort_accounting"]["tech"] = "gas"
    frames["fleet_cohort_accounting"].loc[lambda rows: rows.vintage.eq(2010), "capacity"] -= .0004
    stages = frames["cohort_utilization_energy"]
    selected = stages.vintage.eq(2010)
    stages.loc[selected, "capacity"] -= .0004
    stages.loc[selected, "activity_bn"] -= .0004 * stages.loc[selected, "c2a"] * stages.loc[selected, "annual_utilization"]
    stages.loc[selected, "vehicle_input_pj"] = stages.loc[selected, "activity_bn"] / stages.loc[selected, "efficiency"]
    cleanup = {"epsilon": .001, "removed_rows": [{
        "table": "existing_capacity", "region": "ON", "tech": "bev", "vintage": 2010,
        "value": .0004, "units": "k vehicles", "reason": "below_cleanup_tolerance",
    }]}
    return frames, cleanup


def test_audited_cleanup_preserves_exact_annual_origins_and_inserted_accounting(comparison_with_cleanup):
    frames, cleanup = comparison_with_cleanup
    originals = {name: frame.copy(deep=True) for name, frame in frames.items()}
    result = independent_fleet_comparison(frames, step=5, cleanup_audit=cleanup)
    annual = result["comparison_annual_stock"].query("evidence == 'Current backend'").set_index("vintage_year")
    assert annual.loc[2013, "cleanup_removed_k_vehicles"] == .0001
    assert annual.loc[2014, "cleanup_removed_k_vehicles"] == .0003
    assert annual.allocated_before_cleanup_k_vehicles.sum() == 80.
    assert annual.surviving_k_vehicles.sum() == pytest.approx(79.9996)
    vintage = result["comparison_vintage_stock"].query("evidence == 'Current backend' and vintage == 2010").iloc[0]
    assert vintage.surviving_k_vehicles == pytest.approx(59.9996)
    current = result["baseline_activity_energy_cases"].query("case == 'Current backend'").iloc[0]
    assert current.implied_activity_bn == pytest.approx(79.9996)
    assert current.implied_vehicle_input_pj == pytest.approx(24.9999)
    for name, original in originals.items():
        pd.testing.assert_frame_equal(frames[name], original)


@pytest.mark.parametrize("defect", [
    "missing_audit", "wrong_value", "wrong_technology", "wrong_vintage", "wrong_unit",
    "above_threshold", "retained_removed", "wrong_annual_allocation", "wrong_inserted_capacity",
])
def test_cleanup_does_not_hide_unexplained_stock_differences(comparison_with_cleanup, defect):
    frames, cleanup = comparison_with_cleanup
    if defect == "missing_audit":
        cleanup = None
    elif defect == "wrong_value":
        cleanup["removed_rows"][0]["value"] = .0005
    elif defect == "wrong_technology":
        cleanup["removed_rows"][0]["tech"] = "another_bev"
    elif defect == "wrong_vintage":
        cleanup["removed_rows"][0]["vintage"] = 2015
    elif defect == "wrong_unit":
        cleanup["removed_rows"][0]["units"] = "vehicles"
    elif defect == "above_threshold":
        cleanup["epsilon"] = .0001
    elif defect == "retained_removed":
        frames["fleet_cohort_accounting"] = pd.concat([
            frames["fleet_cohort_accounting"], pd.DataFrame([{
                "region": "ON", "road_class": "cars", "tech": "bev", "vintage": 2010, "capacity": .0004,
            }]),
        ], ignore_index=True)
    elif defect == "wrong_annual_allocation":
        frames["road_fuel_shares"].loc[0, "capacity"] += 1.
    else:
        frames["fleet_cohort_accounting"].loc[0, "capacity"] += .0001
    with pytest.raises(ValueError, match="Annual comparison"):
        independent_fleet_comparison(frames, step=5, cleanup_audit=cleanup)
