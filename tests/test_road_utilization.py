"""Focused contracts for road mileage weighting and the v4 age boundary."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import sys

import pandas as pd
import pytest
from canoe_schema.v4_0 import ExistingCapacity, LifetimeProcess, LifetimeSurvivalCurve, LifetimeTech

from parameterization.build_lifetime_parameters import LifetimePreparation
from parameterization.road_utilization import (
    prepare_road_utilization,
    validate_age_artifact,
    validate_road_utilization,
)
from utils import load_config_bundle, load_harmonization_rules
from validation.provenance import resolve_provenance
from validation.config_models import ScenarioConfig


def stock_inputs(bundle, overrides=()):
    """Keep synthetic curve-capacity sales distinct from surviving vehicles."""
    context = resolve_provenance(
        bundle.sources, source_key="nrcan_ceud_transport_provincial", component_key=21,
        transformation="isolated utilization stock fixture", transformation_version="1",
    )
    defaults = [
        "T_LDV_C_GSL_EX", "T_LDV_LTP_GSL_EX", "T_LDV_LTF_GSL_EX", "T_MDV_T_DSL_EX", "T_HDV_T_DSL_EX",
        "T_LDV_M_GSL_EX", "T_HDV_BS_DSL_EX", "T_HDV_BT_DSL_EX", "T_HDV_BIC_DSL_EX",
    ]
    overridden = {(row.region, row.tech) for row in overrides}
    rows = [row.model_copy(update={"data_id": context.data_id, "units": "k vehicles",
                                   "capacity": 10000. if row.capacity > 0 else 0.}) for row in overrides]
    rows.extend(ExistingCapacity(region=region, tech=tech, vintage=bundle.scenario.periods.latest_observed_vintage,
                                 capacity=10000., units="k vehicles", **context.parameter_fields())
                for region in bundle.scenario.geography.regions for tech in defaults
                if (region, tech) not in overridden)
    physical_stock = pd.DataFrame([
        {"region": row.region, "tech": row.tech, "vintage": row.vintage,
         "surviving_k_vehicles": row.capacity / (2 if bundle.scenario.lifetimes.survival_curves else 1),
         "base_year": bundle.scenario.periods.base_year,
         "units": "k vehicles", "data_id": row.data_id}
        for row in rows
    ])
    return rows, [context], physical_stock


def lifetime_inputs(bundle, stock, *, years=40):
    """Synthetic endpoints exercise coverage without changing registered evidence."""
    context = resolve_provenance(
        bundle.sources, source_key="epa_moves_heavy_truck_survival", component_key="car_survival_rates",
        transformation="synthetic utilization lifetime fixture", transformation_version="1",
    )
    rules = load_harmonization_rules(bundle, "road_utilization")
    classes = rules["age_profile_classes"]
    technology = pd.read_csv(bundle.repo_root / "inputs/0_canoe_template/technology.csv")
    owners = technology.loc[technology.category.isin(set(classes) | set(rules["flat_only_classes"]))]
    fixed, curves, processes = [], [], []
    for region in bundle.scenario.geography.regions:
        for owner in owners.itertuples(index=False):
            tech = owner.tech
            if not bundle.scenario.lifetimes.survival_curves or owner.category not in classes:
                fixed.append(LifetimeTech(region=region, tech=tech, lifetime=years, **context.parameter_fields()))
                continue
            vintages = ([row.vintage for row in stock if row.region == region and row.tech == tech and row.capacity > 0]
                        if tech.endswith("_EX") else bundle.scenario.periods.model)
            for vintage in vintages:
                curves.extend(LifetimeSurvivalCurve(
                    region=region, tech=tech, vintage=vintage, period=vintage + age,
                    fraction=(years - age) / years, **context.parameter_fields(),
                ) for age in range(years + 1))
                processes.append(LifetimeProcess(region=region, tech=tech, vintage=vintage,
                                                 lifetime=years, **context.parameter_fields()))
    representation = "accepted_road_curves" if curves else "fixed_lifetimes"
    return LifetimePreparation(fixed, curves, [context], {"representation": representation}, processes)


@pytest.fixture
def isolated_bundle(isolated_legacy_bundle, tmp_path, monkeypatch):
    current = load_config_bundle("config/scenarios/default.yaml", repo_root=Path(__file__).resolve().parents[1])
    bundle = isolated_legacy_bundle
    scenario = current.scenario.model_copy(update={
        "geography": current.scenario.geography.model_copy(update={"regions": ["ON"]}),
        "lifetimes": current.scenario.lifetimes.model_copy(update={"survival_curves": True}),
        "existing_capacity": current.scenario.existing_capacity.model_copy(update={
            "vehicle_population_year": bundle.scenario.existing_capacity.vehicle_population_year,
        }),
    })

    def artifact_path(config, family, *parts):
        assert family in {"road_utilization_processed", "road_utilization_validation"}
        return tmp_path.joinpath(family, *parts)

    monkeypatch.setattr("parameterization.road_utilization.resolve_artifact_path", artifact_path)
    return replace(bundle, scenario=scenario)


def test_age_mode_retains_vintage_period_grain_and_flat_only_classes(isolated_bundle, tmp_path) -> None:
    bundle = isolated_bundle
    payload = bundle.scenario.model_dump(mode="python")
    payload["lifetimes"]["survival_curve_max_age"] = 10
    payload["road_utilization"].update(vkt_schedules=True)
    scenario = ScenarioConfig.model_validate(payload)
    selected = replace(bundle, scenario=scenario)
    stock, contexts, physical_stock = stock_inputs(selected)
    prepared = prepare_road_utilization(selected, existing_capacity_rows=stock,
                                        existing_capacity_contexts=contexts, physical_stock=physical_stock,
                                        lifetime_preparation=lifetime_inputs(selected, stock, years=10))
    rows = prepared.age_factor_artifact
    assert rows is not None
    assert prepared.audit["sqlite_insertion"] == "blocked_upstream_vintage_plus_period_schema_gap"
    technology = pd.read_csv(bundle.repo_root / "inputs" / "0_canoe_template" / "technology.csv")
    categories = technology.set_index("tech").category.to_dict()
    assert {categories[row.tech_or_group] for row in prepared.flat_factor_rows} == {
        "motorcycles", "school_buses", "urban_transit", "inter_city_buses",
    }
    rules = load_harmonization_rules(bundle, "road_utilization")
    profile = pd.read_csv(
        tmp_path / "road_utilization_processed" / rules["age_profile_file"]
    )
    car = profile.loc[profile.region.eq("ON") & profile.nrcan_ceud_class.eq("Car")]
    horizon_age = scenario.periods.end_of_horizon - min(scenario.periods.existing)
    assert car.age.max() == horizon_age
    assert scenario.lifetimes.survival_curve_max_age == 10
    assert prepared.audit["age_ceiling"] == horizon_age
    assert car.normalized_utilization.max() == pytest.approx(1.0)
    assert car.normalized_utilization.nunique() > 1
    selected = rows.loc[
        rows.region.eq("ON") & rows.road_class.eq("cars")
        & rows.tech_or_group.eq("T_LDV_C_GSL_N")
        & rows.vintage.eq(2025) & rows.period.eq(2025)
    ].iloc[0]
    expected = selected.baseline_utilization * car.loc[
        car.age.between(1, 5), "normalized_utilization"
    ].mean()
    assert selected.factor == pytest.approx(expected)
    assert selected.age_first_year == 1
    assert selected.age_last_year == 5
    assert {"vintage", "period"} <= set(rows)
    assert rows[["region", "tech_or_group", "vintage", "period", "output_comm"]].duplicated().sum() == 0
    assert rows.period.lt(rows.retirement_year).all()
    combined = prepared.period_factor_artifact
    assert combined is not None
    flat_owners = set(technology.loc[technology.category.isin(rules["flat_only_classes"]), "tech"])
    constant = combined.loc[combined.tech_or_group.isin(flat_owners)]
    assert set(categories[tech] for tech in constant.tech_or_group) == set(rules["flat_only_classes"])
    for _, group in constant.groupby(["region", "tech_or_group", "vintage"]):
        vintage = group.vintage.iloc[0]
        assert group.period.tolist() == [period for period in scenario.periods.model if vintage <= period < vintage + 10]
        assert group.factor.nunique() == 1
    assert len(combined) == len(rows) + len(constant)
    assert len(combined) == prepared.audit["period_factor_expected_keys"]
    assert not combined[["region", "tech_or_group", "vintage", "period", "output_comm"]].duplicated().any()
    assert set(combined.data_id) <= {context.data_id for context in prepared.provenance_contexts}
    context_by_id = {context.data_id: context for context in prepared.provenance_contexts}
    old_flat = {(row.region, row.tech_or_group, row.vintage): row for row in prepared.flat_factor_rows}
    for row in constant.itertuples(index=False):
        original = old_flat[row.region, row.tech_or_group, row.vintage]
        assert row.factor == original.factor and row.data_source == original.data_source
        assert {item.source_key for item in context_by_id[row.data_id].contributors} >= {
            "nrcan_ceud_transport_provincial", "epa_moves_heavy_truck_survival",
        }
    pd.testing.assert_frame_equal(
        pd.read_csv(tmp_path / "road_utilization_processed" / rules["period_factor_file"]),
        combined, check_dtype=False,
    )


def test_fixed_vkt_includes_terminal_partial_period_and_checks_complete_coverage(isolated_bundle, tmp_path):
    bundle = replace(isolated_bundle, scenario=isolated_bundle.scenario.model_copy(update={
        "lifetimes": isolated_bundle.scenario.lifetimes.model_copy(update={"survival_curves": False}),
        "road_utilization": isolated_bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": True}),
    }))
    stock, contexts, physical = stock_inputs(bundle)
    prepared = prepare_road_utilization(bundle, existing_capacity_rows=stock,
        existing_capacity_contexts=contexts, physical_stock=physical,
        lifetime_preparation=lifetime_inputs(bundle, stock, years=17))
    rows = prepared.age_factor_artifact
    cohort = rows.loc[rows.tech_or_group.eq("T_LDV_C_GSL_N") & rows.vintage.eq(2025)]
    assert cohort.period.tolist() == [2025, 2030, 2035, 2040]
    terminal = cohort.iloc[-1]
    assert terminal.retirement_year == 2042
    assert terminal.age_first_year == 16 and terminal.age_last_year == 17
    rules = load_harmonization_rules(bundle, "road_utilization")
    profiles = pd.read_csv(tmp_path / "road_utilization_processed" / rules["age_profile_file"])
    car = profiles.loc[profiles.nrcan_ceud_class.eq("Car")]
    assert terminal.factor == pytest.approx(terminal.baseline_utilization * car.loc[
        car.age.between(16, 17), "normalized_utilization"].mean())
    keys = ["region", "tech_or_group", "vintage", "period", "output_comm"]
    expected = set(rows[keys].itertuples(index=False, name=None))
    with pytest.raises(ValueError, match="operating lifetime coverage"):
        validate_age_artifact(rows.drop(cohort.index[-1]), profiles, regions={"ON"},
            periods=bundle.scenario.periods.model, max_age=17, age_classes=rules["age_profile_classes"],
            expected_keys=expected)


def test_curve_vkt_holds_native_endpoints_through_horizon_and_keeps_full_lineage(isolated_bundle, tmp_path):
    bundle = replace(isolated_bundle, scenario=isolated_bundle.scenario.model_copy(update={
        "road_utilization": isolated_bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": True}),
    }))
    override = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=2000,
                                capacity=1, data_id="stock-fixture")]
    stock, contexts, physical = stock_inputs(bundle, override)
    lifetimes = lifetime_inputs(bundle, stock, years=60)
    prepared = prepare_road_utilization(bundle, existing_capacity_rows=stock,
        existing_capacity_contexts=contexts, physical_stock=physical, lifetime_preparation=lifetimes)
    rows = prepared.age_factor_artifact
    cohort = rows.loc[rows.tech_or_group.eq("T_LDV_C_GSL_EX")]
    assert set(cohort.vintage) == {2000}
    assert cohort.period.tolist() == bundle.scenario.periods.model
    assert cohort.age_last_year.iloc[-1] == bundle.scenario.periods.end_of_horizon - 2000
    rules = load_harmonization_rules(bundle, "road_utilization")
    profiles = pd.read_csv(tmp_path / "road_utilization_processed" / rules["age_profile_file"])
    tails = profiles.loc[profiles.age.ge(34)]
    assert all(group.normalized_utilization.nunique() == 1
               for _, group in tails.groupby(["region", "nrcan_ceud_class"]))
    context_by_id = {context.data_id: context for context in prepared.provenance_contexts}
    assert set(rows.data_id) <= set(context_by_id)
    for data_id in set(rows.data_id):
        sources = {contributor.source_key for contributor in context_by_id[data_id].contributors}
        assert {"cer_canadas_energy_future", "epa_moves_heavy_truck_survival",
                "nlr_atb_transportation", "nrcan_ceud_transport_provincial"} <= sources
    assert rows[["dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time", "notes"]].notna().all().all()
    with pytest.raises(ValueError, match="Incomplete VKT lifetime lineage"):
        prepare_road_utilization(bundle, existing_capacity_rows=stock,
            existing_capacity_contexts=contexts, physical_stock=physical,
            lifetime_preparation=replace(lifetimes, provenance_contexts=[]))


def test_vkt_requires_matching_prepared_lifetimes(isolated_bundle):
    bundle = replace(isolated_bundle, scenario=isolated_bundle.scenario.model_copy(update={
        "road_utilization": isolated_bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": True}),
    }))
    stock, contexts, physical = stock_inputs(bundle)
    with pytest.raises(ValueError, match="matching prepared lifetime batches"):
        prepare_road_utilization(bundle, existing_capacity_rows=stock,
            existing_capacity_contexts=contexts, physical_stock=physical)


def test_vkt_uses_only_positive_regional_stock_cohorts(isolated_bundle):
    bundle = replace(isolated_bundle, scenario=isolated_bundle.scenario.model_copy(update={
        "geography": isolated_bundle.scenario.geography.model_copy(update={"regions": ["ON", "AB"]}),
        "road_utilization": isolated_bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": True}),
    }))
    overrides = [ExistingCapacity(region=region, tech="T_LDV_C_GSL_EX", vintage=vintage,
                                 capacity=capacity, data_id="test-stock")
                 for region, vintage, capacity in (("ON", 2000, 0), ("ON", 2015, 1), ("AB", 2020, 1))]
    stock, contexts, physical = stock_inputs(bundle, overrides)
    lifetimes = lifetime_inputs(bundle, stock, years=60)
    prepared = prepare_road_utilization(bundle, existing_capacity_rows=stock,
        existing_capacity_contexts=contexts, physical_stock=physical, lifetime_preparation=lifetimes)
    car = prepared.age_factor_artifact.loc[prepared.age_factor_artifact.tech_or_group.eq("T_LDV_C_GSL_EX")]
    assert set(zip(car.region, car.vintage, strict=True)) == {("ON", 2015), ("AB", 2020)}
    for _, group in car.groupby("region"):
        assert group.period.tolist() == bundle.scenario.periods.model
    incomplete = [row for row in lifetimes.curve_rows
                  if (row.region, row.tech, row.vintage) != ("ON", "T_LDV_C_GSL_N", 2025)]
    with pytest.raises(ValueError, match="Missing lifetime curve for process"):
        prepare_road_utilization(bundle, existing_capacity_rows=stock,
            existing_capacity_contexts=contexts, physical_stock=physical,
            lifetime_preparation=replace(lifetimes, curve_rows=incomplete))


def test_flat_mode_applies_existing_cohort_limits_without_age_profiles(isolated_bundle, tmp_path) -> None:
    bundle = isolated_bundle
    scenario = bundle.scenario.model_copy(update={
        "road_utilization": bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": False}),
    })
    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=vintage, capacity=capacity, data_id="test-stock")
             for vintage, capacity in ((2000, 0.0), (2015, 1.0), (2020, 2.0))]
    selected = replace(bundle, scenario=scenario)
    stock, contexts, physical_stock = stock_inputs(selected, stock)
    prepared = prepare_road_utilization(selected, existing_capacity_rows=stock,
                                        existing_capacity_contexts=contexts, physical_stock=physical_stock)
    assert prepared.age_factor_artifact is None
    assert prepared.period_factor_artifact is None
    assert prepared.audit["vkt_schedules"] is False
    rows = prepared.flat_factor_rows
    existing_suffix = load_harmonization_rules(bundle, "efficiencies")["existing_suffix"]
    for item in prepared.audit["flat_vintages_by_region_technology"]:
        tech, vintages = item["tech"], item["vintages"]
        expected = ([row.vintage for row in stock if row.tech == tech and row.capacity > 0]
                    if tech.endswith(existing_suffix) else scenario.periods.model)
        assert vintages == expected
        selected = [row for row in rows if row.tech_or_group == tech]
        assert {row.vintage for row in selected} == set(expected)
        if expected:
            assert len({row.factor for row in selected}) == 1
            assert {row.operator for row in selected} == {"e"}

    rules = load_harmonization_rules(bundle, "road_utilization")
    output = tmp_path / "road_utilization_processed"
    annual = pd.read_csv(output / rules["annual_audit_file"])
    baseline = pd.read_csv(output / rules["baseline_file"])
    expected_factor = baseline.loc[baseline.road_class.eq("cars"), "utilization"].item()
    cohort = [row for row in rows if row.tech_or_group == "T_LDV_C_GSL_EX" and row.vintage == 2015]
    assert len(cohort) == 1 and cohort[0].factor == pytest.approx(expected_factor)
    assert not any(row.tech_or_group.endswith(existing_suffix) and row.vintage in scenario.periods.model for row in rows)

    # Reintroducing the old future-year key is rejected before publication.
    wrong_vintage = [row.model_copy(update={"vintage": scenario.periods.model[0]})
                     if row is cohort[0] else row for row in rows]
    with pytest.raises(ValueError, match="Flat annual factor row coverage"):
        validate_road_utilization(
            annual, baseline, prepared.capacity_to_activity_rows, wrong_vintage,
            regions={"ON"}, road_classes=set(baseline.road_class), years=prepared.audit["baseline_years"],
            vintages_by_region_tech={(item["region"], item["tech"]): item["vintages"]
                                    for item in prepared.audit["flat_vintages_by_region_technology"]},
            flat_classes=set(baseline.road_class),
            technology=pd.read_csv(bundle.repo_root / "inputs/0_canoe_template/technology.csv"),
        )


def test_existing_utilization_keeps_regional_cohort_coverage(isolated_bundle) -> None:
    bundle = isolated_bundle
    scenario = bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON", "AB"]}),
        "road_utilization": bundle.scenario.road_utilization.model_copy(update={"vkt_schedules": False}),
    })
    stock = [ExistingCapacity(region=region, tech="T_LDV_C_GSL_EX", vintage=vintage, capacity=1, data_id="test-stock")
             for region, vintage in (("ON", 2015), ("AB", 2020), ("QC", 2000))]
    selected = replace(bundle, scenario=scenario)
    stock, contexts, physical_stock = stock_inputs(selected, stock)
    prepared = prepare_road_utilization(selected, existing_capacity_rows=stock,
                                        existing_capacity_contexts=contexts, physical_stock=physical_stock)
    existing = [row for row in prepared.flat_factor_rows if row.tech_or_group == "T_LDV_C_GSL_EX"]
    assert {(row.region, row.tech_or_group, row.vintage) for row in existing} == {
        ("ON", "T_LDV_C_GSL_EX", 2015), ("AB", "T_LDV_C_GSL_EX", 2020),
    }


def test_existing_utilization_rejects_positive_stock_outside_declared_vintages(isolated_bundle) -> None:
    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=1990, capacity=1, data_id="test-stock")]
    with pytest.raises(ValueError, match="outside the scenario"):
        stock, contexts, physical_stock = stock_inputs(isolated_bundle, stock)
        prepare_road_utilization(isolated_bundle, existing_capacity_rows=stock,
                                 existing_capacity_contexts=contexts, physical_stock=physical_stock)


def test_cli_explicitly_prepares_stock_once_before_utilization(monkeypatch) -> None:
    from parameterization import road_utilization as layer

    stock = [ExistingCapacity(region="ON", tech="T_LDV_C_GSL_EX", vintage=2015, capacity=1, data_id="test-stock")]
    events = []

    def prepare_stock(bundle):
        events.append("stock")
        return SimpleNamespace(existing_capacity_rows=stock, provenance_contexts=[], physical_stock=pd.DataFrame())

    def prepare_utilization(bundle, *, existing_capacity_rows, existing_capacity_contexts, physical_stock,
                            lifetime_preparation):
        assert existing_capacity_rows is stock
        assert existing_capacity_contexts == []
        assert lifetime_preparation is None
        events.append("utilization")

    monkeypatch.setattr("parameterization.build_existing_capacity.prepare_existing_capacity_rows", prepare_stock)
    monkeypatch.setattr(layer, "prepare_road_utilization", prepare_utilization)
    monkeypatch.setattr(sys, "argv", ["road_utilization"])
    layer.main()
    assert events == ["stock", "utilization"]


@pytest.mark.parametrize("observed", [False, True], ids=["projected", "observed"])
def test_all_road_utilization_closes_base_year_service_on_physical_stock(isolated_bundle, observed):
    from parameterization.build_demand import road_base_year_activity

    bundle = isolated_bundle
    if observed:
        periods = bundle.scenario.periods.model_copy(update={
            "base_year": 2023, "period_mode": "legacy", "existing": [2000, 2005, 2010, 2015, 2020, 2023],
        })
        bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={"periods": periods}))
    stock, contexts, physical_stock = stock_inputs(bundle)
    with pytest.raises(ValueError, match="Curve capacity requires an explicit physical-stock handoff"):
        prepare_road_utilization(bundle, existing_capacity_rows=stock, existing_capacity_contexts=contexts)
    prepared = prepare_road_utilization(bundle, existing_capacity_rows=stock,
                                        existing_capacity_contexts=contexts, physical_stock=physical_stock)
    expected, metadata = road_base_year_activity(bundle)
    calibration = pd.DataFrame(prepared.audit["base_year_activity_calibration"])
    comparison = calibration.merge(expected[["region", "service", "gdp_indexed_base_year_demand"]],
                                   left_on=["region", "road_class"], right_on=["region", "service"])
    assert len(comparison) == len(expected) == 9
    assert set(comparison.target_basis) == {
        "observed_ceud_activity" if observed else "native_ceud_activity_indexed_by_cer_real_gdp",
    }
    assert (metadata["base_year_gdp_index"] == pytest.approx(1.)) == observed
    assert comparison.target_activity_bn.to_numpy() == pytest.approx(comparison.gdp_indexed_base_year_demand.to_numpy())
    assert comparison.modelled_activity_bn.to_numpy() == pytest.approx(comparison.target_activity_bn.to_numpy())
    assert comparison.target_activity_bn.to_numpy() == pytest.approx(
        comparison.observed_activity_bn.to_numpy() * metadata["base_year_gdp_index"])
    categories = pd.read_csv(bundle.repo_root / "inputs/0_canoe_template/technology.csv").set_index("tech").category
    capacity_to_activity = {row.tech: row.c2a for row in prepared.capacity_to_activity_rows}
    snapshot = physical_stock.assign(
        road_class=physical_stock.tech.map(categories),
        activity_denominator=physical_stock.surviving_k_vehicles * physical_stock.tech.map(capacity_to_activity),
    ).groupby("road_class").agg(
        retained_stock_k_vehicles=("surviving_k_vehicles", "sum"),
        capacity_activity_denominator_bn=("activity_denominator", "sum"),
    )
    for row in comparison.itertuples(index=False):
        assert row.retained_stock_k_vehicles == pytest.approx(snapshot.loc[row.road_class, "retained_stock_k_vehicles"])
        assert row.capacity_activity_denominator_bn == pytest.approx(snapshot.loc[row.road_class, "capacity_activity_denominator_bn"])
        assert row.utilization * row.capacity_activity_denominator_bn == pytest.approx(row.target_activity_bn)
    expected_factors = calibration.set_index("road_class").utilization
    for row in prepared.flat_factor_rows:
        assert row.factor == pytest.approx(expected_factors.loc[categories.loc[row.tech_or_group]])
    for context in prepared.provenance_contexts:
        if "road_utilization.flat." in context.component_key and any(name in context.component_key for name in calibration.road_class):
            assert "cer_canadas_energy_future" in {contributor.source_key for contributor in context.contributors}


@pytest.mark.parametrize("defect", ["missing", "duplicate", "nonfinite", "infeasible"])
def test_projected_activity_calibration_rejects_invalid_target(defect):
    from parameterization.road_utilization import calibrate_base_year_activity

    annual = pd.DataFrame([{"region": "ON", "road_class": "cars", "year": 2025,
                            "activity_bn": 2., "stock_k_vehicles": 1.}])
    if defect == "missing":
        annual = annual.iloc[:0]
    elif defect == "duplicate":
        annual = pd.concat([annual, annual])
    else:
        annual["activity_bn"] = float("nan") if defect == "nonfinite" else 3.
    with pytest.raises(ValueError):
        calibrate_base_year_activity(
            pd.DataFrame([{"region": "ON", "road_class": "cars", "utilization": .2}]), annual,
            pd.DataFrame([{"region": "ON", "tech": "car", "capacity": 1.}]),
            pd.DataFrame([{"category": "cars", "tech": "car", "c2a": 2.}]),
            selected_pairs={("ON", "cars")}, base_year=2025,
        )


@pytest.mark.parametrize("future_car_demand", ["GDP-indexed", "extrapolated"])
def test_reference_year_activity_uses_selected_passenger_split_without_changing_total(monkeypatch, future_car_demand):
    from parameterization.build_demand import road_base_year_activity
    from parameterization.road_stocks_and_demands import extrapolate_passenger_ldv_demand

    bundle = load_config_bundle("config/scenarios/default.yaml")
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON"]}),
        "demand": bundle.scenario.demand.model_copy(update={"future_car_demand": future_car_demand}),
    }))
    road_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    provincial = pd.DataFrame([
        {"region": "ON", "year": year, "table_id": selector["table_id"],
         "raw_series": selector["raw_series"], "unit": "millions",
         "value": 1000. * .98 ** (year - 2013) if service == "cars" else 1000.}
        for service, selector in road_rules["activity_series"].items() for year in range(2013, 2024)
    ])
    macro = pd.DataFrame([
        {"source_id": "cer_canadas_energy_future", "edition": 2026, "scenario": "Current Measures",
         "region": "Canada", "variable_key": "real_gdp", "year": year, "value": value, "unit": "M CAD2020"}
        for year, value in [(2023, 100.), (2025, 110.)]
    ])
    monkeypatch.setattr("parameterization.build_demand.file_sha256", lambda path: "fixture-macro")
    baseline, metadata = road_base_year_activity(bundle, provincial=provincial, macro=macro)
    passengers = baseline.set_index("service").loc[["cars", "passenger_light_trucks"]]
    assert passengers.projected_base_year_demand.sum() == pytest.approx(passengers.baseline_demand.sum() * 1.1)
    if future_car_demand == "GDP-indexed":
        assert passengers.projected_base_year_demand.tolist() == pytest.approx(passengers.gdp_indexed_base_year_demand.tolist())
    else:
        rules = {**road_rules, "car_extrapolation": {
            **road_rules["car_extrapolation"], **load_harmonization_rules(bundle, "ceud_inputs")["cagr"]}}
        expected, _ = extrapolate_passenger_ldv_demand(
            provincial, baseline, regions=["ON"], base_year=2023, evaluation_years={2025: 2025},
            gdp_indices={2025: 1.1}, rules=rules,
        )
        assert passengers.loc["cars", "projected_base_year_demand"] == pytest.approx(expected["ON", 2025, "cars"])
        assert passengers.loc["passenger_light_trucks", "projected_base_year_demand"] == pytest.approx(expected["ON", 2025, "passenger_light_trucks"])
        assert passengers.loc["cars", "projected_base_year_demand"] != pytest.approx(passengers.loc["cars", "gdp_indexed_base_year_demand"])
    assert metadata["future_car_demand"] == future_car_demand
