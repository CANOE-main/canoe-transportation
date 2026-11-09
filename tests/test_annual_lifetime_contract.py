"""Unified tail derivation and annual curve/process-lifetime row contract."""

from dataclasses import replace
import sqlite3

import pandas as pd
import pytest
from canoe_schema.v4_0 import (
    DataSet,
    ExistingCapacity,
    LifetimeProcess,
    LifetimeSurvivalCurve,
    Region,
    Technology,
    TechnologyLabel,
    TimePeriod,
)

from build_transport import TransportContribution, insert_transport_contribution
import parameterization.build_lifetime_parameters as builder
import parameterization.road_lifetimes_survival as survival
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.database_bootstrap import validate_database
from validation.insertion import insert_models, validate_parameter_rows
from validation.provenance import resolve_provenance
from validation.schema_contract import create_v4_schema


def curve_bundle(profile="moves"):
    bundle = load_config_bundle("config/scenarios/validation_baseline.yaml")
    return replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={
            "survival_curves": True, "road_lifetime_survival_source": profile,
            "survival_curve_max_age": None,
        })}))


def test_tail_settings_are_validated_before_source_io(monkeypatch):
    monkeypatch.setattr(builder, "derive_accepted_lifetime_frames",
                        lambda *args, **kwargs: pytest.fail("Invalid tail settings reached source I/O"))
    bundle = curve_bundle()
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"tail_derivation": "unsupported"})}))
    with pytest.raises(ValueError, match="tail_derivation must be constant_retirement"):
        builder.prepare_lifetime_rows(bundle)


def test_tail_threshold_comes_from_scenario_without_a_rules_lookup(monkeypatch):
    bundle = curve_bundle()
    assert bundle.scenario.lifetimes.tail_survival_threshold == 0.005
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"tail_survival_threshold": 0.002})}))
    monkeypatch.setattr(survival, "load_harmonization_rules",
                        lambda *args, **kwargs: pytest.fail("Tail threshold unexpectedly read parameter rules"))
    assert survival.require_tail_derivation(bundle) == {
        "tail_derivation": "constant_retirement", "threshold": 0.002, "hard_zero_age": None,
    }


@pytest.mark.parametrize("profile,common_support", [("moves", 41), ("nhtsa_nems", 34)])
def test_full_source_support_and_annual_endpoint_rows(profile, common_support):
    bundle = curve_bundle(profile)
    frames = survival.derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"))
    selected = survival.selected_road_survival_schedules(bundle, frames["transformed_curves"])
    assert selected.groupby("road_class").age.max().min() == common_support
    harmonized = survival.harmonized_road_survival_schedules(bundle, frames["transformed_curves"])
    for road_class, native in selected.groupby("road_class"):
        actual = harmonized.annual.loc[harmonized.annual.road_class.eq(road_class)].sort_values("age")
        assert actual.survival_probability.iloc[:len(native)].tolist() == native.sort_values("age").survival_probability.tolist()
    assert survival.resolved_survival_max_age(bundle, harmonized.annual) == (55 if profile == "moves" else 141)
    lower_threshold = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"tail_survival_threshold": 0.001})}))
    longer = survival.harmonized_road_survival_schedules(lower_threshold, frames["transformed_curves"])
    assert survival.resolved_survival_max_age(lower_threshold, longer.annual) == (65 if profile == "moves" else 181)
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    acquisition_context = resolve_provenance(
        bundle.sources, source_key="nrcan_ceud_transport_provincial", component_key=21,
        transformation="test_cohort_acquisition_input", transformation_version="1")
    annual = pd.DataFrame({"region": ["ON"], "road_class": ["cars"],
                          "tech": ["T_LDV_C_GSL_EX"], "vintage": [2020],
                          "vintage_year": [2021], "calibrated_acquisitions_k_vehicles": [100.0],
                          "data_id": [acquisition_context.data_id]})
    curves, lifetimes, contexts, audit = survival.prepare_road_survival_curve_rows(
        bundle, transformed=frames["transformed_curves"], technology=technology,
        annual_acquisitions=annual, cohort_provenance_contexts=[acquisition_context])
    for lifetime in lifetimes:
        series = [row for row in curves if (row.region, row.tech, row.vintage)
                  == (lifetime.region, lifetime.tech, lifetime.vintage)]
        assert [row.period for row in series] == list(range(lifetime.vintage, lifetime.vintage + int(lifetime.lifetime) + 1))
        assert series[0].fraction == 1 and series[-1].fraction == 0
        assert all(row.fraction >= 0.0001 for row in series[:-1])
    existing = next(row for row in lifetimes if row.tech == "T_LDV_C_GSL_EX")
    existing_audit = next(row for row in audit["grouped_cohorts"] if row["tech"] == existing.tech)
    assert existing.vintage + existing.lifetime == existing_audit["terminal_year"]
    assert existing_audit["removed_reference_stock_k_vehicles"] == 0
    new = next(row for row in lifetimes if row.tech == "T_LDV_C_GSL_N")
    assert new.lifetime == (42 if profile == "moves" else 44)
    assert existing.lifetime <= new.lifetime + 1
    existing_context = next(context for context in contexts if context.data_id == existing.data_id)
    assert "nrcan_ceud_transport_provincial" in {item.source_key for item in existing_context.contributors}
    future_context = next(context for context in contexts if context.data_id == new.data_id)
    assert "nrcan_ceud_transport_provincial" not in {item.source_key for item in future_context.contributors}
    assert all(row.data_id and row.data_source for row in [*curves, *lifetimes])
    assert len(audit["terminal_schedules"]) == 5


def test_hard_zero_cap_may_exceed_native_support_and_resolves_without_source_io():
    bundle = curve_bundle("nhtsa_nems")
    frames = survival.derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"))
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curve_max_age": 80})}))
    native = survival.selected_road_survival_schedules(bundle, frames["transformed_curves"])
    assert native.groupby("road_class").age.max().min() == 34
    assert survival.resolved_survival_max_age(bundle) == 80
    actual = survival.harmonized_road_survival_schedules(bundle, frames["transformed_curves"])
    heavy = actual.annual.loc[actual.annual.road_class.eq("heavy_trucks")]
    assert heavy.age.max() == 80
    assert heavy.survival_probability.iloc[-1] == 0


@pytest.mark.parametrize("profile", ["moves", "nhtsa_nems"])
def test_explicit_cap_sets_the_same_selected_fixed_medians_as_harmonized_survival(profile):
    bundle = curve_bundle(profile)
    frames = survival.derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"))
    manual = pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv"))
    original = survival.fixed_existing_lifetimes(
        frames["medians"], manual, survival.resolved_road_lifetime_rules(bundle))
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curve_max_age": 10})}))
    harmonized = survival.harmonized_road_survival_schedules(bundle, frames["transformed_curves"])
    capped = survival.fixed_existing_lifetimes(
        frames["medians"], manual, survival.resolved_road_lifetime_rules(bundle))
    for road_class, schedule in harmonized.annual.groupby("road_class"):
        assert capped[road_class] == schedule.loc[schedule.survival_probability.le(0.5), "age"].min() == 10
    for road_class in set(capped) - set(harmonized.annual.road_class):
        assert capped[road_class] == original[road_class]


def test_dense_annual_curve_inserts_outside_model_periods_with_full_provenance():
    bundle = curve_bundle()
    context = resolve_provenance(
        bundle.sources, source_key="epa_moves_heavy_truck_survival",
        component_key="car_survival_rates",
        transformation="synthetic_annual_curve_sqlite_contract_fixture",
        transformation_version="1",
    )
    # Synthetic values exercise the schema/registration seam, not a source-tail
    # assumption; no source values or scenario settings are changed.
    key = {"region": "ON", "tech": "T_TEST_EX", "vintage": 2020}
    fractions = [(2037 - year) / 17 for year in range(2020, 2038)]
    curves = validate_parameter_rows(
        LifetimeSurvivalCurve,
        [{**key, "period": year, "fraction": fraction}
         for year, fraction in zip(range(2020, 2038), fractions, strict=True)],
        context,
    )
    lifetimes = validate_parameter_rows(
        LifetimeProcess, [{**key, "lifetime": 17, "units": "years"}], context,
    )
    contribution = TransportContribution(
        dataset=DataSet(data_id="annual_curve_fixture", label="Synthetic schema test"),
        labels_by_table={"technology_label": [TechnologyLabel(tech=key["tech"])]},
        rows_by_table={"technology": [Technology(
            tech=key["tech"], category="cars", flag="p", data_id="annual_curve_fixture")]},
        load_results=[],
        parameter_rows=[ExistingCapacity(**key, capacity=100, data_id="annual_curve_fixture")],
        lifetime_process_rows=lifetimes,
        lifetime_survival_curve_rows=curves,
        provenance_contexts=[context],
        existing_vintages=(2020,),
        expected_survival_curve_keys=(("ON", key["tech"], 2020),),
        prepared_support_tables=("existing_capacity", "lifetime_tech",
                                 "lifetime_process", "lifetime_survival_curve"),
        scenario_name="synthetic_annual_curve_contract",
    )
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)  # Uses the pinned canoe-schema v4 DDL.
        insert_models(connection, [Region(region="ON")])
        insert_models(connection, [TimePeriod(period=2020, flag="e"),
                                   TimePeriod(period=2025, flag="f"),
                                   TimePeriod(period=2030, flag="f")])
        inserted = insert_transport_contribution(connection, contribution)
        assert contribution.support_audit["ok"]
        assert connection.execute(
            "SELECT period, fraction FROM lifetime_survival_curve ORDER BY period"
        ).fetchall() == list(zip(range(2020, 2038), fractions, strict=True))
        assert connection.execute(
            "SELECT vintage + lifetime FROM lifetime_process"
        ).fetchone() == (2037,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lifetime_survival_curve "
            "WHERE period NOT IN (SELECT period FROM time_period)"
        ).fetchone() == (15,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lifetime_survival_curve "
            "WHERE period > (SELECT MAX(period) FROM time_period)"
        ).fetchone() == (7,)
        for table in ("lifetime_process", "lifetime_survival_curve"):
            fields = context.parameter_fields(table)
            expected = tuple(fields[field] for field in
                             ("data_source", "dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"))
            assert connection.execute(
                f"SELECT DISTINCT data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time FROM {table}"
            ).fetchall() == [expected]
        report = validate_database(
            connection,
            expected_primary_keys={table: [tuple(getattr(row, field) for field in row.__primary_key__)
                                           for row in rows]
                                   for table, rows in inserted.items()},
        )
        assert report["ok"], report["errors"]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
