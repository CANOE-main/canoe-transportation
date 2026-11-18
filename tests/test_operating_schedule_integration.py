"""Registered offline preparation reconciles operating schedules across owners."""

from dataclasses import replace
import sqlite3

import pandas as pd

from build_transport import bootstrap_database
from parameterization.build_costs import prepare_cost_rows
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.build_lifetime_parameters import prepare_lifetime_rows
from parameterization.road_utilization import prepare_road_utilization
from utils import load_harmonization_rules, resolve_artifact_path, resolve_input_path


def test_registered_operating_schedules_share_complete_lifetime_coverage(isolated_legacy_bundle):
    """Replay real evidence privately; independently enumerate every live process."""
    original = isolated_legacy_bundle
    observed_counts = []
    for curves in (False, True):
        scenario = original.scenario.model_copy(update={
            "geography": original.scenario.geography.model_copy(update={"regions": ["ON", "NL"]}),
            "lifetimes": original.scenario.lifetimes.model_copy(update={"survival_curves": curves}),
            "road_utilization": original.scenario.road_utilization.model_copy(update={"vkt_schedules": True}),
        })
        bundle = replace(original, scenario=scenario)
        stock = prepare_existing_capacity_rows(bundle)
        lifetimes = prepare_lifetime_rows(
            bundle, annual_acquisitions=stock.annual_acquisitions,
            cohort_provenance_contexts=stock.provenance_contexts,
        )
        utilization = prepare_road_utilization(
            bundle, existing_capacity_rows=stock.existing_capacity_rows,
            existing_capacity_contexts=stock.provenance_contexts,
            physical_stock=stock.physical_stock, lifetime_preparation=lifetimes,
        )
        costs = prepare_cost_rows(
            bundle, existing_capacity_rows=stock.existing_capacity_rows,
            existing_capacity_contexts=stock.provenance_contexts,
            fixed_lifetime_rows=lifetimes.fixed_rows, survival_curve_rows=lifetimes.curve_rows,
            lifetime_provenance_contexts=lifetimes.provenance_contexts,
        )
        activity = pd.DataFrame(utilization.audit["base_year_activity_calibration"])
        motorcycle = activity.loc[activity.road_class.eq("motorcycles")].set_index("region")
        cost_rules = load_harmonization_rules(bundle, "costs")
        cost_audit = pd.read_csv(resolve_artifact_path(bundle, "costs_interim", cost_rules["files"]["road_variable"]))
        motorcycle_costs = cost_audit.loc[cost_audit["mode"].eq("motorcycles")]
        assert set(motorcycle_costs.region) == set(motorcycle.index)
        for row in motorcycle_costs.itertuples(index=False):
            expected = motorcycle.loc[row.region]
            annual_service = 1000 * expected.target_activity_bn / expected.retained_stock_k_vehicles
            assert abs(row.cost * annual_service - row.target_cad_value) < 1e-8
            assert "nrcan_ceud_transport_provincial/32" in row.source_components
            assert "cer_canadas_energy_future/macro-indicators" in row.source_components
            assert "ontario_ministry_transport_vehicle_population/5" in row.source_components
        technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
        age_classes = load_harmonization_rules(bundle, "road_utilization")["age_profile_classes"]
        age_techs = set(technology.loc[technology.category.isin(age_classes), "tech"])
        fixed = {(row.region, row.tech): row.lifetime for row in lifetimes.fixed_rows}
        terminals = {(row.region, row.tech, row.vintage): row.period for row in lifetimes.curve_rows
                     if row.fraction == 0}
        positive_stock = {(row.region, row.tech, row.vintage) for row in stock.existing_capacity_rows
                          if row.capacity > 0}
        if curves:
            assert {key for key in terminals if key[2] in scenario.periods.existing} == {
                key for key in positive_stock if key[1] in age_techs
            }
        expected = set()
        region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["region_output_map"]
        for source_region in scenario.geography.regions:
            region = region_map.get(source_region, source_region)
            for tech in age_techs:
                vintages = (sorted(vintage for r, t, vintage in positive_stock if (r, t) == (region, tech))
                            if tech.endswith("_EX") else scenario.periods.model)
                for vintage in vintages:
                    end = terminals[region, tech, vintage] if curves else vintage + fixed[region, tech]
                    expected.update((region, tech, vintage, period) for period in scenario.periods.model
                                    if vintage <= period < end)
        rows = utilization.age_factor_artifact
        assert rows is not None
        actual = set(rows[["region", "tech_or_group", "vintage", "period"]].itertuples(index=False, name=None))
        assert actual == expected
        cost_keys = {(row.region, row.tech, row.vintage, row.period) for row in costs.variable_rows
                     if row.tech in age_techs}
        assert cost_keys == expected
        road_classes = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]["activity_series"]
        road_techs = set(technology.loc[technology.category.isin(road_classes), "tech"])
        complete_expected = set()
        for source_region in scenario.geography.regions:
            region = region_map.get(source_region, source_region)
            for tech in road_techs:
                vintages = (sorted(vintage for r, t, vintage in positive_stock if (r, t) == (region, tech))
                            if tech.endswith("_EX") else scenario.periods.model)
                for vintage in vintages:
                    end = (vintage + fixed[region, tech] if (region, tech) in fixed
                           else terminals[region, tech, vintage])
                    complete_expected.update((region, tech, vintage, period)
                                             for period in scenario.periods.model if vintage <= period < end)
        complete = utilization.period_factor_artifact
        assert complete is not None
        complete_keys = set(complete[["region", "tech_or_group", "vintage", "period"]].itertuples(index=False, name=None))
        assert complete_keys == complete_expected
        assert {(row.region, row.tech, row.vintage, row.period) for row in costs.variable_rows
                if row.tech in road_techs} == complete_expected
        assert rows.factor.gt(0).all() and rows.factor.lt(1).all()
        contexts = {item.data_id: item for item in utilization.provenance_contexts}
        assert set(rows.data_id) <= contexts.keys()
        assert set(complete.data_id) <= contexts.keys()
        for data_id in set(rows.data_id):
            context = contexts[data_id]
            context.registry_rows()  # Valid package provenance rows, ready for registration.
            sources = {item.source_key for item in context.contributors}
            assert {"nlr_atb_transportation", "nrcan_ceud_transport_provincial",
                    "cer_canadas_energy_future"} <= sources
            assert sources & {"epa_moves_heavy_truck_survival", "nhtsa_cafe_ldv_survival",
                              "eia_nems_hd_truck_scrappage"}
        if curves:
            assert utilization.audit["age_ceiling"] >= max(scenario.periods.model) + scenario.periods.step - min(scenario.periods.existing)
        else:
            assert (rows.age_last_year <= rows.retirement_year - rows.vintage).all()
            assert (rows.age_last_year < rows.period + rows.period_step - rows.vintage).any()
        observed_counts.append(len(rows))
        # Exercise the current schema's production insertion with the same lifetime
        # selection. Age factors remain behind the schema guard until dual indexing.
        flat_scenario = scenario.model_copy(update={
            "road_utilization": scenario.road_utilization.model_copy(update={"vkt_schedules": False}),
        })
        flat_bundle = replace(bundle, scenario=flat_scenario)
        database = bundle.repo_root / "outputs" / f"operating_schedules_{curves}.sqlite"
        report = bootstrap_database(
            bundle=flat_bundle, template_dir=resolve_input_path(bundle, "template"),
            database_path=database,
        )
        assert report["ok"]
        with sqlite3.connect(database) as connection:
            assert connection.execute(
                "SELECT region, tech FROM lifetime_tech "
                "EXCEPT SELECT region, tech FROM efficiency "
                "EXCEPT SELECT region, tech FROM existing_capacity"
            ).fetchall() == []
            assert ("NLLAB", "T_HDV_BS_CNG_EX") in report["lifetimes"]["inactive_fixed_lifetime_keys"]
            inserted_costs = {tuple(row) for row in connection.execute(
                "SELECT region, tech, vintage, period FROM cost_variable"
            ) if row[1] in road_techs}
            assert inserted_costs == complete_expected
            assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert observed_counts[1] > observed_counts[0]
