from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import (
    derive_ldv_age_distributions,
    distribute_existing_road_capacity,
)
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    fixed_existing_lifetimes,
    resolved_road_lifetime_rules,
)
from utils import load_config_bundle, load_harmonization_rules
from validation.config_models import ScenarioPeriods
from validation.insertion import cleanup_transport_parameter_batches


ROOT = Path(__file__).resolve().parents[1]


def _varying_powertrain_stock_inputs() -> dict:
    bundle = load_config_bundle(
        "config/scenarios/legacy_comparison.yaml", repo_root=ROOT,
    )
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ].copy()
    rules["ceud_stock_series"] = {"cars": rules["ceud_stock_series"]["cars"]}
    rules["survival_curve_classes"] = ["cars"]
    rules["report5_age_classes"] = {}
    stock_series = rules["ceud_stock_series"]["cars"]
    registrations = pd.DataFrame([
        {
            "scenario_region": "ON", "reference_year": year,
            "vehicle_type": "Passenger cars", "fuel_type": fuel,
            "scaled_value": count, "source_table_id": "20-10-0025-01",
        }
        for year, counts in (
            (2005, {"Gasoline": 90, "Diesel": 10, "Battery electric": 0}),
            (2015, {"Gasoline": 75, "Diesel": 15, "Battery electric": 10}),
            (2023, {"Gasoline": 50, "Diesel": 10, "Battery electric": 40}),
        )
        for fuel, count in counts.items()
    ])
    return {
        "provincial": pd.DataFrame([
            {
                "region": "ON", "year": 2023,
                "table_id": stock_series["table_id"],
                "raw_series": stock_series["raw_series"],
                "unit": "thousands", "value": 100.0,
            }
        ]),
        "ldv_age": pd.DataFrame([
            {
                "report_year": 2025, "nrcan_ceud_class": road_class,
                "age": age, "fit_active_stock": count,
            }
            for road_class in ("Car", "Light Truck")
            for age, count in ((0, 30), (8, 20), (23, 50), (24, 100))
        ]),
        "report5_age": pd.DataFrame(),
        "ldv_registrations": registrations,
        "truck_registrations": pd.DataFrame(),
        "dashboard": pd.DataFrame(columns=["source_region", "ytd_market_share"]),
        "regions": ["ON"],
        "period_config": ScenarioPeriods(
            period_mode="prospective", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020], model=[2025], step=5,
        ),
        "vehicle_population_year": 2025,
        "survival_curves": True,
        "survival_curve_max_age": 29,
        "fixed_lifetimes_by_class": {"cars": 14},
        "curve_ages_by_class": {"cars": set(range(30))},
        "rules": rules,
    }


@pytest.mark.parametrize("survival_curves", [False, True])
def test_production_age_coverage_is_independent_of_lifetime_mode(
    survival_curves: bool,
) -> None:
    periods = _varying_powertrain_stock_inputs()["period_config"]
    mapped = pd.DataFrame({
        "report_year": [2025] * 5,
        "MODEL_YEAR": [2025, 2011, 2002, 2001, 2026],
        "FIT_ACTIVE": [10, 20, 30, 40, 50],
        "nrcan_ceud_class": ["Car"] * 5,
        "nlr_atb_class": ["Compact"] * 5,
        "mapping_accepted": [True] * 5,
    })
    distribution, findings = derive_ldv_age_distributions(
        mapped, survival_curves=survival_curves, maximum_age=10,
        median_lifetimes={"Car": 14}, historical_review_minimum_model_year=2000,
        period_config=periods,
    )
    assert distribution.age.tolist() == [0, 14, 23]
    assert distribution.fit_active_stock.sum() == 60
    assert set(findings.cutoff_basis) == {"configured_historical_vintage_coverage"}
    assert findings.cutoff_age.tolist() == [23.0]
    assert findings.over_cutoff_fit_active_stock.tolist() == [40]
    assert findings.negative_age_fit_active_stock.tolist() == [50]


def test_powertrain_totals_survive_lifetime_modes_and_tight_curve_support() -> None:
    arguments = _varying_powertrain_stock_inputs()
    expected = {
        "T_LDV_C_GSL_EX": 75.0,
        "T_LDV_C_DSL_EX": 11.0,
        "T_LDV_C_BEV_EX": 14.0,
    }
    for curves, maximum_age in ((True, 29), (False, 29), (True, 9)):
        cohorts, shares, _, capacity = distribute_existing_road_capacity(
            **(arguments | {
                "survival_curves": curves,
                "survival_curve_max_age": maximum_age,
            })
        )
        assert capacity.groupby("tech").capacity.sum().to_dict() == pytest.approx(expected)
        assert cohorts.cohort_k_vehicles.sum() == pytest.approx(100.0)
        assert shares.capacity.sum() == pytest.approx(100.0)
        assert not cohorts.loc[
            ~cohorts.eligible_first_model_period, "cohort_k_vehicles"
        ].any()
        assert not shares.loc[
            ~shares.eligible_first_model_period, "capacity"
        ].any()
        assert set(capacity.units) == {"k vehicles"}
        expected_vintages = {2000, 2010, 2020} if curves and maximum_age == 29 else {2020}
        assert set(capacity.vintage) == expected_vintages
        if curves and maximum_age == 29:
            # All represented ages survive: retain the original curve-mode rows.
            assert capacity.loc[
                capacity.tech.eq("T_LDV_C_BEV_EX"), ["vintage", "capacity"]
            ].set_index("vintage").capacity.to_dict() == pytest.approx({2010: 2.0, 2020: 12.0})
        if not curves:
            # Age eight survives annually but its prospective model vintage does not.
            assert 2023 - 8 + 14 > 2025
            assert not cohorts.loc[cohorts.age.eq(8), "eligible_first_model_period"].any()
        for (region, road_class, vintage_year, vintage), group in shares.groupby(
            ["region", "road_class", "vintage_year", "vintage"]
        ):
            cohort = cohorts.loc[
                cohorts.region.eq(region) & cohorts.road_class.eq(road_class)
                & cohorts.vintage_year.eq(vintage_year) & cohorts.vintage.eq(vintage)
            ]
            assert group.capacity.sum() == pytest.approx(cohort.cohort_k_vehicles.sum())
            assert group.cohort_k_vehicles.unique().tolist() == pytest.approx(
                [group.capacity.sum()]
            )


def test_positive_powertrain_without_eligible_cohort_is_rejected() -> None:
    arguments = _varying_powertrain_stock_inputs()
    registrations = arguments["ldv_registrations"].copy()
    registrations.loc[
        registrations.reference_year.eq(2023) & registrations.fuel_type.eq("Diesel"),
        "scaled_value",
    ] = 0
    with pytest.raises(ValueError, match="no eligible same-technology cohort.*T_LDV_C_DSL_EX"):
        distribute_existing_road_capacity(**(arguments | {
            "ldv_registrations": registrations,
            "survival_curve_max_age": 9,
        }))


def test_zero_powertrain_stock_does_not_require_an_eligible_cohort() -> None:
    arguments = _varying_powertrain_stock_inputs()
    registrations = arguments["ldv_registrations"].copy()
    registrations.loc[registrations.fuel_type.eq("Diesel"), "scaled_value"] = 0
    _, shares, _, capacity = distribute_existing_road_capacity(**(arguments | {
        "ldv_registrations": registrations,
        "survival_curve_max_age": 9,
    }))
    assert shares.loc[shares.tech.eq("T_LDV_C_DSL_EX"), "capacity"].sum() == 0
    assert "T_LDV_C_DSL_EX" not in set(capacity.tech)
    assert capacity.capacity.sum() == pytest.approx(100.0)


def test_curve_eligibility_requires_the_complete_model_vintage_age_block() -> None:
    arguments = _varying_powertrain_stock_inputs()
    with pytest.raises(ValueError, match="no eligible same-technology cohort"):
        distribute_existing_road_capacity(**(arguments | {
            "survival_curve_max_age": 5,
        }))


def test_fixed_lifetime_sources_resolve_nhtsa_and_nems_medians() -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_comparison.yaml", repo_root=ROOT
    )
    rules = resolved_road_lifetime_rules(bundle)
    lifetimes = fixed_existing_lifetimes(
        derive_accepted_lifetime_frames(
            bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
            road_rules=load_harmonization_rules(bundle, "road_aggregation"),
            assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
        )["medians"],
        pd.read_csv(ROOT / "inputs/0_manual_params/lifetime_process.csv"),
        rules,
    )

    assert lifetimes == {
        "cars": 14.0,
        "passenger_light_trucks": 16.0,
        "freight_light_trucks": 16.0,
        "medium_trucks": 18.0,
        "heavy_trucks": 25.0,
        "motorcycles": 17.0,
    }


@pytest.mark.parametrize("survival_curves", [False, True])
def test_truck_stock_eligibility_traces_selected_lifetime_source(
    tmp_path: Path, survival_curves: bool,
) -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_comparison.yaml", repo_root=ROOT,
    )
    routes = dict(bundle.paths.artifacts)
    for family in (
        "existing_capacity_interim", "existing_capacity_processed",
        "existing_capacity_validation", "road_stocks_and_demands",
    ):
        routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    scenario = bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON"]}),
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": survival_curves}),
    })
    isolated = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}), scenario=scenario)
    rows, contexts, _ = prepare_existing_capacity_rows(isolated)
    motorcycles = [row for row in rows if row.tech == "T_LDV_M_GSL_EX"]
    assert motorcycles
    assert all(row.vintage + 17 > scenario.periods.model[0] for row in motorcycles)
    by_id = {context.data_id: context for context in contexts}
    latest_car = max(
        (row for row in rows if row.tech == "T_LDV_C_GSL_EX"), key=lambda row: row.vintage,
    )
    history_components = {
        component.component_key
        for contributor in by_id[latest_car.data_id].contributors
        if contributor.source_key == "statcan_transport_tables"
        for component in contributor.components
    }
    # Retired historical cohorts still determine the retained technology total.
    assert {"20-10-0021-01", "20-10-0025-01"} <= history_components
    for prefix, required, absent in (
        ("T_MDV_T_", "nhtsa_cafe_ldv_survival", "eia_nems_hd_truck_scrappage"),
        (
            "T_HDV_T_",
            "eia_nems_hd_truck_scrappage",
            "epa_moves4_population_activity_2023",
        ),
    ):
        selected = [row for row in rows if row.tech.startswith(prefix)]
        assert selected
        for row in selected:
            sources = {item.source_key for item in by_id[row.data_id].contributors}
            assert required in sources
            assert absent not in sources
    if not survival_curves:
        heavy = [row for row in rows if row.tech.startswith("T_HDV_T_")]
        assert all(row.vintage + 25 > scenario.periods.model[0] for row in heavy)
        assert any(row.vintage == 2005 for row in heavy)


def test_road_vintage_fuel_shares_and_dashboard_override() -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_comparison.yaml", repo_root=ROOT
    )
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    stocks = {
        "cars": 100.0,
        "passenger_light_trucks": 200.0,
        "freight_light_trucks": 50.0,
        "medium_trucks": 10.0,
        "heavy_trucks": 20.0,
        "motorcycles": 5.0,
    }
    provincial = pd.DataFrame(
        [
            {
                "region": "ON",
                "year": 2023,
                "table_id": item["table_id"],
                "raw_series": item["raw_series"],
                "unit": "thousands",
                "value": stocks[name],
            }
            for name, item in rules["ceud_stock_series"].items()
        ]
    )
    ldv_age = pd.DataFrame(
        [
            {
                "report_year": 2025,
                "nrcan_ceud_class": group,
                "age": age,
                "fit_active_stock": 50,
            }
            for group in ("Car", "Light Truck")
            for age in (0, 3, 20, 24)
        ]
    )
    report5 = pd.DataFrame(
        [
            {"year": 2025, "VEHICLE_CLASS": group, "AGE": age, "AGE_DIST": 1 / 3}
            for group in ("COMMERCIAL", "MOTORCYCLE")
            for age in (0, 3, 30)
        ]
    )
    ldv = pd.DataFrame(
        [
            {
                "scenario_region": "ON",
                "reference_year": 2023,
                "vehicle_type": vehicle_type,
                "fuel_type": fuel,
                "scaled_value": count,
                "source_table_id": "20-10-0025-01",
            }
            for vehicle_type in ("Passenger cars", "Pickup trucks")
            for fuel, count in (
                ("Gasoline", 80),
                ("Battery electric", 10),
                ("Other fuel types", 10),
            )
        ]
        + [
            {
                "scenario_region": "ON",
                "reference_year": 2011,
                "vehicle_type": vehicle_type,
                "fuel_type": fuel,
                "scaled_value": count,
                "source_table_id": "20-10-0021-01",
            }
            for vehicle_type in ("Passenger cars", "Pickup trucks")
            for fuel, count in (
                ("Gasoline", 80),
                ("Diesel", 10),
                ("Battery electric", 10),
            )
        ]
    )
    truck = pd.DataFrame(
        [
            {
                "scenario_region": "ON",
                "reference_period": 2019,
                "vehicle_type": vehicle_type,
                "fuel_type": fuel,
                "scaled_value": count,
            }
            for vehicle_type in rules["statcan_medium_source_types"]
            for fuel, count in (
                ("Gasoline", 40),
                ("Diesel", 50),
                ("Battery electric", 5),
                ("Other fuel types", 5),
            )
        ]
        + [
            {
                "scenario_region": "ON",
                "reference_period": 2023,
                "vehicle_type": rules["statcan_heavy_source_type"],
                "fuel_type": fuel,
                "scaled_value": count,
            }
            for fuel, count in (("Gasoline", 2), ("Diesel", 98))
        ]
    )
    dashboard = pd.DataFrame([{"source_region": "Ontario", "ytd_market_share": 0.03}])

    cohorts, shares, excluded, capacity = distribute_existing_road_capacity(
        provincial=provincial,
        ldv_age=ldv_age,
        report5_age=report5,
        ldv_registrations=ldv,
        truck_registrations=truck,
        dashboard=dashboard,
        regions=["ON"],
        period_config=ScenarioPeriods(
            period_mode="legacy", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020, 2023], model=[2025], step=5,
        ),
        vehicle_population_year=2025,
        survival_curves=True,
        survival_curve_max_age=25,
        fixed_lifetimes_by_class={
            "cars": 14,
            "passenger_light_trucks": 16,
            "freight_light_trucks": 16,
            "medium_trucks": 25,
            "heavy_trucks": 25,
            "motorcycles": 17,
        },
        curve_ages_by_class={
            name: set(range(26))
            for name in rules["survival_curve_classes"]
        },
        rules=rules,
    )

    assert capacity["capacity"].sum() == pytest.approx(sum(stocks.values()))
    assert set(cohorts.loc[cohorts["cohort_k_vehicles"].gt(0), "vintage"]) == {
        2005,
        2020,
        2023,
    }
    assert set(shares.loc[shares["road_class"].eq("cars"), "source_year"]) == {
        2011,
        2023,
    }
    historical_cars = shares.loc[
        shares["road_class"].eq("cars") & shares["vintage_year"].eq(2003)
    ]
    assert set(historical_cars["fuel_type"]) == {"Gasoline", "Diesel"}
    assert historical_cars["historical_combustion_backfill"].all()
    assert "pre_evidence_non_combustion" in set(excluded["exclusion_reason"])
    assert not cohorts.loc[
        cohorts["road_class"].eq("cars") & cohorts["age"].eq(24),
        "eligible_first_model_period",
    ].any()
    medium_latest = shares.loc[
        shares["road_class"].eq("medium_trucks") & shares["vintage"].eq(2023)
    ]
    assert set(shares.loc[shares.tech.eq("T_MDV_T_BEV_EX"), "vintage"]) == {2023}
    assert "configured_latest_vintage_only_fuel" in set(excluded.exclusion_reason)
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Battery electric"), "fuel_share"
    ].iloc[0] == pytest.approx(0.03)
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Gasoline"), "fuel_share"
    ].iloc[0] == pytest.approx(0.97 * 40 / 90)
    assert not capacity["tech"].str.contains("CHRG").any()
    assert "Other fuel types" in set(excluded["fuel_type"])
    assert "Battery electric" in set(
        shares.loc[shares["road_class"].eq("freight_light_trucks"), "fuel_type"]
    )
    assert set(shares.loc[
        shares.road_class.eq("freight_light_trucks") & shares.fuel_type.eq("Battery electric"), "tech"
    ]) == {"T_LDV_LTF_BEV_EX"}
    assert set(shares.loc[shares["road_class"].eq("heavy_trucks"), "fuel_type"]) == {
        "Diesel"
    }
    assert "Gasoline" in set(
        excluded.loc[excluded["road_class"].eq("heavy_trucks"), "fuel_type"]
    )


def test_cleanup_uses_region_tech_vintage_and_preserves_future_rows() -> None:
    def row(region: str, tech: str, vintage: int, capacity: float | None = None):
        payload = {"region": region, "tech": tech, "vintage": vintage}
        if capacity is not None:
            payload.update(capacity=capacity, units="k vehicles")
        return SimpleNamespace(**payload)

    capacity = [
        row("ON", "EX", 2023, 0.00001),
        row("QC", "EX", 2023, 1.0),
    ]
    efficiency = [
        row("ON", "EX", 2023),
        row("QC", "EX", 2023),
        row("ON", "EX", 2025),
    ]
    cost = [row("ON", "EX", 2023), row("QC", "EX", 2023)]
    invest = [row("ON", "EX", 2023), row("QC", "EX", 2023)]

    cleaned, removed = cleanup_transport_parameter_batches(
        {
            "existing_capacity": capacity,
            "efficiency": efficiency,
            "cost_fixed": cost,
            "cost_invest": invest,
        },
        existing_periods=[2023],
        first_model_period=2025,
        epsilon=0.0001,
    )

    assert [(r.region, r.vintage) for r in cleaned["existing_capacity"]] == [
        ("QC", 2023)
    ]
    assert [(r.region, r.vintage) for r in cleaned["efficiency"]] == [
        ("QC", 2023),
        ("ON", 2025),
    ]
    assert [(r.region, r.vintage) for r in cleaned["cost_fixed"]] == [("QC", 2023)]
    assert [(r.region, r.vintage) for r in cleaned["cost_invest"]] == [("QC", 2023)]
    assert {(item["table"], item["region"]) for item in removed} == {
        ("existing_capacity", "ON"),
        ("efficiency", "ON"),
        ("cost_fixed", "ON"),
        ("cost_invest", "ON"),
    }
