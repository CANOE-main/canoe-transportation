from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import (
    distribute_existing_road_capacity,
    fixed_existing_lifetimes,
)
from utils import load_config_bundle, load_harmonization_rules
from validation.insertion import cleanup_transport_parameter_batches


ROOT = Path(__file__).resolve().parents[1]


def test_fixed_lifetime_sources_resolve_reviewed_medium_truck_proxy() -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=ROOT
    )
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    lifetimes = fixed_existing_lifetimes(
        pd.read_csv(
            ROOT
            / "inputs/2_processed/road_lifetimes_survival/source_derived_median_lifetimes.csv"
        ),
        pd.read_csv(ROOT / "inputs/0_manual_params/lifetime_process.csv"),
        rules,
    )

    assert lifetimes == {
        "cars": 14.0,
        "passenger_light_trucks": 16.0,
        "freight_light_trucks": 16.0,
        "medium_trucks": 25.0,
        "heavy_trucks": 19.0,
        "motorcycles": 17.0,
    }


def test_road_vintage_fuel_shares_and_dashboard_override() -> None:
    bundle = load_config_bundle(
        "config/scenarios/legacy_reproduction.yaml", repo_root=ROOT
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
        base_year=2023,
        first_model_period=2025,
        survival_curves=True,
        survival_curve_max_age=25,
        fixed_lifetimes_by_class={
            "cars": 14,
            "passenger_light_trucks": 16,
            "freight_light_trucks": 16,
            "medium_trucks": 25,
            "heavy_trucks": 19,
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
    assert "configured_base_year_only_fuel" in set(excluded.exclusion_reason)
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Battery electric"), "fuel_share"
    ].iloc[0] == pytest.approx(0.03)
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Gasoline"), "fuel_share"
    ].iloc[0] == pytest.approx(0.97 * 40 / 90)
    assert not capacity["tech"].str.contains("CHRG").any()
    assert {"Other fuel types", "Battery electric"} <= set(excluded["fuel_type"])
    assert "Battery electric" not in set(
        shares.loc[shares["road_class"].eq("freight_light_trucks"), "fuel_type"]
    )
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
