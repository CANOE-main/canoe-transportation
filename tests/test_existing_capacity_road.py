from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization.road_stocks_and_demands import (
    distribute_existing_road_capacity,
)
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    fixed_existing_lifetimes,
    resolved_road_lifetime_rules,
)
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.config_models import ScenarioPeriods
from validation.insertion import cleanup_transport_parameter_batches


ROOT = Path(__file__).resolve().parents[1]


def test_fixed_lifetime_sources_resolve_nhtsa_and_nems_medians(isolated_legacy_bundle) -> None:
    bundle = isolated_legacy_bundle
    rules = resolved_road_lifetime_rules(bundle)
    lifetimes = fixed_existing_lifetimes(
        derive_accepted_lifetime_frames(
            bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
            road_rules=load_harmonization_rules(bundle, "road_aggregation"),
            assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
        )["medians"],
        pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv")),
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
    isolated_legacy_bundle, tmp_path: Path, survival_curves: bool,
) -> None:
    bundle = isolated_legacy_bundle
    routes = dict(bundle.paths.artifacts)
    for family in (
        "existing_capacity_interim", "existing_capacity_processed",
        "existing_capacity_validation",
    ):
        routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    scenario = bundle.scenario.model_copy(update={
        "geography": bundle.scenario.geography.model_copy(update={"regions": ["ON"]}),
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": survival_curves}),
    })
    isolated = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}), scenario=scenario)
    prepared = prepare_existing_capacity_rows(isolated)
    rows, contexts = prepared.existing_capacity_rows, prepared.provenance_contexts
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
    else:
        assert prepared.audit["grouped_tail_reference_loss_k_vehicles"] == 0
        assert prepared.audit["vintage_curve_audit"]
        assert sum(row.capacity for row in rows if row.units == "k vehicles") > prepared.physical_stock.surviving_k_vehicles.sum()
        assert prepared.annual_acquisitions.calibrated_acquisitions_k_vehicles.gt(0).all()
        retained_keys = {(row.region, row.tech, row.vintage) for row in rows}
        acquired_keys = set(prepared.annual_acquisitions[["region", "tech", "vintage"]].itertuples(index=False, name=None))
        assert acquired_keys <= retained_keys


@pytest.fixture
def road_fuel_allocation_inputs():
    bundle = load_config_bundle(
        "config/scenarios/validation_opening_periods.yaml", repo_root=ROOT
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
    sales_cohorts = pd.DataFrame([
        {"ceud_region": "ON", "road_class": name, "vintage_year": year,
         "age": 2023 - year, "sales_k_vehicles": stocks[name] / 3,
         "surviving_k_vehicles": stocks[name] / 3, "stock_calibration_multiplier": 1.,
         "stock_calibration_basis": "base_year_ceud_stock",
         "calibrated_surviving_k_vehicles": stocks[name] / 3,
         "calibrated_acquisitions_k_vehicles": stocks[name] / 3,
         "ceud_stock_k_vehicles": stocks[name]}
        for name in rules["ceud_sales_series"] for year in (2003, 2020, 2023)
    ])
    annual_survival = pd.DataFrame([
        {"road_class": name, "age": age, "survival_probability": float(age < 40)}
        for name in rules["ceud_sales_series"] for age in range(41)
    ])
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
                "reference_period": source_year,
                "vehicle_type": vehicle_type,
                "fuel_type": fuel,
                "scaled_value": count,
            }
            for source_year in (2019, 2023)
            for vehicle_type in rules["statcan_medium_source_types"]
            for fuel, count in (
                ("Gasoline", 40),
                ("Diesel", 50),
                ("Battery electric", 5 if source_year == 2019 else 10),
                ("Hybrid electric", 3),
                ("Plug-in hybrid electric", 2),
                ("Other fuel types", 5),
            )
        ]
        + [
            {"scenario_region": "ON", "reference_period": 2023,
             "vehicle_type": "Class 7 vehicles, weighing 11,794 to 14,969 kilograms",
             "fuel_type": fuel, "scaled_value": value}
            for fuel, value in (("Gasoline", 10000), ("Diesel", 20000),
                                ("Battery electric", float("nan")))
        ]
        + [
            {
                "scenario_region": "ON",
                "reference_period": 2023,
                "vehicle_type": rules["statcan_heavy_source_type"],
                "fuel_type": fuel,
                "scaled_value": count,
            }
            for fuel, count in (("Gasoline", 2), ("Diesel", 98), ("Battery electric", 5))
        ]
    )
    return dict(
        provincial=provincial,
        sales_cohorts=sales_cohorts,
        annual_survival=annual_survival,
        report5_age=report5,
        ldv_registrations=ldv,
        truck_registrations=truck,
        regions=["ON"],
        period_config=ScenarioPeriods(
            period_mode="legacy", base_year=2023,
            existing=[2000, 2005, 2010, 2015, 2020, 2023], model=[2025], step=5,
        ),
        vehicle_population_year=2025,
        survival_curves=False,
        survival_curve_max_age=40,
        fixed_lifetimes_by_class={
            "cars": 14,
            "passenger_light_trucks": 16,
            "freight_light_trucks": 16,
            "medium_trucks": 25,
            "heavy_trucks": 25,
            "motorcycles": 17,
        },
        curve_ages_by_class={
            name: set(range(41))
            for name in rules["survival_curve_classes"]
        },
        rules=rules,
    )


@pytest.mark.parametrize("survival_curves", [False, True])
def test_road_vintage_fuel_shares_use_statcan_truck_registrations(
    road_fuel_allocation_inputs, survival_curves: bool,
) -> None:
    inputs = {**road_fuel_allocation_inputs, "survival_curves": survival_curves}
    assert inputs["rules"]["statcan_medium_source_types"] == [
        "Total, vehicles weighing 4,536 to 11,793 kilograms"
    ]
    cohorts, shares, excluded, capacity = distribute_existing_road_capacity(**inputs)
    stocks = {"cars": 100., "passenger_light_trucks": 200., "freight_light_trucks": 50.,
              "medium_trucks": 10., "heavy_trucks": 20., "motorcycles": 5.}

    assert capacity["capacity"].sum() == pytest.approx(sum(stocks.values()))
    assert set(cohorts.loc[cohorts["cohort_k_vehicles"].gt(0), "vintage"]) == {
        2005, 2020, 2023,
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
    medium_latest = shares.loc[
        shares["road_class"].eq("medium_trucks") & shares["vintage"].eq(2023)
    ]
    assert set(shares.loc[shares.tech.eq("T_MDV_T_BEV_EX"), "vintage"]) == {2020, 2023}
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Battery electric"), "fuel_share"
    ].iloc[0] == pytest.approx(10 / 100)
    assert medium_latest.loc[
        medium_latest["fuel_type"].eq("Gasoline"), "fuel_share"
    ].iloc[0] == pytest.approx(40 / 100)
    medium_earlier = shares.loc[shares.road_class.eq("medium_trucks") & shares.vintage_year.eq(2020)]
    assert medium_earlier.loc[medium_earlier.fuel_type.eq("Battery electric"), "fuel_share"].iloc[0] == pytest.approx(5 / 95)
    assert set(medium_earlier.source_year) == {2019}
    assert set(medium_latest.source_year) == {2023}
    by_class = shares.groupby("road_class").capacity.sum()
    for name, count in stocks.items():
        assert by_class[name] == pytest.approx(count)
    medium_annual = shares.loc[shares.road_class.eq("medium_trucks")].groupby("vintage_year")
    assert medium_annual.raw_acquisitions_k_vehicles.sum().to_dict() == pytest.approx({2003: 10 / 3, 2020: 10 / 3, 2023: 10 / 3})
    ldv_latest = shares.loc[shares.road_class.eq("cars") & shares.vintage_year.eq(2023)]
    assert set(ldv_latest.source_table_ids) == {"20-10-0025-01"}
    assert ldv_latest.loc[ldv_latest.fuel_type.eq("Battery electric"), "fuel_share"].iloc[0] == pytest.approx(10 / 90)
    assert set(historical_cars.source_table_ids) == {"20-10-0021-01"}
    assert not capacity["tech"].str.contains("CHRG").any()
    assert "Other fuel types" in set(excluded["fuel_type"])
    medium_excluded = excluded.loc[excluded.road_class.eq("medium_trucks")]
    assert {"Other fuel types", "Hybrid electric", "Plug-in hybrid electric"} <= set(
        medium_excluded.fuel_type
    )
    assert not shares.fuel_type.eq("Other fuel types").any()
    assert set(shares.loc[shares.road_class.eq("medium_trucks"), "source_table_ids"]) == {
        "23-10-0308-01"
    }
    medium_bev = shares.loc[shares.tech.eq("T_MDV_T_BEV_EX")]
    assert medium_bev.raw_acquisitions_k_vehicles.notna().all()
    assert medium_bev.calibrated_acquisitions_k_vehicles.notna().all()
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


@pytest.mark.parametrize("problem", ["missing_row", "suppressed", "duplicate", "negative", "nonfinite"])
def test_medium_share_proxy_rejects_incomplete_supported_counts(road_fuel_allocation_inputs, problem):
    inputs = road_fuel_allocation_inputs
    truck = inputs["truck_registrations"].copy()
    selected = (truck.vehicle_type.eq("Total, vehicles weighing 4,536 to 11,793 kilograms")
                & truck.fuel_type.eq("Battery electric") & truck.reference_period.eq(2023))
    if problem == "missing_row":
        truck = truck.loc[~selected]
    elif problem == "duplicate":
        truck = pd.concat([truck, truck.loc[selected]], ignore_index=True)
    else:
        truck.loc[selected, "scaled_value"] = {
            "suppressed": float("nan"), "negative": -1, "nonfinite": float("inf"),
        }[problem]
    with pytest.raises(ValueError, match="Medium-truck fuel proxy requires complete"):
        distribute_existing_road_capacity(**{**inputs, "truck_registrations": truck})


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
