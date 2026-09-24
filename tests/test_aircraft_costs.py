"""FAA aircraft service capacity and maintenance dimensions."""

import pandas as pd
import pytest

from parameterization.offroad_capex_opex import (
    aircraft_cost_per_billion_service,
    aircraft_service_evidence,
)
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules


def test_faa_aggregate_passenger_and_cargo_dimensions() -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    conversions = load_conversion_factors(bundle)
    rules = load_harmonization_rules(bundle, "costs")
    capacity = pd.read_csv(
        "inputs/1_interim/fetched_assorted_sources/faa_aircraft_capacity_load_speed.csv"
    )
    maintenance = pd.read_csv(
        "inputs/1_interim/fetched_assorted_sources/faa_aircraft_maintenance_costs.csv"
    )
    for group, speed, native_capacity, load, hours, hourly_cost in (
        ("passenger", 366, 169, 0.84, 8.5, 1005),
        ("cargo", 399, 93, 0.48, 4.6, 2815),
    ):
        evidence = aircraft_service_evidence(
            capacity,
            operating_group=group,
            mile_to_km=conversions["length"]["mile_to_km"],
            us_short_ton_to_metric_tonne=conversions["mass"]["us_short_ton_to_metric_tonne"],
            operating_days_per_year=rules["aircraft_operating_days_per_year"],
        )
        tonnes = 1 if group == "passenger" else 0.90718474
        expected_hourly_service = speed * 1.609344 * native_capacity * tonnes * load
        assert evidence.annual_service_output == pytest.approx(
            expected_hourly_service * hours * 365
        )
        actual_hourly_cost = maintenance.loc[
            maintenance.operating_group.eq(group), "value"
        ].iloc[0]
        assert actual_hourly_cost == hourly_cost
        assert aircraft_cost_per_billion_service(
            cost_per_block_hour=actual_hourly_cost, evidence=evidence
        ) == pytest.approx(hourly_cost / expected_hourly_service * 1000)
        assert evidence.source_tables == (
            ("Table 3-6", "Table 3-7") if group == "passenger"
            else ("Table 3-9", "Table 3-10")
        )


def test_faa_rejects_missing_aggregate_daily_utilization() -> None:
    capacity = pd.read_csv(
        "inputs/1_interim/fetched_assorted_sources/faa_aircraft_capacity_load_speed.csv"
    )
    capacity = capacity.loc[~capacity.metric.eq("average_daily_utilization")]
    with pytest.raises(ValueError, match="average_daily_utilization"):
        aircraft_service_evidence(
            capacity, operating_group="cargo", mile_to_km=1.609344,
            us_short_ton_to_metric_tonne=0.90718474, operating_days_per_year=365,
        )
