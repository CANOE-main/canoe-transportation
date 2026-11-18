"""The shared road handoff preserves physical and solver stock boundaries."""

import pandas as pd

from parameterization.build_existing_capacity import annual_road_stock_evidence


def test_common_annual_stock_preserves_raw_bus_years_and_distinct_curve_capacity():
    road = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "tech": "CAR_EX", "vintage": 2020,
         "vintage_year": 2021, "raw_capacity": 8.0, "capacity": 10.0,
         "solver_capacity_k_vehicles": 16.0},
        {"region": "ON", "road_class": "motorcycles", "tech": "MOTORCYCLE_EX",
         "vintage": 2020, "vintage_year": 2022, "raw_capacity": 3.0,
         "capacity": 4.0, "solver_capacity_k_vehicles": 4.0},
    ])
    # Both old buses use the earliest CEUD fuel-share point, but they remain
    # distinct physical age cohorts. A retired cohort still supplies raw weights.
    buses = pd.DataFrame([
        {"region": "ON", "road_class": "school_buses", "tech": "BUS_EX",
         "vintage": 2000, "vintage_year": 1993, "source_year": 2000,
         "raw_cohort_k_vehicles": 2.0, "capacity": 0.0},
        {"region": "ON", "road_class": "school_buses", "tech": "BUS_EX",
         "vintage": 2000, "vintage_year": 1994, "source_year": 2000,
         "raw_cohort_k_vehicles": 3.0, "capacity": 0.0},
        {"region": "ON", "road_class": "school_buses", "tech": "BUS_EX",
         "vintage": 2020, "vintage_year": 2021, "source_year": 2021,
         "raw_cohort_k_vehicles": 5.0, "capacity": 10.0},
    ])
    before_road, before_buses = road.copy(deep=True), buses.copy(deep=True)
    common = annual_road_stock_evidence(road, buses)
    pd.testing.assert_frame_equal(road, before_road)
    pd.testing.assert_frame_equal(buses, before_buses)
    pd.testing.assert_frame_equal(common.iloc[:len(road)].reset_index(drop=True), road)
    bus = common.loc[common.road_class.eq("school_buses")]
    assert bus.vintage_year.tolist() == [1993, 1994, 2021]
    assert bus.raw_capacity.tolist() == [2.0, 3.0, 5.0]
    assert bus.capacity.tolist() == [0.0, 0.0, 10.0]
    assert bus.solver_capacity_k_vehicles.tolist() == [0.0, 0.0, 10.0]
    assert not common.duplicated(["region", "tech", "vintage", "vintage_year"]).any()


def test_common_annual_stock_allows_an_empty_bus_selection():
    columns = ["region", "road_class", "tech", "vintage", "vintage_year",
               "raw_capacity", "capacity", "solver_capacity_k_vehicles"]
    road = pd.DataFrame(columns=columns)
    buses = pd.DataFrame(columns=["region", "road_class", "tech", "vintage", "vintage_year",
                                  "raw_cohort_k_vehicles", "capacity"])
    common = annual_road_stock_evidence(road, buses)
    assert common.empty
    assert common.columns.tolist() == columns
