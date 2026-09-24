"""Efficiency contracts: source-native audit, inverse indexing, schema and coupling."""

from copy import deepcopy
from dataclasses import replace
import socket
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization import efficiencies as layer
from parameterization.offroad_efficiencies import (
    derive_offroad_efficiency,
    manual_ratio,
)
from parameterization.road_efficiencies import (
    RoadEfficiencyEvidence,
    aggregate_atb,
    aggregate_ratings,
    backcast_efficiency,
    classify_ratings,
    derive_load_factors,
    interpolate,
    medium_vocation_weights,
    select_atb_consumption,
)
from utils import (
    file_sha256,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
)


@pytest.fixture
def bundle():
    return load_config_bundle("config/scenarios/legacy_reproduction.yaml")


@pytest.fixture
def rules(bundle):
    return load_harmonization_rules(bundle, "efficiencies")


def test_greet_hhv_and_dimensional_conversions(bundle):
    c = load_conversion_factors(bundle)
    for fuel in ("gasoline", "diesel"):
        si = (
            c["fuel_hhv"][fuel + "_btu_it_per_us_gallon"]
            * c["energy"]["btu_it_to_joule"]
            * c["energy"]["joule_to_mj"]
            / c["volume"]["us_gallon_to_litre"]
        )
        assert si == pytest.approx(c["fuel_hhv"][fuel + "_mj_per_litre"], rel=1e-14)
    assert c["energy"]["kwh_to_mj"] * c["energy"]["wh_to_kwh"] == pytest.approx(
        c["energy"]["wh_to_mj"]
    )
    factor = c["derived"][
        "us_gallon_equivalent_per_mile_to_litre_equivalent_per_100_km"
    ]
    assert factor == pytest.approx(
        c["volume"]["us_gallon_to_litre"]
        / c["length"]["mile_to_km"]
        / c["service"]["per_100_km_to_per_km"]
    )
    assert 1 / ((factor / 50) / factor) == pytest.approx(50)


def test_consumption_backcast_direction_and_interpolation():
    assert backcast_efficiency(2, 20, 30) == pytest.approx(4 / 3)
    series = pd.DataFrame({"year": [2020, 2030], "value": [30, 20]})
    assert interpolate(series, 2023, "value") == 27
    with pytest.raises(ValueError, match="extrapolation"):
        interpolate(series, 2019, "value")
    with pytest.raises(ValueError, match="ambiguous"):
        interpolate(pd.concat([series, series]), 2023, "value")


def test_mhdv_and_bus_backcasts_use_consumption_direction_without_double_load(rules):
    medium = rules["road_classes"]["medium_trucks"]
    bus = rules["road_classes"]["urban_transit"]
    ceud = pd.DataFrame(
        [
            {
                "region": "ON",
                "table_id": table,
                "raw_series": series,
                "unit": unit,
                "year": year,
                "value": value,
            }
            for table, series, unit, values in [
                (
                    medium["stock_table"],
                    medium["cs_prefix"] + "|Diesel Fuel Oil",
                    "L/100 km",
                    [40, 20],
                ),
                (bus["intensity_table"], bus["intensity"], "MJ/Pkm", [2, 1]),
            ]
            for year, value in zip([2000, 2023], values, strict=True)
        ]
    )
    loads = pd.DataFrame(
        [
            {"region": "ON", "mode": mode, "year": year, "load_factor": load}
            for mode in ["medium_trucks", "urban_transit"]
            for year, load in [(2000, 5), (2023, 10)]
        ]
    )
    atb = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": mode,
                "powertrain": "diesel",
                "year": year,
                "weighted_consumption": value,
            }
            for mode in ["medium_trucks", "urban_transit"]
            for year, value in [(2020, 8), (2025, 6), (2030, 4)]
        ]
    )
    ratings = pd.DataFrame(
        columns=[
            "native_class_mean",
            "weight",
            "class_mean_mj_per_vkm",
            "ceud_class",
            "powertrain",
            "year",
        ]
    )
    evidence = RoadEfficiencyEvidence(
        ceud=ceud,
        loads=loads,
        rating_audit=ratings,
        atb_audit=atb,
        gcam=pd.DataFrame(),
        regen=pd.DataFrame(),
        base_year=2023,
        rules=rules,
    )
    assert evidence.derive("ON", "medium_trucks", "diesel", 2000)[
        "efficiency"
    ] == pytest.approx(5 / (6.8 * 2))
    assert evidence.derive("ON", "medium_trucks", "diesel", 2030)[
        "efficiency"
    ] == pytest.approx(10 / 4)
    assert evidence.derive("ON", "urban_transit", "diesel", 2000)[
        "efficiency"
    ] == pytest.approx((10 / 6.8) / 2)


def test_ldv_keeps_last_own_rating_and_indexes_without_future_observations(rules):
    ratings = pd.DataFrame(
        [
            {
                "ceud_class": "Car",
                "powertrain": fuel,
                "year": year,
                "native_class_mean": value,
                "class_mean_mj_per_vkm": value,
                "weight": 1,
            }
            for fuel, year, value in [
                ("gasoline", 2019, 4),
                ("gasoline", 2023, 3),
                ("diesel", 2019, 2),
                ("diesel", 2026, 0.01),
            ]
        ]
    )
    atb = pd.DataFrame(
        [
            {
                "region": "ON",
                "mode": "cars",
                "powertrain": "diesel",
                "year": year,
                "weighted_consumption": value,
            }
            for year, value in [(2023, 10), (2030, 8)]
        ]
    )
    evidence = RoadEfficiencyEvidence(
        ceud=pd.DataFrame(),
        loads=pd.DataFrame(
            {"region": ["ON"], "mode": ["cars"], "year": [2023], "load_factor": [2]}
        ),
        rating_audit=ratings,
        atb_audit=atb,
        gcam=pd.DataFrame(),
        regen=pd.DataFrame(),
        base_year=2023,
        rules=rules,
    )
    assert evidence.rating("cars", "diesel", 2023, "mj") is None
    assert evidence.ldv_baseline("ON", "cars", "diesel") == (1.5, "gasoline")
    assert evidence.derive("ON", "cars", "diesel", 2030)["efficiency"] == pytest.approx(
        2 / (1.5 * 0.8)
    )
    assert any(
        "latest_own_rating_year=2019" in finding[-1] for finding in evidence.findings
    )


def test_load_factor_is_activity_over_vehicle_distance(rules, bundle):
    configured = deepcopy(rules)
    configured["road_classes"] = {"cars": rules["road_classes"]["cars"]}
    activity = {"cars": {"table_id": 20, "raw_series": "activity"}}
    data = pd.DataFrame(
        [
            {
                "region": "ON",
                "year": 2023,
                "table_id": 20,
                "raw_series": "activity",
                "unit": "millions",
                "value": 600,
            },
            {
                "region": "ON",
                "year": 2023,
                "table_id": 21,
                "raw_series": configured["road_classes"]["cars"]["stock"],
                "unit": "thousands",
                "value": 20,
            },
            {
                "region": "ON",
                "year": 2023,
                "table_id": 21,
                "raw_series": configured["road_classes"]["cars"]["distance"],
                "unit": "km",
                "value": 15000,
            },
        ]
    )
    result = derive_load_factors(
        data,
        rules=configured,
        activity_rules=activity,
        conversions=load_conversion_factors(bundle),
    )
    assert result.load_factor.iloc[0] == 2
    with pytest.raises(ValueError, match="wrong-unit"):
        derive_load_factors(
            data.assign(unit="PJ"),
            rules=configured,
            activity_rules=activity,
            conversions=load_conversion_factors(bundle),
        )


def test_hybrid_evidence_requires_unambiguous_exact_key(rules, bundle):
    source = pd.DataFrame(
        [
            {
                "source_row": i,
                "component": "conventional",
                "Model year": 2023,
                "Make": "Example",
                "Model": model,
                "Vehicle class": "Compact",
                "Fuel type": "X",
                "Combined (L/100 km)": 6,
            }
            for i, model in enumerate(
                ["Hidden", "Ambiguous", "Named Hybrid", "Unmatched"], 2
            )
        ]
    )
    epa = pd.DataFrame(
        {
            "year": [2023] * 3,
            "make": ["Example"] * 3,
            "model": ["Hidden", "Ambiguous", "Ambiguous"],
            "atvType": ["Hybrid", "Hybrid", ""],
        }
    )
    result = classify_ratings(
        [source], epa, rules=rules, conversions=load_conversion_factors(bundle)
    ).set_index("Model")
    assert result.loc["Hidden", "powertrain"] == "hev"
    assert result.loc["Ambiguous", "powertrain"] == "gasoline"
    assert result.loc["Ambiguous", "ambiguous_epa_match"]
    assert result.loc["Named Hybrid", "powertrain"] == "hev"
    assert result.loc["Unmatched", "powertrain"] == "gasoline"


def test_phev_rating_keeps_raw_cd_and_only_parses_leading_number(rules, bundle):
    source = pd.DataFrame(
        [
            {
                "source_row": 2,
                "component": "phev",
                "Model year": 2023,
                "Vehicle class": "Compact",
                "Range 1 (km)": 50,
                "Combined (L/100 km)": 5.5,
                "Combined Le/100 km": "1.8 ([15.8 kWh + 0.0 L]/100 km)",
            }
        ]
    )
    epa = pd.DataFrame(columns=rules["ratings"]["evidence_columns"])
    result = classify_ratings(
        [source],
        epa,
        rules=rules,
        conversions=load_conversion_factors(bundle),
        phev_contract=load_harmonization_rules(bundle, "nlr_atb_autonomie")[
            "components"
        ]["phev_efficiency"]["future_nrcan_phev_contract"],
    ).iloc[0]
    assert result.combined_cd_consumption_Le_100_km == 1.8
    assert result.combined_cs_consumption_L_100_km == 5.5
    assert "15.8 kWh" in result.combined_cd_consumption_Le_100_km_raw
    assert pd.isna(result.consumption_mj_per_vkm)


def test_unsplit_suv_sums_weights_and_renormalizes_observed(rules):
    classified = pd.DataFrame(
        [
            {
                "year": 2010,
                "powertrain": "gasoline",
                "rating_class": "Sport utility vehicle",
                "native_consumption": 12,
                "consumption_mj_per_vkm": 4,
                "native_unit": "L/100 km",
                "source_row": 2,
                "included": True,
            }
        ]
    )
    weights = pd.DataFrame(
        [
            {
                "report_year": 2025,
                "weight_basis": "all_vintages",
                "nrcan_ceud_class": "Light Truck",
                "nrcan_vehicle_class": name,
                "aggregation_weight": weight,
            }
            for name, weight in [
                ("Sport utility vehicle: Small", 0.4),
                ("Sport utility vehicle: Standard", 0.3),
                ("Pickup truck: Standard", 0.3),
            ]
        ]
    )
    result = aggregate_ratings(classified, weights, rules=rules)
    assert result.original_weight.iloc[0] == pytest.approx(0.7)
    assert result.weight.iloc[0] == 1
    assert result.native_class_mean.iloc[0] == 12


def test_vocational_consumption_average_is_harmonic_fuel_economy(rules):
    r = deepcopy(rules)
    r["md_gvwr"] = {"MDV3": 3, "MDV6": 6}
    report = pd.DataFrame({"EPA_GVWR": ["MDV3", "MDV6"], "NATIVE_COUNT": [1, 3]})
    classes = [
        "Class 3 Medium Van",
        "Class 3 Medium Pickup",
        "Class 3 Medium School",
        "Class 6 Box",
    ]
    weights = medium_vocation_weights(report, classes, rules=r)
    assert weights == {
        "Class 3 Medium Van": 0.125,
        "Class 3 Medium Pickup": 0.125,
        "Class 6 Box": 0.75,
    }
    atb = pd.DataFrame(
        [
            {
                "powertrain": "diesel",
                "year": 2030,
                "vehicle_class": label,
                "consumption_mj_per_vkm": 1 / mpg,
            }
            for label, mpg in zip(weights, [10, 20, 5], strict=True)
        ]
    )
    result = aggregate_atb(atb, weights, tolerance=1e-9)
    assert 1 / result.weighted_consumption.sum() == pytest.approx(
        1 / (0.125 / 10 + 0.125 / 20 + 0.75 / 5)
    )
    with pytest.raises(ValueError, match="coverage"):
        aggregate_atb(atb.iloc[:2], weights, tolerance=1e-9)


@pytest.mark.parametrize(
    "basis,category,powertrain,detail",
    [
        ("gge", "Light Duty", "phev_35_miles", "LDV PHEV"),
        (
            "dge",
            "Medium/Heavy Duty",
            "phev",
            "Diesel Series Plug-in Hybrid Electric Vehicle",
        ),
    ],
)
def test_phev_uses_derived_source_inputs_and_hhv_not_output_fe(
    bundle, rules, basis, category, powertrain, detail
):
    c = load_conversion_factors(bundle)
    r = deepcopy(rules)
    r["ldv_archetypes"] = {powertrain: {"range_mi": 35}} if basis == "gge" else {}
    r["mhdv_archetypes"] = {powertrain: {"detail": detail}} if basis == "dge" else {}
    h = c["energy"]["wh_per_" + basis]
    fuel, electric = 0.01, 120
    total = fuel * h + electric
    source = pd.DataFrame(
        [
            {
                "year": 2030,
                "trajectory": "Mid",
                "vehicle_weight_category": category,
                "vehicle_class": "Example",
                "vehicle_detail": detail,
                "electric_range_mi": 35,
                "fuel_equivalent_basis": basis,
                "total_utility_weighted_energy_wh_equivalent_per_mi": total,
                "electricity_input_share": electric / total,
                "liquid_fuel_input_share": fuel * h / total,
                "utility_weighted_fuel_consumption_gallon_equivalent_per_mi": fuel,
                "utility_weighted_electricity_consumption_wh_per_mi": electric,
            }
        ]
    )
    output = pd.DataFrame(
        [{"value": 99999}]
    )  # Not even a usable authoritative PHEV schema.
    result = select_atb_consumption(
        output, source, trajectory="Mid", rules=r, conversions=c
    ).iloc[0]
    physical_fuel = r["atb"]["equivalent_gallon_hhv_fuel"][basis]
    fuel_mj = (
        fuel
        * c["fuel_hhv"][physical_fuel + "_mj_per_litre"]
        * c["volume"]["us_gallon_to_litre"]
    )
    electric_mj = electric * c["energy"]["wh_to_mj"]
    assert result.basis == basis
    assert result.consumption_mj_per_vkm == pytest.approx(
        (fuel_mj + electric_mj) / c["length"]["mile_to_km"]
    )
    assert result.electricity_input_share == pytest.approx(
        electric_mj / (electric_mj + fuel_mj)
    )
    assert result.source_electricity_input_share == pytest.approx(electric / total)
    assert result.electricity_input_share != pytest.approx(
        result.source_electricity_input_share
    )


def test_offroad_hfo_consumption_ratio_is_reciprocal(rules):
    national = pd.DataFrame(
        {
            "table_id": [28, 28],
            "raw_series": ["marine"] * 2,
            "unit": ["MJ/Tkm"] * 2,
            "year": [2000, 2023],
            "value": [0.4, 0.2],
        }
    )
    manual = pd.DataFrame(
        {
            "category": ["freight_marine"] * 2,
            "sub_category": ["hfo", "all"],
            "parameter": ["mdo_relative_energy_intensity", "annual_improvement_rate"],
            "period": ["all"] * 2,
            "value": [1.25, 0.01],
        }
    )
    selectors = {
        "freight_marine": {
            "national_table": 28,
            "intensity_series": "marine",
            "units": "bn tonne-km",
        }
    }

    def derive(year):
        return derive_offroad_efficiency(
            national,
            manual,
            mode="freight_marine",
            powertrain="hfo",
            year=year,
            base_year=2023,
            selectors=selectors,
            rules=rules["offroad"],
        )

    assert derive(2000)["efficiency"] == 2
    assert derive(2030)["efficiency"] == pytest.approx(4 * 1.01**7)
    ratios = pd.DataFrame(
        {"period": ["through_2035", "through_2050"], "value": [1.5, 1.8]}
    )
    assert manual_ratio(ratios, 2040, rules=rules["offroad"]) == pytest.approx(1.6)


def test_template_edges_periods_and_default_exclusions(bundle, rules):
    road = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"][
        "activity_series"
    ]
    offroad = load_harmonization_rules(bundle, "offroad_stocks_and_demands")["demand"][
        "energy_intensity_series"
    ]
    technology = pd.read_csv(
        bundle.repo_root / "inputs/0_canoe_template/technology.csv"
    )
    commodities = pd.read_csv(
        bundle.repo_root / "inputs/0_canoe_template/commodity.csv"
    )
    edges = layer.technology_relationships(
        technology, commodities, rules=rules, road_outputs=road, offroad_outputs=offroad
    ).set_index("tech")
    assert edges.loc["T_LDV_C_GSL_N", "input_comm"] == "T_gsl"
    assert edges.loc["T_LDV_C_GSL_PHEV50_N", "input_comm"] == "T_gsl_elc_phev50"
    assert edges.loc["T_HDV_T_FCEV_N", "output_comm"] == "T_D_tkm_hdv_t"
    assert set(edges.loc[edges.kind.eq("unit")].index) == {
        "T_OFF",
        *(v["tech"] for v in rules["phev_blends"].values()),
    }
    assert not edges.index.str.contains("CHRG|REFUEL|dummy").any()
    assert rules["ldv_archetypes"]["fcev"]["analogue"] == "gasoline"
    assert layer.vintage_years(
        2023, existing=[2000, 2005, 2010, 2015, 2020, 2023], step=5
    ) == [2021, 2022, 2023]


def test_offline_deterministic_preparation_and_caller_owned_insertion(
    bundle, tmp_path, monkeypatch
):
    from build_transport import (
        insert_transport_contribution,
        prepare_transport_contribution,
    )
    from validation.insertion import insert_models
    from validation.schema_contract import create_v4_schema
    from canoe_schema.v4_0 import Region, TimePeriod

    payload = bundle.scenario.model_dump()
    payload["geography"]["regions"] = ["ON"]
    configured = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    original_path = layer.resolve_artifact_path

    def output_path(b, family, *parts):
        if family.startswith("efficiencies_"):
            return tmp_path / family / (str(parts[0]) if parts else "")
        return original_path(b, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", output_path)

    def no_network(*args, **kwargs):
        raise AssertionError("Offline preparation attempted a network connection")

    monkeypatch.setattr(socket, "create_connection", no_network)
    capacity = [
        SimpleNamespace(region="ON", tech="T_MDV_T_BEV_EX", vintage=2023, capacity=1)
    ]
    first = layer.prepare_efficiency_rows(configured, existing_capacity_rows=capacity)
    hashes = {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    second = layer.prepare_efficiency_rows(configured, existing_capacity_rows=capacity)
    assert hashes == {
        str(p.relative_to(tmp_path)): file_sha256(p) for p in tmp_path.rglob("*.csv")
    }
    assert first.audit == second.audit
    assert {r.vintage for r in first.efficiency_rows if r.tech == "T_MDV_T_BEV_EX"} == {
        2023
    }
    assert first.audit["historical_source_backed_rows_without_capacity"] == 54
    assert len(first.split_rows) == 40
    annual = pd.read_csv(
        tmp_path / "efficiencies_interim/annual_efficiency_evidence.csv"
    )
    assert set(annual.loc[annual.vintage.eq(2025), "source_year"]) == {2030}
    assert set(annual.loc[annual.tech.eq("T_MDV_T_BEV_EX"), "source_year"]) == {2023}
    assert first.audit["phev_energy_basis"] == "greet_hhv_fuel_plus_electricity"
    with sqlite3.connect(tmp_path / "transport.sqlite") as connection:
        create_v4_schema(connection)
        for model, file in ((Region, "region.csv"), (TimePeriod, "time_period.csv")):
            frame = pd.read_csv(
                bundle.repo_root / "inputs/0_canoe_template" / file
            ).fillna("")
            records = [
                {k: v for k, v in record.items() if v != ""}
                for record in frame.to_dict("records")
            ]
            insert_models(
                connection, [model.model_validate(record) for record in records]
            )
        structural = prepare_transport_contribution(
            connection,
            bundle=configured,
            template_dir=bundle.repo_root / "inputs/0_canoe_template",
            include_existing_capacity=False,
            include_demand=False,
            include_road_utilization=False,
            include_lifetimes=False,
            include_efficiencies=False,
            include_costs=False,
        )
        contribution = replace(
            structural,
            efficiency_rows=first.efficiency_rows,
            input_split_rows=first.split_rows,
            efficiency_assumption_dataset=first.assumption_dataset,
            provenance_contexts=first.provenance_contexts,
        )
        connection.commit()
        insert_transport_contribution(connection, contribution)
        assert connection.in_transaction
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT DISTINCT operator FROM limit_tech_input_split"
        ).fetchall() == [("e",)]
        connection.rollback()
        assert connection.execute("SELECT COUNT(*) FROM efficiency").fetchone()[0] == 0


def test_legacy_efficiency_comparison_does_not_invent_network_aliases(tmp_path):
    from validation.legacy_compare import compare_legacy_efficiency

    candidate, reference = tmp_path / "candidate.sqlite", tmp_path / "legacy.sqlite"
    for path, value, output in (
        (candidate, 2.0, "T_D_pkm"),
        (reference, 1.0, "T_D_pkm"),
    ):
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE efficiency(region, tech, vintage, input_comm, output_comm, efficiency)"
            )
            connection.execute(
                "INSERT INTO efficiency VALUES('ON','T_ONE',2025,'T_gsl',?,?)",
                (output, value),
            )
    result = compare_legacy_efficiency(
        candidate, reference, absolute_tolerance=1e-9, relative_tolerance=1e-6
    )
    assert result["shared_keys"] == result["value_differences"] == 1
    with sqlite3.connect(reference) as connection:
        connection.execute("UPDATE efficiency SET output_comm='T_annual_pkm'")
    result = compare_legacy_efficiency(
        candidate, reference, absolute_tolerance=1e-9, relative_tolerance=1e-6
    )
    assert result["shared_keys"] == 0
