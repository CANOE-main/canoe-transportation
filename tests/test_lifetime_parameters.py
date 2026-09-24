"""Focused technology-owner and provenance checks for fixed lifetimes."""

from pathlib import Path
from dataclasses import replace
import sqlite3

import pandas as pd
import pytest
from canoe_schema.v4_0 import Region, TechnologyLabel

from parameterization.offroad_lifetimes import prepare_reviewed_manual_lifetimes, prepare_statcan_bus_lifetimes
from parameterization.lifetime_parameters import prepare_lifetime_rows
from parameterization.road_lifetimes_survival import prepare_fixed_road_lifetimes, prepare_road_survival_curve_rows
from utils import load_config_bundle
from validation.insertion import insert_models
from validation.provenance import registry_rows
from validation.schema_contract import create_v4_schema


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def test_reviewed_manual_lifetimes_resolve_actual_template_owners() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    rows, contexts, resolution = prepare_reviewed_manual_lifetimes(bundle)

    assert len(contexts) == 5
    assert len(rows) == len(bundle.scenario.geography.regions) * resolution.tech.nunique()
    assert len({row.tech for row in rows}) == 34
    assert {row.units for row in rows} == {"years"}
    assert {(row.region, row.tech) for row in rows} == {
        (region, tech)
        for region in ("ON", "AB", "BCT", "MB", "NB", "NLLAB", "NS", "PEI", "QC", "SK")
        for tech in resolution.tech
    }
    assert all(
        row.lifetime is not None and row.lifetime > 0
        and row.data_source and row.data_id
        and all(getattr(row, field) is not None for field in
                ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"))
        for row in rows
    )


def test_fixed_road_medians_use_existing_class_contract() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    medians = pd.read_csv(
        ROOT / "inputs/2_processed/road_lifetimes_survival/source_derived_median_lifetimes.csv"
    )
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/lifetime_process.csv")
    technology = pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv")

    rows, contexts = prepare_fixed_road_lifetimes(
        bundle, medians=medians, manual=manual, technology=technology
    )
    assert len(contexts) == 4
    assert len(rows) == 10 * 57
    ontario = {row.tech: row for row in rows if row.region == "ON"}
    assert ontario["T_LDV_C_GSL_EX"].lifetime == 14
    assert ontario["T_LDV_LTP_GSL_EX"].lifetime == 16
    assert ontario["T_LDV_LTF_GSL_EX"].lifetime == 16
    assert ontario["T_MDV_T_DSL_EX"].lifetime == 25
    assert {row.units for row in rows} == {"years"}
    with pytest.raises(ValueError, match="Incomplete source median classes"):
        prepare_fixed_road_lifetimes(
            bundle,
            medians=medians.loc[~medians.target_class.eq("Cls 4-6")],
            manual=manual,
            technology=technology,
        )


def test_established_fixed_rows_insert_with_full_provenance() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    manual_rows, manual_contexts, _ = prepare_reviewed_manual_lifetimes(bundle)
    technology = pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv")
    road_rows, road_contexts = prepare_fixed_road_lifetimes(
        bundle,
        medians=pd.read_csv(
            ROOT / "inputs/2_processed/road_lifetimes_survival/source_derived_median_lifetimes.csv"
        ),
        manual=pd.read_csv(ROOT / "inputs/0_manual_params/lifetime_process.csv"),
        technology=technology,
    )
    labels, datasets, sources = registry_rows([*manual_contexts, *road_contexts])
    connection = sqlite3.connect(":memory:")
    try:
        create_v4_schema(connection)
        connection.execute("PRAGMA foreign_keys = ON")
        for batch in (
            [Region(region=region) for region in ("ON", "AB", "BCT", "MB", "NB", "NLLAB", "NS", "PEI", "QC", "SK")],
            [TechnologyLabel(tech=tech) for tech in technology.tech],
            labels,
            datasets,
            sources,
            [*manual_rows, *road_rows],
        ):
            insert_models(connection, batch)
        assert connection.execute("SELECT COUNT(*) FROM lifetime_tech").fetchone()[0] == 910
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_bus_latest_province_and_canada_fallback() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    rows, contexts, audit = prepare_statcan_bus_lifetimes(bundle)
    assert len(rows) == 300
    assert len(contexts) == 1
    by_key = {(row.region, row.tech): row.lifetime for row in rows}
    assert by_key[("ON", "T_HDV_BT_DSL_EX")] == 12
    assert by_key[("ON", "T_HDV_BT_GSL_EX")] == 12
    assert by_key[("ON", "T_HDV_BT_BEV_EX")] == 13
    assert by_key[("BCT", "T_HDV_BT_BEV_EX")] == 15
    assert audit.loc[audit.region.eq("BCT"), "canada_fallback"].all()
    assert set(audit.loc[audit.region.eq("BCT"), "source_year"]) == {2020}
    assert set(audit.loc[audit.tech.str.contains("FCEV|PHEV"), "source_member"]) == {
        "Electric buses, average expected useful life"
    }


def test_survival_period_blocks_and_representation_switch() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    technology = pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv")
    transformed = pd.read_csv(
        ROOT / "inputs/2_processed/road_lifetimes_survival/road_vehicle_transformed_survival_curves.csv"
    )
    curves, contexts = prepare_road_survival_curve_rows(
        bundle, transformed=transformed, technology=technology
    )
    assert len(contexts) == 5
    assert {row.tech for row in curves} == set(technology.loc[
        technology.category.isin(["cars", "passenger_light_trucks", "freight_light_trucks", "medium_trucks", "heavy_trucks"]), "tech"
    ])
    car = transformed.loc[transformed.source_id.eq("wards_weighted_nhtsa_legacy") & transformed.source_class.eq("Car")]
    expected = car.loc[car.age.isin(range(5)), "survival_probability"].mean()
    assert next(row.fraction for row in curves if row.region == "ON" and row.tech == "T_LDV_C_GSL_N" and row.vintage == 2025 and row.period == 2025) == pytest.approx(expected)
    for tech, source_class in (("T_MDV_T_BEV_N", "Cls 4-6"), ("T_HDV_T_BEV_N", "Cls 7-8")):
        source = transformed.loc[
            transformed.source_id.eq("eia_nems_hd_truck_scrappage")
            & transformed.source_class.eq(source_class)
            & transformed.age.isin(range(5)), "survival_probability"
        ]
        assert next(row.fraction for row in curves if row.region == "ON" and row.tech == tech and row.vintage == 2025 and row.period == 2025) == pytest.approx(source.mean())
    assert not any(row.period - row.vintage + 4 > bundle.scenario.switches.survival_curve_max_age for row in curves)
    short_bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "switches": bundle.scenario.switches.model_copy(update={"survival_curve_max_age": 10})
    }))
    short_curves, _ = prepare_road_survival_curve_rows(
        short_bundle, transformed=transformed, technology=technology
    )
    assert len(short_curves) < len(curves)
    assert all(row.period - row.vintage + 4 <= 10 for row in short_curves)
    prepared = prepare_lifetime_rows(bundle)
    assert prepared.audit["fixed_technologies"] == 57
    assert prepared.audit["curve_technologies"] == 64
    fixed_bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "switches": bundle.scenario.switches.model_copy(update={"survival_curves": False})
    }))
    fixed = prepare_lifetime_rows(fixed_bundle)
    assert len(fixed.fixed_rows) == 1210
    assert fixed.curve_rows == []
