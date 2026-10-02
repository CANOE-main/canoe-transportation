"""Focused technology-owner and provenance checks for fixed lifetimes."""

from pathlib import Path
from dataclasses import replace
import sqlite3

import pandas as pd
import pytest
from canoe_schema.v4_0 import Region, TechnologyLabel

from parameterization.offroad_lifetimes import prepare_reviewed_manual_lifetimes, prepare_statcan_bus_lifetimes
from parameterization.build_lifetime_parameters import prepare_lifetime_rows
from parameterization.road_lifetimes_survival import derive_accepted_lifetime_frames, prepare_fixed_road_lifetimes, prepare_road_survival_curve_rows
from utils import load_config_bundle, load_harmonization_rules
from validation.insertion import insert_models
from validation.provenance import registry_rows
from validation.schema_contract import create_v4_schema


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def accepted_frames(bundle):
    return derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
    )


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
    medians = accepted_frames(bundle)["medians"]
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
    assert ontario["T_MDV_T_DSL_EX"].lifetime == 18
    medium_owners = set(technology.loc[technology.category.eq("medium_trucks"), "tech"])
    medium_rows = [row for row in rows if row.tech in medium_owners]
    assert {row.lifetime for row in medium_rows} == {18}
    medium_context = next(context for context in contexts if context.data_id == medium_rows[0].data_id)
    assert {item.source_key for item in medium_context.contributors} == {"nhtsa_cafe_2024_ldv_survival"}
    assert {row.units for row in rows} == {"years"}
    with pytest.raises(ValueError, match="Incomplete source median classes"):
        prepare_fixed_road_lifetimes(
            bundle,
            medians=medians.loc[~medians.target_class.eq("2b/3 Trucks")],
            manual=manual,
            technology=technology,
        )


def test_established_fixed_rows_insert_with_full_provenance() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    manual_rows, manual_contexts, _ = prepare_reviewed_manual_lifetimes(bundle)
    technology = pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv")
    road_rows, road_contexts = prepare_fixed_road_lifetimes(
        bundle,
        medians=accepted_frames(bundle)["medians"],
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
    transformed = derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
    )["transformed_curves"]
    curves, contexts = prepare_road_survival_curve_rows(
        bundle, transformed=transformed, technology=technology
    )
    assert len(contexts) == 5
    assert {row.tech for row in curves} == set(technology.loc[
        technology.category.isin(["cars", "passenger_light_trucks", "freight_light_trucks", "medium_trucks", "heavy_trucks"]), "tech"
    ])
    car = transformed.loc[transformed.source_id.eq("ontario_report_a_weighted_nhtsa_cafe") & transformed.source_class.eq("Car")]
    expected = car.loc[car.age.isin(range(5)), "survival_probability"].mean()
    assert next(row.fraction for row in curves if row.region == "ON" and row.tech == "T_LDV_C_GSL_N" and row.vintage == 2025 and row.period == 2025) == pytest.approx(expected)
    for tech, source_id, source_class in (
        ("T_MDV_T_BEV_N", "nhtsa_cafe_2024_ldv_survival", "2b/3 Trucks"),
        ("T_HDV_T_BEV_N", "eia_nems_hd_truck_scrappage", "Cls 7-8"),
    ):
        source = transformed.loc[
            transformed.source_id.eq(source_id)
            & transformed.source_class.eq(source_class)
            & transformed.age.isin(range(5)), "survival_probability"
        ]
        row = next(row for row in curves if row.region == "ON" and row.tech == tech and row.vintage == 2025 and row.period == 2025)
        assert row.fraction == pytest.approx(source.mean())
        context = next(context for context in contexts if context.data_id == row.data_id)
        assert {item.source_key for item in context.contributors} == {source_id}
    assert not any(row.period - row.vintage + 4 > bundle.scenario.lifetimes.survival_curve_max_age for row in curves)
    short_bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curve_max_age": 10})
    }))
    short_curves, _ = prepare_road_survival_curve_rows(
        short_bundle, transformed=transformed, technology=technology
    )
    assert len(short_curves) < len(curves)
    assert all(row.period - row.vintage + 4 <= 10 for row in short_curves)
    prepared = prepare_lifetime_rows(bundle)
    assert prepared.audit["fixed_technologies"] == 57
    assert prepared.audit["curve_technologies"] == 64
    heavy_at_age_20 = next(
        row for row in prepared.curve_rows
        if row.region == "ON" and row.tech == "T_HDV_T_DSL_N"
        and row.vintage == 2025 and row.period == 2045
    )
    nems = transformed.loc[
        transformed.source_id.eq("eia_nems_hd_truck_scrappage")
        & transformed.source_class.eq("Cls 7-8")
        & transformed.age.between(20, 24), "survival_probability"
    ]
    assert heavy_at_age_20.fraction == pytest.approx(nems.mean())
    assert heavy_at_age_20.fraction > 0
    assert not any(row.tech.startswith("T_HDV_T_") for row in prepared.fixed_rows)
    fixed_bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": False})
    }))
    fixed = prepare_lifetime_rows(fixed_bundle)
    assert len(fixed.fixed_rows) == 1210
    assert fixed.curve_rows == []
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/lifetime_process.csv")
    heavy_lifetime = float(manual.loc[
        manual.category.eq("heavy_trucks") & manual.sub_category.eq("all"), "lifetime"
    ].iloc[0])
    assert heavy_lifetime == 19
    for category, lifetime, source_id in (
        ("medium_trucks", 18, "nhtsa_cafe_2024_ldv_survival"),
        ("heavy_trucks", heavy_lifetime, "epa_moves4_population_activity_2023"),
    ):
        owners = set(technology.loc[technology.category.eq(category), "tech"])
        selected = [row for row in fixed.fixed_rows if row.tech in owners]
        assert len(selected) == len(owners) * len(bundle.scenario.geography.regions)
        assert {row.lifetime for row in selected} == {lifetime}
        assert {
            item.source_key for context in fixed.provenance_contexts
            if context.data_id in {row.data_id for row in selected}
            for item in context.contributors
        } == {source_id}
    assert fixed.audit["road_lifetime_sources"]["medium_trucks"]["curve_source_class"] == "2b/3 Trucks"
