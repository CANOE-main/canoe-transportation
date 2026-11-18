"""Focused technology-owner and provenance checks for fixed lifetimes."""

from pathlib import Path
from dataclasses import replace
import sqlite3

import pandas as pd
import pytest
from canoe_schema.v4_0 import Region, TechnologyLabel

from parameterization.offroad_lifetimes import prepare_reviewed_manual_lifetimes, prepare_statcan_bus_lifetimes
from parameterization.build_lifetime_parameters import build_lifetime_parameter_artifacts, prepare_lifetime_rows
from parameterization.road_lifetimes_survival import derive_accepted_lifetime_frames, prepare_fixed_road_lifetimes
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.insertion import insert_models
from validation.provenance import registry_rows
from validation.schema_contract import create_v4_schema


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/validation_opening_periods.yaml"


def accepted_frames(bundle):
    return derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
    )


def test_reviewed_manual_lifetimes_resolve_actual_template_owners() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    rows, contexts, resolution = prepare_reviewed_manual_lifetimes(bundle)

    assert len(contexts) == 4
    assert len(rows) == len(bundle.scenario.geography.regions) * resolution.tech.nunique()
    assert len({row.tech for row in rows}) == 32
    assert not any(row.tech.startswith("T_HDV_T_") for row in rows)
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


def test_fixed_road_medians_use_existing_class_contract(isolated_legacy_bundle) -> None:
    bundle = isolated_legacy_bundle
    medians = accepted_frames(bundle)["medians"]
    manual = pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv"))
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))

    rows, contexts = prepare_fixed_road_lifetimes(
        bundle, medians=medians, manual=manual, technology=technology
    )
    assert len(contexts) == 5
    assert len(rows) == 10 * 65
    ontario = {row.tech: row for row in rows if row.region == "ON"}
    assert ontario["T_LDV_C_GSL_EX"].lifetime == 14
    assert ontario["T_LDV_LTP_GSL_EX"].lifetime == 16
    assert ontario["T_LDV_LTF_GSL_EX"].lifetime == 16
    assert ontario["T_MDV_T_DSL_EX"].lifetime == 18
    assert ontario["T_HDV_T_DSL_EX"].lifetime == 25
    medium_owners = set(technology.loc[technology.category.eq("medium_trucks"), "tech"])
    medium_rows = [row for row in rows if row.tech in medium_owners]
    assert {row.lifetime for row in medium_rows} == {18}
    medium_context = next(context for context in contexts if context.data_id == medium_rows[0].data_id)
    assert {item.source_key for item in medium_context.contributors} == {"nhtsa_cafe_ldv_survival"}
    heavy_owners = set(technology.loc[technology.category.eq("heavy_trucks"), "tech"])
    heavy_rows = [row for row in rows if row.tech in heavy_owners]
    assert {row.lifetime for row in heavy_rows} == {25}
    heavy_context = next(context for context in contexts if context.data_id == heavy_rows[0].data_id)
    assert {item.source_key for item in heavy_context.contributors} == {"eia_nems_hd_truck_scrappage"}
    assert {row.units for row in rows} == {"years"}
    for source_class in ("2b/3 Trucks", "Cls 7-8"):
        with pytest.raises(ValueError, match="Incomplete source median classes"):
            prepare_fixed_road_lifetimes(
                bundle,
                medians=medians.loc[~medians.target_class.eq(source_class)],
                manual=manual,
                technology=technology,
            )


def test_established_fixed_rows_insert_with_full_provenance(isolated_legacy_bundle) -> None:
    bundle = isolated_legacy_bundle
    manual_rows, manual_contexts, _ = prepare_reviewed_manual_lifetimes(bundle)
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    road_rows, road_contexts = prepare_fixed_road_lifetimes(
        bundle,
        medians=accepted_frames(bundle)["medians"],
        manual=pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv")),
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
        assert connection.execute("SELECT COUNT(*) FROM lifetime_tech").fetchone()[0] == 970
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_bus_latest_province_and_canada_fallback(tmp_path: Path) -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    bus_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")["bus_lifetimes"]
    statcan_rules = load_harmonization_rules(bundle, "statcan_tables")
    table_id = str(bus_rules["statcan_table_id"])
    members = bus_rules["powertrain_source_member"]
    # Explicit normalized cells exercise older provincial values, suppression,
    # the BC proxy and a later observation outside the selected fallback horizon.
    cells = [
        ("Canada", 2020, "diesel", 14),
        ("Canada", 2020, "cng", 10),
        ("Canada", 2020, "bev", 15),
        ("Canada", 2020, "hev", 13),
        ("ON", 2016, "diesel", 9),
        ("ON", 2020, "diesel", 12),
        ("ON", 2020, "cng", 11),
        ("ON", 2016, "bev", 12),
        ("ON", 2020, "bev", 13),
        ("ON", 2020, "hev", 14),
        ("BC", 2018, "diesel", 12),
        ("BC", 2018, "cng", 14),
        ("BC", 2018, "hev", 13),
        ("BC", 2020, "bev", None),
        ("BC", 2022, "bev", 17),
    ]
    interim = tmp_path / "interim"
    evidence_path = (
        interim / statcan_rules["interim_subdir"]
        / statcan_rules["tables"][table_id]["output_file"]
    )
    evidence_path.parent.mkdir(parents=True)
    pd.DataFrame([
        {
            "table_id": table_id,
            "scenario_region": region,
            "reference_period": year,
            "public_transit_assets_average_expected_useful_life": members[powertrain],
            "scaled_value": value,
            "units": "Average years",
        }
        for region, year, powertrain, value in cells
    ]).to_csv(evidence_path, index=False)
    inputs = bundle.paths.inputs.model_copy(update={"interim": str(interim)})
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"inputs": inputs}))
    rows, contexts, audit = prepare_statcan_bus_lifetimes(bundle)
    assert len(rows) == 300
    assert len(contexts) == 1
    by_key = {(row.region, row.tech): row.lifetime for row in rows}
    assert by_key[("ON", "T_HDV_BT_DSL_EX")] == 12
    assert by_key[("ON", "T_HDV_BT_GSL_EX")] == 12
    assert by_key[("ON", "T_HDV_BT_BEV_EX")] == 13
    assert by_key[("BCT", "T_HDV_BT_BEV_EX")] == 15
    bct = audit.loc[audit.region.eq("BCT")]
    electric = bct.source_member.str.startswith("Electric buses")
    assert bct.loc[electric, "canada_fallback"].all()
    assert set(bct.loc[electric, "source_region"]) == {"Canada"}
    assert not bct.loc[~electric, "canada_fallback"].any()
    assert set(bct.loc[~electric, "source_region"]) == {"BC"}
    assert by_key[("BCT", "T_HDV_BT_DSL_HEV_N")] == 13
    assert set(audit.loc[audit.region.eq("BCT"), "source_year"]) == {2018, 2020}
    assert set(audit.loc[audit.tech.str.contains("FCEV|PHEV"), "source_member"]) == {
        "Electric buses, average expected useful life"
    }


def test_fixed_lifetime_representation_preserves_reviewed_medians(isolated_legacy_bundle) -> None:
    bundle = isolated_legacy_bundle
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    transformed = accepted_frames(bundle)["transformed_curves"]
    fixed_bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": False})
    }))
    fixed = prepare_lifetime_rows(fixed_bundle)
    assert len(fixed.fixed_rows) == 1220
    assert fixed.curve_rows == []
    heavy_schedule = transformed.loc[
        transformed.source_id.eq("eia_nems_hd_truck_scrappage")
        & transformed.source_class.eq("Cls 7-8")
    ]
    assert heavy_schedule.loc[heavy_schedule.age.eq(24), "survival_probability"].item() > 0.5
    assert heavy_schedule.loc[heavy_schedule.age.eq(25), "survival_probability"].item() <= 0.5
    for category, lifetime, source_id in (
        ("medium_trucks", 18, "nhtsa_cafe_ldv_survival"),
        ("heavy_trucks", 25, "eia_nems_hd_truck_scrappage"),
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
    assert fixed.audit["road_lifetime_sources"]["heavy_trucks"]["curve_source_class"] == "Cls 7-8"


AGGREGATED_CHARGERS = {
    "T_LDV_CHRG_EX", "T_LDV_CHRG_N", "T_MHDV_CHRG_EX", "T_MHDV_CHRG_N",
}
EXPLICIT_CHARGERS = {
    "T_LDV_CHRG_PUB_L2", "T_LDV_CHRG_PUB_FC", "T_LDV_CHRG_PRIV",
    "T_MHDV_CHRG_NIGHT", "T_MHDV_CHRG_FC",
}


@pytest.fixture(scope="module")
def charger_lifetime_modes(isolated_legacy_bundle):
    results = {}
    for mode in ("aggregated", "explicit"):
        scenario = isolated_legacy_bundle.scenario
        configured = replace(isolated_legacy_bundle, scenario=scenario.model_copy(update={
            "lifetimes": scenario.lifetimes.model_copy(update={"survival_curves": False}),
            "ev_chargers": scenario.ev_chargers.model_copy(update={"charger_type_classes": mode}),
        }))
        results[mode] = configured, prepare_lifetime_rows(configured)
    return results


@pytest.mark.parametrize("mode,expected,inactive", [
    ("aggregated", AGGREGATED_CHARGERS, EXPLICIT_CHARGERS),
    ("explicit", EXPLICIT_CHARGERS, AGGREGATED_CHARGERS),
])
def test_charger_lifetimes_cover_only_selected_technologies_with_reviewed_provenance(
    charger_lifetime_modes, mode, expected, inactive,
):
    _, prepared = charger_lifetime_modes[mode]
    rows = [row for row in prepared.fixed_rows if row.tech in expected]
    assert {(row.region, row.tech) for row in rows} == {
        (region, tech) for region in prepared.audit["regions"] for tech in expected
    }
    assert not {row.tech for row in prepared.fixed_rows} & inactive
    assert {row.lifetime for row in rows} == {15}
    contexts = {context.data_id: context for context in prepared.provenance_contexts}
    for row in rows:
        context = contexts[row.data_id]
        assert context.source_key == "open_energy_outlook"
        for field, value in context.parameter_fields().items():
            assert getattr(row, field) == value
    assert prepared.audit["charger_representation"] == {
        "mode": mode, "selected_technologies": sorted(expected),
        "excluded_technologies": sorted(inactive),
        "excluded_manual_rows": len(inactive) * len(prepared.audit["regions"]),
    }


def test_charger_selection_preserves_every_noncharger_lifetime(charger_lifetime_modes):
    nonchargers = []
    for _, prepared in charger_lifetime_modes.values():
        nonchargers.append([
            row.model_dump() for row in prepared.fixed_rows
            if row.tech not in AGGREGATED_CHARGERS | EXPLICIT_CHARGERS
        ])
    assert nonchargers[0] == nonchargers[1]


@pytest.mark.parametrize("problem", ["missing_selected", "extra_owner"])
def test_charger_selection_preserves_strict_lifetime_coverage(
    charger_lifetime_modes, monkeypatch, problem,
):
    from parameterization import build_lifetime_parameters as layer

    bundle, _ = charger_lifetime_modes["explicit"]
    original = layer.prepare_reviewed_manual_lifetimes

    def invalid_manual(configured):
        rows, contexts, resolution = original(configured)
        if problem == "missing_selected":
            rows = [row for row in rows if row.tech != "T_LDV_CHRG_PUB_L2"]
        else:
            rows.append(rows[0].model_copy(update={"tech": "T_UNDECLARED_OWNER"}))
        return rows, contexts, resolution

    monkeypatch.setattr(layer, "prepare_reviewed_manual_lifetimes", invalid_manual)
    with pytest.raises(ValueError, match="exactly one lifetime representation"):
        prepare_lifetime_rows(bundle)


@pytest.mark.parametrize("mode,expected,inactive", [
    ("aggregated", AGGREGATED_CHARGERS, EXPLICIT_CHARGERS),
    ("explicit", EXPLICIT_CHARGERS, AGGREGATED_CHARGERS),
])
def test_standalone_lifetime_exports_respect_charger_representation(
    charger_lifetime_modes, mode, expected, inactive,
):
    bundle, _ = charger_lifetime_modes[mode]
    routes = dict(bundle.paths.artifacts)
    routes["lifetime_parameters"] = routes["lifetime_parameters"].model_copy(update={
        "path": f"standalone_lifetime_exports/{mode}",
    })
    selected = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}))
    directory = build_lifetime_parameter_artifacts(selected)
    actual = pd.read_csv(directory / "lifetime_tech.csv")
    assert set(actual.tech) & (AGGREGATED_CHARGERS | EXPLICIT_CHARGERS) == expected
    assert not set(actual.tech) & inactive
    assert actual.loc[actual.tech.isin(expected), "lifetime"].eq(15).all()
