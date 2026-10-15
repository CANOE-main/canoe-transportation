"""Heavy-only source selection across medians, curves, stock ages and provenance."""

from dataclasses import replace
from pathlib import Path
import shutil
import sqlite3

from canoe_schema.v4_0 import Region, TechnologyLabel, TimePeriod
import pandas as pd
import pytest

from parameterization.build_lifetime_parameters import prepare_lifetime_rows
from parameterization.road_lifetimes_survival import (
    derive_accepted_lifetime_frames,
    fixed_existing_lifetimes,
    load_heavy_truck_survival_comparison,
    median_equivalent_lifetimes,
    resolved_road_lifetime_rules,
    transform_source_survival_curves,
    validate_accepted_survival_inputs,
)
from parameterization.road_stocks_and_demands import accepted_curve_ages
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.insertion import insert_models
from validation.provenance import registry_rows
from validation.schema_contract import create_v4_schema


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"
NEMS = "eia_nems_hd_truck_scrappage"
MOVES = "epa_moves_heavy_truck_survival"


def selected_bundle(bundle, source, curves):
    return replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={
            "heavy_truck_survival_source": source, "survival_curves": curves,
        }),
    }))


def accepted_frames(bundle):
    return derive_accepted_lifetime_frames(
        bundle, rules=load_harmonization_rules(bundle, "road_lifetimes_survival"),
        road_rules=load_harmonization_rules(bundle, "road_aggregation"),
        assorted_rules=load_harmonization_rules(bundle, "assorted_sources"),
    )


@pytest.fixture
def survival_inputs(tmp_path):
    """Use registered core schedules and a synthetic optional-source interface."""
    bundle = load_config_bundle(SCENARIO, repo_root=ROOT)
    rules = load_harmonization_rules(bundle, "assorted_sources")
    interim = tmp_path / "interim"
    assorted = interim / rules["interim_subdir"]
    assorted.mkdir(parents=True)
    for adapter in ("nhtsa_cafe", "eia_nems"):
        filename = rules[adapter]["output_file"]
        shutil.copyfile(resolve_input_path(bundle, "interim", rules["interim_subdir"], filename), assorted / filename)
    bus = load_harmonization_rules(bundle, "road_lifetimes_survival")["bus_lifetimes"]
    statcan = load_harmonization_rules(bundle, "statcan_tables")
    bus_path = Path(statcan["interim_subdir"]) / statcan["tables"][str(bus["statcan_table_id"])]["output_file"]
    (interim / bus_path).parent.mkdir(parents=True)
    shutil.copyfile(resolve_input_path(bundle, "interim", bus_path), interim / bus_path)
    moves_rule = rules["epa_moves5"]
    # Constant annual survival crosses 50% at age 7; it is not cumulative 90%.
    moves = pd.DataFrame({
        "source_id": MOVES,
        "source_vehicle_class_label": moves_rule["source_vehicle_class_label"],
        "vehicle_age": range(41),
        "annual_survival_rate": [1.0, *([0.9] * 40)],
        "unit": moves_rule["unit"],
    })
    path = assorted / moves_rule["output_file"]
    moves.to_csv(path, index=False)
    inputs = bundle.paths.inputs.model_copy(update={"interim": str(interim)})
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"inputs": inputs}))
    return bundle, path, moves


def test_moves_annual_survival_compounds_without_changing_native_audit(survival_inputs):
    bundle, _, moves = survival_inputs
    rules = load_harmonization_rules(bundle, "assorted_sources")
    assorted = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    nhtsa = pd.read_csv(assorted / rules["nhtsa_cafe"]["output_file"])
    eia = pd.read_csv(assorted / rules["eia_nems"]["output_file"])
    native, transformed = transform_source_survival_curves(nhtsa, eia, moves.sample(frac=1, random_state=4))
    source = native.loc[native.source_id.eq(MOVES)].sort_values("age")
    curve = transformed.loc[transformed.source_id.eq(MOVES)].set_index("age")
    assert source.source_value.tolist() == moves.annual_survival_rate.tolist()
    assert set(source.source_measure) == {"annual_survival_rate"}
    assert curve.loc[0, "survival_probability"] == 1
    assert curve.loc[2, "survival_probability"] == pytest.approx(0.81)
    assert curve.loc[40, "survival_probability"] == pytest.approx(0.9 ** 40)
    assert set(curve.source_unit) == {"dimensionless"}
    medians = median_equivalent_lifetimes(transformed)
    assert medians.loc[medians.source_id.eq(MOVES), "median_equivalent_age"].item() == 7
    for invalid, message in (
        (moves.iloc[:-1], "missing or duplicate ages"),
        (moves.assign(unit="percent"), "identity or units"),
        (moves.assign(annual_survival_rate=1.1), "finite fractions"),
        (moves.assign(annual_survival_rate=0.9), "age zero"),
    ):
        with pytest.raises(ValueError, match=message):
            validate_accepted_survival_inputs(nhtsa, eia, assorted_rules=rules, moves5=invalid)


@pytest.mark.parametrize("curves", [False, True])
def test_source_switch_changes_only_heavy_rows_with_complete_lineage(survival_inputs, curves):
    bundle, _, _ = survival_inputs
    preparations = {
        source: prepare_lifetime_rows(selected_bundle(bundle, source, curves))
        for source in (NEMS, MOVES)
    }
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    heavy = set(technology.loc[technology.category.eq("heavy_trucks"), "tech"])
    regions = {"ON", "AB", "BCT", "MB", "NB", "NLLAB", "NS", "PEI", "QC", "SK"}
    rows = {source: [*prepared.fixed_rows, *prepared.curve_rows] for source, prepared in preparations.items()}
    assert [row.model_dump() for row in rows[NEMS] if row.tech not in heavy] == [
        row.model_dump() for row in rows[MOVES] if row.tech not in heavy
    ]
    for source, prepared in preparations.items():
        heavy_rows = [row for row in rows[source] if row.tech in heavy]
        assert {row.tech for row in heavy_rows} == heavy
        assert {row.region for row in heavy_rows} == regions
        assert {row.data_source for row in heavy_rows} == {"T39" if source == MOVES else "T10"}
        context_ids = {row.data_id for row in heavy_rows}
        assert {
            contributor.source_key for context in prepared.provenance_contexts
            if context.data_id in context_ids for contributor in context.contributors
        } == {source}
        assert all(getattr(row, field) is not None for row in heavy_rows for field in
                   ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"))
        if curves:
            assert not any(row.tech in heavy for row in prepared.fixed_rows)
            if source == MOVES:
                block = next(row for row in heavy_rows if row.region == "ON" and row.tech == "T_HDV_T_DSL_N"
                             and row.vintage == 2025 and row.period == 2045)
                assert block.fraction == pytest.approx(sum(0.9 ** age for age in range(20, 25)) / 5)
        else:
            assert prepared.curve_rows == []
            assert {row.lifetime for row in heavy_rows} == {7 if source == MOVES else 25}

    # Exercise the pinned row/schema interface with the selected EPA provenance.
    prepared = preparations[MOVES]
    labels, datasets, sources = registry_rows(prepared.provenance_contexts)
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        connection.execute("PRAGMA foreign_keys = ON")
        for batch in (
            [Region(region=region) for region in regions],
            [TechnologyLabel(tech=tech) for tech in technology.tech],
            [TimePeriod(period=year) for year in bundle.scenario.periods.all_years()],
            labels, datasets, sources, prepared.fixed_rows, prepared.curve_rows,
        ):
            if batch:
                insert_models(connection, batch)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_stock_eligibility_uses_selected_source_and_supported_ages(survival_inputs):
    bundle, _, _ = survival_inputs
    manual = pd.read_csv(resolve_input_path(bundle, "manual", "lifetime_process.csv"))
    for source, median, last_age in ((NEMS, 25, 34), (MOVES, 7, 40)):
        selected = selected_bundle(bundle, source, False)
        frames = accepted_frames(selected)
        rules = resolved_road_lifetime_rules(selected)
        fixed = fixed_existing_lifetimes(frames["medians"], manual, rules)
        assert fixed["heavy_trucks"] == median
        assert fixed["medium_trucks"] == 18
        # Another source with the same class label must never supply stock ages.
        impostor = frames["transformed_curves"].loc[
            frames["transformed_curves"].source_id.eq(source)
        ].assign(source_id="unselected", age=99)
        ages = accepted_curve_ages(pd.concat([frames["transformed_curves"], impostor]), rules)
        assert ages["heavy_trucks"] == set(range(last_age + 1))


def test_unselected_moves_input_is_ignored_and_selected_input_never_falls_back(survival_inputs):
    bundle, path, _ = survival_inputs
    baseline = accepted_frames(bundle)
    path.write_text("invalid optional evidence\n", encoding="utf-8")
    pd.testing.assert_frame_equal(accepted_frames(bundle)["transformed_curves"], baseline["transformed_curves"])
    path.unlink()
    pd.testing.assert_frame_equal(accepted_frames(bundle)["medians"], baseline["medians"])
    with pytest.raises(FileNotFoundError, match="moves5-survival"):
        accepted_frames(selected_bundle(bundle, MOVES, False))
    with pytest.raises(FileNotFoundError, match="moves5-survival"):
        load_heavy_truck_survival_comparison(bundle)


def test_comparison_uses_the_same_full_schedules_as_parameterization(survival_inputs):
    bundle, _, _ = survival_inputs
    comparison = load_heavy_truck_survival_comparison(bundle)
    assert set(comparison.source_id) == {NEMS, MOVES}
    for source in (NEMS, MOVES):
        production = accepted_frames(selected_bundle(bundle, source, True))["transformed_curves"]
        source_class = resolved_road_lifetime_rules(selected_bundle(bundle, source, True))[
            "fixed_lifetime_sources"
        ]["heavy_trucks"]["curve_source_class"]
        expected = production.loc[
            production.source_id.eq(source) & production.source_class.eq(source_class), comparison.columns,
        ].reset_index(drop=True)
        pd.testing.assert_frame_equal(comparison.loc[comparison.source_id.eq(source)].reset_index(drop=True), expected)
