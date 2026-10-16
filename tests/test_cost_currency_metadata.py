"""Reviewed native dollar years control actual cost harmonization to scenario CAD."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization import build_costs as layer
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path


ROOT = Path(__file__).resolve().parents[1]
TECHNOLOGIES = {
    "T_LDV_C_GSL_N",
    "T_MDV_T_DSL_N",
    "T_HDV_BS_DSL_N",
    "T_HDV_BIC_DSL_N",
    "T_HDV_AJP_JFL_N",
    "T_LDV_M_GSL_N",
}


@pytest.fixture(scope="module")
def harmonized_costs(tmp_path_factory):
    """Exercise native cost families without publishing any shared artifacts."""
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT)
    scenario = bundle.scenario.model_dump(mode="python")
    scenario["geography"]["regions"] = ["ON"]
    scenario["periods"]["model"] = [2025]
    bundle = replace(bundle, scenario=type(bundle.scenario).model_validate(scenario))
    old_sources = bundle.sources.model_dump(mode="python")
    for source, component, year in (
        (layer.BEAN, "mhdv_maintenance_coefficients", 2019),
        (layer.REGEN, "intercity_bus_charts", 2025),
    ):
        adapter = old_sources["sources"][source]["components"][component]["adapter"]
        adapter.update(native_cost_dollar_year=year, cost_metadata_status="provisional_review")
    previous = replace(bundle, sources=type(bundle.sources).model_validate(old_sources))
    lifetimes = [
        SimpleNamespace(region="ON", tech=tech, lifetime=30, data_id="fixture-lifetime")
        for tech in TECHNOLOGIES
    ]
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    source_by_file = {
        assorted["epri_us_regen"]["output_file"]: layer.REGEN,
        assorted["faa"]["capacity_output_file"]: layer.FAA,
        assorted["faa"]["maintenance_output_file"]: layer.FAA,
        atb["components"]["anl_bean"]["output_file"]: layer.BEAN,
    }
    original_read = layer._read

    def selected_read(path, paths):
        frame = original_read(path, paths)
        if path.name == "technology.csv":
            return frame.loc[frame.tech.isin(TECHNOLOGIES)].copy()
        if path.name in source_by_file:
            # Keep this currency-only fixture independent of shared source replay.
            assert frame.source_id.nunique() == 1
            frame["source_id"] = source_by_file[path.name]
        return frame

    results = {}
    evidence = {}
    rules = load_harmonization_rules(bundle, "costs")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(layer, "_read", selected_read)
        # No existing technologies are requested; avoid generating unrelated range audits.
        patch.setattr(
            layer, "existing_range_weights",
            lambda _: ({}, SimpleNamespace(bucket_totals=pd.DataFrame())),
        )
        # Currency behavior is independent of fleet aggregation; keep class mixtures fixed.
        patch.setattr(layer, "_region_weights", lambda *_: {
            ("ON", "cars"): {"Compact": 1.0},
            ("ON", "medium_trucks"): {"Class 2 Medium Van": 1.0},
            ("ON", "school_buses"): {"Class 3 Medium School": 1.0},
            ("ON", "urban_transit"): {"Class 8 Transit Heavy": 1.0},
            ("ON", "inter_city_buses"): {"Class 8 Transit Heavy": 1.0},
        })
        for name, configured in (("reviewed", bundle), ("previous", previous)):
            output = tmp_path_factory.mktemp(f"cost_currency_{name}")

            def isolated_artifact_path(configured_bundle, artifact, *parts):
                assert artifact in {"costs_interim", "costs_processed", "costs_validation"}
                return output / artifact / Path(*parts)

            patch.setattr(layer, "resolve_artifact_path", isolated_artifact_path)
            results[name] = layer.prepare_cost_rows(
                configured, existing_capacity_rows=[],
                fixed_lifetime_rows=lifetimes, survival_curve_rows=[],
            )
            evidence[name] = pd.read_csv(
                output / "costs_interim" / rules["files"]["currency_audit"]
            )
    cer = load_harmonization_rules(bundle, "cer_enerfuture")
    macro = pd.read_csv(resolve_input_path(
        bundle, "interim",
        cer["interim_subdir_template"].format(
            edition=bundle.scenario.sources.selections[layer.CER].edition,
        ),
        cer["components"]["macro-indicators"]["output_file"],
    ))
    selected = macro.loc[
        macro.scenario.eq(bundle.scenario.economics.cer_scenario)
        & macro.region.eq("Canada")
    ].set_index(["variable_key", "year"])

    def usd_to_cad_2020(year):
        return (
            selected.loc[("cad_per_usd", year), "value"]
            * selected.loc[("gdp_deflator", 2020), "value"]
            / selected.loc[("gdp_deflator", year), "value"]
        )

    return bundle, results, evidence, usd_to_cad_2020


@pytest.mark.parametrize(
    ("family", "tech", "changed_years"),
    [
        ("invest_rows", "T_HDV_BIC_DSL_N", [(2023, 2025)]),
        ("variable_rows", "T_MDV_T_DSL_N", [(2020, 2019)]),
        ("variable_rows", "T_HDV_BS_DSL_N", [(2020, 2019)]),
        ("variable_rows", "T_HDV_BIC_DSL_N", [(2020, 2019), (2023, 2025)]),
        ("invest_rows", "T_LDV_C_GSL_N", []),
        ("variable_rows", "T_HDV_AJP_JFL_N", []),
        ("variable_rows", "T_LDV_M_GSL_N", []),
    ],
)
def test_reviewed_native_years_change_only_their_currency_factors(
    harmonized_costs, family, tech, changed_years,
):
    _, results, _, usd_to_cad_2020 = harmonized_costs
    reviewed = next(row for row in getattr(results["reviewed"], family) if row.tech == tech)
    previous = next(row for row in getattr(results["previous"], family) if row.tech == tech)
    expected_ratio = 1.0
    for current_year, previous_year in changed_years:
        expected_ratio *= usd_to_cad_2020(current_year) / usd_to_cad_2020(previous_year)
    assert reviewed.cost == pytest.approx(previous.cost * expected_ratio, rel=1e-12)
    assert reviewed.units == previous.units
    assert "2020CAD" in reviewed.units
    if changed_years:
        assert reviewed.cost != previous.cost


def test_native_year_review_keeps_cost_coverage_and_audit_basis(harmonized_costs):
    bundle, results, evidence, _ = harmonized_costs
    assert bundle.scenario.economics.cost_reference_currency == "CAD"
    assert bundle.scenario.economics.cost_reference_year == 2020
    for family in ("invest_rows", "variable_rows"):
        reviewed = getattr(results["reviewed"], family)
        previous = getattr(results["previous"], family)
        assert [(row.region, row.tech, row.vintage) for row in reviewed] == [
            (row.region, row.tech, row.vintage) for row in previous
        ]
        assert len(reviewed) == len(TECHNOLOGIES)
    current = evidence["reviewed"]
    for family, tech, year in (
        ("variable", "T_MDV_T_DSL_N", 2020),
        ("invest", "T_HDV_BIC_DSL_N", 2023),
        ("variable", "T_HDV_AJP_JFL_N", 2023),
        ("variable", "T_LDV_M_GSL_N", 2005),
    ):
        record = current.loc[current.family.eq(family) & current.tech.eq(tech)].iloc[0]
        assert (record.source_currency, record.source_dollar_year) == ("USD", year)
    registrations = {
        (item["source"], item["component"]): item
        for item in results["reviewed"].audit["source_cost_metadata"]
    }
    for source, component in (
        (layer.BEAN, "mhdv_maintenance_coefficients"),
        (layer.REGEN, "intercity_bus_charts"),
        (layer.FAA, "section_4_operating_costs"),
        (layer.GCAM, "canada_motorcycle_inputs"),
    ):
        assert registrations[source, component]["status"] == "reviewed"
    assert results["reviewed"].audit["source_metadata_provisional"] == {}
    assert set(results["previous"].audit["source_metadata_provisional"]) == {
        layer.BEAN, layer.REGEN,
    }
