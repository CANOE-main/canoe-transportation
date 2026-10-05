"""Calendar boundaries, observed coverage and period-mode compatibility."""

import json
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest
import requests
from pydantic import ValidationError

from validation.config_models import ScenarioPeriods
import build_transport
from utils import load_config_bundle, resolve_artifact_path


def grid(mode: str = "prospective", **updates) -> ScenarioPeriods:
    payload = {
        "period_mode": mode, "base_year": 2023,
        "existing": [2000, 2005, 2010, 2015, 2020] + ([2023] if mode == "legacy" else []),
        "model": [2025, 2030, 2035, 2040, 2045], "step": 5,
    }
    return ScenarioPeriods.model_validate({**payload, **updates})


@pytest.mark.parametrize(
    "year,prospective,legacy",
    [(1999, 2000, 2000), (2000, 2000, 2000), (2001, 2000, 2005),
     (2005, 2000, 2005), (2006, 2005, 2010), (2020, 2015, 2020),
     (2021, 2020, 2023), (2022, 2020, 2023), (2023, 2020, 2023)],
)
def test_annual_cohorts_use_exact_calendar_boundaries(year, prospective, legacy):
    assert grid().existing_vintage(year) == prospective
    assert grid("legacy").existing_vintage(year) == legacy


def test_observation_anchor_does_not_create_a_model_vintage():
    periods = grid()
    assert periods.historical_years(2020) == [2021, 2022, 2023]
    assert periods.historical_years(2000) == [2000, 2001, 2002, 2003, 2004, 2005]
    assert periods.latest_observed_vintage == 2020
    assert 2023 not in periods.all_years()
    assert periods.end_of_horizon == 2050
    assert grid("legacy").historical_years(2023) == [2021, 2022, 2023]
    with pytest.raises(ValueError, match="cannot exceed"):
        periods.existing_vintage(2024)


def test_irregular_existing_labels_partition_observations_without_repeating_years():
    periods = grid(existing=[2000, 2005, 2010, 2015, 2020, 2022, 2023])
    assert periods.historical_years(2020) == [2021, 2022]
    assert periods.historical_years(2022) == [2023]
    assert periods.historical_years(2023) == []
    assert periods.latest_observed_vintage == 2022
    assert periods.audit()["empty_observation_vintages"] == [2023]
    for vintage in periods.existing:
        assert all(periods.existing_vintage(year) == vintage for year in periods.historical_years(vintage))


@pytest.mark.parametrize("period,end", [(2025, 2030), (2045, 2050)])
def test_future_projection_years_preserve_the_mixed_legacy_path(period, end):
    assert grid().projection_year(period, legacy_at_end=True) == end
    assert grid().projection_year(period, legacy_at_end=False) == end
    assert grid("legacy").projection_year(period, legacy_at_end=True) == end
    assert grid("legacy").projection_year(period, legacy_at_end=False) == period


def test_period_modes_reject_unusable_or_ambiguous_grids():
    with pytest.raises(ValidationError, match="must end at base_year"):
        grid("legacy", existing=[2000, 2005, 2010, 2015, 2020])
    with pytest.raises(ValidationError, match="label before base_year"):
        grid(existing=[2023])
    with pytest.raises(ValidationError):
        grid("retrospective")
    payload = grid().model_dump()
    del payload["period_mode"]
    with pytest.raises(ValidationError, match="period_mode"):
        ScenarioPeriods.model_validate(payload)


@pytest.fixture(scope="module")
def prepared_modes(tmp_path_factory, legacy_bundle):
    """Exercise both complete offline database paths with isolated artifacts."""
    root = Path(__file__).resolve().parents[1]
    directory = tmp_path_factory.mktemp("period_parameter_paths")
    results = {}
    captured = []
    original_prepare = build_transport.prepare_transport_contribution

    def capture(*args, **kwargs):
        contribution = original_prepare(*args, **kwargs)
        captured.append(contribution)
        return contribution

    with pytest.MonkeyPatch.context() as patch:
        def reject_network(*args, **kwargs):
            raise AssertionError("Period parameter paths must work offline")

        patch.setattr(requests.sessions.Session, "request", reject_network)
        patch.setattr(build_transport, "prepare_transport_contribution", capture)
        for mode, bundle in (
            ("legacy", legacy_bundle),
            ("prospective", load_config_bundle("config/scenarios/legacy_reproduction.yaml", repo_root=root)),
        ):
            destination = directory / mode
            processed = destination / "processed"
            shutil.copytree(root / "inputs/2_processed", processed)
            routes = {}
            for name, route in bundle.paths.artifacts.items():
                if route.layer == "processed":
                    route = route.model_copy(update={"path": str(processed / Path(route.path).relative_to("inputs/2_processed"))})
                elif route.layer == "input_validation":
                    route = route.model_copy(update={"path": str(destination / "validation" / name)})
                elif route.layer == "interim" and route.owner.startswith("parameterization."):
                    route = route.model_copy(update={"path": str(destination / "interim" / name)})
                routes[name] = route
            paths = bundle.paths.model_copy(update={
                "artifacts": routes,
                "inputs": bundle.paths.inputs.model_copy(update={"processed": str(processed)}),
            })
            bundle = replace(bundle, paths=paths)
            report = build_transport.bootstrap_database(
                bundle=bundle, template_dir=root / "inputs/0_canoe_template",
                database_path=destination / "transport.sqlite",
            )
            (destination / "database_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            contribution = captured[-1]
            results[mode] = (bundle, contribution)
    return results


def test_prospective_existing_stock_uses_available_years_and_preserves_totals(prepared_modes):
    totals = {}
    for mode, (bundle, contribution) in prepared_modes.items():
        assert contribution.support_audit["ok"]
        directory = resolve_artifact_path(bundle, "existing_capacity_interim")
        frames = {}
        for family in ("road", "bus"):
            cohorts = pd.read_csv(directory / f"{family}_existing_capacity_age_cohorts.csv")
            assert all(
                row.vintage == bundle.scenario.periods.existing_vintage(row.vintage_year)
                for row in cohorts.itertuples()
            )
            frames[family] = cohorts.groupby("road_class").cohort_k_vehicles.sum() if family == "road" else cohorts.groupby("road_class").capacity.sum()
        totals[mode] = frames
        assert {row.vintage for row in contribution.parameter_rows}.issubset(bundle.scenario.periods.existing)
    for family in ("road", "bus"):
        assert totals["prospective"][family].to_dict() == pytest.approx(totals["legacy"][family].to_dict())
    prospective = prepared_modes["prospective"][1]
    assert {row.vintage for row in prospective.parameter_rows if row.tech == "T_MDV_T_BEV_EX"} == {2020}
    assert prospective.parameter_audit["period_mapping"]["historical_years_by_vintage"]["2020"] == [2021, 2022, 2023]
    curve_keys = {(row.region, row.tech, row.vintage) for row in prospective.lifetime_survival_curve_rows if row.period == 2025}
    curve_owners = {row.tech for row in prospective.lifetime_survival_curve_rows}
    assert all((row.region, row.tech, row.vintage) in curve_keys for row in prospective.parameter_rows if row.tech in curve_owners)


def test_all_future_purchase_paths_use_end_conditions_in_prospective_mode(prepared_modes):
    by_mode = {
        mode: {(row.tech, row.vintage): row.cost for row in contribution.cost_invest_rows if row.region == "ON"}
        for mode, (_, contribution) in prepared_modes.items()
    }
    for tech in ("T_LDV_C_GSL_N", "T_LDV_C_BEV150_N", "T_LDV_M_GSL_N"):
        assert by_mode["prospective"][tech, 2025] == pytest.approx(by_mode["legacy"][tech, 2030])
    for mode, (bundle, contribution) in prepared_modes.items():
        assert contribution.cost_audit["period_mapping"]["cost_and_charger_years"]["2025"] == (2030 if mode == "prospective" else 2025)
        assert contribution.efficiency_rows
        source = pd.read_csv(resolve_artifact_path(bundle, "efficiencies_interim") / "annual_efficiency_evidence.csv")
        assert set(source.loc[source.vintage.eq(2025), "source_year"]) == {2030}


def test_charger_vintages_costs_and_utilization_follow_the_selected_mode(prepared_modes):
    costs = {}
    factors = {}
    for mode, (bundle, contribution) in prepared_modes.items():
        charger_costs = {(row.tech, row.vintage): row.cost for row in contribution.cost_invest_rows if "CHRG" in row.tech and row.region == "ON"}
        costs[mode] = charger_costs
        factors[mode] = pd.read_csv(resolve_artifact_path(bundle, "ev_chargers_processed") / "charger_limit_annual_capacity_factor.csv")
        expected = 2020 if mode == "prospective" else 2023
        assert {row.vintage for row in contribution.parameter_rows if "CHRG" in row.tech} == {expected}
    for tech, vintage in costs["prospective"]:
        if vintage == 2025:
            assert costs["prospective"][tech, vintage] == pytest.approx(costs["legacy"][tech, 2030])
    prospective = factors["prospective"].query("period == 2025 and region == 'ON'").set_index("tech").factor
    legacy = factors["legacy"].query("period == 2030 and region == 'ON'").set_index("tech").factor
    assert prospective.to_dict() == legacy.to_dict()


def test_prospective_sqlite_contains_the_configured_grid_and_horizon_marker(prepared_modes):
    for mode, (bundle, _) in prepared_modes.items():
        processed = resolve_artifact_path(bundle, "ev_chargers_processed").parent
        with sqlite3.connect(processed.parent / "transport.sqlite") as connection:
            assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
            periods = connection.execute("SELECT period, flag FROM time_period ORDER BY period").fetchall()
        if mode == "prospective":
            expected = [(year, "e") for year in bundle.scenario.periods.existing]
            expected += [(year, "f") for year in [*bundle.scenario.periods.model, 2050]]
            assert periods == expected
