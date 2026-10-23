"""Focused offline checks for the reviewed EV charger parameter family."""

import math
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from pydantic import ValidationError
from canoe_schema.v4_0 import LimitAnnualCapacityFactor

from parameterization import ev_chargers as layer
from parameterization.ev_chargers import _validate_shares, prepare_charger_capacity_to_activity, prepare_charger_utilization_targets, prepare_ev_charger_rows
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.build_lifetime_parameters import prepare_lifetime_rows
from utils import load_harmonization_rules, resolve_artifact_path, resolve_input_path
from validation.config_models import ScenarioEvChargers
from validation.insertion import validate_parameter_rows


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def prepared_inputs(legacy_bundle, tmp_path_factory):
    bundle = legacy_bundle
    directory = tmp_path_factory.mktemp("ev_charger_inputs")
    isolated = {
        "existing_capacity_interim", "existing_capacity_processed", "existing_capacity_validation",
        "road_stocks_and_demands", "lifetime_validation", "road_lifetimes_survival",
        "vehicle_survival_interim", "lifetime_parameters", "ev_chargers_processed", "ev_chargers_validation",
    }

    def artifact_path(config, family, *parts):
        if family in isolated:
            return directory.joinpath(family, *parts)
        return resolve_artifact_path(config, family, *parts)

    with pytest.MonkeyPatch.context() as patch:
        for owner in ("ev_chargers", "build_existing_capacity", "road_stocks_and_demands",
                      "road_lifetimes_survival", "build_lifetime_parameters"):
            patch.setattr(f"parameterization.{owner}.resolve_artifact_path", artifact_path)
        stock, contexts, _ = prepare_existing_capacity_rows(bundle)
        lifetime_rows = prepare_lifetime_rows(bundle).fixed_rows
        yield bundle, stock, contexts, lifetime_rows


@pytest.fixture(scope="module")
def prepared(prepared_inputs):
    bundle, stock, contexts, lifetime_rows = prepared_inputs
    return bundle, prepare_ev_charger_rows(
        bundle, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
        fixed_lifetime_rows=lifetime_rows,
    )


@pytest.fixture
def isolated_ev_outputs(prepared_inputs, tmp_path, monkeypatch):
    original = layer.resolve_artifact_path

    def artifact_path(config, family, *parts):
        if family in {"ev_chargers_processed", "ev_chargers_validation"}:
            return tmp_path.joinpath(family, *parts)
        return original(config, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", artifact_path)
    return prepared_inputs


def test_charger_port_capacity_and_provenance(prepared) -> None:
    bundle, result = prepared
    audit = result.audit
    assert len(result.capacity_rows) == 18
    assert audit["zero_bev_stock_skipped"] == [["NB", "mhdv"], ["PEI", "mhdv"]]
    ports = pd.DataFrame(audit["port_reconciliation"])
    assert len(ports) == 20
    assert ports.loc[ports.charger_class.eq("ldv"), "observed_public_ports"].sum() == 39220
    for row in ports.to_dict("records"):
        assert row["required_ports"] == pytest.approx(row["bev_vehicles"] / row["ev_per_port"])
        if row["charger_class"] == "ldv":
            assert row["derived_private_ports"] + row["observed_public_ports"] == pytest.approx(row["required_ports"])
            expected_kw = row["L1_ports"] * 1.5 + row["L2_ports"] * 7.2 + row["DCFC_ports"] * 50
        else:
            expected_kw = row["50kW_ports"] * 50 + row["350kW_ports"] * 350
        assert row["capacity_gw"] == pytest.approx(expected_kw / 1_000_000)
    assert all(row.units == "GW" and row.data_id and row.data_source for row in result.capacity_rows)
    assert {row.region for row in result.capacity_rows} == set(
        pd.read_csv(ROOT / "inputs/0_canoe_template/region.csv").region
    )
    assert audit["tc_as_of"] == "March 2026"
    assert audit["bev_stock_year"] == 2023
    assert sum(audit["tc_type_counts"].values()) == 39220
    contexts = {context.data_id: context for context in result.provenance_contexts}
    assert len({row.data_id for row in result.capacity_rows}) == len(result.capacity_rows)
    for row in result.capacity_rows:
        category = "mhdv" if "MHDV" in row.tech else "ldv"
        context = contexts[row.data_id]
        assert context.component_key == f"ev_chargers.existing_capacity.{row.region}.{category}"
        contributors = {item.source_key for item in context.contributors}
        if category == "mhdv":
            assert "pollution_probe_ev_charging_survey" not in contributors
            assert "public ports" not in (row.notes or "")
            expected = bundle.sources.resolved_data_quality(
                "dunsky_ev_charging_infrastructure", "charger_shares_and_utilization",
            )
        else:
            assert "pollution_probe_ev_charging_survey" in contributors
            expected = bundle.sources.resolved_data_quality(
                "transport_canada_ev_dashboard", "public_light_duty_chargers",
            )
        assert context.data_quality == expected


def test_charger_costs_utilization_and_efficiency(prepared, prepared_inputs) -> None:
    bundle, result = prepared
    charger_techs = {mapping[kind] for mapping in load_harmonization_rules(bundle, "ev_chargers")["technology"].values()
                     for kind in ("existing", "new")}
    investments_rows = [row for row in result.invest_rows if row.tech in charger_techs]
    fixed_rows = [row for row in result.fixed_rows if row.tech in charger_techs]
    efficiency_rows = [row for row in result.efficiency_rows if row.tech in charger_techs]
    assert len(investments_rows) == 100
    assert all(row.tech.endswith("_N") for row in investments_rows)
    assert result.audit["existing_investment_rows_excluded"] == 18
    lifetimes = {(row.region, row.tech): row.lifetime for row in prepared_inputs[3]}
    expected_fixed_keys = {
        (row.region, row.tech, row.vintage, period)
        for row in [*result.capacity_rows, *investments_rows]
        for period in bundle.scenario.periods.model
        if row.vintage <= period < row.vintage + lifetimes[row.region, row.tech]
    }
    assert {(row.region, row.tech, row.vintage, row.period) for row in fixed_rows} == expected_fixed_keys
    assert len(fixed_rows) == len(expected_fixed_keys)
    assert len(efficiency_rows) == 118
    assert len(result.utilization) == len(result.utilization_rows) == 118
    assert "period" not in LimitAnnualCapacityFactor.model_fields
    assert result.audit["utilization_schema_supported"] is True
    assert bundle.scenario.charging_profiles.travel_behavior_source == "none"
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("LDV"), "operator"]) == {"e"}
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("MHDV"), "operator"]) == {"e"}
    assert set(result.utilization.units) == {"fraction"}
    assert set(result.utilization.loc[result.utilization.vintage.eq(2025) & result.utilization.tech_or_group.str.contains("LDV"), "factor"]) == {0.2}
    assert set(result.utilization.loc[result.utilization.vintage.eq(2030) & result.utilization.tech_or_group.str.contains("LDV"), "factor"]) == {0.2}
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("MHDV"), "factor"]) == {0.3}
    historical = result.utilization.loc[
        result.utilization.tech_or_group.eq("T_LDV_CHRG_EX")
    ]
    assert set(historical.vintage) == {bundle.scenario.periods.latest_observed_vintage}
    assert set(historical.source_period) == {"2025"}
    assert set(historical.factor) == {0.15}
    context_ids = {context.data_id for context in result.provenance_contexts}
    assert all(row.data_id in context_ids and row.data_source and row.dq_cred is not None
               for row in result.utilization_rows)
    investments = {(r.region, r.tech, r.vintage): r.cost for r in investments_rows}
    conversion = pd.DataFrame(result.audit["cost_conversion"])
    assert conversion.groupby(["charger_class", "vintage"]).count_share.sum().eq(1).all()
    for category, tech in (("ldv", "T_LDV_CHRG_N"), ("mhdv", "T_MHDV_CHRG_N")):
        expected = conversion.loc[
            conversion.charger_class.eq(category) & conversion.vintage.eq(2025),
            "weighted_cad_2020_cost_per_gw",
        ].sum()
        assert investments["ON", tech, 2025] == pytest.approx(expected)
    assert set(conversion.loc[conversion.charger_type.eq("L1"), "source_unit"]) == {"M USD"}
    assert set(conversion.loc[conversion.charger_type.ne("L1"), "source_unit"]) == {"M CAD"}
    for row in fixed_rows:
        category = "ldv" if "LDV" in row.tech else "mhdv"
        basis = conversion.loc[
            conversion.charger_class.eq(category) & conversion.vintage.eq(row.vintage),
            "weighted_cad_2020_cost_per_gw",
        ].sum()
        assert row.cost == pytest.approx(basis * 0.01)
    assert all(row.units == "$M 2020CAD / GW" for row in [*investments_rows, *fixed_rows])
    assert set(result.utilization.vintage) == {bundle.scenario.periods.latest_observed_vintage, *bundle.scenario.periods.model}
    assert set(row.efficiency for row in efficiency_rows if "LDV" in row.tech) == {0.95}
    assert set(row.efficiency for row in efficiency_rows if "MHDV" in row.tech) == {0.8}
    assert all(row.units == "PJ/PJ" for row in efficiency_rows)
    assert not result.utilization.duplicated(["region", "vintage", "tech_or_group", "operator"]).any()
    assert all(math.isfinite(row.cost) and row.cost > 0 for row in result.invest_rows)
    output = layer.resolve_artifact_path(bundle, "ev_chargers_processed")
    rules = load_harmonization_rules(bundle, "ev_chargers")
    assert all((output / rules[key]).is_file() for key in (
        "capacity_output_file", "invest_output_file", "fixed_output_file",
        "efficiency_output_file", "utilization_output_file", "port_audit_file",
        "capacity_to_activity_output_file",
    ))


def test_charger_conversion_covers_existing_and_new_even_without_stock(legacy_bundle):
    technology = pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv")
    regions = set(pd.read_csv(ROOT / "inputs/0_canoe_template/region.csv").region)
    chargers = set(technology.loc[technology.category.eq("charger"), "tech"])
    dataset, rows = prepare_charger_capacity_to_activity(
        legacy_bundle, technology=technology, regions=regions,
    )
    assert {(row.region, row.tech) for row in rows} == {
        (region, tech) for region in regions for tech in chargers
    }
    assert len(rows) == len(regions) * 4
    assert all(row.c2a == 31.536 and row.units == "PJ/GW"
               and row.data_id == dataset.data_id
               and row.data_source is None and row.dq_cred is None
               for row in rows)


def test_hydrogen_refueling_values_currency_coverage_and_provenance(prepared, prepared_inputs):
    bundle, result = prepared
    native = {"T_H2_LDV_REFUEL": (41.1, 2.05, "T_h2_ldv"),
              "T_H2_MHDV_REFUEL": (36.88, 1.84, "T_h2_mhdv")}
    regions = set(pd.read_csv(ROOT / "inputs/0_canoe_template/region.csv").region)
    expected_keys = {(region, tech, vintage) for region in regions for tech in native
                     for vintage in bundle.scenario.periods.model}
    investments = [row for row in result.invest_rows if row.tech in native]
    fixed = [row for row in result.fixed_rows if row.tech in native]
    efficiency = [row for row in result.efficiency_rows if row.tech in native]
    assert len(investments) == len(efficiency) == len(expected_keys)
    assert {(row.region, row.tech, row.vintage) for row in investments} == expected_keys
    assert {(row.region, row.tech, row.vintage) for row in efficiency} == expected_keys
    assert not any(row.tech in native for row in result.capacity_rows)
    assert not any(row.tech_or_group in native for row in result.utilization_rows)

    cer = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    macro = pd.read_csv(resolve_input_path(
        bundle, "interim", cer["interim_subdir_template"].format(edition=edition),
        cer["components"]["macro-indicators"]["output_file"],
    ))
    macro = macro.loc[macro.region.eq("Canada") & macro.scenario.eq(bundle.scenario.economics.cer_scenario)]
    fx = macro.loc[macro.variable_key.eq("cad_per_usd") & macro.year.eq(2018), "value"].item()
    deflator_2018 = macro.loc[macro.variable_key.eq("gdp_deflator") & macro.year.eq(2018), "value"].item()
    deflator_2020 = macro.loc[macro.variable_key.eq("gdp_deflator") & macro.year.eq(2020), "value"].item()
    multiplier = fx * deflator_2020 / deflator_2018
    for row in investments:
        assert row.cost == pytest.approx(native[row.tech][0] * multiplier)
    for row in fixed:
        assert row.cost == pytest.approx(native[row.tech][1] * multiplier)
    assert all(row.units == "$M 2020CAD / (PJ/year)" for row in investments)
    assert all(row.units == "$M 2020CAD / (PJ/year) / year" for row in fixed)
    assert all(row.input_comm == "T_h2_700" and row.output_comm == native[row.tech][2]
               and row.efficiency == 0.995 and row.units == "PJ/PJ" for row in efficiency)
    for tech, (capex, annual_fixed, _) in native.items():
        # H2 fixed cost comes from its own reviewed row, not the EV 1% rule.
        assert annual_fixed / capex != pytest.approx(0.01)

    lifetimes = {(row.region, row.tech): row.lifetime for row in prepared_inputs[3]}
    expected_fixed = {(region, tech, vintage, period) for region, tech, vintage in expected_keys
                      for period in bundle.scenario.periods.model
                      if vintage <= period < vintage + lifetimes[region, tech]}
    assert len(fixed) == len(expected_fixed)
    assert {(row.region, row.tech, row.vintage, row.period) for row in fixed} == expected_fixed
    contexts = {context.data_id: context for context in result.provenance_contexts}
    for table, rows, required_sources in (
        ("cost_invest", investments, {"open_energy_outlook", "cer_canadas_energy_future"}),
        ("cost_fixed", fixed, {"open_energy_outlook", "cer_canadas_energy_future", "argonne_hdsam"}),
        ("efficiency", efficiency, {"iea_future_of_hydrogen"}),
    ):
        for row in rows:
            context = contexts[row.data_id]
            assert required_sources <= {source.source_key for source in context.contributors}
            assert context.data_quality.missing_fields() == []
            assert all(getattr(row, key) == value for key, value in context.parameter_fields(table).items())
    audit = pd.DataFrame(result.audit["hydrogen_refueling"]["cost_conversion"])
    assert set(audit.source_currency) == {"USD"}
    assert set(audit.source_dollar_year) == {2018}
    assert set(audit.target_dollar_year) == {2020}


def test_infrastructure_fixed_costs_stop_at_exact_lifetime_boundary(prepared):
    _, result = prepared
    pairs = {(row.tech, row.vintage, row.period) for row in result.fixed_rows if row.region == "ON"}
    assert ("T_LDV_CHRG_N", 2025, 2035) in pairs
    assert ("T_LDV_CHRG_N", 2025, 2040) not in pairs
    assert ("T_H2_LDV_REFUEL", 2025, 2030) in pairs
    assert ("T_H2_LDV_REFUEL", 2025, 2035) not in pairs


@pytest.mark.parametrize("tech", ["T_LDV_CHRG_N", "T_H2_LDV_REFUEL"])
def test_infrastructure_fixed_costs_require_every_selected_lifetime(isolated_ev_outputs, tech):
    bundle, stock, contexts, lifetime_rows = isolated_ev_outputs
    incomplete = [row for row in lifetime_rows if (row.region, row.tech) != ("ON", tech)]
    with pytest.raises(ValueError, match="(?i)lifetime"):
        prepare_ev_charger_rows(bundle, existing_capacity_rows=stock,
                                existing_capacity_contexts=contexts, fixed_lifetime_rows=incomplete)


def test_infrastructure_fixed_costs_use_supplied_lifetimes(isolated_ev_outputs):
    bundle, stock, contexts, lifetime_rows = isolated_ev_outputs
    changed = [row.model_copy(update={"lifetime": 5}) if row.tech == "T_H2_LDV_REFUEL" else row
               for row in lifetime_rows]
    result = prepare_ev_charger_rows(bundle, existing_capacity_rows=stock,
                                     existing_capacity_contexts=contexts, fixed_lifetime_rows=changed)
    h2_fixed = [row for row in result.fixed_rows if row.tech == "T_H2_LDV_REFUEL"]
    assert h2_fixed and all(row.period == row.vintage for row in h2_fixed)


def test_hydrogen_refueling_rejects_power_capacity_basis_before_reading_inputs(legacy_bundle):
    rules = load_harmonization_rules(legacy_bundle, "ev_chargers")
    rules["hydrogen_refueling"]["capacity_units"] = "GW"
    converter = Mock()
    with pytest.raises(ValueError, match="PJ/year capacity"):
        layer._prepare_hydrogen_refueling(
            legacy_bundle, manual=pd.DataFrame(), rules=rules, technology=pd.DataFrame(),
            commodities=set(), regions={"ON"}, converter=converter, fixed_lives={}, digest="test",
        )
    converter.convert.assert_not_called()


def test_manual_lifetime_fallback_matches_supplied_lifetimes(prepared, isolated_ev_outputs, monkeypatch):
    _, expected = prepared
    bundle, stock, contexts, _ = isolated_ev_outputs

    def unrelated_lifetimes(*args, **kwargs):
        raise AssertionError("Infrastructure fallback invoked road/bus lifetime preparation")

    monkeypatch.setattr("parameterization.build_lifetime_parameters.prepare_lifetime_rows", unrelated_lifetimes)
    monkeypatch.setattr("parameterization.road_lifetimes_survival.derive_accepted_lifetime_frames", unrelated_lifetimes)
    monkeypatch.setattr("parameterization.offroad_lifetimes.prepare_statcan_bus_lifetimes", unrelated_lifetimes)
    result = prepare_ev_charger_rows(bundle, existing_capacity_rows=stock,
                                     existing_capacity_contexts=contexts, fixed_lifetime_rows=None)
    assert [row.model_dump() for row in result.fixed_rows] == [row.model_dump() for row in expected.fixed_rows]


@pytest.mark.parametrize("change, match", [
    ("missing", "one reviewed"), ("duplicate", "one reviewed"),
    ("units", "units"), ("zero", "finite and positive"),
    ("negative", "finite and positive"), ("infinite", "finite and positive"),
    ("subcategory", "one reviewed"), ("notes", "one reviewed"),
])
def test_charger_conversion_rejects_invalid_reviewed_input(legacy_bundle, tmp_path, monkeypatch, change, match):
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/capacity_to_activity.csv")
    selected = manual.category.eq("charger")
    if change == "missing":
        manual = manual.loc[~selected]
    elif change == "duplicate":
        manual = pd.concat([manual, manual.loc[selected]])
    elif change == "units":
        manual.loc[selected, "units"] = "PJ/kW"
    elif change == "subcategory":
        manual.loc[selected, "sub_category"] = "ldv"
    elif change == "notes":
        manual.loc[selected, "notes"] = ""
    else:
        manual.loc[selected, "c2a"] = {"zero": 0, "negative": -1, "infinite": float("inf")}[change]
    manual.to_csv(tmp_path / "capacity_to_activity.csv", index=False)
    original = layer.resolve_input_path
    monkeypatch.setattr(layer, "resolve_input_path", lambda bundle, key, *parts:
                        tmp_path / parts[0] if key == "manual" else original(bundle, key, *parts))
    with pytest.raises(ValueError, match=match):
        prepare_charger_capacity_to_activity(
            legacy_bundle, technology=pd.read_csv(ROOT / "inputs/0_canoe_template/technology.csv"),
            regions={"ON"},
        )


def test_charger_share_contract_rejects_mismatch() -> None:
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/charger_parameters.csv", dtype=str, keep_default_na=False)
    selected = manual.parameter.eq("existing_private_L2_share")
    manual.loc[selected, "value"] = "0.8"
    with pytest.raises(ValueError, match="shares do not sum to one"):
        _validate_shares(manual)


def test_ev_to_port_ratios_are_positive_scenario_values() -> None:
    with pytest.raises(ValidationError):
        ScenarioEvChargers(ld_evs_per_port=0, mhd_evs_per_port=1.5)


@pytest.mark.parametrize("profile,operator,multiplier", [("nhts", "ge", 0.98), ("none", "e", 1.0)])
def test_reviewed_utilization_targets_share_the_annual_basis(legacy_bundle, profile, operator, multiplier):
    selected = replace(legacy_bundle, scenario=legacy_bundle.scenario.model_copy(update={
        "charging_profiles": legacy_bundle.scenario.charging_profiles.model_copy(update={"travel_behavior_source": profile}),
    }))
    targets, context = prepare_charger_utilization_targets(selected)
    ldv = targets.loc[targets.tech_or_group.str.startswith("T_LDV_")]
    mhdv = targets.loc[targets.tech_or_group.str.startswith("T_MHDV_")]
    assert set(ldv.operator) == {operator}
    assert (ldv.factor / ldv.target_utilization).tolist() == pytest.approx([multiplier] * len(ldv))
    assert set(mhdv.operator) == {"e"}
    assert set(mhdv.factor) == set(mhdv.target_utilization) == {0.3}
    assert set(targets.loc[targets.tech_or_group.eq("T_LDV_CHRG_EX"), "target_utilization"]) == {0.15}
    new = targets.loc[targets.tech_or_group.eq("T_LDV_CHRG_N")]
    assert set(new.target_utilization) == {0.2}
    assert set(new.source_period) == {"remainder"}
    assert context.data_quality.missing_fields() == []
    assert {source.source_key for source in context.contributors} == {"dunsky_ev_charging_infrastructure"}
    rows = validate_parameter_rows(
        LimitAnnualCapacityFactor,
        targets.drop(columns=["source_period", "units", "target_utilization", "floor_multiplier"]).to_dict("records"),
        context, bundle=selected,
    )
    assert {(r.operator, r.factor) for r in rows if r.tech_or_group.startswith("T_MHDV_")} == {("e", 0.3)}
    assert {(r.operator, r.factor) for r in rows if r.tech_or_group == "T_LDV_CHRG_EX"} == {(operator, 0.15 * multiplier)}
    assert {(r.operator, r.factor) for r in rows if r.tech_or_group == "T_LDV_CHRG_N"} == {(operator, 0.2 * multiplier)}


def test_annual_profile_mode_is_in_utilization_provenance(legacy_bundle):
    profiled = replace(legacy_bundle, scenario=legacy_bundle.scenario.model_copy(update={
        "charging_profiles": legacy_bundle.scenario.charging_profiles.model_copy(update={"travel_behavior_source": "nhts"}),
    }))
    plain_targets, plain_context = prepare_charger_utilization_targets(legacy_bundle)
    profiled_targets, profiled_context = prepare_charger_utilization_targets(profiled)
    assert plain_context.data_id != profiled_context.data_id
    pd.testing.assert_series_equal(plain_targets.target_utilization, profiled_targets.target_utilization)


@pytest.mark.parametrize("multiplier", [0, -0.1, 1.01, float("inf"), float("nan")])
def test_invalid_floor_slack_fails_before_manual_io(legacy_bundle, monkeypatch, multiplier):
    original = layer.load_harmonization_rules

    def rules(bundle, owner):
        result = original(bundle, owner)
        if owner == "ev_chargers":
            result["utilization"]["ldv"]["floor_multiplier"] = multiplier
        return result

    monkeypatch.setattr(layer, "load_harmonization_rules", rules)
    with pytest.raises(ValueError, match="multiplier"):
        prepare_charger_utilization_targets(legacy_bundle)


def test_missing_reviewed_utilization_is_not_filled(legacy_bundle):
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/charger_parameters.csv", dtype=str, keep_default_na=False)
    manual = manual.loc[~manual.parameter.eq("max_annual_utilization")]
    with pytest.raises(ValueError, match="Expected one reviewed charger value"):
        prepare_charger_utilization_targets(legacy_bundle, manual=manual)


def test_equal_utilization_cannot_include_floor_slack(legacy_bundle, monkeypatch):
    original = layer.load_harmonization_rules

    def rules(bundle, owner):
        result = original(bundle, owner)
        if owner == "ev_chargers":
            result["utilization"]["mhdv"]["floor_multiplier"] = 0.98
        return result

    monkeypatch.setattr(layer, "load_harmonization_rules", rules)
    with pytest.raises(ValueError, match="equality requires multiplier one"):
        prepare_charger_utilization_targets(legacy_bundle)


@pytest.mark.parametrize("regions, expected", [(["ON"], {"ON"}), (["NL", "PE"], {"NLLAB", "PEI"})])
def test_selected_regions_keep_full_source_validation_and_equivalent_rows(prepared, prepared_inputs, tmp_path, monkeypatch, regions, expected):
    bundle, full = prepared
    _, stock, contexts, lifetime_rows = prepared_inputs
    payload = bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = regions
    selected = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    original = layer.resolve_artifact_path

    def output_path(config, family, *parts):
        if family in {"ev_chargers_processed", "ev_chargers_validation"}:
            return tmp_path / family / Path(*parts)
        return original(config, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", output_path)
    subset = prepare_ev_charger_rows(selected, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
                                    fixed_lifetime_rows=lifetime_rows)
    for name in ("capacity_rows", "invest_rows", "fixed_rows", "efficiency_rows", "utilization_rows", "capacity_to_activity_rows"):
        actual = [row.model_dump(exclude={"data_id"}) for row in getattr(subset, name)]
        baseline = [row.model_dump(exclude={"data_id"}) for row in getattr(full, name) if row.region in expected]
        assert actual == baseline
    assert {row["region"] for row in subset.audit["port_reconciliation"]} == expected
    assert sum(subset.audit["tc_type_counts"].values()) == 39220
