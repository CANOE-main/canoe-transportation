"""Focused offline checks for the reviewed EV charger parameter family."""

import math
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError
from canoe_schema.v4_0 import LimitAnnualCapacityFactor

from parameterization import ev_chargers as layer
from parameterization.ev_chargers import _validate_shares, prepare_charger_capacity_to_activity, prepare_charger_utilization_targets, prepare_ev_charger_rows
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from utils import load_harmonization_rules, resolve_artifact_path
from validation.config_models import ScenarioEvChargers
from validation.insertion import validate_parameter_rows


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def prepared(legacy_bundle):
    bundle = legacy_bundle
    stock, contexts, _ = prepare_existing_capacity_rows(bundle)
    return bundle, prepare_ev_charger_rows(
        bundle, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
    )


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


def test_charger_costs_utilization_and_efficiency(prepared) -> None:
    bundle, result = prepared
    assert len(result.invest_rows) == 100
    assert all(row.tech.endswith("_N") for row in result.invest_rows)
    assert result.audit["existing_investment_rows_excluded"] == 18
    assert len(result.fixed_rows) == 390
    assert len(result.efficiency_rows) == 118
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
    investments = {(r.region, r.tech, r.vintage): r.cost for r in result.invest_rows}
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
    for row in result.fixed_rows:
        category = "ldv" if "LDV" in row.tech else "mhdv"
        basis = conversion.loc[
            conversion.charger_class.eq(category) & conversion.vintage.eq(row.vintage),
            "weighted_cad_2020_cost_per_gw",
        ].sum()
        assert row.cost == pytest.approx(basis * 0.01)
    assert all(row.units == "$M 2020CAD / GW" for row in [*result.invest_rows, *result.fixed_rows])
    assert set(result.utilization.vintage) == {bundle.scenario.periods.latest_observed_vintage, *bundle.scenario.periods.model}
    assert set(row.efficiency for row in result.efficiency_rows if "LDV" in row.tech) == {0.95}
    assert set(row.efficiency for row in result.efficiency_rows if "MHDV" in row.tech) == {0.8}
    assert all(row.units == "PJ/PJ" for row in result.efficiency_rows)
    assert not result.utilization.duplicated(["region", "vintage", "tech_or_group", "operator"]).any()
    assert all(math.isfinite(row.cost) and row.cost > 0 for row in result.invest_rows)
    output = resolve_artifact_path(bundle, "ev_chargers_processed")
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
def test_selected_regions_keep_full_source_validation_and_equivalent_rows(prepared, tmp_path, monkeypatch, regions, expected):
    bundle, full = prepared
    stock, contexts, _ = prepare_existing_capacity_rows(bundle)
    payload = bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = regions
    selected = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    original = layer.resolve_artifact_path

    def output_path(config, family, *parts):
        if family in {"ev_chargers_processed", "ev_chargers_validation"}:
            return tmp_path / family / Path(*parts)
        return original(config, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", output_path)
    subset = prepare_ev_charger_rows(selected, existing_capacity_rows=stock, existing_capacity_contexts=contexts)
    for name in ("capacity_rows", "invest_rows", "fixed_rows", "efficiency_rows", "utilization_rows", "capacity_to_activity_rows"):
        actual = [row.model_dump(exclude={"data_id"}) for row in getattr(subset, name)]
        baseline = [row.model_dump(exclude={"data_id"}) for row in getattr(full, name) if row.region in expected]
        assert actual == baseline
    assert {row["region"] for row in subset.audit["port_reconciliation"]} == expected
    assert sum(subset.audit["tc_type_counts"].values()) == 39220
