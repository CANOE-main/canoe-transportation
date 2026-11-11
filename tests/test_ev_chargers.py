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
from parameterization.currency import CerCurrencyConverter
from parameterization.ev_chargers import _validate_shares, charger_cost_components, prepare_charger_capacity_to_activity, prepare_charger_utilization_targets, prepare_ev_charger_rows
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.build_lifetime_parameters import prepare_lifetime_rows
from utils import load_harmonization_rules, resolve_artifact_path, resolve_input_path
from validation.config_models import ScenarioEvChargers
from validation.insertion import validate_parameter_rows


ROOT = Path(__file__).resolve().parents[1]


def cost_converter():
    return CerCurrencyConverter(pd.DataFrame([
        {"scenario": "test", "region": "Canada", "year": year,
         "variable_key": key, "value": value, "unit": unit}
        for year in (2020, 2021, 2022)
        for key, value, unit in (("gdp_deflator", 100., "2020=100"),
                                 ("cad_per_usd", 1.25, "C$/US$"))
    ]), scenario="test", target_year=2020)


def _dashboard_observations(bundle):
    rules = load_harmonization_rules(bundle, "assorted_sources")["tc_ev_dashboard"]["public_chargers"]
    directory = resolve_artifact_path(bundle, "tc_ev_dashboard_interim")
    return (pd.read_csv(directory / rules["province_output_file"]),
            pd.read_csv(directory / rules["type_output_file"]))


def _discrepant_charger_observations():
    provincial = pd.DataFrame({"public_chargers": [40164]})
    types = pd.DataFrame({"public_chargers": [31293, 8826],
                          "national_share": [31293 / 40119, 8826 / 40119]})
    return provincial, types


def test_provincial_precedence_preserves_published_type_counts_and_normalized_mix(caplog):
    provincial, types = _discrepant_charger_observations()
    before = types.copy(deep=True)
    assert layer._public_charger_totals(provincial, types, precedence="provincial") == (40164, 40119)
    pd.testing.assert_frame_equal(types, before)
    assert types.national_share.sum() == pytest.approx(1)
    assert sum(40164 * types.national_share) == pytest.approx(40164)
    assert "difference=+45" in caplog.text


def test_strict_charger_total_policy_remains_available():
    provincial, types = _discrepant_charger_observations()
    with pytest.raises(ValueError, match="national totals"):
        layer._public_charger_totals(provincial, types, precedence="require_equal")


@pytest.mark.parametrize("change,match", [
    ("negative", "Negative"), ("fractional", "finite integers"),
    ("nonfinite", "finite integers"), ("missing", "finite integers"),
    ("zero_type_total", "type total must be positive"),
    ("invalid_shares", "type shares"), ("provincial_share_denominator", "type shares"),
    ("unknown_policy", "national_total_precedence"),
])
def test_provincial_precedence_keeps_count_and_type_mix_validation(change, match):
    provincial, types = _discrepant_charger_observations()
    precedence = "provincial"
    if change in {"negative", "fractional", "nonfinite", "missing"}:
        provincial["public_chargers"] = [{
            "negative": -1, "fractional": 1.5, "nonfinite": float("inf"), "missing": float("nan"),
        }[change]]
    elif change == "zero_type_total":
        types["public_chargers"] = 0
    elif change == "invalid_shares":
        types["national_share"] = [.7, .2]
    elif change == "provincial_share_denominator":
        types["national_share"] = types.public_chargers / 40164
    else:
        precedence = "unknown"
    with pytest.raises(ValueError, match=match):
        layer._public_charger_totals(provincial, types, precedence=precedence)


def mhd_cost_inputs():
    records = []
    for kind, share, power, expense in (("50kW", .82, 50., 78637.),
                                        ("350kW", .16, 350., 234129.),
                                        ("2MW", .02, 2000., 645224.)):
        for parameter, unit, year, period, value in (
            (f"{kind}_capex_aggregation", "", "", "through_2050", share),
            (f"{kind}_rated_power", "kW", "", "all", power),
            (f"{kind}_capex", "CAD", "2022", "all", expense),
        ):
            records.append({"category": "charger", "sub_category": "mhdv", "parameter": parameter,
                            "unit": unit, "currency_year": year, "period": period, "value": value})
    return pd.DataFrame(records)


def test_native_port_expenditure_divided_by_power_preserves_cost_conservation():
    rate, components = charger_cost_components(mhd_cost_inputs(), "mhdv", 2025, cost_converter())
    # A literal 100-port fleet: 82*78,637 + 16*234,129 + 2*645,224 CAD,
    # divided by 82*50 + 16*350 + 2*2,000 kW. CAD/kW == M CAD/GW.
    assert rate == pytest.approx(11484746 / 13700)
    assert rate == pytest.approx(838.3020437956204)
    assert {row["mean_rated_power_kw"] for row in components} == {137.}
    assert [row["power_share"] for row in components] == pytest.approx([41/137, 56/137, 40/137])
    assert sum(row["weighted_cad_2020_cost_per_gw"] for row in components) == pytest.approx(rate)
    assert sum(row["count_share"] * row["cad_2020_cost_per_gw"] for row in components) != pytest.approx(rate)
    assert {row["power_source_period"] for row in components} == {"all"}


def test_costs_and_rated_powers_select_the_same_source_calendar_year():
    records = []
    for kind, share in (("L1", .1), ("L2", .8), ("DCFC", .1)):
        records.append({"category": "charger", "sub_category": "ldv", "parameter": f"{kind}_capex_aggregation",
                        "unit": "", "currency_year": "", "period": "through_2050", "value": share})
    for kind, parameter, unit, year, period, value in (
        ("L1", "capex", "USD", "2021", "all", 100.),
        ("L2", "capex", "CAD", "2022", "all", 900.),
        ("DCFC", "capex", "CAD", "2022", "all", 2000.),
        ("L1", "rated_power", "kW", "", "all", 1.),
        ("L2", "rated_power", "kW", "", "2025", 2.),
        ("L2", "rated_power", "kW", "", "2030", 4.),
        ("L2", "rated_power", "kW", "", "through_2050", 5.),
        ("DCFC", "rated_power", "kW", "", "2025", 10.),
        ("DCFC", "rated_power", "kW", "", "through_2050", 20.),
    ):
        records.append({"category": "charger", "sub_category": "ldv", "parameter": f"{kind}_{parameter}",
                        "unit": unit, "currency_year": year, "period": period, "value": value})
    manual = pd.DataFrame(records)
    for source_year, denominator in ((2025, 2.7), (2030, 5.3), (2035, 6.1)):
        rate, components = charger_cost_components(manual, "ldv", source_year, cost_converter())
        # USD100 L1 becomes CAD125 before averaging ordinary per-port costs.
        assert rate == pytest.approx(932.5 / denominator)
        assert {row["source_year"] for row in components} == {source_year}
        assert components[0]["cad_2020_cost_per_port"] == 125.
    with pytest.raises(ValueError, match="Missing charger period selector"):
        charger_cost_components(manual, "ldv", 2055, cost_converter())


@pytest.mark.parametrize("parameter,column,value,match", [
    ("50kW_capex", "unit", "M CAD", "native CAD/USD per port"),
    ("50kW_capex", "value", 0., "positive and finite"),
    ("50kW_capex", "value", float("nan"), "positive and finite"),
    ("50kW_capex", "currency_year", "", "dollar year"),
    ("50kW_rated_power", "unit", "MW", "positive finite kW"),
    ("50kW_rated_power", "value", 0., "positive finite kW"),
    ("50kW_rated_power", "value", float("inf"), "positive finite kW"),
    ("50kW_capex_aggregation", "value", .8, "sum to one"),
])
def test_native_charger_costs_reject_invalid_source_units_values_and_shares(parameter, column, value, match):
    manual = mhd_cost_inputs()
    manual.loc[manual.parameter.eq(parameter), column] = value
    with pytest.raises(ValueError, match=match):
        charger_cost_components(manual, "mhdv", 2025, cost_converter())


@pytest.mark.parametrize("problem", ["missing", "duplicate"])
def test_native_charger_costs_reject_incomplete_or_ambiguous_power_coverage(problem):
    manual = mhd_cost_inputs()
    selected = manual.parameter.eq("50kW_rated_power")
    manual = manual.loc[~selected] if problem == "missing" else pd.concat([manual, manual.loc[selected]])
    with pytest.raises(ValueError, match="Missing charger period selector|Duplicate charger period selector"):
        charger_cost_components(manual, "mhdv", 2025, cost_converter())


@pytest.fixture(scope="module")
def prepared_inputs(isolated_legacy_bundle, tmp_path_factory):
    bundle = isolated_legacy_bundle
    directory = tmp_path_factory.mktemp("ev_charger_inputs")
    isolated = {
        "existing_capacity_interim", "existing_capacity_processed", "existing_capacity_validation",
        "lifetime_validation", "road_lifetimes_survival",
        "vehicle_survival_interim", "lifetime_parameters", "ev_chargers_processed", "ev_chargers_validation",
    }

    def artifact_path(config, family, *parts):
        if family in isolated:
            return directory.joinpath(family, *parts)
        return resolve_artifact_path(config, family, *parts)

    with pytest.MonkeyPatch.context() as patch:
        for owner in ("ev_chargers", "build_existing_capacity",
                      "road_lifetimes_survival", "build_lifetime_parameters"):
            patch.setattr(f"parameterization.{owner}.resolve_artifact_path", artifact_path)
        fleet = prepare_existing_capacity_rows(bundle)
        stock, contexts = fleet.existing_capacity_rows, fleet.provenance_contexts
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
    provincial, types = _dashboard_observations(bundle)
    published_total = int(provincial.public_chargers.sum())
    ports = pd.DataFrame(audit["port_reconciliation"])
    assert len(ports) == 20
    zero_stock = ports.loc[ports.bev_vehicles.eq(0), ["region", "charger_class"]]
    assert audit["zero_bev_stock_skipped"] == zero_stock.values.tolist()
    assert len(result.capacity_rows) == len(ports) - len(zero_stock)
    assert ports.loc[ports.charger_class.eq("ldv"), "observed_public_ports"].sum() == published_total
    for row in ports.to_dict("records"):
        assert row["required_ports"] == pytest.approx(row["bev_vehicles"] / row["ev_per_port"])
        if row["charger_class"] == "ldv":
            assert row["derived_private_ports"] + row["observed_public_ports"] == pytest.approx(row["required_ports"])
            expected_kw = row["L1_ports"] * 1.5 + row["L2_ports"] * 7.2 + row["DCFC_ports"] * 125
        else:
            expected_kw = row["50kW_ports"] * 50 + row["350kW_ports"] * 350
        assert row["capacity_gw"] == pytest.approx(expected_kw / 1_000_000)
        assert row["rated_power_source_year"] == min(bundle.scenario.periods.model)
    assert all(row.units == "GW" and row.data_id and row.data_source for row in result.capacity_rows)
    assert {row.region for row in result.capacity_rows} == set(
        pd.read_csv(ROOT / "inputs/0_canoe_template/region.csv").region
    )
    assert audit["tc_as_of"] == provincial.as_of.iloc[0]
    assert audit["bev_stock_year"] == 2023
    assert audit["tc_public_ports"] == published_total
    assert audit["tc_type_counts"] == types.set_index("charger_type").public_chargers.to_dict()
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


def test_charger_preparation_uses_provincial_total_and_source_reporting_date(
    isolated_ev_outputs, tmp_path, monkeypatch,
):
    bundle, stock, contexts, lifetime_rows = isolated_ev_outputs
    provincial, types = _dashboard_observations(bundle)
    chart_total = int(types.public_chargers.sum())
    provincial.loc[provincial.source_region.eq("Ontario"), "public_chargers"] += (
        chart_total + 45 - int(provincial.public_chargers.sum())
    )
    provincial["as_of"] = types["as_of"] = "June 2026"
    source_key, component_key = "transport_canada_ev_dashboard", "public_light_duty_chargers"
    source = bundle.sources.sources[source_key]
    component = source.component(component_key)
    changed_component = component.model_copy(update={"adapter": {**component.adapter, "reporting_date": "June 2026"}})
    changed_source = source.model_copy(update={"components": {**source.components, component_key: changed_component}})
    changed_sources = bundle.sources.model_copy(update={"sources": {**bundle.sources.sources, source_key: changed_source}})
    selected = replace(bundle, sources=changed_sources)
    dashboard = load_harmonization_rules(bundle, "assorted_sources")["tc_ev_dashboard"]["public_chargers"]
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    provincial.to_csv(source_dir / dashboard["province_output_file"], index=False)
    types.to_csv(source_dir / dashboard["type_output_file"], index=False)
    original_path, original_rules = layer.resolve_artifact_path, layer.load_harmonization_rules

    def artifact_path(config, family, *parts):
        return source_dir.joinpath(*parts) if family == "tc_ev_dashboard_interim" else original_path(config, family, *parts)

    def rules(config, owner):
        result = original_rules(config, owner)
        if owner == "assorted_sources":
            result["tc_ev_dashboard"]["public_chargers"]["national_total_precedence"] = "provincial"
        return result

    monkeypatch.setattr(layer, "resolve_artifact_path", artifact_path)
    monkeypatch.setattr(layer, "load_harmonization_rules", rules)
    result = prepare_ev_charger_rows(
        selected, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
        fixed_lifetime_rows=lifetime_rows,
    )
    assert result.audit["tc_as_of"] == "June 2026"
    assert result.audit["tc_public_ports"] == chart_total + 45
    assert result.audit["tc_type_total"] == chart_total
    assert result.audit["tc_type_counts"] == types.set_index("charger_type").public_chargers.to_dict()
    assert result.audit["tc_total_reconciliation"]["difference"] == 45
    assert result.audit["tc_total_reconciliation"]["policy"] == "provincial"
    ports = [row for row in result.audit["port_reconciliation"] if row["charger_class"] == "ldv"]
    assert sum(row["observed_public_ports"] for row in ports) == chart_total + 45
    dcfc_share = float(types.loc[types.charger_type.eq("DCFC"), "national_share"].iloc[0])
    for row in ports:
        assert row["DCFC_ports"] == pytest.approx(row["observed_public_ports"] * dcfc_share)
        assert row["L1_ports"] + row["L2_ports"] + row["DCFC_ports"] == pytest.approx(row["required_ports"])


def test_charger_costs_utilization_and_efficiency(prepared, prepared_inputs) -> None:
    bundle, result = prepared
    charger_techs = {mapping[kind] for mapping in load_harmonization_rules(bundle, "ev_chargers")["technology"].values()
                     for kind in ("existing", "new")}
    investments_rows = [row for row in result.invest_rows if row.tech in charger_techs]
    fixed_rows = [row for row in result.fixed_rows if row.tech in charger_techs]
    efficiency_rows = [row for row in result.efficiency_rows if row.tech in charger_techs]
    assert len(investments_rows) == 100
    assert all(row.tech.endswith("_N") for row in investments_rows)
    assert result.audit["existing_investment_rows_excluded"] == len(result.capacity_rows)
    lifetimes = {(row.region, row.tech): row.lifetime for row in prepared_inputs[3]}
    expected_fixed_keys = {
        (row.region, row.tech, row.vintage, period)
        for row in [*result.capacity_rows, *investments_rows]
        for period in bundle.scenario.periods.model
        if row.vintage <= period < row.vintage + lifetimes[row.region, row.tech]
    }
    assert {(row.region, row.tech, row.vintage, row.period) for row in fixed_rows} == expected_fixed_keys
    assert len(fixed_rows) == len(expected_fixed_keys)
    expected_efficiency_keys = {(row.region, row.tech, row.vintage)
                                for row in [*result.capacity_rows, *investments_rows]}
    assert len(efficiency_rows) == len(expected_efficiency_keys)
    assert {(row.region, row.tech, row.vintage) for row in efficiency_rows} == expected_efficiency_keys
    assert len(result.utilization) == len(result.utilization_rows) == len(expected_efficiency_keys)
    assert {(row.region, row.tech_or_group, row.vintage) for row in result.utilization_rows} == expected_efficiency_keys
    assert "period" not in LimitAnnualCapacityFactor.model_fields
    assert result.audit["utilization_schema_supported"] is True
    assert bundle.scenario.charging_profiles.travel_behavior_source == "none"
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("LDV"), "operator"]) == {"e"}
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("MHDV"), "operator"]) == {"e"}
    assert set(result.utilization.units) == {"fraction"}
    assert set(result.utilization.loc[result.utilization.vintage.eq(2025) & result.utilization.tech_or_group.str.contains("LDV"), "factor"]) == {0.2}
    assert set(result.utilization.loc[result.utilization.vintage.eq(2030) & result.utilization.tech_or_group.str.contains("LDV"), "factor"]) == {0.2}
    assert set(result.utilization.loc[result.utilization.tech_or_group.str.contains("MHDV"), "factor"]) == {0.2}
    historical = result.utilization.loc[
        result.utilization.tech_or_group.eq("T_LDV_CHRG_EX")
    ]
    assert set(historical.vintage) == {bundle.scenario.periods.latest_observed_vintage}
    assert set(historical.source_period) == {"all"}
    assert set(historical.factor) == {0.2}
    context_ids = {context.data_id for context in result.provenance_contexts}
    assert all(row.data_id in context_ids and row.data_source and row.dq_cred is not None
               for row in result.utilization_rows)
    investments = {(r.region, r.tech, r.vintage): r.cost for r in investments_rows}
    conversion = pd.DataFrame(result.audit["cost_conversion"])
    assert conversion.groupby(["charger_class", "vintage"]).count_share.sum().eq(1).all()
    assert conversion.groupby(["charger_class", "vintage"]).power_share.sum().tolist() == pytest.approx(
        [1] * conversion.groupby(["charger_class", "vintage"]).ngroups,
    )
    assert set(conversion.aggregation_basis) == {"total_expenditure_per_rated_power"}
    for category, tech in (("ldv", "T_LDV_CHRG_N"), ("mhdv", "T_MHDV_CHRG_N")):
        expected = conversion.loc[
            conversion.charger_class.eq(category) & conversion.vintage.eq(2025),
            "weighted_cad_2020_cost_per_gw",
        ].sum()
        assert investments["ON", tech, 2025] == pytest.approx(expected)
    assert set(conversion.loc[conversion.charger_type.eq("L1"), "source_unit"]) == {"USD"}
    assert set(conversion.loc[conversion.charger_type.ne("L1"), "source_unit"]) == {"CAD"}
    for row in fixed_rows:
        category = "ldv" if "LDV" in row.tech else "mhdv"
        basis = conversion.loc[
            conversion.charger_class.eq(category) & conversion.vintage.eq(row.vintage),
            "weighted_cad_2020_cost_per_gw",
        ].sum()
        assert row.cost == pytest.approx(basis * 0.01)
    assert all(row.units == "$M 2020CAD / GW" for row in [*investments_rows, *fixed_rows])
    assert set(result.utilization.vintage) == {bundle.scenario.periods.latest_observed_vintage, *bundle.scenario.periods.model}
    assert {row.efficiency for row in efficiency_rows} == {1.0}
    assert all(row.units == "PJ/PJ" for row in efficiency_rows)
    assert all(row.data_id == result.assumption_dataset.data_id
               and row.data_source is None and row.dq_cred is None and row.dq_geog is None
               and row.dq_struc is None and row.dq_tech is None and row.dq_time is None
               for row in efficiency_rows)
    assert "unity electricity transfer" in result.assumption_dataset.description
    assert not {"icct_ev_charge", "icct_hdv_charge"} & {
        source.source_key for context in result.provenance_contexts for source in context.contributors
    }
    accounting = result.audit["electricity_accounting"]
    assert accounting["vehicle_energy_basis"] == "grid_electricity_including_charging_losses"
    assert accounting["charger_transfer_efficiency"] == 1.0
    assert accounting["internal_assumption_data_id"] == result.assumption_dataset.data_id
    assert {row["charger_class"]: row["reviewed_efficiency"]
            for row in accounting["reviewed_efficiencies_not_applied"]} == {"ldv": 0.95, "mhdv": 0.8}
    assert all("ICCT" in row["source"] for row in accounting["reviewed_efficiencies_not_applied"])
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


@pytest.mark.parametrize("basis,value,match", [
    ("battery_electricity", 1.0, "energy basis"),
    (None, 1.0, "energy basis"),
    ("grid_electricity_including_charging_losses", 0.95, "unity"),
    ("grid_electricity_including_charging_losses", 0.8, "unity"),
    ("grid_electricity_including_charging_losses", float("nan"), "unity"),
    ("grid_electricity_including_charging_losses", float("inf"), "unity"),
    ("grid_electricity_including_charging_losses", None, "unity"),
    ("grid_electricity_including_charging_losses", True, "unity"),
])
def test_charger_accounting_rejects_incompatible_losses_before_input_io(
    legacy_bundle, monkeypatch, basis, value, match,
):
    original = layer.load_harmonization_rules

    def rules(bundle, owner):
        result = original(bundle, owner)
        if owner == "ev_chargers":
            result["electricity_accounting"] = {
                "vehicle_energy_basis": basis, "charger_transfer_efficiency": value,
            }
        return result

    monkeypatch.setattr(layer, "load_harmonization_rules", rules)
    input_path = Mock(side_effect=AssertionError("Invalid accounting reached input I/O"))
    monkeypatch.setattr(layer, "resolve_input_path", input_path)
    with pytest.raises(ValueError, match=match):
        prepare_charger_capacity_to_activity(legacy_bundle, technology=pd.DataFrame(), regions={"ON"})
    input_path.assert_not_called()


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
    assert set(mhdv.factor) == set(mhdv.target_utilization) == {0.2}
    assert set(targets.loc[targets.tech_or_group.eq("T_LDV_CHRG_EX"), "target_utilization"]) == {0.2}
    new = targets.loc[targets.tech_or_group.eq("T_LDV_CHRG_N")]
    assert set(new.target_utilization) == {0.2}
    assert set(new.source_period) == {"all"}
    assert context.data_quality.missing_fields() == []
    assert {source.source_key for source in context.contributors} == {"dunsky_ev_charging_infrastructure"}
    rows = validate_parameter_rows(
        LimitAnnualCapacityFactor,
        targets.drop(columns=["source_period", "units", "target_utilization", "floor_multiplier"]).to_dict("records"),
        context, bundle=selected,
    )
    assert {(r.operator, r.factor) for r in rows if r.tech_or_group.startswith("T_MHDV_")} == {("e", 0.2)}
    assert {(r.operator, r.factor) for r in rows if r.tech_or_group == "T_LDV_CHRG_EX"} == {(operator, 0.2 * multiplier)}
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
    assert subset.audit["tc_type_counts"] == full.audit["tc_type_counts"]
    scoped_stock = [row for row in stock if row.region in expected]
    scoped_ids = {row.data_id for row in scoped_stock}
    repeated = prepare_ev_charger_rows(
        selected, existing_capacity_rows=scoped_stock,
        existing_capacity_contexts=[context for context in contexts if context.data_id in scoped_ids],
        fixed_lifetime_rows=lifetime_rows,
    )
    _assert_preparation_equal(repeated, subset)


def _assert_preparation_equal(actual, expected):
    for name in (
        "capacity_rows", "invest_rows", "fixed_rows", "efficiency_rows", "utilization_rows",
        "capacity_to_activity_rows", "provenance_contexts", "assumption_dataset", "audit",
    ):
        assert getattr(actual, name) == getattr(expected, name), name
    pd.testing.assert_frame_equal(actual.utilization, expected.utilization, check_exact=True)


@pytest.mark.parametrize("disk_state", ["missing", "unrelated"])
def test_supplied_capacity_is_independent_of_incidental_csv(
    prepared, isolated_ev_outputs, tmp_path, monkeypatch, disk_state,
):
    bundle, stock, contexts, lifetimes = isolated_ev_outputs
    incidental = tmp_path / "incidental"
    if disk_state == "unrelated":
        incidental.mkdir()
        (incidental / "existing_capacity.csv").write_text("unrelated,stale\n1,2\n")
    original = layer.resolve_artifact_path

    def artifact_path(config, family, *parts):
        if family == "existing_capacity_processed":
            pytest.fail("Charger preparation accessed an unconsumed capacity CSV")
        return original(config, family, *parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", artifact_path)
    result = prepare_ev_charger_rows(
        bundle, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
        fixed_lifetime_rows=lifetimes,
    )
    _assert_preparation_equal(result, prepared[1])
    assert "existing_capacity.csv" not in result.audit["input_sha256"]
    assert result.audit["prepared_stock"]["contract"] == "supplied_bev_physical_stock_v2"


def test_prepared_stock_identity_tracks_rows_and_full_lineage(prepared_inputs):
    from parameterization.build_existing_capacity import physical_stock_for_consumers

    bundle, stock, contexts, _ = prepared_inputs
    rows = physical_stock_for_consumers(bundle, stock, None)
    rows = rows.loc[rows.tech.str.contains("BEV") & rows.region.eq("ON")].reset_index(drop=True)
    assert len(rows) > 1
    identity, selected = layer._prepared_stock_identity(rows, contexts)
    repeat, _ = layer._prepared_stock_identity(rows, [*reversed(contexts), *contexts])
    assert repeat == identity
    reordered, _ = layer._prepared_stock_identity(rows.iloc[::-1], contexts)
    assert reordered["sha256"] != identity["sha256"]
    changed_rows = rows.copy()
    changed_rows.loc[0, "surviving_k_vehicles"] += .125
    changed, _ = layer._prepared_stock_identity(changed_rows, contexts)
    assert changed["sha256"] != identity["sha256"]
    changed_rows = rows.copy()
    changed_rows.loc[0, "vintage"] = 1900
    changed_key, _ = layer._prepared_stock_identity(changed_rows, contexts)
    assert changed_key["sha256"] != identity["sha256"]
    changed_contexts = [context.model_copy(update={"dataset_description": "changed lineage"})
                        if context.data_id == rows.iloc[0].data_id else context for context in contexts]
    changed_lineage, _ = layer._prepared_stock_identity(rows, changed_contexts)
    assert changed_lineage["sha256"] != identity["sha256"]
    assert set(selected) == set(rows.data_id)


@pytest.mark.parametrize("defect, message", [
    ("duplicate_rows", "duplicate cohort keys"),
    ("missing_context", "provenance contexts are missing"),
    ("conflicting_context", "Conflicting BEV stock provenance"),
    ("nonfinite", "Physical-stock handoff has null"),
])
def test_invalid_supplied_stock_fails_before_publication(isolated_ev_outputs, tmp_path, defect, message):
    bundle, stock, contexts, lifetimes = isolated_ev_outputs
    row = next(row for row in stock if "BEV" in row.tech and row.region == "ON")
    if defect == "duplicate_rows":
        stock = [*stock, row]
    elif defect == "missing_context":
        contexts = [context for context in contexts if context.data_id != row.data_id]
    elif defect == "conflicting_context":
        context = next(context for context in contexts if context.data_id == row.data_id)
        contexts = [*contexts, context.model_copy(update={"dataset_description": "conflict"})]
    else:
        stock = [item.model_copy(update={"capacity": float("nan")}) if item is row else item for item in stock]
    with pytest.raises(ValueError, match=message):
        prepare_ev_charger_rows(
            bundle, existing_capacity_rows=stock, existing_capacity_contexts=contexts,
            fixed_lifetime_rows=lifetimes,
        )
    assert not (tmp_path / "ev_chargers_processed").exists()
    assert not (tmp_path / "ev_chargers_validation").exists()


def test_unused_capacity_and_contexts_do_not_change_identity(prepared, isolated_ev_outputs):
    bundle, stock, contexts, lifetimes = isolated_ev_outputs
    consumed_ids = set(prepared[1].audit["prepared_stock"]["data_ids"])
    changed = [row.model_copy(update={"capacity": row.capacity + 1})
               if "BEV" not in row.tech else row for row in stock]
    contexts = [context.model_copy(update={"dataset_description": "unused context"})
                if context.data_id not in consumed_ids else context for context in contexts]
    result = prepare_ev_charger_rows(
        bundle, existing_capacity_rows=changed, existing_capacity_contexts=contexts,
        fixed_lifetime_rows=lifetimes,
    )
    _assert_preparation_equal(result, prepared[1])


def test_changed_supplied_stock_changes_output_and_repeated_identity(prepared, isolated_ev_outputs):
    bundle, stock, contexts, lifetimes = isolated_ev_outputs
    original = next(row for row in stock if row.tech == "T_LDV_C_BEV_EX" and row.region == "ON")
    changed = [row.model_copy(update={"capacity": row.capacity + 0.125})
               if row is original else row for row in stock]
    results = [prepare_ev_charger_rows(
        bundle, existing_capacity_rows=changed, existing_capacity_contexts=contexts,
        fixed_lifetime_rows=lifetimes,
    ) for _ in range(2)]
    _assert_preparation_equal(*results)
    assert results[0].audit["prepared_stock"]["sha256"] != prepared[1].audit["prepared_stock"]["sha256"]
    before = next(row for row in prepared[1].capacity_rows if row.region == "ON" and row.tech == "T_LDV_CHRG_EX")
    after = next(row for row in results[0].capacity_rows if row.region == "ON" and row.tech == "T_LDV_CHRG_EX")
    assert after.capacity > before.capacity
    assert after.data_id != before.data_id
