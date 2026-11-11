"""Read-only charging comparisons must not manufacture units, zeros or time matches."""

import json
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from parameterization.currency import CerCurrencyConverter
from utils import file_sha256
from validation import charging_cost_comparison as layer
from validation.charging_cost_comparison import (
    ChargingComparisonRules, ChargingCostComparison, ReferenceCostBasis, _read_backend,
    _source_components_for_year, _source_cost_components,
    backend_cost_basis,
    compare_cost_frames, fixed_cost_ratios, inherited_reference_context, read_reference_costs,
    verify_build_snapshot, write_report,
)
from validation.config_models import ScenarioPeriods


def rules(**overrides):
    return ChargingComparisonRules.model_validate({
        "reference_route": "reference", "report_route": "report",
        "files": {"cost_invest": "CostInvest.csv", "cost_fixed": "CostFixed.csv",
                  "cost_variable": "CostVariable_ALL.csv"},
        "groups": {"ldv": {"reference_technologies": ["BEV_CHRG", "PHEV_CHRG"],
                           "existing": "LDV_EX", "new": "LDV_N"}}, **overrides,
    })


def basis(denominator="GW", **overrides):
    return {"name": "declared", "currency": "CAD", "dollar_year": 2020,
            "denominator": denominator, "evidence": "Test provider statement", **overrides}


def periods(mode="legacy"):
    return ScenarioPeriods(period_mode=mode, base_year=2023, existing=[2020, 2023] if mode == "legacy" else [2020],
                           model=[2025, 2030], step=5)


def converter():
    return CerCurrencyConverter(pd.DataFrame([
        {"scenario": "s", "region": "Canada", "year": year, "variable_key": key,
         "value": value, "unit": unit}
        for year, deflator in [(2018, 100), (2020, 110)]
        for key, value, unit in [("gdp_deflator", deflator, "2018=100"), ("cad_per_usd", 1.25, "C$/US$")]
    ]), scenario="s", target_year=2020)


def component_manual():
    records = []
    for kind, share, capex, currency, dollar_year, power in (
        ("L1", .5, 100., "USD", "2018", 1.),
        ("L2", .4, 9000., "CAD", "2020", 9.),
        ("DCFC", .1, 100000., "CAD", "2020", 100.),
    ):
        for suffix, value, unit, year, period in (
            ("capex_aggregation", share, "", "", "through_2050"),
            ("capex", capex, currency, dollar_year, "all"),
            ("rated_power", power, "kW", "", "all"),
        ):
            records.append({"category": "charger", "sub_category": "ldv",
                            "parameter": f"{kind}_{suffix}", "value": value, "unit": unit,
                            "currency_year": year, "period": period, "notes": f"{kind} {suffix}"})
    records.extend([
        {"category": "charger", "sub_category": "ldv", "parameter": "max_annual_utilization",
         "value": .2, "unit": "", "currency_year": "", "period": "all", "notes": "Reviewed utilization"},
        {"category": "charger", "sub_category": "all", "parameter": "fixed_to_capex_ratio",
         "value": .01, "unit": "", "currency_year": "", "period": "all", "notes": "Annual fixed/capex"},
    ])
    return pd.DataFrame(records)


def test_source_capex_and_power_conserve_fleet_expenditure_with_currency_conversion():
    result = _source_components_for_year(component_manual(), category="ldv", source_year=2030,
                                         converter=converter())
    # One hundred ports cost 50*137.5 + 40*9000 + 10*100000 CAD2020 and provide
    # 50*1 + 40*9 + 10*100 kW. Currency and kW scaling cancel in M CAD/GW.
    expected = (50 * 137.5 + 40 * 9000 + 10 * 100000) / (50 * 1 + 40 * 9 + 10 * 100)
    assert result.weighted_cost_per_gw.sum() == pytest.approx(expected)
    assert result.fleet_mean_rated_power_kw.eq(14.1).all()
    assert result.power_share.tolist() == pytest.approx([.5 / 14.1, 3.6 / 14.1, 10 / 14.1])
    assert (result.power_share * result.cad_2020_cost_per_gw).sum() == pytest.approx(expected)
    assert result.weighted_cad_2020_cost_per_port.sum() == pytest.approx(13668.75)
    assert result.loc[result.charger_type.eq("L1"), "source_cost_per_port"].item() == 100
    assert result.loc[result.charger_type.eq("L1"), "cad_2020_cost_per_port"].item() == 137.5
    assert (result.count_share * result.cad_2020_cost_per_gw).sum() != pytest.approx(expected)
    assert set(result.aggregation_basis) == {"total_expenditure_per_rated_power"}


def test_source_cost_and_power_select_the_same_projection_year():
    manual = component_manual()
    power = manual.parameter.eq("L2_rated_power")
    capex = manual.parameter.eq("L2_capex")
    later_power, later_capex = manual.loc[power].copy(), manual.loc[capex].copy()
    manual.loc[power, ["period", "value"]] = ["2025", 7.2]
    manual.loc[capex, ["period", "value"]] = ["2025", 15000]
    later_power["period"], later_power["value"] = "2030", 10
    later_capex["period"], later_capex["value"] = "through_2050", 10000
    manual = pd.concat([manual, later_power, later_capex], ignore_index=True)
    result = _source_components_for_year(manual, category="ldv", source_year=2030, converter=converter())
    l2 = result.loc[result.charger_type.eq("L2")].iloc[0]
    assert l2.rated_power_kw == 10
    assert l2.source_cost_per_port == 10000
    assert l2.cost_source_period == "through_2050"
    assert l2.power_source_period == "2030"


@pytest.mark.parametrize("change,match", [
    ("million_currency", "currency per port"),
    ("power_unit", "rated power in kW"),
    ("zero_power", "positive rated power"),
    ("nonfinite_capex", "positive per-port capex"),
    ("missing_power", "Missing charger period selector"),
    ("share_sum", "shares do not sum"),
])
def test_source_cost_denominator_and_native_units_must_be_explicit(change, match):
    manual = component_manual()
    if change == "million_currency":
        manual.loc[manual.parameter.eq("L1_capex"), "unit"] = "M USD"
    elif change == "power_unit":
        manual.loc[manual.parameter.eq("L2_rated_power"), "unit"] = "MW"
    elif change == "zero_power":
        manual.loc[manual.parameter.eq("L2_rated_power"), "value"] = 0
    elif change == "nonfinite_capex":
        manual.loc[manual.parameter.eq("DCFC_capex"), "value"] = float("nan")
    elif change == "missing_power":
        manual = manual.loc[~manual.parameter.eq("DCFC_rated_power")]
    elif change == "share_sum":
        manual.loc[manual.parameter.eq("L1_capex_aggregation"), "value"] = .4
    with pytest.raises(ValueError, match=match):
        _source_components_for_year(manual, category="ldv", source_year=2030, converter=converter())


@pytest.mark.parametrize("stale_count_weighting", [False, True])
def test_source_reconstruction_reconciles_built_costs_and_rejects_old_weighting(
    tmp_path, monkeypatch, stale_count_weighting,
):
    manual = component_manual()
    manual_path = tmp_path / "charger_parameters.csv"
    manual.to_csv(manual_path, index=False)
    source_rules = {
        "manual_file": manual_path.name,
        "technology": {"ldv": {"existing": "LDV_EX", "new": "LDV_N"}},
        "utilization": {"ldv": {"source_period": {"existing": "all", "new": "all"}}},
    }
    monkeypatch.setattr(layer, "load_harmonization_rules", lambda bundle, key: source_rules if key == "ev_chargers"
                        else {"source_column": "source", "notes_column": "notes"})
    monkeypatch.setattr(layer, "validate_manual_registry", lambda *args, **kwargs: (None, {manual_path.name: manual}))
    monkeypatch.setattr(layer, "resolve_input_path", lambda *args: manual_path)
    expected = (50 * 137.5 + 40 * 9000 + 10 * 100000) / 1410
    investment = 568.75 if stale_count_weighting else expected
    backend = {
        "cost_invest": pd.DataFrame([{"tech": "LDV_N", "vintage": year, "cost": investment}
                                     for year in (2025, 2030)]),
        "cost_fixed": pd.DataFrame([{"tech": tech, "vintage": year, "cost": expected * .01}
                                    for tech, year in (("LDV_EX", 2020), ("LDV_N", 2025), ("LDV_N", 2030))]),
    }
    bundle = SimpleNamespace(scenario=SimpleNamespace(periods=periods("prospective")))
    if stale_count_weighting:
        with pytest.raises(ValueError, match="differs from current reviewed source calculation"):
            _source_cost_components(bundle, backend, converter(), rules())
    else:
        result, utilization, identity, ratio = _source_cost_components(bundle, backend, converter(), rules())
        assert len(result) == 9
        assert result.groupby("backend_vintage").weighted_cost_per_gw.sum().to_list() == pytest.approx([expected] * 3)
        assert utilization == {"LDV_EX": .2, "LDV_N": .2}
        assert identity["sha256"] == file_sha256(manual_path)
        assert ratio == .01


def frames():
    reference = {
        "cost_invest": pd.DataFrame([
            ["ON", "BEV_CHRG", 2025, 10.0], ["ON", "PHEV_CHRG", 2025, 10.0],
            ["ON", "BEV_CHRG", 2030, 8.0],
        ], columns=["region", "tech", "vintage", "cost"]),
        "cost_fixed": pd.DataFrame([
            ["ON", "BEV_CHRG", 2020, 2025, .12], ["ON", "BEV_CHRG", 2025, 2025, .10],
        ], columns=["region", "tech", "vintage", "period", "cost"]),
        "cost_variable": pd.DataFrame(columns=["region", "tech", "vintage", "period", "cost"]),
    }
    backend = {
        "cost_invest": pd.DataFrame([
            ["ON", "LDV_N", 2025, 100.0, "$M 2020CAD / GW"],
            ["ON", "LDV_N", 2030, 80.0, "$M 2020CAD / GW"],
        ], columns=["region", "tech", "vintage", "cost", "units"]),
        "cost_fixed": pd.DataFrame([
            ["ON", "LDV_EX", 2023, 2025, 1.2, "$M 2020CAD / GW"],
            ["ON", "LDV_N", 2025, 2025, 1.0, "$M 2020CAD / GW"],
        ], columns=["region", "tech", "vintage", "period", "cost", "units"]),
        "cost_variable": pd.DataFrame(columns=["region", "tech", "vintage", "period", "cost"]),
        "capacity_to_activity": pd.DataFrame([
            ["ON", "LDV_EX", 31.536], ["ON", "LDV_N", 31.536],
        ], columns=["region", "tech", "c2a"]),
    }
    return reference, backend


def compare(reference=None, backend=None, **kwargs):
    default_ref, default_backend = frames()
    return compare_cost_frames(reference if reference is not None else default_ref,
                               backend if backend is not None else default_backend,
                               converter=converter(), utilization={"LDV_EX": .15, "LDV_N": .2},
                               **{"rules": rules(), "periods": periods(), **kwargs})


def test_unknown_units_and_absent_variable_entries_stay_unresolved():
    comparison, coverage = compare()
    assert set(comparison.basis_status) == {"unresolved"}
    assert comparison.backend_minus_reference.isna().all()
    assert comparison.reference_million_target_cad.isna().all()
    assert set(coverage.status) == {"absent"}
    assert coverage.explicit_zero_rows.sum() == 0
    assert comparison.loc[comparison.reference_vintage.eq(2020), "scope_status"].item() == "historical_rate_bridge"


def test_partial_currency_confirmation_does_not_infer_denominator():
    comparison, _ = compare(rules=rules(reference_basis=basis(None, currency="USD", dollar_year=2018)))
    assert set(comparison.basis_status) == {"unresolved"}
    assert comparison.backend_minus_reference.isna().all()
    assert comparison.reference_million_target_cad.iloc[0] == pytest.approx(13.75)
    assert comparison.backend_million_target_cad.isna().all()


@pytest.mark.parametrize("denominator,factor", [("GW", 1), ("PJ/year_capacity", 31.536),
                                                ("PJ/year_at_backend_utilization", 31.536 * .2)])
def test_confirmed_basis_uses_explicit_denominator_without_adding_constituent_rates(denominator, factor):
    comparison, _ = compare(rules=rules(reference_basis=basis(denominator)))
    rows = comparison.loc[comparison.table.eq("cost_invest") & comparison.reference_vintage.eq(2025)]
    assert len(rows) == 2
    assert rows.backend_million_target_cad.tolist() == pytest.approx([100 / factor] * 2)
    assert rows.backend_minus_reference.tolist() == pytest.approx([100 / factor - 10] * 2)
    assert set(rows.basis_status) == {"confirmed"}


def test_conditional_interpretation_is_not_a_confirmed_finding():
    comparison, _ = compare(rules=rules(conditional_bases=[basis()]))
    assert set(comparison.basis_status) == {"conditional", "unresolved"}
    assert comparison.loc[comparison.basis_status.eq("unresolved"), "relative_percent"].isna().all()
    assert comparison.loc[comparison.basis_status.eq("conditional"), "relative_percent"].notna().all()


def test_prospective_costs_match_source_year_instead_of_same_vintage_label():
    comparison, _ = compare(rules=rules(reference_basis=basis()), periods=periods("prospective"))
    row = comparison.loc[comparison.table.eq("cost_invest") & comparison.reference_vintage.eq(2030)].iloc[0]
    assert row.backend_vintage == 2025
    assert row.backend_cost_source_year == 2030
    assert row.backend_cost_per_gw == 100
    first = comparison.loc[comparison.table.eq("cost_invest") & comparison.reference_vintage.eq(2025)]
    assert set(first.scope_status) == {"backend_key_absent"}
    assert first.backend_minus_reference.isna().all()


def test_fixed_ratio_does_not_require_money_basis_and_handles_missing_or_zero_capex():
    reference, _ = frames()
    result = fixed_cost_ratios(reference)
    assert result.loc[result.vintage.eq(2025), "ratio"].item() == .01
    assert result.loc[result.vintage.eq(2020), "status"].item() == "investment_absent"
    reference["cost_invest"].loc[reference["cost_invest"].vintage.eq(2025), "cost"] = 0
    result = fixed_cost_ratios(reference)
    assert result.loc[result.vintage.eq(2025), "status"].item() == "zero_investment_denominator"
    assert result.ratio.isna().all()


def test_variable_zero_entry_is_distinct_from_absence():
    reference, backend = frames()
    reference["cost_variable"] = pd.DataFrame([["ON", "BEV_CHRG", 2025, 2025, 0]],
                                             columns=reference["cost_variable"].columns)
    _, coverage = compare(reference, backend)
    assert coverage.loc[coverage.side.eq("reference"), "explicit_zero_rows"].item() == 1
    assert coverage.loc[coverage.side.eq("reference"), "status"].item() == "present"
    assert coverage.loc[coverage.side.eq("backend"), "status"].item() == "absent"


def test_reference_reader_preserves_raw_values_and_hashes_without_writing_inputs(tmp_path):
    reference, _ = frames()
    config = rules()
    for table, frame in reference.items():
        frame.to_csv(tmp_path / config.files[table], index=False)
    hashes = {path.name: file_sha256(path) for path in tmp_path.iterdir()}
    selected, evidence = read_reference_costs(tmp_path, config)
    assert selected["cost_invest"].original_cost_text.iloc[0] == "10.0"
    assert selected["cost_invest"].reference_csv_row.tolist() == [2, 3, 4]
    assert evidence["cost_variable"]["selected_rows"] == 0
    assert hashes == {path.name: file_sha256(path) for path in tmp_path.iterdir()}
    reference["cost_invest"] = pd.concat([reference["cost_invest"], reference["cost_invest"].iloc[:1]])
    reference["cost_invest"].to_csv(tmp_path / config.files["cost_invest"], index=False)
    with pytest.raises(ValueError, match="duplicate"):
        read_reference_costs(tmp_path, config)


def test_invalid_scope_and_unresolved_sensitivity_fail_at_config_boundary():
    with pytest.raises(ValidationError, match="denominator"):
        rules(conditional_bases=[basis(None)])
    with pytest.raises(ValidationError, match="disjoint"):
        rules(groups={"ldv": {"reference_technologies": ["A", "A"], "existing": "E", "new": "N"}})
    with pytest.raises(ValidationError):
        ReferenceCostBasis.model_validate(basis("port"))


def test_backend_units_and_missing_physical_conversion_are_errors():
    reference, backend = frames()
    backend["cost_invest"].loc[:, "units"] = "unknown"
    with pytest.raises(ValueError, match="units"):
        compare(reference, backend)
    _, backend = frames()
    backend["capacity_to_activity"] = backend["capacity_to_activity"].iloc[:0]
    with pytest.raises(ValueError, match="capacity-to-activity"):
        compare(reference, backend)


def test_backend_cost_basis_reads_built_rates_and_distinguishes_existing_proxy(tmp_path):
    database = tmp_path / "built.sqlite"
    config = rules(groups={
        "ldv": {"reference_technologies": ["BEV_CHRG"], "existing": "LDV_EX", "new": "LDV_N"},
        "mhdv": {"reference_technologies": ["HDV_CHRG"], "existing": "MHDV_EX", "new": "MHDV_N"},
    })
    with sqlite3.connect(database) as connection:
        connection.executescript("""
            CREATE TABLE cost_invest(region,tech,vintage,cost,units);
            CREATE TABLE cost_fixed(region,tech,vintage,period,cost,units);
            CREATE TABLE cost_variable(region,tech,vintage,period,cost,units);
            CREATE TABLE capacity_to_activity(region,tech,c2a);
            CREATE TABLE efficiency(tech);
            CREATE TABLE lifetime_tech(tech);
            CREATE TABLE limit_annual_capacity_factor(tech_or_group);
            CREATE TABLE existing_capacity(tech);
            INSERT INTO cost_invest VALUES('ON','LDV_N',2025,100,'$M 2020CAD / GW');
            INSERT INTO cost_invest VALUES('ON','LDV_N',2030,80,'$M 2020CAD / GW');
            INSERT INTO cost_invest VALUES('ON','MHDV_N',2025,120,'$M 2020CAD / GW');
            INSERT INTO cost_fixed VALUES('ON','LDV_EX',2020,2025,1.2,'$M 2020CAD / GW');
            INSERT INTO cost_fixed VALUES('ON','LDV_EX',2020,2030,1.2,'$M 2020CAD / GW');
            INSERT INTO cost_fixed VALUES('ON','MHDV_EX',2020,2025,1.5,'$M 2020CAD / GW');
            INSERT INTO capacity_to_activity VALUES('ON','LDV_EX',30),('ON','LDV_N',30);
            INSERT INTO capacity_to_activity VALUES('ON','MHDV_EX',24),('ON','MHDV_N',24);
        """)
    original = file_sha256(database)
    result = backend_cost_basis(
        _read_backend(database, config), rules=config, periods=periods("prospective"),
        utilization={"LDV_EX": .15, "LDV_N": .2, "MHDV_EX": .3, "MHDV_N": .3},
        fixed_to_capex_ratio=.01, target_year=2020,
    )
    assert len(result) == 5
    assert not result.duplicated(["region", "tech", "vintage"]).any()
    existing = result.loc[result.tech.eq("LDV_EX")].iloc[0]
    assert (existing.vintage, existing.source_year) == (2020, 2025)
    assert existing.source_year_status == "existing_first_model_year_proxy"
    assert existing.status == "existing_implied_capex_from_fixed_cost"
    assert existing.cost_mcad_per_gw == pytest.approx(120)
    assert existing.throughput_pj_year_per_gw == pytest.approx(4.5)
    assert existing.cost_mcad_per_pj_year == pytest.approx(26.666666666666668)
    new = result.loc[result.tech.eq("LDV_N") & result.vintage.eq(2025)].iloc[0]
    assert new.source_year == 2030
    assert new.source_year_status == "new_projection_year"
    assert new.status == "new_investment_rate"
    assert new.cost_mcad_per_gw == 100
    assert new.cost_mcad_per_pj_year == pytest.approx(16.666666666666668)
    assert result.loc[result.tech.eq("LDV_N") & result.vintage.eq(2030), "source_year"].item() == 2035
    medium_heavy = result.loc[result.tech.eq("MHDV_EX")].iloc[0]
    assert medium_heavy.cost_mcad_per_gw == 150
    assert medium_heavy.throughput_pj_year_per_gw == pytest.approx(7.2)
    assert medium_heavy.cost_mcad_per_pj_year == pytest.approx(20.833333333333332)
    assert set(result.aggregation_basis) == {"total_expenditure_per_rated_power"}
    assert file_sha256(database) == original


@pytest.mark.parametrize("change,match", [
    ("fixed_period_conflict", "varies across periods"),
    ("conversion_missing", "capacity-to-activity"),
    ("conversion_duplicate", "capacity-to-activity"),
    ("utilization_zero", "utilization target"),
    ("units_unknown", "units differ"),
])
def test_backend_cost_basis_rejects_ambiguous_or_invalid_denominators(change, match):
    _, backend = frames()
    utilization = {"LDV_EX": .15, "LDV_N": .2}
    if change == "fixed_period_conflict":
        extra = backend["cost_fixed"].iloc[:1].copy()
        extra["period"], extra["cost"] = 2030, 1.3
        backend["cost_fixed"] = pd.concat([backend["cost_fixed"], extra], ignore_index=True)
    elif change == "conversion_missing":
        backend["capacity_to_activity"] = backend["capacity_to_activity"].iloc[:0]
    elif change == "conversion_duplicate":
        backend["capacity_to_activity"] = pd.concat([backend["capacity_to_activity"]] * 2)
    elif change == "utilization_zero":
        utilization["LDV_EX"] = 0
    elif change == "units_unknown":
        backend["cost_invest"]["units"] = "unknown"
    with pytest.raises(ValueError, match=match):
        backend_cost_basis(backend, rules=rules(), periods=periods(), utilization=utilization,
                           fixed_to_capex_ratio=.01, target_year=2020)


def test_zero_fixed_ratio_keeps_existing_capital_cost_unavailable():
    _, backend = frames()
    backend["cost_fixed"]["cost"] = 0
    result = backend_cost_basis(
        backend, rules=rules(), periods=periods(), utilization={"LDV_EX": .15, "LDV_N": .2},
        fixed_to_capex_ratio=0, target_year=2020,
    )
    existing = result.loc[result.tech.eq("LDV_EX")].iloc[0]
    assert existing.status == "existing_capex_unavailable_zero_fixed_ratio"
    assert pd.isna(existing.cost_mcad_per_gw)
    assert pd.isna(existing.cost_mcad_per_pj_year)
    assert result.loc[result.tech.eq("LDV_N"), "cost_mcad_per_gw"].tolist() == [100, 80]


def test_build_freshness_accepts_relocated_database_but_rejects_content_and_input_drift(tmp_path):
    files = {}
    for name in ("scenario", "paths", "sources", "parameters", "manual_inputs", "database"):
        path = tmp_path / name
        path.write_text(name, encoding="utf-8")
        files[name] = path
    bundle = SimpleNamespace(repo_root=tmp_path, scenario_path=files["scenario"], paths_path=files["paths"],
                             sources_path=files["sources"], scenario=SimpleNamespace(periods=periods(),
                             scenario=SimpleNamespace(name="test")))
    config = {name: {"path": str(path), "sha256": file_sha256(path)} for name, path in files.items()}
    for name in ("parameters", "manual_inputs"):
        config[name] = [config[name]]
    report = {"ok": True, "scenario": "test", "database": "old/location.sqlite",
              "database_sha256": file_sha256(files["database"]), "periods": periods().model_dump(mode="json"),
              "config": config}
    verify_build_snapshot(bundle, files["database"], report)
    files["parameters"].write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="input changed"):
        verify_build_snapshot(bundle, files["database"], report)
    files["database"].write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        verify_build_snapshot(bundle, files["database"], report)


def test_report_manifest_identifies_regenerable_tables(tmp_path):
    result = ChargingCostComparison({"comparisons": pd.DataFrame({"cost": [1.0]})}, {"status": "unresolved"}, tmp_path)
    assert write_report(result) == tmp_path
    manifest = json.loads((tmp_path / "snapshot.json").read_text())
    assert manifest["exports"]["comparisons.csv"] == {
        "rows": 1, "sha256": file_sha256(tmp_path / "comparisons.csv"),
    }


def test_reference_context_inherits_non_cost_parameters_without_overwriting_costs(tmp_path):
    baseline = tmp_path / "baseline.sqlite"
    with sqlite3.connect(baseline) as connection:
        connection.executescript("""
            CREATE TABLE CapacityToActivity(region,tech,c2a,notes);
            CREATE TABLE Efficiency(region,tech,vintage,efficiency);
            CREATE TABLE CostInvest(region,tech,vintage,cost);
            INSERT INTO CapacityToActivity VALUES('ON','BEV_CHRG',31.536,'rated power');
            INSERT INTO Efficiency VALUES('ON','BEV_CHRG',2025,.9);
            INSERT INTO CostInvest VALUES('ON','BEV_CHRG',2025,999);
        """)
    original = file_sha256(baseline)
    inherited = inherited_reference_context(baseline, rules())
    assert set(inherited.parameter) == {"capacity_to_activity", "efficiency"}
    assert set(inherited.evidence_origin) == {"inherited_original_legacy_baseline"}
    assert inherited.loc[inherited.parameter.eq("efficiency"), "value"].item() == .9
    assert file_sha256(baseline) == original
