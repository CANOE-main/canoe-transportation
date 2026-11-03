"""Read-only charging comparisons must not manufacture units, zeros or time matches."""

import json
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from parameterization.currency import CerCurrencyConverter
from utils import file_sha256
from validation.charging_cost_comparison import (
    ChargingComparisonRules, ChargingCostComparison, ReferenceCostBasis,
    compare_cost_frames, fixed_cost_ratios, read_reference_costs,
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
