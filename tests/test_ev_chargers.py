"""Focused offline checks for the reviewed EV charger parameter family."""

import math
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError
from canoe_schema.v4_0 import LimitAnnualCapacityFactor

from parameterization.ev_chargers import _validate_shares, prepare_ev_charger_rows
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from utils import load_harmonization_rules, resolve_artifact_path
from validation.config_models import ScenarioEvChargers


SCENARIO = "config/scenarios/legacy_reproduction.yaml"
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


def test_charger_costs_utilization_and_efficiency(prepared) -> None:
    bundle, result = prepared
    assert len(result.invest_rows) == 100
    assert all(row.tech.endswith("_N") for row in result.invest_rows)
    assert result.audit["existing_investment_rows_excluded"] == 18
    assert len(result.fixed_rows) == 390
    assert len(result.efficiency_rows) == 118
    assert len(result.utilization) == 190
    assert "period" not in LimitAnnualCapacityFactor.model_fields
    assert result.audit["utilization_schema_inserted"] is False
    assert set(result.utilization.operator) == {"≤"}
    assert set(result.utilization.units) == {"fraction"}
    assert set(result.utilization.loc[result.utilization.period.eq(2025) & result.utilization.tech.str.contains("LDV"), "factor"]) == {0.15}
    assert set(result.utilization.loc[result.utilization.period.eq(2030) & result.utilization.tech.str.contains("LDV"), "factor"]) == {0.2}
    assert set(result.utilization.loc[result.utilization.tech.str.contains("MHDV"), "factor"]) == {0.3}
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
    assert set(result.utilization.period) == set(bundle.scenario.periods.model)
    assert set(row.efficiency for row in result.efficiency_rows if "LDV" in row.tech) == {0.95}
    assert set(row.efficiency for row in result.efficiency_rows if "MHDV" in row.tech) == {0.8}
    assert all(row.units == "PJ/PJ" for row in result.efficiency_rows)
    assert not result.utilization.duplicated(["region", "period", "tech", "operator"]).any()
    assert all(math.isfinite(row.cost) and row.cost > 0 for row in result.invest_rows)
    output = resolve_artifact_path(bundle, "ev_chargers_processed")
    rules = load_harmonization_rules(bundle, "ev_chargers")
    assert all((output / rules[key]).is_file() for key in (
        "capacity_output_file", "invest_output_file", "fixed_output_file",
        "efficiency_output_file", "utilization_output_file", "port_audit_file",
    ))


def test_charger_share_contract_rejects_mismatch() -> None:
    manual = pd.read_csv(ROOT / "inputs/0_manual_params/charger_parameters.csv", dtype=str, keep_default_na=False)
    selected = manual.parameter.eq("existing_private_L2_share")
    manual.loc[selected, "value"] = "0.8"
    with pytest.raises(ValueError, match="shares do not sum to one"):
        _validate_shares(manual)


def test_ev_to_port_ratios_are_positive_scenario_values() -> None:
    with pytest.raises(ValidationError):
        ScenarioEvChargers(ld_evs_per_port=0, mhd_evs_per_port=1.5)
