"""Operating costs cover complete process lives and retain final source formulas."""

from dataclasses import replace
from math import exp
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization import build_costs as layer
from parameterization.road_efficiencies import interpolate
from utils import load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.provenance import resolve_provenance


ROOT = Path(__file__).resolve().parents[1]
TECHNOLOGIES = {"T_LDV_C_GSL_N", "T_LDV_C_GSL_EX", "T_MDV_T_DSL_N"}


def test_cost_tail_holds_final_value_without_historical_backfill():
    frame = pd.DataFrame({"source_year": [2025, 2050], "cost": [3.0, 2.0]})
    year = layer._cost_trajectory_year(
        frame, 2070, year_col="source_year", endpoint_policy="hold_last",
    )
    assert year == 2050
    assert interpolate(frame, year, "cost", year_col="source_year") == 2.0
    earlier = layer._cost_trajectory_year(
        frame, 2020, year_col="source_year", endpoint_policy="hold_last",
    )
    with pytest.raises(ValueError, match="No extrapolation"):
        interpolate(frame, earlier, "cost", year_col="source_year")
    with pytest.raises(ValueError, match="endpoint policy"):
        layer._cost_trajectory_year(frame, 2070, year_col="source_year", endpoint_policy="linear")


def test_bean_cost_uses_native_linear_formula_throughout_vehicle_life():
    coefficients = pd.read_csv(
        ROOT / "inputs/1_interim/fetched_nlr_atb_autonomie/anl_bean_mhdv_maintenance_coefficients.csv",
    )
    for age in (29, 30, 75):
        result = layer.bean_maintenance_per_mile(
            coefficients, vehicle_class="BoxMedium 4", powertrain="Conv", age=age,
        )
        assert result["source_cost_per_mile"] == pytest.approx(0.09 * age + 0.2618)


@pytest.mark.parametrize("changes", [
    {"trajectory_endpoint_policy": "linear"},
    {"bean_maintenance_age_policy": True},
    {"bean_maintenance_age_policy": "hold_last"},
    {"bean_maintenance_age_policy": None},
])
def test_invalid_cost_endpoint_controls_fail_before_source_io(monkeypatch, changes):
    bundle = SimpleNamespace(scenario=SimpleNamespace(lifetimes=SimpleNamespace(survival_curves=False)))
    rules = {"trajectory_endpoint_policy": "hold_last", "bean_maintenance_age_policy": "linear_throughout_life", **changes}
    monkeypatch.setattr(layer, "load_harmonization_rules", lambda *_: rules)
    monkeypatch.setattr(layer, "existing_range_weights", lambda *_: pytest.fail("Invalid controls reached evidence I/O"))
    with pytest.raises(ValueError, match="endpoint policy|age policy"):
        layer.prepare_cost_rows(bundle)


def test_supplied_lifetimes_require_inspectable_provenance_before_source_io():
    bundle = SimpleNamespace(scenario=SimpleNamespace(lifetimes=SimpleNamespace(survival_curves=False)))
    with pytest.raises(ValueError, match="matching lifetime provenance contexts"):
        layer.prepare_cost_rows(bundle, fixed_lifetime_rows=[], survival_curve_rows=[])


@pytest.fixture(params=[False, True], ids=["fixed_lives", "survival_curves"])
def lifetime_costs(request, monkeypatch, tmp_path):
    """Use registered cost evidence with private output routes and explicit test lives."""
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT)
    payload = bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON"]
    payload["periods"].update(model=list(range(2025, 2076, 5)))
    payload["lifetimes"]["survival_curves"] = request.param
    bundle = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    original_read = layer._read

    def selected_read(path, paths):
        frame = original_read(path, paths)
        if path.name == "technology.csv":
            return frame.loc[frame.tech.isin(TECHNOLOGIES)].copy()
        return frame

    monkeypatch.setattr(layer, "_read", selected_read)
    monkeypatch.setattr(layer, "existing_range_weights", lambda _, *, evidence=None: (
        {}, SimpleNamespace(bucket_totals=pd.DataFrame()),
    ))
    monkeypatch.setattr(layer, "_region_weights", lambda *_: {
        ("ON", "cars"): {"Compact": 1.0},
        ("ON", "medium_trucks"): {"Class 2 Medium Van": 1.0},
    })

    def isolated_artifact_path(_, artifact, *parts):
        assert artifact in {"costs_interim", "costs_processed", "costs_validation"}
        return tmp_path / artifact / Path(*parts)

    monkeypatch.setattr(layer, "resolve_artifact_path", isolated_artifact_path)
    existing = [SimpleNamespace(region="ON", tech="T_LDV_C_GSL_EX", vintage=2020, capacity=1.0)]
    fixed = []
    curves = []
    life_contexts = {
        component: resolve_provenance(
            bundle.sources, source_key="epa_moves_heavy_truck_survival", component_key=component,
            transformation="synthetic_cost_lifetime_coverage_fixture", transformation_version="1",
        )
        for component in ("car_survival_rates", "heavy_truck_survival_rates")
    }
    lives = {"T_LDV_C_GSL_N": 16, "T_LDV_C_GSL_EX": 16, "T_MDV_T_DSL_N": 20}
    expected = set()
    for tech in sorted(TECHNOLOGIES):
        vintages = [2020] if tech.endswith("_EX") else bundle.scenario.periods.model
        lifetime = 56 if request.param else lives[tech]
        context = life_contexts["car_survival_rates" if tech.startswith("T_LDV_C") else "heavy_truck_survival_rates"]
        if not request.param:
            fixed.append(SimpleNamespace(region="ON", tech=tech, lifetime=lifetime, data_id=context.data_id))
        for vintage in vintages:
            if request.param:
                curves.extend(SimpleNamespace(
                    region="ON", tech=tech, vintage=vintage, period=year,
                    fraction=(vintage + lifetime - year) / lifetime, data_id=context.data_id,
                ) for year in range(vintage, vintage + lifetime + 1))
            expected.update(("ON", period, tech, vintage)
                            for period in bundle.scenario.periods.model
                            if vintage <= period < vintage + lifetime)
    kwargs = {"existing_capacity_rows": existing, "fixed_lifetime_rows": fixed,
              "survival_curve_rows": curves, "lifetime_provenance_contexts": list(life_contexts.values())}
    prepared = layer.prepare_cost_rows(bundle, **kwargs)
    rules = load_harmonization_rules(bundle, "costs")
    audit_rows = pd.read_csv(tmp_path / "costs_interim" / rules["files"]["road_variable"])
    return bundle, prepared, expected, audit_rows, kwargs


def test_every_operating_period_has_costs_and_expired_pairs_have_none(lifetime_costs):
    _, prepared, expected, _, _ = lifetime_costs
    actual = {(row.region, row.period, row.tech, row.vintage) for row in prepared.variable_rows}
    assert actual == expected
    assert len(actual) == len(prepared.variable_rows)
    assert all(row.cost > 0 and row.data_id and row.data_source for row in prepared.variable_rows)


def test_old_ldv_costs_hold_age_fourteen_through_retirement(lifetime_costs):
    bundle, prepared, _, evidence, _ = lifetime_costs
    rows = evidence.loc[evidence.tech.eq("T_LDV_C_GSL_N") & evidence.vintage.eq(2025)]
    rows = rows.loc[rows.age.ge(14)]
    assert not rows.empty
    assert rows.cost.nunique() == 1
    assert prepared.audit["trajectory_endpoint_policy"] == "hold_last"

    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    directory = resolve_input_path(bundle, "interim", atb["interim_subdir"])
    components = atb["components"]
    prices = pd.read_csv(directory / components["vehicles"]["output_file"])
    gasoline_detail = load_harmonization_rules(bundle, "efficiencies")["ldv_archetypes"]["gasoline"]["detail"]
    source_year = rows.purchase_evidence_year.iloc[0]
    price = prices.loc[
        prices.metric_key.eq("modeled_vehicle_price") & prices.trajectory.eq(bundle.scenario.costs.atb_trajectory)
        & prices.vehicle_class.eq("Compact") & prices.vehicle_detail.eq(gasoline_detail)
        & prices.year.eq(source_year), "value",
    ].item()
    repair_msrp = price * 105.381 / 117.973
    outputs = components["maintenance_ldv"]["outputs"]
    baseline = pd.read_csv(directory / outputs["baseline_repair_cost"])
    maintenance = pd.read_csv(directory / outputs["maintenance_cost"])
    age_coefficient = baseline.loc[baseline.year_index.eq(14), "baseline_repair_cost($/mi)"].item()
    scheduled = maintenance.loc[maintenance.vehicle_powertrain.eq("Gasoline"), "maintenance_cost($/mi)"].item()
    assert rows.weighted_repair_msrp.to_list() == pytest.approx([repair_msrp] * len(rows))
    expected = age_coefficient * exp(0.00002 * repair_msrp) + scheduled
    assert rows.source_value.to_list() == pytest.approx([expected] * len(rows))
    assert rows.source_dollar_year.eq(2020).all()
    assert rows.source_components.str.contains("bea_us_gdp_deflator/annual").all()


def test_new_vintages_beyond_atb_horizon_hold_final_price_formula(lifetime_costs):
    _, prepared, _, _, _ = lifetime_costs
    rows = [row for row in prepared.invest_rows if row.tech == "T_LDV_C_GSL_N" and row.vintage >= 2050]
    assert len(rows) > 1
    assert len({row.cost for row in rows}) == 1
    holds = prepared.audit["trajectory_endpoint_holds"]
    assert holds and any(row["requested_year"] > 2050 and row["source_endpoint_year"] == 2050 for row in holds)


def test_curve_survivors_keep_increasing_linear_bean_cost_throughout_life(lifetime_costs):
    bundle, prepared, _, evidence, _ = lifetime_costs
    if not bundle.scenario.lifetimes.survival_curves:
        return
    rows = evidence.loc[evidence.tech.eq("T_MDV_T_DSL_N") & evidence.vintage.eq(2025) & evidence.age.gt(29)]
    assert not rows.empty
    rows = rows.sort_values("age")
    assert rows.maintenance_formula_age.equals(rows.age)
    assert not rows.maintenance_age_endpoint_held.any()
    assert rows.source_value.to_list() == pytest.approx((0.09 * rows.age + 0.2618).to_list())
    assert rows.cost.gt(0).all() and rows.cost.diff().dropna().gt(0).all()
    assert prepared.audit["maintenance_age_policies"]["bean"] == "linear_throughout_life"


def test_variable_cost_registry_contains_used_lifetime_sources_and_keeps_cost_dq(lifetime_costs):
    bundle, prepared, _, evidence, _ = lifetime_costs
    contexts = {context.data_id: context for context in prepared.provenance_contexts}
    for row in prepared.variable_rows:
        context = contexts[row.data_id]
        lifetime = next(source for source in context.contributors if source.source_key == "epa_moves_heavy_truck_survival")
        expected_component = "car_survival_rates" if row.tech.startswith("T_LDV_C") else "heavy_truck_survival_rates"
        assert {component.component_key for component in lifetime.components} == {expected_component}
        assert any(getattr(item, "source_id", None) == lifetime.source_id for item in context.registry_rows())
        source, component = (layer.ATB, "maintenance_ldv") if row.tech.startswith("T_LDV_C") else (layer.BEAN, "mhdv_maintenance_coefficients")
        native = resolve_provenance(
            bundle.sources, source_key=source, component_key=component,
            transformation="expected_cost_quality_fixture", transformation_version="1",
        )
        assert row.data_source == native.source_id
        assert all(getattr(row, field) == value for field, value in native.data_quality.row_fields().items())
    for row in prepared.invest_rows:
        assert "epa_moves_heavy_truck_survival" not in {source.source_key for source in contexts[row.data_id].contributors}
    assert evidence.lifetime_data_ids.ne("[]").all()
    assert evidence.source_components.str.contains("epa_moves_heavy_truck_survival").all()


def test_missing_lifetime_context_cannot_silently_remove_source_lineage(lifetime_costs):
    bundle, _, _, _, kwargs = lifetime_costs
    with pytest.raises(ValueError, match="lifetime rows lack matching provenance contexts"):
        layer.prepare_cost_rows(bundle, **{**kwargs, "lifetime_provenance_contexts": []})
