"""Fresh raw-cohort stock handoff and consumption-first period aggregation."""

from copy import deepcopy
from dataclasses import replace

from canoe_schema.v4_0 import ExistingCapacity
import pandas as pd
import pytest

from parameterization import build_efficiencies as layer
from parameterization.build_existing_capacity import publish_capacity_handoff
from utils import file_sha256, load_config_bundle, load_harmonization_rules, resolve_artifact_path
from validation.provenance import resolve_provenance


@pytest.fixture
def handoff(tmp_path):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={
        "lifetimes": bundle.scenario.lifetimes.model_copy(update={"survival_curves": False}),
    }))
    routes = dict(bundle.paths.artifacts)
    for family in ("existing_capacity_interim", "existing_capacity_validation"):
        routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}))
    rules = load_harmonization_rules(bundle, "efficiencies")
    stock_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
    path = resolve_artifact_path(bundle, "existing_capacity_interim", stock_rules["annual_stock_file"])
    path.parent.mkdir(parents=True)
    stock = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "tech": "T_LDV_C_BEV_EX",
         "vintage": 2020, "vintage_year": year, "raw_capacity": raw, "capacity": count}
        for year, raw, count in [(2021, 8, 0), (2022, 3, 4), (2023, 1, 6)]
    ])
    stock.to_csv(path, index=False)
    context = resolve_provenance(
        bundle.sources, source_key="statcan_transport_tables", component_key="20-10-0021-01",
        transformation="synthetic annual stock fixture", transformation_version="1",
    )
    inputs = {
        "rules": rules,
        "relationships": pd.DataFrame([{"mode": "cars", "tech": "T_LDV_C_BEV_EX"}]),
        "rows": [ExistingCapacity(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020,
                                  capacity=10, units="k vehicles", **context.parameter_fields())],
        "contexts": [context],
        "audit": {"period_mapping": bundle.scenario.periods.audit(),
                  "annual_stock_evidence": {"path": str(path), "sha256": file_sha256(path)}},
    }
    publish_capacity_handoff(bundle, **{key: inputs[key] for key in ("rows", "contexts", "audit")},
                             artifact_paths=[path])
    return bundle, path, stock, inputs


def publish_stock_case(handoff, *, mode, tech, period_mode, vintage, records):
    """Publish a private, fully reconciled cohort family on either period grid."""
    bundle, path, _, original = handoff
    period_payload = bundle.scenario.periods.model_dump()
    if period_mode == "legacy":
        period_payload.update(
            period_mode="legacy", existing=[2000, 2005, 2010, 2015, 2020, 2025],
            model=[2030, 2035, 2040, 2045],
        )
    periods = type(bundle.scenario.periods).model_validate(period_payload)
    bundle = replace(bundle, scenario=bundle.scenario.model_copy(update={"periods": periods}))
    stock = pd.DataFrame([
        {"region": "ON", "road_class": mode, "tech": tech, "vintage": vintage,
         "vintage_year": year, "raw_capacity": raw, "capacity": count}
        for year, raw, count in records
    ])
    inputs = deepcopy(original)
    inputs["relationships"] = pd.DataFrame([{"mode": mode, "tech": tech}])
    inputs["rows"][0] = inputs["rows"][0].model_copy(update={
        "tech": tech, "vintage": vintage, "capacity": float(stock.capacity.sum()),
    })
    stock.to_csv(path, index=False)
    inputs["audit"] = {
        "period_mapping": periods.audit(),
        "annual_stock_evidence": {"path": str(path), "sha256": file_sha256(path)},
    }
    publish_capacity_handoff(
        bundle, **{key: inputs[key] for key in ("rows", "contexts", "audit")},
        artifact_paths=[path],
    )
    return bundle, path, stock, inputs


def test_fresh_annual_stock_reconciles_capacity_but_retains_raw_cohort_weights(handoff):
    bundle, path, stock, inputs = handoff
    actual_path, selected, components = layer.historical_stock_evidence(bundle, **inputs)
    assert actual_path == path
    # Fixed lifetimes use surviving stock as solver capacity; the consumer
    # makes that lineage explicit while preserving the raw weighting evidence.
    pd.testing.assert_frame_equal(selected, stock.assign(solver_capacity_k_vehicles=stock.capacity))
    assert selected.capacity.sum() == inputs["rows"][0].capacity
    assert selected.raw_capacity.tolist() == [8, 3, 1]
    assert selected.loc[selected.capacity.eq(0), "raw_capacity"].sum() == 8
    assert selected.raw_capacity.sum() != inputs["rows"][0].capacity
    assert components == {("ON", "T_LDV_C_BEV_EX", 2020): (("statcan_transport_tables", "20-10-0021-01"),)}


@pytest.mark.parametrize("problem", ["missing_motorcycles", "duplicate", "unknown"])
def test_configuration_cannot_silently_restore_road_class_weighting_exceptions(handoff, problem):
    bundle, _, _, inputs = handoff
    inputs = deepcopy(inputs)
    modes = inputs["rules"]["historical_stock_weighted_modes"]
    if problem == "missing_motorcycles":
        modes.remove("motorcycles")
    elif problem == "duplicate":
        modes.append("cars")
    else:
        modes.append("unregistered_class")
    with pytest.raises(ValueError, match="include every road class exactly once"):
        layer.historical_stock_evidence(bundle, **inputs)


@pytest.mark.parametrize("mode,tech", [
    ("cars", "T_LDV_C_BEV_EX"),
    ("motorcycles", "T_LDV_M_GSL_EX"),
    ("school_buses", "T_HDV_BS_DSL_EX"),
    ("urban_transit", "T_HDV_BT_DSL_EX"),
    ("inter_city_buses", "T_HDV_BIC_DSL_EX"),
])
@pytest.mark.parametrize("period_mode,years", [
    ("prospective", [2021, 2022, 2023]),
    ("legacy", [2016, 2017, 2018]),
])
def test_all_road_classes_use_raw_physical_cohorts_independently_of_lifetime_redistribution(
    handoff, mode, tech, period_mode, years,
):
    original_raw_weights = [8, 3, 1]
    efficiencies = []
    for redistributed_capacity in ([0, 4, 6], [5, 5, 0]):
        bundle, _, _, inputs = publish_stock_case(
            handoff, mode=mode, tech=tech, period_mode=period_mode, vintage=2020,
            records=list(zip(years, original_raw_weights, redistributed_capacity, strict=True)),
        )
        _, selected, components = layer.historical_stock_evidence(bundle, **inputs)
        assert selected.vintage_year.tolist() == years
        assert selected.capacity.tolist() == redistributed_capacity
        assert selected.raw_capacity.tolist() == original_raw_weights
        assert (selected.raw_capacity / selected.raw_capacity.sum()).tolist() == pytest.approx(
            [2 / 3, 1 / 4, 1 / 12],
        )
        assert ("ON", tech, 2020) in components
        result = layer.aggregate_consumption_period([
            {"native_intensity": consumption, "load_factor": load, "native_unit": "MJ/vkm"}
            for consumption, load in [(2, 1), (4, 2), (8, 3)]
        ], selected.raw_capacity.tolist())
        efficiencies.append(result["efficiency"])
    # Raw vehicle counts determine the representative consumption and load;
    # redistributing survivor capacity does not change the observed cohort mix.
    assert efficiencies == pytest.approx([17 / 36, 17 / 36])


@pytest.mark.parametrize("period_mode,supported_years", [
    ("prospective", [2000, 2001]),
    ("legacy", [1996, 2000]),
])
def test_old_bus_cohorts_reconcile_before_out_of_history_weights_are_excluded(
    handoff, period_mode, supported_years,
):
    records = [(1990, 90, 3), (supported_years[0], 8, 4), (supported_years[1], 2, 3)]
    bundle, _, original, inputs = publish_stock_case(
        handoff, mode="urban_transit", tech="T_HDV_BT_DSL_EX",
        period_mode=period_mode, vintage=2000, records=records,
    )
    _, selected, _ = layer.historical_stock_evidence(bundle, **inputs)
    assert original.capacity.sum() == inputs["rows"][0].capacity == 10
    # The old physical cohort remains necessary for capacity reconciliation,
    # but cannot become a synthetic observation at its clipped fuel-source year.
    assert selected.vintage_year.tolist() == supported_years
    assert selected.capacity.sum() == 7
    assert selected.raw_capacity.tolist() == [8, 2]
    assert (selected.raw_capacity / selected.raw_capacity.sum()).tolist() == pytest.approx([.8, .2])
    assert original.loc[original.vintage_year.eq(1990), "vintage_year"].tolist() == [1990]


@pytest.mark.parametrize("period_mode", ["prospective", "legacy"])
def test_bus_cohort_without_positive_observed_history_cannot_supply_weights(handoff, period_mode):
    bundle, _, _, inputs = publish_stock_case(
        handoff, mode="urban_transit", tech="T_HDV_BT_DSL_EX",
        period_mode=period_mode, vintage=2000, records=[(1990, 12, 10)],
    )
    with pytest.raises(ValueError, match="Missing positive raw age-distribution stock"):
        layer.historical_stock_evidence(bundle, **inputs)


@pytest.mark.parametrize("mode,tech", [
    ("motorcycles", "T_LDV_M_GSL_EX"),
    ("urban_transit", "T_HDV_BT_DSL_EX"),
])
@pytest.mark.parametrize("problem,message", [
    ("missing", "Missing or mismatched age-distribution support"),
    ("duplicate", "Duplicate annual age-distribution cohort"),
    ("path", "configured capacity evidence"),
    ("hash", "changed after capacity preparation"),
])
def test_bus_and_motorcycle_weights_require_complete_fresh_identifiable_support(
    handoff, mode, tech, problem, message,
):
    bundle, path, stock, inputs = publish_stock_case(
        handoff, mode=mode, tech=tech, period_mode="prospective", vintage=2020,
        records=[(2021, 8, 0), (2022, 3, 4), (2023, 1, 6)],
    )
    if problem == "path":
        inputs["audit"]["annual_stock_evidence"]["path"] = str(path.with_name("other_stock.csv"))
    elif problem == "hash":
        path.write_text(path.read_text() + "\n")
    else:
        stock = stock.iloc[0:0] if problem == "missing" else pd.concat([stock, stock.iloc[[0]]])
        stock.to_csv(path, index=False)
        inputs["audit"]["annual_stock_evidence"]["sha256"] = file_sha256(path)
        publish_capacity_handoff(
            bundle, **{key: inputs[key] for key in ("rows", "contexts", "audit")},
            artifact_paths=[path],
        )
    with pytest.raises(ValueError, match=message):
        layer.historical_stock_evidence(bundle, **inputs)


@pytest.mark.parametrize("problem,expected", [
    ("path", "configured capacity evidence"),
    ("periods", "period mapping"),
    ("hash", "changed after capacity preparation"),
    ("capacity", "scenario, rows or contexts"),
    ("context", "scenario, rows or contexts"),
])
def test_annual_stock_rejects_inconsistent_capacity_handoff(handoff, problem, expected):
    bundle, path, _, original = handoff
    inputs = deepcopy(original)
    if problem == "path":
        inputs["audit"]["annual_stock_evidence"]["path"] = str(path.with_name("another_stock.csv"))
    elif problem == "periods":
        inputs["audit"]["period_mapping"]["base_year"] = 2022
    elif problem == "hash":
        path.write_text(path.read_text() + "\n")
    elif problem == "capacity":
        inputs["rows"][0].capacity = 9
    else:
        inputs["contexts"] = []
    with pytest.raises(ValueError, match=expected):
        layer.historical_stock_evidence(bundle, **inputs)


@pytest.mark.parametrize("problem,expected", [
    ("negative", "negative stock"),
    ("nonfinite", "non-finite capacity"),
    ("negative_raw", "negative stock"),
    ("nonfinite_raw", "non-finite raw_capacity"),
    ("negative_solver", "negative stock"),
    ("nonfinite_solver", "non-finite solver_capacity_k_vehicles"),
    ("duplicate", "Duplicate annual"),
    ("year", "scenario vintage mapping"),
    ("zero", "reconcile to prepared capacity"),
    ("zero_raw", "Missing positive raw age-distribution stock"),
    ("missing_raw", "lacks required columns"),
])
def test_annual_stock_rejects_invalid_or_absent_weight_support(handoff, problem, expected):
    bundle, path, stock, inputs = handoff
    if problem == "negative":
        stock.loc[0, "capacity"] = -1
    elif problem == "nonfinite":
        stock["capacity"] = stock.capacity.astype(float)
        stock.loc[0, "capacity"] = float("inf")
    elif problem == "negative_raw":
        stock.loc[0, "raw_capacity"] = -1
    elif problem == "nonfinite_raw":
        stock["raw_capacity"] = stock.raw_capacity.astype(float)
        stock.loc[0, "raw_capacity"] = float("inf")
    elif problem in {"negative_solver", "nonfinite_solver"}:
        stock["solver_capacity_k_vehicles"] = stock.capacity.astype(float)
        stock.loc[0, "solver_capacity_k_vehicles"] = -1 if problem == "negative_solver" else float("inf")
    elif problem == "duplicate":
        stock = pd.concat([stock, stock.iloc[[0]]], ignore_index=True)
    elif problem == "year":
        stock.loc[0, "vintage_year"] = 2020
    elif problem == "zero_raw":
        stock["raw_capacity"] = 0
    elif problem == "missing_raw":
        stock = stock.drop(columns="raw_capacity")
    else:
        stock["capacity"] = 0
    stock.to_csv(path, index=False)
    inputs["audit"]["annual_stock_evidence"]["sha256"] = file_sha256(path)
    publish_capacity_handoff(bundle, **{key: inputs[key] for key in ("rows", "contexts", "audit")},
                             artifact_paths=[path])
    with pytest.raises(ValueError, match=expected):
        layer.historical_stock_evidence(bundle, **inputs)


def test_annual_stock_rejects_changes_during_read(handoff, monkeypatch):
    bundle, path, _, inputs = handoff
    original = pd.read_csv

    def changed_during_read(source):
        frame = original(source)
        path.write_text(path.read_text() + "\n")
        return frame

    monkeypatch.setattr(layer.pd, "read_csv", changed_during_read)
    with pytest.raises(ValueError, match="changed while being read"):
        layer.historical_stock_evidence(bundle, **inputs)


def test_efficiency_cannot_reuse_rows_without_fresh_capacity_audit_and_contexts():
    bundle = load_config_bundle("config/scenarios/default.yaml")
    with pytest.raises(ValueError, match="requires capacity rows, contexts and audit together"):
        layer.prepare_efficiency_rows(bundle, existing_capacity_rows=[])


def test_period_efficiency_uses_mean_consumption_and_same_weight_load_not_mean_efficiency():
    observations = [
        {"native_intensity": 2, "native_unit": "MJ/vkm", "load_factor": 1,
         "source_year": 2021, "efficiency": .5},
        {"native_intensity": 8, "native_unit": "MJ/vkm", "load_factor": 3,
         "source_year": 2023, "efficiency": .375},
    ]
    result = layer.aggregate_consumption_period(observations, [1, 3])
    assert result == pytest.approx({
        "native_intensity": 6.5, "load_factor": 2.5, "efficiency": 2.5 / 6.5,
        "weighted_source_year": 2022.5, "native_unit": "MJ/vkm",
    })
    assert result["efficiency"] != pytest.approx((.5 + 3 * .375) / 4)


def test_service_intensity_period_is_averaged_before_single_reciprocal():
    result = layer.aggregate_consumption_period([
        {"native_intensity": .5, "native_unit": "MJ/Tkm"},
        {"native_intensity": 2, "native_unit": "MJ/Tkm"},
    ], [.25, .75])
    assert result["native_intensity"] == 1.625
    assert result["load_factor"] == 1
    assert result["efficiency"] == pytest.approx(1 / 1.625)
    assert "weighted_source_year" not in result


def test_range_periods_keep_their_own_observed_support_before_sales_weighting():
    def point(consumption, load):
        return {"native_intensity": consumption, "native_unit": "MJ/vkm", "load_factor": load}

    combined = layer.combine_range_observations(
        {"short": {2021: point(2, 1)}, "long": {2023: point(8, 3)}},
        {"short": .75, "long": .25}, {2021: .1, 2022: .2, 2023: .7},
    )
    years = sorted(combined["observations"])
    result = layer.aggregate_consumption_period(
        [combined["observations"][year] for year in years],
        [combined["weights"][year] for year in years],
    )
    assert result["native_intensity"] == 3.5
    assert result["load_factor"] == 1.5
    assert result["efficiency"] == pytest.approx(1.5 / 3.5)
    assert combined["range_coverage"] == {"short": .1, "long": .7}
    assert combined["cohort_coverage"] == pytest.approx(.25)
    assert combined["weights"] == {2021: .75, 2023: .25}
    assert 2022 not in combined["observations"]


def test_range_periods_reject_missing_range_instead_of_dropping_its_sales_share():
    with pytest.raises(ValueError, match="Missing or out-of-period"):
        layer.combine_range_observations(
            {"short": {}, "long": {2023: {"native_intensity": 1, "native_unit": "MJ/vkm", "load_factor": 1}}},
            {"short": .5, "long": .5}, {2023: 1},
        )


@pytest.mark.parametrize("observations,weights,message", [
    ([], [], "nonempty"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm"}], [], "matching"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm"}], [0], "weights"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm"}], [-1], "weights"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm"}], [float("nan")], "weights"),
    ([{"native_intensity": 1, "native_unit": "MJ/vkm"}], [1], "explicit load"),
    ([{"native_intensity": 0, "native_unit": "MJ/Tkm"}], [1], "positive consumption"),
    ([{"native_intensity": float("inf"), "native_unit": "MJ/Tkm"}], [1], "positive consumption"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm", "load_factor": 2}], [1], "unit load"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm"},
      {"native_intensity": 1, "native_unit": "MJ/Pkm"}], [1, 1], "mix native units"),
    ([{"native_intensity": 1, "native_unit": "MJ/Tkm", "source_year": 2020},
      {"native_intensity": 1, "native_unit": "MJ/Tkm"}], [1, 1], "every observation"),
])
def test_consumption_period_rejects_invalid_support(observations, weights, message):
    with pytest.raises(ValueError, match=message):
        layer.aggregate_consumption_period(observations, weights)
