"""Fresh raw-cohort stock handoff and consumption-first period aggregation."""

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization import build_efficiencies as layer
from utils import file_sha256, load_config_bundle, load_harmonization_rules
from validation.provenance import resolve_provenance


@pytest.fixture
def handoff(tmp_path, monkeypatch):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    rules = load_harmonization_rules(bundle, "efficiencies")
    path = tmp_path / "annual_stock.csv"
    stock = pd.DataFrame([
        {"region": "ON", "road_class": "cars", "tech": "T_LDV_C_BEV_EX",
         "vintage": 2020, "vintage_year": year, "raw_capacity": raw, "capacity": count}
        for year, raw, count in [(2021, 8, 0), (2022, 3, 4), (2023, 1, 6)]
    ])
    stock.to_csv(path, index=False)
    monkeypatch.setattr(layer, "resolve_artifact_path", lambda *_args: path)
    context = resolve_provenance(
        bundle.sources, source_key="statcan_transport_tables", component_key="20-10-0021-01",
        transformation="synthetic annual stock fixture", transformation_version="1",
    )
    inputs = {
        "rules": rules,
        "relationships": pd.DataFrame([{"mode": "cars", "tech": "T_LDV_C_BEV_EX"}]),
        "rows": [SimpleNamespace(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020,
                                 capacity=10, data_id=context.data_id)],
        "contexts": [context],
        "audit": {"period_mapping": bundle.scenario.periods.audit(),
                  "annual_stock_evidence": {"path": str(path), "sha256": file_sha256(path)}},
    }
    return bundle, path, stock, inputs


def test_fresh_annual_stock_reconciles_capacity_but_retains_raw_cohort_weights(handoff):
    bundle, path, stock, inputs = handoff
    actual_path, selected, components = layer.historical_stock_evidence(bundle, **inputs)
    assert actual_path == path
    pd.testing.assert_frame_equal(selected, stock)
    assert selected.capacity.sum() == inputs["rows"][0].capacity
    assert selected.raw_capacity.tolist() == [8, 3, 1]
    assert selected.loc[selected.capacity.eq(0), "raw_capacity"].sum() == 8
    assert selected.raw_capacity.sum() != inputs["rows"][0].capacity
    assert components == {("ON", "T_LDV_C_BEV_EX", 2020): (("statcan_transport_tables", "20-10-0021-01"),)}


@pytest.mark.parametrize("problem,expected", [
    ("path", "configured capacity evidence"),
    ("periods", "period mapping"),
    ("hash", "changed after capacity preparation"),
    ("capacity", "reconcile to prepared capacity"),
    ("context", "Missing capacity provenance"),
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
