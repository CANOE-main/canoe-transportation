"""Attribution identities, stale-evidence guards and controlled CAPEX comparisons."""

from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from pydantic import ValidationError
import pytest

from parameterization.road_efficiencies import interpolate
from utils import file_sha256, load_config_bundle, load_harmonization_rules, load_yaml
from validation.road_investment_review import (
    RoadInvestmentReview, RoadInvestmentReviewConfig, _legacy_weights, _raw_ldv,
    _source_checks, factor_identity,
)
from validation.transport_comparison import ComparisonConfig, Diagnostic, write_report


@pytest.fixture
def review_config():
    payload = load_yaml(Path("config/parameters/transport_comparison.yaml"))
    return RoadInvestmentReviewConfig.model_validate(payload["road_investment_review"])


def test_post_fix_multiplicative_identity_does_not_add_percentages():
    assert factor_identity(legacy=200, current=186.2,
                           currency=0.98, weights=1, native_basis=0.95) == pytest.approx(0)
    with pytest.raises(ValueError, match="does not reproduce"):
        factor_identity(legacy=200, current=186,
                        currency=0.98, weights=1, native_basis=0.95)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_attribution_rejects_invalid_factor_values(value):
    with pytest.raises(ValueError, match="finite positive"):
        factor_identity(legacy=200, current=160,
                        currency=value, weights=1, native_basis=1)


def test_school_counterfactual_uses_actual_legacy_cells_without_endorsing_them(review_config):
    cells = {"AB154": 0.18, "AB155": 0.82}
    current = {"Class 3 Medium School": 0.5, "Class 7 Medium School": 0.5}
    old, basis = _legacy_weights(cells, review_config, "school_buses", current, {})
    assert old == {"Class 3 Medium School": 0.18, "Class 7 Medium School": 0.82}
    assert "heavy-truck" in basis
    with pytest.raises(ValueError, match="Unsupported legacy/current"):
        _legacy_weights(cells, review_config, "school_buses", {"New class": 1}, {})


def test_medium_weight_test_retains_current_vocations_and_labels_the_limit(review_config):
    modified = review_config.model_copy(update={"legacy_medium_gvwr_cells": {2: "a", 3: "b"}})
    fleet = load_harmonization_rules(load_config_bundle("config/scenarios/legacy_comparison.yaml"), "road_aggregation")
    classes = {"Class 2 Medium Van": 0.5, "Class 3 Medium Pickup": 0.25, "Class 3 Medium Van": 0.25}
    old, basis = _legacy_weights({"a": 0.6, "b": 0.4}, modified, "medium_trucks", classes, fleet)
    assert old == {"Class 2 Medium Van": 0.6, "Class 3 Medium Pickup": 0.2, "Class 3 Medium Van": 0.2}
    assert "Box absent" in basis


def test_same_source_architecture_comparison_filters_case_and_interpolates(tmp_path, review_config):
    records = []
    for variant, first, last in (("Parallel", 100, 130), ("EREV", 80, 104)):
        for year, amount in ((2035, first), (2050, last)):
            record = {field: 1 for field in review_config.legacy_ldv_columns}
            record.update(performance="Base", progress="Low", **{
                "class": "Compact", "powertrain": variant, "year": year, "manufacturing": amount,
            })
            records.append(record)
    excluded = {**records[0], "performance": "Sport", "manufacturing": 99999}
    path = tmp_path / "raw.xlsx"
    pd.DataFrame([*records, excluded]).rename(columns=review_config.legacy_ldv_columns).to_excel(
        path, sheet_name=review_config.legacy_ldv_raw_sheet, index=False,
    )
    configured = review_config.model_copy(update={"legacy_ldv_raw_workbook": str(path)})
    raw = _raw_ldv(configured, tmp_path)
    parallel = interpolate(raw.loc[raw.powertrain.eq("Parallel")], 2045, "manufacturing")
    erev = interpolate(raw.loc[raw.powertrain.eq("EREV")], 2045, "manufacturing")
    assert parallel == 120 and erev == 96
    assert erev / parallel == pytest.approx(0.8)


def test_stale_price_audit_and_currency_evidence_cannot_produce_attribution():
    audit = SimpleNamespace(cost=20, source_value=10000, cad_per_usd=2,
                            target_year_deflator=100, source_year_deflator=100,
                            normalization_divisor=1000, tech="T_CAR", vintage=2025, rpe_markup=1.5)
    compiled = SimpleNamespace(candidate=20)
    assert _source_checks(audit, compiled, 15000, 1.5) == 2
    with pytest.raises(ValueError, match="audit is stale"):
        _source_checks(audit, SimpleNamespace(candidate=21), 15000, 1.5)
    with pytest.raises(ValueError, match="prices/weights"):
        _source_checks(audit, compiled, 14000, 1.5)
    with pytest.raises(ValueError, match="stale ATB family markup"):
        _source_checks(audit, compiled, 15000, 1.2)
    audit.cad_per_usd = 1
    with pytest.raises(ValueError, match="Currency audit"):
        _source_checks(audit, compiled, 15000, 1.5)


def test_review_configuration_rejects_incomplete_or_invalid_source_basis(review_config):
    payload = review_config.model_dump()
    with pytest.raises(ValidationError, match="performance and progress"):
        RoadInvestmentReviewConfig.model_validate({**payload, "legacy_ldv_filters": {"performance": "Base"}})
    with pytest.raises(ValidationError, match="column contract is incomplete"):
        RoadInvestmentReviewConfig.model_validate({**payload, "legacy_ldv_columns": {"year": "Model Years: {years}"}})


def test_report_logs_optional_cost_study_and_hashes_each_frame(tmp_path):
    bundle = replace(load_config_bundle("config/scenarios/legacy_comparison.yaml"), repo_root=tmp_path)
    config = ComparisonConfig(
        scenario="scenario.yaml", reference_sqlite="legacy.sqlite", assumptions_path="assumptions.md",
        existing_range=(2000, 2023), future_range=(2025, 2050), top_n=10, explanations={},
    )
    frame = pd.DataFrame([{"value": 1}])
    study = RoadInvestmentReview(frame, frame, frame, frame, frame, {"region": "ON"})
    rows = pd.DataFrame([{
        "table": "cost_invest", "band": "Future", "status": "different", "units": "M$/k vehicles",
        "identifier": "T_CAR/2025", "tech": "T_CAR", "region": "ON", "candidate": 12,
        "legacy": 10, "delta": 2, "absolute_difference": 2, "relative_percent": 20,
    }])
    diagnostic = Diagnostic(config, bundle, pd.DataFrame(), rows, {}, {}, study)
    destination = write_report(diagnostic)
    snapshot = json.loads((destination / "snapshot.json").read_text())
    assert set(study.frames()) <= set(snapshot["exports"])
    assert all(file_sha256(destination / name) == snapshot["exports"][name]["sha256"]
               for name in study.frames())
