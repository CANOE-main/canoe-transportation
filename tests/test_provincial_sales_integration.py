"""Registered-input provincial stock/activity integration with disjoint write roots."""
from dataclasses import replace
import os

import pandas as pd
import pytest

from build_transport import prepare_scenario_inputs
from parameterization.build_existing_capacity import prepare_existing_capacity_rows
from parameterization.road_utilization import prepare_road_utilization
from utils import load_config_bundle, load_harmonization_rules, resolve_artifact_path
from validation.config_models import ScenarioConfig


@pytest.mark.skipif(os.environ.get("CANOE_REGISTERED_INPUT_TESTS") != "1", reason="requires registered offline source caches")
def test_each_province_closes_its_own_sales_activity(tmp_path, monkeypatch):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    payload = bundle.scenario.model_dump(mode="python")
    payload["geography"]["regions"] = ["ON", "AB", "BCT", "MB", "NB", "NL", "NS", "PE", "QC", "SK"]
    payload["periods"] = {
        "period_mode": "prospective", "base_year": 2025,
        "existing": [2000, 2005, 2010, 2015, 2020],
        "model": [2025, 2030, 2035, 2040, 2045], "step": 5,
    }
    payload["sources"]["nrcan_ceud_beyond_observed"] = "constant"
    payload["existing_capacity"]["road_stock_method"] = {"other": "sales_survival"}
    payload["lifetimes"]["road_lifetime_survival_source"] = "moves"
    payload["lifetimes"]["survival_curves"] = False
    payload["road_utilization"]["vkt_schedules"] = False
    bundle = replace(bundle, scenario=ScenarioConfig.model_validate(payload))
    write_layers = {"interim", "processed", "input_validation", "output_validation"}
    routes = {name: route.model_copy(update={"path": str(tmp_path / route.path)})
              if route.layer in write_layers else route
              for name, route in bundle.paths.artifacts.items()}
    inputs = bundle.paths.inputs.model_copy(update={
        "interim": str(tmp_path / "inputs/1_interim"), "processed": str(tmp_path / "inputs/2_processed"),
        "validation": str(tmp_path / "inputs/validation"),
    })
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes, "inputs": inputs}))
    # Source CLIs reload YAML. Bind their configuration reads to this disjoint
    # context while exercising their real cached parsing and normalization.
    from fetching import (
        assorted_sources, cer_enerfuture, nlr_atb_autonomie, nrcan_ceud,
        statcan_tables, vehicle_population,
    )
    from parameterization import road_aggregation
    for adapter in (assorted_sources, cer_enerfuture, nlr_atb_autonomie,
                    nrcan_ceud, statcan_tables, vehicle_population, road_aggregation):
        monkeypatch.setattr(adapter, "load_config_bundle", lambda *args, **kwargs: bundle)
    def reject_network(*args, **kwargs):
        raise AssertionError("registered-input integration must remain offline")
    monkeypatch.setattr("requests.sessions.Session.request", reject_network)
    prepare_scenario_inputs(bundle, download=False)
    rows, contexts, _ = prepare_existing_capacity_rows(bundle)
    prepare_road_utilization(bundle, existing_capacity_rows=rows, existing_capacity_contexts=contexts)
    rules = load_harmonization_rules(bundle, "road_utilization")
    audit = pd.read_csv(resolve_artifact_path(bundle, "road_utilization_processed") / rules["sales_calibration_file"])
    assert len(audit) == 50
    assert audit.region.nunique() == 10
    assert audit.groupby("region").road_class.nunique().eq(5).all()
    assert audit.activity_residual_bn.abs().max() < 1e-10
    assert audit.utilization.between(0, 1).all()
    print("Provinces",audit.region.nunique(),"class calibrations",len(audit),"max residual",audit.activity_residual_bn.abs().max())
