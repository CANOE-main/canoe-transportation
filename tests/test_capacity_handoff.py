"""Capacity publication commits one verifiable generation for its consumers."""

from copy import deepcopy
from dataclasses import replace
import json

from canoe_schema.v4_0 import ExistingCapacity
import pandas as pd
import pytest

from parameterization import build_existing_capacity as layer
from utils import file_sha256, load_config_bundle, resolve_artifact_path
from validation.provenance import resolve_provenance


@pytest.fixture
def prepared(tmp_path):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    routes = dict(bundle.paths.artifacts)
    for family in ("existing_capacity_interim", "existing_capacity_processed", "existing_capacity_validation"):
        routes[family] = routes[family].model_copy(update={"path": str(tmp_path / family)})
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={"artifacts": routes}))
    context = resolve_provenance(
        bundle.sources, source_key="statcan_transport_tables", component_key="20-10-0021-01",
        transformation="capacity completion fixture", transformation_version="1",
    )
    rows = [ExistingCapacity(region="ON", tech="T_LDV_C_BEV_EX", vintage=2020,
                             capacity=10, units="k vehicles", **context.parameter_fields())]
    path = resolve_artifact_path(bundle, "existing_capacity_interim", "annual_stock.csv")
    path.parent.mkdir(parents=True)
    path.write_text("capacity\n10\n", encoding="utf-8")
    audit = {"period_mapping": bundle.scenario.periods.audit(),
             "annual_stock_evidence": {"path": str(path), "sha256": file_sha256(path)}}
    arguments = {"rows": rows, "contexts": [context], "audit": audit}
    layer.publish_capacity_handoff(bundle, **arguments, artifact_paths=[path])
    return bundle, path, arguments


def marker_path(bundle):
    return resolve_artifact_path(bundle, "existing_capacity_validation", "integrity.json")


def test_repeated_publication_is_deterministic_and_tracks_exact_rows(prepared):
    bundle, path, arguments = prepared
    before = marker_path(bundle).read_bytes()
    old = deepcopy(arguments)
    layer.publish_capacity_handoff(bundle, **arguments, artifact_paths=[path])
    assert marker_path(bundle).read_bytes() == before
    layer.validate_capacity_handoff(bundle, **arguments)
    arguments["rows"] = [arguments["rows"][0].model_copy(update={"capacity": 11.0})]
    with pytest.raises(ValueError, match="scenario, rows or contexts"):
        layer.validate_capacity_handoff(bundle, **arguments)
    path.write_text("capacity\n11\n", encoding="utf-8")
    arguments["audit"]["annual_stock_evidence"]["sha256"] = file_sha256(path)
    layer.publish_capacity_handoff(bundle, **arguments, artifact_paths=[path])
    assert marker_path(bundle).read_bytes() != before
    layer.validate_capacity_handoff(bundle, **arguments)
    with pytest.raises(ValueError, match="differs from the supplied audit"):
        layer.validate_capacity_handoff(bundle, **old)


def test_handoff_binds_scenario_and_provenance_but_not_output_destination(prepared):
    bundle, _, arguments = prepared
    payload = bundle.scenario.model_dump()
    payload["outputs"]["sqlite_name"] = "elsewhere.sqlite"
    redirected = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    layer.validate_capacity_handoff(redirected, **arguments)
    payload["geography"]["regions"] = ["ON"]
    different_scope = replace(bundle, scenario=type(bundle.scenario).model_validate(payload))
    with pytest.raises(ValueError, match="scenario, rows or contexts"):
        layer.validate_capacity_handoff(different_scope, **arguments)
    changed = deepcopy(arguments)
    changed["contexts"] = [changed["contexts"][0].model_copy(update={"dataset_description": "changed lineage"})]
    with pytest.raises(ValueError, match="scenario, rows or contexts"):
        layer.validate_capacity_handoff(bundle, **changed)


@pytest.mark.parametrize("problem,message", [
    ("missing_marker", "no readable completion marker"),
    ("invalid_marker", "no readable completion marker"),
    ("old_marker", "differs from the supplied audit"),
    ("missing_artifact", "artifact is missing"),
    ("changed_artifact", "artifact changed"),
])
def test_missing_stale_or_changed_publication_is_rejected(prepared, problem, message):
    bundle, path, arguments = prepared
    if problem == "missing_marker":
        marker_path(bundle).unlink()
    elif problem == "invalid_marker":
        marker_path(bundle).write_text("{", encoding="utf-8")
    elif problem == "old_marker":
        marker_path(bundle).write_text(json.dumps({"old": "audit"}), encoding="utf-8")
    elif problem == "missing_artifact":
        path.unlink()
    else:
        path.write_text("capacity\n99\n", encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        layer.validate_capacity_handoff(bundle, **arguments)


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("partial_write", [False, True])
def test_failed_preparation_invalidates_previous_handoff_and_can_recover(
    prepared, monkeypatch, failure, partial_write,
):
    bundle, path, arguments = prepared
    previous = path.read_bytes()

    def fail(*_args, **_kwargs):
        assert not marker_path(bundle).exists()
        if partial_write:
            path.write_text("partial preparation\n", encoding="utf-8")
        raise failure("interrupted capacity preparation")

    if partial_write:
        # Reach the first dependency producer without acquiring or writing real inputs.
        monkeypatch.setattr(layer, "read_ceud_input", lambda *_args: pd.DataFrame())
        monkeypatch.setattr(layer, "validate_capacity_inputs", lambda *_args: None)
        monkeypatch.setattr(layer, "derive_accepted_lifetime_frames", fail)
    else:
        monkeypatch.setattr(layer, "active_source_keys", fail)
    with pytest.raises(failure, match="interrupted capacity preparation"):
        layer.build_existing_capacity_artifacts(bundle)
    assert not marker_path(bundle).exists()
    with pytest.raises(ValueError, match="no readable completion marker"):
        layer.validate_capacity_handoff(bundle, **arguments)
    # The producer's final publication step restores acceptance only after every file exists.
    path.write_bytes(previous)
    layer.publish_capacity_handoff(bundle, **arguments, artifact_paths=[path])
    layer.validate_capacity_handoff(bundle, **arguments)


def test_marker_invalidation_during_validation_is_rejected(prepared, monkeypatch):
    bundle, _, arguments = prepared
    original = layer.file_sha256

    def invalidate(path):
        result = original(path)
        marker_path(bundle).unlink()
        return result

    monkeypatch.setattr(layer, "file_sha256", invalidate)
    with pytest.raises(ValueError, match="marker changed while checking evidence"):
        layer.validate_capacity_handoff(bundle, **arguments)


def test_failed_completion_replace_keeps_incomplete_family_uncommitted(prepared, monkeypatch):
    import utils.files as files

    bundle, path, arguments = prepared
    marker_path(bundle).unlink()  # As on entry to the real producer.

    def fail_replace(*_args):
        raise PermissionError("marker held open")

    monkeypatch.setattr(files.os, "replace", fail_replace)
    with pytest.raises(PermissionError, match="marker held open"):
        layer.publish_capacity_handoff(bundle, **arguments, artifact_paths=[path])
    assert not marker_path(bundle).exists()
    assert not list(marker_path(bundle).parent.glob("*.tmp"))
    with pytest.raises(ValueError, match="no readable completion marker"):
        layer.validate_capacity_handoff(bundle, **arguments)
