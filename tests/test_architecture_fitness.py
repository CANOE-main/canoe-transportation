"""Executable checks for the compact artifact impact map."""

from __future__ import annotations

import importlib
from pathlib import Path

from utils import load_config_bundle, resolve_artifact_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def _resolve_dotted_object(reference: str):
    parts = reference.split(".")
    for split_at in range(len(parts), 0, -1):
        module_name = ".".join(parts[:split_at])
        try:
            value = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        for attribute in parts[split_at:]:
            value = getattr(value, attribute)
        return value
    raise ImportError(reference)


def test_impact_routes_have_live_owners_producers_and_validation_surfaces() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    for family, route in bundle.paths.artifacts.items():
        assert resolve_artifact_path(bundle, family).is_relative_to(REPO_ROOT)
        importlib.import_module(route.owner)
        for producer in route.producers:
            assert callable(_resolve_dotted_object(producer))
        for consumer in route.consumers:
            if consumer not in {"reviewers", "temoa"}:
                _resolve_dotted_object(consumer)
        for surface in route.validation_surfaces:
            assert callable(_resolve_dotted_object(surface)), (family, surface)


def test_important_artifact_families_cover_each_declared_layer() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert {
        "ontario_vehicle_population",
        "road_aggregation",
        "lifetimes_survival",
        "stocks_and_demands",
        "vehicle_mapping_review",
        "database",
        "database_validation",
    } <= set(bundle.paths.artifacts)
    assert {route.layer for route in bundle.paths.artifacts.values()} == {
        "interim",
        "processed",
        "input_validation",
        "database",
        "output_validation",
    }
