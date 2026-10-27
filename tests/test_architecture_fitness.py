"""Executable checks for the compact artifact impact map."""

from __future__ import annotations

import importlib
from pathlib import Path
import subprocess
import sys
from types import ModuleType

from utils import load_config_bundle, resolve_artifact_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


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


def _assert_live_interface(reference: str) -> None:
    if reference in {"marimo.check", "marimo.export"}:
        # These configured validation/export interfaces are CLI subcommands.
        subprocess.run(
            [sys.executable, "-m", "marimo", reference.split(".")[1], "--help"],
            check=True, capture_output=True, cwd=REPO_ROOT,
        )
    else:
        interface = _resolve_dotted_object(reference)
        assert callable(interface) or (
            isinstance(interface, ModuleType) and callable(getattr(interface, "main", None))
        ), reference


def test_impact_routes_have_live_owners_producers_and_validation_surfaces(monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(REPO_ROOT))
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    for family, route in bundle.paths.artifacts.items():
        assert resolve_artifact_path(bundle, family).is_relative_to(REPO_ROOT)
        importlib.import_module(route.owner)
        for producer in route.producers:
            if producer == "reviewers":
                assert route.layer == "documentation"
            else:
                _assert_live_interface(producer)
        for consumer in route.consumers:
            if consumer.startswith("docs."):
                # Notebook routes are document files, not installed runtime packages.
                assert (REPO_ROOT / Path(*consumer.split("."))).with_suffix(".py").is_file()
            elif consumer not in {"reviewers", "publication_reviewers", "temoa"}:
                _resolve_dotted_object(consumer)
        for surface in route.validation_surfaces:
            _assert_live_interface(surface)


def test_important_artifact_families_cover_each_declared_layer() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert {
        "ontario_vehicle_population",
        "road_aggregation",
        "road_lifetimes_survival",
        "road_stocks_and_demands",
        "vehicle_mapping_review",
        "database",
        "database_validation",
    } <= set(bundle.paths.artifacts)
    assert {route.layer for route in bundle.paths.artifacts.values()} == {
        "external",
        "interim",
        "processed",
        "input_validation",
        "database",
        "output_validation",
        "cache",
        "documentation",
        "legacy",
        "logs",
    }


def test_lifetime_routes_separate_accepted_and_mto_diagnostic_publishers() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    accepted = (
        "parameterization.road_lifetimes_survival.build_accepted_lifetime_artifacts"
    )
    diagnostics = (
        "parameterization.road_lifetimes_survival."
        "build_mto_survival_diagnostic_artifacts"
    )

    processed = bundle.paths.artifacts["road_lifetimes_survival"]
    interim = bundle.paths.artifacts["vehicle_survival_interim"]
    validation = bundle.paths.artifacts["lifetime_validation"]

    assert accepted in processed.producers
    assert accepted not in interim.producers
    assert accepted not in validation.producers
    assert diagnostics not in processed.producers
    assert diagnostics in interim.producers
    assert diagnostics in validation.producers
