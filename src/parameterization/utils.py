"""Shared utilities for the CANOE transportation backend scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REQUIRED_PATH_SECTIONS = ("inputs", "outputs", "legacy")
REQUIRED_INPUT_KEYS = ("cache", "external", "interim", "processed", "schema")
REQUIRED_OUTPUT_KEYS = ("sqlite", "validation", "logs")
REQUIRED_SCENARIO_KEYS = (
    "scenario",
    "regions",
    "model_years",
    "active_sources",
    "outputs",
    "validation",
    "switches",
)
REQUIRED_SOURCE_KEYS = ("title", "status", "source_type", "file_type", "path", "validation_rule")


@dataclass(frozen=True)
class ConfigBundle:
    """Loaded paths, sources, and scenario configuration."""

    repo_root: Path
    paths_path: Path
    sources_path: Path
    scenario_path: Path
    paths: dict[str, Any]
    sources: dict[str, Any]
    scenario: dict[str, Any]


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML mapping from disk."""
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


def find_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking up to pyproject.toml."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not find repository root containing pyproject.toml")


def resolve_repo_path(repo_root: Path, value: str | Path) -> Path:
    """Resolve a repo-relative path without requiring it to exist."""
    path = Path(value)
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


def load_config_bundle(
    scenario_path: str | Path,
    *,
    repo_root: Path | None = None,
    paths_path: str | Path = "config/paths.yaml",
    sources_path: str | Path = "config/sources.yaml",
) -> ConfigBundle:
    """Load the three YAML files that define the control-layer scaffold."""
    root = (repo_root or find_repo_root()).resolve()
    resolved_paths = resolve_repo_path(root, paths_path)
    resolved_sources = resolve_repo_path(root, sources_path)
    resolved_scenario = resolve_repo_path(root, scenario_path)
    return ConfigBundle(
        repo_root=root,
        paths_path=resolved_paths,
        sources_path=resolved_sources,
        scenario_path=resolved_scenario,
        paths=load_yaml(resolved_paths),
        sources=load_yaml(resolved_sources),
        scenario=load_yaml(resolved_scenario),
    )


def validate_config_bundle(bundle: ConfigBundle) -> list[str]:
    """Return validation messages for missing required scaffold fields."""
    errors: list[str] = []

    for section in REQUIRED_PATH_SECTIONS:
        if section not in bundle.paths or not isinstance(bundle.paths[section], dict):
            errors.append(f"paths.yaml missing mapping: {section}")

    inputs = bundle.paths.get("inputs", {})
    outputs = bundle.paths.get("outputs", {})
    if isinstance(inputs, dict):
        for key in REQUIRED_INPUT_KEYS:
            if key not in inputs:
                errors.append(f"paths.yaml missing inputs.{key}")
    if isinstance(outputs, dict):
        for key in REQUIRED_OUTPUT_KEYS:
            if key not in outputs:
                errors.append(f"paths.yaml missing outputs.{key}")

    for key in REQUIRED_SCENARIO_KEYS:
        if key not in bundle.scenario:
            errors.append(f"scenario YAML missing {key}")

    sources = bundle.sources.get("sources")
    if not isinstance(sources, dict):
        errors.append("sources.yaml missing sources mapping")
        sources = {}

    active_sources = bundle.scenario.get("active_sources", [])
    if not isinstance(active_sources, list):
        errors.append("scenario YAML active_sources must be a list")
        active_sources = []

    for source_name in active_sources:
        source = sources.get(source_name)
        if not isinstance(source, dict):
            errors.append(f"active source not defined in sources.yaml: {source_name}")
            continue
        for key in REQUIRED_SOURCE_KEYS:
            if key not in source:
                errors.append(f"sources.yaml missing sources.{source_name}.{key}")

    return errors


def configured_directories(bundle: ConfigBundle) -> list[Path]:
    """Return directories created by setup smoke validation."""
    inputs = bundle.paths["inputs"]
    outputs = bundle.paths["outputs"]
    keys = (
        inputs["cache"],
        inputs["external"],
        inputs["interim"],
        inputs["processed"],
        outputs["sqlite"],
        outputs["validation"],
        outputs["logs"],
    )
    return [resolve_repo_path(bundle.repo_root, key) for key in keys]


def create_configured_directories(bundle: ConfigBundle) -> list[Path]:
    """Create configured input/output working directories."""
    directories = configured_directories(bundle)
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
