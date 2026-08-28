"""Shared typed configuration and path utilities for CANOE transportation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from validation.config_models import PathsConfig, ScenarioConfig, SourcesConfig

from .files import file_sha256 as file_sha256
from .files import write_dataframe_atomic as write_dataframe_atomic


@dataclass(frozen=True)
class ConfigBundle:
    """Typed declarative configuration plus resolved runtime paths."""

    repo_root: Path
    paths_path: Path
    sources_path: Path
    scenario_path: Path
    paths: PathsConfig
    sources: SourcesConfig
    scenario: ScenarioConfig


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


def resolve_configured_path(
    bundle: ConfigBundle,
    section: str,
    key: str,
    *parts: str | Path,
) -> Path:
    """Resolve a path from paths.yaml and append optional child parts."""
    section_config = bundle.paths[section]
    base = resolve_repo_path(bundle.repo_root, section_config[key])
    return base.joinpath(*map(Path, parts)) if parts else base


def resolve_input_path(bundle: ConfigBundle, key: str, *parts: str | Path) -> Path:
    """Resolve an input path from paths.yaml."""
    return resolve_configured_path(bundle, "inputs", key, *parts)


def resolve_artifact_path(
    bundle: ConfigBundle,
    family: str,
    *parts: str | Path,
) -> Path:
    """Resolve a stable artifact-family route from paths.yaml."""
    try:
        route = bundle.paths.artifacts[family]
    except KeyError as exc:
        raise KeyError(f"Unknown artifact family: {family}") from exc
    base = resolve_repo_path(bundle.repo_root, route.path)
    return base.joinpath(*map(Path, parts)) if parts else base


def resolve_parameter_path(bundle: ConfigBundle, filename: str | Path) -> Path:
    """Resolve a config/parameters file through paths.yaml."""
    return resolve_repo_path(bundle.repo_root, bundle.paths.config.parameters) / Path(filename)


def load_parameter_yaml(bundle: ConfigBundle, filename: str | Path) -> dict[str, Any]:
    """Load a YAML mapping from config/parameters."""
    return load_yaml(resolve_parameter_path(bundle, filename))


def load_harmonization_rules(bundle: ConfigBundle, module_name: str) -> dict[str, Any]:
    """Load harmonization rules for one parameterization module."""
    rules = load_parameter_yaml(bundle, "rules.yaml")
    try:
        module_rules = rules["parameterization"][module_name]
    except KeyError as exc:
        raise KeyError(
            f"Missing parameterization rules for module: {module_name}"
        ) from exc
    if not isinstance(module_rules, dict):
        raise ValueError(f"Expected mapping for harmonization rules: {module_name}")
    return module_rules


def load_conversion_factors(bundle: ConfigBundle) -> dict[str, Any]:
    """Load shared conversion factors."""
    return load_parameter_yaml(bundle, "conversion.yaml")


def load_config_bundle(
    scenario_path: str | Path,
    *,
    repo_root: Path | None = None,
    paths_path: str | Path = "config/paths.yaml",
    sources_path: str | Path = "config/sources.yaml",
) -> ConfigBundle:
    """Load and strictly validate the three YAML control-layer files."""
    root = (repo_root or find_repo_root()).resolve()
    resolved_paths = resolve_repo_path(root, paths_path)
    resolved_sources = resolve_repo_path(root, sources_path)
    resolved_scenario = resolve_repo_path(root, scenario_path)
    bundle = ConfigBundle(
        repo_root=root,
        paths_path=resolved_paths,
        sources_path=resolved_sources,
        scenario_path=resolved_scenario,
        paths=PathsConfig.model_validate(load_yaml(resolved_paths)),
        sources=SourcesConfig.model_validate(load_yaml(resolved_sources)),
        scenario=ScenarioConfig.model_validate(load_yaml(resolved_scenario)),
    )
    errors = validate_config_bundle(bundle)
    if errors:
        raise ValueError(f"Invalid configuration: {errors}")
    return bundle


def validate_config_bundle(bundle: ConfigBundle) -> list[str]:
    """Return cross-file errors after structural Pydantic validation."""
    errors: list[str] = []
    registered = bundle.sources.sources
    for source_name in bundle.scenario.sources.active:
        if source_name not in registered:
            errors.append(f"active source not defined in sources.yaml: {source_name}")
        elif registered[source_name].status != "active":
            errors.append(
                f"scenario activates inactive source from sources.yaml: {source_name}"
            )
    for source_name in bundle.scenario.sources.selections:
        if source_name not in registered:
            errors.append(f"source selection not defined in sources.yaml: {source_name}")
    return errors


def configured_directories(bundle: ConfigBundle) -> list[Path]:
    """Return directories created by setup smoke validation."""
    keys = (
        bundle.paths.inputs.cache,
        bundle.paths.inputs.external,
        bundle.paths.inputs.manual,
        bundle.paths.inputs.interim,
        bundle.paths.inputs.processed,
        bundle.paths.inputs.validation,
        bundle.paths.outputs.sqlite,
        bundle.paths.outputs.validation,
        bundle.paths.outputs.logs,
    )
    return [resolve_repo_path(bundle.repo_root, key) for key in keys]


def create_configured_directories(bundle: ConfigBundle) -> list[Path]:
    """Create configured input/output working directories."""
    directories = configured_directories(bundle)
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
