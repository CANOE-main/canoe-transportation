"""Shared typed configuration and path utilities for CANOE transportation."""

from __future__ import annotations

import logging
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
        data = yaml.load(handle, Loader=UniqueKeyLoader) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


class UniqueKeyLoader(yaml.SafeLoader):
    """Reject overwritten YAML keys while retaining safe loading and anchors."""

    def construct_mapping(self, node, deep=False):
        keys = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            if key in keys:
                raise ValueError(
                    f"Duplicate YAML key {key!r} at {key_node.start_mark}"
                )
            keys.add(key)
        return super().construct_mapping(node, deep=deep)


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
    """Load extraction/harmonization rules for one executable owner."""
    rules = load_parameter_yaml(bundle, "rules.yaml")
    if rules.get("version") != 2 or set(rules) != {"version", "fetching", "parameterization"}:
        raise ValueError("rules.yaml requires version 2 with fetching and parameterization sections")
    owners = [
        section for section in ("fetching", "parameterization")
        if module_name in rules[section]
    ]
    if not owners:
        raise KeyError(
            f"Missing extraction/harmonization rules for module: {module_name}"
        )
    if len(owners) != 1:
        raise ValueError(f"Rules module {module_name!r} must have exactly one owner")
    module_rules = rules[owners[0]][module_name]
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
    logging.getLogger(__name__).info(
        "Unannotated source/component DQ indicators use registry defaults: %s",
        bundle.sources.defaults.data_quality.row_fields(),
    )
    return bundle


def validate_config_bundle(bundle: ConfigBundle) -> list[str]:
    """Return cross-file errors after structural Pydantic validation."""
    errors: list[str] = []
    registered = bundle.sources.sources
    for source_name in bundle.scenario.sources.selections:
        if source_name not in registered:
            errors.append(f"source selection not defined in sources.yaml: {source_name}")
        elif registered[source_name].status != "active":
            errors.append(f"source selection is inactive in sources.yaml: {source_name}")
    selectors = {
        "nrcan_ceud_transport_provincial": "year",
        "nrcan_ceud_transport_national": "year",
        "transport_canada_ev_dashboard": "year",
        "cer_canadas_energy_future": "edition",
    }
    selections = bundle.scenario.sources.selections
    for source_name, field in selectors.items():
        if source_name not in selections or getattr(selections[source_name], field) is None:
            errors.append(f"sources.selections.{source_name}.{field} is required")
    for source_name, selection in selections.items():
        if source_name not in selectors:
            errors.append(f"No implemented scenario source selector for {source_name}")
        else:
            supplied = {key for key, value in selection.model_dump().items() if value is not None}
            if supplied != {selectors[source_name]}:
                errors.append(f"sources.selections.{source_name} supports only {selectors[source_name]}")
    atb = registered.get("nlr_atb_transportation_2024")
    if atb is not None:
        for section in ("efficiencies", "costs"):
            trajectory = getattr(bundle.scenario, section).atb_trajectory
            if trajectory not in atb.adapter["expected_trajectories"]:
                errors.append(f"{section}.atb_trajectory is not registered: {trajectory}")
    cer = registered.get("cer_canadas_energy_future")
    if cer is not None and "cer_canadas_energy_future" in selections:
        edition = selections["cer_canadas_energy_future"].edition
        editions = cer.adapter["editions"]["allowed"]
        edition_metadata = editions.get(edition, editions.get(str(edition)))
        if edition_metadata is None:
            errors.append(f"sources.selections.cer_canadas_energy_future.edition is not registered: {edition}")
        else:
            for section in ("demand", "economics"):
                trajectory = getattr(bundle.scenario, section).cer_scenario
                if trajectory not in edition_metadata["scenarios"]:
                    errors.append(f"{section}.cer_scenario is not registered for CER {edition}: {trajectory}")
    supported_aggregation = {
        "stock_age": {"ontario_ministry_transport_vehicle_population"},
        "ldv": {"ontario_ministry_transport_vehicle_population"},
        "medium_trucks": {
            "ontario_ministry_transport_vehicle_population", "wards_intelligence_2022_sales_shares",
        },
        "heavy_truck_haul": {"statcan_transport_tables"},
    }
    for role, adapters in supported_aggregation.items():
        for region, source in bundle.scenario.aggregation_sources[role].items():
            if source not in registered or registered[source].status != "active":
                errors.append(f"aggregation_sources.{role}.{region} source is unknown or inactive: {source}")
            elif source not in adapters:
                errors.append(f"aggregation_sources.{role}.{region} has no implemented adapter for {source}")
    return errors


def active_source_keys(bundle: ConfigBundle) -> set[str]:
    """Return sources available for the configured build from the registry."""
    return {
        key for key, source in bundle.sources.sources.items() if source.status == "active"
    }


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
