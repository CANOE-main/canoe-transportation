"""Shared typed configuration and path utilities for CANOE transportation."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter, process_time
from typing import Any

import yaml

from validation.config_models import PathsConfig, RulePartition, RulesManifest, ScenarioConfig, SourcesConfig

from .files import file_sha256 as file_sha256
from .files import write_dataframe_atomic as write_dataframe_atomic
from .files import write_text_atomic as write_text_atomic


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


class UniqueKeyLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
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


@contextmanager
def log_duration(logger: logging.Logger, label: str) -> Iterator[None]:
    """Report elapsed and process CPU time; log completion only on success."""
    started = perf_counter()
    cpu_started = process_time()
    logger.debug("Starting %s", label)
    try:
        yield
    except BaseException:
        logger.error("%s failed after %.2f s (CPU %.2f s)", label,
                     perf_counter() - started, process_time() - cpu_started)
        raise
    else:
        logger.info("%s completed in %.2f s (CPU %.2f s)", label,
                    perf_counter() - started, process_time() - cpu_started)


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


def _rules_index(parameter_dir: Path) -> RulesManifest | dict[str, Any]:
    path = _partition_path(parameter_dir, "rules.yaml")
    rules = load_yaml(path)
    if type(rules.get("version")) is int and rules["version"] == 3:
        return RulesManifest.model_validate(rules)
    # An explicit historical format, never a fallback for missing current members.
    if rules.get("version") != 2 or set(rules) != {"version", "fetching", "parameterization"}:
        raise ValueError(f"{path} requires a version 3 routing manifest or version 2 monolithic rules")
    if any(not isinstance(rules[section], dict) for section in ("fetching", "parameterization")):
        raise ValueError(f"Expected fetching and parameterization mappings in {path}")
    return rules


def _partition_path(parameter_dir: Path, filename: str) -> Path:
    root = parameter_dir.resolve()
    path = root / filename
    if not path.resolve().is_relative_to(root):
        raise ValueError(f"Rules partition escapes its configured parameter directory: {filename}")
    if not path.is_file():
        raise FileNotFoundError(f"Missing declared rules partition: {path}; restore the complete parameter configuration")
    return path


def rule_config_paths(parameter_dir: Path) -> tuple[Path, ...]:
    """Enumerate the complete ordered rules dependency set without directory discovery."""
    index = _rules_index(parameter_dir)
    paths = [_partition_path(parameter_dir, "rules.yaml")]
    if isinstance(index, RulesManifest):
        paths.extend(_partition_path(parameter_dir, part.file) for part in index.partitions)
    if len(paths) != len({path.resolve() for path in paths}):
        raise ValueError("Rules members must resolve to distinct physical files")
    return tuple(paths)


def harmonization_rule_paths(bundle: ConfigBundle) -> tuple[Path, ...]:
    """Resolve all rule controls, including the routing index, for audit/freshness."""
    return rule_config_paths(resolve_parameter_path(bundle, "."))


def harmonization_rule_fingerprints(bundle: ConfigBundle) -> dict[str, str]:
    """Use stable member names for portable internal or external configuration roots."""
    return {path.name: file_sha256(path) for path in harmonization_rule_paths(bundle)}


def _partition_rules(parameter_dir: Path, partition: RulePartition) -> dict[str, Any]:
    path = _partition_path(parameter_dir, partition.file)
    payload = load_yaml(path)
    sections = {section for section in ("fetching", "parameterization") if getattr(partition, section)}
    if type(payload.get("version")) is not int or payload["version"] != 1 or set(payload) != {"version", *sections}:
        raise ValueError(f"Rules partition {path} requires version 1 and exactly its declared sections")
    for section in sections:
        values = payload[section]
        if not isinstance(values, dict) or set(values) != set(getattr(partition, section)):
            raise ValueError(f"Rules partition {path} has missing or undeclared {section} owners")
        if any(not isinstance(value, dict) for value in values.values()):
            raise ValueError(f"Rules partition {path} requires mappings for every {section} owner")
    return payload


def load_rules_config(parameter_dir: Path) -> dict[str, Any]:
    """Validate the complete configuration and expose unchanged logical owner sections.

    Only physical partitioning changes. There is no recursive merge, discovery,
    mutable cache or substitution of files from another configuration root.
    """
    index = _rules_index(parameter_dir)
    if not isinstance(index, RulesManifest):
        for name, value in (index["fetching"] | index["parameterization"]).items():
            if not isinstance(value, dict):
                raise ValueError(f"Expected mapping for harmonization rules: {name}")
        if set(index["fetching"]) & set(index["parameterization"]):
            raise ValueError("Every rules module must have exactly one owner")
        return index
    rule_config_paths(parameter_dir)
    result = {"version": 2, "fetching": {}, "parameterization": {}}
    for partition in index.partitions:
        payload = _partition_rules(parameter_dir, partition)
        for section in ("fetching", "parameterization"):
            result[section].update(payload.get(section, {}))
    return result


def load_rules_for_owner(directory: Path, module_name: str) -> dict[str, Any]:
    """Read one declared owner without requiring a scenario or unrelated payload files."""
    index = _rules_index(directory)
    if isinstance(index, RulesManifest):
        matches = [(part, section) for part in index.partitions for section in ("fetching", "parameterization")
                   if module_name in getattr(part, section)]
        if matches:
            partition, section = matches[0]
            return _partition_rules(directory, partition)[section][module_name]
    else:
        owners = [section for section in ("fetching", "parameterization") if module_name in index[section]]
        if len(owners) > 1:
            raise ValueError(f"Rules module {module_name!r} must have exactly one owner")
        if owners:
            value = index[owners[0]][module_name]
            if not isinstance(value, dict):
                raise ValueError(f"Expected mapping for harmonization rules: {module_name}")
            return value
    raise KeyError(f"Missing extraction/harmonization rules for module: {module_name}")


def load_harmonization_rules(bundle: ConfigBundle, module_name: str) -> dict[str, Any]:
    """Load only the declared partition of one executable owner, freshly on each call."""
    return load_rules_for_owner(resolve_parameter_path(bundle, "."), module_name)


def load_conversion_factors(bundle: ConfigBundle) -> dict[str, Any]:
    """Load shared conversion factors."""
    return load_parameter_yaml(bundle, "conversion.yaml")


def load_config_bundle(
    scenario_path: str | Path | ConfigBundle,
    *,
    repo_root: Path | None = None,
    paths_path: str | Path = "config/paths.yaml",
    sources_path: str | Path = "config/sources.yaml",
    parameter_dir: str | Path | None = None,
) -> ConfigBundle:
    """Load controls, or preserve a caller's already loaded artifact/configuration root."""
    if isinstance(scenario_path, ConfigBundle):
        if (repo_root is not None or paths_path != "config/paths.yaml"
                or sources_path != "config/sources.yaml" or parameter_dir is not None):
            raise ValueError("ConfigBundle controls cannot be combined with configuration path overrides")
        return scenario_path
    root = (repo_root or find_repo_root()).resolve()
    resolved_paths = resolve_repo_path(root, paths_path)
    resolved_sources = resolve_repo_path(root, sources_path)
    resolved_scenario = resolve_repo_path(root, scenario_path)
    paths = PathsConfig.model_validate(load_yaml(resolved_paths))
    if parameter_dir is not None:
        paths = paths.model_copy(update={
            "config": paths.config.model_copy(update={"parameters": str(resolve_repo_path(root, parameter_dir))}),
        })
    scenario_payload = load_yaml(resolved_scenario)
    bundle = ConfigBundle(
        repo_root=root,
        paths_path=resolved_paths,
        sources_path=resolved_sources,
        scenario_path=resolved_scenario,
        paths=paths,
        sources=SourcesConfig.model_validate(load_yaml(resolved_sources)),
        scenario=ScenarioConfig.model_validate(scenario_payload),
    )
    errors = validate_config_bundle(bundle)
    if errors:
        raise ValueError(f"Invalid configuration: {errors}")
    logging.getLogger(__name__).debug(
        "Unannotated source/component DQ indicators use registry defaults: %s",
        bundle.sources.defaults.data_quality.row_fields(),
    )
    period_mapping = bundle.scenario.periods.audit()
    logging.getLogger(__name__).debug("Scenario period mapping: %s", period_mapping)
    if period_mapping["empty_observation_vintages"]:
        logging.getLogger(__name__).warning(
            "Existing labels have no observed years through %s: %s",
            bundle.scenario.periods.base_year, period_mapping["empty_observation_vintages"],
        )
    return bundle


def validate_config_bundle(bundle: ConfigBundle) -> list[str]:
    """Return cross-file errors after structural Pydantic validation."""
    load_rules_config(resolve_parameter_path(bundle, "."))
    errors: list[str] = []
    registered = bundle.sources.sources
    for source_name in bundle.scenario.sources.selections:
        if source_name not in registered:
            errors.append(f"source selection not defined in sources.yaml: {source_name}")
        elif registered[source_name].status != "active":
            errors.append(f"source selection is inactive in sources.yaml: {source_name}")
    stock_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
    profile = bundle.scenario.lifetimes.road_lifetime_survival_source
    source_keys = {item["source_id"] for item in stock_rules["survival_source_profiles"][profile].values()
                   if "source_id" in item}
    if any(item["kind"] == "ceud_median" for item in stock_rules["survival_source_profiles"][profile].values()):
        source_keys.add(load_harmonization_rules(bundle, "assorted_sources")["nhtsa_cafe"]["source_id"])
    for source_key in sorted(source_keys):
        if source_key not in registered or registered[source_key].status != "active":
            errors.append(f"lifetimes.road_lifetime_survival_source uses unknown or inactive source: {source_key}")
    selectors = {
        "nrcan_ceud_transport_provincial": "year",
        "nrcan_ceud_transport_national": "year",
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
    atb = registered.get("nlr_atb_transportation")
    if atb is not None:
        supported = atb.adapter.get("supported_scenario_trajectories")
        if not isinstance(supported, list) or not supported:
            errors.append(
                "sources.nlr_atb_transportation.adapter.supported_scenario_trajectories is missing or invalid; "
                "this source registry predates the supported scenario contract. Rebuild the capture "
                "with the current registered sources.yaml; do not infer scenario choices from native trajectory labels."
            )
        else:
            for section in ("efficiencies", "costs"):
                trajectory = getattr(bundle.scenario, section).atb_trajectory
                if trajectory not in supported:
                    reason = atb.adapter.get("unsupported_scenario_notes", {}).get(trajectory, "")
                    errors.append(f"{section}.atb_trajectory is not supported: {trajectory}; "
                                  f"choose {supported}. {reason}")
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
            "ontario_ministry_transport_vehicle_population", "wards_intelligence_sales_shares",
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
