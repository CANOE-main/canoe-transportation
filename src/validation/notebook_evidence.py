"""Recorded inputs for the two opt-in road diagnostic notebooks.

The scenario builder owns preparation. This owner snapshots already prepared files
and configuration, then advances one latest-successful index after database
publication. Readers never select a scenario or prepare missing evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from contextlib import contextmanager
from datetime import UTC, datetime
import gzip
from pathlib import Path
import re
import shutil
import stat
from typing import Annotated, Iterator, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from utils import (
    ConfigBundle, file_sha256, find_repo_root, load_config_bundle,
    load_harmonization_rules, load_yaml, resolve_artifact_path,
    resolve_parameter_path, resolve_repo_path, write_text_atomic,
)
from validation.config_models import PathsConfig, ScenarioIdentity


Profile = Literal["vehicle_stock", "efficiency"]
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
# Accept historical captures while all new releases use an informative UTC name.
BUILD_ID_PATTERN = r"(?:[A-Za-z0-9][A-Za-z0-9_.-]*-\d{8}T\d{6}Z(?:-[1-9]\d*)?|[0-9a-f]{32})"


class EvidenceFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    path: str = Field(min_length=1)
    source: str = Field(min_length=1)
    sha256: Digest


class EvidenceDatabase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    path: str = Field(min_length=1)
    sha256: Digest


class NotebookManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    profile: Profile
    build_id: Annotated[str, StringConstraints(pattern=rf"^{BUILD_ID_PATTERN}$")]
    built_at: datetime
    scenario: ScenarioIdentity
    database: EvidenceDatabase
    prepare_command: str = Field(min_length=1)
    config: dict[str, EvidenceFile] = Field(min_length=1)
    files: dict[str, EvidenceFile] = Field(min_length=1)


class NotebookIndex(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    bundles: dict[Profile, str]


@dataclass(frozen=True)
class NotebookEvidence:
    bundle: ConfigBundle
    paths: dict[str, Path]
    metadata: dict


@dataclass(frozen=True)
class NotebookPublication:
    index_path: Path
    bundles: dict[str, str]


def _controls(repo_root: Path | None = None) -> tuple[Path, PathsConfig, dict]:
    """Resolve the evidence route without interpreting any reader-selected run."""
    root = (repo_root or find_repo_root()).resolve()
    paths_path = root / "config/paths.yaml"
    paths = PathsConfig.model_validate(load_yaml(paths_path))
    # Only paths and the configured preparation command are needed here. Read rules
    # directly rather than loading a production scenario to locate its own results.
    rules_path = root / paths.config.parameters / "rules.yaml"
    rules = load_yaml(rules_path)["parameterization"]["notebook_diagnostics"]
    return root, paths, rules


def prepare_command(repo_root: Path | None = None) -> str:
    _, _, rules = _controls(repo_root)
    return f"uv run python src/build_transport.py --scenario {rules['prepare_scenario']} --overwrite"


def _within(directory: Path, name: str) -> Path:
    """Require stored snapshot references to remain inside their configured root."""
    target = (directory / name.replace("\\", "/")).resolve()
    if not target.is_relative_to(directory.resolve()) or target == directory.resolve():
        raise ValueError(f"Notebook evidence reference escapes its root: {name}")
    return target


def _copy_evidence(source: Path, target: Path, repo_root: Path, relative: str) -> EvidenceFile:
    before = file_sha256(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    compact_csv = source.suffix.lower() == ".csv" and target.name.endswith(".csv.gz")
    if compact_csv:
        # No source name or timestamp in the gzip header: identical prepared
        # bytes produce identical compact snapshots across builds and machines.
        with source.open("rb") as original, target.open("wb") as stored:
            with gzip.GzipFile(filename="", mode="wb", fileobj=stored, mtime=0) as compressed:
                shutil.copyfileobj(original, compressed)
    else:
        shutil.copyfile(source, target)
    stored_hash = file_sha256(target)
    if file_sha256(source) != before or (not compact_csv and stored_hash != before):
        raise ValueError(f"Notebook evidence changed during snapshot: {source}")
    return EvidenceFile(
        path=relative,
        source=(source.relative_to(repo_root).as_posix() if source.is_relative_to(repo_root) else str(source)),
        sha256=stored_hash,
    )


@contextmanager
def _staging_directory(root: Path) -> Iterator[Path]:
    # These are shared review products. Use normal inherited directory access,
    # rather than tempfile's private Windows DACL surviving a directory rename.
    staging = _within(root, ".staging-" + uuid4().hex)
    staging.mkdir()
    try:
        yield staging
    finally:
        if staging.exists():
            shutil.rmtree(_within(root, staging.name))


def stage_notebook_evidence(bundle: ConfigBundle, report: dict) -> NotebookPublication:
    """Snapshot complete prepared inputs; do not advance the successful-build index."""
    profiles = bundle.scenario.notebook_diagnostics
    root = resolve_artifact_path(bundle, "notebook_evidence").resolve()
    root.mkdir(parents=True, exist_ok=True)
    rules = load_harmonization_rules(bundle, "notebook_diagnostics")
    built_at = datetime.fromisoformat(str(report["timestamp_utc"]))
    built_at = (built_at.replace(tzinfo=UTC) if built_at.tzinfo is None else built_at.astimezone(UTC))
    scenario_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", bundle.scenario.scenario.name).strip(".-_")
    base_id = f"{scenario_name or 'scenario'}-{built_at:%Y%m%dT%H%M%SZ}"
    builds = root / "builds"
    builds.mkdir(parents=True, exist_ok=True)
    # Scenario builds are serialized; repeated captures within one second remain
    # immutable and inspectable without an opaque random suffix.
    same_second = [
        int(match[1] or 1) for path in builds.iterdir()
        if (match := re.fullmatch(re.escape(base_id) + r"(?:-([1-9]\d*))?", path.name))
    ]
    sequence = max(same_second, default=0) + 1
    build_id = base_id if sequence == 1 else f"{base_id}-{sequence}"
    release = builds / build_id
    references = {}
    with _staging_directory(root) as staging:
        for profile in profiles:
            if profile == "vehicle_stock":
                from validation.vehicle_stock_diagnostic import collect_stock_inputs
                paths = collect_stock_inputs(bundle)
            else:
                from validation.ldv_efficiency_diagnostic import collect_efficiency_inputs
                paths = collect_efficiency_inputs(bundle)
            missing = [str(path) for path in paths.values() if not path.is_file()]
            if missing:
                raise FileNotFoundError("Cannot snapshot incomplete notebook evidence: " + "; ".join(missing))
            profile_dir = staging / profile
            files = {}
            for key, path in paths.items():
                suffix = path.suffix + (".gz" if path.suffix.lower() == ".csv" else "")
                relative = f"files/{key}{suffix}"
                files[key] = _copy_evidence(path, profile_dir / relative, bundle.repo_root, relative)
            config = {}
            config_sources = {
                "scenario": bundle.scenario_path,
                "paths": bundle.paths_path,
                "sources": bundle.sources_path,
                "rules": resolve_parameter_path(bundle, "rules.yaml"),
                "conversion": resolve_parameter_path(bundle, "conversion.yaml"),
                "mapping": resolve_parameter_path(bundle, load_harmonization_rules(
                    bundle, "road_aggregation",
                )["vehicle_size_class_map_file"]),
            }
            for key, path in config_sources.items():
                relative = (f"config/parameters/{path.name}" if key in {"rules", "conversion", "mapping"}
                            else f"config/{key}.yaml")
                config[key] = _copy_evidence(path, profile_dir / relative, bundle.repo_root, relative)
            manifest = NotebookManifest(
                profile=profile, build_id=build_id, built_at=report["timestamp_utc"],
                scenario=bundle.scenario.scenario,
                database=EvidenceDatabase(path=report["database"], sha256=report["database_sha256"]),
                prepare_command=f"uv run python src/build_transport.py --scenario {rules['prepare_scenario']} --overwrite",
                files=files, config=config,
            )
            write_text_atomic(manifest.model_dump_json(indent=2) + "\n", profile_dir / "manifest.json")
            references[profile] = (release / profile / "manifest.json").relative_to(root).as_posix()
        # Immutable releases can exist before publication, but are undiscoverable
        # until the one small latest index is advanced after SQLite publication.
        staging.rename(release)
    return NotebookPublication(root / rules["latest_file"], references)


def notebook_index_text(publication: NotebookPublication) -> str:
    """Validate/serialize the next index before the builder publishes any outputs."""
    existing = (NotebookIndex.model_validate_json(publication.index_path.read_text(encoding="utf-8")).bundles
                if publication.index_path.is_file() else {})
    index = NotebookIndex(bundles=existing | publication.bundles)
    return index.model_dump_json(indent=2) + "\n"


def publish_notebook_evidence(publication: NotebookPublication) -> None:
    """Publish an already-successful capture outside the standalone database seam."""
    write_text_atomic(notebook_index_text(publication), publication.index_path)


def _reject_links(path: Path) -> None:
    """Deletion must never traverse a symlink, junction or other reparse point."""
    attributes = path.lstat()
    if path.is_symlink() or getattr(attributes, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
        raise ValueError(f"Unsafe linked notebook evidence path: {path}")


def _check_removal_tree(path: Path) -> None:
    _reject_links(path)
    if not path.is_dir():
        raise ValueError(f"Expected notebook evidence directory: {path}")
    # Inspect directories before descent; Path.rglob/os.walk may treat Windows
    # junctions differently from ordinary symbolic links.
    for child in path.iterdir():
        _reject_links(child)
        if child.is_dir():
            _check_removal_tree(child)


def prune_notebook_evidence(publication: NotebookPublication) -> list[Path]:
    """Remove obsolete local captures after successful index publication.

    The latest index is the sole retention authority. Its referenced build IDs
    remain available even when profiles were last prepared by different builds.
    Call only after publication, with scenario builds serialized.
    """
    root = publication.index_path.parent
    _reject_links(root)
    _reject_links(publication.index_path)
    canonical_root = root.resolve()
    index = NotebookIndex.model_validate_json(publication.index_path.read_text(encoding="utf-8"))
    if not index.bundles:
        raise ValueError("Cannot prune notebook evidence without latest build references")
    builds = root / "builds"
    _reject_links(builds)
    keep = set()
    for profile, reference in index.bundles.items():
        parts = reference.replace("\\", "/").split("/")
        if (len(parts) != 4 or parts[0] != "builds" or not re.fullmatch(BUILD_ID_PATTERN, parts[1])
                or parts[2:] != [profile, "manifest.json"]):
            raise ValueError(f"Invalid latest notebook evidence reference: {reference}")
        current = root
        for part in parts:
            current = current / part
            _reject_links(current)
        if not current.resolve().is_relative_to(canonical_root) or not current.is_file():
            raise ValueError(f"Unsafe latest notebook evidence reference: {reference}")
        keep.add(parts[1])

    stale = []
    for candidate in builds.iterdir():
        if not re.fullmatch(BUILD_ID_PATTERN, candidate.name):
            raise ValueError(f"Unexpected notebook evidence build path: {candidate}")
        if candidate.name not in keep:
            stale.append(candidate)
    stale.extend(root.glob(".staging-*"))
    # Complete the bounded path/link audit before deleting any candidate.
    for candidate in stale:
        if candidate.parent not in {root, builds} or not candidate.resolve().is_relative_to(canonical_root):
            raise ValueError(f"Notebook evidence cleanup escapes its root: {candidate}")
        if candidate.parent == root and not re.fullmatch(r"\.staging-[0-9a-f]{32}", candidate.name):
            raise ValueError(f"Unexpected notebook evidence staging path: {candidate}")
        _check_removal_tree(candidate)
    for candidate in stale:
        shutil.rmtree(candidate)
    return stale


def load_notebook_evidence(profile: Profile, *, repo_root: Path | None = None) -> NotebookEvidence:
    """Read the latest recorded bundle; never prepare or reinterpret live artifacts."""
    root, paths, rules = _controls(repo_root)
    directory = resolve_repo_path(root, paths.artifacts["notebook_evidence"].path)
    index_path = directory / rules["latest_file"]
    command = prepare_command(root)
    if not index_path.is_file():
        raise FileNotFoundError(f"No prepared notebook evidence. Run first: {command}")
    index = NotebookIndex.model_validate_json(index_path.read_text(encoding="utf-8"))
    if profile not in index.bundles:
        raise FileNotFoundError(f"No prepared {profile} notebook evidence. Run first: {command}")
    manifest_path = _within(directory, index.bundles[profile])
    manifest = NotebookManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    if manifest.profile != profile:
        raise ValueError(f"Notebook evidence profile mismatch: requested {profile}, recorded {manifest.profile}")
    base = manifest_path.parent
    resolved = {}
    for key, evidence in (manifest.files | {f"config:{key}": value for key, value in manifest.config.items()}).items():
        path = _within(base, evidence.path)
        if not path.is_file() or file_sha256(path) != evidence.sha256:
            raise ValueError(f"Missing or changed recorded notebook evidence: {path}. Rebuild with: {command}")
        resolved[key] = path
    bundle = load_config_bundle(
        resolved["config:scenario"], repo_root=root,
        paths_path=resolved["config:paths"], sources_path=resolved["config:sources"],
    )
    parameter_dir = resolved["config:rules"].parent
    if (resolved["config:conversion"].parent != parameter_dir
            or not resolved["config:mapping"].is_relative_to(parameter_dir)):
        raise ValueError("Recorded notebook parameters must share their verified directory")
    # Bind reads to verified files here, rather than persisting an absolute release
    # location. A moved/copied workspace then reads its own captured parameters.
    recorded_paths = bundle.paths.model_copy(update={
        "config": bundle.paths.config.model_copy(update={"parameters": str(parameter_dir)}),
    })
    bundle = replace(bundle, paths=recorded_paths)
    # Expose only data files to notebook-specific loaders; configuration is carried
    # by the recorded bundle and the manifest retains original source identities.
    metadata = manifest.model_dump(mode="json") | {"manifest": str(manifest_path)}
    return NotebookEvidence(bundle, {key: resolved[key] for key in manifest.files}, metadata)
