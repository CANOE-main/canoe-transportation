"""Opt-in, offline import and normalization of registered RAMP run companions.

Charging continues to use the production transformer. Mobility is battery-side
power in W; Usage counts active simulated drivers, not charger occupancy.
"""

import argparse
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fetching.legacy_charging_profiles import (
    SOURCE,
    STRATEGY_LABELS,
    SURVEY_LABELS,
    build_request,
    transform_hourly,
    validate_run_metadata,
)
from utils import (
    ConfigBundle,
    file_sha256,
    find_repo_root,
    load_config_bundle,
    load_parameter_yaml,
    resolve_artifact_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)

LOGGER = logging.getLogger(__name__)


class RegisteredProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    registered_path: str
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0)
    expected_rows: int = Field(gt=0)


class RunCompanions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    mobility: RegisteredProfile
    usage: RegisteredProfile


class Presentation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    default_subset: str
    default_scale: str
    default_week_start: str
    line_width: int = Field(ge=200)
    grid_width: int = Field(ge=200)
    heatmap_width: int = Field(ge=200)
    panel_height: int = Field(ge=100)
    seasons: dict[str, list[int]]

    @model_validator(mode="after")
    def validate_seasons(self):
        if sorted(month for months in self.seasons.values() for month in months) != list(range(1, 13)):
            raise ValueError("Diagnostic seasons must partition the twelve calendar months")
        return self


class DiagnosticSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scenario: str
    surveys: list[str] = Field(min_length=1)
    strategies: list[str] = Field(min_length=1)
    import_case_pattern: str
    columns: dict[str, str]
    mobility_watts_per_kw: float = Field(gt=0)
    precision: int = Field(ge=0, le=15)
    max_lag_hours: int = Field(ge=0, le=168)
    hourly_file: str
    manifest_file: str
    import_manifest_file: str
    presentation: Presentation

    @model_validator(mode="after")
    def validate_catalogue(self):
        if set(self.surveys) != set(SURVEY_LABELS) or len(self.surveys) != len(set(self.surveys)):
            raise ValueError("The diagnostic requires complete, unique registered survey coverage")
        if set(self.strategies) != set(STRATEGY_LABELS) or len(self.strategies) != len(set(self.strategies)):
            raise ValueError("The diagnostic requires complete, unique registered strategy coverage")
        if set(self.columns) != {"charging", "mobility", "usage"}:
            raise ValueError("Diagnostic columns must register charging, mobility and usage")
        return self


@dataclass(frozen=True)
class RunProfileEvidence:
    hourly: pd.DataFrame
    manifest: dict
    settings: DiagnosticSettings


def load_settings(bundle: ConfigBundle) -> DiagnosticSettings:
    return DiagnosticSettings.model_validate(load_parameter_yaml(bundle, "ldv_charging_diagnostic.yaml"))


def normalization_settings(settings: DiagnosticSettings) -> dict:
    """Pin only settings that determine normalized evidence or its file contract."""
    return settings.model_dump(include={
        "surveys", "strategies", "columns", "mobility_watts_per_kw", "precision",
        "hourly_file", "manifest_file",
    })


def registered_companions(bundle: ConfigBundle, settings: DiagnosticSettings) -> dict[str, RunCompanions]:
    payload = bundle.sources.sources[SOURCE].adapter["diagnostic_profiles"]
    expected = {f"{survey}_{strategy}" for survey in settings.surveys for strategy in settings.strategies}
    if set(payload) != expected:
        raise ValueError("Registered RAMP companion catalogue has incomplete or unexpected case coverage")
    return {key: RunCompanions.model_validate(value) for key, value in payload.items()}


def validate_file(path: Path, registration: RegisteredProfile) -> None:
    if path.stat().st_size != registration.expected_bytes or file_sha256(path) != registration.expected_sha256:
        raise ValueError(f"Registered RAMP profile differs from its frozen identity: {path}")


def import_registered_runs(bundle: ConfigBundle, import_root: Path) -> dict[str, int]:
    """Copy explicitly selected external files; refuse to replace changed evidence."""
    settings = load_settings(bundle)
    companions = registered_companions(bundle, settings)
    destination_root = resolve_artifact_path(bundle, "ramp_mobility_external").resolve()
    copied = unchanged = 0
    origins = []
    for survey in settings.surveys:
        for strategy in settings.strategies:
            component = f"{survey}_{strategy}"
            request = build_request(bundle, survey, strategy=strategy)
            # Authenticate the frozen metadata already in the backend before accepting companions.
            validate_run_metadata(request)
            source_directory = import_root / settings.import_case_pattern.format(
                survey=SURVEY_LABELS[survey], strategy=strategy, year=request.calendar_year
            )
            source_metadata = source_directory.parents[1] / "run_metadata.json"
            if file_sha256(source_metadata) != request.metadata_sha256:
                raise ValueError(f"Import directory is not the registered run: {source_metadata}")
            registrations = {
                "charging": RegisteredProfile(
                    registered_path=str(request.registered_path), expected_sha256=request.expected_sha256,
                    expected_bytes=request.expected_bytes, expected_rows=request.expected_rows,
                ),
                **companions[component].model_dump(),
            }
            for kind, payload in registrations.items():
                registration = payload if isinstance(payload, RegisteredProfile) else RegisteredProfile.model_validate(payload)
                destination = resolve_repo_path(bundle.repo_root, registration.registered_path)
                if not destination.resolve().is_relative_to(destination_root):
                    raise ValueError("Registered RAMP import destination escapes its configured artifact root")
                source = source_directory / destination.name
                validate_file(source, registration)
                origins.append({"component": component, "kind": kind,
                                "origin_relative_path": source.relative_to(import_root).as_posix(),
                                "registered_path": registration.registered_path,
                                "sha256": registration.expected_sha256})
                if destination.exists():
                    validate_file(destination, registration)
                    unchanged += 1
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(destination.name + ".importing")
                try:
                    shutil.copyfile(source, temporary)
                    validate_file(temporary, registration)
                    temporary.replace(destination)
                finally:
                    temporary.unlink(missing_ok=True)
                copied += 1
                LOGGER.info("Imported registered %s %s: %s bytes", component, kind, registration.expected_bytes)
    write_text_atomic(json.dumps({"origins": origins, "copied": copied, "unchanged": unchanged,
                      "method": "Every strategy origin authenticated; equal survey companions share canonical physical files"},
                      indent=2, sort_keys=True) + "\n",
                      resolve_artifact_path(bundle, "ramp_run_profiles_validation") / settings.import_manifest_file)
    LOGGER.info("RAMP registered import complete: %s copied, %s unchanged", copied, unchanged)
    return {"copied": copied, "unchanged": unchanged}


def transform_run_profiles(
    raw: dict[str, pd.DataFrame], *, year: int, timezone: str,
    fleet_size: int, settings: DiagnosticSettings,
) -> tuple[pd.DataFrame, dict]:
    """Validate a physical run before any hourly aggregation or unit conversion."""
    if set(raw) != {"charging", "mobility", "usage"} or fleet_size <= 0:
        raise ValueError("A RAMP run requires all three profiles and a positive fleet size")
    if not all(raw[kind].index.equals(raw["charging"].index) for kind in raw):
        raise ValueError("Charging, mobility and usage minute timestamps are not aligned")
    transformed = {
        kind: transform_hourly(frame, year=year, timezone=timezone, value_column=settings.columns[kind], precision=settings.precision)
        for kind, frame in raw.items()
    }
    local = pd.to_datetime(raw["usage"].index, utc=True).tz_convert(timezone)
    usage = raw["usage"].iloc[np.flatnonzero(local.year == year), 0].to_numpy(dtype=float)
    if (usage > fleet_size).any() or not np.equal(usage, np.floor(usage)).all():
        raise ValueError("Usage must count whole active drivers between zero and the simulated fleet")
    charging = transformed["charging"].hourly
    result = charging[["hour_index", "timestamp_utc", "timestamp_local"]].copy()
    result["charging_kw"] = charging.native_hourly_mean
    result["charging_peak_normalized"] = charging.factor
    result["mobility_kw"] = transformed["mobility"].hourly.native_hourly_mean / settings.mobility_watts_per_kw
    result["active_drivers"] = transformed["usage"].hourly.native_hourly_mean
    result["driving_share"] = result.active_drivers / fleet_size
    audit = {
        "fleet_size": fleet_size,
        "conversions": {"mobility_watts_per_kw": settings.mobility_watts_per_kw},
        "profiles": {kind: value.audit for kind, value in transformed.items()},
        "definitions": {
            "charging_kw": "Aggregate grid-side charging power, physical-hour mean (kW)",
            "mobility_kw": "Aggregate temperature-adjusted battery-side mobility power, physical-hour mean (kW)",
            "active_drivers": "Mean simultaneous active-driver count in each physical hour",
            "driving_share": "Mean active-driver count / constructed simulated fleet; not charger occupancy",
        },
    }
    return result, audit


def normalize_registered_runs(bundle: ConfigBundle) -> RunProfileEvidence:
    """Replay the complete diagnostic catalogue offline; never touch production outputs."""
    settings = load_settings(bundle)
    companions = registered_companions(bundle, settings)
    directory = resolve_artifact_path(bundle, "ramp_run_profiles_interim")
    frames, cases = [], {}
    for survey in settings.surveys:
        for strategy in settings.strategies:
            component = f"{survey}_{strategy}"
            request = build_request(bundle, survey, strategy=strategy)
            metadata = validate_run_metadata(request)
            registrations = {
                "charging": RegisteredProfile(
                    registered_path=str(request.registered_path.relative_to(bundle.repo_root)),
                    expected_sha256=request.expected_sha256, expected_bytes=request.expected_bytes,
                    expected_rows=request.expected_rows,
                ),
                "mobility": companions[component].mobility,
                "usage": companions[component].usage,
            }
            raw = {}
            for kind, registration in registrations.items():
                path = resolve_repo_path(bundle.repo_root, registration.registered_path)
                validate_file(path, registration)
                raw[kind] = pd.read_csv(path, index_col=0)
                if len(raw[kind]) != registration.expected_rows:
                    raise ValueError(f"Registered {component} {kind} row count differs")
            hourly, audit = transform_run_profiles(
                raw, year=request.calendar_year, timezone=request.calendar_timezone,
                fleet_size=metadata["preflight"]["constructed_users"], settings=settings,
            )
            hourly["survey"] = survey
            hourly["strategy"] = strategy
            hourly["component"] = component
            output = directory / settings.hourly_file.format(component=component)
            write_dataframe_atomic(hourly, output)
            cases[component] = {
                "sources": {kind: value.model_dump() for kind, value in registrations.items()},
                "metadata_sha256": request.metadata_sha256,
                "model_revision": request.expected_model_revision,
                "calendar_year": request.calendar_year,
                "calendar_timezone": request.calendar_timezone,
                "hourly_sha256": file_sha256(output),
                "hourly_rows": len(hourly),
                "audit": audit,
            }
            frames.append(hourly)
            LOGGER.info("Normalized %s: %s physical hours; outside-year exclusions %s; mobility W/kW=%s",
                        component, len(hourly), audit["profiles"]["charging"]["excluded_outside_local_year"],
                        settings.mobility_watts_per_kw)
    manifest = {"source": SOURCE, "normalization_settings": normalization_settings(settings), "cases": cases,
                "production_readiness": "opt-in diagnostic; not a production prerequisite"}
    write_text_atomic(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                      resolve_artifact_path(bundle, "ramp_run_profiles_validation") / settings.manifest_file)
    return RunProfileEvidence(pd.concat(frames, ignore_index=True), manifest, settings)


def validate_bundle(bundle: ConfigBundle) -> RunProfileEvidence:
    """Authenticate normalized evidence and the registered files without writing."""
    settings = load_settings(bundle)
    companions = registered_companions(bundle, settings)
    path = resolve_artifact_path(bundle, "ramp_run_profiles_validation") / settings.manifest_file
    prepare = "Run uv run --offline python -m fetching.ramp_run_profiles from the repository root."
    if not path.is_file():
        raise FileNotFoundError(f"Missing normalized RAMP evidence at {path}. {prepare}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("normalization_settings") != normalization_settings(settings) or set(manifest["cases"]) != set(companions):
        raise ValueError(f"RAMP diagnostic evidence is stale/incomplete. {prepare}")
    frames = []
    for survey in settings.surveys:
        for strategy in settings.strategies:
            component = f"{survey}_{strategy}"
            request = build_request(bundle, survey, strategy=strategy)
            validate_run_metadata(request)
            case = manifest["cases"][component]
            if case["metadata_sha256"] != request.metadata_sha256:
                raise ValueError("RAMP diagnostic metadata registration changed")
            expected = {"mobility": companions[component].mobility, "usage": companions[component].usage,
                        "charging": RegisteredProfile(registered_path=str(request.registered_path.relative_to(bundle.repo_root)),
                            expected_sha256=request.expected_sha256, expected_bytes=request.expected_bytes,
                            expected_rows=request.expected_rows)}
            if case["sources"] != {kind: value.model_dump() for kind, value in expected.items()}:
                raise ValueError("RAMP diagnostic source registration changed")
            for registration in expected.values():
                validate_file(resolve_repo_path(bundle.repo_root, registration.registered_path), registration)
            hourly_path = resolve_artifact_path(bundle, "ramp_run_profiles_interim") / settings.hourly_file.format(component=component)
            if not hourly_path.is_file():
                raise FileNotFoundError(f"Missing normalized RAMP profile at {hourly_path}. {prepare}")
            if file_sha256(hourly_path) != case["hourly_sha256"]:
                raise ValueError(f"RAMP normalized hourly evidence differs from its manifest. {prepare}")
            hourly = pd.read_csv(hourly_path)
            if len(hourly) != case["hourly_rows"] or not hourly.component.eq(component).all():
                raise ValueError("RAMP normalized hourly evidence has invalid coverage")
            frames.append(hourly)
    return RunProfileEvidence(pd.concat(frames, ignore_index=True), manifest, settings)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/validation_baseline.yaml")
    parser.add_argument("--import-root", type=Path, help="Explicit RAMP results/reruns directory; no simulation or acquisition")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    bundle = load_config_bundle(args.scenario, repo_root=find_repo_root())
    if args.import_root is not None:
        import_registered_runs(bundle, args.import_root.resolve())
    evidence = normalize_registered_runs(bundle)
    print(json.dumps({"cases": len(evidence.manifest["cases"]), "hourly_rows": len(evidence.hourly)}))


if __name__ == "__main__":
    main()
