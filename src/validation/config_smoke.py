"""Smoke validation for the YAML configuration control layer."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from parameterization.utils import (
    create_configured_directories,
    load_config_bundle,
    resolve_repo_path,
    validate_config_bundle,
)


def run_smoke_validation(
    scenario_path: str | Path,
    *,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Load configs, validate required keys, create directories, and return status."""
    bundle = load_config_bundle(scenario_path)
    errors = validate_config_bundle(bundle)
    now = timestamp or datetime.now(UTC)
    if errors:
        return {
            "ok": False,
            "timestamp_utc": now.isoformat(),
            "scenario_path": str(bundle.scenario_path),
            "errors": errors,
        }

    created_directories = create_configured_directories(bundle)
    schema_path = resolve_repo_path(bundle.repo_root, bundle.paths["inputs"]["schema"])
    reference_sqlite = resolve_repo_path(bundle.repo_root, bundle.scenario["validation"]["reference_sqlite"])
    active_sources = bundle.scenario["active_sources"]
    sources = bundle.sources["sources"]
    placeholder_sources = [
        source_name
        for source_name in active_sources
        if sources[source_name].get("status") == "placeholder"
    ]

    return {
        "ok": True,
        "timestamp_utc": now.isoformat(),
        "scenario": bundle.scenario["scenario"]["name"],
        "model_years": bundle.scenario["model_years"],
        "scenario_path": str(bundle.scenario_path),
        "paths_path": str(bundle.paths_path),
        "sources_path": str(bundle.sources_path),
        "created_directories": [str(path) for path in created_directories],
        "schema_exists": schema_path.exists(),
        "schema_path": str(schema_path),
        "reference_sqlite_exists": reference_sqlite.exists(),
        "reference_sqlite": str(reference_sqlite),
        "active_sources": active_sources,
        "placeholder_sources": placeholder_sources,
        "non_goals": {
            "download_sources": False,
            "transform_parameters": False,
            "compile_sqlite": False,
        },
    }
