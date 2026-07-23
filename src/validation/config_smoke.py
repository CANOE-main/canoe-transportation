"""Smoke validation for the YAML configuration control layer."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from utils import (
    create_configured_directories,
    load_config_bundle,
    resolve_repo_path,
    validate_config_bundle,
)
from validation.schema_contract import schema_evidence


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
    reference_value = bundle.scenario.validation.reference_sqlite
    reference_sqlite = (
        resolve_repo_path(bundle.repo_root, reference_value)
        if reference_value is not None
        else None
    )
    active_sources = bundle.scenario.sources.active
    sources = bundle.sources.sources
    placeholder_sources = [
        source_name
        for source_name in active_sources
        if sources[source_name].get("status") == "placeholder"
    ]

    return {
        "ok": True,
        "timestamp_utc": now.isoformat(),
        "scenario": bundle.scenario.scenario.name,
        "periods": bundle.scenario.periods.model_dump(mode="json"),
        "currency": bundle.scenario.currency.model_dump(mode="json"),
        "economics": bundle.scenario.economics.model_dump(mode="json"),
        "scenario_path": str(bundle.scenario_path),
        "paths_path": str(bundle.paths_path),
        "sources_path": str(bundle.sources_path),
        "created_directories": [str(path) for path in created_directories],
        "packaged_schema": schema_evidence(),
        "reference_sqlite_exists": (
            reference_sqlite.exists() if reference_sqlite is not None else None
        ),
        "reference_sqlite": (
            str(reference_sqlite) if reference_sqlite is not None else None
        ),
        "active_sources": active_sources,
        "placeholder_sources": placeholder_sources,
        "switches": bundle.scenario.switches.model_dump(mode="json"),
        "planned": bundle.scenario.planned.model_dump(mode="json"),
    }
