"""Persist each scenario build attempt in its own timestamped log."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import gmtime

from utils import ConfigBundle, file_sha256, resolve_repo_path


LOGGER = logging.getLogger(__name__)


def _build_log_path(bundle: ConfigBundle) -> Path:
    """Validate the log destination before any directory or file is created."""
    def filename_token(value: str) -> str:
        token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
        if not token:
            raise ValueError("Scenario and YAML names must contain filename-safe characters")
        return token

    filename = bundle.paths.outputs.build_log.format(
        scenario_name=filename_token(bundle.scenario.scenario.name),
        scenario_yaml=filename_token(bundle.scenario_path.stem),
        timestamp=datetime.now(UTC).strftime("%Y-%m-%d_%H-%M-%S-%fZ"),
    )
    log_path = resolve_repo_path(
        bundle.repo_root, filename,
    ).resolve()
    log_root = resolve_repo_path(bundle.repo_root, bundle.paths.outputs.logs).resolve()
    if log_path == log_root or not log_path.is_relative_to(log_root):
        raise ValueError(f"Scenario build log must be a file within {log_root}: {log_path}")

    protected = {
        "scenario database": resolve_repo_path(
            bundle.repo_root, bundle.paths.outputs.sqlite,
        ) / bundle.scenario.outputs.sqlite_name,
        "validation report": resolve_repo_path(
            bundle.repo_root, bundle.scenario.outputs.validation_report,
        ),
        "setup log": resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.setup_log),
        "paths configuration": bundle.paths_path,
        "source configuration": bundle.sources_path,
        "scenario configuration": bundle.scenario_path,
    }
    if reference := bundle.scenario.comparison.reference_sqlite:
        protected["comparison reference"] = resolve_repo_path(bundle.repo_root, reference)
    for label, path in protected.items():
        path = path.resolve()
        if log_path == path or (
            log_path.exists() and path.exists() and log_path.samefile(path)
        ):
            raise ValueError(f"Scenario build log must differ from {label}: {path}")

    parameter_root = resolve_repo_path(
        bundle.repo_root, bundle.paths.config.parameters,
    ).resolve()
    if log_path.is_relative_to(parameter_root):
        raise ValueError(
            f"Scenario build log must not replace parameter configuration: {log_path}"
        )
    return log_path


@contextmanager
def scenario_build_log(bundle: ConfigBundle, *, debug: bool = False) -> Iterator[Path]:
    """Persist nested module logs and restore the caller's logging on every exit."""
    log_path = _build_log_path(bundle)
    yaml_path = bundle.scenario_path.resolve()
    yaml_sha256 = file_sha256(yaml_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="x", encoding="utf-8")
    verbosity = logging.DEBUG if debug else logging.INFO
    handler.setLevel(verbosity)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    formatter.converter = gmtime
    handler.setFormatter(formatter)
    root = logging.getLogger()
    original_level = root.level
    scenario = bundle.scenario.scenario.name
    level_name = logging.getLevelName(verbosity)
    try:
        root.addHandler(handler)
        root.setLevel(min(original_level, verbosity))
        LOGGER.info(
            "Scenario build started: scenario=%s yaml=%s yaml_sha256=%s verbosity=%s log=%s",
            scenario, yaml_path, yaml_sha256, level_name, log_path,
        )
        try:
            yield log_path
        except BaseException:
            LOGGER.exception(
                "Scenario build failed: scenario=%s verbosity=%s", scenario, level_name,
            )
            raise
        else:
            LOGGER.info(
                "Scenario build completed: scenario=%s verbosity=%s", scenario, level_name,
            )
    finally:
        root.removeHandler(handler)
        root.setLevel(original_level)
        handler.close()
