"""Pytest runtime configuration and isolated registered-input fixtures."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTEST_TEMP_ROOT = REPO_ROOT / ".pytest-tmp" / str(os.getpid())
PYTEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)

for variable in ("TMPDIR", "TEMP", "TMP"):
    os.environ[variable] = str(PYTEST_TEMP_ROOT)
tempfile.tempdir = str(PYTEST_TEMP_ROOT)


@pytest.fixture(scope="module")
def legacy_bundle():
    """Pinned pre-period-mode selections for existing numerical regression checks."""
    from dataclasses import replace

    from utils import load_config_bundle
    from validation.config_models import ScenarioConfig

    bundle = load_config_bundle("config/scenarios/legacy_comparison.yaml", repo_root=REPO_ROOT)
    payload = bundle.scenario.model_dump(mode="python")
    payload["periods"].update(period_mode="legacy", existing=[2000, 2005, 2010, 2015, 2020, 2023])
    payload["lifetimes"]["survival_curve_max_age"] = 25
    # Pin both later representation selections for the original numerical baseline.
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    payload["BEV_PHEV_range_representation"]["mode"] = "none"
    return replace(bundle, scenario=ScenarioConfig.model_validate(payload))


@pytest.fixture(scope="module")
def isolated_legacy_bundle(legacy_bundle, tmp_path_factory):
    """Replay private prerequisites once; no configured route reaches live artifacts.

    A temporary SQLite destination does not isolate preparation: parameter owners
    also publish interim, processed and validation products. Keep their complete
    input/output namespace private, including direct input-root consumers. Cases
    run serially against private prerequisites rebuilt from registered raw inputs.
    """
    from dataclasses import replace
    import shutil
    import socket

    import yaml

    from build_transport import prepare_scenario_inputs
    from utils import resolve_artifact_path, resolve_configured_path

    directory = tmp_path_factory.mktemp("legacy_repository")
    shutil.copytree(REPO_ROOT / "config", directory / "config")
    shutil.copyfile(REPO_ROOT / "pyproject.toml", directory / "pyproject.toml")
    for key in ("cache", "external", "manual", "template"):
        source = resolve_configured_path(legacy_bundle, "inputs", key)
        destination = directory / legacy_bundle.paths.inputs[key]
        assert destination.resolve().is_relative_to(directory)
        shutil.copytree(source, destination)
    isolated = replace(
        legacy_bundle,
        repo_root=directory,
        paths_path=directory / legacy_bundle.paths_path.relative_to(REPO_ROOT),
        sources_path=directory / legacy_bundle.sources_path.relative_to(REPO_ROOT),
        scenario_path=directory / legacy_bundle.scenario_path.relative_to(REPO_ROOT),
    )
    isolated.scenario_path.write_text(
        yaml.safe_dump(isolated.scenario.model_dump(mode="json", by_alias=True)),
        encoding="utf-8",
    )
    for section in ("inputs", "outputs", "legacy", "config"):
        for key in getattr(isolated.paths, section).model_dump():
            assert resolve_configured_path(isolated, section, key).is_relative_to(directory)
    for family in isolated.paths.artifacts:
        assert resolve_artifact_path(isolated, family).is_relative_to(directory)

    def reject_network(*_args, **_kwargs):
        pytest.fail("Preparation regression tests must use registered offline inputs")

    # Some prerequisite entrypoints reload the scenario relative to the current
    # repository. Keep that discovery private for the complete fixture lifetime.
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(directory)
        patch.setattr(socket.socket, "connect", reject_network)
        patch.setattr(socket, "create_connection", reject_network)
        prepare_scenario_inputs(isolated, download=False)
        yield isolated
