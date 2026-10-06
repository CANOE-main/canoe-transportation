"""Pytest runtime configuration for Windows-safe local temp paths."""

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

    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml", repo_root=REPO_ROOT)
    payload = bundle.scenario.model_dump(mode="python")
    payload["periods"].update(period_mode="legacy", existing=[2000, 2005, 2010, 2015, 2020, 2023])
    payload["lifetimes"]["survival_curve_max_age"] = 25
    # Original numerical baseline predates the independently selected charging slice.
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    return replace(bundle, scenario=ScenarioConfig.model_validate(payload))
