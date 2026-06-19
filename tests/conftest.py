"""Pytest runtime configuration for Windows-safe local temp paths."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTEST_TEMP_ROOT = REPO_ROOT / ".pytest-tmp" / str(os.getpid())
PYTEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)

for variable in ("TMPDIR", "TEMP", "TMP"):
    os.environ[variable] = str(PYTEST_TEMP_ROOT)
tempfile.tempdir = str(PYTEST_TEMP_ROOT)
