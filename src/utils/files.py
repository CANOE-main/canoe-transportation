"""Small shared mechanics for deterministic local file publication."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

import pandas as pd


def file_sha256(path: Path) -> str:
    """Return a streaming lowercase SHA-256 digest for one file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_dataframe_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Publish a CSV by atomic same-directory replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_csv(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
