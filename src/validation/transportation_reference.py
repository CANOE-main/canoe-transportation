"""Identity checks for curated historical transportation comparison inputs."""

import json
from pathlib import Path

from utils import file_sha256


def read_reference_manifest(directory: Path, filename: str) -> dict:
    """Verify the retained bytes before any historical comparison consumes them."""
    root = directory.resolve()
    manifest = root / filename
    if Path(filename).name != filename or manifest.resolve().parent != root:
        raise ValueError(f"Invalid transportation reference manifest path: {filename}")
    evidence = json.loads(manifest.read_text(encoding="utf-8"))
    if (not isinstance(evidence, dict) or evidence.get("version") != 1
            or not isinstance(evidence.get("files"), dict) or not evidence["files"]):
        raise ValueError(f"Unsupported transportation reference manifest: {manifest}")
    for name, record in evidence["files"].items():
        path = root / name
        if Path(name).name != name or path.resolve().parent != root:
            raise ValueError(f"Transportation reference file escapes its directory: {name}")
        if not isinstance(record, dict) or not path.is_file() or file_sha256(path) != record.get("sha256"):
            raise ValueError(f"Curated transportation reference differs from its manifest: {path}")
    return evidence
