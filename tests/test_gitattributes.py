"""Actual Git checkout checks for registered bytes and portable text files."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def git(root: Path, *args: str) -> bytes:
    environment = os.environ.copy()
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"):
        environment.pop(name, None)
    environment.update({
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_ATTR_NOSYSTEM": "1",
    })
    return subprocess.check_output(
        ["git", "-c", f"core.attributesFile={os.devnull}", *args],
        cwd=root, env=environment, stderr=subprocess.PIPE,
    )


@pytest.fixture(scope="module")
def registered_blobs() -> dict[str, tuple[bytes, str]]:
    """Resolve the byte-pinned, versioned sources from their owning registry."""
    try:
        checkout_root = Path(git(REPO_ROOT, "rev-parse", "--show-toplevel").decode("utf-8").strip()).resolve()
    except subprocess.CalledProcessError:
        pytest.skip("Checkout conversion requires this repository's Git index; this source copy has no Git checkout")
    if checkout_root != REPO_ROOT:
        pytest.skip("Checkout conversion requires this repository's Git index; the enclosing checkout owns a different root")
    sources = yaml.safe_load((REPO_ROOT / "config/sources.yaml").read_text(encoding="utf-8"))["sources"]
    tracked = set(git(REPO_ROOT, "ls-files").decode("utf-8").splitlines())
    pins: dict[str, str] = {}
    for source in sources.values():
        adapters = [source.get("adapter", {}), *[
            component.get("adapter", {}) for component in source.get("components", {}).values()
        ]]
        for adapter in adapters:
            for path_key, hash_key in (
                ("registered_path", "expected_sha256"),
                ("metadata_path", "metadata_sha256"),
                ("extract_path", "expected_sha256"),
                ("parent_path", "parent_sha256"),
                ("notebook_path", "notebook_sha256"),
            ):
                path = adapter.get(path_key)
                if path in tracked and hash_key in adapter:
                    pins[path] = adapter[hash_key]
            for asset in adapter.get("assets", []):
                # The frozen HATCH originals also exist as versioned legacy
                # evidence; their cache copies have the same registered bytes.
                matches = [path for path in tracked
                           if "/HATCH_dataset/" in path
                           and Path(path).name == Path(asset["cache_path"]).name]
                for path in matches:
                    pins[path] = asset["sha256"]
    # Production profiles, run metadata, diagnostic companions and the compact
    # OMEGA extract are versioned inputs: exercise their complete byte contracts.
    # Optional parent/notebook and HATCH evidence are not necessarily tracked.
    profile_components = sources["legacy_charging_profiles"]["components"]
    required_components = {
        f"{survey}_{strategy}"
        for survey in ("nhts", "tts")
        for strategy in ("uncontrolled", "night", "day")
    }
    assert set(profile_components) == required_components, "Reconcile the supported registered charging profiles"
    required_paths = {
        profile_components[name]["adapter"][path_key]
        for name in required_components
        for path_key in ("registered_path", "metadata_path")
    }
    diagnostic_profiles = sources["legacy_charging_profiles"]["adapter"]["diagnostic_profiles"]
    assert set(diagnostic_profiles) == required_components, "Reconcile the supported diagnostic profile companions"
    for companions in diagnostic_profiles.values():
        assert set(companions) == {"mobility", "usage"}
        for adapter in companions.values():
            path = adapter["registered_path"]
            required_paths.add(path)
            if path in tracked:
                pins[path] = adapter["expected_sha256"]
    required_paths.add(sources["epa_omega_baseline"]["components"]["baseline_sales_ranges"]["adapter"]["extract_path"])
    assert required_paths <= pins.keys(), f"Required registered byte contracts are not versioned/pinned: {sorted(required_paths - pins.keys())}"
    for path, digest in pins.items():
        assert hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest() == digest, f"Registered working source changed: {path}"
    # Use actual stored blobs, not local files whose cached Git statistics may
    # hide a conversion mismatch. The current attributes must restore registered
    # native serialization from the bytes collaborators will check out.
    return {path: (git(REPO_ROOT, "show", f":{path}"), digest) for path, digest in pins.items()}


@pytest.mark.parametrize("autocrlf", ["false", "true", "input"])
def test_checkout_preserves_registered_bytes_and_portable_formats(
    tmp_path: Path, registered_blobs: dict[str, tuple[bytes, str]], autocrlf: str,
) -> None:
    """Exercise Git's actual clean/smudge conversion in a disposable index."""
    git(tmp_path, "init", "--quiet")
    git(tmp_path, "config", "core.autocrlf", autocrlf)
    git(tmp_path, "config", "core.longpaths", "true")
    (tmp_path / ".gitattributes").write_bytes((REPO_ROOT / ".gitattributes").read_bytes())
    files = {path: content for path, (content, _) in registered_blobs.items()}
    for suffix in ("py", "sh", "ps1", "yaml", "yml", "toml", "json", "md", "csv", "sql", "mjs"):
        files[f"portable/sample.{suffix}"] = b"first\r\nsecond\r\n"
    for suffix in ("bat", "cmd"):
        files[f"windows/sample.{suffix}"] = b"first\nsecond\n"
    binary = b"binary\x00header\r\nunchanged\n"
    for suffix in ("sqlite", "sqlite3", "db", "xls", "xlsx", "xlsm", "xla", "pptx", "pdf", "png", "jpg", "jpeg", "zip", "gz"):
        files[f"binary/sample.{suffix}"] = binary
    for name, content in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    git(tmp_path, "add", "--all")
    for name in files:
        (tmp_path / name).unlink()
    git(tmp_path, "checkout-index", "--all", "--force")
    for name, (_, digest) in registered_blobs.items():
        assert hashlib.sha256((tmp_path / name).read_bytes()).hexdigest() == digest, name
    for name in files:
        content = (tmp_path / name).read_bytes()
        if name.startswith("portable/"):
            assert content == b"first\nsecond\n", name
        elif name.startswith("windows/"):
            assert content == b"first\r\nsecond\r\n", name
        elif name.startswith("binary/"):
            assert content == binary, name
