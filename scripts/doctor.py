"""Live-download-free repository doctor for the CANOE transportation backend."""

from __future__ import annotations

import argparse
import importlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SCENARIO = "config/scenarios/legacy_reproduction.yaml"
REQUIRED_IMPORTS = ("fetching", "parameterization", "utils", "validation")


@dataclass(frozen=True)
class DoctorResult:
    """Structured doctor result for tests and CLI reporting."""

    ok: bool
    checks: dict[str, Any]
    errors: list[str]


def find_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking up to pyproject.toml."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not find repository root containing pyproject.toml")


def prepare_import_path(repo_root: Path) -> None:
    """Make src imports available when running this script directly."""
    src_path = str((repo_root / "src").resolve())
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def import_required_packages() -> dict[str, str]:
    """Import required local packages and return their module files."""
    imported: dict[str, str] = {}
    for package in REQUIRED_IMPORTS:
        module = importlib.import_module(package)
        imported[package] = str(getattr(module, "__file__", "namespace"))
    return imported


def configured_generated_directories(bundle: Any) -> list[Path]:
    """Return generated directories the doctor may create with --create-dirs."""
    from utils import resolve_repo_path

    inputs = bundle.paths["inputs"]
    outputs = bundle.paths["outputs"]
    paths = (
        inputs["interim"],
        inputs["processed"],
        outputs["sqlite"],
        outputs["validation"],
        outputs["logs"],
    )
    return [resolve_repo_path(bundle.repo_root, path) for path in paths]


def check_directory_state(paths: list[Path], *, create_dirs: bool) -> list[dict[str, Any]]:
    """Inspect generated directories without mutating unless requested."""
    states: list[dict[str, Any]] = []
    for path in paths:
        existed_before = path.exists()
        if create_dirs:
            path.mkdir(parents=True, exist_ok=True)
        states.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "created": create_dirs and not existed_before and path.exists(),
                "parent_exists": path.parent.exists(),
            }
        )
    return states


def run_doctor(
    scenario_path: str | Path = DEFAULT_SCENARIO,
    *,
    repo_root: Path | None = None,
    create_dirs: bool = False,
) -> DoctorResult:
    """Run live-download-free repository checks."""
    root = (repo_root or find_repo_root()).resolve()
    prepare_import_path(root)

    from utils import load_config_bundle, resolve_repo_path, validate_config_bundle

    errors: list[str] = []
    checks: dict[str, Any] = {"repo_root": str(root), "mutated": create_dirs}

    try:
        imports = import_required_packages()
        checks["imports"] = imports
    except Exception as exc:  # pragma: no cover - exercised through failing CLI use
        errors.append(f"import check failed: {exc}")

    try:
        bundle = load_config_bundle(scenario_path, repo_root=root)
        config_errors = validate_config_bundle(bundle)
        errors.extend(config_errors)
        checks["config"] = {
            "paths": str(bundle.paths_path),
            "sources": str(bundle.sources_path),
            "scenario": str(bundle.scenario_path),
            "validation_errors": config_errors,
        }
        schema_path = resolve_repo_path(bundle.repo_root, bundle.paths["inputs"]["schema"])
        reference_sqlite = resolve_repo_path(bundle.repo_root, bundle.scenario["validation"]["reference_sqlite"])
        checks["paths"] = {
            "schema": str(schema_path),
            "schema_exists": schema_path.exists(),
            "reference_sqlite": str(reference_sqlite),
            "reference_sqlite_exists": reference_sqlite.exists(),
        }
        checks["generated_directories"] = check_directory_state(
            configured_generated_directories(bundle),
            create_dirs=create_dirs,
        )
    except Exception as exc:
        errors.append(f"config/path check failed: {exc}")

    return DoctorResult(ok=not errors, checks=checks, errors=errors)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO, help="Scenario YAML to validate.")
    parser.add_argument("--repo-root", type=Path, default=None, help="Repository root. Defaults to auto-discovery.")
    parser.add_argument(
        "--create-dirs",
        action="store_true",
        help="Create missing generated directories. Default only inspects paths.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_doctor(args.scenario, repo_root=args.repo_root, create_dirs=args.create_dirs)
    print(f"doctor ok={result.ok} mutated={result.checks.get('mutated', False)}")
    for package, location in result.checks.get("imports", {}).items():
        print(f"import\t{package}\t{location}")
    for directory in result.checks.get("generated_directories", []):
        print(
            "directory\t"
            f"{directory['path']}\t"
            f"exists={directory['exists']}\t"
            f"created={directory['created']}\t"
            f"parent_exists={directory['parent_exists']}"
        )
    for error in result.errors:
        print(f"error\t{error}", file=sys.stderr)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
