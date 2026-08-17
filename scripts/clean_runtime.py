"""Safely clean local runtime and generated repository artifacts."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


SCRIPT_REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_SRC = str(SCRIPT_REPO_ROOT / "src")
if SCRIPT_SRC not in sys.path:
    sys.path.insert(0, SCRIPT_SRC)

RUNTIME_GLOBS = (
    ".pytest_cache",
    ".pytest-basetemp*",
    ".pytest-cache-runtime*",
    ".pytest-runtime*",
    ".pytest-tmp*",
    ".ruff_cache",
)
SNAKEMAKE_RUNTIME = (
    ".snakemake/locks",
    ".snakemake/metadata",
    ".snakemake/incomplete",
    ".snakemake/iocache",
)
GENERATED_PATHS = (
    "inputs/1_interim",
    "inputs/2_processed",
    "inputs/validation",
    "outputs",
)
CACHE_PATHS = ("inputs/0_cache",)


@dataclass(frozen=True)
class CleanupTarget:
    """One cleanup target selected by category."""

    path: Path
    category: str


@dataclass(frozen=True)
class CleanupResult:
    """Result for one cleanup target."""

    path: Path
    category: str
    action: str
    reason: str = ""


def find_repo_root(start: Path | None = None) -> Path:
    """Delegate repository discovery to the shared package utility."""
    from utils import find_repo_root as shared_find_repo_root

    return shared_find_repo_root(start)


def resolve_under_root(repo_root: Path, path: Path) -> Path:
    """Resolve a path and ensure it stays inside the repository."""
    resolved = path.resolve()
    if resolved == repo_root:
        raise ValueError("Refusing to clean repository root")
    if repo_root not in (resolved, *resolved.parents):
        raise ValueError(f"Refusing to clean path outside repository: {resolved}")
    return resolved


def tracked_files(repo_root: Path) -> set[Path]:
    """Return git-tracked files, or an empty set if git is unavailable."""
    try:
        completed = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=repo_root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return set()
    names = completed.stdout.decode("utf-8", errors="replace").split("\0")
    return {(repo_root / name).resolve() for name in names if name}


def contains_tracked_file(path: Path, tracked: set[Path]) -> bool:
    """Return true if a cleanup target contains any git-tracked file."""
    if path.is_file():
        return path.resolve() in tracked
    return any(path == tracked_file or path in tracked_file.parents for tracked_file in tracked)


def existing_glob_targets(repo_root: Path, pattern: str, category: str) -> list[CleanupTarget]:
    """Collect existing targets from a repo-root glob pattern."""
    targets: list[CleanupTarget] = []
    for candidate in repo_root.glob(pattern):
        if candidate.exists():
            targets.append(CleanupTarget(resolve_under_root(repo_root, candidate), category))
    return targets


def existing_literal_targets(repo_root: Path, paths: tuple[str, ...], category: str) -> list[CleanupTarget]:
    """Collect existing targets from literal repo-relative paths."""
    targets: list[CleanupTarget] = []
    for item in paths:
        candidate = repo_root / item
        if candidate.exists():
            targets.append(CleanupTarget(resolve_under_root(repo_root, candidate), category))
    return targets


def build_cleanup_plan(
    repo_root: Path,
    *,
    include_generated: bool = False,
    include_cache: bool = False,
) -> list[CleanupTarget]:
    """Build cleanup targets without deleting anything."""
    root = repo_root.resolve()
    targets: list[CleanupTarget] = []
    for pattern in RUNTIME_GLOBS:
        targets.extend(existing_glob_targets(root, pattern, "runtime"))
    targets.extend(existing_literal_targets(root, SNAKEMAKE_RUNTIME, "runtime"))
    if include_generated:
        targets.extend(existing_literal_targets(root, GENERATED_PATHS, "generated"))
    if include_cache:
        targets.extend(existing_literal_targets(root, CACHE_PATHS, "cache"))

    unique: dict[Path, CleanupTarget] = {}
    for target in targets:
        unique.setdefault(target.path, target)
    return sorted(unique.values(), key=lambda target: str(target.path))


def remove_target(path: Path) -> None:
    """Remove one file, symlink, or directory."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def run_cleanup(
    targets: list[CleanupTarget],
    *,
    dry_run: bool,
    tracked: set[Path],
) -> list[CleanupResult]:
    """Run or preview cleanup targets."""
    results: list[CleanupResult] = []
    for target in targets:
        if contains_tracked_file(target.path, tracked):
            results.append(
                CleanupResult(
                    target.path,
                    target.category,
                    "skipped",
                    "contains git-tracked files",
                )
            )
            continue
        if dry_run:
            results.append(CleanupResult(target.path, target.category, "would_remove"))
            continue
        try:
            remove_target(target.path)
        except OSError as exc:
            results.append(CleanupResult(target.path, target.category, "failed", str(exc)))
            continue
        results.append(CleanupResult(target.path, target.category, "removed"))
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=None, help="Repository root. Defaults to auto-discovery.")
    parser.add_argument("--apply", action="store_true", help="Actually remove selected targets. Default is dry-run.")
    parser.add_argument(
        "--include-generated",
        action="store_true",
        help="Also clean generated backend outputs: inputs/1_interim, inputs/2_processed, and outputs.",
    )
    parser.add_argument(
        "--include-cache",
        action="store_true",
        help="Also clean fetched upstream cache under inputs/0_cache. Never includes inputs/0_external_models.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = (args.repo_root or find_repo_root()).resolve()
    targets = build_cleanup_plan(
        repo_root,
        include_generated=args.include_generated,
        include_cache=args.include_cache,
    )
    results = run_cleanup(targets, dry_run=not args.apply, tracked=tracked_files(repo_root))
    mode = "dry-run" if not args.apply else "apply"
    print(f"clean_runtime mode={mode} targets={len(results)}")
    for result in results:
        suffix = f" ({result.reason})" if result.reason else ""
        print(f"{result.action}\t{result.category}\t{result.path}{suffix}")
    return 1 if any(result.action == "failed" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
