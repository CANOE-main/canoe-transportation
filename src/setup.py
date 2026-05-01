"""Setup smoke validation for the CANOE transport config scaffold."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from parameterization.utils import load_config_bundle, resolve_repo_path
from validation.config_smoke import run_smoke_validation


def write_status(status: dict[str, Any], scenario_path: str | Path) -> Path:
    """Write setup status to the scenario-configured log path."""
    bundle = load_config_bundle(scenario_path)
    output_path = resolve_repo_path(bundle.repo_root, bundle.scenario["outputs"]["setup_log"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/baseline.yaml",
        help="Path to the scenario YAML file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    status = run_smoke_validation(args.scenario, timestamp=datetime.now(UTC))
    output_path = write_status(status, args.scenario)
    if not status["ok"]:
        raise SystemExit(f"setup smoke validation failed; see {output_path}")


if __name__ == "__main__":
    main()
