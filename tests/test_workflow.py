"""Execute the production DAG with fixture entrypoints, without production ETL."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"
DATABASE = "outputs/sqlite/canoe_transport_legacy_reproduction.sqlite"
REPORT = "outputs/validation/legacy_reproduction_database_bootstrap.json"
CER_TABLE = "inputs/1_interim/fetched_cer_energy_future/2026/cer_macro_indicators.csv"
CER_CACHE = "inputs/0_cache/cer_energy_future/2026/macro-indicators-2026.csv"

FIXTURE_ENTRYPOINT = """
import argparse
import json
from pathlib import Path
import sqlite3
import sys

from fetching.cer_enerfuture import build_requests as cer_requests, configured_edition
from fetching.statcan_tables import build_requests as statcan_requests
from utils import load_config_bundle, load_harmonization_rules

parser = argparse.ArgumentParser()
parser.add_argument("stage")
parser.add_argument("--scenario", required=True)
args, _ = parser.parse_known_args()
bundle = load_config_bundle(args.scenario)
with Path("events.jsonl").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args.stage) + "\\n")

def source_outputs(stage):
    rules = load_harmonization_rules(bundle, stage)
    if stage == "cer_enerfuture":
        subdir = rules["interim_subdir_template"].format(edition=configured_edition(bundle))
        files = [request.output_file for request in cer_requests(bundle)]
    else:
        subdir = rules["interim_subdir"]
        files = [request.output_file for request in statcan_requests(bundle)]
        files += [rules["ldv_history"]["output_file"], rules["ldv_history"]["overlap_file"],
                  rules["freight"]["output_file"]]
    return [Path(bundle.paths.inputs.interim) / subdir / filename
            for filename in files + [rules["manifest_file"], rules["warnings_file"]]]

if args.stage in ("cer_enerfuture", "statcan_tables"):
    for path in source_outputs(args.stage):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture source\\n", encoding="utf-8")
elif args.stage == "database":
    assert all(path.is_file() for stage in ("cer_enerfuture", "statcan_tables")
               for path in source_outputs(stage)), "assembly ran before normalization"
    database = Path(bundle.paths.outputs.sqlite) / bundle.scenario.outputs.sqlite_name
    report = Path(bundle.scenario.outputs.validation_report)
    database.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    if Path("fail_database").exists():
        assert database.is_file() and report.is_file(), "prior outputs were removed"
        database.write_bytes(b"failed replacement")
        report.write_bytes(b"failed replacement")
        sys.exit(17)
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS fixture (value TEXT)")
        connection.execute("INSERT INTO fixture VALUES ('complete')")
    report.write_text('{"ok": true}\\n', encoding="utf-8")
"""


@pytest.fixture
def workflow_repo(tmp_path: Path) -> Path:
    from fetching.cer_enerfuture import build_requests as cer_requests
    from fetching.statcan_tables import build_requests as statcan_requests
    from utils import load_config_bundle

    for directory in ("config", "src", "workflow", "scripts"):
        shutil.copytree(REPO_ROOT / directory, tmp_path / directory)
    shutil.copytree(
        REPO_ROOT / "inputs/0_manual_params", tmp_path / "inputs/0_manual_params"
    )
    shutil.copytree(
        REPO_ROOT / "inputs/0_canoe_template", tmp_path / "inputs/0_canoe_template"
    )
    for filename in ("pyproject.toml", "uv.lock"):
        shutil.copyfile(REPO_ROOT / filename, tmp_path / filename)
    import yaml

    scenario_path = tmp_path / SCENARIO
    payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    payload["embodied_emissions"] = False
    scenario_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    bundle = load_config_bundle(SCENARIO, repo_root=tmp_path)
    cache_paths = [request.cache_path for request in cer_requests(bundle)] + [
        path
        for request in statcan_requests(bundle)
        for path in (request.archive_cache_path, request.metadata_cache_path)
    ]
    for path in cache_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture cache\n", encoding="utf-8")

    # Keep the production rule graph and IO flags; replace only invoked entrypoints.
    snakefile_path = tmp_path / "workflow/Snakefile"
    snakefile = snakefile_path.read_text(encoding="utf-8")
    python = sys.executable.replace("\\", "/")
    snakefile = snakefile.replace(
        '["uv", "run", "python", "scripts/doctor.py",',
        f'[{python!r}, "fixture_stage.py", "doctor",',
    )
    for original, stage in (
        ("uv run python -m fetching.statcan_tables ", "statcan_tables"),
        ("uv run python -m fetching.cer_enerfuture ", "cer_enerfuture"),
        ("uv run python src/build_transport.py ", "database"),
    ):
        assert original in snakefile
        snakefile = snakefile.replace(
            f'"{original}"',
            repr(f'"{python}" fixture_stage.py {stage} '),
        )
    snakefile_path.write_text(snakefile, encoding="utf-8")
    (tmp_path / "fixture_stage.py").write_text(FIXTURE_ENTRYPOINT, encoding="utf-8")
    return tmp_path


def run_workflow(
    root: Path, *extra: str, succeeds: bool = True
) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "snakemake",
            "--snakefile",
            "workflow/Snakefile",
            "--cores",
            "1",
            *extra,
            "--config",
            f"scenario={SCENARIO}",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    if succeeds:
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        assert result.returncode != 0, result.stdout + result.stderr
    return result


def events(root: Path) -> list[str]:
    path = root / "events.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_workflow_sequences_sources_reuses_outputs_and_rebuilds_missing_table(
    workflow_repo,
):
    root = workflow_repo
    run_workflow(root)
    first_events = events(root)
    assert set(first_events) == {
        "doctor",
        "cer_enerfuture",
        "statcan_tables",
        "database",
    }
    assert first_events[-1] == "database"
    assert (root / DATABASE).is_file()
    assert (root / REPORT).is_file()

    run_workflow(root)
    assert events(root) == first_events

    (root / CER_TABLE).unlink()
    run_workflow(root)
    assert events(root)[len(first_events) :] == ["cer_enerfuture", "database"]


@pytest.mark.parametrize(
    ("changed", "expected"),
    [
        (
            "config/parameters/conversion.yaml",
            {"statcan_tables", "database"},
        ),
        ("src/parameterization/build_costs.py", {"database"}),
        ("src/fetching/nlr_atb_autonomie.py", {"database"}),
        ("src/fetching/fueleconomy_vehicles.py", {"database"}),
        (CER_CACHE, {"cer_enerfuture", "database"}),
        ("inputs/0_manual_params/lifetime_process.csv", {"doctor", "database"}),
        ("inputs/0_canoe_template/region.csv", {"doctor", "database"}),
    ],
)
def test_workflow_invalidates_changed_prerequisites(workflow_repo, changed, expected):
    root = workflow_repo
    run_workflow(root)
    previous = len(events(root))
    path = root / changed
    path.write_text(
        path.read_text(encoding="utf-8") + "\n# fixture change\n", encoding="utf-8"
    )
    run_workflow(root)
    assert set(events(root)[previous:]) == expected
    assert events(root)[-1] == "database"


def test_workflow_restores_previous_publication_when_writer_fails(workflow_repo):
    root = workflow_repo
    run_workflow(root)
    previous = {
        filename: (root / filename).read_bytes() for filename in (DATABASE, REPORT)
    }
    (root / "fail_database").touch()
    run_workflow(root, "--forcerun", "transport_database", succeeds=False)
    assert {
        filename: (root / filename).read_bytes() for filename in previous
    } == previous


def test_offline_workflow_rejects_missing_cache_before_running_jobs(workflow_repo):
    root = workflow_repo
    (root / CER_CACHE).unlink()
    result = run_workflow(root, "--dry-run", succeeds=False)
    assert "MissingInputException" in result.stdout + result.stderr
    assert not (root / "events.jsonl").exists()
    assert not (root / DATABASE).exists()


def test_embodied_workbooks_are_conditional_and_invalidate_only_assembly(workflow_repo):
    import yaml

    from fetching.greet_vehicle_cycle import required_inputs
    from utils import load_config_bundle

    root = workflow_repo
    assert required_inputs(load_config_bundle(SCENARIO, repo_root=root)) == []
    run_workflow(root, "--dry-run")
    scenario_path = root / SCENARIO
    payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    payload["embodied_emissions"] = True
    scenario_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    result = run_workflow(root, "--dry-run", succeeds=False)
    assert "MissingInputException" in result.stdout + result.stderr
    assert "GREET" in result.stdout + result.stderr
    workbooks = required_inputs(load_config_bundle(SCENARIO, repo_root=root))
    assert len(workbooks) == 8  # Two models, complete five-file bank, and ATB archetypes.
    for path in workbooks:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture saved workbook\n", encoding="utf-8")
    run_workflow(root)
    previous = len(events(root))
    workbooks[1].write_text("changed fixture saved workbook\n", encoding="utf-8")
    run_workflow(root)
    assert events(root)[previous:] == ["database"]
