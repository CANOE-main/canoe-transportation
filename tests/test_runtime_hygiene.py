from pathlib import Path
import importlib.util
import sys
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def load_script_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


clean_runtime = load_script_module("clean_runtime", REPO_ROOT / "scripts" / "clean_runtime.py")
doctor = load_script_module("doctor", REPO_ROOT / "scripts" / "doctor.py")


def touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")


def test_cleanup_plan_separates_runtime_generated_cache_and_external(tmp_path: Path) -> None:
    touch(tmp_path / ".pytest_cache" / "nodeids")
    touch(tmp_path / ".pytest-basetemp-example" / "file.txt")
    touch(tmp_path / ".pytest-tmp-example" / "file.txt")
    touch(tmp_path / ".snakemake" / "locks" / "0.input.lock")
    touch(tmp_path / "inputs" / "1_interim" / "generated.csv")
    touch(tmp_path / "inputs" / "2_processed" / "processed.csv")
    touch(tmp_path / "inputs" / "validation" / "review.csv")
    touch(tmp_path / "outputs" / "logs" / "run.log")
    touch(tmp_path / "inputs" / "0_cache" / "source.xls")
    touch(tmp_path / "inputs" / "0_external_models" / "model.csv")

    default_targets = {target.path.relative_to(tmp_path).as_posix() for target in clean_runtime.build_cleanup_plan(tmp_path)}
    generated_targets = {
        target.path.relative_to(tmp_path).as_posix()
        for target in clean_runtime.build_cleanup_plan(tmp_path, include_generated=True)
    }
    cache_targets = {
        target.path.relative_to(tmp_path).as_posix()
        for target in clean_runtime.build_cleanup_plan(tmp_path, include_cache=True)
    }

    assert ".pytest_cache" in default_targets
    assert ".pytest-basetemp-example" in default_targets
    assert ".pytest-tmp-example" in default_targets
    assert ".snakemake/locks" in default_targets
    assert "inputs/1_interim" not in default_targets
    assert "outputs" not in default_targets
    assert "inputs/0_cache" not in default_targets
    assert "inputs/0_external_models" not in generated_targets
    assert {
        "inputs/1_interim",
        "inputs/2_processed",
        "inputs/validation",
        "outputs",
    }.issubset(generated_targets)
    assert "inputs/0_cache" in cache_targets
    assert "inputs/0_external_models" not in cache_targets


def test_cleanup_dry_run_does_not_remove_targets(tmp_path: Path) -> None:
    touch(tmp_path / ".ruff_cache" / "cache")
    targets = clean_runtime.build_cleanup_plan(tmp_path)

    results = clean_runtime.run_cleanup(targets, dry_run=True, tracked=set())

    assert results[0].action == "would_remove"
    assert (tmp_path / ".ruff_cache").exists()


def test_cleanup_skips_tracked_targets(tmp_path: Path) -> None:
    tracked_file = tmp_path / "outputs" / "logs" / "tracked.log"
    touch(tracked_file)
    targets = clean_runtime.build_cleanup_plan(tmp_path, include_generated=True)

    results = clean_runtime.run_cleanup(targets, dry_run=False, tracked={tracked_file.resolve()})

    assert results[0].action == "skipped"
    assert "tracked" in results[0].reason
    assert tracked_file.exists()


def test_doctor_runs_without_mutating_by_default() -> None:
    result = doctor.run_doctor(SCENARIO, repo_root=REPO_ROOT)

    assert result.ok is True
    assert result.checks["mutated"] is False
    assert set(result.checks["imports"]) == {"fetching", "parameterization", "utils", "validation"}
    assert result.checks["paths"]["schema_package"]["package"] == "canoe-schema"
    assert result.checks["manual_parameters"] == {
        "files": [
            "cost_invest_multipliers.csv",
            "cost_variable_multipliers.csv",
                "efficiency_multipliers.csv",
                "lifetime_process.csv",
                "vehicle_class_market_shares.csv",
            ],
            "file_count": 5,
            "component_count": 11,
            "selected_cited_rows": 84,
        }


def test_pytest_defaults_do_not_pin_runtime_directories() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pytest_options = config["tool"]["pytest"]["ini_options"]

    assert "cache_dir" not in pytest_options
    addopts = pytest_options.get("addopts", [])
    assert all("--basetemp" not in option for option in addopts)


def test_snakefile_keeps_coarse_source_and_single_writer_build_layers() -> None:
    snakefile = (REPO_ROOT / "workflow" / "Snakefile").read_text(encoding="utf-8")

    assert "load_config_bundle" in snakefile
    assert "SCENARIO.scenario.outputs.sqlite_name" in snakefile
    assert "SCENARIO.scenario.outputs.validation_report" in snakefile
    assert "SCENARIO.scenario.switches.download_sources" in snakefile
    assert "configured_edition" in snakefile
    assert "scripts/doctor.py" in snakefile
    assert "fetching.statcan_tables" in snakefile
    assert "STATCAN_RULES['interim_subdir']" in snakefile
    assert "rule transport_database" in snakefile
    assert "src/build_transport.py" in snakefile
    assert "{params.scenario:q}" in snakefile
    assert "fetching.vehicle_population" not in snakefile
    assert "configfile:" not in snakefile
