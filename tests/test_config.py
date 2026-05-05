from pathlib import Path

from parameterization.utils import (
    configured_directories,
    load_config_bundle,
    resolve_repo_path,
    validate_config_bundle,
)
from validation.config_smoke import run_smoke_validation


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def test_config_bundle_loads() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert bundle.paths["inputs"]["schema"] == "inputs/canoe_dataset_schema.sql"
    assert bundle.scenario["scenario"]["name"] == "legacy_reproduction"
    assert bundle.scenario["model_years"]["years"] == [2021, 2025, 2030, 2035, 2040, 2045, 2050]
    assert "sources" in bundle.sources


def test_required_config_keys_are_present() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    assert validate_config_bundle(bundle) == []


def test_active_sources_are_registered() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    registered_sources = bundle.sources["sources"]

    for source_name in bundle.scenario["active_sources"]:
        assert source_name in registered_sources
        assert registered_sources[source_name]["status"] == "pending"


def test_path_resolution_and_directory_list() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    schema_path = resolve_repo_path(bundle.repo_root, bundle.paths["inputs"]["schema"])
    directories = configured_directories(bundle)

    assert schema_path == REPO_ROOT / "inputs" / "canoe_dataset_schema.sql"
    assert schema_path.exists()
    assert REPO_ROOT / "outputs" / "logs" in directories


def test_setup_smoke_status_does_not_download_or_compile() -> None:
    status = run_smoke_validation(SCENARIO)

    assert status["ok"] is True
    assert status["scenario"] == "legacy_reproduction"
    assert status["model_years"] == {"years": [2021, 2025, 2030, 2035, 2040, 2045, 2050]}
    assert status["schema_exists"] is True
    assert status["reference_sqlite_exists"] is True
    assert status["non_goals"] == {
        "download_sources": False,
        "transform_parameters": False,
        "compile_sqlite": False,
    }
