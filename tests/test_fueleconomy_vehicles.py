from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from fetching.fueleconomy_vehicles import (
    FuelEconomyVehicleRequest,
    build_request,
    module_rules,
    normalize_vehicle_classes,
    read_selected_vehicle_columns,
    validate_cache,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = REPO_ROOT / "config" / "scenarios" / "legacy_reproduction.yaml"


def configured_request() -> FuelEconomyVehicleRequest:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return build_request(bundle)


def write_vehicle_zip(path: Path, rows: pd.DataFrame) -> bytes:
    csv_bytes = rows.to_csv(index=False).encode("utf-8")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("vehicles.csv", csv_bytes)
    return path.read_bytes()


def test_config_pins_official_four_column_vehicle_snapshot() -> None:
    request = configured_request()
    assert request.url == "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"
    assert request.archive_member == "vehicles.csv"
    assert request.expected_bytes == 2_185_627
    assert request.expected_sha256 == (
        "83ee4bf48e65e8e962e55952e0bfbdc6ab94d4bf63f42e2d38aa39143d6f1ecc"
    )
    assert request.required_columns == ("year", "make", "model", "VClass")
    assert request.expected_model_year_from == 1984
    assert request.expected_model_year_to == 2027


def test_request_rejects_nonofficial_download_url() -> None:
    with pytest.raises(ValueError, match="official HTTPS ZIP"):
        request = configured_request()
        FuelEconomyVehicleRequest.model_validate(
            {**request.model_dump(), "url": "https://example.com/vehicles.csv.zip"}
        )


def test_cache_validation_and_reader_select_only_authorized_columns(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "vehicles.csv.zip"
    content = write_vehicle_zip(
        cache,
        pd.DataFrame(
            {
                "year": [2000, 2027],
                "make": ["Honda", "Toyota"],
                "model": ["Civic", "Camry"],
                "VClass": ["Compact Cars", "Midsize Cars"],
                "id": [1, 2],
            }
        ),
    )
    request = configured_request().model_copy(
        update={
            "cache_path": cache,
            "expected_bytes": len(content),
            "expected_sha256": hashlib.sha256(content).hexdigest(),
            "expected_model_year_from": 2000,
            "expected_model_year_to": 2027,
        }
    )
    validate_cache(request)
    selected = read_selected_vehicle_columns(request)
    assert selected.columns.tolist() == ["year", "make", "model", "VClass"]
    assert "id" not in selected


def test_vclass_normalization_maps_known_and_retains_ambiguous_vans() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = module_rules(bundle)
    source = pd.DataFrame(
        {
            "year": [2024, 2024, 1997],
            "make": ["Toyota", "Ford", "Ford"],
            "model": ["RAV4", "F-150", "E150 Econoline 2WD"],
            "VClass": [
                "Small Sport Utility Vehicle 4WD",
                "Standard Pickup Trucks 4WD",
                "Vans",
            ],
        }
    )
    normalized, warnings = normalize_vehicle_classes(source, rules=rules)
    assert normalized["nrcan_vehicle_class"].tolist()[:2] == [
        "Sport utility vehicle: Small",
        "Pickup truck: Standard",
    ]
    assert pd.isna(normalized.loc[2, "nrcan_vehicle_class"])
    assert normalized.loc[2, "class_normalization_status"] == "unresolved"
    assert warnings and warnings[0].startswith("Vans: 1 rows")


def test_vclass_normalization_rejects_unreviewed_class() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = module_rules(bundle)
    source = pd.DataFrame(
        {
            "year": [2026],
            "make": ["Example"],
            "model": ["Example"],
            "VClass": ["Unreviewed Future Class"],
        }
    )
    with pytest.raises(ValueError, match="unexpected VClass"):
        normalize_vehicle_classes(source, rules=rules)
