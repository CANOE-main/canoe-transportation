from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from fetching.vehicle_population import (
    OntarioVehiclePopulationRequest,
    build_request,
    fetch_to_cache,
    module_rules,
    normalize_report4,
    normalize_report5,
    read_report_from_zip,
    read_vehicle_population_txt,
    resolve_zip_member_name,
    select_ckan_resource,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def ontario_rules() -> dict[str, object]:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return module_rules(bundle)


def package_metadata(resources: list[dict[str, str]]) -> dict[str, object]:
    return {"success": True, "result": {"resources": resources}}


def test_select_ckan_resource_matches_year_and_zip_format() -> None:
    metadata = package_metadata(
        [
            {"id": "csv-2022", "name": "2022 CSV", "format": "CSV", "url": "https://example.test/2022.csv"},
            {"id": "zip-2021", "name": "Vehicle population 2021", "format": "ZIP", "url": "https://example.test/2021.zip"},
            {"id": "zip-2022", "name": "Vehicle population 2022", "format": "ZIP", "url": "https://example.test/2022.zip"},
        ]
    )

    resource = select_ckan_resource(
        metadata,
        year=2022,
        selector={"format": "zip", "year_fields": ["name", "url"]},
    )

    assert resource["id"] == "zip-2022"


def test_select_ckan_resource_rejects_ambiguous_matches() -> None:
    metadata = package_metadata(
        [
            {"id": "a", "name": "Vehicle population 2022", "format": "ZIP", "url": "https://example.test/a.zip"},
            {"id": "b", "name": "Vehicle population 2022", "format": "ZIP", "url": "https://example.test/b.zip"},
        ]
    )

    with pytest.raises(ValueError, match="Multiple CKAN resources"):
        select_ckan_resource(metadata, year=2022, selector={"format": "zip", "year_fields": ["name"]})


def test_build_request_uses_configured_ckan_metadata_and_default_year() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    metadata = package_metadata(
        [
            {
                "id": "resource-2022",
                "name": "Vehicle population data 2022",
                "format": "ZIP",
                "url": "https://example.test/vehicle-population-2022.zip",
            }
        ]
    )

    request = build_request(bundle, package_metadata=metadata)

    assert request.year == 2022
    assert request.package_id == "vehicle-population-data"
    assert request.cache_path == REPO_ROOT / "inputs" / "0_cache" / "ontario_vehicle_population" / "2022_vehicle_population_data.zip"
    assert request.report4_member == "2022_Reg_Veh_Report4_Weight_Class&Status.TXT"
    assert request.report5_member == "2022_Reg_Veh_Report5_Class&Status&Descriptors.TXT"


def test_ontario_rules_load_paths_and_extraction_parameters_from_config() -> None:
    rules = ontario_rules()

    assert rules["interim_subdir"] == "fetched_ontario_vehicle_population"
    assert rules["reports"][4]["kept_weight_class"] == "COMMERCIAL"
    assert rules["reports"][5]["max_age"] == 30


def test_read_vehicle_population_txt_and_zip_members(tmp_path: Path) -> None:
    report = tmp_path / "2022_Reg_Veh_Report4_Weight_Class&Status.TXT"
    report.write_text("WEIGHT_CLASS\tKG_FROM\tKG_TO\tFIT-ACTIVE\nCOMMERCIAL\t0\t2000\t10\n", encoding="utf-8")
    archive = tmp_path / "vehicle_population.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.write(report, f"2022/{report.name}")

    from_txt = read_vehicle_population_txt(report)
    from_zip = read_report_from_zip(archive, report.name)

    assert resolve_zip_member_name(archive, report.name) == f"2022/{report.name}"
    assert from_txt.to_dict("records") == from_zip.to_dict("records")
    assert from_zip.loc[0, "FIT-ACTIVE"] == "10"


def test_normalize_report4_assigns_epa_gvwr_bins_and_distribution() -> None:
    raw = pd.DataFrame(
        {
            "WEIGHT_CLASS": ["COMMERCIAL", "COMMERCIAL", "PASSENGER", "COMMERCIAL"],
            "KG_FROM": ["0", "2001", "0", "15000"],
            "KG_TO": ["2000", "3000", "2000", "16000"],
            "FIT-ACTIVE": ["10", "20", "999", "5"],
        }
    )

    cleaned, distribution = normalize_report4(
        raw,
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        raw_file="report4.txt",
        cached_zip=Path("cache.zip"),
        rules=ontario_rules(),
        kg_to_lb=2.20462,
    )

    assert cleaned["EPA_GVWR"].astype(str).tolist() == ["LDT1-2", "LDT3-4", "MDV8"]
    counts = dict(zip(distribution["EPA_GVWR"].astype(str), distribution["NATIVE_COUNT"], strict=True))
    assert counts["LDT1-2"] == 10
    assert counts["LDT3-4"] == 20
    assert counts["MDV8"] == 5
    assert distribution["SHARE"].sum() == pytest.approx(1.0)


def test_normalize_report5_calculates_age_and_class_normalized_shares() -> None:
    raw = pd.DataFrame(
        {
            "VEHICLE_CLASS": ["PASSENGER", "PASSENGER", "PASSENGER", "COMMERCIAL", "BUS", "TRAILER"],
            "DESCRIPTOR": ["YEAR", "YEAR", "YEAR", "YEAR", "MOTIVE POWER", "YEAR"],
            "VALUE": ["2022", "2021", "1991", "2020", "D", "2022"],
            "FIT-ACTIVE": ["10", "30", "100", "8", "99", "7"],
        }
    )

    cleaned, distribution = normalize_report5(
        raw,
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        raw_file="report5.txt",
        cached_zip=Path("cache.zip"),
        rules=ontario_rules(),
    )

    assert cleaned[["VEHICLE_CLASS", "VALUE", "AGE", "FIT-ACTIVE"]].to_dict("records") == [
        {"VEHICLE_CLASS": "COMMERCIAL", "VALUE": 2020, "AGE": 2, "FIT-ACTIVE": 8},
        {"VEHICLE_CLASS": "PASSENGER", "VALUE": 2022, "AGE": 0, "FIT-ACTIVE": 10},
        {"VEHICLE_CLASS": "PASSENGER", "VALUE": 2021, "AGE": 1, "FIT-ACTIVE": 30},
    ]
    passenger = distribution[distribution["VEHICLE_CLASS"] == "PASSENGER"].sort_values("AGE")
    assert passenger["AGE_DIST"].tolist() == pytest.approx([0.25, 0.75])


def test_fetch_to_cache_reuses_existing_zip_without_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_path = tmp_path / "vehicle_population.zip"
    cache_path.write_bytes(b"already cached")
    request = OntarioVehiclePopulationRequest(
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        package_id="vehicle-population-data",
        resource_id="resource-2022",
        resource_name="Vehicle population 2022",
        url="https://example.test/vehicle-population-2022.zip",
        cache_path=cache_path,
        report4_member="report4.txt",
        report5_member="report5.txt",
    )

    def fail_get(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("requests.get should not be called for cached files")

    monkeypatch.setattr("fetching.vehicle_population.requests.get", fail_get)

    assert fetch_to_cache(request) == "cached"
