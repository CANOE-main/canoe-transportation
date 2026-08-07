from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from fetching.vehicle_population import (
    CkanLookupRequest,
    OntarioVehiclePopulationRequest,
    build_request,
    build_requests,
    current_stock_input,
    discover_ckan_resources,
    fetch_to_cache,
    module_rules,
    normalize_report_a,
    report_a_cohort_usability,
    normalize_report4,
    normalize_report5,
    read_report_from_zip,
    read_vehicle_population_txt,
    resolve_archive_member,
    resolve_zip_member_name,
    select_ckan_resource,
    validate_source,
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


def test_discover_ckan_resources_sorts_years_and_reports_gaps() -> None:
    metadata = package_metadata(
        [
            {
                "id": "zip-2025",
                "name": "Vehicle population data - 2025",
                "format": "ZIP",
                "url": "https://example.test/2025.zip",
            },
            {
                "id": "dictionary",
                "name": "Data dictionary",
                "format": "XLSX",
                "url": "https://example.test/dictionary.xlsx",
            },
            {
                "id": "zip-2023",
                "name": "Vehicle population data - 2023",
                "format": "ZIP",
                "url": "https://example.test/2023.zip",
            },
        ]
    )

    resources = discover_ckan_resources(
        metadata,
        selector={
            "format": "zip",
            "year_fields": ["name", "url"],
            "year_pattern": r"(?<!\d)(20\d{2})(?!\d)",
        },
    )

    assert [resource.year for resource in resources] == [2023, 2025]


def test_build_request_uses_configured_ckan_metadata_and_latest_year() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    metadata = package_metadata(
        [
            {
                "id": "resource-2022",
                "name": "Vehicle population data 2022",
                "format": "ZIP",
                "url": "https://example.test/vehicle-population-2022.zip",
            },
            {
                "id": "resource-2025",
                "name": "Vehicle population data 2025",
                "format": "ZIP",
                "url": "https://example.test/vehicle-population-2025.zip",
            },
        ]
    )

    request = build_request(bundle, package_metadata=metadata)

    assert request.year == 2025
    assert request.package_id == "vehicle-population-data"
    assert request.cache_path == REPO_ROOT / "inputs" / "0_cache" / "ontario_vehicle_population" / "2025_vehicle_population_data.zip"
    assert request.report_a_members[0] == "2025_Reg_Veh_ReportA_Class&Make&Model&Year.TXT"
    assert request.report4_member == "2025_Reg_Veh_Report4_Weight_Class&Status.TXT"
    assert request.report5_member == "2025_Reg_Veh_Report5_Class&Status&Descriptors.TXT"


def test_build_requests_excludes_2015_edition() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    metadata = package_metadata(
        [
            {
                "id": f"resource-{year}",
                "name": f"Vehicle population data {year}",
                "format": "ZIP",
                "url": f"https://example.test/vehicle-population-{year}.zip",
            }
            for year in [2014, 2015, 2016]
        ]
    )

    requests, gaps = build_requests(bundle, metadata)

    assert [request.year for request in requests] == [2014, 2016]
    assert gaps == []
    with pytest.raises(ValueError, match="excluded by configuration"):
        build_requests(bundle, metadata, year=2015)


def test_lookup_request_rejects_invalid_override_before_metadata_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="greater than 1900"):
        CkanLookupRequest(
            base_url="https://data.ontario.ca",
            package_id="vehicle-population-data",
            year=-1,
            selector={"format": "zip"},
            cache_path=(tmp_path / "vehicle.zip").resolve(),
        )

    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    def fail_fetch(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("metadata I/O should not run")

    monkeypatch.setattr(
        "fetching.vehicle_population.fetch_ckan_package_metadata", fail_fetch
    )
    with pytest.raises(ValueError, match="greater than 1900"):
        build_request(bundle, year=-1)


def test_ontario_rules_load_paths_and_extraction_parameters_from_config() -> None:
    rules = ontario_rules()

    assert rules["interim_subdir"] == "fetched_ontario_vehicle_population"
    assert rules["reports"]["A"]["passenger_class"] == "PASSENGER"
    assert rules["reports"]["A"]["excluded_years"] == [2015]
    assert rules["reports"]["A"]["kept_vehicle_classes"] == [
        "PASSENGER",
        "COMMERCIAL",
    ]
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


def test_resolve_report_a_from_one_nested_zip(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "vehicle_population.zip"
    nested_bytes = tmp_path / "VPS.zip"
    member = "2017_Reg_Veh_ReportA_Class&Make&Model&Year.TXT"
    with ZipFile(nested_bytes, "w") as nested:
        nested.writestr(member, "VEHICLE-CLASS\tMAKE\nPASSENGER\tACUR\n")
    with ZipFile(archive, "w") as outer:
        outer.write(nested_bytes, "VPS.zip")

    resolved = resolve_archive_member(
        archive,
        (
            "2017_Reg_Veh_ReportA_Class&Make&Model&Year.TXT",
        ),
        max_depth=1,
    )

    assert resolved.display_name == f"VPS.zip!{member}"
    assert read_report_from_zip(archive, resolved).loc[0, "MAKE"] == "ACUR"


def test_source_validation_reports_missing_required_member(tmp_path: Path) -> None:
    archive = tmp_path / "vehicle_population.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.writestr("report4.txt", "header\n")
        zip_file.writestr("report_a.txt", "header\n")
    request = OntarioVehiclePopulationRequest(
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        package_id="vehicle-population-data",
        resource_id="resource-2022",
        resource_name="Vehicle population 2022",
        url="https://example.test/vehicle-population-2022.zip",
        cache_path=archive.resolve(),
        report_a_members=("report_a.txt",),
        report4_member="report4.txt",
        report5_member="report5.txt",
    )

    with pytest.raises(FileNotFoundError, match="report5.txt"):
        validate_source(request)


def test_normalize_report_a_preserves_statuses_and_reconciles_long_table() -> None:
    raw = pd.DataFrame(
        {
            "VEHICLE-CLASS": ["PASSENGER", "PASSENGER", "COMMERCIAL", "BUS"],
            "MAKE": ["ACUR", "****", "FORD", "BUS"],
            "MODEL": ["RDX", "***", "F15", "001"],
            "MODEL-YEAR": ["2022", "2023", "1990", "2020"],
            "FIT-ACTIVE": ["10", "1", "5", "99"],
            "FIT-INACTIVE": ["2", "0", "1", "0"],
            "UNFIT": ["0", "0", "0", "0"],
            "WRECKED": ["0", "0", "0", "0"],
            "OUT-OF-PROV": ["0", "0", "0", "0"],
            "SOLD": ["0", "0", "0", "0"],
            "SUSPENDED": ["0", "0", "0", "0"],
            "TEMPORARY": ["0", "0", "0", "0"],
            "TOTAL": ["12", "1", "6", "99"],
        }
    )

    normalized, status_long, reconciliation, findings = normalize_report_a(
        raw,
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        raw_file="2022_Reg_Veh_ReportA_Class&Make&Model&Year.TXT",
        cached_zip=Path("cache.zip"),
        rules=ontario_rules(),
    )

    assert normalized["MODEL_YEAR"].tolist() == [2022, 2023, 1990]
    assert set(status_long["stock_status"]) == {
        "FIT_ACTIVE",
        "FIT_INACTIVE",
        "UNFIT",
        "WRECKED",
        "OUT_OF_PROV",
        "SOLD",
        "SUSPENDED",
        "TEMPORARY",
        "TOTAL",
    }
    assert reconciliation["reconciled"].all()
    assert set(normalized["VEHICLE_CLASS"]) == {"PASSENGER", "COMMERCIAL"}
    assert {
        "model_year_after_report_year",
        "pre_2000_stock",
        "stock_over_age_30",
        "suppressed_make_code",
        "suppressed_model_code",
        "excluded_vehicle_class",
    } <= set(findings["issue_type"])
    usable, reason = report_a_cohort_usability(normalized, rules=ontario_rules())
    assert usable
    assert "available" in reason


def test_report_a_all_suppressed_passenger_keys_are_not_cohort_usable() -> None:
    raw = pd.DataFrame(
        {
            "VEHICLE-CLASS": ["PASSENGER", "COMMERCIAL"],
            "MAKE": ["****", "****"],
            "MODEL": ["***", "***"],
            "MODEL-YEAR": ["2016", "2016"],
            "FIT-ACTIVE": ["7", "9"],
            "FIT-INACTIVE": ["2", "2"],
            "UNFIT": ["0", "0"],
            "WRECKED": ["0", "0"],
            "OUT-OF-PROV": ["0", "0"],
            "SOLD": ["0", "0"],
            "SUSPENDED": ["0", "0"],
            "TEMPORARY": ["0", "0"],
            "TOTAL": ["9", "11"],
        }
    )

    normalized, _, _, findings = normalize_report_a(
        raw,
        source_id="ontario_ministry_transport_vehicle_population",
        year=2016,
        raw_file="2016_Reg_Veh_ReportA_Class&Make&Model&Year.TXT",
        cached_zip=Path("cache.zip"),
        rules=ontario_rules(),
    )
    usable, reason = report_a_cohort_usability(normalized, rules=ontario_rules())

    assert not usable
    assert "exclude from cohort transitions" in reason
    assert "cohort_snapshot_unusable" in set(findings["issue_type"])


def test_current_stock_input_keeps_passenger_and_commercial() -> None:
    normalized = pd.DataFrame(
        {
            "source_id": ["source", "source"],
            "report_year": [2025, 2025],
            "VEHICLE_CLASS": ["PASSENGER", "COMMERCIAL"],
            "MAKE": ["TOYT", "FORD"],
            "MODEL": ["RAV", "COF"],
            "MODEL_YEAR": [2024, 2024],
            "FIT_ACTIVE": [100, 200],
            "FIT_INACTIVE": [0, 0],
            "UNFIT": [0, 0],
            "WRECKED": [0, 0],
            "OUT_OF_PROV": [0, 0],
            "SOLD": [0, 0],
            "SUSPENDED": [0, 0],
            "TEMPORARY": [0, 0],
            "TOTAL": [100, 200],
        }
    )

    current = current_stock_input(normalized, rules=ontario_rules())

    assert set(current["VEHICLE_CLASS"]) == {"PASSENGER", "COMMERCIAL"}
    assert current.groupby("VEHICLE_CLASS")["FIT_ACTIVE"].sum().to_dict() == {
        "COMMERCIAL": 200,
        "PASSENGER": 100,
    }


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
        {"VEHICLE_CLASS": "PASSENGER", "VALUE": 1991, "AGE": 31, "FIT-ACTIVE": 100},
    ]
    passenger = distribution[distribution["VEHICLE_CLASS"] == "PASSENGER"].sort_values("AGE")
    assert passenger["AGE_DIST"].tolist() == pytest.approx([0.25, 0.75])


def test_fetch_to_cache_reuses_existing_zip_without_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_path = tmp_path / "vehicle_population.zip"
    with ZipFile(cache_path, "w") as archive:
        archive.writestr("report_a.txt", "header\n")
    request = OntarioVehiclePopulationRequest(
        source_id="ontario_ministry_transport_vehicle_population",
        year=2022,
        package_id="vehicle-population-data",
        resource_id="resource-2022",
        resource_name="Vehicle population 2022",
        url="https://example.test/vehicle-population-2022.zip",
        cache_path=cache_path,
        report_a_members=("report_a.txt",),
        report4_member="report4.txt",
        report5_member="report5.txt",
    )

    def fail_get(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("requests.get should not be called for cached files")

    monkeypatch.setattr("fetching.vehicle_population.requests.get", fail_get)

    assert fetch_to_cache(request) == "cached"
