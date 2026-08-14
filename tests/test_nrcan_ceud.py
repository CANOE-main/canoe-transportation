import hashlib
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

import fetching.nrcan_ceud as nrcan_ceud
from fetching.nrcan_ceud import (
    CeudTableRequest,
    FuelConsumptionRatingRequest,
    clean_label,
    extract_unit,
    fetch_rating_to_cache,
    iter_rating_requests,
    iter_table_requests,
    module_rules,
    normalize_rating_dataframe,
    normalize_ceud_dataframe,
    ratings_rules,
    read_rating_csv,
    render_ceud_url,
    validate_rating_cache,
    validate_source,
)
from utils import load_config_bundle, resolve_input_path
from validation.config_models import SourceComponent


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def nrcan_rules() -> dict[str, object]:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return module_rules(bundle)


def rating_request(
    path: Path,
    *,
    content: bytes = b"fixture",
    years: tuple[int, ...] = (2025,),
    required_columns: tuple[str, ...] = (
        "Model year",
        "Make",
        "Model",
        "Vehicle class",
        "Combined (L/100 km)",
    ),
) -> FuelConsumptionRatingRequest:
    return FuelConsumptionRatingRequest(
        source_id="nrcan_fuel_consumption_ratings",
        component_key="fixture",
        component_meta=SourceComponent(
            label="Fixture ratings",
            short_name="fixture_ratings",
            version="2025-01-01",
        ),
        resource_id="00000000-0000-0000-0000-000000000000",
        resource_title="Fixture ratings",
        url=(
            "https://open.canada.ca/data/dataset/package/resource/"
            "00000000-0000-0000-0000-000000000000/download/fixture.csv"
        ),
        cache_path=path.resolve(),
        expected_md5=hashlib.md5(content, usedforsecurity=False).hexdigest(),
        expected_bytes=len(content),
        expected_model_years=years,
        required_columns=required_columns,
        required_non_null_columns=required_columns,
        output_file="fixture.csv",
    )


def test_render_ceud_url_uses_year_region_and_table_id() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    source = bundle.sources["sources"]["nrcan_ceud_transport_provincial"]

    url = render_ceud_url(source, year=2021, region="ON", table_id=20)

    assert url.endswith("/2021/tran_on_e_20.xls")


def test_iter_table_requests_uses_raw_cache_names() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    requests = iter_table_requests(bundle, regions=["ON"], include_national=False)

    assert requests
    assert requests[0].cache_path.parent == REPO_ROOT / "inputs" / "0_cache" / "nrcan_ceud_transport"
    assert requests[0].cache_path.name.startswith("2021_tran_on_e_")


def test_iter_rating_requests_pins_exact_english_resources() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)

    requests = iter_rating_requests(bundle)

    assert [request.component_key for request in requests] == [
        "battery_electric_2012_2026",
        "plug_in_hybrid_2012_2026",
        "fuel_consumption_1995_2014",
        "fuel_consumption_2015_2024",
        "fuel_consumption_2025",
        "fuel_consumption_2026",
    ]
    assert [request.resource_id for request in requests] == [
        "026e45b4-eb63-451f-b34f-d9308ea3a3d9",
        "8812228b-a6aa-4303-b3d0-66489225120d",
        "42495676-28b7-40f3-b0e0-3d7fe005ca56",
        "c98b9dc8-b23f-4cd8-8b19-e892da1e4688",
        "d589f2bc-9a85-4f65-be2f-20f17debfcb1",
        "9df1b18d-d036-4783-a61c-99f1f75b3ac5",
    ]
    assert all("open.canada.ca" in request.url for request in requests)
    assert all(
        request.cache_path.parent
        == REPO_ROOT / "inputs" / "0_cache" / "nrcan_fuel_consumption_ratings"
        for request in requests
    )
    assert requests[0].expected_model_years == tuple(range(2012, 2027))
    assert requests[1].expected_model_years == tuple(range(2012, 2027))
    assert requests[2].expected_model_years == tuple(range(1995, 2015))
    assert requests[-1].expected_model_years == (2026,)


def test_request_validation_rejects_before_file_or_network_io(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="URL"):
        CeudTableRequest(
            source_id="nrcan_ceud_transport_provincial",
            region="on",
            output_region="ON",
            year=2021,
            table_id=20,
            table_meta={"label": "Cars", "short_name": "cars"},
            url="not-a-url",
            cache_path=(tmp_path / "table.xls").resolve(),
        )

    valid = CeudTableRequest(
        source_id="nrcan_ceud_transport_provincial",
        region="on",
        output_region="ON",
        year=2021,
        table_id=20,
        table_meta={"label": "Cars", "short_name": "cars"},
        url="https://example.test/table.xls",
        cache_path=(tmp_path / "missing.xls").resolve(),
    )
    with pytest.raises(FileNotFoundError, match="nrcan_ceud_transport_provincial/20"):
        validate_source(valid)


def test_rating_request_validation_rejects_before_network_io(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="official HTTPS CSV"):
        FuelConsumptionRatingRequest(
            source_id="nrcan_fuel_consumption_ratings",
            component_key="fixture",
            component_meta={"label": "Fixture", "short_name": "fixture"},
            resource_id="00000000-0000-0000-0000-000000000000",
            resource_title="Fixture",
            url="https://example.test/fixture.csv",
            cache_path=(tmp_path / "fixture.csv").resolve(),
            expected_md5="0" * 32,
            expected_bytes=1,
            expected_model_years=(2025,),
            required_columns=("Model year",),
            required_non_null_columns=("Model year",),
            output_file="fixture.csv",
        )


def test_nrcan_rules_load_paths_and_extraction_parameters_from_config() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = module_rules(bundle)

    assert rules["interim_subdir"] == "fetched_nrcan_ceud_inputs"
    assert rules["raw_excel_skiprows"] == 10
    assert rules["region_output_template"] == "nrcan_ceud_transport_{region}.csv"

    rating_config = ratings_rules(bundle)
    assert (
        rating_config["interim_subdir"]
        == "fetched_nrcan_fuel_consumption_ratings"
    )
    assert rating_config["encoding_candidates"] == ["utf-8-sig", "cp1252"]
    class_rules = rating_config["vehicle_class_harmonization"]
    assert class_rules["target_to_nrcan"]["Small SUV"] == [
        "Sport utility vehicle: Small"
    ]
    assert "Van: Cargo" in class_rules["target_to_nrcan"]["Pickup"]
    assert class_rules["unresolved_nrcan_classes"] == [
        "Special purpose vehicle",
        "Sport utility vehicle",
    ]
    assert class_rules["unmapped_policy"] == "error"
    assert "legacy_wards_market_shares" not in rating_config

    market_source = bundle.sources.sources[
        "wards_intelligence_2022_sales_shares"
    ]
    market_component = market_source.component("vehicle_class_market_shares")
    market_path = resolve_input_path(
        bundle,
        "manual",
        market_component.adapter["manual_parameter_path"],
    )
    shares = pd.read_csv(market_path)
    assert list(shares.columns) == market_source.adapter["expected_columns"]
    assert len(shares) == 32
    assert not shares.duplicated(
        market_source.adapter["unique_key"]
    ).any()
    assert shares["market_share"].between(0, 1).all()
    assert set(shares["year"]) == {2018, 2020, 2021}
    assert set(shares["data_year -> dq_time"]) == {2022}
    assert shares.groupby("year").size().to_dict() == {2018: 13, 2020: 6, 2021: 13}
    ldv_shares = shares.loc[shares["vehicle_scope"].eq("ldv")].copy()
    mhdv_shares = shares.loc[shares["vehicle_scope"].eq("mhdv")].copy()
    hierarchy = set(
        ldv_shares[
            ["nrcan_vehicle_class", "nlr_atb_class", "nrcan_ceud_class"]
        ]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    assert hierarchy == {
        ("Compact", "Compact", "Car"),
        ("Full-size", "Midsize", "Car"),
        ("Mid-size", "Midsize", "Car"),
        ("Minicompact", "Compact", "Car"),
        ("Station wagon: Small", "Midsize", "Car"),
        ("Subcompact", "Compact", "Car"),
        ("Two-seater", "Midsize", "Car"),
        ("Minivan", "Midsize SUV", "Light Truck"),
        ("Pickup truck: Small", "Pickup", "Light Truck"),
        ("Pickup truck: Standard", "Pickup", "Light Truck"),
        ("Sport utility vehicle: Small", "Small SUV", "Light Truck"),
        ("Sport utility vehicle: Standard", "Midsize SUV", "Light Truck"),
        ("Van: Passenger", "Pickup", "Light Truck"),
    }
    source_to_ratings = dict(
        ldv_shares[["wards_size_class", "nrcan_vehicle_class"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    assert source_to_ratings["SUV: Small"] == "Sport utility vehicle: Small"
    assert source_to_ratings["SUV: Standard"] == "Sport utility vehicle: Standard"

    group_sums = shares.groupby(["nrcan_ceud_class", "year"])[
        "market_share"
    ].sum()
    assert all(value == pytest.approx(1.0) for value in group_sums)
    assert mhdv_shares.loc[
        mhdv_shares["nrcan_ceud_class"].eq("Medium Trucks"), "wards_size_class"
    ].tolist() == ["Class 3", "Class 4", "Class 5", "Class 6", "Class 7"]
    assert mhdv_shares.loc[
        mhdv_shares["nrcan_ceud_class"].eq("Heavy Trucks"), "wards_size_class"
    ].tolist() == ["Class 8"]
    assert mhdv_shares[["nrcan_vehicle_class", "nlr_atb_class"]].isna().all().all()

    nlr_sums = ldv_shares.groupby(
        ["year", "nrcan_ceud_class", "nlr_atb_class"]
    )["market_share"].sum()
    expected_nlr_sums = {
        (2018, "Car", "Compact"): 0.6070910018780811,
        (2018, "Car", "Midsize"): 0.3929089981219187,
        (2018, "Light Truck", "Midsize SUV"): 0.15997427434954858,
        (2018, "Light Truck", "Pickup"): 0.3120629538539029,
        (2018, "Light Truck", "Small SUV"): 0.5279627717965483,
        (2021, "Car", "Compact"): 0.545003976856256,
        (2021, "Car", "Midsize"): 0.4549960231437437,
        (2021, "Light Truck", "Midsize SUV"): 0.147468658086318,
        (2021, "Light Truck", "Pickup"): 0.28384509480798503,
        (2021, "Light Truck", "Small SUV"): 0.5686862471056968,
    }
    assert set(nlr_sums.index) == set(expected_nlr_sums)
    for key, expected in expected_nlr_sums.items():
        assert nlr_sums.loc[key] == pytest.approx(expected)


def test_clean_label_removes_superscript_noise() -> None:
    assert clean_label("Passenger-km (10^9)\N{SUPERSCRIPT ONE}", nrcan_rules()) == "Passenger-km (109)"


def test_extract_unit_uses_last_parenthesized_token() -> None:
    assert extract_unit("Vehicle Activity (million passenger-km)|Diesel (PJ)") == "PJ"
    assert extract_unit("Vehicle Activity") is None


def test_rating_csv_cp1252_fallback_and_source_normalization(
    tmp_path: Path,
) -> None:
    content = (
        "Model year,Make,Model,Vehicle class ,Combined (L/100 km)\n"
        "2025,Caf\xe9,Example,Compact,6.5\n"
    ).encode("cp1252")
    path = tmp_path / "ratings.csv"
    path.write_bytes(content)
    request = rating_request(path, content=content)

    raw, encoding = read_rating_csv(
        request,
        encoding_candidates=["utf-8-sig", "cp1252"],
    )
    normalized = normalize_rating_dataframe(raw, request)

    assert encoding == "cp1252"
    assert normalized.loc[0, "Make"] == "Café"
    assert normalized.loc[0, "Vehicle class"] == "Compact"
    assert normalized.loc[0, "source_row"] == 2
    assert normalized.loc[0, "resource_id"] == request.resource_id


def test_rating_cache_validation_and_atomic_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = b"fixture"
    request = rating_request(tmp_path / "ratings.csv", content=content)

    class FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self.content = payload

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        nrcan_ceud.requests,
        "get",
        lambda url, timeout: FakeResponse(content),
    )

    assert fetch_rating_to_cache(request) == "downloaded"
    assert request.cache_path.read_bytes() == content
    assert not request.cache_path.with_suffix(".csv.part").exists()
    assert validate_rating_cache(request) == (
        len(content),
        hashlib.md5(content, usedforsecurity=False).hexdigest(),
    )
    assert fetch_rating_to_cache(request) == "cached"

    request.cache_path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="MD5"):
        validate_rating_cache(request)


def test_rating_normalization_rejects_wrong_model_year_coverage(
    tmp_path: Path,
) -> None:
    request = rating_request(tmp_path / "ratings.csv", years=(2024, 2025))
    raw = pd.DataFrame(
        {
            "Model year": [2025],
            "Make": ["Example"],
            "Model": ["Example"],
            "Vehicle class": ["Compact"],
            "Combined (L/100 km)": [6.5],
        }
    )

    with pytest.raises(ValueError, match="covers model years"):
        normalize_rating_dataframe(raw, request)


def test_normalize_ceud_dataframe_drops_noise_and_preserves_provenance() -> None:
    raw = pd.DataFrame(
        {
            "Unnamed: 0": [None, None, None, None],
            "Unnamed: 1": ["Activity (PJ)", "Diesel", "Shares", "GHG Emissions"],
            "2000": [pd.NA, "1.5", "20", "99"],
            "2001": [pd.NA, "n.a.", "30", "100"],
        }
    )
    request = CeudTableRequest(
        source_id="nrcan_ceud_transport_provincial",
        region="on",
        output_region="ON",
        year=2021,
        table_id=36,
        table_meta={
            "label": ["Medium Trucks", "Heavy Trucks"],
            "short_name": "medium_heavy_trucks_activity",
            "applies_to": ["medium_trucks", "heavy_trucks"],
            "parameter_modules": ["stocks_and_demands"],
        },
        url="https://example.test/table.xls",
        cache_path=REPO_ROOT / "inputs" / "0_cache" / "nrcan_ceud_transport" / "2021_tran_on_e_36.xls",
    )

    normalized = normalize_ceud_dataframe(raw, request, nrcan_rules())

    assert normalized.to_dict("records") == [
        {
            "source_id": "nrcan_ceud_transport_provincial",
            "region": "ON",
            "table_id": 36,
            "table_label": "Medium Trucks|Heavy Trucks",
            "short_name": "medium_heavy_trucks_activity",
            "applies_to": "medium_trucks|heavy_trucks",
            "parameter_modules": "stocks_and_demands",
            "raw_series": "Medium Trucks|Activity (PJ)|Diesel",
            "series_group": "Medium Trucks",
            "series_name": "Activity (PJ)|Diesel",
            "year": 2000,
            "value": 1.5,
            "unit": "PJ",
            "cached_file": str(
                REPO_ROOT / "inputs" / "0_cache" / "nrcan_ceud_transport" / "2021_tran_on_e_36.xls"
            ),
        }
    ]


def test_table_7_total_energy_use_can_be_value_and_group_header() -> None:
    raw = pd.DataFrame(
        {
            "Unnamed: 0": [None, None, None, None, None, None, None],
            "Unnamed: 1": [
                None,
                "Total Energy Use (PJ)",
                "Passenger Transportation",
                "Freight Transportation",
                "Off-Road1",
                "Energy Use by Transportation Mode (PJ)",
                "Cars",
            ],
            "2000": [pd.NA, "10", "6", "3", "1", pd.NA, "5"],
        }
    )
    request = CeudTableRequest(
        source_id="nrcan_ceud_transport_provincial",
        region="on",
        output_region="ON",
        year=2021,
        table_id=7,
        table_meta={
            "label": "Off-Road",
            "short_name": "off_road_fuel_use",
            "applies_to": ["off_road"],
            "parameter_modules": ["stocks_and_demands"],
        },
        url="https://example.test/table.xls",
        cache_path=REPO_ROOT / "inputs" / "0_cache" / "nrcan_ceud_transport" / "2021_tran_on_e_7.xls",
    )

    normalized = normalize_ceud_dataframe(raw, request, nrcan_rules())

    assert normalized[["raw_series", "unit", "value"]].to_dict("records") == [
        {"raw_series": "Total Energy Use (PJ)", "unit": "PJ", "value": 10.0},
        {"raw_series": "Total Energy Use (PJ)|Passenger Transportation", "unit": "PJ", "value": 6.0},
        {"raw_series": "Total Energy Use (PJ)|Freight Transportation", "unit": "PJ", "value": 3.0},
        {"raw_series": "Total Energy Use (PJ)|Off-Road1", "unit": "PJ", "value": 1.0},
        {"raw_series": "Energy Use by Transportation Mode (PJ)|Cars", "unit": "PJ", "value": 5.0},
    ]
