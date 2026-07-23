from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from fetching.nrcan_ceud import (
    CeudTableRequest,
    clean_label,
    extract_unit,
    iter_table_requests,
    module_rules,
    normalize_ceud_dataframe,
    render_ceud_url,
    validate_source,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def nrcan_rules() -> dict[str, object]:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return module_rules(bundle)


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


def test_nrcan_rules_load_paths_and_extraction_parameters_from_config() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = module_rules(bundle)

    assert rules["interim_subdir"] == "fetched_nrcan_ceud_inputs"
    assert rules["raw_excel_skiprows"] == 10
    assert rules["region_output_template"] == "nrcan_ceud_transport_{region}.csv"


def test_clean_label_removes_superscript_noise() -> None:
    assert clean_label("Passenger-km (10^9)\N{SUPERSCRIPT ONE}", nrcan_rules()) == "Passenger-km (109)"


def test_extract_unit_uses_last_parenthesized_token() -> None:
    assert extract_unit("Vehicle Activity (million passenger-km)|Diesel (PJ)") == "PJ"
    assert extract_unit("Vehicle Activity") is None


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
