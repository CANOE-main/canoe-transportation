from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from fetching.cer_enerfuture import (
    CerEnergyFutureError,
    CerTableRequest,
    build_requests,
    configured_edition,
    configured_scenario,
    fetch_to_cache,
    module_rules,
    normalize_component,
    read_and_validate_source,
    scenario_region_labels,
    write_outputs,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


def test_source_contract_targets_readme_parameter_roles(bundle) -> None:
    source = bundle.sources.sources["cer_canadas_energy_future"]

    assert source.component("macro-indicators").parameter_modules == [
        "stocks_and_demands",
        "capex_opex",
    ]
    assert source.component("end-use-demand").parameter_modules == []
    assert source.component("end-use-prices").produces == ["fuel_price_scenarios"]


def test_request_resolution_supports_both_configured_editions(bundle) -> None:
    requests_2023 = build_requests(bundle, edition=2023)
    requests_2026 = build_requests(bundle, edition=2026)

    assert configured_edition(bundle) == 2026
    assert configured_scenario(bundle, module_rules(bundle)) == "Current Measures"
    assert {request.component_id for request in requests_2023} == {
        "macro-indicators",
        "end-use-demand",
        "end-use-prices",
    }
    assert requests_2023[0].doi.endswith("zppg-yr91")
    assert requests_2026[0].doi.endswith("rd69-q158")
    assert all("energyfutures2023" in request.url for request in requests_2023)
    assert all("energyfutures2026" in request.url for request in requests_2026)
    assert requests_2023[0].cache_path.parent.name == "2023"
    assert requests_2026[0].cache_path.parent.name == "2026"


def test_unsupported_edition_and_region_fail_before_io(bundle) -> None:
    with pytest.raises(CerEnergyFutureError, match="Unsupported CER edition 2024"):
        build_requests(bundle, edition=2024)
    with pytest.raises(CerEnergyFutureError, match="No CER geography mapping"):
        scenario_region_labels(bundle, ["XX"])


def test_request_model_rejects_invalid_cache_and_url(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="CER URL is invalid"):
        CerTableRequest(
            component_id="macro-indicators",
            title="Macro",
            edition=2026,
            doi="doi",
            open_government_record_id="record",
            url="not-a-url",
            cache_path=tmp_path / "macro.csv",
            output_file="macro.csv",
            required_columns=("Scenario",),
            expected_scenarios=("Current Measures",),
            first_year=2005,
            last_year=2050,
        )


def _local_request(bundle, tmp_path: Path, component_id: str) -> CerTableRequest:
    request = next(
        item
        for item in build_requests(bundle, edition=2023)
        if item.component_id == component_id
    )
    return request.model_copy(
        update={
            "cache_path": tmp_path / f"{component_id}.csv",
            "expected_scenarios": ("Current Measures",),
        }
    )


def test_ef2023_unnamed_index_is_removed_and_source_is_validated(
    bundle, tmp_path: Path
) -> None:
    request = _local_request(bundle, tmp_path, "macro-indicators")
    pd.DataFrame(
        {
            "Unnamed: 0": [1, 2],
            "Scenario": ["Current Measures", "Current Measures"],
            "Region": ["Canada", "Canada"],
            "Variable": [
                "Real Gross Domestic Product ($2012 Millions)",
                "Real Gross Domestic Product ($2012 Millions)",
            ],
            "Year": [2005, 2050],
            "Value": [1.0, 2.0],
        }
    ).to_csv(request.cache_path, index=False)

    validated = read_and_validate_source(request)

    assert "Unnamed: 0" not in validated.columns
    assert validated["Year"].dtype.kind in {"i", "u"}


def test_source_validation_reports_missing_scenario(bundle, tmp_path: Path) -> None:
    request = _local_request(bundle, tmp_path, "macro-indicators").model_copy(
        update={"expected_scenarios": ("Current Measures", "Global Net-zero")}
    )
    pd.DataFrame(
        {
            "Scenario": ["Current Measures", "Current Measures"],
            "Region": ["Canada", "Canada"],
            "Variable": ["Population (thousands)", "Population (thousands)"],
            "Year": [2005, 2050],
            "Value": [1.0, 2.0],
        }
    ).to_csv(request.cache_path, index=False)

    with pytest.raises(CerEnergyFutureError, match="missing scenarios"):
        read_and_validate_source(request)


def test_macro_normalization_preserves_reference_year_labels(bundle) -> None:
    request = build_requests(bundle, edition=2023)[0]
    frame = pd.DataFrame(
        {
            "Scenario": ["Current Measures"] * 4,
            "Region": ["Canada"] * 4,
            "Variable": [
                "Real Gross Domestic Product ($2012 Millions)",
                "Gross Domestic Product Deflator (2012=100)",
                "Canada-US Exchange Rate (C$/US$)",
                "Population (thousands)",
            ],
            "Year": [2025] * 4,
            "Value": [2_000_000.0, 130.0, 1.3, 40_000.0],
        }
    )

    normalized = normalize_component(
        frame,
        request,
        region_labels=["Ontario"],
        rules=module_rules(bundle),
    )

    assert set(normalized["variable_key"]) == {
        "real_gdp",
        "gdp_deflator",
        "cad_per_usd",
    }
    assert "$2012 Millions" in set(normalized["unit"])
    assert "2012=100" in set(normalized["unit"])
    assert normalized["is_default_scenario"].all()


def test_demand_and_price_normalization_use_transport_targeting(bundle) -> None:
    rules = module_rules(bundle)
    demand_request = next(
        request
        for request in build_requests(bundle, edition=2026)
        if request.component_id == "end-use-demand"
    )
    demand = pd.DataFrame(
        {
            "Scenario": ["Current Measures"] * 4,
            "Region": ["Ontario", "Canada", "Ontario", "Alberta"],
            "Variable": ["Diesel", "Diesel", "Diesel", "Diesel"],
            "Year": [2030] * 4,
            "Value": [1.0, 2.0, 3.0, 4.0],
            "Sector": [
                "Transportation",
                "Transportation",
                "Residential",
                "Transportation",
            ],
        }
    )
    normalized_demand = normalize_component(
        demand,
        demand_request,
        region_labels=["Ontario"],
        rules=rules,
    )

    price_request = next(
        request
        for request in build_requests(bundle, edition=2026)
        if request.component_id == "end-use-prices"
    )
    prices = pd.DataFrame(
        {
            "Scenario": ["Current Measures"] * 4,
            "Region": ["Ontario"] * 3 + ["Alberta"],
            "Variable": ["Gasoline", "Diesel", "Electricity", "Gasoline"],
            "Year": [2030] * 4,
            "Value": [40.0, 41.0, 42.0, 43.0],
            "Sector": ["Transportation"] * 4,
        }
    )
    normalized_prices = normalize_component(
        prices,
        price_request,
        region_labels=["Ontario"],
        rules=rules,
    )

    assert set(normalized_demand["region"]) == {"Ontario", "Canada"}
    assert set(normalized_demand["unit"]) == {"PJ"}
    assert set(normalized_prices["variable"]) == {"Gasoline", "Diesel"}
    assert set(normalized_prices["unit"]) == {"2022 CAD/GJ"}


class _Response:
    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        assert chunk_size > 0
        yield b"Scenario,Region,Variable,Year,Value\n"
        yield b"Current Measures,Canada,Population,2005,1\n"


class _Session:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, url: str, *, stream: bool, timeout: int):
        assert url.startswith("https://")
        assert stream is True
        assert timeout > 0
        self.calls += 1
        return _Response()


def test_fetch_is_atomic_and_reuses_existing_cache(bundle, tmp_path: Path) -> None:
    request = _local_request(bundle, tmp_path, "macro-indicators")
    session = _Session()

    assert fetch_to_cache(request, session=session) == "downloaded"
    assert request.cache_path.read_text(encoding="utf-8").startswith("Scenario")
    assert not request.cache_path.with_suffix(".csv.part").exists()
    assert fetch_to_cache(request, session=session) == "cached"
    assert session.calls == 1


def test_manifest_and_warning_outputs_are_written(bundle, tmp_path: Path) -> None:
    request = _local_request(bundle, tmp_path, "macro-indicators")
    output = pd.DataFrame({"scenario": ["Current Measures"]})
    rules = module_rules(bundle)

    write_outputs(
        normalized={"macro-indicators": output},
        requests_by_component={"macro-indicators": request},
        manifest_rows=[{"component_id": "macro-indicators", "status": "ok"}],
        warnings=[],
        output_dir=tmp_path / "output",
        rules=rules,
    )

    manifest = pd.read_csv(tmp_path / "output" / rules["manifest_file"])
    assert manifest.loc[0, "status"] == "ok"
    assert (tmp_path / "output" / request.output_file).exists()
    assert (tmp_path / "output" / rules["warnings_file"]).read_text() == ""
