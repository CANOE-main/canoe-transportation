import json
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from fetching.vpic_model_years import (
    CanadianSpecificationResponse,
    REQUEST_COLUMNS,
    VPicTemporalRequest,
    build_requests,
    fetch_and_normalize,
    normalize_evidence,
)
from fetching.vpic_vehicle_types import VPicResponse
from utils import load_config_bundle


SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def eligible_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "mto_make_code": "MERZ",
                "mto_model_code": "GLC",
                "model_year": year,
                "canonical_make": "Mercedes-Benz",
                "canonical_model": "GLC-Class",
                "exact_public_year_evidence": "false",
                "high_stock_reaudit": "true",
                "latest_fit_active_stock": "1000",
            }
            for year in [1994, 2020]
        ],
        columns=REQUEST_COLUMNS,
    )


def models_response(model: str = "GLC-Class") -> VPicResponse:
    return VPicResponse.model_validate(
        {
            "Count": 1,
            "Message": "Response returned successfully",
            "SearchCriteria": "Make:Mercedes-Benz | ModelYear:2020",
            "Results": [
                {
                    "Make_ID": 449,
                    "Make_Name": "Mercedes-Benz",
                    "Model_ID": 1,
                    "Model_Name": model,
                    "VehicleTypeId": 7,
                    "VehicleTypeName": "Multipurpose Passenger Vehicle (MPV)",
                }
            ],
        }
    )


def canadian_response(model: str = "C-Class") -> CanadianSpecificationResponse:
    return CanadianSpecificationResponse.model_validate(
        {
            "Count": 1,
            "Message": "Response returned successfully",
            "SearchCriteria": "Year:1994 | Make:Mercedes-Benz",
            "Results": [
                {
                    "Specs": [
                        {"Name": "Make", "Value": "Mercedes-Benz"},
                        {"Name": "Model", "Value": model},
                    ]
                }
            ],
        }
    )


def test_requests_route_pre_1996_to_canadian_specifications() -> None:
    bundle = load_config_bundle(SCENARIO)

    requests = build_requests(bundle, eligible_rows())

    assert [request.endpoint for request in requests] == [
        "canadian_vehicle_specifications",
        "models_for_make_year",
    ]
    assert "GetCanadianVehicleSpecifications" in requests[0].url
    assert "GetModelsForMakeYear" in requests[1].url


def test_temporal_normalization_is_affirmative_and_case_insensitive() -> None:
    eligible = eligible_rows().loc[lambda frame: frame["model_year"].eq(2020)]

    confirmed = normalize_evidence(
        eligible,
        {("Mercedes-Benz", 2020): models_response("glc class")},
        endpoints={("Mercedes-Benz", 2020): "models_for_make_year"},
    )
    missing = normalize_evidence(
        eligible,
        {("Mercedes-Benz", 2020): models_response("AMG GT")},
        endpoints={("Mercedes-Benz", 2020): "models_for_make_year"},
    )

    assert confirmed.loc[0, "temporal_validation_status"] == "confirmed"
    assert missing.loc[0, "temporal_validation_status"] == "not_corroborated"


def test_invalid_canadian_count_is_rejected() -> None:
    with pytest.raises(ValidationError, match="does not equal"):
        CanadianSpecificationResponse.model_validate(
            {
                "Count": 2,
                "Message": "ok",
                "SearchCriteria": "test",
                "Results": [
                    {
                        "Specs": [
                            {"Name": "Make", "Value": "Ford"},
                            {"Name": "Model", "Value": "Mustang"},
                        ]
                    }
                ],
            }
        )


def test_no_download_replays_cached_temporal_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    eligible = eligible_rows().loc[lambda frame: frame["model_year"].eq(2020)]
    cache_path = tmp_path / "response.json"
    cache_path.write_text(
        json.dumps(models_response().model_dump(by_alias=True)), encoding="utf-8"
    )
    request = VPicTemporalRequest(
        source_id="nhtsa_vpic_vehicle_models",
        canonical_make="Mercedes-Benz",
        query_model_year=2020,
        endpoint="models_for_make_year",
        url=(
            "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/"
            "make/Mercedes-Benz/modelyear/2020?format=json"
        ),
        cache_path=cache_path.resolve(),
    )

    class OfflineSession:
        def get(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("offline replay must not call the API")

    monkeypatch.setattr(
        "fetching.vpic_model_years.load_eligible_rows", lambda _bundle: eligible
    )
    monkeypatch.setattr(
        "fetching.vpic_model_years.build_requests", lambda _bundle, _eligible: [request]
    )
    monkeypatch.setattr(
        "fetching.vpic_model_years.resolve_input_path", lambda *_args: tmp_path
    )

    output = fetch_and_normalize(SCENARIO, download=False, session=OfflineSession())
    evidence = pd.read_csv(output / "vpic_model_year_evidence.csv")

    assert evidence.loc[0, "temporal_validation_status"] == "confirmed"


def test_no_download_missing_cache_manifest_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    eligible = eligible_rows().loc[lambda frame: frame["model_year"].eq(2020)]
    request = VPicTemporalRequest(
        source_id="nhtsa_vpic_vehicle_models",
        canonical_make="Mercedes-Benz",
        query_model_year=2020,
        endpoint="models_for_make_year",
        url=(
            "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/"
            "make/Mercedes-Benz/modelyear/2020?format=json"
        ),
        cache_path=(tmp_path / "missing.json").resolve(),
    )
    monkeypatch.setattr(
        "fetching.vpic_model_years.load_eligible_rows", lambda _bundle: eligible
    )
    monkeypatch.setattr(
        "fetching.vpic_model_years.build_requests", lambda _bundle, _eligible: [request]
    )
    monkeypatch.setattr(
        "fetching.vpic_model_years.resolve_input_path", lambda *_args: tmp_path
    )

    fetch_and_normalize(SCENARIO, download=False)
    first = (tmp_path / "manifest.csv").read_bytes()
    fetch_and_normalize(SCENARIO, download=False)

    assert (tmp_path / "manifest.csv").read_bytes() == first
    manifest = pd.read_csv(tmp_path / "manifest.csv", keep_default_na=False)
    assert manifest.loc[0, "retrieved_at_utc"] == ""
