import json
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from fetching.vpic_vehicle_types import (
    VPicResponse,
    build_requests,
    fetch_and_normalize,
    normalize_evidence,
)
from utils import load_config_bundle


SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def eligible_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "mto_make_code": "FRHT",
                "mto_model_code": code,
                "canonical_make": "Freightliner",
                "canonical_model": model,
                "query_model_year": 2025,
                "latest_fit_active_stock": stock,
                "promotion_status": "promoted",
                "class_evidence_status": "missing_ldv_class",
            }
            for code, model, stock in [("FM2", "M2", 20), ("M2", "M2", 10)]
        ]
    )


def response(*, vehicle_types: list[str]) -> VPicResponse:
    results = [
        {
            "Make_ID": 450,
            "Make_Name": "Freightliner",
            "Model_ID": index + 1,
            "Model_Name": "M2",
            "VehicleTypeId": index + 1,
            "VehicleTypeName": vehicle_type,
        }
        for index, vehicle_type in enumerate(vehicle_types)
    ]
    return VPicResponse.model_validate(
        {
            "Count": len(results),
            "Message": "Response returned successfully",
            "SearchCriteria": "Make:freightliner | ModelYear:2025",
            "Results": results,
        }
    )


def test_requests_collapse_duplicate_make_year() -> None:
    bundle = load_config_bundle(SCENARIO)

    requests = build_requests(bundle, eligible_rows())

    assert len(requests) == 1
    assert requests[0].url.startswith(
        "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/"
    )


def test_only_conclusive_truck_types_receive_mhdv_scope() -> None:
    eligible = eligible_rows().head(1)
    key = ("Freightliner", 2025)

    truck = normalize_evidence(
        eligible,
        {key: response(vehicle_types=["Truck", "Incomplete Vehicle"])},
        truck_vehicle_types={"Truck", "Incomplete Vehicle"},
    )
    mixed = normalize_evidence(
        eligible,
        {key: response(vehicle_types=["Truck", "Multipurpose Passenger Vehicle"])},
        truck_vehicle_types={"Truck", "Incomplete Vehicle"},
    )

    assert truck.loc[0, "vehicle_scope"] == "mhdv"
    assert mixed.loc[0, "vehicle_scope"] == "non_ldv_unclassified"
    assert mixed.loc[0, "vpic_match_status"] == "mixed_or_nontruck_vehicle_type"


def test_invalid_response_count_is_rejected() -> None:
    with pytest.raises(ValidationError, match="does not equal"):
        VPicResponse.model_validate(
            {
                "Count": 1,
                "Message": "ok",
                "SearchCriteria": "test",
                "Results": [],
            }
        )


def test_no_download_replays_cached_response_identically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bundle = load_config_bundle(SCENARIO)
    eligible = eligible_rows().head(1)
    request = build_requests(bundle, eligible)[0]
    cache_path = tmp_path / "response.json"
    payload = response(vehicle_types=["Truck"]).model_dump(by_alias=True)
    cache_path.write_text(json.dumps(payload), encoding="utf-8")
    request = request.model_copy(update={"cache_path": cache_path.resolve()})

    class OfflineSession:
        def get(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("offline replay must not call the API")

    monkeypatch.setattr(
        "fetching.vpic_vehicle_types.load_eligible_rows", lambda _bundle: eligible
    )
    monkeypatch.setattr(
        "fetching.vpic_vehicle_types.build_requests",
        lambda _bundle, _eligible: [request],
    )
    monkeypatch.setattr(
        "fetching.vpic_vehicle_types.resolve_input_path",
        lambda *_args: tmp_path,
    )

    output = fetch_and_normalize(SCENARIO, download=False, session=OfflineSession())
    evidence = pd.read_csv(output / "vpic_vehicle_type_evidence.csv")

    assert evidence.loc[0, "vehicle_scope"] == "mhdv"
    assert evidence.loc[0, "vpic_match_status"] == "truck_type_confirmed"
