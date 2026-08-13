"""Fetch gated vPIC model/vehicle-type evidence for classless MTO mappings."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from typing import Annotated

from fetching.vehicle_population import write_dataframe_atomic
from utils import ConfigBundle, load_config_bundle, load_harmonization_rules, resolve_input_path
from utils.vehicle_labels import vehicle_families_equivalent


SOURCE_KEY = "nhtsa_vpic_vehicle_models"
RULE_KEY = "vpic_vehicle_types"
NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
EVIDENCE_COLUMNS = [
    "mto_make_code",
    "mto_model_code",
    "canonical_make",
    "canonical_model",
    "query_model_year",
    "vpic_match_status",
    "vehicle_scope",
    "vpic_model_names",
    "vpic_vehicle_types",
    "vpic_result_count",
    "vpic_matched_count",
    "evidence_source",
    "diagnostic_message",
]
MANIFEST_COLUMNS = [
    "source_key",
    "canonical_make",
    "query_model_year",
    "vehicle_type",
    "url",
    "cache_path",
    "cache_status",
    "sha256",
    "bytes",
    "result_count",
    "retrieved_at_utc",
    "diagnostic_message",
]


class VPicModelResult(BaseModel):
    """Validated subset of one vPIC model response row."""

    model_config = ConfigDict(extra="allow", strict=True)

    make_id: int = Field(alias="Make_ID")
    make_name: NonEmptyString = Field(alias="Make_Name")
    model_id: int = Field(alias="Model_ID")
    model_name: NonEmptyString = Field(alias="Model_Name")
    vehicle_type_id: int | None = Field(default=None, alias="VehicleTypeId")
    vehicle_type_name: str | None = Field(default=None, alias="VehicleTypeName")


class VPicResponse(BaseModel):
    """Validated vPIC envelope for GetModelsForMakeYear."""

    model_config = ConfigDict(extra="allow", strict=True)

    count: int = Field(alias="Count", ge=0)
    message: str = Field(alias="Message")
    search_criteria: str = Field(alias="SearchCriteria")
    results: list[VPicModelResult] = Field(alias="Results")

    @model_validator(mode="after")
    def validate_count(self) -> "VPicResponse":
        if self.count != len(self.results):
            raise ValueError(
                f"vPIC Count={self.count} does not equal Results={len(self.results)}"
            )
        return self


class VPicRequest(BaseModel):
    """Validated, eligibility-gated request for one make/model-year response."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_id: Literal["nhtsa_vpic_vehicle_models"]
    canonical_make: NonEmptyString
    query_model_year: int = Field(ge=1996, le=2100)
    vehicle_type: str | None = None
    url: NonEmptyString
    cache_path: Path

    @model_validator(mode="after")
    def validate_request(self) -> "VPicRequest":
        parsed = urlparse(self.url)
        if parsed.scheme != "https" or parsed.hostname != "vpic.nhtsa.dot.gov":
            raise ValueError("vPIC requests must use the official HTTPS API host")
        if "/GetModelsForMakeYear/" not in parsed.path:
            raise ValueError("vPIC request must use GetModelsForMakeYear")
        if not self.cache_path.is_absolute() or self.cache_path.suffix != ".json":
            raise ValueError("vPIC cache path must be an absolute JSON file")
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load vPIC acquisition and normalization rules."""
    return load_harmonization_rules(bundle, RULE_KEY)


def file_sha256(path: Path) -> str:
    """Return a lowercase SHA-256 digest."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_name(make: str, year: int, vehicle_type: str | None = None) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", make.casefold()).strip("_") or "make"
    type_slug = re.sub(
        r"[^a-z0-9]+", "_", str(vehicle_type or "all").casefold()
    ).strip("_")
    identity = hashlib.sha256(
        f"{make.casefold()}|{year}|{vehicle_type or 'all'}".encode()
    ).hexdigest()[:12]
    return f"{slug}_{year}_{type_slug}_{identity}.json"


def load_eligible_rows(bundle: ConfigBundle) -> pd.DataFrame:
    """Load only the bootstrap-produced vPIC eligibility interface."""
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, "ontario_vehicle_population")
    path = resolve_input_path(
        bundle,
        "interim",
        ontario_rules["interim_subdir"],
        rules["eligible_request_file"],
    )
    if not path.is_file():
        raise FileNotFoundError(
            "Generate the promotion-approved vPIC request artifact first: " f"{path}"
        )
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = [
        "mto_make_code",
        "mto_model_code",
        "canonical_make",
        "canonical_model",
        "query_model_year",
        "latest_fit_active_stock",
        "promotion_status",
        "class_evidence_status",
    ]
    if list(frame.columns) != required:
        raise ValueError(f"vPIC eligibility columns differ: {list(frame.columns)}")
    if not frame["promotion_status"].eq("promoted").all():
        raise ValueError("vPIC request artifact contains a non-promoted mapping")
    if not frame["class_evidence_status"].eq("missing_ldv_class").all():
        raise ValueError("vPIC request artifact contains existing LDV class evidence")
    stock = pd.to_numeric(frame["latest_fit_active_stock"], errors="raise")
    if not stock.gt(0).all():
        raise ValueError("vPIC request artifact contains non-positive latest stock")
    frame["query_model_year"] = pd.to_numeric(
        frame["query_model_year"], errors="raise"
    ).astype(int)
    return frame


def build_requests(bundle: ConfigBundle, eligible: pd.DataFrame) -> list[VPicRequest]:
    """Deduplicate eligible keys to one HTTP request per canonical make and year."""
    rules = module_rules(bundle)
    source = bundle.sources.sources[SOURCE_KEY]
    component = source.component("model_vehicle_types")
    api_base = str(component.adapter["api_base_url"]).rstrip("/")
    cache_dir = resolve_input_path(bundle, "cache", str(component.adapter["cache_subdir"]))
    unique = eligible[["canonical_make", "query_model_year"]].drop_duplicates()
    requests_out: list[VPicRequest] = []
    for row in unique.sort_values(["canonical_make", "query_model_year"]).itertuples(
        index=False
    ):
        make = str(row.canonical_make)
        year = int(row.query_model_year)
        url = (
            f"{api_base}/GetModelsForMakeYear/make/{quote(make, safe='')}"
            f"/modelyear/{year}?format=json"
        )
        requests_out.append(
            VPicRequest(
                source_id=SOURCE_KEY,
                canonical_make=make,
                query_model_year=year,
                vehicle_type=None,
                url=url,
                cache_path=(cache_dir / _cache_name(make, year, None)).resolve(),
            )
        )
    maximum = int(rules["maximum_requests"])
    if len(requests_out) > maximum:
        raise ValueError(
            f"Eligible vPIC request count {len(requests_out)} exceeds configured {maximum}"
        )
    return requests_out


def build_typed_requests(
    base_requests: list[VPicRequest],
    eligible: pd.DataFrame,
    responses: dict[tuple[str, int], VPicResponse],
    *,
    vehicle_types: list[str],
) -> list[VPicRequest]:
    """Probe vehicle types only where the unfiltered response corroborates a family."""
    requests_out: list[VPicRequest] = []
    eligible_by_key = {
        key: rows
        for key, rows in eligible.groupby(["canonical_make", "query_model_year"])
    }
    for request in base_requests:
        key = (request.canonical_make, request.query_model_year)
        response = responses.get(key)
        if response is None:
            continue
        families = eligible_by_key[key]["canonical_model"].astype(str)
        if not any(
            vehicle_families_equivalent(family, result.model_name)
            for family in families
            for result in response.results
        ):
            continue
        base_url = request.url.removesuffix("?format=json")
        for vehicle_type in vehicle_types:
            typed_url = (
                f"{base_url}/vehicletype/{quote(vehicle_type, safe='')}?format=json"
            )
            requests_out.append(
                request.model_copy(
                    update={
                        "vehicle_type": vehicle_type,
                        "url": typed_url,
                        "cache_path": (
                            request.cache_path.parent
                            / _cache_name(
                                request.canonical_make,
                                request.query_model_year,
                                vehicle_type,
                            )
                        ).resolve(),
                    }
                )
            )
    return requests_out


def validate_cache(request: VPicRequest) -> VPicResponse:
    """Read and validate one cached JSON response."""
    payload = json.loads(request.cache_path.read_text(encoding="utf-8"))
    return VPicResponse.model_validate(payload)


def fetch_to_cache(
    request: VPicRequest,
    *,
    session: requests.Session,
    timeout: int,
) -> tuple[VPicResponse, str]:
    """Fetch one response atomically or reuse its validated cache."""
    if request.cache_path.is_file():
        return validate_cache(request), "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    response = session.get(request.url, timeout=timeout)
    response.raise_for_status()
    parsed = VPicResponse.model_validate(response.json())
    with tempfile.NamedTemporaryFile(
        dir=request.cache_path.parent,
        prefix=f".{request.cache_path.name}.",
        suffix=".tmp",
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(response.json(), handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    try:
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    return parsed, "downloaded"


def normalize_evidence(
    eligible: pd.DataFrame,
    responses: dict[tuple[str, int], VPicResponse],
    *,
    truck_vehicle_types: set[str],
    request_errors: dict[tuple[str, int], str] | None = None,
    typed_responses: dict[tuple[str, int, str], VPicResponse] | None = None,
) -> pd.DataFrame:
    """Match eligible families and classify only conclusive truck-compatible evidence."""
    records: list[dict[str, object]] = []
    normalized_truck_types = {value.casefold() for value in truck_vehicle_types}
    errors = request_errors or {}
    typed = typed_responses or {}
    for row in eligible.itertuples(index=False):
        key = (str(row.canonical_make), int(row.query_model_year))
        response = responses.get(key)
        if response is None:
            records.append(
                {
                    "mto_make_code": row.mto_make_code,
                    "mto_model_code": row.mto_model_code,
                    "canonical_make": row.canonical_make,
                    "canonical_model": row.canonical_model,
                    "query_model_year": int(row.query_model_year),
                    "vpic_match_status": "request_failed",
                    "vehicle_scope": "non_ldv_unclassified",
                    "vpic_model_names": "",
                    "vpic_vehicle_types": "",
                    "vpic_result_count": 0,
                    "vpic_matched_count": 0,
                    "evidence_source": SOURCE_KEY,
                    "diagnostic_message": errors.get(key, "response unavailable"),
                }
            )
            continue
        matches = [
            result
            for result in response.results
            if vehicle_families_equivalent(row.canonical_model, result.model_name)
        ]
        matched_names = sorted({result.model_name for result in matches}, key=str.casefold)
        vehicle_types = sorted(
            {
                str(result.vehicle_type_name).strip()
                for result in matches
                if result.vehicle_type_name and str(result.vehicle_type_name).strip()
            },
            key=str.casefold,
        )
        vehicle_types.extend(
            vehicle_type
            for (make, year, vehicle_type), typed_response in typed.items()
            if make == key[0]
            and year == key[1]
            and any(
                vehicle_families_equivalent(row.canonical_model, result.model_name)
                for result in typed_response.results
            )
            and vehicle_type not in vehicle_types
        )
        vehicle_types = sorted(set(vehicle_types), key=str.casefold)
        type_names = {value.casefold() for value in vehicle_types}
        scope = (
            "mhdv"
            if matches and type_names and type_names <= normalized_truck_types
            else "non_ldv_unclassified"
        )
        if not matches:
            status = "no_family_match"
        elif not vehicle_types:
            status = "missing_vehicle_type"
        elif scope == "mhdv":
            status = "truck_type_confirmed"
        else:
            status = "mixed_or_nontruck_vehicle_type"
        records.append(
            {
                "mto_make_code": row.mto_make_code,
                "mto_model_code": row.mto_model_code,
                "canonical_make": row.canonical_make,
                "canonical_model": row.canonical_model,
                "query_model_year": int(row.query_model_year),
                "vpic_match_status": status,
                "vehicle_scope": scope,
                "vpic_model_names": " | ".join(matched_names),
                "vpic_vehicle_types": " | ".join(vehicle_types),
                "vpic_result_count": len(response.results),
                "vpic_matched_count": len(matches),
                "evidence_source": SOURCE_KEY,
                "diagnostic_message": response.message,
            }
        )
    return pd.DataFrame(records, columns=EVIDENCE_COLUMNS)


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    download: bool = True,
    session: requests.Session | None = None,
) -> Path:
    """Fetch only eligible vPIC requests and publish auditable normalized evidence."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    eligible = load_eligible_rows(bundle)
    request_rows = build_requests(bundle, eligible)
    client = session or requests.Session()
    timeout = int(rules["request_timeout_seconds"])
    delay = float(rules["request_delay_seconds"])
    responses: dict[tuple[str, int], VPicResponse] = {}
    typed_responses: dict[tuple[str, int, str], VPicResponse] = {}
    request_errors: dict[tuple[str, int], str] = {}
    manifest_rows: list[dict[str, object]] = []

    def process_request(request: VPicRequest) -> None:
        key = (request.canonical_make, request.query_model_year)
        parsed: VPicResponse | None = None
        diagnostic = ""
        try:
            if download:
                parsed, cache_status = fetch_to_cache(
                    request, session=client, timeout=timeout
                )
            else:
                if not request.cache_path.is_file():
                    raise FileNotFoundError(
                        "vPIC cache is required during --no-download execution: "
                        f"{request.cache_path}"
                    )
                parsed = validate_cache(request)
                cache_status = "cached"
            if request.vehicle_type is None:
                responses[key] = parsed
            else:
                typed_responses[
                    (request.canonical_make, request.query_model_year, request.vehicle_type)
                ] = parsed
        except (OSError, ValueError, requests.RequestException) as error:
            cache_status = "failed"
            diagnostic = f"{type(error).__name__}: {error}"
            request_errors[key] = diagnostic
        cache_exists = request.cache_path.is_file()
        retrieved_at = (
            datetime.fromtimestamp(
                request.cache_path.stat().st_mtime, tz=UTC
            ).isoformat()
            if cache_exists
            else datetime.now(tz=UTC).isoformat()
        )
        manifest_rows.append(
            {
                "source_key": request.source_id,
                "canonical_make": request.canonical_make,
                "query_model_year": request.query_model_year,
                "vehicle_type": request.vehicle_type or "",
                "url": request.url,
                "cache_path": str(request.cache_path),
                "cache_status": cache_status,
                "sha256": file_sha256(request.cache_path) if cache_exists else "",
                "bytes": request.cache_path.stat().st_size if cache_exists else 0,
                "result_count": parsed.count if parsed is not None else 0,
                "retrieved_at_utc": retrieved_at,
                "diagnostic_message": diagnostic,
            }
        )
    for index, request in enumerate(request_rows):
        process_request(request)
        if download and index + 1 < len(request_rows) and delay > 0:
            time.sleep(delay)
    typed_requests = build_typed_requests(
        request_rows,
        eligible,
        responses,
        vehicle_types=[str(value) for value in rules["vehicle_types_to_probe"]],
    )
    if len(request_rows) + len(typed_requests) > int(rules["maximum_requests"]):
        raise ValueError("Eligible vPIC base and typed requests exceed configured maximum")
    for index, request in enumerate(typed_requests):
        process_request(request)
        if download and index + 1 < len(typed_requests) and delay > 0:
            time.sleep(delay)
    evidence = normalize_evidence(
        eligible,
        responses,
        truck_vehicle_types={str(value) for value in rules["truck_vehicle_types"]},
        request_errors=request_errors,
        typed_responses=typed_responses,
    )
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    write_dataframe_atomic(evidence, output_dir / str(rules["output_file"]))
    write_dataframe_atomic(
        pd.DataFrame(manifest_rows, columns=MANIFEST_COLUMNS),
        output_dir / str(rules["manifest_file"]),
    )
    warnings = evidence.loc[
        ~evidence["vpic_match_status"].eq("truck_type_confirmed"),
        [
            "mto_make_code",
            "mto_model_code",
            "vpic_match_status",
            "vpic_model_names",
            "vpic_vehicle_types",
            "diagnostic_message",
        ],
    ]
    write_dataframe_atomic(warnings, output_dir / str(rules["warnings_file"]))
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    parser.add_argument("--no-download", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    output_dir = fetch_and_normalize(args.scenario, download=not args.no_download)
    logging.info("Wrote gated vPIC evidence to %s", output_dir)


if __name__ == "__main__":
    main()
