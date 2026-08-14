"""Fetch request-scoped vPIC evidence for MTO make-model-year corroboration."""

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
from typing import Annotated, Any, Literal
from urllib.parse import quote, urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError, model_validator

from fetching.vehicle_population import write_dataframe_atomic
from fetching.vpic_vehicle_types import VPicResponse, file_sha256
from utils import ConfigBundle, load_config_bundle, load_harmonization_rules, resolve_input_path
from utils.vehicle_labels import vehicle_families_equivalent


SOURCE_KEY = "nhtsa_vpic_vehicle_models"
RULE_KEY = "vpic_model_years"
NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Endpoint = Literal["models_for_make_year", "canadian_vehicle_specifications"]
REQUEST_COLUMNS = [
    "mto_make_code",
    "mto_model_code",
    "model_year",
    "canonical_make",
    "canonical_model",
    "exact_public_year_evidence",
    "high_stock_reaudit",
    "latest_fit_active_stock",
]
EVIDENCE_COLUMNS = [
    *REQUEST_COLUMNS,
    "temporal_validation_status",
    "vpic_endpoint",
    "vpic_model_names",
    "vpic_result_count",
    "vpic_matched_count",
    "evidence_source",
    "diagnostic_message",
]
MANIFEST_COLUMNS = [
    "source_key",
    "canonical_make",
    "query_model_year",
    "endpoint",
    "url",
    "cache_path",
    "cache_status",
    "sha256",
    "bytes",
    "result_count",
    "retrieved_at_utc",
    "diagnostic_message",
]


class CanadianSpecificationValue(BaseModel):
    """One name/value item from a Canadian specification result."""

    model_config = ConfigDict(extra="allow", strict=True)

    name: NonEmptyString = Field(alias="Name")
    value: str = Field(alias="Value")


class CanadianSpecificationResult(BaseModel):
    """Validated Canadian specification row with required make/model values."""

    model_config = ConfigDict(extra="allow", strict=True)

    specs: list[CanadianSpecificationValue] = Field(alias="Specs", min_length=1)

    @model_validator(mode="after")
    def require_make_and_model(self) -> "CanadianSpecificationResult":
        names = {item.name.casefold() for item in self.specs}
        if not {"make", "model"} <= names:
            raise ValueError("Canadian specification row requires Make and Model")
        return self

    def value_for(self, name: str) -> str:
        return next(
            item.value for item in self.specs if item.name.casefold() == name.casefold()
        )


class CanadianSpecificationResponse(BaseModel):
    """Validated vPIC envelope for GetCanadianVehicleSpecifications."""

    model_config = ConfigDict(extra="allow", strict=True)

    count: int = Field(alias="Count", ge=0)
    message: str = Field(alias="Message")
    search_criteria: str = Field(alias="SearchCriteria")
    results: list[CanadianSpecificationResult] = Field(alias="Results")

    @model_validator(mode="after")
    def validate_count(self) -> "CanadianSpecificationResponse":
        if self.count != len(self.results):
            raise ValueError(
                f"vPIC Count={self.count} does not equal Results={len(self.results)}"
            )
        return self


class VPicTemporalRequest(BaseModel):
    """Validated request for one canonical make and observed model year."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_id: Literal["nhtsa_vpic_vehicle_models"]
    canonical_make: NonEmptyString
    query_model_year: int = Field(ge=1990, le=2100)
    endpoint: Endpoint
    url: NonEmptyString
    cache_path: Path

    @model_validator(mode="after")
    def validate_request(self) -> "VPicTemporalRequest":
        parsed = urlparse(self.url)
        if parsed.scheme != "https" or parsed.hostname != "vpic.nhtsa.dot.gov":
            raise ValueError("vPIC requests must use the official HTTPS API host")
        expected_path = (
            "/GetModelsForMakeYear/"
            if self.endpoint == "models_for_make_year"
            else "/GetCanadianVehicleSpecifications/"
        )
        if expected_path not in parsed.path:
            raise ValueError(f"vPIC request path does not match {self.endpoint}")
        if not self.cache_path.is_absolute() or self.cache_path.suffix != ".json":
            raise ValueError("vPIC cache path must be an absolute JSON file")
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    return load_harmonization_rules(bundle, RULE_KEY)


def load_eligible_rows(bundle: ConfigBundle) -> pd.DataFrame:
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
            f"Generate the temporal vPIC request artifact first: {path}"
        )
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if list(frame.columns) != REQUEST_COLUMNS:
        raise ValueError(f"Temporal vPIC request columns differ: {list(frame.columns)}")
    frame["model_year"] = pd.to_numeric(frame["model_year"], errors="raise").astype(int)
    if frame["model_year"].lt(int(rules["canadian_specifications_minimum_year"])).any():
        raise ValueError("Temporal vPIC request contains an unsupported model year")
    if not frame["high_stock_reaudit"].isin({"true", "false"}).all():
        raise ValueError("Temporal request re-audit flags must be true or false")
    return frame


def _cache_name(make: str, year: int, endpoint: Endpoint) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", make.casefold()).strip("_") or "make"
    if endpoint == "models_for_make_year":
        identity = hashlib.sha256(
            f"{make.casefold()}|{year}|all".encode()
        ).hexdigest()[:12]
        return f"{slug}_{year}_all_{identity}.json"
    identity = hashlib.sha256(
        f"temporal|{endpoint}|{make.casefold()}|{year}".encode()
    ).hexdigest()[:12]
    return f"{slug}_{year}_{endpoint}_{identity}.json"


def build_requests(
    bundle: ConfigBundle, eligible: pd.DataFrame
) -> list[VPicTemporalRequest]:
    rules = module_rules(bundle)
    component = bundle.sources.sources[SOURCE_KEY].component("model_year_validation")
    api_base = str(component.adapter["api_base_url"]).rstrip("/")
    cache_dir = resolve_input_path(bundle, "cache", component.adapter["cache_subdir"])
    minimum_models_year = int(rules["models_endpoint_minimum_year"])
    unique = eligible[["canonical_make", "model_year"]].drop_duplicates()
    requests_out: list[VPicTemporalRequest] = []
    for row in unique.sort_values(["canonical_make", "model_year"]).itertuples(index=False):
        make = str(row.canonical_make)
        year = int(row.model_year)
        if year >= minimum_models_year:
            endpoint: Endpoint = "models_for_make_year"
            url = (
                f"{api_base}/GetModelsForMakeYear/make/{quote(make, safe='')}"
                f"/modelyear/{year}?format=json"
            )
        else:
            endpoint = "canadian_vehicle_specifications"
            url = (
                f"{api_base}/GetCanadianVehicleSpecifications/"
                f"?year={year}&make={quote(make, safe='')}&format=json"
            )
        requests_out.append(
            VPicTemporalRequest(
                source_id=SOURCE_KEY,
                canonical_make=make,
                query_model_year=year,
                endpoint=endpoint,
                url=url,
                cache_path=(cache_dir / _cache_name(make, year, endpoint)).resolve(),
            )
        )
    maximum = int(rules["maximum_requests"])
    if len(requests_out) > maximum:
        raise ValueError(
            f"Temporal vPIC request count {len(requests_out)} exceeds configured {maximum}"
        )
    return requests_out


def _validate_payload(
    request: VPicTemporalRequest, payload: object
) -> VPicResponse | CanadianSpecificationResponse:
    if request.endpoint == "models_for_make_year":
        return VPicResponse.model_validate(payload)
    return CanadianSpecificationResponse.model_validate(payload)


def validate_cache(
    request: VPicTemporalRequest,
) -> VPicResponse | CanadianSpecificationResponse:
    payload = json.loads(request.cache_path.read_text(encoding="utf-8"))
    return _validate_payload(request, payload)


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def fetch_to_cache(
    request: VPicTemporalRequest,
    *,
    session: requests.Session,
    timeout: int,
) -> tuple[VPicResponse | CanadianSpecificationResponse, str]:
    if request.cache_path.is_file():
        return validate_cache(request), "cached"
    response = session.get(request.url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    _write_json_atomic(request.cache_path, payload)
    return _validate_payload(request, payload), "downloaded"


def _model_names(
    response: VPicResponse | CanadianSpecificationResponse,
) -> list[str]:
    return [str(result.model_name) for result in response.results] if isinstance(
        response, VPicResponse
    ) else [str(result.value_for("Model")) for result in response.results]


def normalize_evidence(
    eligible: pd.DataFrame,
    responses: dict[tuple[str, int], VPicResponse | CanadianSpecificationResponse],
    *,
    request_errors: dict[tuple[str, int], tuple[str, str]] | None = None,
    endpoints: dict[tuple[str, int], Endpoint] | None = None,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    errors = request_errors or {}
    endpoint_lookup = endpoints or {}
    for row in eligible.itertuples(index=False):
        key = (str(row.canonical_make), int(row.model_year))
        response = responses.get(key)
        endpoint = endpoint_lookup.get(key, "models_for_make_year")
        if response is None:
            status, message = errors.get(key, ("request_failed", "response unavailable"))
            records.append(
                {
                    **{column: getattr(row, column) for column in REQUEST_COLUMNS},
                    "temporal_validation_status": status,
                    "vpic_endpoint": endpoint,
                    "vpic_model_names": "",
                    "vpic_result_count": 0,
                    "vpic_matched_count": 0,
                    "evidence_source": SOURCE_KEY,
                    "diagnostic_message": message,
                }
            )
            continue
        models = _model_names(response)
        matches = sorted(
            {
                model
                for model in models
                if vehicle_families_equivalent(row.canonical_model, model)
            },
            key=str.casefold,
        )
        mutually_equivalent = all(
            vehicle_families_equivalent(matches[0], model) for model in matches[1:]
        ) if matches else True
        status = "confirmed" if matches and mutually_equivalent else (
            "ambiguous" if matches else "not_corroborated"
        )
        records.append(
            {
                **{column: getattr(row, column) for column in REQUEST_COLUMNS},
                "temporal_validation_status": status,
                "vpic_endpoint": endpoint,
                "vpic_model_names": " | ".join(matches),
                "vpic_result_count": len(models),
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
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    eligible = load_eligible_rows(bundle)
    request_rows = build_requests(bundle, eligible)
    client = session or requests.Session()
    timeout = int(rules["request_timeout_seconds"])
    delay = float(rules["request_delay_seconds"])
    responses: dict[tuple[str, int], VPicResponse | CanadianSpecificationResponse] = {}
    errors: dict[tuple[str, int], tuple[str, str]] = {}
    endpoints: dict[tuple[str, int], Endpoint] = {}
    manifest_rows: list[dict[str, object]] = []
    for index, request in enumerate(request_rows):
        key = (request.canonical_make, request.query_model_year)
        endpoints[key] = request.endpoint
        parsed: VPicResponse | CanadianSpecificationResponse | None = None
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
            responses[key] = parsed
        except ValidationError as error:
            cache_status = "invalid"
            diagnostic = f"ValidationError: {error}"
            errors[key] = ("invalid_response", diagnostic)
        except (OSError, ValueError, requests.RequestException) as error:
            cache_status = "failed"
            diagnostic = f"{type(error).__name__}: {error}"
            errors[key] = ("request_failed", diagnostic)
        cache_exists = request.cache_path.is_file()
        retrieved_at = (
            datetime.fromtimestamp(request.cache_path.stat().st_mtime, tz=UTC).isoformat()
            if cache_exists
            else ""
        )
        manifest_rows.append(
            {
                "source_key": request.source_id,
                "canonical_make": request.canonical_make,
                "query_model_year": request.query_model_year,
                "endpoint": request.endpoint,
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
        if download and index + 1 < len(request_rows) and delay > 0:
            time.sleep(delay)
    evidence = normalize_evidence(
        eligible,
        responses,
        request_errors=errors,
        endpoints=endpoints,
    )
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    write_dataframe_atomic(evidence, output_dir / str(rules["output_file"]))
    write_dataframe_atomic(
        pd.DataFrame(manifest_rows, columns=MANIFEST_COLUMNS),
        output_dir / str(rules["manifest_file"]),
    )
    warnings = evidence.loc[
        ~evidence["temporal_validation_status"].eq("confirmed")
    ].copy()
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
    logging.info("Wrote vPIC temporal evidence to %s", output_dir)


if __name__ == "__main__":
    main()
