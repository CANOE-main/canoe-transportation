"""Replay pinned iZEV incentive records without treating them as complete sales."""

import argparse
import csv
import hashlib
import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from uuid import UUID

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.config_models import SourceComponent


SOURCE = "transport_canada_izev"
LOGGER = logging.getLogger(__name__)
POWERTRAIN_COLUMN = "Battery, Plug-in Hybrid, or Fuel Cell EV (BEV, PHEV or FCEV)"


class IzevRequest(BaseModel):
    """One immutable English administrative-data snapshot, checked before I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    component_key: str = Field(min_length=1)
    component_meta: SourceComponent
    resource_id: UUID
    url: str
    cache_path: Path
    expected_md5: str = Field(pattern=r"^[0-9a-f]{32}$")
    expected_bytes: int = Field(gt=0)
    expected_model_years: tuple[int, ...]
    expected_provinces: tuple[str, ...]
    expected_powertrains: tuple[Literal["bev", "phev", "fcev"], ...]
    expected_request_date_from: date
    expected_request_date_to: date
    required_columns: tuple[str, ...]
    download_hosts: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_contract(self) -> "IzevRequest":
        parsed = urlparse(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "open.canada.ca"
            or parsed.username
            or parsed.password
            or not parsed.path.endswith(".csv")
            or f"/resource/{self.resource_id}/download/" not in parsed.path
        ):
            raise ValueError("iZEV URL must identify its registered official HTTPS CSV")
        if not self.cache_path.is_absolute() or self.cache_path.suffix != ".csv":
            raise ValueError("iZEV cache must be an absolute CSV path")
        if (
            not self.expected_model_years
            or self.expected_model_years != tuple(sorted(set(self.expected_model_years)))
            or min(self.expected_model_years) < 1900
        ):
            raise ValueError("iZEV model years must be nonempty, sorted and unique")
        if self.expected_request_date_from > self.expected_request_date_to:
            raise ValueError("iZEV request-date bounds are reversed")
        for name in ("required_columns", "expected_provinces", "expected_powertrains"):
            values = getattr(self, name)
            if not values or len(values) != len(set(values)) or any(not v.strip() for v in values):
                raise ValueError(f"iZEV {name} must contain distinct nonblank values")
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    return load_harmonization_rules(bundle, SOURCE)


def build_requests(bundle: ConfigBundle) -> list[IzevRequest]:
    """Resolve only the registered resources; a build does not discover new ones."""
    source = bundle.sources.sources[SOURCE]
    if source.status != "active" or not source.components:
        raise ValueError("iZEV requires an active source with registered components")
    cache_root = resolve_input_path(bundle, "cache").resolve()
    requests_to_fetch = []
    for key, component in source.components.items():
        adapter = dict(source.adapter)
        adapter.update(component.adapter)
        cache = (cache_root / str(adapter["cache_path"])).resolve()
        if not cache.is_relative_to(cache_root):
            raise ValueError("iZEV cache_path must stay inside the configured cache")
        requests_to_fetch.append(IzevRequest(
            component_key=str(key), component_meta=component,
            resource_id=adapter["resource_id"], url=adapter["url"], cache_path=cache,
            expected_md5=adapter["expected_md5"], expected_bytes=adapter["expected_bytes"],
            expected_model_years=adapter["expected_model_years"],
            expected_provinces=adapter["expected_provinces"],
            expected_powertrains=adapter["expected_powertrains"],
            expected_request_date_from=adapter["expected_request_date_from"],
            expected_request_date_to=adapter["expected_request_date_to"],
            required_columns=adapter["required_columns"],
            download_hosts=source.update.download_hosts if source.update else (),
        ))
    if len({r.resource_id for r in requests_to_fetch}) != len(requests_to_fetch):
        raise ValueError("iZEV resource IDs must be unique")
    ordered = sorted(requests_to_fetch, key=lambda r: r.expected_request_date_from)
    if any(a.expected_request_date_to >= b.expected_request_date_from for a, b in zip(ordered, ordered[1:])):
        raise ValueError("Registered iZEV request-date coverage overlaps between resources")
    return requests_to_fetch


def validate_cache(request: IzevRequest) -> None:
    """Reject a republished or incomplete source before parsing it."""
    if not request.cache_path.is_file():
        raise FileNotFoundError(f"Missing registered iZEV cache: {request.cache_path}")
    if request.cache_path.stat().st_size != request.expected_bytes:
        raise ValueError(f"iZEV {request.component_key} byte count differs from the registry")
    with request.cache_path.open("rb") as handle:
        actual = hashlib.file_digest(handle, "md5").hexdigest()
    if actual != request.expected_md5:
        raise ValueError(f"iZEV {request.component_key} MD5 differs from the registry")


def fetch_to_cache(request: IzevRequest, *, timeout: int = 120) -> str:
    """Acquire only missing, pinned resources and never replace registered bytes."""
    if request.cache_path.exists():
        validate_cache(request)
        return "cached"
    response = requests.get(request.url, timeout=timeout)
    response.raise_for_status()
    final = urlparse(response.url)
    if final.scheme != "https" or final.hostname not in {"open.canada.ca", *request.download_hosts}:
        raise ValueError("iZEV download redirected to an unregistered host")
    content = response.content
    if len(content) != request.expected_bytes or hashlib.md5(content).hexdigest() != request.expected_md5:
        raise ValueError("iZEV download differs from the pinned bytes; use the explicit source updater")
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=request.cache_path.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
    try:
        temporary.write_bytes(content)
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    LOGGER.info("Downloaded pinned iZEV resource %s (%s bytes)", request.component_key, len(content))
    return "downloaded"


def parse_csv(
    request: IzevRequest, *, rules: dict[str, Any], enforce_coverage: bool = True,
) -> pd.DataFrame:
    """Preserve each incentive transaction, including indistinguishable repeats.

    Fully empty publisher rows are audited and removed. Repeated complete rows are
    separate incentives: the source exposes no transaction ID for deduplication.
    Native request timing, vehicle year, recipient and dealership geography remain
    separate. No market-share or family inference belongs in this adapter.
    """
    with request.cache_path.open(encoding=rules["encoding"], newline="") as handle:
        header = [name.strip() for name in next(csv.reader(handle), [])]
    if not header or len(header) != len(set(header)):
        raise ValueError("iZEV has empty or duplicate physical column names")
    raw = pd.read_csv(request.cache_path, encoding=rules["encoding"], dtype=str, skip_blank_lines=False)
    raw.columns = raw.columns.str.strip()
    if raw.columns.duplicated().any():
        raise ValueError("iZEV has duplicate physical column names")
    if missing := set(request.required_columns) - set(raw.columns):
        raise ValueError(f"iZEV missing required columns: {sorted(missing)}")
    stripped = raw.fillna("").apply(lambda col: col.str.strip())
    blank = stripped.eq("").all(axis=1)
    native = raw.loc[~blank].copy()
    normalized = stripped.loc[~blank]
    if normalized.empty:
        raise ValueError("iZEV source has no incentive records")
    if normalized[list(request.required_columns)].eq("").any().any():
        raise ValueError("iZEV contains incomplete nonblank incentive records")
    transaction_type = normalized["Purchase or Lease"]
    if not transaction_type.isin(["Purchase", "Lease"]).all():
        raise ValueError("iZEV contains unknown purchase/lease categories")
    request_dates = pd.to_datetime(normalized["Incentive Request Date"], format="%Y-%m-%d", errors="coerce")
    if request_dates.isna().any():
        raise ValueError("iZEV contains invalid incentive request dates")
    years = pd.to_numeric(normalized["Vehicle Year"], errors="coerce")
    calendar = pd.to_numeric(normalized["Calendar Year"], errors="coerce")
    if years.isna().any() or years.mod(1).ne(0).any() or years.lt(1900).any():
        raise ValueError("iZEV contains invalid vehicle model years")
    if calendar.isna().any() or calendar.ne(request_dates.dt.year).any():
        raise ValueError("iZEV Calendar Year disagrees with its request date")
    province_map = rules["province_map"]
    if any(not isinstance(value, str) or len(value) != 2 or not value.isupper() for value in province_map.values()):
        raise ValueError("iZEV province_map values must be uppercase postal province codes")
    province = normalized["Recipient Province/Territory"].map(province_map)
    if province.isna().any():
        unknown = sorted(normalized.loc[province.isna(), "Recipient Province/Territory"].unique())
        raise ValueError(f"iZEV has unmapped recipient provinces: {unknown}")
    powertrain = normalized[POWERTRAIN_COLUMN].str.lower()
    if not powertrain.isin(["bev", "phev", "fcev"]).all():
        raise ValueError("iZEV contains unknown propulsion types")
    model_years, provinces, powertrains = sorted(years.astype(int).unique().tolist()), sorted(province.unique()), sorted(powertrain.unique())
    date_from, date_to = request_dates.min().date(), request_dates.max().date()
    actual = (model_years, provinces, powertrains, date_from, date_to)
    expected = (list(request.expected_model_years), sorted(request.expected_provinces), sorted(request.expected_powertrains), request.expected_request_date_from, request.expected_request_date_to)
    if enforce_coverage and actual != expected:
        raise ValueError(f"iZEV {request.component_key} coverage differs from the registered snapshot: {actual}")
    result = pd.DataFrame({
        "source_id": SOURCE, "component": request.component_key,
        "resource_id": str(request.resource_id), "source_row": native.index + 2,
        "resource_version": request.component_meta.version or "",
        "resource_url": request.url, "model_year": years.astype(int),
        "request_year": request_dates.dt.year,
        "request_month": request_dates.dt.month,
        "request_date": request_dates.dt.strftime("%Y-%m-%d"),
        "province": province, "make": normalized["Vehicle Make"],
        "model": normalized["Vehicle Model"], "powertrain": powertrain, "count": 1,
    }, index=native.index)
    result = pd.concat([result, native], axis=1).reset_index(drop=True)
    result.attrs["audit"] = {
        "component": request.component_key, "physical_rows": len(raw),
        "blank_rows": int(blank.sum()), "rows": len(result),
        "transaction_types": {str(key): int(value) for key, value in transaction_type.value_counts().items()},
        "identical_native_rows_retained": int(native.duplicated().sum()),
        "model_years": model_years, "provinces": provinces,
        "powertrains": powertrains, "request_date_from": date_from.isoformat(),
        "request_date_to": date_to.isoformat(),
    }
    return result


def fetch_izev(bundle: ConfigBundle, *, download: bool = False) -> dict[str, Any]:
    """Replay all registered iZEV records offline unless acquisition is explicit."""
    rules = module_rules(bundle)
    folder = resolve_artifact_path(bundle, SOURCE)
    expected_folder = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    if folder.resolve() != expected_folder.resolve():
        raise ValueError("iZEV artifact route and configured interim_subdir disagree")
    frames, manifest = [], []
    for request in build_requests(bundle):
        status = fetch_to_cache(request) if download else "cached"
        validate_cache(request)
        frame = parse_csv(request, rules=rules)
        audit = frame.attrs["audit"]
        LOGGER.info("iZEV %s: %s records, %s blank rows removed, %s identical native rows retained", request.component_key, audit["rows"], audit["blank_rows"], audit["identical_native_rows_retained"])
        frames.append(frame)
        manifest.append({
            **audit, "source_id": SOURCE, "resource_id": str(request.resource_id),
            "url": request.url, "cached_file": str(request.cache_path),
            "md5": request.expected_md5, "bytes": request.expected_bytes,
            "sha256": file_sha256(request.cache_path), "cache_status": status,
        })
    result = pd.concat(frames, ignore_index=True)
    write_dataframe_atomic(result, folder / rules["output_file"])
    write_text_atomic(json.dumps({"source_id": SOURCE, "rows": len(result), "components": manifest}, indent=2) + "\n", folder / rules["manifest_file"])
    LOGGER.info("Wrote %s observed iZEV incentive records to %s", len(result), folder)
    return {"rows": len(result), "output_dir": str(folder), "manifest": manifest}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--download", action="store_true", help="Acquire missing pinned caches explicitly")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    fetch_izev(load_config_bundle(args.scenario), download=args.download)


if __name__ == "__main__":
    main()
