"""Pinned, opt-in HATCH/Norway/literature acquisition and native normalization.

This adapter is deliberately absent from production source readiness and orchestration.
Missing caches fail offline. Downloads and supplied-PDF registration are explicit CLI
actions; existing bytes are verified before reuse and never silently repaired.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import re
import time
import ast
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import requests
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pypdf import PdfReader

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    resolve_artifact_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)

LOGGER = logging.getLogger(__name__)
SOURCE_KEYS = (
    "hatch_original_growth_evidence",
    "hatch_extended_growth_evidence",
    "eafo_norway_growth_evidence",
    "technology_growth_literature",
    "temoa_growth_reference",
)


class EvidenceAsset(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    key: str
    cache_path: str
    url: str | None = None
    supplied_name: str | None = None
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bytes: int = Field(ge=0)
    format: Literal["csv", "xlsx", "yaml", "html", "pdf", "python", "json"]
    encoding: str = "utf-8"

    @model_validator(mode="after")
    def access_identity(self):
        if (self.url is None) == (self.supplied_name is None):
            raise ValueError(
                "Exactly one download URL or supplied filename is required"
            )
        if Path(self.cache_path).is_absolute() or ".." in Path(self.cache_path).parts:
            raise ValueError("Evidence cache paths must be relative and bounded")
        if self.url and not self.url.startswith("https://"):
            raise ValueError("Evidence download URLs must use HTTPS")
        if self.bytes == 0 and self.format != "python":
            raise ValueError("Only an empty Python package marker may have zero bytes")
        if self.supplied_name and Path(self.supplied_name).name != self.supplied_name:
            raise ValueError("Supplied PDF name must be a basename")
        return self


class GrowthSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    assets: list[EvidenceAsset]

    @model_validator(mode="after")
    def unique_assets(self):
        if len({a.key for a in self.assets}) != len(self.assets):
            raise ValueError("Duplicate evidence asset keys")
        if len({a.cache_path for a in self.assets}) != len(self.assets):
            raise ValueError("Duplicate evidence asset paths")
        return self


def registered_assets(bundle: ConfigBundle) -> list[tuple[str, EvidenceAsset]]:
    assets = []
    for key in SOURCE_KEYS:
        source = bundle.sources.sources[key]
        request = GrowthSourceRequest.model_validate(source.adapter)
        assets.extend((key, asset) for asset in request.assets)
    if len({asset.key for _, asset in assets}) != len(assets) or len(
        {asset.cache_path for _, asset in assets}
    ) != len(assets):
        raise ValueError(
            "Evidence asset keys/paths must be unique across source families"
        )
    return assets


def validate_asset(content: bytes, asset: EvidenceAsset) -> None:
    if (
        len(content) != asset.bytes
        or hashlib.sha256(content).hexdigest() != asset.sha256
    ):
        raise ValueError(
            f"Evidence bytes/checksum changed: {asset.key}; review a new source snapshot"
        )
    if asset.format == "pdf":
        if not content.startswith(b"%PDF-") or not PdfReader(io.BytesIO(content)).pages:
            raise ValueError(f"Invalid PDF: {asset.key}")
    elif asset.format == "xlsx":
        if not content.startswith(b"PK"):
            raise ValueError(f"Invalid XLSX signature: {asset.key}")
        pd.ExcelFile(io.BytesIO(content))
    elif asset.format == "csv":
        frame = pd.read_csv(io.BytesIO(content), encoding=asset.encoding)
        if frame.empty or len(frame.columns) < 2:
            raise ValueError(f"Empty/invalid evidence CSV: {asset.key}")
    elif asset.format == "html":
        if b"<html" not in content.lower():
            raise ValueError(f"Invalid HTML: {asset.key}")
    elif asset.format == "yaml":
        if not isinstance(yaml.safe_load(content.decode(asset.encoding)), (dict, list)):
            raise ValueError(f"Invalid evidence YAML: {asset.key}")
    elif asset.format == "python":
        ast.parse(content.decode(asset.encoding), filename=asset.key)
    elif asset.format == "json":
        json.loads(content.decode(asset.encoding))


def download_asset(url: str, session: requests.Session) -> bytes:
    """Honor EAFO's decimal-seconds Retry-After; bounded transient retries."""
    for attempt in range(5):
        response = session.get(url, timeout=(15, 60))
        if response.status_code not in {429, 500, 502, 503, 504}:
            response.raise_for_status()
            return response.content
        if attempt == 4:
            response.raise_for_status()
        retry = response.headers.get("Retry-After", "")
        pause = (
            min(60.0, float(retry))
            if re.fullmatch(r"\d+(\.\d+)?", retry)
            else 2**attempt
        )
        LOGGER.warning(
            "Retrying evidence HTTP %s after %ss: %s", response.status_code, pause, url
        )
        time.sleep(pause)
    raise RuntimeError("Unreachable evidence retry state")


def acquire_evidence(
    bundle: ConfigBundle, *, offline: bool = True, papers_dir: Path | None = None
) -> pd.DataFrame:
    """Verify immutable caches, or explicitly acquire missing registered bytes."""
    rows = []
    cache = resolve_artifact_path(bundle, "technology_growth_cache").resolve()
    # Validate every adapter before allowing any I/O.
    assets = registered_assets(bundle)
    with requests.Session() as session:
        for key, asset in assets:
            path = (cache / asset.cache_path).resolve()
            if not path.is_relative_to(cache):
                raise ValueError("Evidence path escapes configured cache")
            if path.exists():
                content = path.read_bytes()
                status = "verified_cache"
            elif offline:
                raise FileNotFoundError(
                    f"Offline evidence missing: {path}; use --download-missing"
                )
            else:
                if asset.supplied_name:
                    if papers_dir is None:
                        raise FileNotFoundError(
                            f"Use --papers-dir to register {asset.supplied_name}"
                        )
                    content = (papers_dir / asset.supplied_name).read_bytes()
                    status = "registered_supplied_pdf"
                else:
                    content = download_asset(asset.url, session)
                    status = "downloaded"
                validate_asset(content, asset)
                path.parent.mkdir(parents=True, exist_ok=True)
                # Exclusive creation preserves any concurrent/current cache.
                with path.open("xb") as handle:
                    handle.write(content)
            validate_asset(content, asset)
            source = bundle.sources.sources[key]
            rows.append(
                {
                    "source_key": key,
                    "asset_key": asset.key,
                    "version": source.version,
                    "citation": source.citation,
                    "cache_path": path.relative_to(bundle.repo_root).as_posix(),
                    "sha256": asset.sha256,
                    "bytes": asset.bytes,
                    "format": asset.format,
                    "access": asset.url or asset.supplied_name,
                    "status": "verified",
                }
            )
            LOGGER.info("Evidence %s %s (%s bytes)", asset.key, status, asset.bytes)
    manifest = pd.DataFrame(rows)
    write_dataframe_atomic(
        manifest,
        resolve_artifact_path(
            bundle, "technology_growth_interim", "source_manifest.csv"
        ),
    )
    return manifest


def asset_path(bundle: ConfigBundle, asset_key: str) -> Path:
    matches = [a for _, a in registered_assets(bundle) if a.key == asset_key]
    if len(matches) != 1:
        raise KeyError(f"Expected exactly one registered evidence asset: {asset_key}")
    return resolve_artifact_path(
        bundle, "technology_growth_cache", matches[0].cache_path
    )


def normalize_hatch(path: Path, release: str) -> pd.DataFrame:
    """Long rows include interior missing observations and literal zero values."""
    wide = pd.read_csv(path, low_memory=False).copy()
    region = "Country Code" if "Country Code" in wide else "Region"
    required = {
        "ID",
        region,
        "Technology Name",
        "Metric",
        "Unit",
        "Variable",
        "Data Source",
    }
    if not required.issubset(wide):
        raise ValueError(f"HATCH physical schema missing {required - set(wide)}")
    if wide.ID.isna().any() or wide.ID.duplicated().any():
        raise ValueError("HATCH raw IDs are missing or duplicated")
    years = [c for c in wide if re.fullmatch(r"\d{4}", c)]
    if not years:
        raise ValueError("HATCH has no observation years")
    wide["raw_row"] = np.arange(len(wide)) + 2
    rename = {
        "ID": "native_id",
        region: "country",
        "Country Name": "country_name",
        "Technology Name": "technology",
        "Metric": "metric",
        "Unit": "unit",
        "Variable": "variable",
        "Data Source": "native_source",
        "Spatial Scale": "spatial_scale",
    }
    meta = [c for c in rename if c in wide] + ["raw_row"]
    long = wide.melt(
        id_vars=meta, value_vars=years, var_name="year", value_name="value"
    ).rename(columns=rename)
    long["year"] = long.year.astype(int)
    long["value"] = pd.to_numeric(long.value, errors="raise")
    if np.isinf(long.value.to_numpy(dtype=float)).any():
        raise ValueError("HATCH contains infinite observation values")
    # Retain the observation span; no need for centuries of exterior blank cells.
    observed = (
        long.dropna(subset=["value"]).groupby("native_id").year.agg(["min", "max"])
    )
    long = long.join(observed, on="native_id")
    long = long.loc[long.year.ge(long["min"]) & long.year.le(long["max"])].drop(
        columns=["min", "max"]
    )
    long["release"] = release
    long["series_id"] = release + ":" + long.native_id
    long["observation_status"] = np.select(
        [long.value.isna(), long.value.eq(0), long.value.lt(0)],
        ["missing", "zero", "negative"],
        default="observed",
    )
    return long.sort_values(["series_id", "year"]).reset_index(drop=True)


def normalize_eafo(
    path: Path, *, vehicle_class: str, quantity: str, snapshot_year: int
) -> pd.DataFrame:
    wide = pd.read_csv(path)
    if "YEAR" not in wide or not {"BEV", "PHEV", "H2", "CNG"}.issubset(wide):
        raise ValueError(f"Unexpected EAFO export schema: {path.name}")
    if wide.YEAR.duplicated().any():
        raise ValueError("Duplicate EAFO year")
    long = wide.melt(id_vars="YEAR", var_name="powertrain", value_name="value").rename(
        columns={"YEAR": "year"}
    )
    long["value"] = pd.to_numeric(long.value, errors="raise")
    long["year"] = pd.to_numeric(long.year, errors="raise").astype(int)
    if long.value.lt(0).any() or np.isinf(long.value.to_numpy(dtype=float)).any():
        raise ValueError("EAFO contains negative/infinite counts or shares")
    share = quantity.endswith("share")
    if share and long.value.gt(100).any():
        raise ValueError("EAFO percentage exceeds 100")
    long["release"] = "eafo"
    long["native_id"] = vehicle_class + ":" + quantity + ":" + long.powertrain
    long["series_id"] = "eafo:" + long.native_id
    long["technology"] = vehicle_class + " " + long.powertrain
    long["country"] = "NOR"
    long["country_name"] = "Norway"
    long["metric"] = quantity
    long["unit"] = "percent" if share else "vehicles"
    long["variable"] = quantity
    long["native_source"] = "EAFO"
    long["spatial_scale"] = "National"
    long["raw_row"] = np.tile(np.arange(len(wide)) + 2, len(wide.columns) - 1)
    long["complete_year"] = long.year.lt(snapshot_year)
    long["vehicle_class"] = vehicle_class
    long["powertrain"] = long.powertrain
    long["quantity"] = quantity
    long["observation_status"] = np.select(
        [long.value.isna(), long.value.eq(0)], ["missing", "zero"], default="observed"
    )
    return long.sort_values(["series_id", "year"]).reset_index(drop=True)


def extract_paper_pages(bundle: ConfigBundle, *, persist: bool = True) -> pd.DataFrame:
    pages = []
    for key, asset in registered_assets(bundle):
        if asset.format != "pdf":
            continue
        path = asset_path(bundle, asset.key)
        for i, page in enumerate(PdfReader(path).pages):
            pages.append(
                {
                    "source_key": key,
                    "asset_key": asset.key,
                    "pdf_page": i + 1,
                    "text": page.extract_text() or "",
                    "sha256": file_sha256(path),
                }
            )
    frame = pd.DataFrame(pages)
    if persist:
        write_dataframe_atomic(
            frame,
            resolve_artifact_path(
                bundle, "technology_growth_interim", "paper_pages.csv"
            ),
        )
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    parser.add_argument("--download-missing", action="store_true")
    parser.add_argument("--papers-dir", type=Path)
    args = parser.parse_args()
    bundle = load_config_bundle(args.scenario)
    log = (
        resolve_repo_path(bundle.repo_root, bundle.paths.outputs.logs)
        / "technology_growth_acquisition.log"
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler(), logging.FileHandler(log, encoding="utf-8")],
    )
    manifest = acquire_evidence(
        bundle, offline=not args.download_missing, papers_dir=args.papers_dir
    )
    pages = extract_paper_pages(bundle)
    write_text_atomic(
        json.dumps(
            {"assets": len(manifest), "pdf_pages": len(pages), "diagnostic_only": True},
            indent=2,
        ),
        resolve_artifact_path(
            bundle, "technology_growth_validation", "acquisition.json"
        ),
    )


if __name__ == "__main__":
    main()
