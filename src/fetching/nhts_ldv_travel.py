"""Acquire and normalize the pinned NHTS public-use evidence, explicitly opt-in."""

import argparse
import csv
import hashlib
import json
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_parameter_yaml,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)

SOURCE = "nhts_household_travel"
LOGGER = logging.getLogger(__name__)


class SurveyTable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    member: str
    expected_rows: int = Field(gt=0)
    keys: tuple[str, ...] = Field(min_length=1)


class NhtsRequest(BaseModel):
    """Source-native physical contract, checked before any I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    component: str
    url: str
    cache_path: Path
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0)
    kind: Literal["zip", "xlsx", "pdf"]
    archive_members: tuple[str, ...] = ()
    tables: dict[str, SurveyTable] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_source(self):
        parsed = urlparse(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "nhts.ornl.gov"
            or parsed.username
            or parsed.password
        ):
            raise ValueError(
                "NHTS resources must use the registered official HTTPS host"
            )
        if not self.cache_path.is_absolute():
            raise ValueError("NHTS cache path must be absolute")
        if self.kind == "zip" and (not self.archive_members or not self.tables):
            raise ValueError("NHTS ZIP requires explicit members and table contracts")
        if len(set(self.archive_members)) != len(self.archive_members):
            raise ValueError("NHTS archive members must be distinct")
        if any(t.member not in self.archive_members for t in self.tables.values()):
            raise ValueError("NHTS tables must reference registered archive members")
        return self


def build_requests(bundle: ConfigBundle) -> list[NhtsRequest]:
    source = bundle.sources.sources[SOURCE]
    if source.status != "active":
        raise ValueError("NHTS evidence source is inactive")
    root = resolve_input_path(bundle, "cache").resolve()
    result = []
    for key, component in source.components.items():
        payload = dict(component.adapter)
        path = (root / payload.pop("cache_path")).resolve()
        if not path.is_relative_to(root):
            raise ValueError("NHTS cache path escapes its configured cache")
        result.append(NhtsRequest(component=str(key), cache_path=path, **payload))
    if {r.component for r in result} != {
        "public_use",
        "codebook",
        "user_guide",
        "release_notes",
    }:
        raise ValueError(
            "NHTS requires the public-use data and its three registered documentation assets"
        )
    return result


def validate_cache(request: NhtsRequest) -> None:
    if not request.cache_path.is_file():
        raise FileNotFoundError(
            f"Missing pinned NHTS cache {request.cache_path}; acquire with --download"
        )
    if (
        request.cache_path.stat().st_size != request.expected_bytes
        or file_sha256(request.cache_path) != request.expected_sha256
    ):
        raise ValueError(
            f"NHTS {request.component} differs from its pinned bytes; review source refresh explicitly"
        )
    if request.kind == "zip":
        with zipfile.ZipFile(request.cache_path) as archive:
            if (
                sorted(archive.namelist()) != sorted(request.archive_members)
                or archive.testzip() is not None
            ):
                raise ValueError(
                    "NHTS ZIP member catalogue or CRC differs from registration"
                )
    elif request.kind == "pdf" and not request.cache_path.read_bytes().startswith(
        b"%PDF-"
    ):
        raise ValueError("NHTS documentation is not a PDF")
    elif request.kind == "xlsx" and not zipfile.is_zipfile(request.cache_path):
        raise ValueError("NHTS codebook is not an XLSX workbook")


def fetch_to_cache(request: NhtsRequest, *, timeout: int = 120) -> str:
    """Acquire missing pinned bytes only; never silently replace a cached release."""
    if request.cache_path.exists():
        validate_cache(request)
        return "cached"
    response = requests.get(request.url, timeout=timeout)
    response.raise_for_status()
    if (
        urlparse(response.url).scheme != "https"
        or urlparse(response.url).hostname != "nhts.ornl.gov"
    ):
        raise ValueError("NHTS download redirected outside its registered host")
    if (
        len(response.content) != request.expected_bytes
        or hashlib.sha256(response.content).hexdigest() != request.expected_sha256
    ):
        raise ValueError("NHTS downloaded release differs from registered bytes")
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=request.cache_path.parent, suffix=".tmp", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        temporary.write_bytes(response.content)
        validate_cache(request.model_copy(update={"cache_path": temporary}))
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    LOGGER.info("Acquired NHTS %s: %s bytes", request.component, request.expected_bytes)
    return "downloaded"


def read_tables(
    request: NhtsRequest, columns: dict[str, list[str]]
) -> dict[str, pd.DataFrame]:
    """Read selected native numeric audit fields without filtering survey records."""
    validate_cache(request)
    result = {}
    with zipfile.ZipFile(request.cache_path) as archive:
        for name, table in request.tables.items():
            with archive.open(table.member) as stream:
                header = next(csv.reader(line.decode("utf-8-sig") for line in stream))
            if len(header) != len(set(header)) or not set(columns[name]).issubset(
                header
            ):
                raise ValueError(
                    f"NHTS {name} has duplicate or missing required physical columns"
                )
            with archive.open(table.member) as stream:
                frame = pd.read_csv(stream, usecols=columns[name])
            if (
                len(frame) != table.expected_rows
                or frame[list(table.keys)].isna().any().any()
                or frame.duplicated(list(table.keys)).any()
            ):
                raise ValueError(
                    f"NHTS {name} row count or native keys differ from registration"
                )
            for column in columns[name]:
                frame[column] = pd.to_numeric(frame[column], errors="raise")
            frame.insert(0, "source_row", range(2, len(frame) + 2))
            frame.insert(0, "source_member", table.member)
            frame.insert(0, "source_key", SOURCE)
            result[name] = frame
    return result


def normalize_survey(bundle: ConfigBundle, *, download: bool = False) -> dict:
    settings = load_parameter_yaml(bundle, "travel_survey.yaml")
    if (
        settings["source"] != SOURCE
        or settings["edition"] != bundle.sources.sources[SOURCE].adapter["edition"]
    ):
        raise ValueError("Travel survey settings and source edition disagree")
    requests_to_read = build_requests(bundle)
    assets = []
    for request in requests_to_read:
        status = fetch_to_cache(request) if download else "cached"
        validate_cache(request)
        assets.append(
            {
                "component": request.component,
                "url": request.url,
                "sha256": request.expected_sha256,
                "bytes": request.expected_bytes,
                "cache_status": status,
            }
        )
    tables = read_tables(
        next(r for r in requests_to_read if r.component == "public_use"),
        settings["normalization"]["columns"],
    )
    directory = resolve_artifact_path(bundle, "nhts_interim")
    files = settings["normalization"]["files"]
    hashes = {}
    for name, frame in tables.items():
        path = directory / files[name]
        write_dataframe_atomic(frame, path)
        hashes[name] = file_sha256(path)
        LOGGER.info(
            "Normalized NHTS %s: %s native records; no survey exclusions",
            name,
            len(frame),
        )
    manifest = {
        "source": SOURCE,
        "source_id": bundle.sources.sources[SOURCE].source_id,
        "edition": settings["edition"],
        "assets": assets,
        "rows": {key: len(frame) for key, frame in tables.items()},
        "normalized_sha256": hashes,
        "data_quality": bundle.sources.sources[SOURCE].data_quality.model_dump(),
        "purpose": "Opt-in diagnostic evidence; no production parameter insertion",
    }
    write_text_atomic(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        directory / files["manifest"],
    )
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument(
        "--download",
        action="store_true",
        help="Acquire missing registered source assets explicitly",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    normalize_survey(load_config_bundle(args.scenario), download=args.download)


if __name__ == "__main__":
    main()
