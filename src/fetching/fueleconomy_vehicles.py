"""Fetch and normalize the pinned FuelEconomy.gov vehicle-class evidence."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, model_validator

from utils import ConfigBundle, load_config_bundle, load_harmonization_rules, resolve_input_path
from validation.config_models import SourceComponent


SOURCE_KEY = "fueleconomy_gov_vehicle_data"
RULE_KEY = "fueleconomy_vehicle_data"


class FuelEconomyVehicleRequest(BaseModel):
    """Validated request for one pinned FuelEconomy.gov vehicle ZIP."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: Literal["fueleconomy_gov_vehicle_data"]
    component_key: Literal["vehicles"]
    component_meta: SourceComponent
    url: str
    cache_path: Path
    archive_member: str
    expected_sha256: str
    expected_bytes: int
    expected_model_year_from: int
    expected_model_year_to: int
    required_columns: tuple[str, ...]
    required_non_null_columns: tuple[str, ...]
    output_file: str

    @model_validator(mode="after")
    def validate_request(self) -> "FuelEconomyVehicleRequest":
        parsed = urlparse(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www.fueleconomy.gov"
            or not parsed.path.endswith("vehicles.csv.zip")
        ):
            raise ValueError(
                "FuelEconomy.gov vehicle URL must be the official HTTPS ZIP"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", self.expected_sha256):
            raise ValueError("FuelEconomy.gov expected_sha256 must be lowercase SHA-256")
        if self.expected_bytes <= 0:
            raise ValueError("FuelEconomy.gov expected_bytes must be positive")
        if (
            not self.cache_path.is_absolute()
            or self.cache_path.name.casefold().endswith(".csv.zip") is False
        ):
            raise ValueError(
                "FuelEconomy.gov cache path must be an absolute .csv.zip path"
            )
        if Path(self.archive_member).name != self.archive_member:
            raise ValueError("FuelEconomy.gov archive_member must be a filename")
        if self.expected_model_year_from > self.expected_model_year_to:
            raise ValueError("FuelEconomy.gov model-year bounds are reversed")
        if not self.required_columns or not self.required_non_null_columns:
            raise ValueError("FuelEconomy.gov physical column contracts cannot be empty")
        unexpected = sorted(
            set(self.required_non_null_columns) - set(self.required_columns)
        )
        if unexpected:
            raise ValueError(
                "FuelEconomy.gov non-null columns are not required columns: "
                f"{unexpected}"
            )
        if Path(self.output_file).name != self.output_file:
            raise ValueError("FuelEconomy.gov output_file must be a filename")
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load FuelEconomy.gov selection and normalization rules."""
    return load_harmonization_rules(bundle, RULE_KEY)


def build_request(bundle: ConfigBundle) -> FuelEconomyVehicleRequest:
    """Build the one exact configured vehicle-data request."""
    source = bundle.sources["sources"][SOURCE_KEY]
    if set(source.components) != {"vehicles"}:
        raise ValueError("FuelEconomy.gov source must define only the vehicles component")
    component = source.components["vehicles"]
    adapter = component.adapter
    configured_path = str(adapter["cache_path"]).replace("\\", "/")
    for prefix in ("inputs/cache/", "inputs/0_cache/"):
        if configured_path.startswith(prefix):
            configured_path = configured_path.removeprefix(prefix)
            break
    rules = module_rules(bundle)
    return FuelEconomyVehicleRequest(
        source_id=SOURCE_KEY,
        component_key="vehicles",
        component_meta=component,
        url=str(adapter["url"]),
        cache_path=resolve_input_path(bundle, "cache", configured_path),
        archive_member=str(adapter["archive_member"]),
        expected_sha256=str(adapter["expected_sha256"]),
        expected_bytes=int(adapter["expected_bytes"]),
        expected_model_year_from=int(adapter["expected_model_year_from"]),
        expected_model_year_to=int(adapter["expected_model_year_to"]),
        required_columns=tuple(str(value) for value in adapter["required_columns"]),
        required_non_null_columns=tuple(
            str(value) for value in adapter["required_non_null_columns"]
        ),
        output_file=str(rules["output_file"]),
    )


def file_sha256(path: Path) -> str:
    """Return a lowercase SHA-256 for one file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_cache(request: FuelEconomyVehicleRequest) -> None:
    """Validate bytes, hash, CRC, and the exact configured archive member."""
    actual_bytes = request.cache_path.stat().st_size
    if actual_bytes != request.expected_bytes:
        raise ValueError(
            f"FuelEconomy.gov cache has {actual_bytes} bytes; "
            f"expected {request.expected_bytes}"
        )
    actual_sha256 = file_sha256(request.cache_path)
    if actual_sha256 != request.expected_sha256:
        raise ValueError(
            f"FuelEconomy.gov cache SHA-256 is {actual_sha256}; "
            f"expected {request.expected_sha256}"
        )
    with zipfile.ZipFile(request.cache_path) as archive:
        if archive.testzip() is not None:
            raise ValueError("FuelEconomy.gov cache failed ZIP CRC validation")
        members = [
            name
            for name in archive.namelist()
            if Path(name).name.casefold() == request.archive_member.casefold()
        ]
        if members != [request.archive_member]:
            raise ValueError(
                "FuelEconomy.gov cache must contain exactly the configured "
                f"{request.archive_member!r} member; found {members}"
            )


def fetch_to_cache(
    request: FuelEconomyVehicleRequest,
    *,
    timeout: int = 120,
) -> str:
    """Download the pinned ZIP atomically, or reuse a validated cache."""
    if request.cache_path.is_file():
        validate_cache(request)
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=request.cache_path.parent,
        prefix=f".{request.cache_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        response = requests.get(request.url, timeout=timeout)
        response.raise_for_status()
        temporary.write_bytes(response.content)
        temporary_request = request.model_copy(update={"cache_path": temporary})
        validate_cache(temporary_request)
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    return "downloaded"


def read_selected_vehicle_columns(
    request: FuelEconomyVehicleRequest,
) -> pd.DataFrame:
    """Read only the four configured source columns from vehicles.csv."""
    with zipfile.ZipFile(request.cache_path) as archive:
        with archive.open(request.archive_member) as source:
            frame = pd.read_csv(source, usecols=list(request.required_columns))
    missing = sorted(set(request.required_columns) - set(frame.columns))
    if missing:
        raise ValueError(
            "FuelEconomy.gov vehicles.csv missing columns: " + ", ".join(missing)
        )
    null_counts = frame.loc[:, request.required_non_null_columns].isna().sum()
    invalid_nulls = null_counts.loc[null_counts.gt(0)]
    if not invalid_nulls.empty:
        raise ValueError(
            "FuelEconomy.gov required columns contain nulls: "
            + ", ".join(
                f"{column}={int(count)}"
                for column, count in invalid_nulls.items()
            )
        )
    years = pd.to_numeric(frame["year"], errors="raise").astype(int)
    actual_bounds = (int(years.min()), int(years.max()))
    expected_bounds = (
        request.expected_model_year_from,
        request.expected_model_year_to,
    )
    if actual_bounds != expected_bounds:
        raise ValueError(
            f"FuelEconomy.gov model-year bounds are {actual_bounds}; "
            f"expected {expected_bounds}"
        )
    frame["year"] = years
    return frame.loc[:, request.required_columns].copy()


def normalize_vehicle_classes(
    frame: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, list[str]]:
    """Map EPA VClass labels to NRCan labels without guessing unresolved classes."""
    selected = [str(value) for value in rules["selected_columns"]]
    if list(frame.columns) != selected:
        raise ValueError(
            f"FuelEconomy.gov selected columns differ: {list(frame.columns)} != {selected}"
        )
    class_map = {
        str(source): str(target)
        for source, target in rules["vclass_to_nrcan"].items()
    }
    unresolved_rules = {
        str(source): str(reason)
        for source, reason in rules["unresolved_vclasses"].items()
    }
    observed = set(frame["VClass"].astype(str).unique())
    unexpected = sorted(observed - set(class_map) - set(unresolved_rules))
    if unexpected:
        raise ValueError(
            "FuelEconomy.gov has unexpected VClass labels: " + ", ".join(unexpected)
        )

    normalized = frame.rename(
        columns={
            str(source): str(target)
            for source, target in rules["output_columns"].items()
        }
    )
    normalized["nrcan_vehicle_class"] = normalized["Source vehicle class"].map(
        class_map
    )
    normalized["class_normalization_status"] = normalized[
        "nrcan_vehicle_class"
    ].notna().map({True: "mapped", False: "unresolved"})
    normalized["class_normalization_note"] = normalized[
        "Source vehicle class"
    ].map(unresolved_rules).fillna("")
    normalized["evidence_source"] = SOURCE_KEY
    warnings = [
        f"{label}: {int(normalized['Source vehicle class'].eq(label).sum())} rows; {reason}"
        for label, reason in sorted(unresolved_rules.items())
        if label in observed
    ]
    return normalized, warnings


def write_dataframe_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Publish a CSV by atomic same-directory replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_csv(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_text_atomic(lines: list[str], path: Path) -> None:
    """Publish deterministic warning text by atomic replacement."""
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
        handle.write("\n".join(lines))
        if lines:
            handle.write("\n")
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    download: bool = True,
) -> Path:
    """Fetch, validate, normalize, and publish FuelEconomy.gov evidence."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    request = build_request(bundle)
    if download:
        cache_status = fetch_to_cache(request)
    else:
        if not request.cache_path.is_file():
            raise FileNotFoundError(
                "FuelEconomy.gov cache is required during --no-download execution: "
                f"{request.cache_path}"
            )
        validate_cache(request)
        cache_status = "cached"
    raw = read_selected_vehicle_columns(request)
    normalized, warnings = normalize_vehicle_classes(raw, rules=rules)
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    write_dataframe_atomic(normalized, output_dir / request.output_file)
    manifest = pd.DataFrame(
        [
            {
                "source_key": request.source_id,
                "component_key": request.component_key,
                "label": request.component_meta.label,
                "url": request.url,
                "cache_path": str(request.cache_path),
                "cache_status": cache_status,
                "sha256": request.expected_sha256,
                "bytes": request.expected_bytes,
                "archive_member": request.archive_member,
                "selected_columns": "|".join(request.required_columns),
                "rows": len(normalized),
                "model_year_from": int(normalized["Model year"].min()),
                "model_year_to": int(normalized["Model year"].max()),
                "unresolved_rows": int(
                    normalized["class_normalization_status"].eq("unresolved").sum()
                ),
                "output_file": request.output_file,
            }
        ]
    )
    write_dataframe_atomic(manifest, output_dir / str(rules["manifest_file"]))
    write_text_atomic(warnings, output_dir / str(rules["warnings_file"]))
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    parser.add_argument("--no-download", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    output_dir = fetch_and_normalize(
        args.scenario,
        download=not args.no_download,
    )
    logging.info("Wrote FuelEconomy.gov evidence to %s", output_dir)


if __name__ == "__main__":
    main()
