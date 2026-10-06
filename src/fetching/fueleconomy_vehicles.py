"""Fetch and normalize the pinned FuelEconomy.gov vehicle-class evidence."""

from __future__ import annotations

import argparse
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

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.config_models import SourceComponent


SOURCE_KEY = "fueleconomy_gov_vehicle_data"
RULE_KEY = "fueleconomy_vehicle_data"


class FuelEconomyVehicleRequest(BaseModel):
    """Validated request for one pinned FuelEconomy.gov vehicle ZIP."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: Literal["fueleconomy_gov_vehicle_data"]
    component_key: Literal["vehicles", "range_evidence"]
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


def build_request(bundle: ConfigBundle, component_key: str = "vehicles") -> FuelEconomyVehicleRequest:
    """Build the one exact configured vehicle-data request."""
    source = bundle.sources["sources"][SOURCE_KEY]
    if source.status != "active":
        raise ValueError("FuelEconomy.gov source is inactive")
    if "vehicles" not in source.components or set(source.components) - {"vehicles", "range_evidence"}:
        raise ValueError("FuelEconomy.gov source requires vehicles and only supported explicit components")
    if component_key not in source.components:
        raise ValueError(f"FuelEconomy.gov {component_key} component is not registered")
    component = source.component(component_key)
    adapter = dict(source.components["vehicles"].adapter)
    if component_key == "range_evidence":
        extension = dict(component.adapter)
        if extension.pop("cache_component", None) != "vehicles":
            raise ValueError("Range evidence must reuse the pinned classification cache")
        adapter.update(extension)
    configured_path = str(adapter["cache_path"]).replace("\\", "/")
    for prefix in ("inputs/cache/", "inputs/0_cache/"):
        if configured_path.startswith(prefix):
            configured_path = configured_path.removeprefix(prefix)
            break
    rules = module_rules(bundle)
    return FuelEconomyVehicleRequest(
        source_id=SOURCE_KEY,
        component_key=component_key,
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
        output_file=str(rules["range_output_file"] if component_key == "range_evidence" else rules["output_file"]),
    )


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
    """Read only the component's explicitly authorized columns from vehicles.csv."""
    with zipfile.ZipFile(request.cache_path) as archive:
        with archive.open(request.archive_member) as source:
            frame = pd.read_csv(source, usecols=list(request.required_columns), low_memory=False)
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


def normalize_range_evidence(frame: pd.DataFrame, *, rules: dict[str, Any]) -> pd.DataFrame:
    """Preserve source fields; PHEV range is secondary electricity/CD, never total range."""
    selected = frame.loc[frame.year.eq(rules["range_model_year"]) & frame.atvType.isin(rules["range_powertrain_map"])].copy()
    if selected.id.isna().any() or selected.id.duplicated().any():
        raise ValueError("FuelEconomy range evidence requires unique source vehicle IDs")
    selected["powertrain"] = selected.atvType.map(rules["range_powertrain_map"])
    bev = selected.powertrain.eq("BEV")
    selected["cd_range_miles"] = pd.to_numeric(selected[rules["bev_range_column"]].where(
        bev, selected[rules["phev_range_column"]]), errors="coerce")
    electric = selected.fuelType1.eq(rules["electricity_label"]).where(
        bev, selected.fuelType2.eq(rules["electricity_label"]))
    valid = electric & selected.cd_range_miles.gt(0) & selected.cd_range_miles.lt(float("inf"))
    selected["range_status"] = valid.map({True: "valid", False: "unresolved"})
    selected.loc[~valid, "cd_range_miles"] = float("nan")
    selected["range_field"] = selected.powertrain.map({"BEV": rules["bev_range_column"], "PHEV": rules["phev_range_column"]})
    selected["range_definition"] = selected.powertrain.map({"BEV": "EPA combined electric range", "PHEV": "EPA electricity/CD range, including blended CD operation; not total driving range"})
    return selected.sort_values("id", kind="stable").reset_index(drop=True)


def fetch_range_evidence(bundle: ConfigBundle, *, download: bool = False) -> pd.DataFrame:
    """Independent range extension; classification artifacts and cache pin are preserved."""
    request = build_request(bundle, "range_evidence")
    if download:
        fetch_to_cache(request)
    else:
        validate_cache(request)
    result = normalize_range_evidence(read_selected_vehicle_columns(request), rules=module_rules(bundle))
    output_dir = resolve_input_path(bundle, "interim", module_rules(bundle)["interim_subdir"])
    write_dataframe_atomic(result, output_dir / request.output_file)
    write_dataframe_atomic(pd.DataFrame([{
        "component": request.component_key, "sha256": request.expected_sha256,
        "bytes": request.expected_bytes, "selected_columns": "|".join(request.required_columns),
        "rows": len(result), "unresolved_ranges": int(result.range_status.eq("unresolved").sum()),
    }]), output_dir / module_rules(bundle)["range_manifest_file"])
    logging.getLogger(__name__).info("FuelEconomy range extension: %s rows, %s unresolved ranges", len(result), result.range_status.eq("unresolved").sum())
    return result


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
    write_text_atomic(
        "\n".join(warnings) + ("\n" if warnings else ""),
        output_dir / str(rules["warnings_file"]),
        newline="\n",
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--range-evidence", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    if args.range_evidence:
        result = fetch_range_evidence(load_config_bundle(args.scenario), download=not args.no_download)
        logging.info("Wrote %s FuelEconomy.gov range records", len(result))
        return
    output_dir = fetch_and_normalize(
        args.scenario,
        download=not args.no_download,
    )
    logging.info("Wrote FuelEconomy.gov evidence to %s", output_dir)


if __name__ == "__main__":
    main()
