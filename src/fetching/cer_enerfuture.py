"""Fetch, cache, and normalize selected CER Canada's Energy Future tables."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, model_validator

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.config_models import SourceSpec


SOURCE_ID = "cer_canadas_energy_future"


class CerEnergyFutureError(ValueError):
    """Raised when a configured CER source contract is unavailable or changed."""


class CerTableRequest(BaseModel):
    """Resolved source/cache/output contract for one CER CSV component."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    component_id: str
    title: str
    edition: int
    doi: str
    open_government_record_id: str
    url: str
    cache_path: Path
    output_file: str
    required_columns: tuple[str, ...]
    expected_scenarios: tuple[str, ...]
    first_year: int
    last_year: int

    @model_validator(mode="after")
    def validate_request(self) -> "CerTableRequest":
        parsed = urlparse(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"CER URL is invalid: {self.url}")
        if not self.cache_path.is_absolute() or self.cache_path.suffix.casefold() != ".csv":
            raise ValueError(f"CER cache must be an absolute .csv path: {self.cache_path}")
        if self.edition <= 0 or self.first_year > self.last_year:
            raise ValueError("CER edition and year range must be valid")
        if not self.component_id or not self.title or not self.required_columns:
            raise ValueError("CER component identity and required columns are required")
        if not self.expected_scenarios:
            raise ValueError("CER expected scenarios must be configured")
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load CER selectors and output rules."""
    return load_harmonization_rules(bundle, "cer_enerfuture")


def _source(bundle: ConfigBundle) -> SourceSpec:
    source = bundle.sources.sources.get(SOURCE_ID)
    if not isinstance(source, SourceSpec):
        raise CerEnergyFutureError(f"sources.yaml missing {SOURCE_ID}")
    return source


def _mapping_value(mapping: dict[Any, Any], key: int | str) -> Any:
    for candidate in (key, str(key)):
        if candidate in mapping:
            return mapping[candidate]
    if isinstance(key, str) and key.isdigit() and int(key) in mapping:
        return mapping[int(key)]
    raise KeyError(key)


def configured_edition(bundle: ConfigBundle, requested: int | None = None) -> int:
    """Resolve an allowed edition from a CLI override or the source default."""
    editions = _source(bundle).adapter.get("editions")
    if not isinstance(editions, dict) or not isinstance(editions.get("allowed"), dict):
        raise CerEnergyFutureError("CER edition metadata is missing from sources.yaml")
    selection = bundle.scenario.sources.selections.get(SOURCE_ID)
    scenario_edition = selection.edition if selection is not None else None
    edition = int(
        requested
        if requested is not None
        else scenario_edition
        if scenario_edition is not None
        else editions.get("default", 0)
    )
    try:
        _mapping_value(editions["allowed"], edition)
    except KeyError as exc:
        allowed = sorted(int(value) for value in editions["allowed"])
        raise CerEnergyFutureError(
            f"Unsupported CER edition {edition}; configured editions are {allowed}"
        ) from exc
    return edition


def configured_scenario(bundle: ConfigBundle, rules: dict[str, Any]) -> str:
    """Resolve the CER scenario marker from scenario YAML or rule default."""
    selection = bundle.scenario.sources.selections.get(SOURCE_ID)
    if selection is not None and selection.scenario is not None:
        return selection.scenario
    return str(rules["default_scenario"])


def scenario_region_labels(
    bundle: ConfigBundle, requested: list[str] | None = None
) -> list[str]:
    """Map scenario region codes to CER geography labels."""
    region_codes = (
        requested
        if requested is not None
        else list(bundle.scenario.geography.regions)
    )
    if not region_codes:
        raise CerEnergyFutureError("Scenario regions must be a non-empty list")
    geography = module_rules(bundle).get("geography")
    if not isinstance(geography, dict):
        raise CerEnergyFutureError("CER geography rules are missing")

    labels: list[str] = []
    unknown: list[str] = []
    for code in region_codes:
        configured = geography.get(str(code))
        if configured is None:
            unknown.append(str(code))
            continue
        values = configured if isinstance(configured, list) else [configured]
        labels.extend(str(value) for value in values)
    if unknown:
        raise CerEnergyFutureError(
            f"No CER geography mapping for scenario regions: {unknown}"
        )
    return list(dict.fromkeys(labels))


def build_requests(
    bundle: ConfigBundle, *, edition: int | None = None
) -> list[CerTableRequest]:
    """Build the three configured CER component requests for one edition."""
    source = _source(bundle)
    selected_edition = configured_edition(bundle, edition)
    rules = module_rules(bundle)
    selected_scenario = configured_scenario(bundle, rules)
    rules = {**rules, "default_scenario": selected_scenario}
    component_rules = rules.get("components")
    if not isinstance(component_rules, dict):
        raise CerEnergyFutureError("CER component rules are missing")
    if set(source.components) != set(component_rules):
        raise CerEnergyFutureError(
            "CER source/rule component IDs differ: "
            f"sources={sorted(source.components)}, rules={sorted(component_rules)}"
        )

    editions = source.adapter["editions"]["allowed"]
    edition_meta = _mapping_value(editions, selected_edition)
    access = source.adapter.get("access", {})
    url_template = str(access.get("url_template", ""))
    cache_template = str(source.adapter.get("cache_path_template", ""))
    first_year, last_year = (int(value) for value in edition_meta["year_range"])
    requests_to_make: list[CerTableRequest] = []
    for component_id, component in source.components.items():
        if isinstance(component.label, list):
            raise CerEnergyFutureError(
                f"CER component {component_id} label must be one title"
            )
        resource = str(component.adapter.get("resource", ""))
        table_rules = component_rules[component_id]
        requests_to_make.append(
            CerTableRequest(
                component_id=component_id,
                title=component.label,
                edition=selected_edition,
                doi=str(edition_meta["doi"]),
                open_government_record_id=str(
                    edition_meta["open_government_record_id"]
                ),
                url=url_template.format(
                    edition=selected_edition,
                    resource=resource,
                ),
                cache_path=resolve_input_path(
                    bundle,
                    "cache",
                    cache_template.format(
                        edition=selected_edition,
                        resource=resource,
                    ),
                ),
                output_file=str(table_rules["output_file"]),
                required_columns=tuple(str(value) for value in table_rules["required_columns"]),
                expected_scenarios=tuple(
                    str(value) for value in edition_meta["scenarios"]
                ),
                first_year=first_year,
                last_year=last_year,
            )
        )
    return requests_to_make


def fetch_to_cache(
    request: CerTableRequest,
    *,
    session: requests.Session | None = None,
    timeout: int = 120,
) -> str:
    """Fetch one official CSV atomically, or reuse its existing cache."""
    if request.cache_path.exists():
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = request.cache_path.with_suffix(request.cache_path.suffix + ".part")
    client = session or requests.Session()
    try:
        response = client.get(request.url, stream=True, timeout=timeout)
        response.raise_for_status()
        with temporary.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        if not temporary.exists() or temporary.stat().st_size == 0:
            raise CerEnergyFutureError(f"CER download was empty: {request.url}")
        os.replace(temporary, request.cache_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return "downloaded"


def _drop_unnamed_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[:, ~frame.columns.astype(str).str.match(r"^Unnamed:")].copy()


def read_and_validate_source(request: CerTableRequest) -> pd.DataFrame:
    """Read a cached CSV and validate its physical and semantic source contract."""
    if not request.cache_path.is_file() or request.cache_path.stat().st_size == 0:
        raise FileNotFoundError(request.cache_path)
    frame = _drop_unnamed_columns(pd.read_csv(request.cache_path))
    missing = sorted(set(request.required_columns) - set(frame.columns))
    if missing:
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} missing columns {missing}; "
            f"available columns are {list(frame.columns)}"
        )
    if frame.empty:
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} contains no rows"
        )

    scenarios = set(frame["Scenario"].dropna().astype(str))
    missing_scenarios = sorted(set(request.expected_scenarios) - scenarios)
    if missing_scenarios:
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} missing scenarios "
            f"{missing_scenarios}"
        )
    frame["Year"] = pd.to_numeric(frame["Year"], errors="coerce")
    frame["Value"] = pd.to_numeric(frame["Value"], errors="coerce")
    if frame[["Year", "Value"]].isna().any().any():
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} has invalid year/value rows"
        )
    if not np.isfinite(frame["Value"]).all():
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} has non-finite values"
        )
    actual_range = (int(frame["Year"].min()), int(frame["Year"].max()))
    expected_range = (request.first_year, request.last_year)
    if actual_range != expected_range:
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} year range changed: "
            f"expected {expected_range}, got {actual_range}"
        )
    frame["Year"] = frame["Year"].astype(int)
    return frame


def _macro_unit(variable: str) -> str:
    if "(" in variable and variable.endswith(")"):
        return variable.rsplit("(", maxsplit=1)[1][:-1]
    return "source-labelled"


def normalize_component(
    frame: pd.DataFrame,
    request: CerTableRequest,
    *,
    region_labels: list[str],
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Normalize one CER table to its configured transport-focused interim contract."""
    table_rules = rules["components"][request.component_id]
    selected = frame.copy()
    if request.component_id == "macro-indicators":
        selected = selected[selected["Region"].eq(str(table_rules["region"]))].copy()
        mappings: list[pd.DataFrame] = []
        for variable_key, selector in table_rules["variables"].items():
            mask = selected["Variable"].astype(str).str.startswith(str(selector))
            matched = selected.loc[mask].copy()
            if matched.empty:
                raise CerEnergyFutureError(
                    f"CER macro indicator {variable_key!r} not found for edition "
                    f"{request.edition}"
                )
            matched["variable_key"] = str(variable_key)
            mappings.append(matched)
        selected = pd.concat(mappings, ignore_index=True)
        selected["unit"] = selected["Variable"].astype(str).map(_macro_unit)
    elif request.component_id == "end-use-demand":
        allowed_regions = list(region_labels)
        if bool(table_rules.get("include_canada_total")):
            allowed_regions.append("Canada")
        selected = selected[
            selected["Region"].isin(allowed_regions)
            & selected["Sector"].eq(str(table_rules["sector"]))
        ].copy()
        selected["variable_key"] = (
            selected["Variable"]
            .astype(str)
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
            .str.strip("_")
        )
        selected["unit"] = str(table_rules["unit"])
    elif request.component_id == "end-use-prices":
        selected = selected[
            selected["Region"].isin(region_labels)
            & selected["Sector"].eq(str(table_rules["sector"]))
            & selected["Variable"].isin(table_rules["variables"])
        ].copy()
        selected["variable_key"] = selected["Variable"].astype(str).str.lower()
        selected["unit"] = str(table_rules["unit"])
    else:
        raise CerEnergyFutureError(f"No CER normalizer for {request.component_id}")

    if selected.empty:
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} selectors produced no rows"
        )
    selected = selected.rename(
        columns={
            "Scenario": "scenario",
            "Region": "region",
            "Variable": "variable",
            "Year": "year",
            "Value": "value",
            "Sector": "sector",
        }
    )
    selected["is_default_scenario"] = selected["scenario"].eq(
        str(rules["default_scenario"])
    )
    selected["source_id"] = SOURCE_ID
    selected["edition"] = request.edition
    selected["component_id"] = request.component_id
    selected["source_url"] = request.url
    selected["cached_file"] = str(request.cache_path)
    selected["doi"] = request.doi
    selected["open_government_record_id"] = request.open_government_record_id

    keys = ["scenario", "region", "variable", "year"]
    if "sector" in selected.columns:
        keys.append("sector")
    if selected.duplicated(keys).any():
        raise CerEnergyFutureError(
            f"CER {request.component_id} {request.edition} has duplicate normalized keys"
        )
    first = ["scenario", "region"]
    if "sector" in selected.columns:
        first.append("sector")
    first.extend(["variable", "variable_key", "year", "value", "unit"])
    provenance = [
        "is_default_scenario",
        "source_id",
        "edition",
        "component_id",
        "source_url",
        "cached_file",
        "doi",
        "open_government_record_id",
    ]
    return selected[first + provenance].sort_values(keys).reset_index(drop=True)


def write_outputs(
    *,
    normalized: dict[str, pd.DataFrame],
    requests_by_component: dict[str, CerTableRequest],
    manifest_rows: list[dict[str, Any]],
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Write normalized tables, manifest, and warnings."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for component_id, frame in normalized.items():
        frame.to_csv(
            output_dir / requests_by_component[component_id].output_file,
            index=False,
        )
    pd.DataFrame(manifest_rows).to_csv(output_dir / rules["manifest_file"], index=False)
    (output_dir / rules["warnings_file"]).write_text(
        "\n".join(warnings) + ("\n" if warnings else ""),
        encoding="utf-8",
    )


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    edition: int | None = None,
    regions: list[str] | None = None,
    download: bool = True,
    session: requests.Session | None = None,
) -> Path:
    """Fetch/cache one CER edition and write transport-focused interim CSVs."""
    bundle = load_config_bundle(scenario_path)
    selected_edition = configured_edition(bundle, edition)
    rules = module_rules(bundle)
    selected_scenario = configured_scenario(bundle, rules)
    rules = {**rules, "default_scenario": selected_scenario}
    region_labels = scenario_region_labels(bundle, regions)
    output_dir = resolve_input_path(
        bundle,
        "interim",
        str(rules["interim_subdir_template"]).format(edition=selected_edition),
    )
    normalized: dict[str, pd.DataFrame] = {}
    requests_by_component: dict[str, CerTableRequest] = {}
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    requests_to_process = build_requests(bundle, edition=selected_edition)
    expected_scenarios = set(requests_to_process[0].expected_scenarios)
    if selected_scenario not in expected_scenarios:
        raise CerEnergyFutureError(
            f"CER scenario {selected_scenario!r} is unavailable in edition "
            f"{selected_edition}; expected one of {sorted(expected_scenarios)}"
        )

    for request in requests_to_process:
        if download:
            cache_status = fetch_to_cache(request, session=session)
        elif request.cache_path.exists():
            cache_status = "cached"
        else:
            raise FileNotFoundError(
                f"CER offline cache missing for edition {request.edition} "
                f"component {request.component_id}: {request.cache_path}"
            )
        raw = read_and_validate_source(request)
        output = normalize_component(
            raw,
            request,
            region_labels=region_labels,
            rules=rules,
        )
        normalized[request.component_id] = output
        requests_by_component[request.component_id] = request
        manifest_rows.append(
            {
                "source_id": SOURCE_ID,
                "edition": request.edition,
                "component_id": request.component_id,
                "title": request.title,
                "doi": request.doi,
                "open_government_record_id": request.open_government_record_id,
                "url": request.url,
                "cached_file": str(request.cache_path),
                "sha256": file_sha256(request.cache_path),
                "cache_status": cache_status,
                "input_rows": len(raw),
                "selected_rows": len(output),
                "output_file": request.output_file,
                "regions": "|".join(region_labels),
                "default_scenario": str(rules["default_scenario"]),
                "status": "ok",
            }
        )

    write_outputs(
        normalized=normalized,
        requests_by_component=requests_by_component,
        manifest_rows=manifest_rows,
        warnings=warnings,
        output_dir=output_dir,
        rules=rules,
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
        help="Scenario YAML controlling regions and configured paths.",
    )
    parser.add_argument(
        "--edition",
        type=int,
        default=None,
        help="CER report edition; defaults to the edition configured in sources.yaml.",
    )
    parser.add_argument(
        "--regions",
        nargs="*",
        default=None,
        help="Optional scenario-region override; defaults to scenario regions.",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Require and reuse all three cached CSVs without network access.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    try:
        output_dir = fetch_and_normalize(
            args.scenario,
            edition=args.edition,
            regions=args.regions,
            download=not args.no_download,
        )
    except (FileNotFoundError, CerEnergyFutureError, requests.RequestException) as exc:
        raise SystemExit(f"CER Energy Future adapter failed: {exc}") from exc
    logging.info("Wrote CER Energy Future interim outputs to %s", output_dir)


if __name__ == "__main__":
    main()
