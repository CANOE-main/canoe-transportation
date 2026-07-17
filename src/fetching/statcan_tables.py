"""Fetch, cache, and normalize configured Statistics Canada transport tables."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

import numpy as np
import pandas as pd
import requests

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_input_path,
)


SOURCE_ID = "statcan_transport_tables"


class StatCanSourceError(ValueError):
    """Raised when a configured StatCan source contract is unavailable or changed."""


@dataclass(frozen=True)
class StatCanTableRequest:
    """Resolved API/cache/output contract for one configured StatCan table."""

    table_id: str
    product_id: int
    title: str
    metadata_url: str
    download_api_url: str
    archive_cache_path: Path
    metadata_cache_path: Path
    output_file: str
    normalizer: str
    table_rules: dict[str, Any]


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load StatCan harmonization and selector rules."""
    return load_harmonization_rules(bundle, "statcan_tables")


def scenario_regions(bundle: ConfigBundle, requested: list[str] | None = None) -> list[str]:
    """Resolve and validate one or more scenario region codes."""
    regions = requested if requested is not None else bundle.scenario.get("regions", [])
    if not isinstance(regions, list) or not regions:
        raise StatCanSourceError("Scenario regions must be a non-empty list")
    configured = module_rules(bundle)["geography"]
    unknown = [str(region) for region in regions if str(region) not in configured]
    if unknown:
        raise StatCanSourceError(f"No StatCan geography mapping for scenario regions: {unknown}")
    return list(dict.fromkeys(str(region) for region in regions))


def build_requests(bundle: ConfigBundle) -> list[StatCanTableRequest]:
    """Build the five explicit table requests from source and rule configuration."""
    source = bundle.sources["sources"].get(SOURCE_ID)
    if not isinstance(source, dict):
        raise StatCanSourceError(f"sources.yaml missing {SOURCE_ID}")
    tables = source.get("tables")
    rules = module_rules(bundle)
    rule_tables = rules.get("tables")
    if not isinstance(tables, dict) or not isinstance(rule_tables, dict):
        raise StatCanSourceError("StatCan source and rule table mappings are required")
    if set(tables) != set(rule_tables):
        raise StatCanSourceError(
            "StatCan source/rule table IDs differ: "
            f"sources={sorted(tables)}, rules={sorted(rule_tables)}"
        )

    access = source["access"]
    language = str(access["language"])
    requests_to_make: list[StatCanTableRequest] = []
    for table_id, table_meta in tables.items():
        product_id = int(table_meta["product_id"])
        table_rules = rule_tables[table_id]
        requests_to_make.append(
            StatCanTableRequest(
                table_id=table_id,
                product_id=product_id,
                title=str(table_meta["title"]),
                metadata_url=str(access["cube_metadata_url"]),
                download_api_url=str(access["full_table_url_template"]).format(
                    product_id=product_id,
                    language=language,
                ),
                archive_cache_path=resolve_input_path(
                    bundle,
                    "cache",
                    str(source["cache_path_template"]).format(product_id=product_id),
                ),
                metadata_cache_path=resolve_input_path(
                    bundle,
                    "cache",
                    str(source["metadata_cache_path_template"]).format(
                        product_id=product_id
                    ),
                ),
                output_file=str(table_rules["output_file"]),
                normalizer=str(table_rules["normalizer"]),
                table_rules=table_rules,
            )
        )
    return requests_to_make


def resolve_download_url(
    request: StatCanTableRequest,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
) -> str:
    """Resolve the current full-table ZIP through the official WDS endpoint."""
    client = session or requests.Session()
    response = client.get(request.download_api_url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "SUCCESS" or not payload.get("object"):
        raise StatCanSourceError(
            f"StatCan download resolution failed for {request.table_id}: {payload}"
        )
    return str(payload["object"])


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def fetch_metadata_to_cache(
    request: StatCanTableRequest,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
) -> str:
    """Cache the authoritative cube metadata response if it is not already present."""
    if request.metadata_cache_path.exists():
        return "cached"
    client = session or requests.Session()
    response = client.post(
        request.metadata_url,
        json=[{"productId": request.product_id}],
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    metadata_object(payload, request)
    _write_json_atomic(request.metadata_cache_path, payload)
    return "downloaded"


def fetch_archive_to_cache(
    request: StatCanTableRequest,
    *,
    session: requests.Session | None = None,
    timeout: int = 300,
) -> tuple[str, str]:
    """Cache the resolved authoritative full-table ZIP if absent."""
    if request.archive_cache_path.exists():
        return "cached", ""
    client = session or requests.Session()
    download_url = resolve_download_url(request, session=client, timeout=timeout)
    request.archive_cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = request.archive_cache_path.with_suffix(
        request.archive_cache_path.suffix + ".part"
    )
    try:
        with client.get(download_url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        with ZipFile(temporary) as archive:
            if not archive.namelist():
                raise StatCanSourceError(f"Downloaded ZIP is empty: {download_url}")
        os.replace(temporary, request.archive_cache_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return "downloaded", download_url


def ensure_cached_artifacts(
    request: StatCanTableRequest,
    *,
    download: bool,
    session: requests.Session | None = None,
) -> tuple[str, str, str]:
    """Fetch missing artifacts or fail deterministically in offline mode."""
    if not download:
        missing = [
            path
            for path in (request.metadata_cache_path, request.archive_cache_path)
            if not path.exists()
        ]
        if missing:
            raise FileNotFoundError(
                f"Offline StatCan cache is incomplete for {request.table_id}: {missing}"
            )
        return "cached", "cached", ""
    metadata_status = fetch_metadata_to_cache(request, session=session)
    archive_status, download_url = fetch_archive_to_cache(request, session=session)
    return metadata_status, archive_status, download_url


def metadata_object(payload: Any, request: StatCanTableRequest) -> dict[str, Any]:
    """Extract and validate one getCubeMetadata response object."""
    if not isinstance(payload, list) or len(payload) != 1:
        raise StatCanSourceError(
            f"Unexpected metadata response for {request.table_id}: expected one item"
        )
    item = payload[0]
    if not isinstance(item, dict) or item.get("status") != "SUCCESS":
        raise StatCanSourceError(f"Metadata request failed for {request.table_id}: {item}")
    metadata = item.get("object")
    if not isinstance(metadata, dict):
        raise StatCanSourceError(f"Metadata object missing for {request.table_id}")
    if int(metadata.get("productId", -1)) != request.product_id:
        raise StatCanSourceError(
            f"Metadata product ID mismatch for {request.table_id}: "
            f"{metadata.get('productId')}"
        )
    return metadata


def read_cached_metadata(request: StatCanTableRequest) -> dict[str, Any]:
    """Read a cached WDS metadata artifact."""
    try:
        payload = json.loads(request.metadata_cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StatCanSourceError(
            f"Cannot read cached metadata for {request.table_id}: {exc}"
        ) from exc
    return metadata_object(payload, request)


def validate_metadata_contract(
    metadata: dict[str, Any],
    request: StatCanTableRequest,
) -> None:
    """Fail if configured dimensions or selector members no longer exist."""
    if str(metadata.get("cubeTitleEn", "")).strip() != request.title:
        raise StatCanSourceError(
            f"Title changed for {request.table_id}: {metadata.get('cubeTitleEn')!r}"
        )
    dimensions = metadata.get("dimension")
    if not isinstance(dimensions, list):
        raise StatCanSourceError(f"Metadata dimensions missing for {request.table_id}")
    by_name = {str(item.get("dimensionNameEn", "")).strip(): item for item in dimensions}
    expected_dimensions = [
        str(name) for name in request.table_rules["metadata_dimensions"]
    ]
    missing_dimensions = [name for name in expected_dimensions if name not in by_name]
    if missing_dimensions:
        raise StatCanSourceError(
            f"Required dimensions changed for {request.table_id}: {missing_dimensions}"
        )
    for dimension, selected_members in request.table_rules.get("selectors", {}).items():
        if dimension not in by_name:
            raise StatCanSourceError(
                f"Selector dimension missing for {request.table_id}: {dimension}"
            )
        available = {
            str(member.get("memberNameEn", "")).strip()
            for member in by_name[dimension].get("member", [])
        }
        missing_members = [
            str(member) for member in selected_members if str(member) not in available
        ]
        if missing_members:
            raise StatCanSourceError(
                f"Selector members changed for {request.table_id} {dimension}: "
                f"{missing_members}"
            )


def main_csv_member(request: StatCanTableRequest) -> str:
    """Find the PID data CSV inside a full-table archive."""
    expected = f"{request.product_id}.csv".casefold()
    try:
        with ZipFile(request.archive_cache_path) as archive:
            matches = [
                name
                for name in archive.namelist()
                if Path(name).name.casefold() == expected
            ]
    except (OSError, BadZipFile) as exc:
        raise StatCanSourceError(
            f"Invalid StatCan ZIP for {request.table_id}: {exc}"
        ) from exc
    if len(matches) != 1:
        raise StatCanSourceError(
            f"Expected one {request.product_id}.csv data member in "
            f"{request.archive_cache_path}, found {matches}"
        )
    return matches[0]


def _freight_region_frames(
    frame: pd.DataFrame,
    *,
    regions: list[str],
    geography_rules: dict[str, Any],
) -> list[pd.DataFrame]:
    origin_column = "GEO"
    destination_column = "Geography, destination of shipments"
    origin = frame[origin_column].fillna("").astype(str)
    destination = frame[destination_column].fillna("").astype(str)
    selected: list[pd.DataFrame] = []
    for region in regions:
        tokens = [str(value).casefold() for value in geography_rules[region]["freight_contains"]]
        origin_match = origin.str.casefold().map(
            lambda value: any(token in value for token in tokens)
        )
        destination_match = destination.str.casefold().map(
            lambda value: any(token in value for token in tokens)
        )
        keep = origin_match | destination_match
        if not keep.any():
            continue
        region_frame = frame.loc[keep].copy()
        region_frame["scenario_region"] = region
        region_frame["region_match"] = np.select(
            [origin_match.loc[keep] & destination_match.loc[keep], origin_match.loc[keep]],
            ["both", "origin"],
            default="destination",
        )
        selected.append(region_frame)
    return selected


def _select_chunk(
    chunk: pd.DataFrame,
    *,
    request: StatCanTableRequest,
    regions: list[str],
    geography_rules: dict[str, Any],
) -> list[pd.DataFrame]:
    selected = chunk
    for column, members in request.table_rules.get("selectors", {}).items():
        selected = selected[selected[column].isin(members)]
    if request.normalizer == "freight":
        return _freight_region_frames(
            selected,
            regions=regions,
            geography_rules=geography_rules,
        )

    labels_to_region = {
        str(geography_rules[region]["label"]).strip(): region for region in regions
    }
    geography = selected["GEO"].fillna("").astype(str).str.strip()
    selected = selected[geography.isin(labels_to_region)].copy()
    if selected.empty:
        return []
    selected["scenario_region"] = (
        selected["GEO"].astype(str).str.strip().map(labels_to_region)
    )
    return [selected]


def read_selected_table(
    request: StatCanTableRequest,
    *,
    regions: list[str],
    geography_rules: dict[str, Any],
    chunksize: int,
    provenance_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, str, int]:
    """Stream the data member and retain only configured dimensions/geographies."""
    member = main_csv_member(request)
    required_columns = list(
        dict.fromkeys(
            [str(column) for column in request.table_rules["required_columns"]]
            + [str(column) for column in (provenance_columns or [])]
        )
    )
    frames: list[pd.DataFrame] = []
    input_rows = 0
    with ZipFile(request.archive_cache_path) as archive, archive.open(member) as handle:
        reader = pd.read_csv(handle, chunksize=chunksize, low_memory=False)
        for chunk in reader:
            input_rows += len(chunk)
            missing = [column for column in required_columns if column not in chunk.columns]
            if missing:
                raise StatCanSourceError(
                    f"Required CSV columns changed for {request.table_id}: {missing}; "
                    f"available={list(chunk.columns)}"
                )
            for column in chunk.columns:
                dtype = chunk[column].dtype
                if pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
                    chunk[column] = chunk[column].map(
                        lambda value: value.strip() if isinstance(value, str) else value
                    )
            frames.extend(
                _select_chunk(
                    chunk,
                    request=request,
                    regions=regions,
                    geography_rules=geography_rules,
                )
            )
    if not frames:
        raise StatCanSourceError(
            f"No rows matched configured regions/selectors for {request.table_id}: {regions}"
        )
    return pd.concat(frames, ignore_index=True), member, input_rows


def _snake_case(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", value.strip()).strip("_").lower()
    return text


def normalize_table(
    frame: pd.DataFrame,
    *,
    request: StatCanTableRequest,
    source_member: str,
    scalar_multipliers: dict[str, float],
) -> pd.DataFrame:
    """Normalize names and attach source/cache provenance without aggregating."""
    special_names = {
            "REF_DATE": "reference_period",
            "GEO": (
                "geography_origin_of_shipments"
                if request.normalizer == "freight"
                else "geography"
            ),
            "UOM": "units",
            "VALUE": "value",
            "SCALAR_FACTOR": "scalar_factor",
        }
    renamed = frame.rename(
        columns={column: _snake_case(str(column)) for column in frame.columns}
        | special_names
    ).copy()
    renamed["reference_period"] = renamed["reference_period"].astype(str)
    renamed["value"] = pd.to_numeric(renamed["value"], errors="coerce")
    scalar_keys = renamed["scalar_factor"].fillna("units").astype(str).str.strip().str.lower()
    unknown_scalars = sorted(set(scalar_keys) - set(scalar_multipliers))
    if unknown_scalars:
        raise StatCanSourceError(
            f"Unconfigured StatCan scalar factors for {request.table_id}: {unknown_scalars}"
        )
    renamed["scalar_multiplier"] = scalar_keys.map(scalar_multipliers).astype(float)
    renamed["scaled_value"] = renamed["value"] * renamed["scalar_multiplier"]
    renamed.insert(0, "table_id", request.table_id)
    renamed.insert(1, "product_id", request.product_id)
    renamed["cached_zip"] = str(request.archive_cache_path)
    renamed["source_member"] = source_member
    leading = [
        "table_id",
        "product_id",
        "scenario_region",
        "reference_period",
        "geography",
    ]
    return renamed[[column for column in leading if column in renamed] + [
        column for column in renamed.columns if column not in leading
    ]]


def build_ldv_history(
    archived: pd.DataFrame,
    current: pd.DataFrame,
    *,
    rules: dict[str, Any],
    warnings: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Annualize current quarters and apply the explicit archived/current policy."""
    policy = rules["ldv_history"]
    group_columns = [
        "scenario_region",
        "geography",
        "fuel_type",
        "vehicle_type",
        "statistics",
        "units",
    ]
    archived_rows = archived.dropna(subset=["scaled_value"]).copy()
    current_rows = current.dropna(subset=["scaled_value"]).copy()
    archived_rows["reference_year"] = archived_rows["reference_period"].str[:4].astype(int)
    current_rows["reference_year"] = current_rows["reference_period"].str[:4].astype(int)

    annual_current = (
        current_rows.groupby(group_columns + ["reference_year"], dropna=False)
        .agg(
            scaled_value=("scaled_value", "sum"),
            source_period_count=("reference_period", "nunique"),
        )
        .reset_index()
    )
    expected_periods = int(policy["complete_periods_per_year"])
    incomplete = annual_current["source_period_count"] != expected_periods
    if incomplete.any():
        warnings.append(
            f"{incomplete.sum()} current LDV annual series had fewer/more than "
            f"{expected_periods} quarters and were excluded"
        )
        annual_current = annual_current[~incomplete].copy()
    if annual_current.empty:
        raise StatCanSourceError("No complete current LDV annual series are available")
    annual_current["source_table_id"] = str(policy["current_table"])
    annual_current["series_policy"] = "current_quarterly_annualized"

    annual_archived = archived_rows[group_columns + ["reference_year", "scaled_value"]].copy()
    if annual_archived.duplicated(group_columns + ["reference_year"]).any():
        raise StatCanSourceError("Archived LDV table contains duplicate annual observations")
    annual_archived["source_period_count"] = 1
    annual_archived["source_table_id"] = str(policy["archived_table"])
    annual_archived["series_policy"] = "archived_precoverage_backfill"

    overlap = annual_archived.merge(
        annual_current,
        on=group_columns + ["reference_year"],
        how="inner",
        suffixes=("_archived", "_current"),
    )
    overlap["difference_current_minus_archived"] = (
        overlap["scaled_value_current"] - overlap["scaled_value_archived"]
    )
    overlap["absolute_difference"] = overlap[
        "difference_current_minus_archived"
    ].abs()
    tolerance = float(policy["overlap_absolute_tolerance"])
    differing = overlap["absolute_difference"] > tolerance
    if differing.any():
        warnings.append(
            f"{differing.sum()} archived/current LDV overlap observations differ; "
            "current annualized values take precedence"
        )

    current_start = int(annual_current["reference_year"].min())
    archived_backfill = annual_archived[annual_archived["reference_year"] < current_start]
    history = pd.concat([archived_backfill, annual_current], ignore_index=True)
    history["overlap_policy"] = str(policy["policy"])
    return history.sort_values(group_columns + ["reference_year"]).reset_index(drop=True), overlap


def build_freight_candidates(
    freight: pd.DataFrame,
    *,
    rules: dict[str, Any],
    mile_to_km: float,
    warnings: list[str],
) -> pd.DataFrame:
    """Derive shipment-average fields, filter class-8 candidates, and classify haul."""
    freight_rules = rules["freight"]
    provenance_columns = [
        column
        for column in ("table_id", "product_id", "cached_zip", "source_member")
        if column in freight.columns
    ]
    index_columns = provenance_columns + [
        "scenario_region",
        "region_match",
        "reference_period",
        "geography_origin_of_shipments",
        "geography_destination_of_shipments",
        "mode_of_transportation",
        "commodity_group",
    ]
    duplicate_keys = index_columns + ["characteristics"]
    if freight.duplicated(duplicate_keys).any():
        raise StatCanSourceError(
            "Freight source has duplicate characteristic observations for a shipment group"
        )
    pivoted = freight.pivot(
        index=index_columns,
        columns="characteristics",
        values="scaled_value",
    ).reset_index()
    pivoted.columns.name = None
    required = [
        str(freight_rules["shipments_characteristic"]),
        str(freight_rules["weight_characteristic"]),
        str(freight_rules["distance_characteristic"]),
        str(freight_rules["tonne_km_characteristic"]),
    ]
    missing = [column for column in required if column not in pivoted.columns]
    if missing:
        raise StatCanSourceError(f"Freight characteristics missing after pivot: {missing}")
    invalid = pivoted[required].isna().any(axis=1) | (pivoted[required[0]] <= 0)
    if invalid.any():
        warnings.append(
            f"{invalid.sum()} freight groups lacked complete positive shipment measures "
            "and were excluded"
        )
        pivoted = pivoted[~invalid].copy()

    shipments, weight, distance, tonne_km = required
    pivoted["average_shipment_weight_kg"] = pivoted[weight] / pivoted[shipments]
    pivoted["average_shipment_distance_km"] = pivoted[distance] / pivoted[shipments]
    pivoted["gross_vehicle_weight_kg"] = (
        pivoted["average_shipment_weight_kg"]
        + float(freight_rules["assumed_truck_curb_weight_kg"])
    )
    threshold = float(freight_rules["class8_gross_vehicle_weight_threshold_kg"])
    if freight_rules["gross_weight_boundary"] != "greater_than_or_equal":
        raise StatCanSourceError("Unsupported freight gross-weight boundary policy")
    pivoted = pivoted[pivoted["gross_vehicle_weight_kg"] >= threshold].copy()

    distance_boundary_km = (
        float(freight_rules["regional_long_haul_threshold_miles"]) * mile_to_km
    )
    boundary_assignment = str(freight_rules["exact_boundary_assignment"])
    if boundary_assignment == "regional":
        regional = pivoted["average_shipment_distance_km"] <= distance_boundary_km
    elif boundary_assignment == "long_haul":
        regional = pivoted["average_shipment_distance_km"] < distance_boundary_km
    else:
        raise StatCanSourceError(
            f"Unknown exact freight boundary assignment: {boundary_assignment}"
        )
    pivoted["haul_class"] = np.where(regional, "regional", "long_haul")
    pivoted["distance_boundary_miles"] = float(
        freight_rules["regional_long_haul_threshold_miles"]
    )
    pivoted["distance_boundary_km"] = distance_boundary_km
    pivoted["exact_boundary_assignment"] = boundary_assignment
    pivoted["assumed_truck_curb_weight_kg"] = float(
        freight_rules["assumed_truck_curb_weight_kg"]
    )
    pivoted["class8_gross_vehicle_weight_threshold_kg"] = threshold
    pivoted = pivoted.rename(
        columns={
            shipments: "shipments",
            weight: "aggregate_weight_kg",
            distance: "aggregate_distance_km",
            tonne_km: "tonne_kilometres",
        }
    )
    distribution_rules = freight_rules["haul_distribution"]
    expected_distribution_rules = {
        "category_column": "haul_class",
        "weight_column": "tonne_kilometres",
        "aggregation": "sum",
        "normalization": "share_of_total_tonne_kilometres",
    }
    invalid_distribution_rules = {
        key: distribution_rules.get(key)
        for key, expected in expected_distribution_rules.items()
        if distribution_rules.get(key) != expected
    }
    if invalid_distribution_rules:
        raise StatCanSourceError(
            "Freight haul distribution must sum tonne_kilometres by haul_class "
            "and normalize the tonne-kilometre totals; incompatible rules: "
            f"{invalid_distribution_rules}"
        )
    if (pivoted["tonne_kilometres"] < 0).any():
        raise StatCanSourceError("Freight tonne-kilometres cannot be negative")
    return pivoted.reset_index(drop=True)


def write_outputs(
    *,
    normalized_tables: dict[str, pd.DataFrame],
    history: pd.DataFrame,
    overlap: pd.DataFrame,
    freight_candidates: pd.DataFrame,
    manifest_rows: list[dict[str, Any]],
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Write normalized tables, derived fetch-stage outputs, manifest, and warnings."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_id, frame in normalized_tables.items():
        frame.to_csv(output_dir / rules["tables"][table_id]["output_file"], index=False)
    history.to_csv(output_dir / rules["ldv_history"]["output_file"], index=False)
    overlap.to_csv(output_dir / rules["ldv_history"]["overlap_file"], index=False)
    freight_candidates.to_csv(output_dir / rules["freight"]["output_file"], index=False)
    pd.DataFrame(manifest_rows).to_csv(output_dir / rules["manifest_file"], index=False)
    (output_dir / rules["warnings_file"]).write_text(
        "\n".join(warnings) + ("\n" if warnings else ""),
        encoding="utf-8",
    )


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    regions: list[str] | None = None,
    download: bool = True,
    session: requests.Session | None = None,
) -> Path:
    """Fetch/cache five StatCan tables and write scenario-filtered interim outputs."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    selected_regions = scenario_regions(bundle, regions)
    conversions = load_conversion_factors(bundle)
    scalar_multipliers = {
        str(key).lower(): float(value)
        for key, value in conversions["statcan"]["scalar_factor_multipliers"].items()
    }
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    normalized_tables: dict[str, pd.DataFrame] = {}
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    for request in build_requests(bundle):
        metadata_status, archive_status, download_url = ensure_cached_artifacts(
            request,
            download=download,
            session=session,
        )
        metadata = read_cached_metadata(request)
        validate_metadata_contract(metadata, request)
        selected, member, input_rows = read_selected_table(
            request,
            regions=selected_regions,
            geography_rules=rules["geography"],
            chunksize=int(rules["read_chunksize"]),
            provenance_columns=rules["provenance_columns"],
        )
        normalized = normalize_table(
            selected,
            request=request,
            source_member=member,
            scalar_multipliers=scalar_multipliers,
        )
        normalized_tables[request.table_id] = normalized
        manifest_rows.append(
            {
                "source_id": SOURCE_ID,
                "table_id": request.table_id,
                "product_id": request.product_id,
                "title": request.title,
                "regions": "|".join(selected_regions),
                "metadata_cache": str(request.metadata_cache_path),
                "archive_cache": str(request.archive_cache_path),
                "source_member": member,
                "metadata_status": metadata_status,
                "archive_status": archive_status,
                "download_url": download_url,
                "input_rows": input_rows,
                "selected_rows": len(normalized),
                "output_file": request.output_file,
                "cube_start_date": metadata.get("cubeStartDate", ""),
                "cube_end_date": metadata.get("cubeEndDate", ""),
                "cube_archive_status": metadata.get("archiveStatusEn", ""),
                "status": "ok",
            }
        )

    history_rules = rules["ldv_history"]
    history, overlap = build_ldv_history(
        normalized_tables[str(history_rules["archived_table"])],
        normalized_tables[str(history_rules["current_table"])],
        rules=rules,
        warnings=warnings,
    )
    freight_table_id = next(
        table_id
        for table_id, table_rules in rules["tables"].items()
        if table_rules["normalizer"] == "freight"
    )
    freight_candidates = build_freight_candidates(
        normalized_tables[freight_table_id],
        rules=rules,
        mile_to_km=float(conversions["length"]["mile_to_km"]),
        warnings=warnings,
    )
    derived_outputs = {
        str(history_rules["archived_table"]): "|".join(
            [history_rules["output_file"], history_rules["overlap_file"]]
        ),
        str(history_rules["current_table"]): "|".join(
            [history_rules["output_file"], history_rules["overlap_file"]]
        ),
        freight_table_id: str(rules["freight"]["output_file"]),
    }
    for manifest_row in manifest_rows:
        manifest_row["derived_outputs"] = derived_outputs.get(
            str(manifest_row["table_id"]), ""
        )
        manifest_row["warning_count"] = len(warnings)
    write_outputs(
        normalized_tables=normalized_tables,
        history=history,
        overlap=overlap,
        freight_candidates=freight_candidates,
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
        help="Scenario YAML controlling region selection and configured paths.",
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
        help="Require and reuse cached metadata/ZIP artifacts without network access.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    try:
        output_dir = fetch_and_normalize(
            args.scenario,
            regions=args.regions,
            download=not args.no_download,
        )
    except (FileNotFoundError, StatCanSourceError, requests.RequestException) as exc:
        raise SystemExit(f"Statistics Canada adapter failed: {exc}") from exc
    logging.info("Wrote Statistics Canada interim outputs to %s", output_dir)


if __name__ == "__main__":
    main()
