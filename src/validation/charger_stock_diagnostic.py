"""Read point-year battery-vehicle registrations without adopting source updates."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fetching import statcan_tables
from fetching.source_updater import _review_fingerprint
from utils import (file_sha256, load_conversion_factors, load_harmonization_rules,
                   load_yaml, resolve_artifact_path, resolve_parameter_path)


RoadClass = Literal["cars", "light_trucks", "medium_trucks", "heavy_trucks"]


class StockGroup(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    vehicle_class: Literal["LDV", "MHDV"]
    vehicle_types: list[str] = Field(min_length=1)


class StockSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    table_id: str = Field(min_length=1)
    fuel_type: str = Field(min_length=1)
    statistic: str = Field(min_length=1)
    region_members: dict[str, list[str]]
    groups: dict[RoadClass, StockGroup]

    @model_validator(mode="after")
    def distinct_groups(self):
        if set(self.groups) != {"cars", "light_trucks", "medium_trucks", "heavy_trucks"}:
            raise ValueError("Point-year stock requires all four configured vehicle groups")
        members = [member for group in self.groups.values() for member in group.vehicle_types]
        if len(members) != len(set(members)) or any(not member.strip() for member in members):
            raise ValueError("Stock source vehicle types must be nonempty and distinct between groups")
        for name, group in self.groups.items():
            if group.vehicle_class != ("LDV" if name in {"cars", "light_trucks"} else "MHDV"):
                raise ValueError(f"Stock group has an incompatible vehicle class: {name}")
        if any(not members or len(members) != len(set(members)) for members in self.region_members.values()):
            raise ValueError("Regional stock members must be nonempty and unique")
        return self


@dataclass(frozen=True)
class ChargerStockEvidence:
    fleet_counts: pd.DataFrame
    components: pd.DataFrame
    metadata: dict


def _review_file(report_path: Path, report: dict, digest: str) -> Path:
    entries = [item for item in report["acquisitions"] if item["sha256"] == digest]
    if not entries:
        raise ValueError("Source review lacks the candidate's physical download")
    files = {item["file"] for item in entries}
    if len(files) != 1:
        raise ValueError("Source review has conflicting candidate download paths")
    relative = Path(entries[0]["file"])
    path = (report_path.parent / relative).resolve()
    if relative.is_absolute() or not path.is_relative_to((report_path.parent / "downloads").resolve()):
        raise ValueError("Source review download escapes its downloads directory")
    if (not path.is_file() or any(path.stat().st_size != item["bytes"] for item in entries)
            or file_sha256(path) != digest):
        raise ValueError("Source review download is missing or changed")
    return path


def _reviewed_request(bundle, base, review_report: str | Path):
    path = Path(review_report)
    path = (path if path.is_absolute() else bundle.repo_root / path).resolve()
    if not path.is_relative_to(resolve_artifact_path(bundle, "source_update_audits").resolve()):
        raise ValueError("Stock source review must reside in the configured source-update audit route")
    if not path.is_file():
        raise FileNotFoundError(f"Stock source review is missing: {path}. Prepare it with: "
                                f"uv run python -m fetching.source_updater --scenario {bundle.scenario_path} --source {statcan_tables.SOURCE_ID}")
    report = json.loads(path.read_text(encoding="utf-8"))
    if (report.get("review_schema_version") != 2 or report.get("status") != "report_only"
            or report.get("mode") != "report" or report.get("errors")
            or report.get("review_fingerprint") != _review_fingerprint(report)):
        raise ValueError("Stock source review is not a complete, unchanged report-only proposal")
    candidates = [row for row in report["candidates"]
                  if row["source"] == statcan_tables.SOURCE_ID and row["component"] == base.table_id]
    if len(candidates) != 1:
        raise ValueError("Stock source review must identify exactly one requested StatCan table")
    evidence = candidates[0]["evidence"]
    if (urlparse(evidence["url"]).hostname != urlparse(base.download_api_url).hostname
            or evidence["metadata_url"] != base.metadata_url):
        raise ValueError("Stock source candidate does not use the registered official endpoints")
    archive = _review_file(path, report, evidence["archive_sha256"])
    if any(item["sha256"] == evidence["metadata_sha256"] for item in report["acquisitions"]):
        metadata = _review_file(path, report, evidence["metadata_sha256"])
    else:
        # The updater fingerprints canonical indented metadata, while retaining
        # the HTTP response bytes separately. Authenticate both representations.
        entries = [item for item in report["acquisitions"]
                   if item["request"]["url"] == base.metadata_url
                   and item["request"].get("payload") == [{"productId": base.product_id}]]
        if not entries:
            raise ValueError("Source review lacks its metadata response")
        metadata = _review_file(path, report, entries[0]["sha256"])
        canonical = (json.dumps(json.loads(metadata.read_text(encoding="utf-8")),
                                ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        if hashlib.sha256(canonical).hexdigest() != evidence["metadata_sha256"]:
            raise ValueError("Source review metadata differs from the candidate")
    # The updater stores authenticated response bodies as .bin blobs. This is a
    # read-only parser view, not a cache registration. Hash, metadata and ZIP/CSV
    # validation protect the bytes before the existing source owner parses them.
    request = base.model_copy(update={"archive_cache_path": archive,
                                      "metadata_cache_path": metadata, "snapshot": None})
    return request, {"source_is_candidate": True, "review_report": str(path),
                     "review_report_sha256": file_sha256(path), "release_time": evidence["release_time"],
                     "source_url": evidence["url"], "archive_sha256": evidence["archive_sha256"],
                     "metadata_sha256": file_sha256(metadata),
                     "canonical_metadata_sha256": evidence["metadata_sha256"]}


def summarize_stock_components(normalized: pd.DataFrame, *, settings: StockSettings,
                               region_members: dict[str, list[str]], source_year: int):
    """Keep incomplete counts unknown while exposing the observed lower bound."""
    required = {"scenario_region", "reference_period", "vehicle_type", "scaled_value", "units"}
    if required - set(normalized):
        raise ValueError("Point-year stock lacks required source fields")
    selected = normalized.loc[normalized.reference_period.astype(str).eq(str(source_year))].copy()
    keys = ["scenario_region", "vehicle_type"]
    if selected.duplicated(keys).any():
        raise ValueError("Duplicate region/type counts in point-year stock")
    if not selected.units.eq("Number").all():
        raise ValueError("Point-year stock must be reported in vehicle counts")
    known = pd.to_numeric(selected.scaled_value, errors="coerce").dropna()
    if not np.isfinite(known).all() or known.lt(0).any() or known.mod(1).ne(0).any():
        raise ValueError("Published battery-vehicle counts must be nonnegative integers")
    selected = selected.set_index(keys)
    components, totals = [], []
    for region, sources in region_members.items():
        for name, group in settings.groups.items():
            pieces = []
            for source in sources:
                for vehicle_type in group.vehicle_types:
                    key = (source, vehicle_type)
                    row = selected.loc[key] if key in selected.index else None
                    value = float(row.scaled_value) if row is not None else np.nan
                    status = str(row.get("status", "")).strip() if row is not None else ""
                    symbol = str(row.get("symbol", "")).strip() if row is not None else ""
                    observation = ("observed" if pd.notna(value) else "absent" if row is None
                                   else "suppressed" if status == "x" else "unavailable")
                    pieces.append({"region": region, "source_region": source, "source_year": source_year,
                                   "road_class": name, "vehicle_class": group.vehicle_class,
                                   "vehicle_type": vehicle_type, "bev_vehicles": value,
                                   "observation_status": observation, "source_status": status,
                                   "source_symbol": symbol, "units": "vehicles"})
            components.extend(pieces)
            values = pd.Series([piece["bev_vehicles"] for piece in pieces], dtype=float)
            missing = int(values.isna().sum())
            known_count = float(values.sum(min_count=1))
            totals.append({"region": region, "source_year": source_year,
                           "vehicle_class": group.vehicle_class, "road_class": name,
                           "bev_vehicles": known_count if missing == 0 else np.nan,
                           "known_bev_vehicles": known_count, "missing_components": missing,
                           "expected_components": len(pieces),
                           "observation_status": "observed" if missing == 0 else "partly_observed" if values.notna().any() else "unavailable",
                           "units": "vehicles"})
    return pd.DataFrame(totals), pd.DataFrame(components)


def load_stock_evidence(bundle, *, review_report: str | Path | None = None) -> ChargerStockEvidence:
    """Read exact baseline-year registrations; never fetch, adopt or hold older years."""
    path = resolve_parameter_path(bundle, "charger_infrastructure_diagnosis.yaml")
    settings = StockSettings.model_validate(load_yaml(path)["stock_evidence"])
    rules = statcan_tables.module_rules(bundle)
    requests = [request for request in statcan_tables.build_requests(bundle) if request.table_id == settings.table_id]
    if len(requests) != 1:
        raise ValueError("Configure one registered source table for diagnostic stock")
    request = requests[0]
    identity = {"source_is_candidate": False, "review_report": None}
    if review_report is not None:
        request, identity = _reviewed_request(bundle, request, review_report)
    selectors = {"Vehicle Type": [value for group in settings.groups.values() for value in group.vehicle_types],
                 "Fuel Type": [settings.fuel_type], "Statistics": [settings.statistic]}
    request = request.model_copy(update={"table_rules": {**request.table_rules, "selectors": selectors}})
    statcan_tables.validate_source(request)
    metadata = statcan_tables.read_cached_metadata(request)
    statcan_tables.validate_metadata_contract(metadata, request)
    output_map = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"]
    region_members = {output_map.get(region, region): settings.region_members.get(region, [region])
                      for region in bundle.scenario.geography.regions}
    source_regions = list(dict.fromkeys(source for members in region_members.values() for source in members))
    selected, member, input_rows = statcan_tables.read_selected_table(
        request, regions=source_regions, geography_rules=rules["geography"],
        chunksize=int(rules["read_chunksize"]), provenance_columns=rules["provenance_columns"])
    multipliers = {str(key).lower(): float(value) for key, value in
                   load_conversion_factors(bundle)["statcan"]["scalar_factor_multipliers"].items()}
    normalized = statcan_tables.normalize_table(selected, request=request, source_member=member,
                                               scalar_multipliers=multipliers)
    counts, components = summarize_stock_components(normalized, settings=settings,
        region_members=region_members, source_year=bundle.scenario.periods.base_year)
    identity = {"production_adoption": False, "stock_basis": "point_year_registered_battery_vehicles",
                "table_id": request.table_id, "source_year": bundle.scenario.periods.base_year,
                "available_years": sorted(normalized.reference_period.astype(int).unique().tolist()),
                "stock_regions": list(region_members), "source_member": member, "input_rows": input_rows,
                "archive_path": str(request.archive_cache_path), "archive_sha256": file_sha256(request.archive_cache_path),
                "metadata_path": str(request.metadata_cache_path), "metadata_sha256": file_sha256(request.metadata_cache_path),
                "release_time": metadata.get("releaseTime"), "source_url": f"https://doi.org/10.25318/{request.product_id}01-eng",
                "table_url": f"https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid={request.product_id}01",
                "preparation_command": f"uv run python -m fetching.source_updater --scenario {bundle.scenario_path} --source {statcan_tables.SOURCE_ID}",
                "scope": "BEVs only. LDVs include cars, pickups, multi-purpose vehicles and vans; other LDVs, buses and motorcycles are excluded. Class 3–6 registrations include larger SUVs/pickups and are not a freight-only count.",
                "settings_sha256": file_sha256(path), **identity}
    return ChargerStockEvidence(counts, components, identity)
