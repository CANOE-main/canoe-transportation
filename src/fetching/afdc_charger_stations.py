"""Replay exact AFDC station inventories; locations, ports and access stay distinct."""

import argparse
import hashlib
import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle, load_config_bundle, load_harmonization_rules,
    resolve_artifact_path, resolve_input_path, write_dataframe_atomic, write_text_atomic,
)


SOURCE = "afdc_alternative_fueling_stations"
COMPONENT = "public_electric_charging_ports"
LOGGER = logging.getLogger(__name__)
PORT_COLUMNS = {"L1": "level_1_ports", "L2": "level_2_ports", "DCFC": "dc_fast_ports"}


class AFDCQuery(BaseModel):
    """Supported source-native selection; full inventory is mandatory."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    country: Literal["CA"]
    fuel_type: Literal["ELEC"]
    access: Literal["public", "private", "all"]
    status: Literal["E"]
    limit: Literal["all"]
    maximum_vehicle_class: Literal["MD", "HD"] | None = None


class AFDCSnapshotRequest(BaseModel):
    """A reviewed dated source snapshot, validated before network or file access."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    component_key: str = Field(min_length=1)
    url: str
    api_key: str = Field(default="DEMO_KEY", repr=False, exclude=True)
    cache_path: Path
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0)
    snapshot_date: date
    query: AFDCQuery
    geography: dict[str, str]
    port_fields: dict[str, str]
    null_port_counts: Literal["zero"]

    @model_validator(mode="after")
    def validate_contract(self) -> "AFDCSnapshotRequest":
        parsed = urlparse(self.url)
        expected_history = f"/api/alt-fuel-stations/v0/historical-date/{self.snapshot_date.isoformat()}.json"
        if (
            parsed.scheme != "https" or parsed.hostname != "developer.nlr.gov"
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in {"/api/alt-fuel-stations/v1.json", expected_history}
        ):
            raise ValueError("AFDC URL must be its official current API or match the registered historical date")
        if not self.cache_path.is_absolute() or self.cache_path.suffix != ".json":
            raise ValueError("AFDC cache must be an absolute JSON path")
        if set(self.geography) != {"NL", "PE", "NS", "NB", "QC", "ON", "MB", "SK", "AB", "BC", "YT", "NT", "NU"}:
            raise ValueError("AFDC geography must cover all 13 Canadian provinces and territories")
        if len(set(self.geography.values())) != 13 or any(not value for value in self.geography.values()):
            raise ValueError("AFDC geography names must be unique and nonblank")
        if set(self.port_fields) != set(PORT_COLUMNS) or len(set(self.port_fields.values())) != 3:
            raise ValueError("AFDC requires distinct L1/L2/DCFC native port fields")
        return self


def build_request(bundle: ConfigBundle, component_key: str = COMPONENT) -> AFDCSnapshotRequest:
    source = bundle.sources.sources[SOURCE]
    if source.status != "active" or source.file_type != "json":
        raise ValueError("AFDC requires an active registered JSON source")
    component = source.component(component_key)
    adapter = {**source.adapter, **component.adapter}
    missing = [
        field for field in ("snapshot_date", "expected_sha256", "expected_bytes")
        if adapter.get(field) in (None, "")
    ]
    if missing:
        raise ValueError(
            f"AFDC component '{component_key}' lacks a complete reviewed snapshot pin "
            f"(missing: {', '.join(missing)}). Select and register snapshot_date, "
            "expected_sha256 and expected_bytes in config/sources.yaml before replay."
        )
    rules = load_harmonization_rules(bundle, "afdc")
    root = resolve_input_path(bundle, "cache").resolve()
    cache_path = (root / adapter["cache_path"]).resolve()
    if not cache_path.is_relative_to(root):
        raise ValueError("AFDC cache_path escapes the configured cache")
    access = adapter["access"]
    return AFDCSnapshotRequest(
        component_key=component_key, url=access["url"], api_key=access.get("api_key", "DEMO_KEY"),
        cache_path=cache_path, expected_sha256=adapter["expected_sha256"],
        expected_bytes=adapter["expected_bytes"], snapshot_date=adapter["snapshot_date"],
        query=adapter.get("query", rules["query"]), geography=rules["geography"],
        port_fields=rules["port_fields"], null_port_counts=rules["null_port_counts"],
    )


def validate_bytes(content: bytes, request: AFDCSnapshotRequest) -> None:
    if len(content) != request.expected_bytes or hashlib.sha256(content).hexdigest() != request.expected_sha256:
        raise ValueError("AFDC bytes differ from the registered snapshot; review and register a new source edition")


def ensure_cached(
    request: AFDCSnapshotRequest, *, download: bool = False, session: Any = None,
) -> Path:
    """Never refresh existing bytes or access the network during offline replay."""
    if request.cache_path.exists():
        validate_bytes(request.cache_path.read_bytes(), request)
        return request.cache_path
    if not download:
        raise FileNotFoundError(f"Missing registered AFDC snapshot: {request.cache_path}")
    client = session or requests
    response = client.get(request.url, params={
        **request.query.model_dump(exclude_none=True), "api_key": request.api_key,
    }, timeout=180)
    response.raise_for_status()
    final = urlparse(response.url)
    if final.scheme != "https" or final.hostname != "developer.nlr.gov":
        raise ValueError("AFDC acquisition redirected outside its registered official host")
    content = response.content
    validate_bytes(content, request)
    normalize_station_inventory(content, request)
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=request.cache_path.parent, suffix=".tmp", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        temporary.write_bytes(content)
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    LOGGER.info("Acquired AFDC %s (%d bytes)", request.component_key, len(content))
    return request.cache_path


def _count(value: Any, label: str, *, nullable: bool = False) -> int:
    if value is None and nullable:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"AFDC {label} must be a nonnegative integer")
    return value


def normalize_station_inventory(content: bytes, request: AFDCSnapshotRequest) -> pd.DataFrame:
    """Keep source-native classification and nulls alongside reconciled EVSE counts."""
    validate_bytes(content, request)
    payload = json.loads(content)
    if not isinstance(payload, dict) or not isinstance(payload.get("fuel_stations"), list):
        raise ValueError("AFDC response lacks its complete fuel_stations inventory")
    stations = payload["fuel_stations"]
    if not stations or len(stations) != _count(payload.get("total_results"), "total_results"):
        raise ValueError("AFDC station inventory is empty or truncated")
    if payload.get("warnings"):
        raise ValueError(f"AFDC source reports historical coverage warnings: {payload['warnings']}")
    rows = []
    ids = set()
    for station in stations:
        station_id = _count(station.get("id"), "station ID")
        if not station_id or station_id in ids:
            raise ValueError("AFDC station IDs must be positive and unique")
        ids.add(station_id)
        for field, expected in (("country", "CA"), ("fuel_type_code", "ELEC"), ("status_code", "E")):
            if station.get(field) != expected:
                raise ValueError(f"AFDC station {station_id} differs from requested {field}")
        access = station.get("access_code")
        if access not in {"public", "private"} or request.query.access != "all" and access != request.query.access:
            raise ValueError(f"AFDC station {station_id} differs from requested access")
        state = station.get("state")
        if state not in request.geography:
            raise ValueError(f"AFDC station {station_id} has an unknown Canadian province/territory")
        vehicle_class = station.get("maximum_vehicle_class")
        if vehicle_class not in {None, "LD", "MD", "HD"}:
            raise ValueError("AFDC maximum vehicle class is unknown")
        accepted_classes = {"MD", "HD"} if request.query.maximum_vehicle_class == "MD" else {"HD"}
        if request.query.maximum_vehicle_class and vehicle_class not in accepted_classes:
            raise ValueError("AFDC station differs from requested maximum vehicle class")
        if station.get("ev_other_evse") not in {None, ""}:
            raise ValueError("AFDC reports unallocated legacy/other EVSE; review its port accounting")
        counts, raw = {}, {}
        for level, field in request.port_fields.items():
            if field not in station:
                raise ValueError(f"AFDC station {station_id} lacks native field {field}")
            raw[field] = station[field]
            counts[PORT_COLUMNS[level]] = _count(station[field], field, nullable=True)
        total = sum(counts.values())
        if total <= 0:
            raise ValueError(f"AFDC electric station {station_id} has no known EVSE ports")
        rows.append({
            "station_id": station_id, "province_code": state, "source_region": request.geography[state],
            "source_year": request.snapshot_date.year, "as_of": request.snapshot_date.isoformat(),
            "country": "CA", "access_code": access, "status_code": "E", "fuel_type_code": "ELEC",
            **counts, "charging_ports": total, "units": "ports", **raw,
            "maximum_vehicle_class": vehicle_class, "restricted_access": station.get("restricted_access"),
            "access_detail_code": station.get("access_detail_code"), "access_days_time": station.get("access_days_time"),
            "facility_type": station.get("facility_type"), "station_name": station.get("station_name"),
            "open_date": station.get("open_date"), "date_last_confirmed": station.get("date_last_confirmed"),
            "updated_at": station.get("updated_at"), "ev_workplace_charging": station.get("ev_workplace_charging"),
            "ev_connector_types": json.dumps(station.get("ev_connector_types"), ensure_ascii=False),
            "ev_charging_units": json.dumps(station.get("ev_charging_units"), sort_keys=True, ensure_ascii=False),
        })
    frame = pd.DataFrame(rows).sort_values("station_id").reset_index(drop=True)
    electricity = payload.get("station_counts", {}).get("fuels", {}).get("ELEC", {})
    if _count(electricity.get("total"), "electric port total") != int(frame.charging_ports.sum()):
        raise ValueError("AFDC station-level ports do not reconcile with its response total")
    if _count(electricity.get("stations", {}).get("total"), "electric station total") != len(frame):
        raise ValueError("AFDC station count does not reconcile with its response total")
    LOGGER.info("Validated AFDC %s: %d stations, %d ports, %d unknown vehicle classes",
                request.component_key, len(frame), frame.charging_ports.sum(), frame.maximum_vehicle_class.isna().sum())
    return frame


def normalize_snapshot(
    content: bytes, request: AFDCSnapshotRequest,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aggregate the complete public inventory, including explicit zero provinces."""
    if request.query.access != "public" or request.query.maximum_vehicle_class is not None:
        raise ValueError("Production public port normalization requires all vehicle classes and public access")
    stations = normalize_station_inventory(content, request)
    records = []
    for code, name in request.geography.items():
        selected = stations.loc[stations.province_code.eq(code)]
        counts = {column: int(selected[column].sum()) for column in PORT_COLUMNS.values()}
        records.append({"source_region": name, "province_code": code,
                        "source_year": request.snapshot_date.year, "as_of": request.snapshot_date.isoformat(),
                        "public_chargers": sum(counts.values()), **counts,
                        "station_locations": len(selected), "units": "ports"})
    provincial = pd.DataFrame(records)
    total = int(provincial.public_chargers.sum())
    types = pd.DataFrame([{
        "charger_type": level, "public_chargers": int(provincial[column].sum()),
        "national_share": float(provincial[column].sum()) / total,
        "source_year": request.snapshot_date.year, "as_of": request.snapshot_date.isoformat(), "units": "ports",
    } for level, column in PORT_COLUMNS.items()])
    return provincial, types, stations


def _manifest(bundle: ConfigBundle, request: AFDCSnapshotRequest, frame: pd.DataFrame) -> dict[str, Any]:
    source = bundle.sources.sources[SOURCE]
    component = source.component(request.component_key)
    return {
        "source_key": SOURCE, "component_key": request.component_key,
        "source_id": source.source_id, "citation": component.citation or source.citation,
        "data_quality": bundle.sources.resolved_data_quality(SOURCE, request.component_key).row_fields(),
        "snapshot_date": request.snapshot_date.isoformat(), "url": request.url,
        "query": request.query.model_dump(exclude_none=True), "sha256": request.expected_sha256,
        "bytes": request.expected_bytes, "station_locations": len(frame),
        "charging_ports": int(frame.charging_ports.sum()),
        "unknown_vehicle_class_stations": int(frame.maximum_vehicle_class.isna().sum()),
        "restricted_public_stations": int((frame.access_code.eq("public") & frame.restricted_access.eq(True)).sum()),
        "null_level_counts": {field: int(frame[field].isna().sum()) for field in request.port_fields.values()},
        "port_accounting": "Native per-level nulls contribute zero only after all level fields, positive station totals and API totals reconcile; raw nulls are preserved.",
        "vehicle_scope": "Maximum vehicle class is physical access, not dedicated truck use; unknown classification is not LD-only.",
        "historical_scope": "Snapshot records on the selected date; opening dates do not reconstruct inventories or measure installations.",
    }


def fetch_and_normalize(scenario: str | Path | ConfigBundle, *, download: bool = False) -> dict[str, Any]:
    bundle = scenario if isinstance(scenario, ConfigBundle) else load_config_bundle(scenario)
    request = build_request(bundle)
    content = ensure_cached(request, download=download).read_bytes()
    provincial, types, stations = normalize_snapshot(content, request)
    rules = load_harmonization_rules(bundle, "afdc")
    output = resolve_artifact_path(bundle, "afdc_interim")
    output.mkdir(parents=True, exist_ok=True)
    for key, frame in (("province_output_file", provincial), ("type_output_file", types), ("stations_output_file", stations)):
        write_dataframe_atomic(frame, output / rules[key])
    manifest = _manifest(bundle, request, stations)
    write_text_atomic(json.dumps(manifest, indent=2, sort_keys=True) + "\n", output / rules["manifest_file"])
    return manifest


def fetch_diagnostic_evidence(scenario: str | Path | ConfigBundle, *, download: bool = False) -> list[dict[str, Any]]:
    """Opt-in evidence only: never enrolled as a production prerequisite."""
    bundle = scenario if isinstance(scenario, ConfigBundle) else load_config_bundle(scenario)
    rules = load_harmonization_rules(bundle, "afdc")
    output = resolve_artifact_path(bundle, "afdc_interim")
    output.mkdir(parents=True, exist_ok=True)
    manifests = []
    for key, filenames in rules.get("diagnostic_components", {}).items():
        request = build_request(bundle, key)
        stations = normalize_station_inventory(ensure_cached(request, download=download).read_bytes(), request)
        write_dataframe_atomic(stations, output / filenames["stations_output_file"])
        manifest = _manifest(bundle, request, stations)
        write_text_atomic(json.dumps(manifest, indent=2, sort_keys=True) + "\n", output / filenames["manifest_file"])
        manifests.append(manifest)
    return manifests


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/default.yaml")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--diagnostics", action="store_true", help="Replay only registered optional diagnostic station inventories")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    prepare = fetch_diagnostic_evidence if args.diagnostics else fetch_and_normalize
    prepare(args.scenario, download=args.download)


if __name__ == "__main__":
    main()
