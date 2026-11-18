"""Bounded AFDC inventory, pinning and offline contracts."""

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from fetching import afdc_charger_stations as afdc
from fetching.afdc_charger_stations import (
    AFDCSnapshotRequest, ensure_cached, normalize_snapshot, normalize_station_inventory,
)
from utils import load_config_bundle


GEOGRAPHY = {
    "NL": "Newfoundland and Labrador", "PE": "Prince Edward Island", "NS": "Nova Scotia",
    "NB": "New Brunswick", "QC": "Quebec", "ON": "Ontario", "MB": "Manitoba",
    "SK": "Saskatchewan", "AB": "Alberta", "BC": "British Columbia", "YT": "Yukon",
    "NT": "Northwest Territories", "NU": "Nunavut",
}
FIELDS = {"L1": "ev_level1_evse_num", "L2": "ev_level2_evse_num", "DCFC": "ev_dc_fast_num"}


def payload():
    return {"total_results": 2, "warnings": [], "station_counts": {
        "fuels": {"ELEC": {"total": 7, "stations": {"total": 2}}}}, "fuel_stations": [
        {"id": 1, "country": "CA", "state": "ON", "access_code": "public", "status_code": "E",
         "fuel_type_code": "ELEC", "ev_level1_evse_num": None, "ev_level2_evse_num": 2,
         "ev_dc_fast_num": 3, "maximum_vehicle_class": None, "restricted_access": True},
        {"id": 2, "country": "CA", "state": "QC", "access_code": "public", "status_code": "E",
         "fuel_type_code": "ELEC", "ev_level1_evse_num": 1, "ev_level2_evse_num": 1,
         "ev_dc_fast_num": None, "maximum_vehicle_class": "HD", "restricted_access": False},
    ]}


def request_for(tmp_path, native=None, **changes):
    content = json.dumps(native if native is not None else payload()).encode()
    attributes = {
        "component_key": "public_electric_charging_ports",
        "url": "https://developer.nlr.gov/api/alt-fuel-stations/v0/historical-date/2025-12-31.json",
        "cache_path": tmp_path / "snapshot.json", "expected_sha256": hashlib.sha256(content).hexdigest(),
        "expected_bytes": len(content), "snapshot_date": "2025-12-31",
        "query": {"country": "CA", "fuel_type": "ELEC", "access": "public", "status": "E", "limit": "all"},
        "geography": GEOGRAPHY, "port_fields": FIELDS, "null_port_counts": "zero", **changes,
    }
    return content, AFDCSnapshotRequest(**attributes)


def test_complete_snapshot_preserves_raw_nulls_and_observed_provincial_types(tmp_path):
    content, request = request_for(tmp_path)
    province, types, stations = normalize_snapshot(content, request)
    assert len(province) == 13
    assert province.public_chargers.sum() == 7
    assert province.loc[province.province_code.eq("NU"), "public_chargers"].item() == 0
    ontario = province.loc[province.province_code.eq("ON")].iloc[0]
    assert (ontario.level_1_ports, ontario.level_2_ports, ontario.dc_fast_ports) == (0, 2, 3)
    assert types.set_index("charger_type").public_chargers.to_dict() == {"L1": 1, "L2": 3, "DCFC": 3}
    assert types.national_share.sum() == pytest.approx(1)
    assert stations.loc[0, "level_1_ports"] == 0
    assert stations.ev_level1_evse_num.isna().sum() == 1
    assert stations.maximum_vehicle_class.isna().sum() == 1
    assert stations.loc[0, "restricted_access"]


@pytest.mark.parametrize("mutation,match", [
    (lambda p: p.update(total_results=3), "truncated"),
    (lambda p: p.update(warnings=["Historical vehicle fields unavailable"]), "coverage warnings"),
    (lambda p: p["fuel_stations"][1].update(id=1), "unique"),
    (lambda p: p["fuel_stations"][0].update(country="US"), "country"),
    (lambda p: p["fuel_stations"][0].update(access_code="private"), "access"),
    (lambda p: p["fuel_stations"][0].update(status_code="T"), "status"),
    (lambda p: p["fuel_stations"][0].update(fuel_type_code="CNG"), "fuel_type"),
    (lambda p: p["fuel_stations"][0].update(state="ZZ"), "unknown Canadian"),
    (lambda p: p["fuel_stations"][0].update(maximum_vehicle_class="unknown"), "vehicle class"),
    (lambda p: p["fuel_stations"][0].update(ev_other_evse="Legacy 1"), "unallocated"),
    (lambda p: p["fuel_stations"][0].pop("ev_level2_evse_num"), "lacks native"),
    (lambda p: p["fuel_stations"][0].update(ev_dc_fast_num=-1), "nonnegative integer"),
    (lambda p: p["fuel_stations"][0].update(ev_dc_fast_num=1.5), "nonnegative integer"),
    (lambda p: p["fuel_stations"][0].update(ev_dc_fast_num=True), "nonnegative integer"),
    (lambda p: p["fuel_stations"][0].update(ev_dc_fast_num=None, ev_level2_evse_num=None), "no known EVSE"),
    (lambda p: p["station_counts"]["fuels"]["ELEC"].update(total=8), "ports do not reconcile"),
    (lambda p: p["station_counts"]["fuels"]["ELEC"]["stations"].update(total=3), "count does not reconcile"),
])
def test_native_scope_completeness_and_counts_fail_loudly(tmp_path, mutation, match):
    native = payload()
    mutation(native)
    content, request = request_for(tmp_path, native)
    with pytest.raises(ValueError, match=match):
        normalize_snapshot(content, request)


def test_cache_is_offline_and_never_repairs_mismatched_bytes(tmp_path):
    content, request = request_for(tmp_path)
    session = Mock()
    with pytest.raises(FileNotFoundError, match="Missing registered"):
        ensure_cached(request, session=session)
    session.get.assert_not_called()
    request.cache_path.write_bytes(content)
    assert ensure_cached(request, session=session) == request.cache_path
    session.get.assert_not_called()
    request.cache_path.write_bytes(content + b" ")
    with pytest.raises(ValueError, match="registered snapshot"):
        ensure_cached(request, download=True, session=session)
    session.get.assert_not_called()


def test_download_validates_before_publication(tmp_path):
    content, request = request_for(tmp_path)
    response = Mock(content=content, url=request.url)
    session = Mock()
    session.get.return_value = response
    assert ensure_cached(request, download=True, session=session).read_bytes() == content
    assert session.get.call_args.kwargs["params"]["limit"] == "all"
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("bad_response", ["hash", "redirect"])
def test_invalid_download_never_publishes(tmp_path, bad_response):
    content, request = request_for(tmp_path)
    response = Mock(content=content if bad_response != "hash" else b"changed",
                    url=request.url if bad_response != "redirect" else "https://example.com/data")
    session = Mock()
    session.get.return_value = response
    with pytest.raises(ValueError):
        ensure_cached(request, download=True, session=session)
    assert not request.cache_path.exists()


@pytest.mark.parametrize("changes", [
    {"snapshot_date": "2024-12-31"}, {"url": "http://developer.nlr.gov/api/alt-fuel-stations/v1.json"},
    {"url": "https://example.com/api/alt-fuel-stations/v1.json"},
    {"cache_path": Path("relative.json")}, {"geography": {"ON": "Ontario"}},
    {"query": {"country": "CA", "fuel_type": "ELEC", "access": "public", "status": "all", "limit": "all"}},
])
def test_request_rejects_unpinned_scope_before_io(tmp_path, changes):
    with pytest.raises(ValidationError):
        request_for(tmp_path, **changes)


@pytest.mark.parametrize("missing", [
    ("snapshot_date",), ("expected_sha256",), ("expected_bytes",),
    ("snapshot_date", "expected_sha256", "expected_bytes"),
])
def test_unselected_snapshot_reports_missing_reviewed_pin_before_paths(tmp_path, monkeypatch, missing):
    _, request = request_for(tmp_path)
    bundle = load_config_bundle("config/scenarios/default.yaml")
    source = bundle.sources.sources[afdc.SOURCE]
    component = source.component(afdc.COMPONENT)
    for field in ("snapshot_date", "expected_sha256", "expected_bytes"):
        source.adapter[field] = getattr(request, field)
        component.adapter.pop(field, None)
    for field in missing:
        source.adapter.pop(field)
    paths = Mock(side_effect=AssertionError("Incomplete source pin reached path resolution"))
    monkeypatch.setattr(afdc, "resolve_input_path", paths)
    with pytest.raises(ValueError, match="lacks a complete reviewed snapshot pin") as error:
        afdc.build_request(bundle)
    assert afdc.COMPONENT in str(error.value)
    assert all(field in str(error.value) for field in missing)
    paths.assert_not_called()


def test_optional_mhdv_inventory_preserves_access_and_does_not_enter_public_parameterization(tmp_path):
    native = payload()
    native["fuel_stations"][0].update(maximum_vehicle_class="MD", access_code="private")
    content, request = request_for(tmp_path, native, query={
        "country": "CA", "fuel_type": "ELEC", "access": "all", "status": "E", "limit": "all",
        "maximum_vehicle_class": "MD",
    })
    result = normalize_station_inventory(content, request)
    assert set(result.access_code) == {"public", "private"}
    assert set(result.maximum_vehicle_class) == {"MD", "HD"}
    with pytest.raises(ValueError, match="all vehicle classes and public access"):
        normalize_snapshot(content, request)


def test_offline_publication_is_deterministic_and_retains_source_identity(tmp_path, monkeypatch):
    content, request = request_for(tmp_path)
    request.cache_path.write_bytes(content)
    bundle = load_config_bundle("config/scenarios/default.yaml")
    output = tmp_path / "normalized"
    monkeypatch.setattr(afdc, "build_request", lambda config: request)
    monkeypatch.setattr(afdc, "resolve_artifact_path", lambda config, family: output)
    network = Mock(side_effect=AssertionError("offline replay attempted network access"))
    monkeypatch.setattr(afdc.requests, "get", network)
    manifest = afdc.fetch_and_normalize(bundle)
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    repeated = afdc.fetch_and_normalize(bundle)
    assert before == {p.name: p.read_bytes() for p in output.iterdir()}
    assert manifest == repeated
    assert manifest["source_id"] == "T38"
    assert manifest["sha256"] == request.expected_sha256
    assert set(manifest["data_quality"]) == {"dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"}
    assert manifest["unknown_vehicle_class_stations"] == 1
    assert manifest["restricted_public_stations"] == 1
    network.assert_not_called()
