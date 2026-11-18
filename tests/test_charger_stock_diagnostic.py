"""Point-year registrations preserve known counts, absent evidence and review bytes."""

import json
import hashlib
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from utils import file_sha256
from validation import charger_stock_diagnostic as stock


@pytest.fixture
def settings():
    return stock.StockSettings.model_validate({
        "table_id": "23-10-0308-01", "fuel_type": "Battery electric", "statistic": "Number of vehicles",
        "region_members": {"BCT": ["BC", "YT", "NT", "NU"]},
        "groups": {"cars": {"vehicle_class": "LDV", "vehicle_types": ["car"]},
                   "light_trucks": {"vehicle_class": "LDV", "vehicle_types": ["pickup", "van"]},
                   "medium_trucks": {"vehicle_class": "MHDV", "vehicle_types": ["medium"]},
                   "heavy_trucks": {"vehicle_class": "MHDV", "vehicle_types": ["class7", "class8"]}},
    })


def source_rows(values, *, region="ON", year=2025):
    return pd.DataFrame([{"scenario_region": region, "reference_period": str(year),
                          "vehicle_type": name, "scaled_value": value, "units": "Number",
                          "status": ".." if pd.isna(value) else "", "symbol": ""}
                         for name, value in values.items()])


def test_stock_counts_are_direct_observations_and_explicit_known_lower_bounds(settings):
    frame = source_rows({"car": 10, "pickup": 4, "van": 0, "medium": 1707,
                         "class7": np.nan, "class8": np.nan})
    totals, components = stock.summarize_stock_components(frame, settings=settings,
        region_members={"ON": ["ON"]}, source_year=2025)
    result = totals.set_index("road_class")
    assert result.loc["cars", "bev_vehicles"] == 10
    assert result.loc["light_trucks", "bev_vehicles"] == 4
    assert result.loc["medium_trucks", "bev_vehicles"] == 1707
    assert pd.isna(result.loc["heavy_trucks", "bev_vehicles"])
    assert pd.isna(result.loc["heavy_trucks", "known_bev_vehicles"])
    assert result.loc["heavy_trucks", "missing_components"] == 2
    assert result.loc["heavy_trucks", "observation_status"] == "unavailable"
    assert components.loc[components.vehicle_type.eq("van"), "observation_status"].item() == "observed"
    assert components.loc[components.vehicle_type.eq("class7"), "source_status"].item() == ".."
    assert set(totals.units) == {"vehicles"}


def test_partial_heavy_count_retains_known_positive_value_and_suppression(settings):
    frame = source_rows({"car": 1, "pickup": 1, "van": 1, "medium": 1, "class7": 8, "class8": np.nan})
    frame.loc[frame.vehicle_type.eq("class8"), "status"] = "x"
    totals, components = stock.summarize_stock_components(frame, settings=settings,
        region_members={"ON": ["ON"]}, source_year=2025)
    result = totals.set_index("road_class").loc["heavy_trucks"]
    assert pd.isna(result.bev_vehicles)
    assert result.known_bev_vehicles == 8
    assert result.missing_components == 1
    assert result.observation_status == "partly_observed"
    assert components.loc[components.vehicle_type.eq("class8"), "observation_status"].item() == "suppressed"


def test_missing_year_is_not_held_or_relabelled(settings):
    frame = source_rows({"car": 100, "pickup": 10, "van": 20, "medium": 30, "class7": 2, "class8": 1}, year=2024)
    totals, components = stock.summarize_stock_components(frame, settings=settings,
        region_members={"ON": ["ON"]}, source_year=2025)
    assert totals.bev_vehicles.isna().all()
    assert totals.known_bev_vehicles.isna().all()
    assert totals.source_year.eq(2025).all()
    assert components.observation_status.eq("absent").all()


def test_bct_aggregates_four_source_geographies_without_duplicating_them(settings):
    frame = pd.concat([source_rows({"car": index, "pickup": 0, "van": 0, "medium": 0,
                                   "class7": 0, "class8": 0}, region=region)
                       for index, region in enumerate(["BC", "YT", "NT", "NU"], start=1)])
    totals, components = stock.summarize_stock_components(frame, settings=settings,
        region_members={"BCT": ["BC", "YT", "NT", "NU"]}, source_year=2025)
    assert totals.set_index("road_class").loc["cars", "bev_vehicles"] == 10
    assert len(components) == 24
    assert set(components.region) == {"BCT"}
    assert set(components.source_region) == {"BC", "YT", "NT", "NU"}


@pytest.mark.parametrize("invalid", [-1, 1.5, np.inf])
def test_invalid_published_counts_fail(settings, invalid):
    with pytest.raises(ValueError, match="nonnegative integers"):
        stock.summarize_stock_components(source_rows({"car": invalid}), settings=settings,
            region_members={"ON": ["ON"]}, source_year=2025)


def test_duplicate_source_rows_fail(settings):
    frame = source_rows({"car": 10})
    with pytest.raises(ValueError, match="Duplicate"):
        stock.summarize_stock_components(pd.concat([frame, frame]), settings=settings,
            region_members={"ON": ["ON"]}, source_year=2025)


def test_overlapping_config_groups_fail(settings):
    data = settings.model_dump()
    data["groups"]["light_trucks"]["vehicle_types"].append("car")
    with pytest.raises(ValidationError, match="distinct"):
        stock.StockSettings.model_validate(data)


def review_fixture(tmp_path, monkeypatch):
    root = tmp_path / "reviews"
    folder = root / "candidate"
    downloads = folder / "downloads"
    downloads.mkdir(parents=True)
    archive, metadata = downloads / "a.bin", downloads / "m.bin"
    archive.write_bytes(b"archive bytes")
    metadata.write_text("metadata bytes", encoding="utf-8")
    evidence = {"url": "https://www150.statcan.gc.ca/data.zip",
                "metadata_url": "https://www150.statcan.gc.ca/metadata",
                "archive_sha256": file_sha256(archive), "metadata_sha256": file_sha256(metadata),
                "release_time": "2026-09-22T08:30"}
    report = {"review_schema_version": 2, "status": "report_only", "mode": "report", "errors": [],
              "candidates": [{"source": "statcan_transport_tables", "component": "23-10-0308-01", "evidence": evidence}],
              "acquisitions": [{"file": str(path.relative_to(folder)), "sha256": file_sha256(path), "bytes": path.stat().st_size,
                                "request": {"url": "https://www150.statcan.gc.ca/unused", "payload": None}}
                               for path in [archive, metadata]]}
    report["review_fingerprint"] = stock._review_fingerprint(report)
    path = folder / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(stock, "resolve_artifact_path", lambda *_: root)
    bundle = SimpleNamespace(repo_root=tmp_path)
    base = SimpleNamespace(table_id="23-10-0308-01", metadata_url=evidence["metadata_url"],
                           download_api_url="https://www150.statcan.gc.ca/resolver",
                           model_copy=lambda update: SimpleNamespace(**update))
    return bundle, base, path, report, archive


def test_review_downloads_are_authenticated_without_adopting_cache(tmp_path, monkeypatch):
    bundle, base, path, _, archive = review_fixture(tmp_path, monkeypatch)
    before = set(tmp_path.rglob("*"))
    request, metadata = stock._reviewed_request(bundle, base, path)
    assert request.archive_cache_path == archive
    assert request.snapshot is None
    assert metadata["source_is_candidate"] is True
    assert metadata["review_report_sha256"] == file_sha256(path)
    assert set(tmp_path.rglob("*")) == before


def test_changed_review_download_is_rejected(tmp_path, monkeypatch):
    bundle, base, path, _, archive = review_fixture(tmp_path, monkeypatch)
    archive.write_bytes(b"changed")
    with pytest.raises(ValueError, match="missing or changed"):
        stock._reviewed_request(bundle, base, path)


def test_review_report_fingerprint_is_required(tmp_path, monkeypatch):
    bundle, base, path, report, _ = review_fixture(tmp_path, monkeypatch)
    report["candidates"][0]["evidence"]["release_time"] = "changed"
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="unchanged report-only"):
        stock._reviewed_request(bundle, base, path)


def test_review_download_path_cannot_escape_its_owner(tmp_path, monkeypatch):
    bundle, base, path, report, _ = review_fixture(tmp_path, monkeypatch)
    report["acquisitions"][0]["file"] = "../../outside.bin"
    report["review_fingerprint"] = stock._review_fingerprint(report)
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="escapes"):
        stock._reviewed_request(bundle, base, path)


def test_updater_canonical_metadata_matches_retained_http_bytes(tmp_path, monkeypatch):
    bundle, base, path, report, _ = review_fixture(tmp_path, monkeypatch)
    base.product_id = 23100308
    metadata = path.parent / report["acquisitions"][1]["file"]
    payload = [{"status": "SUCCESS", "object": {"productId": base.product_id}}]
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    report["acquisitions"][1].update(sha256=file_sha256(metadata), bytes=metadata.stat().st_size,
        request={"url": base.metadata_url, "payload": [{"productId": base.product_id}]})
    canonical = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    expected = hashlib.sha256(canonical).hexdigest()
    report["candidates"][0]["evidence"]["metadata_sha256"] = expected
    report["review_fingerprint"] = stock._review_fingerprint(report)
    path.write_text(json.dumps(report), encoding="utf-8")

    request, identity = stock._reviewed_request(bundle, base, path)

    assert request.metadata_cache_path == metadata
    assert identity["metadata_sha256"] == file_sha256(metadata)
    assert identity["canonical_metadata_sha256"] == expected
    assert identity["metadata_sha256"] != expected
