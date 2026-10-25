"""Offline checks for the reviewed NRCan Ratings refresh command (#10)."""

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import fetching.nrcan_ceud as nrcan_ceud
import fetching.nrcan_ratings_refresh as refresh
from fetching.nrcan_ceud import fetch_rating_to_cache, iter_rating_requests
from utils import load_config_bundle

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"
BEV = "battery_electric_2012_2026"


@pytest.fixture(scope="module")
def bev_request():
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    return next(r for r in iter_rating_requests(bundle) if r.component_key == BEV)


def md5(content: bytes) -> str:
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def portal(request, *, content: bytes, modified: str = "2026-09-29T21:33:51.575288") -> dict:
    return {"id": request.resource_id, "hash": md5(content), "size": len(content), "last_modified": modified}


def ratings_csv(request, years) -> bytes:
    text_columns = {"Make", "Model", "Vehicle class", "Transmission", "Fuel type"}
    header = ",".join(f'"{column}"' for column in request.required_columns)
    rows = [
        ",".join(str(year) if column == "Model year" else ("X" if column in text_columns else "1")
                 for column in request.required_columns)
        for year in years
    ]
    return ("\n".join([header, *rows]) + "\n").encode("utf-8")


class FakeResponse:
    def __init__(self, content: bytes = b"", payload: dict | None = None) -> None:
        self.content, self._payload = content, payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_status_reports_changed_and_current_releases(bev_request) -> None:
    changed = refresh.refresh_status(bev_request, portal(bev_request, content=b"new release"))
    assert changed.changed
    assert (changed.pinned_version, changed.portal_version) == ("2026-07-17", "2026-09-29")
    current = refresh.refresh_status(bev_request, {
        "id": bev_request.resource_id, "hash": bev_request.expected_md5,
        "size": bev_request.expected_bytes, "last_modified": "2026-07-17T10:00:00",
    })
    assert not current.changed


def test_status_requires_the_portal_md5(bev_request) -> None:
    with pytest.raises(ValueError, match="publishes no MD5"):
        refresh.refresh_status(bev_request, {**portal(bev_request, content=b"x"), "hash": ""})
    with pytest.raises(ValueError, match="different resource"):
        refresh.refresh_status(bev_request, {**portal(bev_request, content=b"x"), "id": "other"})


def test_download_must_match_the_portal_metadata(bev_request, monkeypatch) -> None:
    status = refresh.refresh_status(bev_request, portal(bev_request, content=b"published"))
    monkeypatch.setattr(refresh.requests, "get", lambda url, timeout: FakeResponse(b"tampered"))
    with pytest.raises(ValueError, match="does not match the portal metadata"):
        refresh.download_verified(bev_request, status)
    monkeypatch.setattr(refresh.requests, "get", lambda url, timeout: FakeResponse(b"published"))
    assert refresh.download_verified(bev_request, status) == b"published"


def test_contracts_reject_a_changed_model_year_coverage(bev_request) -> None:
    years = bev_request.expected_model_years
    assert refresh.check_contracts(bev_request, ratings_csv(bev_request, years), encoding_candidates=["utf-8"]) == len(years)
    with pytest.raises(ValueError, match="covers model years"):
        refresh.check_contracts(bev_request, ratings_csv(bev_request, (*years, years[-1] + 1)), encoding_candidates=["utf-8"])


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_rewrite_changes_only_the_selected_component(bev_request, newline) -> None:
    original = (REPO_ROOT / "config" / "sources.yaml").read_bytes().decode("utf-8").replace("\r\n", "\n")
    original = original.replace("\n", newline)
    status = refresh.refresh_status(bev_request, portal(bev_request, content=b"new release"))
    updated = refresh.rewrite_component_pins(original, status)
    changed = [(a, b) for a, b in zip(original.split(newline), updated.split(newline)) if a != b]
    assert len(changed) == 5 and len(original.split(newline)) == len(updated.split(newline))
    assert ("\r\n" in updated) == (newline == "\r\n")
    components = yaml.safe_load(updated)["sources"]["nrcan_fuel_consumption_ratings"]["components"]
    entry = components[BEV]
    assert entry["version"] == "2026-09-29" and entry["label"].endswith("(2026-09-29)")
    assert entry["adapter"]["cache_path"].startswith("nrcan_fuel_consumption_ratings/2026-09-29_")
    assert entry["adapter"]["expected_md5"] == md5(b"new release")
    assert entry["adapter"]["expected_bytes"] == len(b"new release")
    before = yaml.safe_load(original)["sources"]["nrcan_fuel_consumption_ratings"]["components"]
    assert {k: v for k, v in components.items() if k != BEV} == {k: v for k, v in before.items() if k != BEV}


def test_apply_adopts_only_changed_releases(bev_request, tmp_path, monkeypatch) -> None:
    content = ratings_csv(bev_request, bev_request.expected_model_years)
    (tmp_path / "config").mkdir()
    registry = tmp_path / "config" / "sources.yaml"
    registry.write_bytes((REPO_ROOT / "config" / "sources.yaml").read_bytes())
    request = bev_request.model_copy(update={"cache_path": tmp_path / "cache" / bev_request.cache_path.name})
    monkeypatch.setattr(refresh, "iter_rating_requests", lambda bundle: [request])
    monkeypatch.setattr(refresh, "ratings_rules", lambda bundle: {"encoding_candidates": ["utf-8"]})
    monkeypatch.setattr(refresh, "portal_resource", lambda resource_id: portal(request, content=content))
    monkeypatch.setattr(refresh.requests, "get", lambda url, timeout: FakeResponse(content))
    bundle = SimpleNamespace(repo_root=tmp_path)

    [report] = refresh.refresh(bundle)
    assert report.changed and registry.read_bytes() == (REPO_ROOT / "config" / "sources.yaml").read_bytes()
    assert not (tmp_path / "cache").exists()

    refresh.refresh(bundle, apply=True)
    assert (tmp_path / "cache" / "2026-09-29_battery_electric_2012_2026.csv").read_bytes() == content
    entry = yaml.safe_load(registry.read_text(encoding="utf-8"))["sources"]["nrcan_fuel_consumption_ratings"]["components"][BEV]
    assert entry["adapter"]["expected_md5"] == md5(content)


def test_pinned_mismatch_error_points_to_the_refresh_command(bev_request, tmp_path, monkeypatch) -> None:
    request = bev_request.model_copy(update={"cache_path": tmp_path / "pinned.csv"})
    monkeypatch.setattr(nrcan_ceud.requests, "get", lambda url, timeout: FakeResponse(b"republished"))
    with pytest.raises(ValueError, match="download has 11 bytes; expected .*fetching.nrcan_ratings_refresh") as error:
        fetch_rating_to_cache(request)
    assert f"--component {BEV}" in str(error.value)
    assert not request.cache_path.exists()
