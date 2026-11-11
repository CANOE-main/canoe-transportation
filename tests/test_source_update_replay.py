"""Offline consumption of explicitly adopted immutable source revisions."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from pydantic import ValidationError

from fetching import cer_enerfuture as cer
from fetching import statcan_tables as statcan
from fetching import vehicle_population as ontario
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def bundle(tmp_path):
    configured = load_config_bundle("config/scenarios/default.yaml", repo_root=REPO_ROOT)
    configured.paths.inputs.cache = str(tmp_path / "cache")
    return configured


def zip_bytes(member: str, content: str = "fixture\n") -> bytes:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr(member, content)
    return stream.getvalue()


def statcan_snapshot(bundle, *, metadata: bytes, archive: bytes) -> statcan.StatCanTableRequest:
    bundle.sources.sources[statcan.SOURCE_ID].component("20-10-0025-01").adapter["snapshot"] = {
        "archive_cache_path": "statcan_transport/revision/20100025-eng.zip",
        "metadata_cache_path": "statcan_transport/revision/20100025-metadata.json",
        "archive_sha256": sha256(archive),
        "metadata_sha256": sha256(metadata),
        "release_time": "2026-10-26T08:30:00",
    }
    return next(item for item in statcan.build_requests(bundle) if item.table_id == "20-10-0025-01")


def metadata_bytes() -> bytes:
    payload = [{"status": "SUCCESS", "object": {"productId": 20100025}}]
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


class Response:
    def __init__(self, content=b"", payload=None):
        self.content = content
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload

    def iter_content(self, chunk_size):
        yield self.content

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


class WdsSession:
    def __init__(self, *, metadata: bytes, archive: bytes):
        self.metadata = metadata
        self.archive = archive
        self.calls = 0

    def post(self, *_args, **_kwargs):
        self.calls += 1
        return Response(payload=json.loads(self.metadata))

    def get(self, url, **_kwargs):
        self.calls += 1
        if "getFullTableDownloadCSV" in url:
            return Response(payload={"status": "SUCCESS", "object": "https://example.test/revision.zip"})
        return Response(self.archive)


def test_statcan_explicit_snapshot_replays_pair_without_network_and_rejects_tampering(bundle):
    metadata = metadata_bytes()
    archive = zip_bytes("20100025.csv")
    request = statcan_snapshot(bundle, metadata=metadata, archive=archive)
    original = Path(bundle.paths.inputs.cache) / "statcan_transport" / "20100025-eng.zip"
    original.parent.mkdir(parents=True)
    original.write_bytes(b"preserved original snapshot")
    session = WdsSession(metadata=metadata, archive=archive)
    assert statcan.ensure_cached_artifacts(request, download=True, session=session)[:2] == ("downloaded", "downloaded")
    assert request.metadata_cache_path.read_bytes() == metadata
    assert request.archive_cache_path.read_bytes() == archive
    calls = session.calls
    assert statcan.ensure_cached_artifacts(request, download=False, session=session) == ("cached", "cached", "")
    assert statcan.ensure_cached_artifacts(request, download=True, session=session) == ("cached", "cached", "")
    assert session.calls == calls
    assert original.read_bytes() == b"preserved original snapshot"
    request.archive_cache_path.write_bytes(zip_bytes("20100025.csv", "changed\n"))
    with pytest.raises(statcan.StatCanSourceError, match="snapshot SHA-256 changed"):
        statcan.ensure_cached_artifacts(request, download=False)
    with pytest.raises(statcan.StatCanSourceError, match="snapshot SHA-256 changed"):
        statcan.validate_source(request)


def test_statcan_new_metadata_and_archive_must_match_adopted_hashes_before_publication(bundle):
    metadata = metadata_bytes()
    archive = zip_bytes("20100025.csv")
    request = statcan_snapshot(bundle, metadata=metadata, archive=archive)
    # A different metadata JSON value also receives a valid product ID.
    altered_payload = json.loads(metadata)
    altered_payload[0]["object"]["newField"] = "changed"
    session = WdsSession(metadata=json.dumps(altered_payload).encode(), archive=archive)
    with pytest.raises(statcan.StatCanSourceError, match="downloaded metadata SHA-256 changed"):
        statcan.fetch_metadata_to_cache(request, session=session)
    assert not request.metadata_cache_path.exists()
    session = WdsSession(metadata=metadata, archive=zip_bytes("20100025.csv", "changed\n"))
    with pytest.raises(statcan.StatCanSourceError, match="downloaded archive SHA-256 changed"):
        statcan.fetch_archive_to_cache(request, session=session)
    assert not request.archive_cache_path.exists()
    assert not request.archive_cache_path.with_suffix(".zip.part").exists()


@pytest.mark.parametrize("field,value", [
    ("archive_cache_path", "../outside.zip"),
    ("metadata_cache_path", "C:\\outside.json"),
    ("archive_cache_path", "/outside.zip"),
    ("metadata_cache_path", "\\\\server\\share\\outside.json"),
    ("archive_sha256", "not-a-hash"),
    ("metadata_sha256", ""),
    ("release_time", ""),
])
def test_statcan_snapshot_configuration_rejects_invalid_values_before_io(bundle, field, value):
    statcan_snapshot(bundle, metadata=metadata_bytes(), archive=zip_bytes("20100025.csv"))
    snapshot = bundle.sources.sources[statcan.SOURCE_ID].component("20-10-0025-01").adapter["snapshot"]
    snapshot[field] = value
    with pytest.raises(ValidationError):
        statcan.build_requests(bundle)


def test_statcan_snapshot_requires_complete_pair(bundle):
    bundle.sources.sources[statcan.SOURCE_ID].component("20-10-0025-01").adapter["snapshot"] = {
        "archive_cache_path": "revision.zip", "archive_sha256": "a" * 64,
    }
    with pytest.raises(ValidationError, match="metadata_cache_path"):
        statcan.build_requests(bundle)


def cer_snapshot(bundle, content: bytes) -> cer.CerTableRequest:
    bundle.sources.sources[cer.SOURCE_ID].adapter["registered_snapshots"] = {
        2026: {"macro-indicators": {
            "url": "https://www.cer-rec.gc.ca/open/energy/revised.csv",
            "cache_path": "cer_energy_future/2026/revision/macro-indicators.csv",
            "expected_sha256": sha256(content),
            "expected_bytes": len(content),
        }},
    }
    return next(item for item in cer.build_requests(bundle) if item.component_id == "macro-indicators")


def test_cer_adopted_same_edition_routes_and_validates_cached_identity(bundle):
    content = b"Scenario,Region,Variable,Year,Value\nCurrent Measures,Canada,Population,2005,1\n"
    request = cer_snapshot(bundle, content)
    assert request.edition == 2026
    assert request.url.endswith("/revised.csv")
    assert request.cache_path.is_relative_to(Path(bundle.paths.inputs.cache))
    request.cache_path.parent.mkdir(parents=True)
    request.cache_path.write_bytes(content)
    assert cer.fetch_to_cache(request) == "cached"
    request.cache_path.write_bytes(content.replace(b",1\n", b",2\n"))
    with pytest.raises(cer.CerEnergyFutureError, match="snapshot SHA-256 changed"):
        cer.fetch_to_cache(request)
    with pytest.raises(cer.CerEnergyFutureError, match="snapshot SHA-256 changed"):
        cer.read_and_validate_source(request)
    request.cache_path.write_bytes(b"short")
    with pytest.raises(cer.CerEnergyFutureError, match="snapshot byte count changed"):
        cer.read_and_validate_source(request)


def test_cer_adopted_download_must_match_snapshot_before_publication(bundle):
    request = cer_snapshot(bundle, b"registered bytes")

    class Session:
        def get(self, *_args, **_kwargs):
            return Response(b"different bytes!")

    with pytest.raises(cer.CerEnergyFutureError, match="snapshot SHA-256 changed"):
        cer.fetch_to_cache(request, session=Session())
    assert not request.cache_path.exists()
    assert not request.cache_path.with_suffix(".csv.part").exists()


@pytest.mark.parametrize("field,value", [
    ("cache_path", "../outside.csv"),
    ("cache_path", "C:outside.csv"),
    ("expected_sha256", "wrong"),
    ("expected_bytes", 0),
])
def test_cer_snapshot_rejects_invalid_metadata_before_io(bundle, field, value):
    cer_snapshot(bundle, b"content")
    snapshot = bundle.sources.sources[cer.SOURCE_ID].adapter["registered_snapshots"][2026]["macro-indicators"]
    snapshot[field] = value
    with pytest.raises(ValidationError):
        cer.build_requests(bundle)


def test_cer_snapshot_rejects_unconfigured_component_or_edition(bundle):
    cer_snapshot(bundle, b"content")
    snapshots = bundle.sources.sources[cer.SOURCE_ID].adapter["registered_snapshots"]
    snapshots[2027] = snapshots.pop(2026)
    with pytest.raises(cer.CerEnergyFutureError, match="unregistered edition"):
        cer.build_requests(bundle)


def test_ontario_adopted_cache_override_preserves_history_and_registered_hash(bundle):
    source = bundle.sources.sources[ontario.SOURCE_ID]
    selected = source.adapter["registered_resources"][-1]
    year = selected["year"]
    baseline = ontario.registered_requests(bundle, year=year)[0]
    original = baseline.cache_path
    original.parent.mkdir(parents=True)
    original.write_bytes(b"old annual snapshot")
    selected["cache_path"] = f"ontario_vehicle_population/{year}_revised.zip"
    content = zip_bytes(baseline.report_a_members[0])
    selected["cache_sha256"] = sha256(content)
    adopted = ontario.registered_requests(bundle, year=year)[0]
    adopted.cache_path.write_bytes(content)
    assert adopted.cache_path != original
    assert len(ontario.registered_requests(bundle, year=None)) == len(source.adapter["registered_resources"])
    assert ontario.validate_source(adopted, require_auxiliary=False)["A"]
    assert original.read_bytes() == b"old annual snapshot"
    adopted.cache_path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="Cached Ontario archive SHA-256 changed"):
        ontario.validate_source(adopted, require_auxiliary=False)


@pytest.mark.parametrize("cache_path", ["../outside.zip", "/outside.zip", "C:\\outside.zip", "\\\\host\\share\\outside.zip"])
def test_ontario_cache_override_rejects_unsafe_paths_before_io(bundle, cache_path):
    bundle.sources.sources[ontario.SOURCE_ID].adapter["registered_resources"][-1]["cache_path"] = cache_path
    with pytest.raises(ValidationError, match="cache-relative"):
        ontario.registered_requests(bundle, year=None)
