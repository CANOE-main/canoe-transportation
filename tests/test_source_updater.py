"""Offline maintenance behavior with isolated registries, caches and audit roots."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import requests
import yaml

from fetching import nrcan_ceud
from fetching import statcan_tables as statcan
from fetching import source_updater as updater
from utils import (
    harmonization_rule_fingerprints, harmonization_rule_paths,
    load_config_bundle, resolve_input_path,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"
NRCAN = "nrcan_fuel_consumption_ratings"
FUEL = "fueleconomy_gov_vehicle_data"
STATCAN = "statcan_transport_tables"
ANNUAL_STOCK = "23-10-0308-01"


def digest(content: bytes, algorithm: str = "sha256") -> str:
    return hashlib.new(algorithm, content, usedforsecurity=False).hexdigest()


def rating_csv(request, *, make="Fixture", years=None) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(request.required_columns)
    text = {"Make", "Model", "Vehicle class", "Transmission", "Fuel type", "Fuel type 1", "Fuel type 2"}
    for year in request.expected_model_years if years is None else years:
        writer.writerow([year if col == "Model year" else make if col in text else "1"
                         for col in request.required_columns])
    return stream.getvalue().encode("utf-8")


class FakeResponse:
    def __init__(self, url, *, content=b"", payload=None, headers=None):
        self.url = url
        self.content = content if payload is None else json.dumps(payload).encode("utf-8")
        self.headers = headers or {}
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload

    def iter_content(self, chunk_size=65536):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]

    def close(self):
        return None


class FakeSession:
    def __init__(self):
        self.resources = {}
        self.downloads = {}
        self.calls = []

    def get(self, url, *, timeout, params=None, stream=False):
        self.calls.append((url, params))
        if params is not None:
            return FakeResponse(url, payload={"success": True, "result": self.resources[params["id"]]})
        if url not in self.downloads:
            raise AssertionError(f"Unexpected network request: {url}")
        content, headers = self.downloads[url]
        return FakeResponse(url, content=content, headers=headers)


def save_registry(bundle, data):
    bundle.sources_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8", newline="\n")
    return load_config_bundle(SCENARIO, repo_root=bundle.repo_root)


@pytest.fixture
def isolated_bundle(tmp_path):
    shutil.copytree(REPO_ROOT / "config", tmp_path / "config")
    bundle = load_config_bundle(SCENARIO, repo_root=tmp_path)
    registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    # These source-updater fixtures model a legacy unpinned baseline regardless
    # of which real StatCan releases the checkout has since accepted.
    for component in registry["sources"][STATCAN]["components"].values():
        component["adapter"].pop("snapshot", None)
    for request in nrcan_ceud.iter_rating_requests(bundle):
        original = rating_csv(request, make="Original")
        request.cache_path.parent.mkdir(parents=True, exist_ok=True)
        request.cache_path.write_bytes(original)
        component = registry["sources"][NRCAN]["components"][request.component_key]
        component["adapter"].update(expected_bytes=len(original), expected_md5=digest(original, "md5"))
    return save_registry(bundle, registry)


def rating_session(bundle, *, changed=True):
    session = FakeSession()
    for request in nrcan_ceud.iter_rating_requests(bundle):
        content = rating_csv(request, make="Replacement" if changed else "Original")
        session.resources[request.resource_id] = {
            "id": request.resource_id, "url": request.url, "hash": digest(content, "md5"),
            "size": len(content), "last_modified": f"{request.component_meta.version}T12:00:00",
        }
        session.downloads[request.url] = (content, {})
    return session


def cache_bytes(bundle):
    root = resolve_input_path(bundle, "cache")
    return {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_source_without_policy_is_skipped_even_without_refresh_notes(isolated_bundle):
    source = "nrcan_ceud_transport_provincial"
    registry = yaml.safe_load(isolated_bundle.sources_path.read_text(encoding="utf-8"))
    registry["sources"][source]["refresh_notes"] = ""
    bundle = save_registry(isolated_bundle, registry)
    result = updater.run_update(bundle, sources={source}, session=FakeSession())
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    row = next(item for item in report["sources"] if item["source"] == source)
    assert row["status"] == "skipped"
    assert "No automatic update policy" in row["reason"]
    assert len(result.errors) == 1
    assert "not eligible for automated updates" in result.errors[0]
    assert report["acquisitions"] == []


def assert_preserved(bundle, original, caches):
    assert bundle.sources_path.read_bytes() == original
    assert cache_bytes(bundle) == caches
    assert not bundle.sources_path.with_name(f".{bundle.sources_path.name}.update.lock").exists()


def test_download_redirects_require_source_owned_host_and_remain_checksum_verified(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    original_get = session.get

    def redirected(url, **kwargs):
        response = original_get(url, **kwargs)
        if kwargs.get("params") is None:
            response.url = "https://opencanada.blob.core.windows.net/registered-resource.csv?temporary-token=redacted"
        return response

    session.get = redirected
    result = updater.run_update(bundle, sources={NRCAN}, session=session)
    assert not result.errors
    assert not result.applied
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert any(entry["url"] == "https://opencanada.blob.core.windows.net/registered-resource.csv"
               for entry in report["acquisitions"])
    assert "temporary-token" not in json.dumps(report)
    response = FakeResponse("https://unregistered.example/resource", content=b"bytes")
    session.get = lambda *_args, **_kwargs: response
    with pytest.raises(updater.UpdateError, match="unregistered host"):
        updater._response(session, "https://open.canada.ca/resource", bundle.sources.updater)


def test_http_failure_audit_uses_registered_endpoint_without_signed_query(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    original_get = session.get
    target = nrcan_ceud.iter_rating_requests(bundle)[0].url

    def unavailable(url, **kwargs):
        if url != target:
            return original_get(url, **kwargs)
        response = requests.Response()
        response.status_code = 503
        response.url = "https://opencanada.blob.core.windows.net/registered.csv?temporary-token=fixture"
        response._content_consumed = True
        return response

    session.get = unavailable
    result = updater.run_update(bundle, sources={NRCAN}, session=session, apply=True)
    assert not result.applied
    assert result.errors == (f"{NRCAN}: GET {target} failed: HTTP 503",)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    failure = next(item for item in report["sources"] if item["source"] == NRCAN)
    assert failure["evidence"] == {
        "url": target, "method": "GET", "params": None,
        "exception": "HTTPError", "http_status": 503,
    }
    assert "temporary-token" not in result.log_path.read_text(encoding="utf-8")


def test_default_update_session_retries_only_transient_statuses_with_a_bounded_delay():
    with updater._update_session() as session:
        retries = session.get_adapter("https://open.canada.ca").max_retries
        assert retries.total == 3
        assert retries.retry_after_max == 10
        assert retries.is_retry("GET", 503)
        assert retries.is_retry("POST", 429)
        assert not retries.is_retry("GET", 404)


def test_html_catalogue_response_includes_the_endpoint_and_resource_id(isolated_bundle):
    bundle = isolated_bundle
    request = nrcan_ceud.iter_rating_requests(bundle)[0]
    endpoint = bundle.sources.sources[NRCAN].update.metadata_url
    response = requests.Response()
    response.status_code = 200
    response.url = endpoint
    response.headers["Content-Type"] = "text/html"
    response._content = b"<html>Service unavailable</html>"
    response._content_consumed = True
    session = FakeSession()
    session.get = lambda *_args, **_kwargs: response
    with pytest.raises(updater.UpdateError, match="Expected JSON metadata") as error:
        updater._resource(session, endpoint, request.resource_id, bundle.sources.updater)
    assert error.value.evidence == {
        "url": endpoint, "params": {"id": request.resource_id}, "content_type": "text/html",
    }


def test_download_size_limit_is_enforced_while_streaming(isolated_bundle):
    settings = isolated_bundle.sources.updater.model_copy(update={"max_download_bytes": 3})
    session = FakeSession()
    session.downloads["https://open.canada.ca/resource"] = (b"1234", {})
    with pytest.raises(updater.UpdateError, match="max_download_bytes"):
        updater._response(session, "https://open.canada.ca/resource", settings)


def test_existing_block_snapshot_mapping_can_adopt_a_second_release_preserving_comments(isolated_bundle):
    bundle = isolated_bundle
    text = bundle.sources_path.read_text(encoding="utf-8")
    path = ("sources", "cer_canadas_energy_future", "adapter", "registered_snapshots")
    first = {2026: {"macro-indicators": {"cache_path": "cer_energy_future/first.csv", "expected_bytes": 12}}}
    updated = updater.edit_registry(text, [(path, first)])
    updated = updated.replace("expected_bytes: 12", "expected_bytes: 12  # Keep the reviewer note")
    second = {2026: {"macro-indicators": {"cache_path": "cer_energy_future/second.csv", "expected_bytes": 13}}}
    final = updater.edit_registry(updated, [(path, second)])
    assert "# Keep the reviewer note" in final
    assert yaml.safe_load(final)["sources"]["cer_canadas_energy_future"]["adapter"]["registered_snapshots"] == second
    assert "components:" in final


def test_cache_write_failure_cleans_unregistered_temporary_file_and_lock(isolated_bundle, monkeypatch):
    bundle = isolated_bundle
    original = bundle.sources_path.read_bytes()
    caches = cache_bytes(bundle)
    target = resolve_input_path(bundle, "cache", "failure_fixture", "candidate.csv")
    real_temporary = updater.tempfile.NamedTemporaryFile

    @contextmanager
    def failing_temporary(*args, **kwargs):
        with real_temporary(*args, **kwargs) as handle:
            class Writer:
                name = handle.name

                def write(self, _content):
                    handle.write(b"partial")
                    raise OSError("injected disk-full failure")

            yield Writer()

    monkeypatch.setattr(updater.tempfile, "NamedTemporaryFile", failing_temporary)
    with pytest.raises(OSError, match="disk-full"):
        updater._publish(bundle, original, original.decode("utf-8"), {target: b"candidate"})
    assert_preserved(bundle, original, caches)
    assert list(target.parent.iterdir()) == []


@pytest.mark.parametrize("matches", [True, False])
def test_publication_rechecks_a_cache_created_by_another_process(isolated_bundle, monkeypatch, matches):
    bundle = isolated_bundle
    original = bundle.sources_path.read_bytes()
    first = resolve_input_path(bundle, "cache", "publication_fixture", "first.csv")
    second = first.with_name("second.csv")
    expected = b"candidate"
    concurrent = expected if matches else b"conflicting"
    publish_name = "rename" if updater.os.name == "nt" else "link"
    publish = getattr(updater.os, publish_name)

    def competing_publish(temporary, destination):
        if destination == second:
            destination.write_bytes(concurrent)
        publish(temporary, destination)

    monkeypatch.setattr(updater.os, publish_name, competing_publish)
    files = {first: expected, second: expected}
    if matches:
        updater._publish(bundle, original, original.decode("utf-8"), files)
        assert first.read_bytes() == expected
    else:
        with pytest.raises(updater.UpdateError, match="conflicting cache"):
            updater._publish(bundle, original, original.decode("utf-8"), files)
        assert not first.exists()
    assert second.read_bytes() == concurrent
    assert bundle.sources_path.read_bytes() == original
    assert not bundle.sources_path.with_name(f".{bundle.sources_path.name}.update.lock").exists()


def test_postcommit_lock_cleanup_failure_is_reported_as_applied(isolated_bundle, monkeypatch):
    bundle = isolated_bundle
    lock = bundle.sources_path.with_name(f".{bundle.sources_path.name}.update.lock")
    real_unlink = Path.unlink

    def failed_lock_unlink(path, *args, **kwargs):
        if path == lock:
            raise PermissionError("injected lock cleanup failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", failed_lock_unlink)
    result = updater.run_update(bundle, sources={NRCAN}, session=rating_session(bundle), apply=True)
    assert result.applied and not result.errors
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "applied"
    assert report["applied"]
    assert any("lock cleanup failed" in warning for warning in report["warnings"])
    real_unlink(lock)


@pytest.mark.parametrize("missing", [True, False])
def test_current_publisher_can_restore_missing_or_stale_cache_without_overwriting(isolated_bundle, missing):
    bundle = isolated_bundle
    request = nrcan_ceud.iter_rating_requests(bundle)[0]
    if missing:
        request.cache_path.unlink()
    else:
        request.cache_path.write_bytes(b"old unregistered cache")
    session = rating_session(bundle, changed=False)
    old = cache_bytes(bundle)
    review = updater.run_update(bundle, sources={NRCAN}, session=session)
    assert not review.errors and not review.applied
    row = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert row["evidence"]["registered_cache"]["status"] == ("missing" if missing else "mismatch")
    assert cache_bytes(bundle) == old
    applied = updater.run_update(bundle, sources={NRCAN}, session=session, apply=True)
    assert applied.applied and not applied.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    repaired = nrcan_ceud.iter_rating_requests(current)[0]
    assert repaired.cache_path != request.cache_path
    nrcan_ceud.validate_rating_cache(repaired)
    for path, content in old.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == content


def test_report_only_records_diff_without_adopting(isolated_bundle):
    bundle = isolated_bundle
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, session=rating_session(bundle))
    assert not result.applied and not result.errors
    assert_preserved(bundle, original, caches)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "report_only"
    assert report["registry_sha256_before"] == digest(original)
    assert len(report["candidates"]) == len(nrcan_ceud.iter_rating_requests(bundle))
    assert all(row["status"] == "changed" for row in report["candidates"])
    assert "expected_md5" in result.diff_path.read_text(encoding="utf-8")
    assert "REGISTRY DIFF" in result.log_path.read_text(encoding="utf-8")


def test_apply_same_date_republication_preserves_old_snapshots_and_is_idempotent(isolated_bundle):
    bundle = isolated_bundle
    caches = cache_bytes(bundle)
    session = rating_session(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert result.applied and not result.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    for request in nrcan_ceud.iter_rating_requests(current):
        content = session.downloads[request.url][0]
        assert request.cache_path.read_bytes() == content
        assert digest(content)[:16] in request.cache_path.name
        assert request.expected_md5 == digest(content, "md5")
    for path, content in caches.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == content
    original, snapshots = current.sources_path.read_bytes(), cache_bytes(current)
    second = updater.run_update(current, sources={NRCAN}, apply=True, session=session)
    assert not second.applied and not second.errors
    assert second.diff_path.read_text(encoding="utf-8") == ""
    assert_preserved(current, original, snapshots)


def test_metadata_only_release_update_does_not_replace_cache(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle, changed=False)
    for resource in session.resources.values():
        resource["last_modified"] = "2099-01-15T12:00:00"
    caches = cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert result.applied and not result.errors
    assert cache_bytes(bundle) == caches
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    assert all(request.component_meta.version == "2099-01-15"
               for request in nrcan_ceud.iter_rating_requests(current))


def test_existing_conflicting_snapshot_is_never_overwritten(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    request = nrcan_ceud.iter_rating_requests(bundle)[0]
    content = session.downloads[request.url][0]
    target = updater._snapshot_path(bundle, request.cache_path, content)
    target.write_bytes(b"different existing file")
    caches = cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert not result.errors and result.applied
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    replacement = nrcan_ceud.iter_rating_requests(current)[0].cache_path
    assert replacement != target and replacement.read_bytes() == content
    assert target.read_bytes() == b"different existing file"
    for path, previous in caches.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == previous


def test_adopted_missing_snapshot_is_restored_without_registry_repin(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    first = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert first.applied and not first.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    request = nrcan_ceud.iter_rating_requests(current)[0]
    content = request.cache_path.read_bytes()
    request.cache_path.unlink()
    registry = current.sources_path.read_bytes()
    result = updater.run_update(current, sources={NRCAN}, apply=True, session=session)
    assert result.applied and not result.errors
    assert current.sources_path.read_bytes() == registry
    assert request.cache_path.read_bytes() == content
    assert result.diff_path.read_text(encoding="utf-8") == ""


def test_catalogue_change_during_download_blocks_adoption(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    get = session.get
    first = nrcan_ceud.iter_rating_requests(bundle)[0]
    count = 0

    def racing_get(url, **kwargs):
        nonlocal count
        if kwargs.get("params", {}).get("id") == first.resource_id:
            count += 1
            if count == 2:
                session.resources[first.resource_id]["last_modified"] = "2099-01-15T12:00:00"
        return get(url, **kwargs)

    session.get = racing_get
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert result.errors and not result.applied
    assert "changed during download" in result.errors[0]
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_registry_edit_accepts_single_quoted_labels_and_preserves_unrelated_text(isolated_bundle, newline):
    bundle = isolated_bundle
    request = nrcan_ceud.iter_rating_requests(bundle)[0]
    original = bundle.sources_path.read_text(encoding="utf-8")
    label = request.component_meta.label
    original = original.replace(f"label: {label}", f"label: '{label}'", 1)
    original = "# keep this review comment\n" + original
    original = original.replace("\n", newline)
    path = ("sources", NRCAN, "components", request.component_key, "version")
    updated = updater.edit_registry(original, [(path, "2099-01-15")])
    assert updated.startswith("# keep this review comment" + newline)
    assert f"label: '{label}'" in updated
    assert updated.replace('version: "2099-01-15"', f"version: '{request.component_meta.version}'", 1) == original


@pytest.mark.parametrize("failure", ["checksum", "size", "missing_md5", "year_coverage", "columns", "wrong_identity", "redirect"])
def test_invalid_candidate_preserves_registry_and_all_caches(isolated_bundle, failure):
    bundle = isolated_bundle
    session = rating_session(bundle)
    request = nrcan_ceud.iter_rating_requests(bundle)[0]
    resource = session.resources[request.resource_id]
    if failure == "checksum":
        resource["hash"] = "0" * 32
    elif failure == "size":
        resource["size"] += 1
    elif failure == "missing_md5":
        resource["hash"] = ""
    elif failure in {"year_coverage", "columns"}:
        content = rating_csv(request, years=request.expected_model_years[:-1]) if failure == "year_coverage" else b"Wrong,Columns\n1,2\n"
        session.downloads[request.url] = (content, {})
        resource.update(hash=digest(content, "md5"), size=len(content))
    elif failure == "wrong_identity":
        resource["id"] = "another-resource"
    else:
        resource["url"] = "https://another.example/resource.csv"
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert result.errors and not result.applied
    assert_preserved(bundle, original, caches)
    assert json.loads(result.report_path.read_text(encoding="utf-8"))["status"] == "failed"
    assert result.log_path.is_file()


@pytest.mark.parametrize("selection", [{"not_registered"}, {"epa_omega_baseline"}, set()])
def test_unknown_or_ineligible_selection_fails_with_audit(isolated_bundle, selection):
    session = FakeSession()
    result = updater.run_update(isolated_bundle, sources=selection, apply=True, session=session)
    assert result.errors and not result.applied
    assert not session.calls
    assert result.report_path.is_file() and result.diff_path.is_file() and result.log_path.is_file()


@pytest.mark.parametrize("failure", ["rewrite", "publish", "registry_replace"])
def test_rewrite_and_publication_failures_preserve_inputs(isolated_bundle, monkeypatch, failure):
    bundle = isolated_bundle
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    def fail(*args, **kwargs):
        raise OSError("injected publication failure")

    if failure == "rewrite":
        monkeypatch.setattr(updater, "edit_registry", fail)
    elif failure == "publish":
        monkeypatch.setattr(updater, "_publish", fail)
    else:
        atomic = updater.write_text_atomic

        def fail_registry(text, path, **kwargs):
            if path == bundle.sources_path:
                fail()
            return atomic(text, path, **kwargs)

        monkeypatch.setattr(updater, "write_text_atomic", fail_registry)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=rating_session(bundle))
    assert result.errors and not result.applied
    assert_preserved(bundle, original, caches)
    assert "injected publication failure" in result.log_path.read_text(encoding="utf-8")


def test_failed_selected_batch_does_not_adopt_valid_source(isolated_bundle):
    bundle = isolated_bundle
    session = rating_session(bundle)
    fuel = bundle.sources.sources[FUEL].component("vehicles").adapter
    session.downloads[fuel["url"]] = (b"not a ZIP", {"Last-Modified": "Wed, 15 Jan 2099 12:00:00 GMT"})
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN, FUEL}, apply=True, session=session)
    assert result.errors and not result.applied
    assert_preserved(bundle, original, caches)


def fuel_zip(*, range_columns=True, make="Example", displacement=0, configuration_columns=True):
    columns = ["year", "make", "model", "VClass"]
    if range_columns:
        columns += ["id", "mfrCode", "engId", "atvType", "fuelType1", "fuelType2", "range", "rangeA", "phevBlended"]
    if configuration_columns:
        columns += ["displ", "cylinders", "drive", "trany"]
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for index, year in enumerate([1984, 2024, 2027], 1):
        row = {"year": year, "make": make, "model": "Car", "VClass": "Compact Cars"}
        if range_columns:
            row.update(id=index, mfrCode="EX", engId=index, atvType="EV", fuelType1="Electricity", fuelType2="", range=200, rangeA=0, phevBlended=False)
        if configuration_columns:
            row.update(displ=displacement, cylinders=0, drive="Front-Wheel Drive", trany="Automatic (A1)")
        writer.writerow(row)
    output = io.BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("vehicles.csv", stream.getvalue())
    return output.getvalue()


@pytest.mark.parametrize("range_columns", [True, False])
def test_fueleconomy_validates_shared_class_and_range_contracts(isolated_bundle, range_columns):
    bundle = isolated_bundle
    content = fuel_zip(range_columns=range_columns)
    source = bundle.sources.sources[FUEL]
    session = FakeSession()
    session.downloads[source.component("vehicles").adapter["url"]] = (content, {"Last-Modified": "Thu, 15 Jan 2099 12:00:00 GMT"})
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={FUEL}, apply=True, session=session)
    if not range_columns:
        assert result.errors and not result.applied
        assert_preserved(bundle, original, caches)
        return
    assert result.errors and not result.applied  # No old comparison cache: review is explicit.
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    result = updater.run_update(bundle, sources={FUEL}, apply=True, session=session,
                                review_report=review.report_path)
    assert result.applied and not result.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root).sources.sources[FUEL]
    assert "2099-01-15" in current.version
    assert all("2099-01-15" in comp.label for comp in current.components.values())
    assert current.component("range_evidence").adapter["cache_component"] == "vehicles"
    target = resolve_input_path(bundle, "cache", current.component("vehicles").adapter["cache_path"])
    assert target.read_bytes() == content


def fuel_review_fixture(bundle):
    """Pin a complete synthetic baseline and propose a changed selected record."""
    registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    adapter = registry["sources"][FUEL]["components"]["vehicles"]["adapter"]
    original = fuel_zip(make="Original")
    target = resolve_input_path(bundle, "cache", adapter["cache_path"])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(original)
    adapter.update(expected_sha256=digest(original), expected_bytes=len(original))
    bundle = save_registry(bundle, registry)
    content = fuel_zip(make="Replacement")
    session = FakeSession()
    session.downloads[adapter["url"]] = (
        content, {"Last-Modified": "Thu, 15 Jan 2099 12:00:00 GMT"}
    )
    return bundle, target, original, content, session


def test_fueleconomy_propulsion_only_change_requires_exact_review(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    component = bundle.sources.sources[FUEL].component("vehicles").adapter
    session.downloads[component["url"]] = (
        fuel_zip(make="Original", displacement=2),
        {"Last-Modified": "Thu, 15 Jan 2099 12:00:00 GMT"},
    )
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    report = json.loads(review.report_path.read_text())
    candidate = report["candidates"][0]
    records = candidate["evidence"]["selected_records"]
    assert records["vehicles"]["changed"] is False
    assert records["range_evidence"]["changed"] is False
    assert records["propulsion_configuration"]["changed"] is True
    assert "FuelEconomy propulsion configuration selected records changed" in candidate["review_required"]
    blocked = updater.run_update(bundle, sources={FUEL}, apply=True, session=session)
    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)
    adopted = updater.run_update(bundle, sources={FUEL}, apply=True, session=session, review_report=review.report_path)
    assert adopted.applied and not adopted.errors


def test_fueleconomy_missing_consumed_propulsion_column_blocks_adoption(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    component = bundle.sources.sources[FUEL].component("vehicles").adapter
    session.downloads[component["url"]] = (
        fuel_zip(make="Original", configuration_columns=False),
        {"Last-Modified": "Thu, 15 Jan 2099 12:00:00 GMT"},
    )
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={FUEL}, apply=True, session=session)
    assert result.errors and not result.applied
    assert any("Usecols" in error or "columns" in error for error in result.errors)
    assert_preserved(bundle, original, caches)


def test_review_required_blocks_other_valid_sources_in_the_batch(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    ratings = rating_session(bundle)
    session.downloads.update(ratings.downloads)
    session.resources.update(ratings.resources)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    blocked = updater.run_update(bundle, sources={FUEL, NRCAN}, session=session, apply=True)

    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)


def test_review_report_is_only_accepted_for_explicit_apply(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    rejected = updater.run_update(bundle, sources={FUEL}, session=session,
                                  review_report=review.report_path)

    assert rejected.errors and not rejected.applied
    assert "--apply" in " ".join(rejected.errors)
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("changed", ["candidate_bytes", "registry", "rules", "rules_partition", "scenario", "selection",
                                    "failed_report", "tampered_evidence", "missing_report"])
def test_review_rejects_changed_context_and_untrusted_report(isolated_bundle, changed):
    bundle, _target, _old, content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    report_path = review.report_path
    selection = {FUEL}
    if changed == "candidate_bytes":
        report = json.loads(report_path.read_text(encoding="utf-8"))
        captured = report_path.parent / report["acquisitions"][0]["file"]
        captured.write_bytes(content + b"changed after review")
    elif changed == "registry":
        with bundle.sources_path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Registry edited after review.\n")
        bundle = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    elif changed in {"rules", "rules_partition", "scenario"}:
        path = (harmonization_rule_paths(bundle)[1] if changed == "rules_partition" else
                bundle.repo_root / ("config/parameters/rules.yaml" if changed == "rules" else SCENARIO))
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Configuration edited after review.\n")
        bundle = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    elif changed == "selection":
        selection.add(NRCAN)
    elif changed in {"failed_report", "tampered_evidence"}:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if changed == "failed_report":
            report["status"] = "failed"
            report["errors"] = ["The reviewed run failed validation."]
        else:
            report["candidates"][0]["evidence"]["sha256"] = "0" * 64
        report_path.write_text(json.dumps(report), encoding="utf-8")
    else:
        report_path = report_path.with_name("missing-report.json")
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    result = updater.run_update(bundle, sources=selection, session=session, apply=True,
                                review_report=report_path)

    assert result.errors and not result.applied
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("live_change", [b"changed ZIP packaging", b"different live records"])
def test_reviewed_apply_revalidates_exact_captured_bytes_without_network(isolated_bundle, live_change):
    bundle, original_path, original, content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    assert not review.errors and report["review_schema_version"] == 3
    assert report["candidates"][0]["review_required"]
    assert (review.report_path.parent / report["acquisitions"][0]["file"]).read_bytes() == content
    assert original_path.read_bytes() == original
    # Adopt the exact reviewed capture even if the live URL has since changed.
    session.downloads = {url: (live_change, {}) for url in session.downloads}
    session.calls.clear()
    applied = updater.run_update(bundle, sources={FUEL}, session=session, apply=True,
                                 review_report=review.report_path)
    assert not applied.errors and applied.applied
    assert session.calls == []
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    adapter = current.sources.sources[FUEL].component("vehicles").adapter
    target = resolve_input_path(current, "cache", adapter["cache_path"])
    assert target.read_bytes() == content
    assert adapter["expected_sha256"] == digest(content)
    assert original_path.read_bytes() == original
    applied_report = json.loads(applied.report_path.read_text(encoding="utf-8"))
    assert (applied.report_path.parent / applied_report["acquisitions"][0]["file"]).read_bytes() == content


@pytest.mark.parametrize("damage", ["missing_payload", "outside_path", "legacy_schema"])
def test_reviewed_acquisition_damage_fails_without_network_or_publication(isolated_bundle, damage):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    if damage == "missing_payload":
        (review.report_path.parent / report["acquisitions"][0]["file"]).unlink()
    elif damage == "outside_path":
        report["acquisitions"][0]["file"] = "../outside.bin"
        report["review_fingerprint"] = updater._review_fingerprint(report)
    else:
        report["review_schema_version"] = 1
        report["review_fingerprint"] = updater._review_fingerprint(report)
    review.report_path.write_text(json.dumps(report), encoding="utf-8")
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    session.calls.clear()
    result = updater.run_update(bundle, sources={FUEL}, session=session, apply=True,
                                review_report=review.report_path)
    assert result.errors and not result.applied
    assert not session.calls
    assert_preserved(bundle, original, caches)


def test_review_binds_every_declared_rules_file_and_its_digest(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    expected = harmonization_rule_fingerprints(bundle)
    assert not review.errors
    assert len(expected) > 1 and "rules.yaml" in expected
    assert report["rules_fingerprints"] == expected
    serialized = json.dumps(expected, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert report["rules_sha256"] == hashlib.sha256(serialized).hexdigest()
    report["rules_fingerprints"][harmonization_rule_paths(bundle)[1].name] = "0" * 64
    assert updater._review_fingerprint(report) != report["review_fingerprint"]


def test_old_single_file_review_requires_fresh_complete_rules_review(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    report["review_schema_version"] = 2
    report.pop("rules_fingerprints")
    report["review_fingerprint"] = updater._review_fingerprint(report)
    review.report_path.write_text(json.dumps(report), encoding="utf-8")
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    session.calls.clear()

    result = updater.run_update(bundle, sources={FUEL}, session=session, apply=True, review_report=review.report_path)

    assert result.errors and not result.applied
    assert "complete rules-file identity contract" in " ".join(result.errors)
    assert "report-only review again" in " ".join(result.errors)
    assert not session.calls
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("change_at", ["candidate_validation", "durable_review"])
def test_partition_change_during_apply_fails_before_any_publication(isolated_bundle, monkeypatch, change_at):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    partition = harmonization_rule_paths(bundle)[1]
    changed = False

    def change_partition():
        nonlocal changed
        if not changed:
            with partition.open("a", encoding="utf-8") as handle:
                handle.write("\n# Declared partition changed during reviewed apply.\n")
            changed = True

    if change_at == "candidate_validation":
        validate = updater._fueleconomy

        def mutate_after_validation(*args, **kwargs):
            candidates = validate(*args, **kwargs)
            change_partition()
            return candidates

        monkeypatch.setattr(updater, "_fueleconomy", mutate_after_validation)
    else:
        write = updater.write_text_atomic

        def mutate_after_durable_report(text, path, *args, **kwargs):
            result = write(text, path, *args, **kwargs)
            if Path(path).name == "report.json":
                change_partition()
            return result

        monkeypatch.setattr(updater, "write_text_atomic", mutate_after_durable_report)
    session.calls.clear()

    result = updater.run_update(bundle, sources={FUEL}, session=session, apply=True, review_report=review.report_path)

    assert changed and result.errors and not result.applied
    assert "Rules configuration changed" in " ".join(result.errors)
    assert not session.calls
    assert_preserved(bundle, original, caches)


class StatCanSelectionSession(FakeSession):
    """Expose one table only, so requesting an unselected table fails the test."""

    def __init__(self, request, payload, content):
        super().__init__()
        self.request = request
        self.payload = payload
        self.archive_url = f"https://www150.statcan.gc.ca/n1/tbl/csv/{request.product_id}-eng.zip"
        self.downloads[self.archive_url] = (content, {})

    def post(self, url, *, json, timeout, stream=False):
        assert url == self.request.metadata_url
        assert json == [{"productId": self.request.product_id}]
        self.calls.append((url, json))
        return FakeResponse(url, payload=self.payload)

    def get(self, url, *, timeout, params=None, stream=False):
        if url == self.request.download_api_url:
            self.calls.append((url, params))
            return FakeResponse(url, payload={"status": "SUCCESS", "object": self.archive_url})
        return super().get(url, timeout=timeout, params=params, stream=stream)


def annual_statcan_selection_fixture(bundle):
    request = next(item for item in statcan.build_requests(bundle) if item.table_id == ANNUAL_STOCK)
    table = request.table_rules
    metadata = {
        "productId": request.product_id, "cubeTitleEn": request.title,
        "releaseTime": "2026-09-22T08:30:00", "cubeStartDate": "2023-01-01",
        "cubeEndDate": "2025-01-01", "frequencyCode": 12,
        "dimension": [{
            "dimensionNameEn": name,
            "member": [{"memberNameEn": member} for member in table.get("selectors", {}).get(name, ["Ontario"])],
        } for name in table["metadata_dimensions"]],
    }
    rows = []
    for year in (2023, 2025):
        row = {column: "fixture" for column in table["required_columns"]}
        row.update(REF_DATE=year, GEO="Ontario", UOM="Number", SCALAR_FACTOR="units", VALUE=2)
        for column, members in table["selectors"].items():
            row[column] = members[0]
        rows.append(row)
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=table["required_columns"], lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    zipped = io.BytesIO()
    with ZipFile(zipped, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(f"{request.product_id}.csv", stream.getvalue())
    request.metadata_cache_path.parent.mkdir(parents=True, exist_ok=True)
    previous = dict(metadata, cubeEndDate="2023-01-01", releaseTime="2024-01-01T08:30:00")
    request.metadata_cache_path.write_text(json.dumps([{"status": "SUCCESS", "object": previous}]), encoding="utf-8")
    request.archive_cache_path.write_bytes(b"preserved legacy archive")
    return StatCanSelectionSession(request, [{"status": "SUCCESS", "object": metadata}], zipped.getvalue())


def test_statcan_component_review_downloads_only_selected_table_and_preserves_inputs(isolated_bundle):
    bundle = isolated_bundle
    session = annual_statcan_selection_fixture(bundle)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    result = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session)

    assert not result.errors and not result.applied
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["selected_components"] == [ANNUAL_STOCK]
    assert [(row["source"], row["component"]) for row in report["candidates"]] == [(STATCAN, ANNUAL_STOCK)]
    assert len(report["acquisitions"]) == 4  # Metadata, resolver, archive, metadata stability check.
    for row in report["candidates"]:
        assert row["review_required"] == ["StatCan reference-period coverage expanded"]
        assert all(change["path"][:4] == ["sources", STATCAN, "components", ANNUAL_STOCK] for change in row["changes"])
    assert_preserved(bundle, original, caches)


def test_selected_statcan_review_replays_offline_and_cannot_edit_other_tables(isolated_bundle):
    bundle = isolated_bundle
    session = annual_statcan_selection_fixture(bundle)
    original_registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    old_caches = cache_bytes(bundle)
    review = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session)
    assert not review.errors
    session.calls.clear()
    session.downloads.clear()

    result = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session,
                                apply=True, review_report=review.report_path)

    assert result.applied and not result.errors
    assert session.calls == []
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    current_registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    for component, before in original_registry["sources"][STATCAN]["components"].items():
        if component != ANNUAL_STOCK:
            assert current_registry["sources"][STATCAN]["components"][component] == before
    request = next(item for item in statcan.build_requests(adopted) if item.table_id == ANNUAL_STOCK)
    assert request.snapshot is not None
    assert statcan.ensure_cached_artifacts(request, download=False) == ("cached", "cached", "")
    for relative, content in old_caches.items():
        assert (resolve_input_path(bundle, "cache") / relative).read_bytes() == content


@pytest.mark.parametrize("sources,components", [
    (None, {ANNUAL_STOCK}), (set(), {ANNUAL_STOCK}),
    ({STATCAN, NRCAN}, {ANNUAL_STOCK}), ({STATCAN}, set()),
    ({STATCAN}, {"missing-table"}), ({NRCAN}, {"vehicles"}),
])
def test_invalid_component_selection_fails_before_acquisition(isolated_bundle, sources, components):
    bundle = isolated_bundle
    session = FakeSession()
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    result = updater.run_update(bundle, sources=sources, components=components, session=session, apply=True)

    assert result.errors and not result.applied
    assert "selection" in " ".join(result.errors).lower()
    assert not session.calls
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("selection", [None, {"20-10-0025-01"}])
def test_review_rejects_dropped_or_changed_component_selection(isolated_bundle, selection):
    bundle = isolated_bundle
    session = annual_statcan_selection_fixture(bundle)
    review = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session)
    assert not review.errors
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    session.calls.clear()

    result = updater.run_update(bundle, sources={STATCAN}, components=selection, session=session,
                                apply=True, review_report=review.report_path)

    assert result.errors and not result.applied
    assert "selection" in " ".join(result.errors)
    assert not session.calls
    assert_preserved(bundle, original, caches)


def test_component_selection_is_bound_to_review_fingerprint(isolated_bundle):
    bundle = isolated_bundle
    session = annual_statcan_selection_fixture(bundle)
    review = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session)
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    report["selected_components"] = ["20-10-0025-01"]
    assert updater._review_fingerprint(report) != report["review_fingerprint"]


def test_selected_candidate_cannot_propose_cross_component_registry_edit(isolated_bundle, monkeypatch):
    bundle = isolated_bundle
    session = FakeSession()
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    candidate = updater.Candidate(STATCAN, ANNUAL_STOCK,
        [(("sources", STATCAN, "components", "20-10-0025-01", "label"), "unselected edit")], {}, {})
    monkeypatch.setattr(updater, "_statcan", lambda *args, **kwargs: [candidate])

    result = updater.run_update(bundle, sources={STATCAN}, components={ANNUAL_STOCK}, session=session, apply=True)

    assert result.errors and not result.applied
    assert "outside its component" in " ".join(result.errors)
    assert_preserved(bundle, original, caches)


def test_legacy_unfiltered_review_fingerprint_and_offline_replay_remain_compatible(isolated_bundle):
    bundle, _target, _old, _content, session = fuel_review_fixture(isolated_bundle)
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    assert report.pop("selected_components") is None
    assert updater._review_fingerprint(report) == report["review_fingerprint"]
    review.report_path.write_text(json.dumps(report), encoding="utf-8")
    session.calls.clear()

    result = updater.run_update(bundle, sources={FUEL}, session=session, apply=True, review_report=review.report_path)

    assert not result.errors and result.applied
    assert session.calls == []


@pytest.mark.parametrize("sources", [[], [STATCAN, NRCAN]])
def test_cli_component_filter_requires_exactly_one_source(monkeypatch, sources):
    arguments = ["source_updater", "--scenario", SCENARIO, "--component", ANNUAL_STOCK]
    for source in sources:
        arguments += ["--source", source]
    monkeypatch.setattr("sys.argv", arguments)
    monkeypatch.setattr(updater, "load_config_bundle", lambda *_: pytest.fail("CLI must validate selection before loading config"))
    with pytest.raises(SystemExit) as error:
        updater.main()
    assert error.value.code == 2
