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
import yaml

from fetching import nrcan_ceud
from fetching import source_updater as updater
from utils import load_config_bundle, resolve_input_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"
NRCAN = "nrcan_fuel_consumption_ratings"
FUEL = "fueleconomy_gov_vehicle_data"
TC = "transport_canada_ev_dashboard"


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
    response = FakeResponse("https://unregistered.example/resource", content=b"bytes")
    session.get = lambda *_args, **_kwargs: response
    with pytest.raises(updater.UpdateError, match="unregistered host"):
        updater._response(session, "https://open.canada.ca/resource", bundle.sources.updater)


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
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={NRCAN}, apply=True, session=session)
    assert result.errors and not result.applied
    assert "conflicting cache" in result.errors[0]
    assert_preserved(bundle, original, caches)


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


def fuel_zip(*, range_columns=True):
    columns = ["year", "make", "model", "VClass"]
    if range_columns:
        columns += ["id", "mfrCode", "engId", "atvType", "fuelType1", "fuelType2", "range", "rangeA", "phevBlended"]
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for index, year in enumerate([1984, 2024, 2027], 1):
        row = {"year": year, "make": "Example", "model": "Car", "VClass": "Compact Cars"}
        if range_columns:
            row.update(id=index, mfrCode="EX", engId=index, atvType="EV", fuelType1="Electricity", fuelType2="", range=200, rangeA=0, phevBlended=False)
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
    assert result.applied and not result.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root).sources.sources[FUEL]
    assert "2099-01-15" in current.version
    assert all("2099-01-15" in comp.label for comp in current.components.values())
    assert current.component("range_evidence").adapter["cache_component"] == "vehicles"
    target = resolve_input_path(bundle, "cache", current.component("vehicles").adapter["cache_path"])
    assert target.read_bytes() == content


def dashboard_html(bundle, *, share="1%", ytd_share=None, columns=("Q1", "YTD"),
                   quarter_values=None, reporting_date=None):
    from fetching.assorted_sources import module_rules

    rules = module_rules(bundle)["tc_ev_dashboard"]
    chargers = rules["public_chargers"]
    charger_regions = chargers["source_regions"]
    year = bundle.scenario.sources.selections[TC].year
    headers = {quarter: f"Market share ({quarter} {year})" for quarter in ("Q1", "Q2", "Q3", "Q4")}
    headers["YTD"] = rules["ytd_header"]
    values = {quarter: share for quarter in ("Q1", "Q2", "Q3", "Q4")}
    values.update(quarter_values or {})
    values["YTD"] = share if ytd_share is None else ytd_share
    header_cells = "".join(f"<th>{headers[column]}</th>" for column in columns)
    share_cells = "".join(f"<td>{values[column]}</td>" for column in columns)
    province_rows = "".join(f"<tr><td>{region}</td><td>Total public chargers: 1</td></tr>" for region in charger_regions)
    share_rows = "".join(f"<tr><td>{region}</td>{share_cells}</tr>" for region in rules["source_regions"])
    return (f'<h2 id="{rules["heading_id"]}">{rules["heading_text"]}</h2><table id="{rules["table_id"]}">'
            f'<tr><th>Province/territory</th>{header_cells}</tr>'
            f'{share_rows}</table>'
            f'<h2 id="{chargers["heading_id"]}">Public</h2><p>Total chargers: <strong>{len(charger_regions)}</strong></p>'
            '<h3 id="ratio-of-light-duty-electric-vehicles-to-public-ev-chargers">Ratio</h3>'
            f'<table id="{chargers["chart_table_id"]}"><tr><th>Year</th><th>Level 2</th><th>Level 3</th></tr>'
            f'<tr><td>{reporting_date or chargers["as_of"]}</td><td>10</td><td>{len(charger_regions)-10}</td></tr></table>'
            f'<table id="{chargers["province_table_id"]}"><tr><th>Province/territory</th><th>Charger data</th></tr>'
            f'{province_rows}</table>').encode("utf-8")


def pin_dashboard(bundle):
    """Register fixture bytes without touching the repository's accepted snapshot."""
    registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    source = registry["sources"][TC]["adapter"]
    old = dashboard_html(bundle)
    target = resolve_input_path(bundle, "cache", source["cache_path"])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(old)
    source.update(expected_sha256=digest(old), expected_bytes=len(old))
    return save_registry(bundle, registry), target, old


def dashboard_session(bundle, content):
    session = FakeSession()
    session.downloads[bundle.sources.sources[TC].adapter["access"]["url"]] = (content, {})
    return session


@pytest.mark.parametrize("changed_values", [False, True])
def test_tc_republish_requires_identical_shares_and_chargers(isolated_bundle, changed_values):
    bundle, target, old = pin_dashboard(isolated_bundle)
    new = dashboard_html(bundle, share="2%" if changed_values else "1%") + b"<!-- harmless footer update -->"
    session = dashboard_session(bundle, new)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={TC}, apply=True, session=session)
    if changed_values:
        assert result.errors and not result.applied
        assert "review" in " ".join(result.errors).lower()
        assert_preserved(bundle, original, caches)
    else:
        assert result.applied and not result.errors
        assert target.read_bytes() == old


def test_tc_additional_reordered_quarter_keeps_configured_observation_columns(isolated_bundle):
    bundle, target, old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle, columns=("Q2", "YTD", "Q1"), quarter_values={"Q2": "9%"})
    session = dashboard_session(bundle, content)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    result = updater.run_update(bundle, sources={TC}, session=session)

    assert not result.errors and not result.applied
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "report_only"
    evidence = report["candidates"][0]["evidence"]
    selection = evidence["publication_selection"]
    assert selection["before"]["selected_headers"]["quarter"] == "Market share (Q1 2026)"
    assert selection["after"]["selected_headers"]["quarter"] == "Market share (Q1 2026)"
    assert selection["after"]["additional_quarter_columns"] == ["Market share (Q2 2026)"]
    assert evidence["observations"]["before"] == evidence["observations"]["after"]
    assert evidence["observation_changes"] == []
    assert_preserved(bundle, original, caches)
    applied = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                 review_report=result.report_path)
    assert applied.applied and not applied.errors
    assert target.read_bytes() == old


def test_tc_ytd_change_requires_review_then_normalizes_offline_and_is_idempotent(isolated_bundle, monkeypatch):
    from fetching import assorted_sources as adapter

    bundle, target, old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle, ytd_share="2%", columns=("Q2", "YTD", "Q1"),
                             quarter_values={"Q2": "9%"})
    session = dashboard_session(bundle, content)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    review = updater.run_update(bundle, sources={TC}, session=session)

    assert not review.errors and not review.applied
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    candidate = report["candidates"][0]
    assert candidate["review_required"]
    evidence = candidate["evidence"]
    assert len(evidence["observations"]["before"]["market_shares"]) == 14
    assert len(evidence["observations"]["after"]["market_shares"]) == 14
    assert len(evidence["observation_changes"]) == 14
    assert all(change["component"] == "market_shares" and
               change["before"]["ytd_market_share"] == 0.01 and
               change["after"]["ytd_market_share"] == 0.02
               for change in evidence["observation_changes"])
    assert_preserved(bundle, original, caches)
    blocked = updater.run_update(bundle, sources={TC}, session=session, apply=True)
    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)

    adopted = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                 review_report=review.report_path)

    assert adopted.applied and not adopted.errors
    assert target.read_bytes() == old
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    source = current.sources.sources[TC]
    assert source.adapter["expected_sha256"] == digest(content)
    assert resolve_input_path(current, "cache", source.adapter["cache_path"]).read_bytes() == content
    no_network = FakeSession()
    monkeypatch.setattr(adapter, "load_config_bundle", lambda _path: current)
    output = adapter.fetch_and_normalize_tc_dashboard(SCENARIO, download=False, session=no_network)
    assert no_network.calls == []
    rules = adapter.module_rules(current)["tc_ev_dashboard"]
    with (output / rules["output_file"]).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 14
    assert all(float(row["quarter_market_share"]) == 0.01 for row in rows)
    assert all(float(row["ytd_market_share"]) == 0.02 for row in rows)

    snapshots, registry = cache_bytes(current), current.sources_path.read_bytes()
    repeated = updater.run_update(current, sources={TC}, session=session, apply=True)
    assert not repeated.applied and not repeated.errors
    assert_preserved(current, registry, snapshots)


def test_tc_missing_pinned_baseline_is_reviewable_and_preserved_until_adoption(isolated_bundle):
    bundle, target, _old = pin_dashboard(isolated_bundle)
    target.unlink()
    content = dashboard_html(bundle, ytd_share="2%", columns=("Q1", "Q2", "YTD"))
    session = dashboard_session(bundle, content)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    review = updater.run_update(bundle, sources={TC}, session=session)

    assert not review.errors and not review.applied
    candidate = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert candidate["review_required"]
    assert candidate["evidence"]["registered_cache"]["status"] == "missing"
    assert candidate["evidence"]["observations"]["before"] is None
    assert len(candidate["evidence"]["observations"]["after"]["market_shares"]) == 14
    assert_preserved(bundle, original, caches)
    blocked = updater.run_update(bundle, sources={TC}, session=session, apply=True)
    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)
    adopted = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                 review_report=review.report_path)
    assert adopted.applied and not adopted.errors
    assert not target.exists()


def test_tc_review_required_blocks_other_valid_sources_in_the_batch(isolated_bundle):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    session = rating_session(bundle)
    session.downloads.update(dashboard_session(bundle, dashboard_html(bundle, ytd_share="2%")).downloads)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    blocked = updater.run_update(bundle, sources={TC, NRCAN}, session=session, apply=True)

    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)


def test_tc_review_report_is_only_accepted_for_explicit_apply(isolated_bundle):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    session = dashboard_session(bundle, dashboard_html(bundle, ytd_share="2%"))
    review = updater.run_update(bundle, sources={TC}, session=session)
    assert not review.errors
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    rejected = updater.run_update(bundle, sources={TC}, session=session,
                                  review_report=review.report_path)

    assert rejected.errors and not rejected.applied
    assert "--apply" in " ".join(rejected.errors)
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("changed", ["candidate_bytes", "registry", "rules", "scenario", "selection",
                                    "failed_report", "tampered_evidence", "missing_report"])
def test_tc_review_rejects_changed_context_and_untrusted_report(isolated_bundle, changed):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle, ytd_share="2%")
    session = dashboard_session(bundle, content)
    review = updater.run_update(bundle, sources={TC}, session=session)
    assert not review.errors
    report_path = review.report_path
    selection = {TC}
    if changed == "candidate_bytes":
        session = dashboard_session(bundle, content + b"<!-- changed after review -->")
    elif changed == "registry":
        with bundle.sources_path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Registry edited after review.\n")
        bundle = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    elif changed in {"rules", "scenario"}:
        path = bundle.repo_root / ("config/parameters/rules.yaml" if changed == "rules" else SCENARIO)
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Configuration edited after review.\n")
        bundle = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    elif changed == "selection":
        selection.add(NRCAN)
        ratings = rating_session(bundle)
        ratings.downloads.update(session.downloads)
        session = ratings
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


def test_tc_reviewed_charger_reporting_date_updates_metadata_and_offline_parser(isolated_bundle, monkeypatch):
    from fetching import assorted_sources as adapter

    bundle, target, old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle, columns=("Q1", "Q2", "YTD"), reporting_date="June 2026")
    session = dashboard_session(bundle, content)
    review = updater.run_update(bundle, sources={TC}, session=session)
    assert not review.errors
    candidate = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert candidate["review_required"]

    adopted = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                 review_report=review.report_path)

    assert adopted.applied and not adopted.errors
    assert target.read_bytes() == old
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    source = current.sources.sources[TC]
    chargers = source.component("public_light_duty_chargers")
    assert chargers.adapter["reporting_date"] == "June 2026"
    assert "June 2026" in chargers.label
    assert "June 2026" in chargers.database_note
    assert "June 2026" in source.version
    monkeypatch.setattr(adapter, "load_config_bundle", lambda _path: current)
    no_network = FakeSession()
    output = adapter.fetch_and_normalize_tc_dashboard(SCENARIO, download=False, session=no_network)
    assert no_network.calls == []
    rules = adapter.module_rules(current)["tc_ev_dashboard"]["public_chargers"]
    for key in ("province_output_file", "type_output_file"):
        with (output / rules[key]).open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows and all(row["as_of"] == "June 2026" for row in rows)


@pytest.mark.parametrize("configuration", ["rules", "scenario"])
def test_tc_configuration_edit_during_download_blocks_adoption(isolated_bundle, configuration):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle) + b"<!-- harmless footer update -->"
    session = dashboard_session(bundle, content)
    original_get = session.get
    path = bundle.repo_root / ("config/parameters/rules.yaml" if configuration == "rules" else SCENARIO)

    def edit_configuration_while_downloading(url, **kwargs):
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Edited while the source candidate was downloading.\n")
        return original_get(url, **kwargs)

    session.get = edit_configuration_while_downloading
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    result = updater.run_update(bundle, sources={TC}, session=session, apply=True)

    assert result.errors and not result.applied
    assert "chang" in " ".join(result.errors).lower()
    assert_preserved(bundle, original, caches)


def test_tc_change_to_additional_quarter_observations_requires_new_review(isolated_bundle):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    columns = ("Q1", "Q2", "YTD")
    first_content = dashboard_html(bundle, columns=columns, quarter_values={"Q2": "9%"})
    session = dashboard_session(bundle, first_content)
    review = updater.run_update(bundle, sources={TC}, session=session)
    assert not review.errors
    adopted = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                 review_report=review.report_path)
    assert adopted.applied and not adopted.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    next_content = dashboard_html(current, columns=columns, quarter_values={"Q2": "8%"})
    session = dashboard_session(current, next_content)
    original, caches = current.sources_path.read_bytes(), cache_bytes(current)

    changed = updater.run_update(current, sources={TC}, session=session)

    assert not changed.errors and not changed.applied
    candidate = json.loads(changed.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert candidate["review_required"]
    evidence = candidate["evidence"]
    assert evidence["observations"]["before"] == evidence["observations"]["after"]
    assert evidence["observation_changes"] == []
    assert evidence["published_observations"]["before"] != evidence["published_observations"]["after"]
    assert_preserved(current, original, caches)
    blocked = updater.run_update(current, sources={TC}, session=session, apply=True)
    assert blocked.errors and not blocked.applied
    assert_preserved(current, original, caches)


def test_tc_rejected_charger_totals_retain_market_and_reconciliation_evidence(isolated_bundle):
    bundle, _target, _old = pin_dashboard(isolated_bundle)
    content = dashboard_html(bundle, columns=("Q1", "Q2", "YTD"), ytd_share="2%")
    content = content.replace(b"Total public chargers: 1</td>",
                              b"Total public chargers: 2</td>", 1)
    session = dashboard_session(bundle, content)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)

    rejected = updater.run_update(bundle, sources={TC}, session=session)

    assert rejected.errors and not rejected.applied
    report = json.loads(rejected.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    source = next(row for row in report["sources"] if row["source"] == TC)
    assert source["status"] == "error"
    evidence = source["evidence"]
    assert evidence["sha256"] == digest(content)
    validation_error = evidence["validation_error"]
    assert {key: validation_error[key] for key in ("national_total", "provincial_total", "difference")} == {
        "national_total": 13, "provincial_total": 14, "difference": 1,
    }
    assert validation_error["reporting_date"] == "March 2026"
    assert len(validation_error["provincial_records"]) == 13
    assert sum(row["public_chargers"] for row in validation_error["provincial_records"]) == 14
    assert {row["charger_type"] for row in validation_error["charger_type_records"]} == {"L2", "DCFC"}
    market = evidence["market_shares"]
    assert market["publication_selection"]["selected_headers"]["quarter"] == "Market share (Q1 2026)"
    assert len(market["observations"]) == 14
    assert all(row["ytd_market_share"] == 0.02 for row in market["observations"])
    assert market["published_observations"]["headers"] == [
        "Province/territory", "Market share (Q1 2026)", "Market share (Q2 2026)", "2026 year-to-date",
    ]
    assert len(market["published_observations"]["rows"]) == 14
    assert_preserved(bundle, original, caches)

    attempted = updater.run_update(bundle, sources={TC}, session=session, apply=True,
                                   review_report=rejected.report_path)

    assert attempted.errors and not attempted.applied
    assert_preserved(bundle, original, caches)
