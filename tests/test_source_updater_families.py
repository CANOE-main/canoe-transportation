"""Real source-family parsers behind isolated, deterministic update lifecycles."""

from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import shutil
from pathlib import Path
from zipfile import ZipFile

import pytest
import yaml

from fetching import cer_enerfuture as cer
from fetching import source_updater as updater
from fetching import statcan_tables as statcan
from fetching import vehicle_population as ontario
from utils import load_config_bundle, resolve_input_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


def digest(content):
    return hashlib.sha256(content).hexdigest()


def csv_bytes(rows, columns, *, delimiter=","):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", delimiter=delimiter)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def archive_bytes(members):
    stream = io.BytesIO()
    with ZipFile(stream, "w") as zipped:
        for name, content in members.items():
            zipped.writestr(name, content)
    return stream.getvalue()


def cache_bytes(bundle):
    cache = resolve_input_path(bundle, "cache")
    return {p.relative_to(cache): p.read_bytes() for p in cache.rglob("*") if p.is_file()}


class FakeResponse:
    def __init__(self, url, *, payload=None, content=b""):
        self.url = url
        self.payload = copy.deepcopy(payload)
        self.content = json.dumps(payload).encode("utf-8") if payload is not None else content
        self.headers = {}

    def raise_for_status(self):
        return None

    def json(self):
        return copy.deepcopy(self.payload)

    def iter_content(self, chunk_size=65536):
        yield self.content

    def close(self):
        return None


class FakeSession:
    def __init__(self):
        self.packages = {}
        self.metadata = {}
        self.resolved = {}
        self.downloads = {}
        self.calls = []
        self.racing_product = None
        self.metadata_calls = {}
        self.racing_package = None
        self.package_calls = {}

    def get(self, url, *, timeout, params=None, **_kwargs):
        self.calls.append(("GET", url, params))
        if params is not None:
            package_id = params["id"]
            self.package_calls[package_id] = self.package_calls.get(package_id, 0) + 1
            package = copy.deepcopy(self.packages[package_id])
            if package_id == self.racing_package and self.package_calls[package_id] > 1:
                package["resources"][0]["metadata_modified"] = "2026-10-27T08:30:00"
            return FakeResponse(url, payload={"success": True, "result": package})
        if url in self.resolved:
            return FakeResponse(url, payload={"status": "SUCCESS", "object": self.resolved[url]})
        if url not in self.downloads:
            raise AssertionError(f"Unexpected source request: {url}")
        return FakeResponse(url, content=self.downloads[url])

    def post(self, url, *, json, timeout, **_kwargs):
        product = json[0]["productId"]
        self.calls.append(("POST", url, product))
        self.metadata_calls[product] = self.metadata_calls.get(product, 0) + 1
        payload = copy.deepcopy(self.metadata[product])
        if product == self.racing_product and self.metadata_calls[product] > 1:
            payload[0]["object"]["releaseTime"] = "2026-10-27T08:30:00"
        return FakeResponse(url, payload=payload)


@pytest.fixture
def bundle(tmp_path):
    shutil.copytree(REPO_ROOT / "config", tmp_path / "config")
    return load_config_bundle(SCENARIO, repo_root=tmp_path)


def save_registry(bundle, registry):
    bundle.sources_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8", newline="\n")
    return load_config_bundle(SCENARIO, repo_root=bundle.repo_root)


def assert_preserved(bundle, registry_bytes, caches):
    assert bundle.sources_path.read_bytes() == registry_bytes
    assert cache_bytes(bundle) == caches


def configure_statcan(bundle, session):
    for request in statcan.build_requests(bundle):
        table = request.table_rules
        metadata = {
            "productId": request.product_id,
            "cubeTitleEn": request.title,
            "releaseTime": "2026-10-26T08:30:00",
            "cubeStartDate": "2020-01-01",
            "cubeEndDate": "2025-01-01",
            "frequencyCode": 4,
            "dimension": [{
                "dimensionNameEn": name,
                "member": [{"memberNameEn": member} for member in table.get("selectors", {}).get(name, ["Ontario"])],
            } for name in table["metadata_dimensions"]],
        }
        session.metadata[request.product_id] = [{"status": "SUCCESS", "object": metadata}]
        rows = []
        for period in (2020, 2025):
            row = {column: "fixture" for column in table["required_columns"]}
            row.update(REF_DATE=period, GEO="Ontario", UOM="Number", SCALAR_FACTOR="units", VALUE=2)
            for column, members in table.get("selectors", {}).items():
                row[column] = members[0]
            if request.normalizer == "freight":
                row["Geography, destination of shipments"] = "Ontario"
            rows.append(row)
        content = archive_bytes({f"{request.product_id}.csv": csv_bytes(rows, table["required_columns"])})
        url = f"https://www150.statcan.gc.ca/n1/tbl/csv/{request.product_id}-eng.zip"
        session.resolved[request.download_api_url] = url
        session.downloads[url] = content
        request.metadata_cache_path.parent.mkdir(parents=True, exist_ok=True)
        previous = copy.deepcopy(session.metadata[request.product_id])
        previous[0]["object"]["releaseTime"] = "2026-10-01T08:30:00"
        request.metadata_cache_path.write_text(json.dumps(previous), encoding="utf-8")
        request.archive_cache_path.write_bytes(b"preserved old table archive")


def test_statcan_real_handler_apply_replays_coherent_pair_and_is_idempotent(bundle):
    session = FakeSession()
    configure_statcan(bundle, session)
    old = cache_bytes(bundle)
    result = updater.run_update(bundle, sources={statcan.SOURCE_ID}, session=session, apply=True)
    assert result.applied and not result.errors
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    for request in statcan.build_requests(adopted):
        assert request.snapshot is not None
        assert statcan.ensure_cached_artifacts(request, download=False) == ("cached", "cached", "")
        assert statcan.read_cached_metadata(request)["releaseTime"] == request.snapshot.release_time
        assert request.archive_cache_path.read_bytes() == session.downloads[session.resolved[request.download_api_url]]
    for path, content in old.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == content
    repeated = updater.run_update(adopted, sources={statcan.SOURCE_ID}, session=session, apply=True)
    assert not repeated.applied and not repeated.errors
    assert repeated.diff_path.read_text(encoding="utf-8") == ""


def test_statcan_release_race_blocks_the_entire_batch(bundle):
    session = FakeSession()
    configure_statcan(bundle, session)
    session.racing_product = 20100025
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={statcan.SOURCE_ID}, session=session, apply=True)
    assert not result.applied
    assert any("release changed during download" in error for error in result.errors)
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("mismatch", ["period", "product"])
def test_statcan_metadata_archive_mismatch_is_rejected_without_adoption(bundle, mismatch):
    session = FakeSession()
    configure_statcan(bundle, session)
    request = statcan.build_requests(bundle)[0]
    url = session.resolved[request.download_api_url]
    with ZipFile(io.BytesIO(session.downloads[url])) as archive:
        content = archive.read(f"{request.product_id}.csv")
    if mismatch == "period":
        content = content.replace(b"2020,", b"1990,").replace(b"2025,", b"1995,")
        member = f"{request.product_id}.csv"
    else:
        member = "99999999.csv"
    session.downloads[url] = archive_bytes({member: content})
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={statcan.SOURCE_ID}, session=session, apply=True)
    assert not result.applied and result.errors
    assert_preserved(bundle, original, caches)


def test_statcan_metadata_period_expansion_requires_review(bundle):
    session = FakeSession()
    configure_statcan(bundle, session)
    session.metadata[20100025][0]["object"]["cubeEndDate"] = "2026-01-01"
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={statcan.SOURCE_ID}, session=session, apply=True)
    assert not result.applied and any("cubeEndDate changed" in error for error in result.errors)
    assert_preserved(bundle, original, caches)


def cer_csv(bundle, request, *, value=2):
    rules = cer.module_rules(bundle)["components"][request.component_id]
    rows = []
    if request.component_id == "macro-indicators":
        variables = [text + "fixture)" if text.endswith("(") else text for text in rules["variables"].values()]
        region = "Canada"
    else:
        variables = rules.get("variables", ["Gasoline"])
        region = "Ontario"
    for scenario in request.expected_scenarios:
        for year in (request.first_year, request.last_year):
            for variable in variables:
                row = {"Scenario": scenario, "Region": region, "Variable": variable, "Year": year, "Value": value}
                if "Sector" in request.required_columns:
                    row["Sector"] = rules["sector"]
                rows.append(row)
    return csv_bytes(rows, request.required_columns)


def configure_cer(bundle, session):
    requests = cer.build_requests(bundle)
    resources = []
    for request in requests:
        content = cer_csv(bundle, request)
        session.downloads[request.url] = content
        resources.append({"id": request.component_id, "url": request.url, "format": "CSV", "size": len(content), "hash": ""})
        request.cache_path.parent.mkdir(parents=True, exist_ok=True)
        request.cache_path.write_bytes(cer_csv(bundle, request, value=1))
    package_id = requests[0].open_government_record_id
    session.packages[package_id] = {"id": package_id, "resources": resources}


def test_cer_real_handler_stays_in_selected_edition_and_roundtrips_snapshots(bundle):
    session = FakeSession()
    configure_cer(bundle, session)
    package = next(iter(session.packages.values()))
    package["resources"].append({"id": "future-edition", "url": "https://www.cer-rec.gc.ca/open/energy/energyfutures2027/macro-indicators-2027.csv", "format": "CSV"})
    old = cache_bytes(bundle)
    result = updater.run_update(bundle, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert result.applied and not result.errors
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    snapshots = adopted.sources.sources[cer.SOURCE_ID].adapter["registered_snapshots"]
    assert set(snapshots) == {2026}
    for request in cer.build_requests(adopted):
        assert request.edition == 2026
        assert request.expected_sha256 == digest(session.downloads[request.url])
        assert not cer.read_and_validate_source(request).empty
    for path, content in old.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == content
    repeated = updater.run_update(adopted, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert not repeated.applied and not repeated.errors
    assert repeated.diff_path.read_text(encoding="utf-8") == ""
    assert not any("2027" in url for _, url, _ in session.calls)


def test_cer_duplicate_matching_resources_require_manual_review_and_preserve_old_files(bundle):
    session = FakeSession()
    configure_cer(bundle, session)
    resources = next(iter(session.packages.values()))["resources"]
    resources.append({**resources[0], "id": "duplicate-resource"})
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert not result.applied and any("missing or ambiguous" in error for error in result.errors)
    assert_preserved(bundle, original, caches)


def test_cer_can_adopt_two_changed_releases_and_replay_both_snapshots(bundle):
    session = FakeSession()
    configure_cer(bundle, session)
    first = updater.run_update(bundle, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert first.applied and not first.errors
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    first_snapshot = cache_bytes(adopted)
    for request in cer.build_requests(adopted):
        content = cer_csv(adopted, request, value=3)
        session.downloads[request.url] = content
        package = session.packages[request.open_government_record_id]
        for resource in package["resources"]:
            if resource["url"] == request.url:
                resource["size"] = len(content)
    second = updater.run_update(adopted, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert second.applied and not second.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    for request in cer.build_requests(current):
        assert set(cer.read_and_validate_source(request)["Value"]) == {3}
    for path, content in first_snapshot.items():
        assert (resolve_input_path(current, "cache") / path).read_bytes() == content


def test_cer_catalogue_change_during_download_blocks_adoption(bundle):
    session = FakeSession()
    configure_cer(bundle, session)
    session.racing_package = next(iter(session.packages))
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={cer.SOURCE_ID}, session=session, apply=True)
    assert not result.applied and any("catalogue changed during download" in error for error in result.errors)
    assert_preserved(bundle, original, caches)


def ontario_zip(request, *, valid=True):
    primary = (
        "VEHICLE-CLASS\tMAKE\tMODEL\tMODEL-YEAR\tFIT-ACTIVE\tFIT-INACTIVE\tUNFIT\tWRECKED\tOUT-OF-PROV\tSOLD\tSUSPENDED\tTEMPORARY\tTOTAL\n"
        f"PASSENGER\tFORD\tF15\t{request.year - 1}\t10\t2\t0\t0\t0\t0\t0\t0\t{12 if valid else 99}\n"
    )
    return archive_bytes({
        request.report_a_members[0]: primary,
        request.report4_member: "WEIGHT_CLASS\tKG_FROM\tKG_TO\tFIT-ACTIVE\nCOMMERCIAL\t2001\t3000\t10\n",
        request.report5_member: f"VEHICLE_CLASS\tDESCRIPTOR\tVALUE\tFIT-ACTIVE\nPASSENGER\tYEAR\t{request.year - 1}\t10\n",
    })


def configure_ontario(bundle, session):
    source = bundle.sources.sources[ontario.SOURCE_ID]
    package_id = source.adapter["access"]["package_id"]
    resources = []
    for request in ontario.registered_requests(bundle, year=None):
        content = ontario_zip(request)
        session.downloads[request.url] = content
        resources.append({"id": request.resource_id, "url": request.url, "name": request.resource_name, "format": "ZIP", "size": len(content), "last_modified": "2026-10-26T08:30:00"})
        request.cache_path.parent.mkdir(parents=True, exist_ok=True)
        request.cache_path.write_bytes(b"preserved historical cache")
    for year in (2015, 2026):
        resources.append({"id": f"new-{year}", "url": f"https://data.ontario.ca/annual/{year}.zip", "name": f"Vehicle population data - {year}", "format": "ZIP", "last_modified": "2026-10-26T08:30:00"})
    session.packages[package_id] = {"name": package_id, "resources": resources}


@pytest.mark.parametrize("source,configure", [
    (statcan.SOURCE_ID, configure_statcan),
    (cer.SOURCE_ID, configure_cer),
    (ontario.SOURCE_ID, configure_ontario),
])
def test_real_family_report_mode_preserves_registry_and_every_cache(bundle, source, configure):
    session = FakeSession()
    configure(bundle, session)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={source}, session=session)
    assert not result.applied and not result.errors
    assert result.diff_path.read_text(encoding="utf-8")
    assert_preserved(bundle, original, caches)


def test_ontario_real_handler_preserves_history_and_reports_new_year_without_enrollment(bundle):
    session = FakeSession()
    configure_ontario(bundle, session)
    old = cache_bytes(bundle)
    original_years = [r.year for r in ontario.registered_requests(bundle, year=None)]
    result = updater.run_update(bundle, sources={ontario.SOURCE_ID}, session=session, apply=True)
    assert result.applied and not result.errors
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    requests = ontario.registered_requests(adopted, year=None)
    assert [r.year for r in requests] == original_years
    assert 2015 not in original_years and 2026 not in original_years
    for request in requests:
        assert request.expected_sha256 == digest(session.downloads[request.url])
        ontario.validate_source(request, require_auxiliary=request.year == max(original_years))
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    for candidate in report["candidates"]:
        assert candidate["evidence"]["new_years_for_review"] == [2026]
        assert candidate["evidence"]["excluded_years"] == [2015]
    for path, content in old.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == content
    repeated = updater.run_update(adopted, sources={ontario.SOURCE_ID}, session=session, apply=True)
    assert not repeated.applied and not repeated.errors
    assert repeated.diff_path.read_text(encoding="utf-8") == ""


def test_invalid_ontario_archive_blocks_other_valid_source_updates_in_batch(bundle):
    session = FakeSession()
    configure_ontario(bundle, session)
    configure_cer(bundle, session)
    request = ontario.registered_requests(bundle, year=None)[0]
    content = ontario_zip(request, valid=False)
    session.downloads[request.url] = content
    resources = session.packages[bundle.sources.sources[ontario.SOURCE_ID].adapter["access"]["package_id"]]["resources"]
    next(item for item in resources if item["id"] == request.resource_id)["size"] = len(content)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    result = updater.run_update(bundle, sources={cer.SOURCE_ID, ontario.SOURCE_ID}, session=session, apply=True)
    assert not result.applied and result.errors
    assert_preserved(bundle, original, caches)
