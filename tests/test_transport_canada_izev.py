"""Pinned administrative-data replay and updater checks on isolated artifacts."""

import csv
import hashlib
import io
import json
import shutil
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml
from pydantic import ValidationError

from fetching import source_updater as updater
from fetching import transport_canada_izev as izev
from utils import load_config_bundle, resolve_artifact_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


def native_row(columns, **overrides):
    row = dict.fromkeys(columns, "native")
    row.update({
        "Incentive Request Date": "2022-03-14", "Month and Year": "March 2022",
        "Government of Canada Fiscal Year (FY)": "2021-22", "Calendar Year": "2022",
        "Dealership Province/Territory": "Quebec", "Dealership Postal Code": "A1B 2C3",
        "Purchase or Lease": "Lease", "Vehicle Year": "2021", "Vehicle Make": "Tesla",
        "Vehicle Model": "Model 3", "Vehicle Make and Model": "Tesla Model 3",
        izev.POWERTRAIN_COLUMN: "BEV", "Recipient Province/Territory": "Ontario",
        "Eligible Incentive Amount (in dollars)": "5,000", "Country": "Canada",
    })
    row.update(overrides)
    return row


def csv_bytes(columns, rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8-sig")


def md5(content):
    return hashlib.md5(content).hexdigest()


@pytest.fixture
def bundle(tmp_path):
    shutil.copytree(REPO_ROOT / "config", tmp_path / "config")
    original = load_config_bundle(SCENARIO, repo_root=tmp_path)
    registry = yaml.safe_load(original.sources_path.read_text(encoding="utf-8"))
    source = registry["sources"][izev.SOURCE]
    source["adapter"]["expected_provinces"] = ["ON"]
    source["adapter"]["expected_powertrains"] = ["bev"]
    component = source["components"]["fy2019_2022"]
    source["components"] = {"fy2019_2022": component}
    columns = source["adapter"]["required_columns"]
    row = native_row(columns)
    content = csv_bytes(columns, [row, dict.fromkeys(columns, ""), row])
    component["adapter"].update(
        expected_model_years=[2021], expected_request_date_from="2022-03-14",
        expected_request_date_to="2022-03-14", expected_bytes=len(content), expected_md5=md5(content),
    )
    original.sources_path.write_text(yaml.safe_dump(registry, sort_keys=False, allow_unicode=True), encoding="utf-8")
    rules_path = tmp_path / "config/parameters/rules.yaml"
    rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    owner = next(section for section in ("fetching", "parameterization") if izev.SOURCE in rules[section])
    rules[owner][izev.SOURCE]["province_map"]["Ontario"] = "ON"
    rules_path.write_text(yaml.safe_dump(rules, sort_keys=False, allow_unicode=True), encoding="utf-8")
    result = load_config_bundle(SCENARIO, repo_root=tmp_path)
    request = izev.build_requests(result)[0]
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    request.cache_path.write_bytes(content)
    return result


def test_transactions_are_not_deduplicated_and_model_year_is_not_request_year(bundle):
    request = izev.build_requests(bundle)[0]
    izev.validate_cache(request)
    frame = izev.parse_csv(request, rules=izev.module_rules(bundle))
    assert frame.source_row.tolist() == [2, 4]
    assert frame["count"].sum() == 2
    assert set(frame.model_year) == {2021}
    assert set(frame.request_year) == {2022}
    assert set(frame.request_month) == {3}
    assert set(frame.province) == {"ON"}
    assert set(frame["Dealership Province/Territory"]) == {"Quebec"}
    assert set(frame["Purchase or Lease"]) == {"Lease"}
    assert frame.attrs["audit"]["blank_rows"] == 1
    assert frame.attrs["audit"]["identical_native_rows_retained"] == 1
    assert frame.attrs["audit"]["transaction_types"] == {"Lease": 2}


@pytest.mark.parametrize("overrides,match", [
    ({"Purchase or Lease": "Other"}, "purchase/lease categories"),
    ({"Vehicle Year": "2021.5"}, "vehicle model years"),
    ({"Vehicle Year": ""}, "incomplete nonblank"),
    ({"Incentive Request Date": "2022-02-30"}, "invalid incentive request dates"),
    ({"Calendar Year": "2021"}, "disagrees"),
    ({"Recipient Province/Territory": "Other"}, "unmapped recipient"),
    ({izev.POWERTRAIN_COLUMN: "HEV"}, "unknown propulsion"),
    ({"Vehicle Year": "2020"}, "coverage differs"),
])
def test_physical_records_and_registered_coverage_fail_closed(bundle, overrides, match):
    request = izev.build_requests(bundle)[0]
    request.cache_path.write_bytes(csv_bytes(request.required_columns, [native_row(request.required_columns, **overrides)]))
    with pytest.raises(ValueError, match=match):
        izev.parse_csv(request, rules=izev.module_rules(bundle))


def test_missing_columns_and_unquoted_boolean_province_code_are_rejected(bundle):
    request = izev.build_requests(bundle)[0]
    rules = izev.module_rules(bundle)
    rules["province_map"]["Ontario"] = True
    with pytest.raises(ValueError, match="postal province codes"):
        izev.parse_csv(request, rules=rules)
    request.cache_path.write_text("Incentive Request Date\n2022-03-14\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required columns"):
        izev.parse_csv(request, rules=izev.module_rules(bundle))


@pytest.mark.parametrize("updates,match", [
    ({"expected_model_years": [2022, 2021]}, "sorted and unique"),
    ({"expected_request_date_to": date(2021, 1, 1)}, "reversed"),
    ({"expected_md5": "x"}, "String should match"),
    ({"cache_path": Path("relative.csv")}, "absolute CSV"),
    ({"url": "https://other.example/source.csv"}, "official HTTPS CSV"),
])
def test_adapter_contract_is_validated_before_io(bundle, updates, match):
    request = izev.build_requests(bundle)[0]
    with pytest.raises(ValidationError, match=match):
        izev.IzevRequest.model_validate({**request.model_dump(), **updates})


def test_offline_replay_preserves_source_and_has_no_network_access(bundle, monkeypatch):
    request = izev.build_requests(bundle)[0]
    original = request.cache_path.read_bytes()
    monkeypatch.setattr(izev.requests, "get", lambda *_args, **_kwargs: pytest.fail("offline network access"))
    stats = izev.fetch_izev(bundle, download=False)
    output = resolve_artifact_path(bundle, izev.SOURCE)
    rules = izev.module_rules(bundle)
    normalized = pd.read_csv(output / rules["output_file"])
    manifest = json.loads((output / rules["manifest_file"]).read_text(encoding="utf-8"))
    assert stats["rows"] == len(normalized) == manifest["rows"] == 2
    assert manifest["components"][0]["sha256"] == hashlib.sha256(original).hexdigest()
    assert request.cache_path.read_bytes() == original
    request.cache_path.unlink()
    with pytest.raises(FileNotFoundError, match="registered iZEV cache"):
        izev.fetch_izev(bundle, download=False)
    assert pd.read_csv(output / rules["output_file"]).equals(normalized)


def test_existing_corrupted_cache_is_not_overwritten(bundle, monkeypatch):
    request = izev.build_requests(bundle)[0]
    request.cache_path.write_bytes(b"wrong")
    monkeypatch.setattr(izev.requests, "get", lambda *_args, **_kwargs: pytest.fail("must not redownload a registered path"))
    with pytest.raises(ValueError, match="byte count"):
        izev.fetch_to_cache(request)
    assert request.cache_path.read_bytes() == b"wrong"


def test_duplicate_native_headers_are_rejected_before_pandas_can_rename_them(bundle):
    request = izev.build_requests(bundle)[0]
    columns = [*request.required_columns, "Vehicle Year"]
    request.cache_path.write_bytes(csv_bytes(columns, [native_row(columns)]))
    with pytest.raises(ValueError, match="duplicate physical column"):
        izev.parse_csv(request, rules=izev.module_rules(bundle))


def test_wrong_publisher_download_never_creates_a_registered_cache(bundle, monkeypatch):
    request = izev.build_requests(bundle)[0]
    request.cache_path.unlink()
    monkeypatch.setattr(izev.requests, "get", lambda *_args, **_kwargs: FakeResponse(request.url, content=b"corrupt"))
    with pytest.raises(ValueError, match="differs from the pinned bytes"):
        izev.fetch_to_cache(request)
    assert not request.cache_path.exists()
    assert list(request.cache_path.parent.glob("*.tmp")) == []


class FakeResponse:
    def __init__(self, url, *, payload=None, content=b""):
        self.url, self.payload, self.content = url, payload, content

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload

    def iter_content(self, chunk_size):
        yield self.content

    def close(self):
        return None


class FakeSession:
    def __init__(self, request, content):
        self.request, self.content = request, content
        self.calls = []

    def get(self, url, *, params=None, **_kwargs):
        self.calls.append((url, params))
        if params:
            assert params["id"] == str(self.request.resource_id)
            return FakeResponse(url, payload={"success": True, "result": {
                "id": str(self.request.resource_id), "url": self.request.url,
                "hash": md5(self.content), "size": len(self.content),
                "last_modified": self.request.component_meta.version + "T12:00:00",
            }})
        assert url == self.request.url
        return FakeResponse(url, content=self.content)


def test_updater_report_apply_and_replay_preserve_registered_snapshot(bundle):
    request = izev.build_requests(bundle)[0]
    original = request.cache_path.read_bytes()
    columns = request.required_columns
    replacement = csv_bytes(columns, [native_row(columns), native_row(columns, **{"Vehicle Model": "Model Y"})])
    session = FakeSession(request, replacement)
    registry = bundle.sources_path.read_bytes()
    report = updater.run_update(bundle, sources={izev.SOURCE}, session=session)
    assert not report.errors and not report.applied
    assert bundle.sources_path.read_bytes() == registry
    assert request.cache_path.read_bytes() == original
    result = updater.run_update(bundle, sources={izev.SOURCE}, session=session, apply=True)
    assert not result.errors and result.applied
    updated = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    current = izev.build_requests(updated)[0]
    assert current.cache_path != request.cache_path
    assert request.cache_path.read_bytes() == original
    izev.validate_cache(current)
    assert set(izev.parse_csv(current, rules=izev.module_rules(updated)).model) == {"Model 3", "Model Y"}
    repeated = updater.run_update(updated, sources={izev.SOURCE}, session=session, apply=True)
    assert not repeated.errors and not repeated.applied


@pytest.mark.parametrize("overrides,coverage", [
    ({"Vehicle Year": "2022"}, "model_years"),
    ({"Recipient Province/Territory": "Quebec"}, "provinces"),
    ({izev.POWERTRAIN_COLUMN: "PHEV"}, "powertrains"),
])
def test_updater_rejects_disappeared_coverage_without_any_adoption(bundle, overrides, coverage):
    request = izev.build_requests(bundle)[0]
    registry, original = bundle.sources_path.read_bytes(), request.cache_path.read_bytes()
    replacement = csv_bytes(request.required_columns, [native_row(request.required_columns, **overrides)])
    result = updater.run_update(bundle, sources={izev.SOURCE}, session=FakeSession(request, replacement), apply=True)
    assert result.errors and not result.applied
    assert f"lost registered {coverage}" in result.errors[0]
    assert bundle.sources_path.read_bytes() == registry
    assert request.cache_path.read_bytes() == original


def test_updater_expands_existing_resource_year_coverage_explicitly(bundle):
    request = izev.build_requests(bundle)[0]
    replacement = csv_bytes(request.required_columns, [native_row(request.required_columns), native_row(request.required_columns, **{"Vehicle Year": "2022"})])
    result = updater.run_update(bundle, sources={izev.SOURCE}, session=FakeSession(request, replacement), apply=True)
    assert result.applied and not result.errors
    updated = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    current = izev.build_requests(updated)[0]
    assert current.expected_model_years == (2021, 2022)
    assert len(izev.parse_csv(current, rules=izev.module_rules(updated))) == 2


def test_updater_rejects_overlapping_revised_resources_before_publication(bundle, monkeypatch, tmp_path):
    first = izev.build_requests(bundle)[0]
    second = first.model_copy(update={
        "component_key": "second", "expected_request_date_from": date(2022, 3, 15),
        "expected_request_date_to": date(2022, 3, 15),
    })
    first_bytes = csv_bytes(first.required_columns, [
        native_row(first.required_columns),
        native_row(first.required_columns, **{"Incentive Request Date": "2022-03-15"}),
    ])
    second_bytes = csv_bytes(first.required_columns, [native_row(first.required_columns, **{"Incentive Request Date": "2022-03-15"})])
    contents = iter([first_bytes, second_bytes])
    monkeypatch.setattr(izev, "build_requests", lambda _bundle: [first, second])
    # Each metadata response is rechecked once after parsing its candidate.
    resources = iter([*([updater.CatalogueResource(
        id=str(first.resource_id), url=first.url, hash=md5(first_bytes),
        size=len(first_bytes), last_modified=first.component_meta.version,
    )] * 2), *([updater.CatalogueResource(
        id=str(second.resource_id), url=second.url, hash=md5(second_bytes),
        size=len(second_bytes), last_modified=second.component_meta.version,
    )] * 2)])
    monkeypatch.setattr(updater, "_resource", lambda *_args, **_kwargs: next(resources))
    monkeypatch.setattr(updater, "_verified_bytes", lambda *_args, **_kwargs: next(contents))
    with pytest.raises(updater.UpdateError, match="overlapping request-date coverage"):
        updater._tc_izev(bundle, None, bundle.sources.updater, tmp_path)

