"""Reviewed rolling-source extensions and auditable FuelEconomy data changes."""

from __future__ import annotations

import csv
import io
import json
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pytest
import yaml

from fetching import fueleconomy_vehicles as fuel
from fetching import nrcan_ceud as nrcan
from fetching import source_updater as updater
from test_source_updater import (
    FUEL,
    NRCAN,
    SCENARIO,
    FakeSession,
    assert_preserved,
    cache_bytes,
    digest,
    isolated_bundle as isolated_bundle,
    rating_csv,
    rating_session,
    save_registry,
)
from utils import load_config_bundle, resolve_input_path


@pytest.mark.parametrize("extend", [True, False])
def test_nrcan_coverage_extension_is_reviewed_and_loss_is_rejected(isolated_bundle, extend):
    bundle = isolated_bundle
    request = nrcan.iter_rating_requests(bundle)[0]
    session = rating_session(bundle, changed=False)
    years = [*request.expected_model_years, max(request.expected_model_years) + 1] if extend else request.expected_model_years[:-1]
    content = rating_csv(request, make="Original", years=years)
    session.resources[request.resource_id].update(hash=digest(content, "md5"), size=len(content))
    session.downloads[request.url] = (content, {})
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    blocked = updater.run_update(bundle, sources={NRCAN}, session=session, apply=True)
    assert blocked.errors and not blocked.applied
    assert_preserved(bundle, original, caches)
    if not extend:
        assert "lost registered model years" in " ".join(blocked.errors)
        return
    assert "review-report" in " ".join(blocked.errors)
    review = updater.run_update(bundle, sources={NRCAN}, session=session)
    assert not review.errors
    candidate = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert candidate["evidence"]["model_years"] == {"before": list(request.expected_model_years), "after": years}
    adopted = updater.run_update(bundle, sources={NRCAN}, session=session,
                                 apply=True, review_report=review.report_path)
    assert adopted.applied and not adopted.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    updated = nrcan.iter_rating_requests(current)[0]
    nrcan.validate_rating_cache(updated)
    raw, _ = nrcan.read_rating_csv(updated, encoding_candidates=nrcan.ratings_rules(current)["encoding_candidates"])
    assert sorted(nrcan.normalize_rating_dataframe(raw, updated)["Model year"].unique()) == years
    assert current.scenario.sources == bundle.scenario.sources
    for path, old in caches.items():
        assert (resolve_input_path(bundle, "cache") / path).read_bytes() == old


def vehicle_zip(*, years=(1984, 2024, 2027), electric_range=200, comment=b"", reverse=False):
    columns = ["year", "make", "model", "VClass", "id", "mfrCode", "engId", "atvType",
               "fuelType1", "fuelType2", "range", "rangeA", "phevBlended"]
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    rows = [dict(year=year, make="Example", model="Car", VClass="Compact Cars", id=year,
                 mfrCode="EX", engId=year, atvType="EV", fuelType1="Electricity",
                 fuelType2="", range=electric_range, rangeA=0, phevBlended=False) for year in years]
    writer.writerows(reversed(rows) if reverse else rows)
    output = io.BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        member = ZipInfo("vehicles.csv", date_time=(2026, 1, 1, 0, 0, 0))
        archive.writestr(member, stream.getvalue())
        archive.comment = comment
    return output.getvalue()


def pin_fuel(bundle):
    content = vehicle_zip()
    registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    registry["sources"][FUEL]["components"]["vehicles"]["adapter"].update(
        expected_sha256=digest(content), expected_bytes=len(content)
    )
    bundle = save_registry(bundle, registry)
    request = fuel.build_request(bundle)
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    request.cache_path.write_bytes(content)
    return bundle, request


def fuel_session(bundle, content):
    session = FakeSession()
    session.downloads[fuel.build_request(bundle).url] = (
        content, {"Last-Modified": "Wed, 04 Nov 2026 12:00:00 GMT"}
    )
    return session


def test_fuel_next_model_year_is_reviewable_and_replays_strictly_offline(isolated_bundle):
    bundle, original_request = pin_fuel(isolated_bundle)
    session = fuel_session(bundle, vehicle_zip(years=(1984, 2024, 2027, 2028)))
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    candidate = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert "coverage expanded" in " ".join(candidate["review_required"])
    rejected = updater.run_update(bundle, sources={FUEL}, session=session, apply=True)
    assert rejected.errors and not rejected.applied
    applied = updater.run_update(bundle, sources={FUEL}, session=session,
                                 apply=True, review_report=review.report_path)
    assert applied.applied and not applied.errors
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    request = fuel.build_request(current)
    assert request.expected_model_year_to == 2028
    fuel.validate_cache(request)
    assert fuel.read_selected_vehicle_columns(request).year.max() == 2028
    assert original_request.cache_path.read_bytes() == vehicle_zip()
    assert current.scenario.sources == bundle.scenario.sources


def test_fuel_range_change_has_reviewable_counts_hashes_and_samples(isolated_bundle):
    bundle, _ = pin_fuel(isolated_bundle)
    session = fuel_session(bundle, vehicle_zip(electric_range=250))
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    candidate = json.loads(review.report_path.read_text(encoding="utf-8"))["candidates"][0]
    evidence = candidate["evidence"]
    assert evidence["change_kind"] == "selected_records"
    assert not evidence["selected_records"]["vehicles"]["changed"]
    changes = evidence["selected_records"]["range_evidence"]
    assert changes["added_rows"] == changes["removed_rows"] == 1
    assert changes["added_sample"][0]["record"]["cd_range_miles"] == 250
    assert changes["removed_sample"][0]["record"]["cd_range_miles"] == 200
    assert changes["before_sha256"] != changes["after_sha256"]
    rejected = updater.run_update(bundle, sources={FUEL}, session=session, apply=True)
    assert rejected.errors and not rejected.applied
    applied = updater.run_update(bundle, sources={FUEL}, session=session,
                                 apply=True, review_report=review.report_path)
    assert applied.applied and not applied.errors


@pytest.mark.parametrize("reorder", [False, True])
def test_fuel_packaging_and_row_order_preserve_selected_data(isolated_bundle, reorder):
    bundle, _ = pin_fuel(isolated_bundle)
    session = fuel_session(bundle, vehicle_zip(comment=b"new packaging", reverse=reorder))
    result = updater.run_update(bundle, sources={FUEL}, session=session, apply=True)
    assert result.applied and not result.errors
    candidate = json.loads(result.report_path.read_text(encoding="utf-8"))["candidates"][0]
    assert not candidate["review_required"]
    assert not any(row["changed"] for row in candidate["evidence"]["selected_records"].values())
    assert candidate["evidence"]["change_kind"] == (
        "unconsumed_fields_or_csv_formatting" if reorder else "zip_packaging_only"
    )


def test_fuel_lost_interior_year_is_rejected_even_when_bounds_match(isolated_bundle):
    bundle, _ = pin_fuel(isolated_bundle)
    original, caches = bundle.sources_path.read_bytes(), cache_bytes(bundle)
    session = fuel_session(bundle, vehicle_zip(years=(1984, 2027)))
    result = updater.run_update(bundle, sources={FUEL}, session=session)
    assert "lost registered model years" in " ".join(result.errors)
    assert_preserved(bundle, original, caches)


@pytest.mark.parametrize("modified", [None, "not a date"])
def test_fuel_missing_publication_header_has_reviewable_content_identity(isolated_bundle, modified):
    bundle, _ = pin_fuel(isolated_bundle)
    content = vehicle_zip(electric_range=250)
    session = fuel_session(bundle, content)
    url = fuel.build_request(bundle).url
    session.downloads[url] = (content, {} if modified is None else {"Last-Modified": modified})
    review = updater.run_update(bundle, sources={FUEL}, session=session)
    assert not review.errors
    report = json.loads(review.report_path.read_text(encoding="utf-8"))
    assert any("Last-Modified" in reason for reason in report["candidates"][0]["review_required"])
    session.calls.clear()
    applied = updater.run_update(bundle, sources={FUEL}, session=session,
                                 apply=True, review_report=review.report_path)
    assert applied.applied and not applied.errors and not session.calls
    current = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    assert f"sha256-{digest(content)[:12]}" in current.sources.sources[FUEL].version
