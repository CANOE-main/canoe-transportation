"""Dunsky archive recovery and review against authenticated report evidence."""

from __future__ import annotations

import hashlib
from html import escape
import json
from pathlib import Path
import re
import shutil
from types import SimpleNamespace

import pandas as pd
import pytest
import requests
import yaml

from fetching import dunsky_charger_inputs as dunsky
from fetching import source_updater as updater
from utils import load_config_bundle, resolve_input_path, resolve_parameter_path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


def _sha(content):
    return hashlib.sha256(content).hexdigest()


def _report_html(settings):
    years = list(range(2025, 2051, 5))
    native_cost_years = years[:4]
    policy = "Policy Reference (primary scenario)"
    rows = {
        6: [["Scenario", "Charger", *native_cost_years],
            *[[f"Scenario {i}", "L2", 100, 101, 102, 103] for i in range(24)]],
        12: [["Charger", *native_cost_years],
             ["L2", *(["$15,000 (7 kW average)"] * 4)],
             ["DCFC", *[f"$240,000 ({power} kW average)" for power in (125, 175, 250, 300)]]],
        19: [["Location", *(["Public"] * 6)], ["Charger", *(["Fast"] * 6)],
             *[[label, *([value] * 6)] for label, value in (
                 ("Power", "50"), ("Session", "2"), ("Daily throughput", "10"),
                 ("Pre-2027 share", "20%"), ("Post-2027 share", "30%"),
             )]],
        21: [["MHDV", *(["Cost"] * 5)], ["Charger", "Power", *native_cost_years],
             *[[label, power, *(["$200,000"] * 4)] for label, power in (
                 ("Overnight", "50"), ("Fast Charge", "350"), ("Ultra-Fast Charge", "2,000"),
             )]],
        30: [["Charger", *years], *[[label, *([value] * 6)] for label, value in (
            ("Home - L1", "1.5"), ("Public - L2", "10"), ("Home - L2", "10"),
            ("DCFC", "125"), ("Public overnight", "50"), ("Private overnight", "50"),
        )]],
        31: [["Charger", *years], *[[f"Charger {i}", *(["20%"] * 6)] for i in range(4)]],
        38: [["Scenario", "Location", "Charger", *native_cost_years],
             *[[policy, location, charger, *counts]
               for location in ("Public", "Private")
               for charger, counts in (("Overnight", [8, 9, 10, 11]), ("FC", [2, 3, 4, 5]),
                                       ("UFC", ["-", 1, 2, 3]))],
             *[[f"Other scenario {i}", "Public", "FC", 6, 7, 8, 9] for i in range(21)]],
        44: [[f"Cell {row}/{column}" for column in range(17)] for row in range(12)],
    }
    paragraphs = [
        *settings["required_edition_tokens"],
        "For example, the 6-vehicle partial charge throughput input for public vehicles which would draw "
        "12 kWh daily on average. Return to footnote 67 referrer",
        "We mapped the CEUD vehicle categories used in this study (as described in Section 3.1)",
        "We assume a ZEV scrappage rate of 2% over time.",
        "Three MHDV sales forecast scenarios: Policy Reference and Slow Adoption (35% by 2032).",
        "We further assume that all MHDVs will be battery-electric vehicles at lower hydrogen prices.",
    ]
    body = [f"<p>{escape(paragraph)}</p>" for paragraph in paragraphs]
    for number, spec in settings["tables"].items():
        caption = re.sub(r"\\([.()])", r"\1", spec["caption_pattern"])
        body.append(f'<table id="{spec["html_id"]}"><caption>{escape(caption)}</caption><thead>')
        for index, cells in enumerate(rows[number]):
            if index == spec["header_rows"]:
                body.append("</thead><tbody>")
            body.append("<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in cells) + "</tr>")
        body.append("</tbody></table>")
    return ("<!doctype html><html><body>" + "".join(body) + "</body></html>").encode()


@pytest.fixture
def registered_report(tmp_path):
    shutil.copytree(REPO_ROOT / "config", tmp_path / "config")
    bundle = load_config_bundle(SCENARIO, repo_root=tmp_path)
    content = _report_html(dunsky.settings_for(bundle))
    registry = yaml.safe_load(bundle.sources_path.read_text(encoding="utf-8"))
    registration = registry["sources"][dunsky.SOURCE]["adapter"]["diagnostic_report"]
    registration.update(expected_sha256=_sha(content), expected_bytes=len(content))
    bundle.sources_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    bundle = load_config_bundle(SCENARIO, repo_root=tmp_path)
    request = dunsky.ReportRequest.model_validate(registration)
    path = resolve_input_path(bundle, "cache", request.cache_path)
    assert request.url != request.acquisition_url
    assert not path.exists()
    return bundle, request, path, content


def _archive_session(request, content):
    def get(url, **_kwargs):
        assert url == request.acquisition_url
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response.headers["Content-Type"] = "text/html"
        response._content = content
        response._content_consumed = True
        return response

    return SimpleNamespace(get=get)


def _record(result):
    return json.loads(result.report_path.read_text(encoding="utf-8"))


def test_dunsky_repackaging_requires_and_replays_captured_review(registered_report):
    bundle, request, cache, content = registered_report
    cache.parent.mkdir(parents=True)
    cache.write_bytes(content)
    original_registry = bundle.sources_path.read_bytes()
    before, _ = dunsky.production_inputs(bundle)
    repackaged = content + b"\n<!-- government archive metadata -->\n"
    sources = {dunsky.SOURCE}

    proposal = updater.run_update(bundle, sources=sources, session=_archive_session(request, repackaged))
    assert not proposal.errors
    reviewed = _record(proposal)
    assert "dunsky_charger_inputs.yaml" in reviewed["rules_fingerprints"]
    candidate, = reviewed["candidates"]
    assert candidate["review_required"]
    assert candidate["evidence"]["same_normalized_evidence"] is True
    assert candidate["evidence"]["publication_url"] == request.url
    assert candidate["evidence"]["acquisition_url"] == request.acquisition_url
    capture, = reviewed["acquisitions"]
    assert capture["sha256"] == _sha(repackaged)
    assert (proposal.report_path.parent / capture["file"]).read_bytes() == repackaged

    denied = updater.run_update(bundle, sources=sources, apply=True, session=_archive_session(request, repackaged))
    assert any("require --review-report" in error for error in denied.errors)
    assert bundle.sources_path.read_bytes() == original_registry
    assert list(cache.parent.iterdir()) == [cache]

    def deny_network(*_args, **_kwargs):
        pytest.fail("Reviewed adoption must use captured bytes")

    applied = updater.run_update(bundle, sources=sources, apply=True, review_report=proposal.report_path,
                                 session=SimpleNamespace(get=deny_network))
    assert applied.applied and not applied.errors
    adopted = load_config_bundle(SCENARIO, repo_root=bundle.repo_root)
    after, adopted_cache = dunsky.production_inputs(adopted)
    assert adopted_cache != cache
    assert adopted_cache.read_bytes() == repackaged
    assert cache.read_bytes() == content
    citation = bundle.sources.sources[dunsky.SOURCE].citation
    assert request.url in citation and request.acquisition_url not in citation
    assert set(after["source -> data_source"]) == {citation}
    evidence, _ = dunsky.load_evidence(adopted)
    assert evidence["url"] == request.url
    assert evidence["acquisition_url"] == request.acquisition_url
    assert set(after["source_sha256"]) == {_sha(repackaged)}
    pd.testing.assert_frame_equal(before.drop(columns="source_sha256"), after.drop(columns="source_sha256"))


def test_dunsky_review_expires_when_extraction_rules_change(registered_report):
    bundle, request, cache, content = registered_report
    cache.parent.mkdir(parents=True)
    cache.write_bytes(content)
    registry = bundle.sources_path.read_bytes()
    sources = {dunsky.SOURCE}
    proposal = updater.run_update(bundle, sources=sources,
                                  session=_archive_session(request, content + b"<!-- archive metadata -->"))
    assert not proposal.errors
    rules = resolve_parameter_path(bundle, "dunsky_charger_inputs.yaml")
    rules.write_bytes(rules.read_bytes() + b"\n# Rule file edited after review\n")
    result = updater.run_update(bundle, sources=sources, apply=True, review_report=proposal.report_path)
    assert any("no longer matches" in error for error in result.errors)
    assert not result.applied
    assert bundle.sources_path.read_bytes() == registry
    assert list(cache.parent.iterdir()) == [cache]


@pytest.mark.parametrize("entrypoint", ["production", "updater"])
def test_dunsky_recovers_exact_pin_from_archive_into_empty_cache(registered_report, entrypoint):
    bundle, request, cache, content = registered_report
    registry = bundle.sources_path.read_bytes()
    session = _archive_session(request, content)
    if entrypoint == "production":
        dunsky.load_evidence(bundle, download=True, session=session)
    else:
        result = updater.run_update(bundle, sources={dunsky.SOURCE}, apply=True, session=session)
        assert result.applied and not result.errors
        candidate, = _record(result)["candidates"]
        assert not candidate["review_required"]
        assert not candidate["changes"]
    assert cache.read_bytes() == content
    assert bundle.sources_path.read_bytes() == registry
    evidence, _ = dunsky.load_evidence(bundle)
    assert evidence["url"] == request.url
    assert evidence["source_sha256"] == request.expected_sha256


@pytest.mark.parametrize(("change", "message"), [
    ("table", "Dunsky report tables changed"),
    ("context", "Dunsky report context changed"),
    ("baseline_missing", "Restore the authenticated Dunsky baseline"),
    ("baseline_corrupt", "Restore the authenticated Dunsky baseline"),
])
def test_dunsky_rejects_changed_evidence_or_unverified_baseline(registered_report, change, message):
    bundle, request, cache, content = registered_report
    original_registry = bundle.sources_path.read_bytes()
    revised = content + b"<!-- archive metadata -->"
    if change != "baseline_missing":
        cache.parent.mkdir(parents=True)
        cache.write_bytes(content if change != "baseline_corrupt" else b"damaged baseline")
    if change == "table":
        revised = content.replace(b"<td>100</td>", b"<td>110</td>", 1)
    elif change == "context":
        revised = content.replace(b"scrappage rate of 2%", b"scrappage rate of 3%", 1)
    original_cache = {path.name: path.read_bytes() for path in cache.parent.glob("*")}
    result = updater.run_update(bundle, sources={dunsky.SOURCE}, apply=True,
                                session=_archive_session(request, revised))
    assert any(message in error for error in result.errors)
    assert not result.applied
    assert bundle.sources_path.read_bytes() == original_registry
    assert {path.name: path.read_bytes() for path in cache.parent.glob("*")} == original_cache
