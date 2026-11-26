"""Diagnostic extraction preserves native evidence and explicit denominators."""

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from fetching import dunsky_charger_inputs as dunsky
from fetching.dunsky_charger_inputs import ReportRequest, ReportTables, expand_cells, normalize_report
from validation.charger_infrastructure_diagnostic import annual_rows, number, projected_compositions, source_anomalies


def test_html_spans_preserve_source_labels_and_numeric_text():
    parser = ReportTables()
    parser.feed('<table id="t"><caption>Example</caption><tr><th rowspan="2">Public</th>'
                '<th colspan="2">Power</th></tr><tr><td>2,000</td><td>-26</td></tr></table>')
    assert expand_cells(parser.tables["t"]["rows"]) == [["Public", "Power", "Power"], ["Public", "2,000", "-26"]]


def test_parser_rejects_duplicate_tables_and_incomplete_spans():
    parser = ReportTables()
    with pytest.raises(ValueError, match="Duplicate"):
        parser.feed('<table id="a"></table><table id="a"></table>')
    with pytest.raises(ValueError, match="beyond"):
        expand_cells([{"cells": [{"text": "x", "rowspan": 2, "colspan": 1}]}])


@pytest.mark.parametrize("cell,expected", [("$160,000 (125 kW average)", 160000), ("6.5%", .065), ("-26", -26), ("2,000", 2000)])
def test_native_numbers_keep_units_and_negative_evidence(cell, expected):
    assert number(cell) == expected


def test_dash_is_not_an_implicit_missing_value_fill():
    with pytest.raises(ValueError, match="Unrecognized"):
        number("-")
    assert number("-", dash_zero=True) == 0
    with pytest.raises(ValueError, match="Unrecognized"):
        number("suppressed")


@pytest.mark.parametrize("charger,year", [("UFC", 2025), ("FC", 2025), ("UFC", 2030)])
def test_source_dash_policy_is_limited_to_configured_pre_introduction_cells(charger, year):
    evidence = {"url": "https://example.org/report", "tables": {"38": {
        "header_rows": 1, "evidence_kind": "projected_requirement", "id": "t38",
        "matrix": [["scenario", "location", "charger", str(year)], ["base", "Public", charger, "-"]],
    }}}
    args = (evidence, 38, ["scenario", "location", "charger"], "ports")
    if (charger, year) == ("UFC", 2025):
        parsed = annual_rows(*args, zero_dash_cells={("UFC", 2025)})
        assert parsed.value.item() == 0
        assert parsed.raw_value.item() == "-"
    else:
        with pytest.raises(ValueError, match="Unrecognized"):
            annual_rows(*args, zero_dash_cells={("UFC", 2025)})
    with pytest.raises(ValueError, match="Unrecognized"):
        annual_rows(*args, zero_dash_cells=set())


def test_edition_caption_shape_and_raw_identity_are_checked():
    content = b'<p>February 2024</p><table id="t6"><caption>Table 6. Counts</caption><tr><td>2025</td><td>2</td></tr></table>'
    settings = {"required_edition_tokens": ["February 2024"], "tables": {6: {
        "html_id": "t6", "caption_pattern": r"Table 6\. Counts", "rows": 1, "columns": 2,
        "header_rows": 0, "evidence_kind": "projected_requirement"}}}
    result = normalize_report(content, settings)
    assert result["source_sha256"] == hashlib.sha256(content).hexdigest()
    with pytest.raises(ValueError, match="edition marker"):
        normalize_report(content.replace(b"2024", b"2021"), settings)
    with pytest.raises(ValueError, match="caption changed"):
        normalize_report(content.replace(b"Counts", b"Costs"), settings)
    with pytest.raises(ValueError, match="shape changed"):
        normalize_report(content.replace(b"<td>2</td>", b""), settings)


def test_report_request_rejects_cache_traversal():
    with pytest.raises(ValidationError, match="bounded"):
        ReportRequest(url="https://example.org/report", cache_path="../report.html", expected_bytes=100,
                      expected_sha256="a" * 64, publication_date="2024-02")


def test_offline_missing_and_changed_cache_never_download_or_repair(tmp_path, monkeypatch):
    payload = {"url": "https://example.org/report", "cache_path": "report.html", "expected_bytes": 2,
               "expected_sha256": hashlib.sha256(b"ok").hexdigest(), "publication_date": "2024-02"}
    bundle = SimpleNamespace(sources=SimpleNamespace(sources={dunsky.SOURCE: SimpleNamespace(adapter={"diagnostic_report": payload})}))
    path = tmp_path / "report.html"
    monkeypatch.setattr(dunsky, "resolve_input_path", lambda *args: path)
    class NoNetwork:
        def get(self, *args, **kwargs):
            pytest.fail("Offline extraction attempted network access")
    with pytest.raises(FileNotFoundError, match="Pinned Dunsky"):
        dunsky.load_evidence(bundle, session=NoNetwork())
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Cached Dunsky"):
        dunsky.load_evidence(bundle, download=True, session=NoNetwork())
    assert path.read_bytes() == b"changed"


def test_download_identity_validated_before_cache_publication(tmp_path, monkeypatch):
    payload = {"url": "https://example.org/report", "cache_path": "report.html", "expected_bytes": 2,
               "expected_sha256": hashlib.sha256(b"ok").hexdigest(), "publication_date": "2024-02"}
    bundle = SimpleNamespace(sources=SimpleNamespace(sources={dunsky.SOURCE: SimpleNamespace(adapter={"diagnostic_report": payload})}))
    path = tmp_path / "report.html"
    monkeypatch.setattr(dunsky, "resolve_input_path", lambda *args: path)
    response = SimpleNamespace(content=b"new", status_code=200, raise_for_status=lambda: None)
    with pytest.raises(ValueError, match="Downloaded Dunsky"):
        dunsky.load_evidence(bundle, download=True, session=SimpleNamespace(get=lambda *args, **kwargs: response))
    assert not path.exists()


def sample_frames():
    ldv = pd.DataFrame([
        {"scenario": "base", "year": 2025, "charger": kind, "value": ports}
        for kind, ports in [("L1 - home", 100), ("L2 - home", 800), ("Total - L2", 90),
                            ("Total - DCFC", 10), ("L2 - work", 30), ("Total Private", 930)]
    ])
    mhdv = pd.DataFrame([
        {"scenario": "base", "location": location, "year": 2025, "charger": kind, "value": value}
        for location, values in [("Public", [4, 6, 0, 10]), ("Private", [80, 10, 0, 5])]
        for kind, value in zip(["Overnight", "FC", "UFC", "Total"], values, strict=True)
    ])
    regional = pd.DataFrame([{"geography": "SK", "charger": "UFC", "year": 2030, "value": -26}])
    return {"ldv": ldv, "mhdv": mhdv, "regional": regional}


def test_compositions_count_workplace_once_and_do_not_use_bad_subtotals():
    frames = sample_frames()
    before = {key: value.copy(deep=True) for key, value in frames.items()}
    result = projected_compositions(frames)
    all_ldv = result.loc[result.population.eq("All ports, workplace once (diagnostic)")]
    assert all_ldv.ports.sum() == 1000
    assert all_ldv.port_share.sum() == pytest.approx(1)
    combined = result.loc[result.population.eq("Public + Private")]
    assert combined.ports.sum() == 100
    assert combined.set_index("charger").port_share.to_dict() == pytest.approx({"Overnight": .84, "FC": .16, "UFC": 0})
    issues = source_anomalies(frames)
    assert len(issues) == 2
    assert issues.loc[issues.table.eq(38), "difference"].item() == -85
    assert issues.loc[issues.table.eq(44), "published"].item() == -26
    for key in frames:
        pd.testing.assert_frame_equal(frames[key], before[key])


def test_negative_national_component_cannot_enter_illustrated_shares():
    frames = sample_frames()
    frames["mhdv"].loc[frames["mhdv"].charger.eq("UFC"), "value"] = -1
    with pytest.raises(ValueError, match="Negative"):
        projected_compositions(frames)


def test_grand_totals_are_audited_against_components_and_published_subtotals():
    frames = sample_frames()
    frames["mhdv"] = pd.concat([frames["mhdv"], pd.DataFrame([{
        "scenario": "base", "location": "Total Ports", "year": 2025, "charger": "Total Ports", "value": 108,
    }])], ignore_index=True)
    findings = source_anomalies(frames)
    grand = findings.loc[findings.scope.str.contains("Total Ports")]
    assert set(grand.difference) == {8, 93}
    assert set(grand.published) == {108}


def test_report_policy_reference_2025_grand_total_discrepancy_is_retained():
    # Published Table 38, February 2024 edition: 43,318 versus 8,091 + 35,219.
    frames = sample_frames()
    records = [
        {"scenario": "Policy Reference (primary scenario)", "location": location,
         "year": 2025, "charger": charger, "value": value}
        for location, values in [("Public", [3561, 4530, 0, 8091]),
                                 ("Private", [30993, 4226, 0, 35219])]
        for charger, value in zip(["Overnight", "FC", "UFC", "Total"], values, strict=True)
    ]
    records.append({"scenario": "Policy Reference (primary scenario)", "location": "Total Ports",
                    "year": 2025, "charger": "Total Ports", "value": 43318})
    frames["mhdv"] = pd.DataFrame(records)
    grand = source_anomalies(frames).loc[lambda rows: rows.table.eq(38)]
    assert len(grand) == 2
    assert set(grand.published) == {43318}
    assert set(grand.component_sum) == {43310}
    assert set(grand.difference) == {8}


def test_notebook_remains_read_only_and_covers_all_subjects():
    text = (Path(__file__).resolve().parents[1] / "docs/insights/charger_infrastructure_diagnosis.py").read_text(encoding="utf-8")
    assert "load_diagnostic()" in text
    assert "write=True" not in text
    for subject in ["report's ports", "parameter trajectories", "capacity shares", "existing stock", "utilization"]:
        assert subject in text
