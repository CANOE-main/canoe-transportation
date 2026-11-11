"""Publication identity tests use source-like HTML and private in-memory fixtures."""

import pandas as pd
import pytest

from fetching.assorted_sources import AssortedSourcesError, normalize_tc_ev_dashboard_table
from fetching.publication_tables import (
    PublicationTableError,
    normalize_publication_label,
    select_publication_table,
)


HEADING = "Medium- and heavy-duty EV market share"
HEADERS = ["Province/territory", "Market share (Q1 2026)", "2026 year-to-date"]
REGIONS = ["Ontario", "Canada"]


def table(*, identifier="tbl-mhzev", headers=None, rows=None):
    headers = HEADERS if headers is None else headers
    rows = [["Ontario", "3.1%", "3.1%"], ["Canada", "2.7%", "2.7%"]] if rows is None else rows
    cells = "".join("<th>" + value + "</th>" for value in headers)
    body = "".join("<tr>" + "".join("<td>" + value + "</td>" for value in row) + "</tr>" for row in rows)
    return f'<table id="{identifier}"><tr>{cells}</tr>{body}</table>'


def page(content=None, *, heading_id="medium-heavy-duty-zev-market-share", heading_text=HEADING):
    return f'<h2 id="{heading_id}">{heading_text}</h2>' + (table() if content is None else content)


def select(html, **options):
    return select_publication_table(html, **{
        "table_id": "tbl-mhzev", "heading_id": "medium-heavy-duty-zev-market-share",
        "heading_text": HEADING, "required_headers": HEADERS, "source_regions": REGIONS,
        **options,
    })


def shares(html, **options):
    return normalize_tc_ev_dashboard_table(html, **{
        "table_id": "tbl-mhzev", "heading_id": "medium-heavy-duty-zev-market-share",
        "heading_text": HEADING, "quarter_header": HEADERS[1], "ytd_header": HEADERS[2],
        "year": 2026, "source_regions": REGIONS, **options,
    })


def test_label_normalization_preserves_semantics_and_periods():
    assert normalize_publication_label("  Ｍarket SHARE (Q１\u00a02026) ") == "market share q1 2026"
    assert normalize_publication_label("2026 year–to–date") == "2026 year to date"
    assert normalize_publication_label("Light-duty") != normalize_publication_label("Heavy-duty")
    assert normalize_publication_label("Q1 2026") != normalize_publication_label("Q2 2026")


@pytest.mark.parametrize("option", ["table_id_aliases", "heading_id_aliases", "required_headers", "source_regions"])
@pytest.mark.parametrize("value", ["accidental YAML scalar", [""]])
def test_publication_identity_configuration_rejects_scalar_or_empty_labels(option, value):
    with pytest.raises(PublicationTableError, match="sequence of nonempty labels"):
        select(page(), **{option: value})


def test_identity_and_containing_section_are_recorded():
    selected = select(page())
    assert selected.rows[0] == ["Ontario", "3.1%", "3.1%"]
    assert selected.selection["table_resolution"] == "id"
    assert selected.selection["heading_resolution"] == "id"


def test_presentation_changes_are_normalized_and_regions_are_canonicalized():
    html = page(table(headers=["PROVINCE / Territory", "market share [Q1\u00a02026]", "2026 year–to–date"],
                      rows=[["ONTARIO", "3.1%", "3.1%"], ["canada", "2.7%", "2.7%"]]),
                heading_text="MEDIUM – AND HEAVY DUTY EV MARKET SHARE")
    result = shares(html)
    assert result.source_region.tolist() == REGIONS
    assert result.attrs["publication_selection"]["selected_headers"]["ytd"] == "2026 year–to–date"


@pytest.mark.parametrize("heading_id, table_id", [("renamed-heading", "tbl-mhzev"),
                                                   ("medium-heavy-duty-zev-market-share", "renamed-table"),
                                                   ("renamed-heading", "renamed-table")])
def test_renamed_identifiers_require_exact_section_and_structural_signature(heading_id, table_id):
    selected = select(page(table(identifier=table_id), heading_id=heading_id))
    assert selected.selection["heading_resolution"] == ("id" if heading_id.startswith("medium") else "normalized_text")
    assert selected.selection["table_resolution"] == ("id" if table_id == "tbl-mhzev" else "section_signature")


def test_configured_identifier_aliases_work_without_fuzzy_selection():
    selected = select(page(table(identifier="reviewed-table"), heading_id="reviewed-heading"),
                      heading_id_aliases=["reviewed-heading"], table_id_aliases=["reviewed-table"])
    assert selected.selection["table_resolution"] == "id"
    assert selected.selection["heading_resolution"] == "id"


def test_matching_table_outside_section_is_rejected_even_with_valid_fallback():
    html = '<h2 id="wrong">Light-duty EV market share</h2>' + table() + page(table(identifier="alternate"))
    with pytest.raises(PublicationTableError, match="outside"):
        select(html)


def test_heading_containment_includes_subsections_but_stops_at_peer_heading():
    assert select(page('<h3 id="details">Data</h3>' + table())).rows
    with pytest.raises(PublicationTableError, match="outside"):
        select(page('<h2 id="next">Other</h2>' + table()))


@pytest.mark.parametrize("content", [table() + table(), table(identifier="one") + table(identifier="two")])
def test_duplicate_id_or_competing_structural_candidates_are_rejected(content):
    with pytest.raises(PublicationTableError, match="ambiguous"):
        select(page(content))


def test_id_and_configured_alias_are_ambiguous_when_both_exist():
    with pytest.raises(PublicationTableError, match="ambiguous"):
        select(page(table() + table(identifier="alias")), table_id_aliases=["alias"])


def test_fuzzy_heading_similarity_is_only_a_suggestion():
    with pytest.raises(PublicationTableError, match="suggestions only") as caught:
        select(page(heading_id="changed", heading_text="Medium-duty EV market share"))
    assert "Medium-duty EV market share" in str(caught.value)


def test_table_fallback_needs_both_reviewed_headers_and_regions():
    html = page(table(identifier="renamed"))
    for options in ({"required_headers": ()}, {"source_regions": None}):
        with pytest.raises(PublicationTableError, match="suggestions only"):
            select(html, **options)


@pytest.mark.parametrize("rows", [
    [["Ontario", "3.1%", "3.1%"]],
    [["Ontario", "3.1%", "3.1%"], ["Quebec", "2.7%", "2.7%"]],
    [["Ontario", "3.1%", "3.1%"], ["Ontario", "2.7%", "2.7%"]],
    [["Ontario", "3.1%"], ["Canada", "2.7%", "2.7%"]],
])
def test_incomplete_conflicting_or_changed_regions_are_rejected(rows):
    with pytest.raises(PublicationTableError, match="regional/row structure"):
        select(page(table(rows=rows)))


@pytest.mark.parametrize("headers", [HEADERS + ["2026 YEAR TO DATE"], HEADERS + [""]])
def test_duplicate_or_empty_normalized_headers_are_rejected(headers):
    rows = [["Ontario", "3.1%", "3.1%", "3.1%"], ["Canada", "2.7%", "2.7%", "2.7%"]]
    with pytest.raises(PublicationTableError, match="headers changed"):
        select(page(table(headers=headers, rows=rows)))


def test_known_id_cannot_hide_changed_heading_semantics():
    with pytest.raises(PublicationTableError, match="text changed"):
        select(page(heading_text="Light-duty EV market share"))


@pytest.mark.parametrize("mutation", [lambda html: html.replace("</table>", ""),
                                      lambda html: html.replace("<td>Ontario", '<td colspan="2">Ontario'),
                                      lambda html: html.replace("<td>Ontario", '<td rowspan="2">Ontario')])
def test_incomplete_tables_and_spanning_cells_are_rejected(mutation):
    with pytest.raises(PublicationTableError, match="regional/row structure"):
        select(mutation(page()))


@pytest.mark.parametrize("regions", [[], ["Ontario", "ONTARIO"], [""]])
def test_invalid_region_configuration_is_rejected_before_selection(regions):
    with pytest.raises(PublicationTableError, match="Configured publication regions"):
        select(page(), source_regions=regions)


def test_extra_quarters_and_reordered_measures_preserve_selected_q1():
    headers = [HEADERS[0], HEADERS[2], "Market share (Q2 2026)", HEADERS[1], "Market share (Q4 2026)"]
    rows = [["Ontario", "4%", "5%", "3.1%", "6%"], ["Canada", "3%", "4%", "2.7%", "5%"]]
    result = shares(page(table(headers=headers, rows=rows)))
    assert result.quarter_market_share.tolist() == pytest.approx([.031, .027])
    assert result.ytd_market_share.tolist() == pytest.approx([.04, .03])
    assert result.attrs["publication_selection"]["additional_quarter_columns"] == [headers[2], headers[4]]
    assert result.attrs["published_observations"] == {"headers": headers, "rows": rows}


@pytest.mark.parametrize("extra", ["Market share (Q2 2025)", "Market share (Q2 2027)",
                                   "Market share (Q5 2026)", "BEV share (Q2 2026)", "EV count"])
def test_added_columns_must_be_known_same_year_quarters(extra):
    rows = [["Ontario", "3.1%", "3.1%", "5%"], ["Canada", "2.7%", "2.7%", "4%"]]
    with pytest.raises(AssortedSourcesError, match="headers changed"):
        shares(page(table(headers=HEADERS + [extra], rows=rows)))


@pytest.mark.parametrize("invalid", ["101%", "NaN%", "-1%", "5", "unknown"])
def test_unselected_quarter_values_are_also_validated(invalid):
    rows = [["Ontario", "3.1%", "3.1%", invalid], ["Canada", "2.7%", "2.7%", "4%"]]
    with pytest.raises(AssortedSourcesError, match="share"):
        shares(page(table(headers=HEADERS + ["Market share (Q2 2026)"], rows=rows)))


def test_absent_configured_q1_cannot_implicitly_select_q2():
    with pytest.raises(AssortedSourcesError, match="headers changed"):
        shares(page(table(headers=[HEADERS[0], "Market share (Q2 2026)", HEADERS[2]])))


def test_original_output_contract_is_preserved_without_policy_extensions():
    result = shares(page(), heading_text=None, source_regions=None)
    expected = pd.DataFrame({"source_region": REGIONS, "source_year": [2026, 2026],
                             "quarter_market_share": [.031, .027], "ytd_market_share": [.031, .027],
                             "units": ["fraction", "fraction"]})
    pd.testing.assert_frame_equal(result, expected)
