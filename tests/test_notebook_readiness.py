"""Unavailable evidence stops results without swallowing scientific or I/O errors."""

from pathlib import Path
import html
import json
import re
import runpy
from types import SimpleNamespace

import marimo as mo
import pytest
from bs4 import BeautifulSoup


NOTEBOOKS = Path(__file__).resolve().parents[1] / "docs/insights"
CASES = [
    ("growth_constraint_diagnosis", "growth_evidence", "load_diagnostic_tables", "growth_tables",
     "Stale growth diagnostic handoff: config/parameters/rules.yaml; rebuild explicitly",
     "parameterization.road_adoption_constraints"),
    ("transportation_validation_diagnosis", "transport_comparison_evidence", "load_validation_diagnosis", "diagnosis",
     "Research build has stale paths configuration", "src/build_transport.py"),
]


def evidence_cell(case, reader):
    notebook, name, reader_name, *_ = case
    cell = runpy.run_path(str(NOTEBOOKS / f"{notebook}.py"))[name]
    arguments = {"mo": mo, reader_name: reader}
    if notebook == "growth_constraint_diagnosis":
        arguments.update(load_config_bundle=lambda path: object(), load_diagnostic_config=lambda bundle: object())
    else:
        arguments["transport_prepare_commands"] = "uv run python src/build_transport.py --scenario comparison.yaml --overwrite"
    return cell, arguments


@pytest.mark.parametrize("case", CASES, ids=lambda case: case[0])
@pytest.mark.parametrize("missing", [False, True], ids=["stale", "missing"])
def test_unavailable_evidence_shows_reason_and_preparation_without_results(case, missing):
    reason = "Required registered evidence is missing" if missing else case[4]

    def reader(*args, **kwargs):
        raise FileNotFoundError(reason) if missing else ValueError(reason)

    cell, arguments = evidence_cell(case, reader)
    output, definitions = cell.run(**arguments)
    rendered = output._repr_html_()
    # Marimo callouts encode syntax-highlighted HTML in a JSON attribute.
    embedded = re.findall(r"data-html='([^']*)'", rendered)
    rendered += " ".join(BeautifulSoup(json.loads(html.unescape(item)), "html.parser").get_text()
                         for item in embedded)
    assert reason in rendered
    assert case[5] in rendered
    assert not hasattr(definitions, case[3])


@pytest.mark.parametrize("case", CASES, ids=lambda case: case[0])
@pytest.mark.parametrize("error", [ValueError("Invalid scientific quantity"), PermissionError("Access denied")])
def test_other_errors_remain_visible(case, error):
    def reader(*args, **kwargs):
        raise error

    cell, arguments = evidence_cell(case, reader)
    with pytest.raises(type(error), match=re.escape(str(error))):
        cell.run(**arguments)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case[0])
def test_valid_evidence_is_returned_without_substitution(case):
    evidence = object()
    cell, arguments = evidence_cell(case, lambda *args, **kwargs: evidence)
    _, definitions = cell.run(**arguments)
    assert definitions[case[3]] is evidence


def test_empty_day_selection_does_not_construct_an_unexplained_empty_timing_chart():
    cell = runpy.run_path(str(NOTEBOOKS / "travel_survey_diagnosis.py"))["trip_timing"]

    def no_chart(*args, **kwargs):
        pytest.fail("No timing chart should be constructed without a selected population")

    output, _ = cell.run(DERIVATION="reviewed", clock=None, clock_chart=no_chart,
                         clock_heatmap=no_chart, clock_view=None,
                         days=SimpleNamespace(value=[]), mo=mo, survey=None)
    assert "Select at least one day type" in output._repr_html_()
