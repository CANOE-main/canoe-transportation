"""Cost provenance preserves native component DQ alongside unlike support evidence."""

from dataclasses import replace
from pathlib import Path

import pytest

from parameterization.build_costs import ATB, CER, FAA, _context
from utils import load_config_bundle
from validation.config_models import SourcesConfig


ROOT = Path(__file__).resolve().parents[1]
DQ_FIELDS = ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")
COMPONENT_SCORES = {
    (ATB, "vehicles"): (1, 2, 3, 2, 4),
    (ATB, "maintenance_ldv"): (2, 3, 2, 3, 1),
    (FAA, "section_3_capacity"): (1, 1, 1, 1, 1),
    (FAA, "section_4_operating_costs"): (3, 2, 3, 2, 2),
    (CER, "macro-indicators"): (4, 4, 4, 4, 4),
}


@pytest.fixture
def annotated_bundle():
    """Use registered identities with deliberately different in-memory annotations."""
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT)
    payload = bundle.sources.model_dump(mode="python")
    for (source, component), scores in COMPONENT_SCORES.items():
        payload["sources"][source]["components"][component]["data_quality"] = dict(
            zip(DQ_FIELDS, scores, strict=True)
        )
    return replace(bundle, sources=SourcesConfig.model_validate(payload))


@pytest.mark.parametrize(
    ("source", "native_component", "supporting_component", "source_id"),
    [
        (ATB, "maintenance_ldv", "vehicles", "T06"),
        (FAA, "section_4_operating_costs", "section_3_capacity", "T13"),
    ],
)
def test_native_cost_component_governs_dq_with_distinct_support_evidence(
    annotated_bundle, source, native_component, supporting_component, source_id,
):
    components = [
        (source, supporting_component),
        (CER, "macro-indicators"),
        (source, native_component),
    ]
    expected = dict(zip(DQ_FIELDS, COMPONENT_SCORES[source, native_component], strict=True))
    context = _context(
        annotated_bundle, key="variable.fixture", components=components,
        governing=source, governing_component=native_component,
        digest="unchanged-cost-inputs", version="1",
    )
    reversed_context = _context(
        annotated_bundle, key="variable.fixture", components=list(reversed(components)),
        governing=source, governing_component=native_component,
        digest="unchanged-cost-inputs", version="1",
    )

    assert context.parameter_fields() == {
        "data_source": source_id, "data_id": context.data_id, **expected,
    }
    assert reversed_context == context
    assert {item.source_key for item in context.contributors} == {source, CER}


def test_unique_investment_component_keeps_dq_when_currency_evidence_differs(annotated_bundle):
    context = _context(
        annotated_bundle, key="invest.fixture",
        components=[(CER, "macro-indicators"), (ATB, "vehicles")],
        governing=ATB, digest="unchanged-cost-inputs", version="1",
    )

    assert context.data_quality.row_fields() == dict(
        zip(DQ_FIELDS, COMPONENT_SCORES[ATB, "vehicles"], strict=True)
    )
    assert {item.source_key for item in context.contributors} == {ATB, CER}


@pytest.mark.parametrize("governing_component", [None, "vmt_ldv"])
def test_cost_dq_rejects_ambiguous_or_absent_native_component(
    annotated_bundle, governing_component,
):
    with pytest.raises(ValueError, match="one native cost component"):
        _context(
            annotated_bundle, key="variable.fixture",
            components=[(ATB, "vehicles"), (ATB, "maintenance_ldv")],
            governing=ATB, governing_component=governing_component,
            digest="unchanged-cost-inputs", version="1",
        )
