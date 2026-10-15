"""Registry precedence selects one actual source/DQ without combining its scores."""

from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from validation.config_models import SourcesConfig
from validation.provenance import (
    ProvenanceError,
    resolve_composite_provenance,
    resolve_provenance,
)


DQ_FIELDS = ("dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time")


def quality(score):
    return dict.fromkeys(DQ_FIELDS, score)


def registry_payload():
    sources = {}
    for source_id, (key, score) in enumerate(
        (("native", 2), ("support", 4), ("unused", 1)), start=1,
    ):
        sources[key] = {
            "source_id": f"T{source_id:02d}",
            "title": key,
            "status": "active",
            "source_type": "manual_parameter",
            "file_type": "csv",
            "version": "2025",
            "citation": f"{key} citation",
            "database_note": f"{key} database context",
            "database_precedence": [],
            "validation_rule": "Validate the registered evidence.",
            "refresh_notes": "Review a new release.",
            "data_quality": quality(score),
            "components": {
                "primary": {
                    "label": "primary",
                    "short_name": "primary",
                },
            },
        }
    return {
        "version": 3,
        "defaults": {
            "required": True,
            "component_required": True,
            "data_quality": quality(5),
        },
        "sources": sources,
    }


def composite(payload, components=(("native", "primary"), ("support", "primary"))):
    registry = SourcesConfig.model_validate(payload)
    inputs = [
        resolve_provenance(
            registry, source_key=source, component_key=component,
            transformation="fixture_input", transformation_version="1",
        )
        for source, component in components
    ]
    native = inputs[0]
    return resolve_composite_provenance(
        inputs=inputs, dataset_key="fixture",
        transformation="fixture transformation", transformation_version="1",
        governing_source_id=native.governing_source_id,
        data_quality=native.data_quality,
    )


def test_precedence_uses_selected_source_quality_without_aggregation():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = ["cost_variable"]
    context = composite(payload)

    assert context.parameter_fields("cost_variable") == {
        "data_source": "T02", "data_id": context.data_id, **quality(4),
    }
    assert context.parameter_fields() == {
        "data_source": "T01", "data_id": context.data_id, **quality(2),
    }


def test_precedence_considers_only_actual_contributors():
    payload = registry_payload()
    payload["sources"]["unused"]["database_precedence"] = ["cost_variable"]
    context = composite(payload)

    assert context.parameter_fields("cost_variable") == context.parameter_fields()
    assert {item.source_key for item in context.contributors} == {"native", "support"}


def test_precedence_is_specific_to_actual_parameter_table():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = ["cost_fixed"]
    context = composite(payload)

    assert context.parameter_fields("cost_variable") == context.parameter_fields()
    assert context.parameter_fields("cost_fixed")["data_source"] == "T02"


@pytest.mark.parametrize(("category", "source_id"), [("cars", "T01"), ("charger", "T02")])
def test_category_scope_disambiguates_vehicle_and_charger_stock(category, source_id):
    payload = registry_payload()
    payload["sources"]["native"]["database_precedence"] = [
        {"table": "existing_capacity", "categories": ["cars"]},
    ]
    payload["sources"]["support"]["database_precedence"] = [
        {"table": "existing_capacity", "categories": ["charger"]},
    ]
    context = composite(payload)

    fields = context.parameter_fields("existing_capacity", category=category)

    assert fields["data_source"] == source_id
    assert {fields[name] for name in DQ_FIELDS} == {2 if source_id == "T01" else 4}


@pytest.mark.parametrize(("suffix", "source_id"), [("_EX", "T01"), ("_N", "T02")])
def test_technology_suffix_scope_disambiguates_historical_and_new_efficiency(suffix, source_id):
    payload = registry_payload()
    for source, selected_suffix in (("native", "_EX"), ("support", "_N")):
        payload["sources"][source]["database_precedence"] = [{
            "table": "efficiency", "categories": ["cars"],
            "technology_suffixes": [selected_suffix],
        }]
    context = composite(payload)

    fields = context.parameter_fields("efficiency", category="cars", tech="car" + suffix)

    assert fields["data_source"] == source_id


def test_scoped_precedence_requires_every_supplied_selector():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = [{
        "table": "efficiency", "categories": ["cars"], "technology_suffixes": ["_N"],
    }]
    context = composite(payload)

    assert context.parameter_fields("efficiency", category="charger", tech="charger_N") == context.parameter_fields()
    assert context.parameter_fields("efficiency", category="cars", tech="car_EX") == context.parameter_fields()
    assert context.parameter_fields("efficiency", category="cars", tech="car_N")["data_source"] == "T02"


def test_two_preferred_actual_sources_fail_without_order_tiebreaking():
    payload = registry_payload()
    for source in ("native", "support"):
        payload["sources"][source]["database_precedence"] = ["cost_variable"]
    context = composite(payload)

    with pytest.raises(ProvenanceError, match="(?i)ambig|precedence|multiple"):
        context.parameter_fields("cost_variable")


def test_component_override_is_preserved_when_selecting_preferred_source():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = ["cost_variable"]
    payload["sources"]["support"]["components"]["primary"]["data_quality"] = {
        "dq_tech": 1, "dq_time": 3,
    }
    context = composite(payload)

    assert context.parameter_fields("cost_variable") == {
        "data_source": "T02", "data_id": context.data_id,
        **quality(4), "dq_tech": 1, "dq_time": 3,
    }


def test_existing_governor_retains_explicit_native_component_quality():
    payload = registry_payload()
    payload["sources"]["native"]["database_precedence"] = ["cost_variable"]
    payload["sources"]["native"]["components"]["denominator"] = {
        "label": "denominator", "short_name": "denominator", "data_quality": quality(1),
    }
    context = composite(payload, (
        ("native", "primary"), ("native", "denominator"), ("support", "primary"),
    ))

    assert context.parameter_fields("cost_variable") == context.parameter_fields()


def test_new_preferred_source_requires_unambiguous_component_quality():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = ["cost_variable"]
    payload["sources"]["support"]["components"]["secondary"] = {
        "label": "secondary", "short_name": "secondary", "data_quality": quality(1),
    }
    context = composite(payload, (
        ("native", "primary"), ("support", "primary"), ("support", "secondary"),
    ))

    with pytest.raises(ProvenanceError, match="(?i)ambig|component|quality|different"):
        context.parameter_fields("cost_variable")


def test_component_scope_selects_quality_within_new_preferred_source():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = [{
        "table": "cost_variable", "components": ["secondary"],
    }]
    payload["sources"]["support"]["components"]["secondary"] = {
        "label": "secondary", "short_name": "secondary", "data_quality": quality(1),
    }
    context = composite(payload, (
        ("native", "primary"), ("support", "primary"), ("support", "secondary"),
    ))

    assert context.parameter_fields("cost_variable") == {
        "data_source": "T02", "data_id": context.data_id, **quality(1),
    }


def test_component_precedence_override_can_opt_out_of_source_declaration():
    payload = registry_payload()
    payload["sources"]["support"]["database_precedence"] = ["cost_variable"]
    payload["sources"]["support"]["components"]["primary"]["database_precedence"] = []
    context = composite(payload)

    assert context.parameter_fields("cost_variable") == context.parameter_fields()


@pytest.mark.parametrize(("sub_category", "source_id"), [("hev", "T01"), ("diesel", "T02")])
def test_subcategory_scope_disambiguates_intercity_bus_efficiency(sub_category, source_id):
    payload = registry_payload()
    for source, selected_powertrain in (("native", "hev"), ("support", "diesel")):
        payload["sources"][source]["database_precedence"] = [{
            "table": "efficiency", "categories": ["inter_city_buses"],
            "sub_categories": [selected_powertrain],
        }]
    context = composite(payload)

    assert context.parameter_fields(
        "efficiency", category="inter_city_buses", sub_category=sub_category,
    )["data_source"] == source_id


@pytest.mark.parametrize("rule", [
    "not_a_parameter_table",
    {"table": "not_a_parameter_table"},
    {"table": "cost_variable", "categories": []},
    {"table": "cost_variable", "technology_suffixes": []},
    {"table": "cost_variable", "components": []},
    {"table": "cost_variable", "components": ["unknown_component"]},
    {"table": "cost_variable", "unknown_selector": ["cars"]},
])
def test_invalid_precedence_tables_and_scopes_fail_configuration_validation(rule):
    payload = deepcopy(registry_payload())
    payload["sources"]["native"]["database_precedence"] = [rule]

    with pytest.raises(ValidationError):
        SourcesConfig.model_validate(payload)
