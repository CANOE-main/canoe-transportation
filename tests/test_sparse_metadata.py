"""Sparse registry notes preserve actual treatment changes and row provenance."""

from pathlib import Path

import pytest
from canoe_schema.v4_0 import Demand, ExistingCapacity
from pydantic import ValidationError

from utils import load_config_bundle
from validation.config_models import SourcesConfig
from validation.insertion import (
    cleanup_transport_parameter_batches, compact_parameter_notes, validate_parameter_rows,
)
from validation.provenance import (
    ProvenanceError, resolve_composite_provenance, resolve_provenance, source_id_mapping,
)


ROOT = Path(__file__).resolve().parents[1]


def context():
    sources = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT).sources
    return resolve_provenance(
        sources, source_key="nrcan_ceud_transport_provincial", component_key=20,
        transformation="note_fixture", transformation_version="1",
    )


def records():
    return [
        {"region": region, "period": period, "commodity": commodity,
         "demand": 2.0, "units": "bn passenger-km"}
        for region in ("ON", "AB") for period in (2030, 2025)
        for commodity in ("car_service", "other_service")
    ]


def test_notes_are_sparse_order_independent_and_dq_remains_on_every_row():
    provenance = context()
    rows = validate_parameter_rows(Demand, records(), provenance)
    reversed_rows = validate_parameter_rows(Demand, list(reversed(records())), provenance)
    def identity(row):
        return row.region, row.period, row.commodity
    assert {identity(row): row.notes for row in rows} == {
        identity(row): row.notes for row in reversed_rows
    }
    annotated = [row for row in rows if row.notes]
    assert {(row.region, row.period) for row in annotated} == {("AB", 2025), ("ON", 2025)}
    assert len(annotated) == 4
    for row in rows:
        assert row.data_source == provenance.source_id
        assert row.demand == 2.0
        assert row.dq_time == 1


def test_special_period_treatment_retains_source_owned_caveat():
    provenance = context()
    payload = records()
    payload[0]["database_note_variants"] = ("car_extrapolation",)
    payload[1]["notes"] = "Unregistered artifact prose must not be promoted"
    rows = validate_parameter_rows(Demand, payload, provenance)
    assert len([row for row in rows if row.notes]) == 5
    special = next(row for row in rows if row.region == "ON" and row.period == 2030
                   and row.commodity == "car_service")
    assert "20.car_extrapolation:" in special.notes
    assert all("Unregistered artifact prose" not in (row.notes or "") for row in rows)
    assert compact_parameter_notes(rows, ["commodity", "region"]) == rows


def test_unknown_note_variant_fails_before_row_construction():
    payload = records()
    payload[0]["database_note_variants"] = ("not_registered",)
    with pytest.raises(ProvenanceError, match="Unknown or ambiguous"):
        validate_parameter_rows(Demand, payload, context())


def test_sparse_capacity_note_survives_removal_of_the_first_vintage():
    provenance = context()
    rows = validate_parameter_rows(
        ExistingCapacity,
        [{"region": "ON", "tech": "car", "vintage": vintage,
          "capacity": capacity, "units": "k vehicles"}
         for vintage, capacity in [(2000, 0.00001), (2010, 1.0), (2020, 2.0)]],
        provenance, compact_notes=False,
    )
    cleaned, removed = cleanup_transport_parameter_batches(
        {"existing_capacity": rows}, existing_periods=[2000, 2010, 2020],
        first_model_period=2025, epsilon=0.001,
    )
    retained = compact_parameter_notes(cleaned["existing_capacity"], ["tech"])
    assert len(removed) == 1
    assert [(row.vintage, row.capacity) for row in retained] == [(2010, 1.0), (2020, 2.0)]
    assert retained[0].notes == provenance.parameter_notes()
    assert retained[1].notes is None


def test_explicit_ids_survive_reordering_and_retirement():
    sources = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT).sources
    original = source_id_mapping(sources)
    payload = sources.model_dump(mode="python")
    payload["sources"] = dict(reversed(list(payload["sources"].items())))
    payload["sources"].pop("nrcan_ceud_transport_national")
    changed = source_id_mapping(SourcesConfig.model_validate(payload))
    assert changed == {key: value for key, value in original.items() if key != "nrcan_ceud_transport_national"}


def test_source_and_component_notes_are_both_labelled_once():
    provenance = context()
    source = provenance.contributors[0]
    note = provenance.parameter_notes()
    assert note.startswith(f"{source.source_key}: {source.database_note}\n")
    assert note.count("  - 20:") == 1


@pytest.mark.parametrize("keys", [["tech"], [], ["sub_group", "sub_group"]])
def test_note_placement_keys_match_the_actual_schema_contract(keys):
    sources = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT).sources
    payload = sources.model_dump(mode="python")
    payload["defaults"]["database_note_keys"]["limit_new_capacity_share"] = keys
    with pytest.raises(ValidationError, match="database_note_keys"):
        SourcesConfig.model_validate(payload)


@pytest.mark.parametrize("sub_category,component", [
    ("gasoline", "vehicles"), ("phev_35_miles", "phev_vehicle_inputs"),
])
def test_atb_precedence_uses_the_actual_consumption_component(sub_category, component):
    sources = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT).sources
    inputs = [resolve_provenance(
        sources, source_key=source, component_key=part,
        transformation="fixture", transformation_version="1",
    ) for source, part in [
        ("nrcan_ceud_transport_provincial", 20),
        ("nlr_atb_transportation", "vehicles"),
        ("nlr_atb_transportation", "phev_vehicle_inputs"),
    ]]
    combined = resolve_composite_provenance(
        inputs=inputs, dataset_key="fixture", transformation="fixture",
        transformation_version="1", governing_source_id="T01",
        data_quality=inputs[0].data_quality,
    )
    fields = combined.parameter_fields(
        "efficiency", category="cars", sub_category=sub_category, tech="car_N",
    )
    assert fields["data_source"] == "T06"
    assert {field: fields[field] for field in type(inputs[0].data_quality).model_fields} == (
        sources.resolved_data_quality("nlr_atb_transportation", component).row_fields()
    )
