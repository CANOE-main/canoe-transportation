"""Readable SQLite aliases preserve immutable provenance and caller transactions."""

from dataclasses import replace
import json
import re
import sqlite3

from canoe_schema.v4_0 import (
    Commodity, CommodityLabel, DataSet, Efficiency, ExistingCapacity, Region,
    TechnologyLabel, TimePeriod,
)
import pytest

import build_transport
from build_transport import TransportContribution, insert_transport_contribution
from utils import load_config_bundle
from validation.database_bootstrap import validate_database
from validation.insertion import insert_models, validate_parameter_rows
from validation.provenance import (
    ProvenanceError, publish_data_ids, published_content_identity,
    readable_data_id_mapping, registry_rows, resolve_composite_provenance,
    resolve_provenance,
)
from validation.schema_contract import TransportationTechnology, create_v4_schema


@pytest.fixture
def contexts():
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml")
    source = resolve_provenance(
        bundle.sources, source_key="nrcan_ceud_transport_provincial", component_key=20,
        transformation="fixture input", transformation_version="1",
    )
    return [resolve_composite_provenance(
        inputs=[source], dataset_key="efficiency.cars", transformation="fixture aggregation",
        transformation_version="1", governing_source_id=source.source_id,
        value_variant={"input_digest": str(value) * 64},
    ) for value in (1, 2)]


def efficiency(context, *, tech="T_CAR", region="ON"):
    return validate_parameter_rows(Efficiency, [{
        "region": region, "tech": tech, "vintage": 2020,
        "input_comm": "fuel", "output_comm": "service", "efficiency": 2,
    }], context)[0]


def mapping(contexts, rows, **kwargs):
    return readable_data_id_mapping(
        datasets=registry_rows(contexts)[1], contexts=contexts, batches={"efficiency": rows},
        scenario_name="notebook_diagnostics", **kwargs,
    )


def test_readable_aliases_are_deterministic_and_keep_actual_dataset_grain(contexts):
    rows = [efficiency(context) for context in contexts]
    identifiers = mapping(contexts, rows)
    assert identifiers == mapping(list(reversed(contexts)), list(reversed(rows)))
    assert set(identifiers.values()) == {
        "efficiency-cars:ON:T_CAR:T01:notebook_diagnostics",
        "efficiency-cars:ON:T_CAR:T01:notebook_diagnostics:2",
    }
    mixed = [efficiency(contexts[0]), efficiency(contexts[0], tech="T_TRUCK", region="QC")]
    assert mapping(contexts[:1], mixed)[contexts[0].data_id] == "efficiency-cars:T01:notebook_diagnostics"
    assert all(re.search(r"[0-9a-f]{16}", key) is None for key in identifiers.values())


def test_publishing_copies_all_provenance_edges_and_preserves_internal_identity(contexts):
    rows = [efficiency(contexts[0])]
    labels, datasets, sources = registry_rows(contexts[:1])
    original = rows[0].model_dump()
    identifiers = mapping(contexts[:1], rows)
    published = publish_data_ids([*labels, *datasets, *sources, *rows], identifiers,
                                 scenario_name="notebook_diagnostics")
    assert rows[0].model_dump() == original
    assert contexts[0].data_id not in {getattr(row, "data_id", None) for row in published}
    dataset = next(row for row in published if isinstance(row, DataSet))
    assert published_content_identity(dataset.notes) == contexts[0].data_id
    assert json.loads(dataset.notes)["scenario"] == "notebook_diagnostics"
    assert published[-1].data_source == rows[0].data_source == "T01"
    parent = datasets[0].model_copy(update={"data_id": "child", "parent_id": datasets[0].data_id})
    child = publish_data_ids([parent], identifiers | {"child": "child:readable"}, scenario_name="fixture")[0]
    assert child.parent_id == identifiers[datasets[0].data_id]


def test_existing_aliases_are_reused_without_redefining_caller_datasets(contexts):
    first = mapping(contexts[:1], [efficiency(contexts[0])])
    published = publish_data_ids(registry_rows(contexts[:1])[1], first, scenario_name="notebook_diagnostics")
    assert mapping(contexts[:1], [efficiency(contexts[0])], existing_datasets=published) == first
    second = mapping(contexts[1:], [efficiency(contexts[1])], existing_datasets=published)
    assert second[contexts[1].data_id] == first[contexts[0].data_id] + ":2"
    conflicting = published[0].model_copy(update={"description": "changed by caller"})
    with pytest.raises(ProvenanceError, match="Conflicting published dataset"):
        mapping(contexts[:1], [efficiency(contexts[0])], existing_datasets=[conflicting])


def test_publication_rejects_missing_or_conflicting_registry_definitions(contexts):
    datasets = registry_rows(contexts[:1])[1]
    with pytest.raises(ProvenanceError, match="Unregistered publication"):
        mapping(contexts[:1], [efficiency(contexts[1])])
    with pytest.raises(ProvenanceError, match="Conflicting publication dataset"):
        readable_data_id_mapping(
            datasets=[*datasets, datasets[0].model_copy(update={"label": "different"})],
            contexts=contexts[:1], batches={}, scenario_name="fixture",
        )
    with pytest.raises(ProvenanceError, match="Unregistered publication"):
        publish_data_ids([efficiency(contexts[0])], {}, scenario_name="fixture")


def test_scenarios_have_distinct_aliases_for_the_same_content(contexts):
    first = mapping(contexts[:1], [efficiency(contexts[0])])
    second = readable_data_id_mapping(
        datasets=registry_rows(contexts[:1])[1], contexts=contexts[:1],
        batches={"efficiency": [efficiency(contexts[0])]}, scenario_name="alternative",
    )
    assert first.keys() == second.keys()
    assert set(first.values()).isdisjoint(second.values())


@pytest.fixture
def contribution(contexts):
    dataset = DataSet(data_id="canoe-transport-template:" + "a" * 16,
                      label="Internal fixture structure", version="1")
    capacity = validate_parameter_rows(ExistingCapacity, [{
        "region": "ON", "tech": "T_CAR", "vintage": 2020,
        "capacity": 1, "units": "k vehicles",
    }], contexts[0])
    return TransportContribution(
        dataset=dataset, scenario_name="notebook_diagnostics", load_results=[],
        labels_by_table={
            "technology_label": [TechnologyLabel(tech="T_CAR")],
            "commodity_label": [CommodityLabel(commodity=name) for name in ("fuel", "service")],
        },
        rows_by_table={
            "technology": [TransportationTechnology(tech="T_CAR", flag="p", data_id=dataset.data_id)],
            "commodity": [Commodity(name=name, data_id=dataset.data_id) for name in ("fuel", "service")],
        },
        parameter_rows=capacity, efficiency_rows=[efficiency(contexts[0])],
        provenance_contexts=contexts[:1], existing_vintages=(2020,),
        prepared_support_tables=("existing_capacity", "efficiency"),
    )


def test_caller_insertion_preserves_fingerprints_foreign_keys_and_rollback(contribution):
    connection = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(connection)
    insert_models(connection, [Region(region="ON")])
    insert_models(connection, [TimePeriod(period=2020, flag="e")])
    unrelated = DataSet(data_id="caller-owned", label="Other sector")
    insert_models(connection, [unrelated])
    originals = [row.model_dump() for row in contribution.efficiency_rows]
    connection.execute("BEGIN")
    inserted = insert_transport_contribution(connection, contribution)
    assert connection.in_transaction
    insert_transport_contribution(connection, contribution, conflict="ignore_identical")
    assert originals == [row.model_dump() for row in contribution.efficiency_rows]
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert connection.execute("SELECT COUNT(*) FROM efficiency").fetchone() == (1,)
    assert connection.execute("SELECT label FROM data_set WHERE data_id='caller-owned'").fetchone() == ("Other sector",)
    validation = validate_database(connection, expected_primary_keys={}, touched_tables=list(inserted))
    assert validation["ok"], validation["errors"]
    assert all("a" * 16 not in row.data_id for row in inserted["data_set"])
    connection.rollback()
    assert connection.execute("SELECT data_id FROM data_set").fetchall() == [("caller-owned",)]
    connection.close()


def test_standalone_publication_records_actual_readable_ids(contribution, tmp_path, monkeypatch):
    # Exercise real packaged-DDL compilation and atomic publication, with bounded
    # already-prepared evidence and isolated base templates instead of shared ETL.
    bundle = load_config_bundle("config/scenarios/notebook_diagnostics.yaml")
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "region.csv").write_text("region\nON\n", encoding="utf-8")
    (templates / "time_period.csv").write_text("period,flag\n2020,e\n", encoding="utf-8")
    monkeypatch.setattr(build_transport, "prepare_transport_contribution", lambda *args, **kwargs: contribution)
    database = tmp_path / "transport.sqlite"
    report = build_transport.bootstrap_database(bundle=bundle, template_dir=templates, database_path=database)
    assert report["validation"]["ok"]
    assert report["template"]["data_id"] == "canoe-transport-template:internal:notebook_diagnostics"
    with sqlite3.connect(database) as connection:
        actual = connection.execute("SELECT data_id FROM technology").fetchone()[0]
        assert actual == report["template"]["data_id"]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    # A failed remap cannot replace the last successful SQLite.
    previous = database.read_bytes()
    invalid = replace(contribution, provenance_contexts=[])
    monkeypatch.setattr(build_transport, "prepare_transport_contribution", lambda *args, **kwargs: invalid)
    with pytest.raises(ProvenanceError, match="Unregistered publication"):
        build_transport.bootstrap_database(bundle=bundle, template_dir=templates,
                                           database_path=database, overwrite=True)
    assert database.read_bytes() == previous

