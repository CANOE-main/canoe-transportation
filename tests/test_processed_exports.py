"""Prepared-row CSV publication shares the native build handoff, without insertion."""

import csv
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

from canoe_schema.v4_0 import (
    CapacityToActivity, Commodity, CommodityLabel, DataSet, Efficiency, ExistingCapacity, TechnologyLabel,
)
import pytest

import build_transport as build
from utils import (
    file_sha256, load_config_bundle, load_harmonization_rules,
    resolve_input_path, resolve_parameter_path,
)
from validation.insertion import insert_models, validate_parameter_rows
from validation.provenance import ProvenanceError, resolve_provenance
from validation.schema_contract import TransportationTechnology, create_v4_schema


@pytest.fixture
def prepared(tmp_path):
    bundle = load_config_bundle("config/scenarios/default.yaml")
    reference_rules = load_harmonization_rules(bundle, "vehicle_reference")
    reference_controls = [
        (filename, resolve_parameter_path(bundle, filename))
        for filename in ("rules.yaml", reference_rules["canonical_file"], reference_rules["source_links_file"])
    ]
    metadata = bundle.scenario.scenario.model_copy(update={"name": "csv_fixture"})
    scenario = bundle.scenario.model_copy(update={"scenario": metadata, "notebook_diagnostics": []})
    isolated = replace(bundle, repo_root=tmp_path, scenario=scenario)
    templates = resolve_input_path(isolated, "template")
    templates.mkdir(parents=True)
    (templates / "region.csv").write_text("region\nON\n", encoding="utf-8")
    (templates / "time_period.csv").write_text("period,flag\n2020,e\n", encoding="utf-8")
    context = resolve_provenance(
        bundle.sources, source_key="nrcan_ceud_transport_provincial", component_key=20,
        transformation="processed fixture", transformation_version="1",
    )
    dataset = DataSet(data_id="canoe-transport-template:" + "a" * 16, label="Fixture structure", version="1")
    contribution = build.TransportContribution(
        dataset=dataset, scenario_name="csv_fixture", load_results=[],
        labels_by_table={"technology_label": [TechnologyLabel(tech="T_CAR")],
                         "commodity_label": [CommodityLabel(commodity=name) for name in ("fuel", "service")]},
        rows_by_table={"technology": [TransportationTechnology(tech="T_CAR", flag="p", data_id=dataset.data_id)],
                       "commodity": [Commodity(name=name, data_id=dataset.data_id) for name in ("fuel", "service")]},
        parameter_rows=validate_parameter_rows(ExistingCapacity, [{
            "region": "ON", "tech": "T_CAR", "vintage": 2020, "capacity": 1, "units": "k vehicles",
        }], context),
        efficiency_rows=validate_parameter_rows(Efficiency, [{
            "region": "ON", "tech": "T_CAR", "vintage": 2020,
            "input_comm": "fuel", "output_comm": "service", "efficiency": 2,
        }], context),
        provenance_contexts=[context], existing_vintages=(2020,),
        prepared_support_tables=("existing_capacity", "efficiency"),
    )
    return SimpleNamespace(bundle=isolated, templates=templates, contribution=contribution,
                           reference_controls=reference_controls)


def read_models(path, model):
    with path.open(encoding="utf-8", newline="") as stream:
        return [model.model_validate({key: None if value == "" else value for key, value in row.items()})
                for row in csv.DictReader(stream)]


def payloads(rows):
    return sorted((row.model_dump(mode="json") for row in rows), key=lambda value: json.dumps(value, sort_keys=True))


def test_all_model_tables_export_before_any_parameter_insertion(prepared):
    contribution = prepared.contribution
    original = [row.model_dump() for row in contribution.efficiency_rows]
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, contribution, bundle=prepared.bundle,
                                               template_dir=prepared.templates)
        assert connection.execute("SELECT COUNT(*) FROM efficiency").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM data_set").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM region").fetchone()[0] == 0
        directory = Path(report["directory"])
        assert set(report["tables"]) == set(build.EXPORT_MODELS)
        assert report["validation"]["sqlite_insertion"] is False
        assert report["tables"]["efficiency"]["rows"] == 1
        assert report["tables"]["cost_fixed"]["rows"] == 0
        with (directory / "cost_fixed.csv").open(encoding="utf-8", newline="") as stream:
            assert next(csv.reader(stream)) == build._table_columns(connection, "cost_fixed")
        efficiency = read_models(directory / "efficiency.csv", Efficiency)[0]
        datasets = read_models(directory / "data_set.csv", DataSet)
        assert efficiency.data_id in {row.data_id for row in datasets}
        assert efficiency.data_id != contribution.efficiency_rows[0].data_id
    assert original == [row.model_dump() for row in contribution.efficiency_rows]


def test_csv_rows_equal_the_normal_sqlite_insertion_handoff(prepared):
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        build.apply_scenario_economics(connection, prepared.bundle)
        report = build.export_processed_tables(connection, prepared.contribution,
                                               bundle=prepared.bundle, template_dir=prepared.templates)
        for specification in build.STANDALONE_BASE_TABLES:
            rows, _ = build._prepare_base_table(connection, specification, prepared.templates,
                                                prepared.bundle, prepared.contribution.dataset.data_id)
            insert_models(connection, rows)
        build.insert_transport_contribution(connection, prepared.contribution)
        directory = Path(report["directory"])
        for table, model in build.EXPORT_MODELS.items():
            columns = build._table_columns(connection, table)
            actual = [model.model_validate(dict(zip(columns, row, strict=True)))
                      for row in connection.execute(f'SELECT * FROM "{table}"')]
            assert payloads(read_models(directory / f"{table}.csv", model)) == payloads(actual), table
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_capacity_to_activity_notes_keep_the_insertion_contract(prepared):
    context = prepared.contribution.provenance_contexts[0].model_copy(
        update={"database_note_keys": {"capacity_to_activity": ["tech"]}},
    )
    rows = validate_parameter_rows(CapacityToActivity, [
        {"region": region, "tech": "T_CAR", "c2a": 31.536, "notes": "Shared conversion note"}
        for region in ("ON", "QC")
    ], context)
    contribution = replace(prepared.contribution, provenance_contexts=[context], capacity_to_activity_rows=rows)
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, contribution, bundle=prepared.bundle,
                                               template_dir=prepared.templates)
    exported = read_models(Path(report["directory"]) / "capacity_to_activity.csv", CapacityToActivity)
    assert [row.notes for row in exported] == [row.notes for row in rows]


def test_export_refuses_missing_provenance_before_replacing_previous_tables(prepared):
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, prepared.contribution,
                                               bundle=prepared.bundle, template_dir=prepared.templates)
        directory = Path(report["directory"])
        previous = {path.name: file_sha256(path) for path in directory.iterdir()}
        invalid = replace(prepared.contribution, provenance_contexts=[])
        with pytest.raises(ProvenanceError, match="Unregistered publication"):
            build.export_processed_tables(connection, invalid, bundle=prepared.bundle,
                                          template_dir=prepared.templates)
        assert previous == {path.name: file_sha256(path) for path in directory.iterdir()}


@pytest.mark.parametrize("table", ["efficiency", "capacity_to_activity"])
def test_export_rejects_duplicate_parameter_keys_before_compaction_or_publication(prepared, table):
    if table == "efficiency":
        row = prepared.contribution.efficiency_rows[0]
        invalid = replace(prepared.contribution, efficiency_rows=[row, row])
    else:
        context = prepared.contribution.provenance_contexts[0]
        row = validate_parameter_rows(CapacityToActivity, [
            {"region": "ON", "tech": "T_CAR", "c2a": 31.536},
        ], context)[0]
        invalid = replace(prepared.contribution, capacity_to_activity_rows=[row, row])
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, prepared.contribution,
                                               bundle=prepared.bundle, template_dir=prepared.templates)
        directory = Path(report["directory"])
        previous = {path.name: file_sha256(path) for path in directory.iterdir()}
        with pytest.raises(ValueError, match=f"Duplicate processed {table} primary key"):
            build.export_processed_tables(connection, invalid, bundle=prepared.bundle,
                                          template_dir=prepared.templates)
        assert previous == {path.name: file_sha256(path) for path in directory.iterdir()}


def test_export_rejects_individually_registered_but_unlinked_source_and_dataset(prepared):
    # Both the source label and the internal structural dataset exist, but no
    # data_source row links this external source to that internal dataset.
    row = prepared.contribution.efficiency_rows[0].model_copy(
        update={"data_id": prepared.contribution.dataset.data_id},
    )
    invalid = replace(prepared.contribution, efficiency_rows=[row])
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, prepared.contribution,
                                               bundle=prepared.bundle, template_dir=prepared.templates)
        directory = Path(report["directory"])
        previous = {path.name: file_sha256(path) for path in directory.iterdir()}
        with pytest.raises(ValueError, match="source/dataset pair"):
            build.export_processed_tables(connection, invalid, bundle=prepared.bundle,
                                          template_dir=prepared.templates)
        assert previous == {path.name: file_sha256(path) for path in directory.iterdir()}


def test_export_restores_previous_directory_on_publication_failure(prepared, monkeypatch):
    with sqlite3.connect(":memory:") as connection:
        create_v4_schema(connection)
        report = build.export_processed_tables(connection, prepared.contribution,
                                               bundle=prepared.bundle, template_dir=prepared.templates)
        directory = Path(report["directory"])
        previous = {path.name: file_sha256(path) for path in directory.iterdir()}
        original = build.os.replace

        def fail_stage(source, destination):
            if Path(source).name == "prepared" and Path(destination) == directory:
                raise PermissionError("fixture CSV publication lock")
            return original(source, destination)

        monkeypatch.setattr(build.os, "replace", fail_stage)
        with pytest.raises(PermissionError, match="publication lock"):
            build.export_processed_tables(connection, prepared.contribution, bundle=prepared.bundle,
                                          template_dir=prepared.templates)
        assert previous == {path.name: file_sha256(path) for path in directory.iterdir()}


def test_processed_only_replays_preparation_but_never_calls_insertion_or_sqlite_publication(prepared, monkeypatch):
    bundle = prepared.bundle
    # Use the real loader against copied, immutable controls. A global loader
    # monkeypatch can leak through another module's lazy ``from ... import``.
    for filename, source in prepared.reference_controls:
        destination = resolve_parameter_path(bundle, filename)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    monkeypatch.setattr(build, "load_config_bundle", lambda *_: bundle)
    calls, connections = [], []
    monkeypatch.setattr(build, "prepare_scenario_inputs", lambda *_, **kwargs: calls.append(kwargs))

    def prepare(connection, **_kwargs):
        connections.append(connection)
        assert connection.execute("SELECT COUNT(*) FROM efficiency").fetchone()[0] == 0
        return prepared.contribution

    monkeypatch.setattr(build, "prepare_transport_contribution", prepare)

    def forbidden(*_args, **_kwargs):
        pytest.fail("Processed-only must stop before insertion/database publication")

    monkeypatch.setattr(build, "insert_models", forbidden)
    monkeypatch.setattr(build, "insert_transport_contribution", forbidden)
    monkeypatch.setattr(build, "bootstrap_database", forbidden)
    monkeypatch.setattr(build, "_publish_scenario", forbidden)
    database = bundle.repo_root / bundle.paths.outputs.sqlite / bundle.scenario.outputs.sqlite_name
    database.parent.mkdir(parents=True)
    database.write_bytes(b"preserve configured SQLite")
    validation = bundle.repo_root / bundle.scenario.outputs.validation_report
    validation.parent.mkdir(parents=True)
    validation.write_bytes(b"preserve normal SQLite validation report")
    report, manifest = build.build_from_scenario(bundle.scenario_path, processed_only=True)
    assert report["ok"] and manifest.name == "manifest.json"
    assert calls == [{"download": False}]
    assert database.read_bytes() == b"preserve configured SQLite"
    assert validation.read_bytes() == b"preserve normal SQLite validation report"
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def test_normal_build_can_export_the_same_prepared_rows_as_a_byproduct(prepared, monkeypatch, tmp_path):
    monkeypatch.setattr(build, "prepare_transport_contribution", lambda *_args, **_kwargs: prepared.contribution)
    database = tmp_path / "fixture.sqlite"
    report = build.bootstrap_database(bundle=prepared.bundle, template_dir=prepared.templates,
                                      database_path=database, export_processed=True)
    assert report["validation"]["ok"]
    assert report["processed_tables"]["tables"]["efficiency"]["rows"] == 1
    assert database.exists()


def test_cli_exposes_csv_stop_and_optional_normal_build_export(monkeypatch):
    monkeypatch.setattr("sys.argv", ["build_transport.py", "--processed-only", "--export-processed"])
    args = build.parse_args()
    assert args.processed_only and args.export_processed
