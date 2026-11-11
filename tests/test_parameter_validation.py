from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from canoe_schema.v4_0 import (
    DataSourceLabel,
    ExistingCapacity,
    Region,
    TimePeriod,
    TechnologyLabel,
)
from pydantic import ValidationError

from utils import load_config_bundle
from validation.database_bootstrap import validate_database
from validation.insertion import insert_models, validate_parameter_rows
from validation.provenance import registry_rows, resolve_composite_provenance, resolve_provenance
from validation.schema_contract import TransportationTechnology, create_v4_schema


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


def prepared_connection(*, composite: bool = False) -> tuple[sqlite3.Connection, object]:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    provenance = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=20,
        transformation="parameter_fixture",
        transformation_version="1",
    )
    if composite:
        fuel = resolve_provenance(
            bundle.sources, source_key="statcan_transport_tables",
            component_key="20-10-0025-01", transformation="fuel_fixture",
            transformation_version="1",
        )
        provenance = resolve_composite_provenance(
            inputs=[provenance, fuel], dataset_key="capacity_fixture",
            transformation="disaggregate", transformation_version="1",
            governing_source_id=provenance.source_id,
            data_quality=provenance.data_quality,
        )
    connection = sqlite3.connect(":memory:")
    create_v4_schema(connection)
    labels, datasets, sources = registry_rows([provenance])
    insert_models(connection, labels)
    insert_models(connection, datasets)
    insert_models(connection, sources)
    insert_models(connection, [TechnologyLabel(tech="T_TEST")])
    insert_models(
        connection,
        [TransportationTechnology(tech="T_TEST", flag="p", data_id=provenance.data_id)],
    )
    insert_models(connection, [Region(region="ON")])
    insert_models(connection, [TimePeriod(period=2020, flag="e")])
    return connection, provenance


@pytest.mark.parametrize("composite", [False, True])
def test_parameter_rows_inherit_provenance_and_insert_with_plain_sql(composite) -> None:
    connection, provenance = prepared_connection(composite=composite)
    try:
        rows = validate_parameter_rows(
            ExistingCapacity,
            [
                {
                    "region": "ON",
                    "tech": "T_TEST",
                    "vintage": 2020,
                    "capacity": 1.5,
                    "units": "PJ",
                    "notes": "Parameter-specific allocation",
                }
            ],
            provenance,
        )
        assert rows[0].data_source == provenance.source_id
        assert rows[0].dq_cred == provenance.data_quality.dq_cred
        assert insert_models(connection, rows) == 1
        assert connection.execute("SELECT notes FROM existing_capacity").fetchone() == (
            provenance.parameter_notes(),
        )
        assert "Parameter-specific allocation" not in rows[0].notes
        for contributor in provenance.contributors:
            assert rows[0].notes.count(contributor.source_key + ":") == 1
        assert dict(connection.execute(
            "SELECT s.source_id, s.notes FROM data_source s "
            "JOIN existing_capacity p ON p.data_id = s.data_id"
        )) == {item.source_id: item.notes() for item in provenance.contributors}
        assert len(provenance.contributors) == (2 if composite else 1)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        with pytest.raises(sqlite3.IntegrityError):
            insert_models(connection, rows)
    finally:
        connection.close()


def test_parameter_validation_rejects_restatement_bad_rows_and_batch_duplicates() -> None:
    connection, provenance = prepared_connection()
    try:
        with pytest.raises(ValueError, match="restates provenance"):
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": "ON",
                        "tech": "T_TEST",
                        "vintage": 2020,
                        "data_id": "manual",
                    }
                ],
                provenance,
            )
        with pytest.raises(ValidationError):
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": "ON",
                        "tech": "T_TEST",
                        "vintage": "not-a-year",
                    }
                ],
                provenance,
            )
        record = {"region": "ON", "tech": "T_TEST", "vintage": 2020}
        with pytest.raises(ValueError, match="Duplicate existing_capacity key"):
            validate_parameter_rows(
                ExistingCapacity,
                [record, record],
                provenance,
            )
    finally:
        connection.close()


def test_ignore_identical_does_not_hide_conflicting_registry_content() -> None:
    connection, _ = prepared_connection()
    try:
        assert insert_models(
            connection,
            [TechnologyLabel(tech="T_TEST")],
            conflict="ignore_identical",
        ) == 0
        with pytest.raises(ValueError, match="Conflicting existing"):
            insert_models(
                connection,
                [TechnologyLabel(tech="T_TEST", notes="different")],
                conflict="ignore_identical",
            )
    finally:
        connection.close()


def test_touched_table_audit_catches_unregistered_source_dataset_pair() -> None:
    connection, provenance = prepared_connection()
    try:
        insert_models(connection, [DataSourceLabel(source_id="T99")])
        row = ExistingCapacity(
            region="ON",
            tech="T_TEST",
            vintage=2020,
            data_source="T99",
            data_id=provenance.data_id,
        )
        insert_models(connection, [row])
        report = validate_database(
            connection,
            expected_primary_keys={"existing_capacity": [
                ("ON", "T_TEST", 2020, provenance.data_id)
            ]},
            touched_tables=["existing_capacity"],
        )
        assert report["ok"] is False
        assert report["touched_table_audit"]["existing_capacity"][
            "unregistered_source_pair"
        ] == 1
    finally:
        connection.close()
