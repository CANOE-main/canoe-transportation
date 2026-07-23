from __future__ import annotations

import sqlite3
import tomllib
from pathlib import Path

import pytest
from canoe_schema import CanoeBaseModel
from canoe_schema.v4_0 import (
    DataQualityCredibilityLevel,
    ExistingCapacity,
    Technology,
)
from pydantic import ValidationError

from validation.schema_contract import (
    SCHEMA_COMMIT,
    TransportationTechnology,
    create_v4_schema,
    packaged_ddl,
    schema_evidence,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DDL_SHA256 = "cc949df14e654c0b2e549ea02ae165fe646974ddd5454c3f4ebf5a08838fb996"


def test_schema_dependency_and_ddl_are_commit_pinned() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    evidence = schema_evidence()

    assert project["tool"]["uv"]["sources"]["canoe-schema"]["rev"] == SCHEMA_COMMIT
    assert SCHEMA_COMMIT in (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    assert evidence["package_version"] == "4.0.0"
    assert evidence["ddl_sha256"] == EXPECTED_DDL_SHA256
    assert "CREATE TABLE IF NOT EXISTS technology" in packaged_ddl()


def test_package_models_enums_and_parameterized_sql_contract() -> None:
    row = ExistingCapacity(
        region="ON",
        tech="T_TEST",
        vintage=2020,
        capacity=1.0,
        data_id="dataset",
        dq_cred=DataQualityCredibilityLevel.EXCELLENT,
    )
    sql, parameters = row.to_insert_sql()
    bulk_sql, bulk_parameters = ExistingCapacity.to_bulk_insert_sql([row])

    assert isinstance(row, CanoeBaseModel)
    assert sql.startswith('INSERT INTO "existing_capacity"')
    assert "?" in sql
    assert parameters[-2:] == (1, "dataset")
    assert bulk_sql == sql
    assert bulk_parameters == [parameters]


def test_notes_extension_is_explicit_and_effective() -> None:
    with pytest.raises(ValidationError, match="notes"):
        Technology(tech="T_TEST", flag="p", data_id="dataset", notes="transport")
    row = TransportationTechnology(
        tech="T_TEST", flag="p", data_id="dataset", notes="transport"
    )
    assert row.notes == "transport"

    with sqlite3.connect(":memory:") as connection:
        preflight = create_v4_schema(connection)
        columns = {
            item[1] for item in connection.execute("PRAGMA table_info(technology)")
        }
        assert "notes" in columns
        assert preflight["technology_notes_extension_applied"] is True
        assert preflight["rates"] == {
            "global_discount_rate": 0.03,
            "default_loan_rate": 0.03,
        }
        assert all(
            values == [1, 2, 3, 4, 5]
            for values in preflight["data_quality_lookups"].values()
        )
