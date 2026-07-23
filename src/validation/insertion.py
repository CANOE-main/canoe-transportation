"""Validated, parameterized insertion for homogeneous CANOE v4 model batches."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar

from canoe_schema import CanoeBaseModel
from pydantic import TypeAdapter

from validation.provenance import ResolvedProvenance


ModelT = TypeVar("ModelT", bound=CanoeBaseModel)
ConflictPolicy = Literal["error", "ignore_identical"]


def validate_parameter_rows(
    model: type[ModelT],
    records: Sequence[Mapping[str, Any]],
    provenance: ResolvedProvenance,
) -> list[ModelT]:
    """Attach one resolved provenance context and construct package row models."""
    protected = set(provenance.parameter_fields())
    payloads: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        conflicting = protected.intersection(record)
        if conflicting:
            raise ValueError(
                f"Parameter record {index} restates provenance fields: "
                f"{sorted(conflicting)}"
            )
        payloads.append({**record, **provenance.parameter_fields()})
    rows = TypeAdapter(list[model]).validate_python(payloads)
    seen: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        payload = row.model_dump(mode="python")
        key = tuple(payload[field] for field in row.__primary_key__)
        if key in seen:
            raise ValueError(f"Duplicate {row.table_name()} key in batch at row {index}: {key}")
        seen.add(key)
    return rows


def _existing_row_matches(
    connection: sqlite3.Connection,
    row: CanoeBaseModel,
) -> bool | None:
    payload = row._dump_for_sql(include_nulls=True, include_defaults=True)
    key_values = [payload[field] for field in row.__primary_key__]
    quoted_columns = ", ".join(row._quote_identifier(field) for field in payload)
    where = " AND ".join(
        f"{row._quote_identifier(field)} = ?" for field in row.__primary_key__
    )
    actual = connection.execute(
        f"SELECT {quoted_columns} FROM {row._quote_identifier(row.table_name())} "
        f"WHERE {where}",
        tuple(row._coerce_sql_value(value) for value in key_values),
    ).fetchone()
    if actual is None:
        return None
    expected = tuple(row._coerce_sql_value(value) for value in payload.values())
    return tuple(actual) == expected


def insert_models(
    connection: sqlite3.Connection,
    rows: Sequence[ModelT],
    *,
    conflict: ConflictPolicy = "error",
) -> int:
    """Insert a non-empty homogeneous batch using package-generated SQL."""
    if not rows:
        raise ValueError("rows cannot be empty")
    row_type = type(rows[0])
    if not issubclass(row_type, CanoeBaseModel) or any(type(row) is not row_type for row in rows):
        raise TypeError("rows must be a homogeneous CanoeBaseModel sequence")

    pending = list(rows)
    if conflict == "ignore_identical":
        filtered: list[ModelT] = []
        for row in pending:
            matches = _existing_row_matches(connection, row)
            if matches is False:
                key = tuple(getattr(row, field) for field in row.__primary_key__)
                raise ValueError(
                    f"Conflicting existing {row.table_name()} definition for {key}"
                )
            if matches is None:
                filtered.append(row)
        pending = filtered
    elif conflict != "error":
        raise ValueError(f"Unknown conflict policy: {conflict}")

    if not pending:
        return 0
    sql, parameters = row_type.to_bulk_insert_sql(
        pending,
        include_nulls=True,
        include_defaults=True,
        parameterized=True,
    )
    connection.executemany(sql, parameters)
    return len(pending)
