"""Validated, parameterized insertion for homogeneous CANOE v4 model batches."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from math import isfinite
from typing import Any, Literal, TypeVar

from canoe_schema import CanoeBaseModel
from pydantic import TypeAdapter

from validation.provenance import ResolvedProvenance


ModelT = TypeVar("ModelT", bound=CanoeBaseModel)
ConflictPolicy = Literal["error", "ignore_identical"]


def cleanup_transport_parameter_batches(
    batches: Mapping[str, Sequence[CanoeBaseModel]],
    *,
    existing_periods: Sequence[int],
    first_model_period: int,
    epsilon: float,
) -> tuple[dict[str, list[CanoeBaseModel]], list[dict[str, Any]]]:
    """Prune negligible existing capacity and its unsupported historical rows.

    Support keys include region, technology and vintage. Future-vintage and
    technology-only parameters are retained; they can support new investments.
    The function never mutates a caller-owned database or input batch.
    """
    if not isfinite(epsilon) or epsilon < 0:
        raise ValueError(
            "Existing-capacity cleanup epsilon must be finite and non-negative"
        )
    capacity_rows = list(batches.get("existing_capacity", ()))
    if any(
        not isfinite(float(row.capacity)) or row.capacity < 0 for row in capacity_rows
    ):
        raise ValueError("Existing-capacity cleanup found invalid capacity")
    historical = set(int(period) for period in existing_periods)
    if not historical or first_model_period <= max(historical):
        raise ValueError("The first model period must follow existing periods")
    existing_techs = {str(row.tech) for row in capacity_rows}
    retained_capacity = [
        row for row in capacity_rows if row.capacity > 0 and row.capacity >= epsilon
    ]
    active_keys = {
        (str(row.region), str(row.tech), int(row.vintage)) for row in retained_capacity
    }
    removed: list[dict[str, Any]] = [
        {
            "table": "existing_capacity",
            "region": row.region,
            "tech": row.tech,
            "vintage": row.vintage,
            "value": row.capacity,
            "units": row.units,
            "reason": "below_cleanup_epsilon",
        }
        for row in capacity_rows
        if row.capacity <= 0 or row.capacity < epsilon
    ]
    result = {name: list(rows) for name, rows in batches.items()}
    result["existing_capacity"] = retained_capacity
    capacity_gated = {
        "efficiency",
        "cost_variable",
        "cost_fixed",
        "cost_invest",
        "emission_activity",
    }
    efficiency_gated = {
        "cost_variable",
        "cost_fixed",
        "cost_invest",
        "emission_activity",
    }
    efficiency_present = "efficiency" in batches
    valid_efficiency_keys = {
        (str(row.region), str(row.tech), int(row.vintage))
        for row in batches.get("efficiency", ())
        if (str(row.region), str(row.tech), int(row.vintage)) in active_keys
    }
    for table in sorted(capacity_gated | efficiency_gated):
        if table not in batches:
            continue
        kept: list[CanoeBaseModel] = []
        for row in batches[table]:
            key = (str(row.region), str(row.tech), int(row.vintage))
            is_historical_existing = (
                row.tech in existing_techs
                and int(row.vintage) in historical
                and int(row.vintage) < first_model_period
            )
            reason: str | None = None
            if (
                is_historical_existing
                and table in capacity_gated
                and key not in active_keys
            ):
                reason = "missing_active_existing_capacity"
            elif (
                is_historical_existing
                and table in efficiency_gated
                and efficiency_present
                and key not in valid_efficiency_keys
            ):
                reason = "missing_historical_efficiency"
            if reason is None:
                kept.append(row)
            else:
                removed.append(
                    {
                        "table": table,
                        "region": row.region,
                        "tech": row.tech,
                        "vintage": row.vintage,
                        "reason": reason,
                    }
                )
        result[table] = kept
    return result, removed


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
            raise ValueError(
                f"Duplicate {row.table_name()} key in batch at row {index}: {key}"
            )
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
    if not issubclass(row_type, CanoeBaseModel) or any(
        type(row) is not row_type for row in rows
    ):
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
