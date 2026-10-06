"""Validated, parameterized insertion for homogeneous CANOE v4 model batches."""

from __future__ import annotations

import sqlite3
import logging
from collections.abc import Mapping, Sequence
from math import isfinite
from typing import Any, Literal, TypeVar, TYPE_CHECKING

from canoe_schema import CanoeBaseModel
from pydantic import TypeAdapter

from validation.provenance import ResolvedProvenance


ModelT = TypeVar("ModelT", bound=CanoeBaseModel)
ConflictPolicy = Literal["error", "ignore_identical"]

if TYPE_CHECKING:
    from parameterization.ldv_charging_profiles import ChargingPreparation
    from parameterization.ldv_ev_ranges import RangePreparation


def replace_owned_ldv_rows(
    connection: sqlite3.Connection,
    *,
    charging: ChargingPreparation | None,
    ranges: RangePreparation | None,
) -> None:
    """Replace only this slice's registered rows in caller-selected regions.

    Called inside the caller's transaction. No schema creation, commits, shared-axis
    rewrites or deletion of other sectors' datasets. Conflicts are checked before
    deleting this slice's prior rows. Dataset identity plus transformation establish
    ownership, rather than technology/region names alone.
    """
    specifications = []
    if charging is not None:
        specifications.append(
            (
                "capacity_factor_tech",
                charging.regions,
                charging.rows,
                "tech",
                charging.audit["target_technologies"],
                "legacy-charging-profiles-%",
                "Legacy hourly charging mean/peak and explicit temporal projection v1;%",
            )
        )
    if ranges is not None:
        specifications.append(
            (
                "limit_new_capacity_share",
                ranges.regions,
                ranges.share_rows,
                "super_group",
                ranges.audit["scope_super_groups"],
                "ldv-range-minimum-new-capacity-shares:%",
                "Within-class/powertrain minimum new capacity shares v1",
            )
        )
    pending = []
    for (
        table,
        regions,
        rows,
        scope_column,
        scope_values,
        identifier,
        description,
    ) in specifications:
        owned = {
            r[0]
            for r in connection.execute(
                "SELECT data_id FROM data_set WHERE data_id LIKE ? AND description LIKE ?",
                (identifier, description),
            )
        }
        for row in rows:
            payload = row.model_dump()
            business_key = [
                column
                for column in row.__primary_key__
                if column not in {"data_id", "operator"}
            ]
            clause = " AND ".join(f'"{column}"=?' for column in business_key)
            existing = connection.execute(
                f'SELECT data_id FROM "{table}" WHERE {clause}',
                tuple(payload[c] for c in business_key),
            ).fetchall()
            if any(value[0] not in owned for value in existing):
                raise ValueError(
                    f"Refusing to replace unrelated/shared {table} row for {tuple(payload[c] for c in business_key)}"
                )
        pending.append((table, regions, owned, scope_column, scope_values))
    for table, regions, owned, scope_column, scope_values in pending:
        removed = 0
        for region in regions:
            for data_id in owned:
                for scope_value in scope_values:
                    removed += connection.execute(
                        f'DELETE FROM "{table}" WHERE region=? AND data_id=? AND "{scope_column}"=?',
                        (region, data_id, scope_value),
                    ).rowcount
        logging.getLogger(__name__).info(
            "Scoped %s replacement removed %s owned rows in %s", table, removed, regions
        )
    if ranges is None:
        return
    # Keep groups referenced by any caller table, or containing caller-owned members.
    candidates = connection.execute(
        "SELECT group_name, data_id FROM tech_group WHERE data_id IN "
        "(SELECT data_id FROM data_set WHERE data_id LIKE 'canoe-transport-range-groups:%' "
        "AND label='Internal transport range group structure')"
    ).fetchall()
    tables = [
        r[0]
        for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    references = [
        (table, r[3])
        for table in tables
        if table not in {"tech_group", "tech_group_member"}
        for r in connection.execute(f'PRAGMA foreign_key_list("{table}")')
        if r[2] == "tech_group_label"
    ]
    # v4 share table does not declare FK links for these logical group references.
    references.extend(
        [
            ("limit_new_capacity_share", "sub_group"),
            ("limit_new_capacity_share", "super_group"),
        ]
    )
    for group, data_id in candidates:
        if group not in ranges.audit["scope_group_names"]:
            continue
        used = any(
            connection.execute(
                f'SELECT 1 FROM "{table}" WHERE "{column}"=? LIMIT 1', (group,)
            ).fetchone()
            for table, column in references
        )
        shared_member = connection.execute(
            "SELECT 1 FROM tech_group_member WHERE group_name=? AND data_id<>? LIMIT 1",
            (group, data_id),
        ).fetchone()
        if not used and not shared_member:
            connection.execute(
                "DELETE FROM tech_group_member WHERE group_name=? AND data_id=?",
                (group, data_id),
            )
            connection.execute(
                "DELETE FROM tech_group WHERE group_name=? AND data_id=?",
                (group, data_id),
            )
            if not connection.execute(
                "SELECT 1 FROM tech_group WHERE group_name=?", (group,)
            ).fetchone():
                connection.execute(
                    "DELETE FROM tech_group_label WHERE group_name=?", (group,)
                )


def replace_owned_ldv_representation(
    connection: sqlite3.Connection,
    *,
    ranges: RangePreparation | None,
    batches: Mapping[str, Sequence[CanoeBaseModel]],
    contexts: Sequence[ResolvedProvenance],
    internal_datasets: Sequence[CanoeBaseModel],
    check_only: bool = False,
) -> None:
    """Switch investable LDV representations within selected regions and ownership.

    Incoming prerequisite IDs and the established representative namespace identify
    owned rows. Unrelated variant parameters block replacement; unrelated rows,
    shared structures used elsewhere and every EX stock row are protected.
    """
    if ranges is None:
        return
    representatives = set(ranges.audit["scope_representative_technologies"])
    variants = set(ranges.audit["scope_variant_technologies"])
    replacing_variants = ranges.audit["mode"] == "representative_archetype"
    scope = representatives | (variants if replacing_variants else set())
    owned = {r.data_id for r in [*contexts, *internal_datasets]}
    owned.update(
        r[0]
        for r in connection.execute(
            "SELECT data_id FROM data_set WHERE "
            "(data_id LIKE 'ldv-ev-representative-%' AND description LIKE 'LDV representative % aggregation v1') OR "
            "(data_id LIKE 'canoe-transport-ev-representatives:%' AND label='Internal transport LDV representative structure')"
        )
    )
    tables = (
        "capacity_to_activity",
        "limit_annual_capacity_factor",
        "lifetime_tech",
        "lifetime_survival_curve",
        "efficiency",
        "limit_tech_input_split",
        "cost_invest",
        "cost_fixed",
        "cost_variable",
        "emission_embodied",
    )
    for table in tables:
        column = "tech_or_group" if table == "limit_annual_capacity_factor" else "tech"
        for region in ranges.regions:
            if replacing_variants:
                for tech in variants:
                    existing = connection.execute(
                        f'SELECT data_id FROM "{table}" WHERE region=? AND "{column}"=?',
                        (region, tech),
                    )
                    if any(r[0] not in owned for r in existing):
                        raise ValueError(
                            f"Refusing to replace caller-owned {table} variant rows for {region}/{tech}"
                        )
            for row in batches.get(table, ()):
                if getattr(row, column) not in scope or row.region != region:
                    continue
                key = [
                    k for k in row.__primary_key__ if k not in {"data_id", "operator"}
                ]
                clause = " AND ".join(f'"{k}"=?' for k in key)
                existing = connection.execute(
                    f'SELECT data_id FROM "{table}" WHERE {clause}',
                    tuple(getattr(row, k) for k in key),
                )
                if any(r[0] not in owned for r in existing):
                    raise ValueError(
                        f"Refusing to replace unrelated/shared representative {table} row"
                    )
    if replacing_variants:
        for region in ranges.regions:
            for tech in variants:
                if connection.execute(
                    "SELECT 1 FROM existing_capacity WHERE region=? AND tech=? LIMIT 1",
                    (region, tech),
                ).fetchone():
                    raise ValueError(
                        "Representative replacement cannot redistribute caller stock on investable variants"
                    )
        groups = connection.execute(
            "SELECT group_name,data_id FROM tech_group_member WHERE tech IN ("
            + ",".join("?" for _ in variants)
            + ")",
            tuple(sorted(variants)),
        ).fetchall()
        own_groups = set(ranges.audit["scope_group_names"])
        owned_group_ids = {
            r[0]
            for r in connection.execute(
                "SELECT data_id FROM data_set WHERE data_id LIKE 'canoe-transport-range-groups:%' "
                "AND label='Internal transport range group structure'"
            )
        }
        if any(
            group not in own_groups or data_id not in owned_group_ids
            for group, data_id in groups
        ):
            raise ValueError("Caller-owned group refers to replaced LDV range variants")
        owned_share_ids = {
            r[0]
            for r in connection.execute(
                "SELECT data_id FROM data_set WHERE data_id LIKE 'ldv-range-minimum-new-capacity-shares:%' "
                "AND description='Within-class/powertrain minimum new capacity shares v1'"
            )
        }
        for group, _ in groups:
            for region in ranges.regions:
                references = connection.execute(
                    "SELECT data_id FROM limit_new_capacity_share WHERE region=? AND (sub_group=? OR super_group=?)",
                    (region, group, group),
                )
                if any(r[0] not in owned_share_ids for r in references):
                    raise ValueError(
                        "Caller-owned constraint refers to replaced LDV range variants"
                    )
    if check_only:
        return
    for table in tables:
        column = "tech_or_group" if table == "limit_annual_capacity_factor" else "tech"
        removed = 0
        for region in ranges.regions:
            for tech in sorted(scope):
                for (data_id,) in connection.execute(
                    f'SELECT DISTINCT data_id FROM "{table}" WHERE region=? AND "{column}"=?',
                    (region, tech),
                ).fetchall():
                    if data_id in owned:
                        removed += connection.execute(
                            f'DELETE FROM "{table}" WHERE region=? AND "{column}"=? AND data_id=?',
                            (region, tech, data_id),
                        ).rowcount
        logging.getLogger(__name__).info(
            "LDV representation replacement removed %s owned %s rows", removed, table
        )
    # Structural rows may be shared across regions. Remove them only when no other
    # FK/logical reference remains, then let the incoming contribution insert its rows.
    database_tables = [
        r[0]
        for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    for table, label_table, field, labels in (
        ("technology", "technology_label", "tech", scope),
        (
            "commodity",
            "commodity_label",
            "name",
            set(ranges.audit["scope_representative_commodities"]),
        ),
    ):
        references = [
            (t, r[3])
            for t in database_tables
            if t not in {table, label_table}
            for r in connection.execute(f'PRAGMA foreign_key_list("{t}")')
            if r[2] == label_table
        ]
        if table == "technology":
            references.append(("limit_annual_capacity_factor", "tech_or_group"))
        label_field = "commodity" if table == "commodity" else field
        for label in sorted(labels):
            used = any(
                connection.execute(
                    f'SELECT 1 FROM "{t}" WHERE "{col}"=? LIMIT 1', (label,)
                ).fetchone()
                for t, col in references
            )
            if used:
                continue
            for (data_id,) in connection.execute(
                f'SELECT data_id FROM "{table}" WHERE "{field}"=?', (label,)
            ).fetchall():
                if data_id in owned:
                    connection.execute(
                        f'DELETE FROM "{table}" WHERE "{field}"=? AND data_id=?',
                        (label, data_id),
                    )
            if not connection.execute(
                f'SELECT 1 FROM "{table}" WHERE "{field}"=? LIMIT 1', (label,)
            ).fetchone():
                connection.execute(
                    f'DELETE FROM "{label_table}" WHERE "{label_field}"=?', (label,)
                )


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
            "reason": "below_cleanup_tolerance",
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
                int(row.vintage) in historical
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


def validate_transport_parameter_support(
    batches: Mapping[str, Sequence[CanoeBaseModel]], *, existing_vintages: Sequence[int],
    new_technologies: Sequence[str] = (),
) -> dict[str, Any]:
    """Check active transport keys that SQLite foreign keys cannot express.

    An explicitly prepared empty dependency is checked; an omitted development
    layer is outside this audit. Lifetime/scaling defaults do not activate a
    technology and may remain without stock or efficiency. No rows are mutated.
    """
    historical = set(existing_vintages)
    keys = {
        table: {(row.region, row.tech, row.vintage) for row in rows}
        for table, rows in batches.items() if table != "limit_annual_capacity_factor"
    }
    checks: dict[str, Any] = {}
    errors: list[str] = []

    def record(name: str, checked: set[tuple], supported: set[tuple]) -> None:
        missing = checked - supported
        checks[name] = {
            "checked_keys": len(checked), "unsupported_keys": len(missing),
            "examples": sorted(missing)[:15],
        }
        if missing:
            errors.append(f"{name}: {len(missing)} unsupported keys {sorted(missing)[:8]}")

    if "existing_capacity" in keys:
        invalid = [
            (row.region, row.tech, row.vintage, row.capacity)
            for row in batches["existing_capacity"]
            if not isfinite(float(row.capacity)) or row.capacity <= 0
        ]
        checks["existing_capacity_positive"] = {
            "checked_rows": len(batches["existing_capacity"]),
            "invalid_rows": len(invalid), "examples": invalid[:15],
        }
        if invalid:
            errors.append(f"existing_capacity_positive: {len(invalid)} invalid rows {invalid[:8]}")
        for table in ("efficiency", "cost_invest", "cost_fixed", "cost_variable", "emission_embodied"):
            if table in keys:
                record(f"{table}_historical_capacity",
                       {key for key in keys[table] if key[2] in historical},
                       keys["existing_capacity"])
    if "efficiency" in keys:
        for table in ("existing_capacity", "cost_invest", "cost_fixed", "cost_variable", "emission_embodied"):
            if table in keys:
                record(f"{table}_efficiency", keys[table], keys["efficiency"])
        if "limit_annual_capacity_factor" in batches:
            new_techs = set(new_technologies)
            record("new_capacity_factor_efficiency", {
                (row.region, row.tech_or_group, row.vintage)
                for row in batches["limit_annual_capacity_factor"]
                if row.tech_or_group in new_techs
            }, keys["efficiency"])
    return {"ok": not errors, "errors": errors, "checks": checks,
            "prepared_tables": sorted(batches)}


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
