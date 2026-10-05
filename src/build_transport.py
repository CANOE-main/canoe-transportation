"""Build an atomic CANOE v4 transportation database from validated templates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field as dataclass_field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from canoe_schema import CanoeBaseModel
from canoe_schema.v4_0 import (
    Commodity,
    CommodityLabel,
    DataSet,
    Region,
    TechnologyLabel,
    TimePeriod,
    ExistingCapacity,
    Demand,
    CapacityToActivity,
    LimitAnnualCapacityFactor,
    LifetimeTech,
    LifetimeSurvivalCurve,
    Efficiency,
    LimitTechInputSplit,
    CostInvest,
    CostFixed,
    CostVariable,
    EmissionEmbodied,
)
from pydantic import ValidationError
from pydantic_core import PydanticUndefined

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_configured_path,
    resolve_input_path,
    resolve_repo_path,
)
from validation.database_bootstrap import validate_database
from validation.insertion import ConflictPolicy, insert_models, validate_transport_parameter_support
from validation.legacy_compare import compare_legacy_costs, compare_legacy_demand, compare_legacy_efficiency, compare_legacy_existing_capacity, compare_legacy_lifetime_tech, compare_legacy_tables, compare_legacy_emission_embodied
from validation.provenance import (
    ResolvedProvenance,
    registry_rows,
    source_id_mapping,
)
from validation.schema_contract import (
    TransportationTechnology,
    create_v4_schema,
    schema_evidence,
)
from validation.sqlite_compare import compare_scenario_databases, validate_scenario_comparison_reference
from validation.sqlite_utils import quote_identifier


LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from parameterization.road_embodied_emissions import EmbodiedEmissionPreparation


@dataclass(frozen=True)
class TemplateTable:
    """One explicitly supported, v4-model-backed bootstrap template."""

    table: str
    filename: str
    model: type[CanoeBaseModel]
    label_model: type[CanoeBaseModel] | None = None
    label_field: str | None = None


@dataclass(frozen=True)
class TableLoadResult:
    """Auditable result of loading one CSV template."""

    table: str
    template: str
    source_encoding: str
    source_rows: int
    inserted_rows: int
    csv_columns: list[str]
    target_columns: list[str]
    inserted_columns: list[str]
    ignored_fields: list[str]
    missing_optional_fields: list[str]
    schema_defaults_used: dict[str, int]
    blank_values_as_null: dict[str, int]
    primary_key_columns: list[str]
    primary_keys: list[tuple[Any, ...]]


@dataclass(frozen=True)
class TransportContribution:
    """Validated transport-owned rows prepared for a compatible caller connection."""

    dataset: DataSet
    labels_by_table: dict[str, list[CanoeBaseModel]]
    rows_by_table: dict[str, list[CanoeBaseModel]]
    load_results: list[TableLoadResult]
    parameter_rows: list[ExistingCapacity] = dataclass_field(default_factory=list)
    demand_rows: list[Demand] = dataclass_field(default_factory=list)
    road_assumption_dataset: DataSet | None = None
    capacity_to_activity_rows: list[CapacityToActivity] = dataclass_field(default_factory=list)
    flat_road_factor_rows: list[LimitAnnualCapacityFactor] = dataclass_field(default_factory=list)
    lifetime_tech_rows: list[LifetimeTech] = dataclass_field(default_factory=list)
    lifetime_survival_curve_rows: list[LifetimeSurvivalCurve] = dataclass_field(default_factory=list)
    efficiency_rows: list[Efficiency] = dataclass_field(default_factory=list)
    input_split_rows: list[LimitTechInputSplit] = dataclass_field(default_factory=list)
    cost_invest_rows: list[CostInvest] = dataclass_field(default_factory=list)
    cost_fixed_rows: list[CostFixed] = dataclass_field(default_factory=list)
    cost_variable_rows: list[CostVariable] = dataclass_field(default_factory=list)
    charger_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    efficiency_assumption_dataset: DataSet | None = None
    provenance_contexts: list[ResolvedProvenance] = dataclass_field(default_factory=list)
    parameter_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    demand_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    road_utilization_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    lifetime_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    efficiency_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    cost_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    existing_vintages: tuple[int, ...] = ()
    prepared_support_tables: tuple[str, ...] = ()
    support_audit: dict[str, Any] = dataclass_field(default_factory=dict)
    new_technologies: tuple[str, ...] = ()
    emission_embodied: EmbodiedEmissionPreparation | None = None

    @property
    def emission_embodied_rows(self) -> list[EmissionEmbodied]:
        return self.emission_embodied.rows if self.emission_embodied is not None else []

    @property
    def emission_embodied_audit(self) -> dict[str, Any]:
        return self.emission_embodied.audit if self.emission_embodied is not None else {}


TEMPLATE_TABLES = (
    TemplateTable(
        table="technology",
        filename="technology.csv",
        model=TransportationTechnology,
        label_model=TechnologyLabel,
        label_field="tech",
    ),
    TemplateTable(
        table="commodity",
        filename="commodity.csv",
        model=Commodity,
        label_model=CommodityLabel,
        label_field="name",
    ),
)

STANDALONE_BASE_TABLES = (
    TemplateTable(table="region", filename="region.csv", model=Region),
    TemplateTable(table="time_period", filename="time_period.csv", model=TimePeriod),
)


class TemplateLoadError(ValueError):
    """Raised when a template cannot be mapped safely to its v4 row model."""


def _read_template(path: Path) -> tuple[list[str], list[dict[str, str | None]], str]:
    if not path.is_file():
        raise FileNotFoundError(f"Template CSV does not exist: {path}")
    if path.stat().st_size <= 0:
        raise TemplateLoadError(f"Template CSV is empty: {path}")
    fields: list[str] | None = None
    rows: list[dict[str, str | None]] = []
    selected_encoding = ""
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames
                rows = list(reader)
        except UnicodeDecodeError:
            continue
        selected_encoding = encoding
        break
    if fields is None or not fields:
        raise TemplateLoadError(f"Template CSV has no readable header: {path}")
    duplicates = sorted(field for field, count in Counter(fields).items() if count > 1)
    if duplicates:
        raise TemplateLoadError(f"Template CSV has duplicate fields {duplicates}: {path}")
    if not rows:
        raise TemplateLoadError(f"Template CSV has no data rows: {path}")
    for row_number, row in enumerate(rows, start=2):
        if None in row:
            raise TemplateLoadError(f"Template CSV row {row_number} has extra values: {path}")
    return fields, rows, selected_encoding


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    columns = [
        str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({quote_identifier(table)})"
        ).fetchall()
    ]
    if not columns:
        raise TemplateLoadError(f"Target schema does not define table {table!r}")
    return columns


def _primary_key(row: CanoeBaseModel) -> tuple[Any, ...]:
    return tuple(getattr(row, field) for field in row.__primary_key__)


def _deduplicate_models(rows: Sequence[CanoeBaseModel]) -> list[CanoeBaseModel]:
    by_key: dict[tuple[type[CanoeBaseModel], tuple[Any, ...]], CanoeBaseModel] = {}
    for row in rows:
        identity = (type(row), _primary_key(row))
        existing = by_key.get(identity)
        if existing is not None and existing.model_dump(mode="python") != row.model_dump(
            mode="python"
        ):
            raise TemplateLoadError(
                f"Conflicting {row.table_name()} definitions for {_primary_key(row)}"
            )
        by_key[identity] = row
    return list(by_key.values())


def prepare_template_table(
    connection: sqlite3.Connection,
    *,
    specification: TemplateTable,
    template_path: Path,
    data_id: str,
) -> tuple[list[CanoeBaseModel], list[CanoeBaseModel], TableLoadResult]:
    """Construct validated v4 rows and labels without writing to SQLite."""
    csv_columns, raw_rows, source_encoding = _read_template(template_path)
    model_fields = specification.model.model_fields
    target_columns = _table_columns(connection, specification.table)
    missing_target_columns = sorted(set(model_fields) - set(target_columns))
    if missing_target_columns:
        raise TemplateLoadError(
            f"Target schema table {specification.table!r} is missing transport "
            f"columns required by {specification.model.__name__}: "
            f"{missing_target_columns}"
        )
    ignored_fields = [field for field in csv_columns if field not in model_fields]
    missing_fields = [field for field in model_fields if field not in csv_columns]
    missing_required = [
        field
        for field in missing_fields
        if field != "data_id" and model_fields[field].is_required()
    ]
    if missing_required:
        raise TemplateLoadError(
            f"Template {template_path} is missing required {specification.table} "
            f"fields: {missing_required}"
        )

    default_counts: Counter[str] = Counter()
    null_counts: Counter[str] = Counter()
    rows: list[CanoeBaseModel] = []
    labels: list[CanoeBaseModel] = []
    for row_number, raw in enumerate(raw_rows, start=2):
        payload: dict[str, Any] = {"data_id": data_id} if "data_id" in model_fields else {}
        for field in csv_columns:
            if field not in model_fields:
                continue
            value = raw[field]
            field_info = model_fields[field]
            if value != "":
                payload[field] = value
            elif field_info.default is not PydanticUndefined and field_info.default is not None:
                default_counts[field] += 1
            elif field_info.is_required():
                raise TemplateLoadError(
                    f"Template {template_path} row {row_number} has a blank required "
                    f"field: {field}"
                )
            else:
                payload[field] = None
                null_counts[field] += 1
        try:
            row = specification.model.model_validate(payload)
        except ValidationError as exc:
            raise TemplateLoadError(
                f"Invalid {specification.table} row {row_number} in {template_path}: {exc}"
            ) from exc
        rows.append(row)
        if specification.label_model is not None:
            if specification.label_field is None:
                raise TemplateLoadError(
                    f"Template {specification.table} has a label model without a field"
                )
            label_value = getattr(row, specification.label_field)
            labels.append(specification.label_model.model_validate({
                specification.label_model.__primary_key__[0]: label_value
            }))

    primary_keys = [_primary_key(row) for row in rows]
    if len(set(primary_keys)) != len(primary_keys):
        raise TemplateLoadError(
            f"Template {template_path} contains duplicate {specification.table} keys"
        )
    result = TableLoadResult(
        table=specification.table,
        template=str(template_path),
        source_encoding=source_encoding,
        source_rows=len(raw_rows),
        inserted_rows=len(rows),
        csv_columns=csv_columns,
        target_columns=target_columns,
        inserted_columns=[field for field in csv_columns if field in model_fields]
        + (["data_id"] if "data_id" in model_fields else []),
        ignored_fields=ignored_fields,
        missing_optional_fields=[
            field for field in missing_fields if field != "data_id"
        ],
        schema_defaults_used=dict(sorted(default_counts.items())),
        blank_values_as_null=dict(sorted(null_counts.items())),
        primary_key_columns=list(specification.model.__primary_key__),
        primary_keys=primary_keys,
    )
    return rows, _deduplicate_models(labels), result


def _expected_keys(rows: Sequence[CanoeBaseModel]) -> list[tuple[Any, ...]]:
    return [_primary_key(row) for row in rows]


def apply_scenario_economics(
    connection: sqlite3.Connection, bundle: ConfigBundle
) -> dict[str, float]:
    """Apply validated scenario rates after checking the packaged schema defaults."""
    configured = {
        "global_discount_rate": bundle.scenario.economics.global_discount_rate,
        "default_loan_rate": bundle.scenario.economics.default_loan_rate,
    }
    connection.executemany(
        "UPDATE metadata_real SET value = ? WHERE element = ?",
        [(value, element) for element, value in configured.items()],
    )
    actual = {
        str(element): float(value)
        for element, value in connection.execute(
            "SELECT element, value FROM metadata_real "
            "WHERE element IN ('global_discount_rate', 'default_loan_rate')"
        )
    }
    if actual != configured:
        raise TemplateLoadError(
            f"Could not apply configured scenario economics: {actual}"
        )
    return actual


def apply_technology_note_overrides(
    rows: Sequence[CanoeBaseModel], overrides: dict[str, str]
) -> list[CanoeBaseModel]:
    """Replace technology notes by exact technology key and reject stale keys."""
    if not overrides:
        return list(rows)
    available = {str(row.tech) for row in rows}
    unknown = sorted(set(overrides) - available)
    if unknown:
        raise TemplateLoadError(
            f"Technology note overrides reference unknown technologies: {unknown}"
        )
    return [
        type(row).model_validate(
            {
                **row.model_dump(mode="python"),
                "notes": overrides.get(str(row.tech), row.notes),
            }
        )
        for row in rows
    ]


def _internal_template_dataset(template_dir: Path) -> DataSet:
    """Describe the backend-owned template without inventing external provenance."""
    digest = hashlib.sha256()
    for specification in TEMPLATE_TABLES:
        path = template_dir / specification.filename
        if not path.is_file():
            raise FileNotFoundError(f"Template CSV does not exist: {path}")
        digest.update(specification.filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    version = digest.hexdigest()[:16]
    return DataSet(
        data_id=f"canoe-transport-template:{version}",
        label="CANOE transportation backend structural template",
        version=version,
        description=(
            "Backend-owned technology and commodity archetypes; this is a structural "
            "model reference, not an external input source."
        ),
    )


def prepare_transport_contribution(
    connection: sqlite3.Connection,
    *,
    bundle: ConfigBundle,
    template_dir: Path,
    include_existing_capacity: bool | None = None,
    include_demand: bool | None = None,
    include_road_utilization: bool | None = None,
    include_lifetimes: bool | None = None,
    include_efficiencies: bool | None = None,
    include_costs: bool | None = None,
    include_ev_chargers: bool | None = None,
    include_emission_embodied: bool | None = None,
) -> TransportContribution:
    """Prepare the currently supported transport rows without writing to SQLite."""
    if not template_dir.is_dir():
        raise FileNotFoundError(f"Template directory does not exist: {template_dir}")
    template_dataset = _internal_template_dataset(template_dir)
    table_rows: dict[str, list[CanoeBaseModel]] = {}
    table_labels: dict[str, list[CanoeBaseModel]] = {}
    load_results: list[TableLoadResult] = []
    for specification in TEMPLATE_TABLES:
        rows, labels, result = prepare_template_table(
            connection,
            specification=specification,
            template_path=template_dir / specification.filename,
            data_id=template_dataset.data_id,
        )
        if specification.table == "technology":
            rows = apply_technology_note_overrides(
                rows, bundle.scenario.row_note_overrides.technology
            )
        table_rows[specification.table] = rows
        if specification.label_model is not None:
            table_labels[specification.label_model.table_name()] = labels
        load_results.append(result)

    selected_capacity = True if include_existing_capacity is None else include_existing_capacity
    parameter_rows: list[ExistingCapacity] = []
    contexts: list[ResolvedProvenance] = []
    parameter_audit: dict[str, Any] = {}
    if selected_capacity:
        from parameterization.build_existing_capacity import prepare_existing_capacity_rows

        parameter_rows, contexts, parameter_audit = prepare_existing_capacity_rows(bundle)
    selected_demand = True if include_demand is None else include_demand
    demand_rows: list[Demand] = []
    demand_audit: dict[str, Any] = {}
    if selected_demand:
        from parameterization.build_demand import prepare_demand_rows

        demand_rows, demand_contexts, demand_audit = prepare_demand_rows(bundle)
        contexts.extend(demand_contexts)
    selected_road_utilization = (
        True if include_road_utilization is None else include_road_utilization
    )
    road_assumption_dataset: DataSet | None = None
    c2a_rows: list[CapacityToActivity] = []
    flat_factor_rows: list[LimitAnnualCapacityFactor] = []
    road_audit: dict[str, Any] = {}
    if selected_road_utilization:
        from parameterization.road_utilization import prepare_road_utilization

        road = prepare_road_utilization(bundle)
        road_assumption_dataset = road.assumption_dataset
        c2a_rows = road.capacity_to_activity_rows
        flat_factor_rows = road.flat_factor_rows
        contexts.extend(road.provenance_contexts)
        road_audit = road.audit
    lifetime_tech_rows: list[LifetimeTech] = []
    lifetime_curve_rows: list[LifetimeSurvivalCurve] = []
    lifetime_audit: dict[str, Any] = {}
    if include_lifetimes is not False:
        from parameterization.build_lifetime_parameters import prepare_lifetime_rows

        lifetimes = prepare_lifetime_rows(bundle)
        lifetime_tech_rows = lifetimes.fixed_rows
        lifetime_curve_rows = lifetimes.curve_rows
        contexts.extend(lifetimes.provenance_contexts)
        lifetime_audit = lifetimes.audit
    efficiency_rows: list[Efficiency] = []
    split_rows: list[LimitTechInputSplit] = []
    efficiency_dataset: DataSet | None = None
    efficiency_audit: dict[str, Any] = {}
    if include_efficiencies is not False:
        from parameterization.build_efficiencies import prepare_efficiency_rows

        efficiencies = prepare_efficiency_rows(
            bundle, existing_capacity_rows=parameter_rows if selected_capacity else None,
        )
        efficiency_rows = efficiencies.efficiency_rows
        split_rows = efficiencies.split_rows
        efficiency_dataset = efficiencies.assumption_dataset
        efficiency_audit = efficiencies.audit
        contexts.extend(efficiencies.provenance_contexts)
    cost_invest_rows: list[CostInvest] = []
    cost_fixed_rows: list[CostFixed] = []
    cost_variable_rows: list[CostVariable] = []
    cost_audit: dict[str, Any] = {}
    if include_costs is not False:
        from parameterization.build_costs import prepare_cost_rows

        costs = prepare_cost_rows(
            bundle, existing_capacity_rows=parameter_rows if selected_capacity else None,
            fixed_lifetime_rows=lifetime_tech_rows if include_lifetimes is not False else None,
            survival_curve_rows=lifetime_curve_rows if include_lifetimes is not False else None,
        )
        cost_invest_rows = costs.invest_rows
        cost_variable_rows = costs.variable_rows
        cost_audit = costs.audit
        contexts.extend(costs.provenance_contexts)
        if efficiency_rows:
            supported = {(r.region, r.tech, r.vintage) for r in efficiency_rows}
            cost_keys = {
                (r.region, r.tech, r.vintage)
                for r in [*cost_invest_rows, *cost_variable_rows]
            }
            missing = cost_keys - supported
            if missing:
                raise ValueError(
                    f"Cost technology/vintage lacks prepared efficiency: {sorted(missing)[:8]}"
                )
            cost_audit["efficiency_supported_tech_vintages"] = len(cost_keys)
    charger_audit: dict[str, Any] = {}
    if selected_capacity and include_ev_chargers is not False:
        from parameterization.ev_chargers import prepare_ev_charger_rows

        chargers = prepare_ev_charger_rows(
            bundle, existing_capacity_rows=parameter_rows,
            existing_capacity_contexts=contexts,
        )
        parameter_rows.extend(chargers.capacity_rows)
        if include_efficiencies is not False:
            efficiency_rows.extend(chargers.efficiency_rows)
        if include_costs is not False:
            cost_invest_rows.extend(chargers.invest_rows)
            cost_fixed_rows.extend(chargers.fixed_rows)
        contexts.extend(chargers.provenance_contexts)
        charger_audit = chargers.audit
    embodied = None
    if include_emission_embodied is not False:
        from parameterization.road_embodied_emissions import prepare_emission_embodied_rows

        embodied = prepare_emission_embodied_rows(bundle)
        contexts.extend(embodied.provenance_contexts)
    support_tables = []
    if selected_capacity:
        support_tables.append("existing_capacity")
    if include_efficiencies is not False:
        support_tables.append("efficiency")
    if include_costs is not False:
        support_tables.extend(("cost_invest", "cost_fixed", "cost_variable"))
    if selected_road_utilization:
        support_tables.append("limit_annual_capacity_factor")
    if embodied is not None and embodied.audit["enabled"]:
        support_tables.append("emission_embodied")
    existing_suffix = load_harmonization_rules(bundle, "efficiencies")["existing_suffix"]
    contribution = TransportContribution(
        dataset=template_dataset,
        labels_by_table=table_labels,
        rows_by_table=table_rows,
        load_results=load_results,
        parameter_rows=parameter_rows,
        demand_rows=demand_rows,
        road_assumption_dataset=road_assumption_dataset,
        capacity_to_activity_rows=c2a_rows,
        flat_road_factor_rows=flat_factor_rows,
        lifetime_tech_rows=lifetime_tech_rows,
        lifetime_survival_curve_rows=lifetime_curve_rows,
        efficiency_rows=efficiency_rows,
        input_split_rows=split_rows,
        efficiency_assumption_dataset=efficiency_dataset,
        cost_invest_rows=cost_invest_rows,
        cost_fixed_rows=cost_fixed_rows,
        cost_variable_rows=cost_variable_rows,
        charger_audit=charger_audit,
        provenance_contexts=contexts,
        parameter_audit=parameter_audit,
        demand_audit=demand_audit,
        road_utilization_audit=road_audit,
        lifetime_audit=lifetime_audit,
        efficiency_audit=efficiency_audit,
        cost_audit=cost_audit,
        emission_embodied=embodied,
        existing_vintages=tuple(bundle.scenario.periods.existing),
        prepared_support_tables=tuple(support_tables),
        new_technologies=tuple(
            row.tech for row in table_rows["technology"]
            if not row.tech.endswith(existing_suffix)
        ),
    )
    contribution.support_audit.update(_require_transport_parameter_support(contribution))
    return contribution


def _require_transport_parameter_support(contribution: TransportContribution) -> dict[str, Any]:
    batches = {
        "existing_capacity": contribution.parameter_rows,
        "efficiency": contribution.efficiency_rows,
        "cost_invest": contribution.cost_invest_rows,
        "cost_fixed": contribution.cost_fixed_rows,
        "cost_variable": contribution.cost_variable_rows,
        "limit_annual_capacity_factor": contribution.flat_road_factor_rows,
        "emission_embodied": contribution.emission_embodied_rows,
    }
    if contribution.emission_embodied is not None and contribution.emission_embodied.audit["enabled"]:
        from parameterization.road_embodied_emissions import validate_embodied_outputs

        validate_embodied_outputs(
            contribution.emission_embodied_rows,
            expected_keys=set(contribution.emission_embodied.expected_keys),
            technologies={row.tech for row in contribution.rows_by_table["technology"]},
            emission_commodities={row.name: row.units for row in contribution.rows_by_table["commodity"] if row.flag == "e"},
            units=contribution.emission_embodied.audit["output_units"],
            capacity_units=contribution.emission_embodied.audit["capacity_units"],
        )
    audit = validate_transport_parameter_support(
        {table: batches[table] for table in contribution.prepared_support_tables},
        existing_vintages=contribution.existing_vintages,
        new_technologies=contribution.new_technologies,
    )
    if not audit["ok"]:
        LOGGER.error("Transport parameter support failed: %s", audit["errors"])
        raise ValueError(f"Transport parameter support failed: {audit['errors']}")
    LOGGER.info("Transport cross-table parameter support: %s", audit["checks"])
    return audit


def insert_transport_contribution(
    connection: sqlite3.Connection,
    contribution: TransportContribution,
    *,
    conflict: ConflictPolicy = "error",
) -> dict[str, list[CanoeBaseModel]]:
    """Insert a prepared contribution into a transaction owned by the caller."""
    # Recheck mutable batches before any SQLite writes, including caller insertions.
    contribution.support_audit.clear()
    contribution.support_audit.update(_require_transport_parameter_support(contribution))
    embodied_rows = [EmissionEmbodied.model_validate(row.model_dump()) for row in contribution.emission_embodied_rows]
    insert_models(
        connection,
        [contribution.dataset],
        conflict=conflict,
    )
    inserted: dict[str, list[CanoeBaseModel]] = {
        contribution.dataset.table_name(): [contribution.dataset]
    }
    for rows in contribution.labels_by_table.values():
        insert_models(
            connection,
            rows,
            conflict=conflict,
        )
        inserted[rows[0].table_name()] = list(rows)
    for rows in contribution.rows_by_table.values():
        insert_models(
            connection,
            rows,
            conflict=conflict,
        )
        inserted[rows[0].table_name()] = list(rows)
    if contribution.road_assumption_dataset is not None:
        insert_models(connection, [contribution.road_assumption_dataset], conflict=conflict)
        inserted.setdefault("data_set", []).append(contribution.road_assumption_dataset)
    if contribution.efficiency_assumption_dataset is not None:
        insert_models(connection, [contribution.efficiency_assumption_dataset], conflict=conflict)
        inserted.setdefault("data_set", []).append(contribution.efficiency_assumption_dataset)
    if (contribution.parameter_rows or contribution.demand_rows
            or contribution.flat_road_factor_rows or contribution.lifetime_tech_rows
            or contribution.lifetime_survival_curve_rows or contribution.efficiency_rows
            or contribution.input_split_rows or contribution.cost_invest_rows
            or contribution.cost_fixed_rows
            or contribution.cost_variable_rows or embodied_rows):
        labels, datasets, sources = registry_rows(contribution.provenance_contexts)
        for registry_batch in (labels, datasets, sources):
            if registry_batch:
                insert_models(connection, registry_batch, conflict=conflict)
                inserted.setdefault(registry_batch[0].table_name(), []).extend(registry_batch)
        if contribution.parameter_rows:
            insert_models(connection, contribution.parameter_rows, conflict=conflict)
            inserted["existing_capacity"] = list(contribution.parameter_rows)
        if contribution.demand_rows:
            insert_models(connection, contribution.demand_rows, conflict=conflict)
            inserted["demand"] = list(contribution.demand_rows)
        if contribution.flat_road_factor_rows:
            insert_models(connection, contribution.flat_road_factor_rows, conflict=conflict)
            inserted["limit_annual_capacity_factor"] = list(contribution.flat_road_factor_rows)
        if contribution.lifetime_tech_rows:
            insert_models(connection, contribution.lifetime_tech_rows, conflict=conflict)
            inserted["lifetime_tech"] = list(contribution.lifetime_tech_rows)
        if contribution.lifetime_survival_curve_rows:
            insert_models(connection, contribution.lifetime_survival_curve_rows, conflict=conflict)
            inserted["lifetime_survival_curve"] = list(contribution.lifetime_survival_curve_rows)
        if contribution.efficiency_rows:
            insert_models(connection, contribution.efficiency_rows, conflict=conflict)
            inserted["efficiency"] = list(contribution.efficiency_rows)
        if contribution.cost_invest_rows:
            insert_models(connection, contribution.cost_invest_rows, conflict=conflict)
            inserted["cost_invest"] = list(contribution.cost_invest_rows)
        if contribution.cost_fixed_rows:
            insert_models(connection, contribution.cost_fixed_rows, conflict=conflict)
            inserted["cost_fixed"] = list(contribution.cost_fixed_rows)
        if contribution.cost_variable_rows:
            insert_models(connection, contribution.cost_variable_rows, conflict=conflict)
            inserted["cost_variable"] = list(contribution.cost_variable_rows)
        if contribution.input_split_rows:
            insert_models(connection, contribution.input_split_rows, conflict=conflict)
            inserted["limit_tech_input_split"] = list(contribution.input_split_rows)
        if embodied_rows:
            insert_models(connection, embodied_rows, conflict=conflict)
            inserted["emission_embodied"] = embodied_rows
    if contribution.capacity_to_activity_rows:
        insert_models(connection, contribution.capacity_to_activity_rows, conflict=conflict)
        inserted["capacity_to_activity"] = list(contribution.capacity_to_activity_rows)
    return inserted


def bootstrap_database(
    *,
    bundle: ConfigBundle,
    template_dir: Path,
    database_path: Path,
    overwrite: bool = False,
    comparison_reference: Path | None = None,
) -> dict[str, Any]:
    """Build, audit, and atomically publish one package-DDL v4 database."""
    if database_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing SQLite database: {database_path}. "
            "Pass --overwrite to replace it explicitly."
        )
    if comparison_reference is not None:
        if comparison_reference.resolve() == database_path.resolve():
            raise ValueError("Comparison reference must differ from the output database")
        if not comparison_reference.is_file():
            raise FileNotFoundError(f"Comparison reference is missing: {comparison_reference}")
        if bundle.scenario.comparison.mode == "scenario":
            validate_scenario_comparison_reference(comparison_reference)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{database_path.name}.",
        suffix=".tmp",
        dir=database_path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(temporary_path, isolation_level=None)
        preflight = create_v4_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        preflight["packaged_rates"] = preflight.pop("rates")
        preflight["configured_rates"] = apply_scenario_economics(connection, bundle)

        contribution = prepare_transport_contribution(
            connection,
            bundle=bundle,
            template_dir=template_dir,
        )
        base_load_results: list[TableLoadResult] = []
        inserted: dict[str, list[CanoeBaseModel]] = {}
        for specification in STANDALONE_BASE_TABLES:
            base_rows, _, result = prepare_template_table(
                connection,
                specification=specification,
                template_path=template_dir / specification.filename,
                data_id=contribution.dataset.data_id,
            )
            if specification.table == "time_period" and bundle.scenario.periods.period_mode == "prospective":
                periods = bundle.scenario.periods
                base_rows = [TimePeriod(period=year, flag="e", sequence=None) for year in periods.existing]
                base_rows.extend(
                    TimePeriod(period=year, flag="f", sequence=index)
                    for index, year in enumerate([*periods.model, periods.end_of_horizon])
                )
                result = replace(result, inserted_rows=len(base_rows), primary_keys=_expected_keys(base_rows))
            insert_models(connection, base_rows)
            inserted[specification.table] = base_rows
            base_load_results.append(result)
        inserted.update(insert_transport_contribution(connection, contribution))

        expected_primary_keys = {
            table: _expected_keys(rows) for table, rows in inserted.items()
        }
        touched_tables = list(inserted)
        validation = validate_database(
            connection,
            expected_primary_keys=expected_primary_keys,
            touched_tables=touched_tables,
        )
        if not validation["ok"]:
            raise TemplateLoadError(
                f"Database validation failed before publish: {validation['errors']}"
            )
        connection.commit()
        connection.close()
        connection = None
        comparison_report = (
            compare_transport_database(bundle, temporary_path, comparison_reference)
            if comparison_reference is not None else {"enabled": False, "mode": "none"}
        )
        os.replace(temporary_path, database_path)
    except Exception:
        if connection is not None:
            if connection.in_transaction:
                connection.rollback()
            connection.close()
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "ok": True,
        "database": str(database_path),
        "schema": schema_evidence(),
        "source_id_mapping": source_id_mapping(bundle.sources),
        "template": {
            "kind": "backend_internal_reference",
            "data_id": contribution.dataset.data_id,
            "content_version": contribution.dataset.version,
        },
        "existing_capacity": contribution.parameter_audit,
        "periods": bundle.scenario.periods.audit(),
        "demand": contribution.demand_audit,
        "road_utilization": contribution.road_utilization_audit,
        "lifetimes": contribution.lifetime_audit,
        "efficiencies": contribution.efficiency_audit,
        "costs": contribution.cost_audit,
        "ev_chargers": contribution.charger_audit,
        "emission_embodied": contribution.emission_embodied_audit,
        "parameter_support": contribution.support_audit,
        "comparison": comparison_report,
        "preflight": preflight,
        "templates": [
            {key: value for key, value in asdict(result).items() if key != "primary_keys"}
            for result in (*base_load_results, *contribution.load_results)
        ],
        "touched_table_row_counts": {
            table: audit["row_count"]
            for table, audit in validation["touched_table_audit"].items()
        },
        "validation": validation,
    }


def compare_transport_database(
    bundle: ConfigBundle, candidate_path: Path, reference: Path,
) -> dict[str, Any]:
    """Run the selected optional comparison before atomic database publication."""
    comparison = bundle.scenario.comparison
    if comparison.mode == "scenario":
        result = compare_scenario_databases(
            candidate_path, reference,
            absolute_tolerance=comparison.absolute_tolerance,
            relative_tolerance=comparison.relative_tolerance,
            include_provenance=comparison.include_provenance,
        )
    elif comparison.mode == "legacy":
        tolerances = {
            "absolute_tolerance": comparison.absolute_tolerance,
            "relative_tolerance": comparison.relative_tolerance,
        }
        result = compare_legacy_tables(
            candidate_path,
            reference,
            tables=("technology", "commodity"),
        )
        result.update(mode="legacy", **tolerances)
        result["existing_capacity"] = compare_legacy_existing_capacity(
            candidate_path, reference, **tolerances,
        )
        result["demand"] = compare_legacy_demand(
            candidate_path, reference, **tolerances,
        )
        result["lifetime_tech"] = compare_legacy_lifetime_tech(
            candidate_path, reference, **tolerances,
        )
        result["efficiency"] = compare_legacy_efficiency(
            candidate_path, reference,
            **tolerances,
        )
        result["costs"] = compare_legacy_costs(
            candidate_path, reference,
            **tolerances,
        )
        result["emission_embodied"] = compare_legacy_emission_embodied(
            candidate_path, reference, **tolerances,
        )
    else:
        result = {"enabled": False, "mode": "none"}
    return result


def write_validation_report(report: dict[str, Any], path: Path) -> None:
    """Write the concise configured validation artifact atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_from_scenario(
    scenario_path: str | Path,
    *,
    overwrite: bool = False,
) -> tuple[dict[str, Any], Path]:
    """Resolve scenario paths, build the database, and write validation JSON."""
    bundle = load_config_bundle(scenario_path)
    database_path = resolve_configured_path(
        bundle,
        "outputs",
        "sqlite",
        bundle.scenario.outputs.sqlite_name,
    )
    comparison = bundle.scenario.comparison
    reference = None
    if comparison.mode != "none":
        reference = resolve_repo_path(bundle.repo_root, comparison.reference_sqlite)
        if reference.resolve() == database_path.resolve():
            raise ValueError("Comparison reference must differ from the output database")
        if not reference.is_file():
            raise FileNotFoundError(f"Comparison reference is missing: {reference}")
        if comparison.mode == "scenario":
            validate_scenario_comparison_reference(reference)
    report = bootstrap_database(
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        database_path=database_path,
        overwrite=overwrite,
        comparison_reference=reference,
    )
    report.update(
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "scenario": bundle.scenario.scenario.name,
            "config": {
                "scenario": {
                    "path": str(bundle.scenario_path),
                    "sha256": file_sha256(bundle.scenario_path),
                },
                "paths": {
                    "path": str(bundle.paths_path),
                    "sha256": file_sha256(bundle.paths_path),
                },
                "sources": {
                    "path": str(bundle.sources_path),
                    "sha256": file_sha256(bundle.sources_path),
                },
            },
        }
    )

    validation_path = resolve_repo_path(
        bundle.repo_root,
        bundle.scenario.outputs.validation_report,
    )
    write_validation_report(report, validation_path)
    return report, validation_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
        help="Path to the scenario YAML file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly replace the configured SQLite output if it already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report, validation_path = build_from_scenario(
            args.scenario,
            overwrite=args.overwrite,
        )
    except (
        FileNotFoundError,
        FileExistsError,
        TemplateLoadError,
        ValidationError,
        ValueError,
        sqlite3.Error,
    ) as exc:
        raise SystemExit(f"Database build failed: {exc}") from exc
    LOGGER.info(
        "Created %s and loaded %s template tables; validation: %s",
        report["database"],
        len(report["templates"]),
        validation_path,
    )


if __name__ == "__main__":
    main()
