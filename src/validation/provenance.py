"""Deterministic source IDs, dataset IDs, and v4 provenance rows."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from canoe_schema import CanoeBaseModel
from canoe_schema.v4_0 import DataSet, DataSource, DataSourceLabel
from pydantic import BaseModel, ConfigDict

from validation.config_models import DataQuality, SourceComponent, SourcesConfig


class ProvenanceError(ValueError):
    """Raised when provenance cannot be resolved without ambiguity."""


class Contributor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_key: str
    source_id: str
    title: str
    citation: str
    refresh_notes: str
    data_id: str


class ResolvedProvenance(BaseModel):
    """Small immutable provenance context passed to row construction."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_key: str
    source_id: str
    component_key: str
    data_id: str
    dataset_label: str
    dataset_version: str
    dataset_description: str
    data_quality: DataQuality
    governing_source_id: str
    contributors: tuple[Contributor, ...]

    def parameter_fields(self) -> dict[str, Any]:
        return {
            "data_source": self.governing_source_id,
            "data_id": self.data_id,
            **self.data_quality.row_fields(),
        }

    def registry_rows(self) -> tuple[CanoeBaseModel, ...]:
        labels = tuple(
            DataSourceLabel(
                source_id=item.source_id,
                notes=f"{item.source_key}: {item.title}",
            )
            for item in self.contributors
        )
        dataset = DataSet(
            data_id=self.data_id,
            label=self.dataset_label,
            version=self.dataset_version,
            description=self.dataset_description,
        )
        sources = tuple(
            DataSource(
                source_id=item.source_id,
                source=item.citation,
                notes=item.refresh_notes,
                data_id=self.data_id,
            )
            for item in self.contributors
        )
        return (*labels, dataset, *sources)


def source_id_mapping(sources: SourcesConfig) -> dict[str, str]:
    """Map YAML source order to stable two-digit source labels."""
    if len(sources.sources) > 99:
        raise ProvenanceError("At most 99 sources can be mapped to Txx labels")
    return {
        source_key: f"T{index:02d}"
        for index, source_key in enumerate(sources.sources, start=1)
    }


def _canonical_digest(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def make_data_id(
    *,
    dataset_key: str,
    source_version: str,
    transformation: str,
    transformation_version: str,
    value_variant: Mapping[str, Any] | None = None,
    contributors: Sequence[str] = (),
) -> str:
    """Build one stable ID from value-bearing identity inputs only."""
    variant = dict(value_variant or {})
    forbidden = [
        key
        for key in variant
        if any(token in key.casefold() for token in ("path", "filename", "logging"))
    ]
    if forbidden:
        raise ProvenanceError(
            f"Runtime-only data_id variant keys are forbidden: {sorted(forbidden)}"
        )
    payload = {
        "dataset_key": dataset_key,
        "source_version": source_version,
        "transformation": transformation,
        "transformation_version": transformation_version,
        "value_variant": variant,
        "contributors": sorted(contributors),
    }
    slug = re.sub(r"[^a-z0-9]+", "-", dataset_key.casefold()).strip("-")
    return f"{slug}:{source_version}:{transformation_version}:{_canonical_digest(payload)}"


def _component_label(component: SourceComponent) -> str:
    if isinstance(component.label, list):
        return " / ".join(component.label)
    return component.label


def resolve_provenance(
    sources: SourcesConfig,
    *,
    source_key: str,
    component_key: str | int,
    transformation: str,
    transformation_version: str,
    value_variant: Mapping[str, Any] | None = None,
) -> ResolvedProvenance:
    """Resolve a configured single-source component into immutable row context."""
    try:
        source = sources.sources[source_key]
    except KeyError as exc:
        raise ProvenanceError(f"Unknown source: {source_key}") from exc
    try:
        component = source.component(component_key)
    except KeyError as exc:
        raise ProvenanceError(
            f"Unknown component {component_key!r} for source {source_key!r}"
        ) from exc
    mapping = source_id_mapping(sources)
    source_id = mapping[source_key]
    version = component.version or source.version
    dataset_key = component.dataset_key or f"{source_key}.{component_key}"
    data_id = make_data_id(
        dataset_key=dataset_key,
        source_version=version,
        transformation=transformation,
        transformation_version=transformation_version,
        value_variant=value_variant,
    )
    contributor = Contributor(
        source_key=source_key,
        source_id=source_id,
        title=source.title,
        citation=component.citation or source.citation,
        refresh_notes=source.refresh_notes,
        data_id=data_id,
    )
    description = (
        f"{transformation} v{transformation_version}; produces "
        f"{', '.join(component.produces)} for {', '.join(component.parameter_modules)}"
    )
    return ResolvedProvenance(
        source_key=source_key,
        source_id=source_id,
        component_key=str(component_key),
        data_id=data_id,
        dataset_label=_component_label(component),
        dataset_version=version,
        dataset_description=description,
        data_quality=component.data_quality or source.data_quality,
        governing_source_id=source_id,
        contributors=(contributor,),
    )


def resolve_composite_provenance(
    *,
    inputs: Sequence[ResolvedProvenance],
    dataset_key: str,
    transformation: str,
    transformation_version: str,
    governing_source_id: str,
    data_quality: DataQuality | None = None,
    value_variant: Mapping[str, Any] | None = None,
) -> ResolvedProvenance:
    """Resolve one derived dataset with explicit governing source and DQ policy."""
    if not inputs:
        raise ProvenanceError("Composite provenance requires at least one contributor")
    contributors: list[Contributor] = []
    metadata_by_source: dict[str, tuple[str, str, str, str]] = {}
    for item in inputs:
        for contributor in item.contributors:
            metadata = (
                contributor.source_key,
                contributor.title,
                contributor.citation,
                contributor.refresh_notes,
            )
            existing = metadata_by_source.get(contributor.source_id)
            if existing is not None and existing != metadata:
                raise ProvenanceError(
                    f"Conflicting contributor definition for {contributor.source_id}"
                )
            metadata_by_source[contributor.source_id] = metadata
            contributors.append(contributor)
    if governing_source_id not in metadata_by_source:
        raise ProvenanceError(
            f"Governing source {governing_source_id!r} is not a contributor"
        )
    resolved_quality = data_quality
    qualities = {item.data_quality for item in inputs}
    if resolved_quality is None:
        if len(qualities) != 1:
            raise ProvenanceError(
                "Composite contributors have different DQ values; configure an override"
            )
        resolved_quality = next(iter(qualities))
    input_ids = sorted(item.data_id for item in inputs)
    data_id = make_data_id(
        dataset_key=dataset_key,
        source_version="composite",
        transformation=transformation,
        transformation_version=transformation_version,
        value_variant=value_variant,
        contributors=input_ids,
    )
    sorted_contributors = tuple(
        sorted(contributors, key=lambda item: (item.source_id, item.data_id))
    )
    return ResolvedProvenance(
        source_key="composite",
        source_id=governing_source_id,
        component_key=dataset_key,
        data_id=data_id,
        dataset_label=dataset_key,
        dataset_version="composite",
        dataset_description=f"{transformation} v{transformation_version}",
        data_quality=resolved_quality,
        governing_source_id=governing_source_id,
        contributors=sorted_contributors,
    )


def registry_rows(
    contexts: Sequence[ResolvedProvenance],
) -> tuple[list[DataSourceLabel], list[DataSet], list[DataSource]]:
    """De-duplicate registry rows and reject conflicting definitions."""
    groups: tuple[tuple[type[CanoeBaseModel], list[CanoeBaseModel]], ...] = (
        (DataSourceLabel, []),
        (DataSet, []),
        (DataSource, []),
    )
    by_type = {model: rows for model, rows in groups}
    seen: dict[tuple[type[CanoeBaseModel], tuple[Any, ...]], CanoeBaseModel] = {}
    for context in contexts:
        for row in context.registry_rows():
            row_type = type(row)
            payload = row.model_dump(mode="python")
            key = tuple(payload[field] for field in row.__primary_key__)
            identity = (row_type, key)
            previous = seen.get(identity)
            if previous is not None:
                if previous.model_dump(mode="python") != payload:
                    raise ProvenanceError(
                        f"Conflicting {row.table_name()} registry definition for {key}"
                    )
                continue
            seen[identity] = row
            by_type[row_type].append(row)
    return (
        list(by_type[DataSourceLabel]),
        list(by_type[DataSet]),
        list(by_type[DataSource]),
    )
