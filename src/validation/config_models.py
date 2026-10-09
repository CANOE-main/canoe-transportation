"""Strict typed models for the transportation YAML control layer."""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterator, Mapping
from copy import deepcopy
from typing import Annotated, Any, Literal, Self

from canoe_schema.v4_0 import (
    DataQualityCredibilityLevel,
    DataQualityGeographyLevel,
    DataQualityStructureLevel,
    DataQualityTechnologyLevel,
    DataQualityTimeLevel,
)
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator


class MappingModel(BaseModel, Mapping[str, Any]):
    """Strict Pydantic model with read-compatible mapping access."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    def __getitem__(self, key: str) -> Any:
        if key in type(self).model_fields:
            return getattr(self, key)
        for name, field in type(self).model_fields.items():
            if field.alias == key:
                return getattr(self, name)
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(
            field.alias or name for name, field in type(self).model_fields.items()
        )

    def __len__(self) -> int:
        return len(type(self).model_fields)


class DataQuality(MappingModel):
    """Source-owned v4 scores; nulls are review placeholders in the registry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dq_cred: DataQualityCredibilityLevel | None = None
    dq_geog: DataQualityGeographyLevel | None = None
    dq_struc: DataQualityStructureLevel | None = None
    dq_tech: DataQualityTechnologyLevel | None = None
    dq_time: DataQualityTimeLevel | None = None

    def missing_fields(self) -> list[str]:
        return [name for name in type(self).model_fields if getattr(self, name) is None]

    def row_fields(self) -> dict[str, int]:
        if missing := self.missing_fields():
            raise ValueError(f"Unresolved data_quality scores: {', '.join(missing)}")
        return {name: int(getattr(self, name)) for name in type(self).model_fields}


class ConfigPaths(MappingModel):
    parameters: str


class InputPaths(MappingModel):
    root: str
    cache: str
    external: str
    manual: str
    interim: str
    processed: str
    validation: str
    template: str


class OutputPaths(MappingModel):
    root: str
    sqlite: str
    validation: str
    logs: str


class LegacyPaths(MappingModel):
    root: str
    reference_sqlite: str
    schema_path: str = Field(alias="schema")
    transportation_compiler: str
    charging_profiles: str
    constraints: str


class ArtifactRoute(MappingModel):
    """Stable ownership and impact route for one artifact family."""

    path: str = Field(min_length=1)
    layer: Literal[
        "cache",
        "documentation",
        "external",
        "interim",
        "processed",
        "input_validation",
        "database",
        "output_validation",
        "legacy",
    ]
    owner: str = Field(min_length=1)
    producers: list[str] = Field(min_length=1)
    consumers: list[str] = Field(min_length=1)
    validation_surfaces: list[
        Annotated[
            str,
            StringConstraints(
                pattern=r"^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)+$"
            ),
        ]
    ] = Field(min_length=1)


class PathsConfig(MappingModel):
    version: int
    root: str
    config: ConfigPaths
    inputs: InputPaths
    outputs: OutputPaths
    legacy: LegacyPaths
    artifacts: dict[str, ArtifactRoute] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_artifact_layers(self) -> Self:
        layer_roots = {
            "cache": self.inputs.cache,
            "documentation": "docs",
            "external": self.inputs.external,
            "interim": self.inputs.interim,
            "processed": self.inputs.processed,
            "input_validation": self.inputs.validation,
            "database": self.outputs.sqlite,
            "output_validation": self.outputs.validation,
            "legacy": self.legacy.root,
        }
        duplicate_paths: dict[str, list[str]] = {}
        for name, route in self.artifacts.items():
            normalized = route.path.replace("\\", "/").rstrip("/")
            root = layer_roots[route.layer].replace("\\", "/").rstrip("/")
            if normalized.startswith("/") or ".." in normalized.split("/"):
                raise ValueError(
                    f"artifacts.{name}.path must be a repository-relative path"
                )
            if normalized != root and not normalized.startswith(f"{root}/"):
                raise ValueError(
                    f"artifacts.{name}.path must be within the {route.layer} root "
                    f"{root}"
                )
            duplicate_paths.setdefault(normalized, []).append(name)
        collisions = {
            path: names for path, names in duplicate_paths.items() if len(names) > 1
        }
        if collisions:
            raise ValueError(f"artifact family paths must be unique: {collisions}")
        return self


AggregationRole = Literal["stock_age", "ldv", "medium_trucks", "heavy_truck_haul"]
RegionalSourceMap = dict[
    Annotated[str, StringConstraints(pattern=r"^(other|[A-Z]{2,5})$")],
    Annotated[str, StringConstraints(min_length=1)],
]


class AggregationSources(MappingModel):
    """Scenario source precedence by fleet role: explicit region, then other."""

    stock_age: RegionalSourceMap
    ldv: RegionalSourceMap
    medium_trucks: RegionalSourceMap
    heavy_truck_haul: RegionalSourceMap

    @model_validator(mode="after")
    def validate_regional_defaults(self) -> Self:
        for role in self:
            if not {"ON", "other"} <= self[role].keys():
                raise ValueError(f"aggregation_sources.{role} requires ON and other selections")
        return self

    def source_for(self, role: AggregationRole, region: str) -> str:
        selections = self[role]
        if region in selections:
            return selections[region]
        if region == "BCT" and "BC" in selections:
            return selections["BC"]
        return selections["other"]


class ScenarioIdentity(MappingModel):
    name: str
    description: str
    purpose: str


class ScenarioGeography(MappingModel):
    regions: list[str] = Field(min_length=1)


class ScenarioPeriods(MappingModel):
    """Observed-data anchor and explicit historical/model period coordinates."""

    period_mode: Literal["prospective", "legacy"]
    base_year: int = Field(gt=0)
    existing: list[int] = Field(min_length=1)
    model: list[int] = Field(min_length=1)
    step: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_period_grid(self) -> Self:
        if self.existing != sorted(set(self.existing)):
            raise ValueError("periods.existing must be sorted and unique")
        if self.model != sorted(set(self.model)):
            raise ValueError("periods.model must be sorted and unique")
        if any(year > self.base_year for year in self.existing):
            raise ValueError("periods.existing cannot be later than base_year")
        if any(year <= self.base_year for year in self.model):
            raise ValueError("periods.model must contain only years after base_year")
        if set(self.existing) & set(self.model):
            raise ValueError("periods.existing and periods.model cannot overlap")
        if any(
            later - earlier != self.step
            for earlier, later in zip(self.model, self.model[1:], strict=False)
        ):
            raise ValueError("periods.model must follow the configured step")
        if self.period_mode == "legacy" and self.existing[-1] != self.base_year:
            raise ValueError("Legacy periods.existing must end at base_year")
        if self.period_mode == "prospective" and self.existing[0] >= self.base_year:
            raise ValueError("Prospective periods.existing needs a label before base_year")
        return self

    def all_years(self) -> list[int]:
        """Parameter coordinates; the observation year is not an implicit vintage."""
        return [*self.existing, *self.model]

    @property
    def end_of_horizon(self) -> int:
        return self.model[-1] + self.step

    def existing_vintage(self, observation_year: int) -> int:
        """Bin observed annual cohorts, retaining the first-label oldest-cohort proxy."""
        if observation_year > self.base_year:
            raise ValueError("Historical cohorts cannot exceed periods.base_year")
        index = bisect_left(self.existing, observation_year)
        if self.period_mode == "prospective":
            index -= 1
        return self.existing[max(0, index)]

    @property
    def latest_observed_vintage(self) -> int:
        return self.existing_vintage(self.base_year)

    def historical_years(self, vintage: int) -> list[int]:
        """Observed years in a historical interval, clipped at the observation anchor."""
        index = self.existing.index(vintage)
        if self.period_mode == "prospective":
            # The first label also owns the initial-stock/oldest-cohort proxy.
            start = vintage if index == 0 else vintage + 1
            end = self.existing[index + 1] if index + 1 < len(self.existing) else self.model[0]
        else:
            start = self.existing[index - 1] + 1 if index else vintage - self.step + 1
            end = vintage
        return list(range(start, min(end, self.base_year) + 1))

    def projection_year(self, period: int, *, legacy_at_end: bool) -> int:
        """Prospective inputs use period ends; legacy adapters retain their old timing."""
        if period not in self.model:
            raise ValueError(f"Unknown model period: {period}")
        return period + self.step if self.period_mode == "prospective" or legacy_at_end else period

    def audit(self) -> dict[str, Any]:
        intervals = {str(vintage): self.historical_years(vintage) for vintage in self.existing}
        return {
            **self.model_dump(mode="json"),
            "base_year_role": "latest observed year for stock, demand and historical calibration",
            "historical_years_by_vintage": intervals,
            "empty_observation_vintages": [int(vintage) for vintage, years in intervals.items() if not years],
            "latest_observed_vintage": self.latest_observed_vintage,
            "efficiency_and_demand_years": {
                str(period): self.projection_year(period, legacy_at_end=True) for period in self.model
            },
            "cost_and_charger_years": {
                str(period): self.projection_year(period, legacy_at_end=False) for period in self.model
            },
            "end_of_horizon": self.end_of_horizon,
        }


class ScenarioSourceSelection(MappingModel):
    year: int | None = Field(default=None, gt=0)
    edition: int | None = Field(default=None, gt=0)


class ScenarioSources(MappingModel):
    selections: dict[str, ScenarioSourceSelection] = Field(default_factory=dict)


class ScenarioEconomics(MappingModel):
    cer_scenario: str = Field(min_length=1)
    global_discount_rate: float = Field(ge=0.0, le=1.0)
    default_loan_rate: float = Field(ge=0.0, le=1.0)
    cost_reference_currency: Literal["CAD"]
    cost_reference_year: int = Field(ge=1900, le=2100)


class ScenarioOutputs(MappingModel):
    sqlite_name: str
    validation_report: str
    setup_log: str


class ScenarioComparison(MappingModel):
    mode: Literal["none", "legacy", "scenario"]
    reference_sqlite: str | None
    absolute_tolerance: float = Field(ge=0, allow_inf_nan=False)
    relative_tolerance: float = Field(ge=0, allow_inf_nan=False)
    include_provenance: bool

    @model_validator(mode="after")
    def validate_reference(self) -> Self:
        if self.mode != "none" and not self.reference_sqlite:
            raise ValueError(
                "comparison.reference_sqlite is required when comparison.mode is enabled"
            )
        if self.mode == "legacy" and self.include_provenance:
            raise ValueError("Legacy comparison cannot compare v4 provenance")
        return self


class ScenarioLifetimes(MappingModel):
    survival_curves: bool
    survival_curve_max_age: int = Field(gt=0)


class ScenarioRowNoteOverrides(MappingModel):
    technology: dict[str, str]


class ScenarioExistingCapacity(MappingModel):
    vehicle_population_year: int = Field(gt=0)
    cleanup_tolerance: float = Field(ge=0, allow_inf_nan=False)


class ScenarioDemand(MappingModel):
    cer_scenario: str = Field(min_length=1)
    future_car_demand: Literal["GDP-indexed", "extrapolated"]


class ScenarioRoadUtilization(MappingModel):
    vkt_schedules: bool
    vkt_max_age: int = Field(gt=0)


class ScenarioEfficiencies(MappingModel):
    atb_trajectory: str = Field(min_length=1)


class ScenarioCosts(MappingModel):
    atb_trajectory: str = Field(min_length=1)


class ScenarioEvChargers(MappingModel):
    ld_evs_per_port: float = Field(gt=0, allow_inf_nan=False)
    mhd_evs_per_port: float = Field(gt=0, allow_inf_nan=False)


class ScenarioChargingProfiles(MappingModel):
    travel_behavior_source: Literal["none", "nhts", "tts"]
    # An explicit mapping can project physical hours onto inherited time slices.
    time_mapping: str | None = Field(min_length=1)


class ScenarioRangeRepresentation(MappingModel):
    mode: Literal["none", "new_capacity_shares", "representative_archetype"]


class ScenarioConfig(MappingModel):
    version: Literal[3]
    scenario: ScenarioIdentity
    geography: ScenarioGeography
    periods: ScenarioPeriods
    sources: ScenarioSources
    aggregation_sources: AggregationSources
    economics: ScenarioEconomics
    outputs: ScenarioOutputs
    comparison: ScenarioComparison
    lifetimes: ScenarioLifetimes
    existing_capacity: ScenarioExistingCapacity
    demand: ScenarioDemand
    road_utilization: ScenarioRoadUtilization
    efficiencies: ScenarioEfficiencies
    costs: ScenarioCosts
    ev_chargers: ScenarioEvChargers
    charging_profiles: ScenarioChargingProfiles
    BEV_PHEV_range_representation: ScenarioRangeRepresentation
    embodied_emissions: bool
    embodied_materials: Literal["conventional", "lightweight"]
    row_note_overrides: ScenarioRowNoteOverrides

    @model_validator(mode="after")
    def validate_parameter_horizons(self) -> Self:
        if self.lifetimes.survival_curves and (
            self.lifetimes.survival_curve_max_age < self.periods.step - 1
        ):
            raise ValueError("lifetimes.survival_curve_max_age cannot contain a full period")
        if self.road_utilization.vkt_schedules and (
            self.road_utilization.vkt_max_age < self.periods.step
        ):
            raise ValueError("road_utilization.vkt_max_age cannot contain a full period")
        return self


class SourceComponent(MappingModel):
    """Shared component contract; ``adapter`` owns source-native extensions."""

    label: str | list[str]
    short_name: str
    inputs: list[str] = Field(default_factory=list)
    applies_to: list[str] = Field(default_factory=list)
    produces: list[str] = Field(default_factory=list)
    parameter_modules: list[str] = Field(default_factory=list)
    required: bool = True
    version: str | None = None
    dataset_key: str | None = None
    citation: str | None = None
    validation_rule: str | None = None
    units: str | None = None
    notes: str | None = None
    database_note: str | None = None
    data_quality: DataQuality | None = None
    adapter: dict[str, Any] = Field(default_factory=dict)


class SourceSpec(MappingModel):
    """Small shared source contract plus an adapter-owned extension mapping."""

    title: str
    status: Literal["active", "inactive"]
    source_type: str
    file_type: str
    version: str
    citation: str
    validation_rule: str
    refresh_notes: str
    database_note: str
    units: str | None = None
    required: bool
    data_quality: DataQuality
    components: dict[str | int, SourceComponent] = Field(default_factory=dict)
    adapter: dict[str, Any] = Field(default_factory=dict)

    def component(self, key: str | int) -> SourceComponent:
        for candidate in (key, str(key)):
            if candidate in self.components:
                return self.components[candidate]
        if isinstance(key, str) and key.isdigit() and int(key) in self.components:
            return self.components[int(key)]
        raise KeyError(key)


class SourceDefaults(MappingModel):
    """Registry-owned defaults applied before individual source validation."""

    required: bool
    component_required: bool
    data_quality: DataQuality

    @model_validator(mode="after")
    def validate_complete_fallback(self) -> Self:
        self.data_quality.row_fields()
        return self


class SourcesConfig(MappingModel):
    version: Literal[3]
    defaults: SourceDefaults
    sources: dict[str, SourceSpec]

    def resolved_data_quality(
        self, source_key: str, component_key: str | int,
    ) -> DataQuality:
        """Resolve each indicator: component override, source annotation, then fallback."""
        source = self.sources[source_key]
        component = source.component(component_key)
        values = self.defaults.data_quality.model_dump()
        for quality in (source.data_quality, component.data_quality):
            if quality is not None:
                values.update({
                    key: value for key, value in quality.model_dump().items()
                    if value is not None
                })
        return DataQuality.model_validate(values)

    @model_validator(mode="before")
    @classmethod
    def apply_registry_defaults(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        defaults = value.get("defaults")
        sources = value.get("sources")
        if not isinstance(defaults, dict) or not isinstance(sources, dict):
            return value
        if not {"required", "component_required"} <= defaults.keys():
            return value
        resolved = deepcopy(value)
        resolved_defaults = resolved["defaults"]
        for source in resolved["sources"].values():
            if not isinstance(source, dict):
                continue
            source.setdefault("required", resolved_defaults["required"])
            components = source.get("components", {})
            if not isinstance(components, dict):
                continue
            for component in components.values():
                if isinstance(component, dict):
                    component.setdefault(
                        "required", resolved_defaults["component_required"]
                    )
        return resolved

    @model_validator(mode="after")
    def validate_registry(self) -> Self:
        if not self.sources:
            raise ValueError("sources.yaml must define at least one source")
        if len(self.sources) > 99:
            raise ValueError("sources.yaml supports at most 99 stable Txx source IDs")
        return self
