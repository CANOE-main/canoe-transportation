"""Strict typed models for the transportation YAML control layer."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
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
    """The five v4 data-quality scores, defaulting to unreviewed score 5."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dq_cred: DataQualityCredibilityLevel = DataQualityCredibilityLevel(5)
    dq_geog: DataQualityGeographyLevel = DataQualityGeographyLevel(5)
    dq_struc: DataQualityStructureLevel = DataQualityStructureLevel(5)
    dq_tech: DataQualityTechnologyLevel = DataQualityTechnologyLevel(5)
    dq_time: DataQualityTimeLevel = DataQualityTimeLevel(5)

    def row_fields(self) -> dict[str, int]:
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
        "interim",
        "processed",
        "input_validation",
        "database",
        "output_validation",
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
            "interim": self.inputs.interim,
            "processed": self.inputs.processed,
            "input_validation": self.inputs.validation,
            "database": self.outputs.sqlite,
            "output_validation": self.outputs.validation,
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


class ScenarioIdentity(MappingModel):
    name: str
    description: str
    purpose: str


class ScenarioGeography(MappingModel):
    regions: list[str] = Field(min_length=1)


class ScenarioPeriods(MappingModel):
    base_year: int = Field(gt=0)
    existing: list[int] = Field(default_factory=list)
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
        return self

    def all_years(self) -> list[int]:
        return list(dict.fromkeys([*self.existing, self.base_year, *self.model]))


class ScenarioSourceSelection(MappingModel):
    year: int | None = Field(default=None, gt=0)
    edition: int | None = Field(default=None, gt=0)
    scenario: str | None = None
    trajectory: str | None = None


class ScenarioSources(MappingModel):
    active: list[str] = Field(min_length=1)
    selections: dict[str, ScenarioSourceSelection] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_source_selections(self) -> Self:
        if self.active != list(dict.fromkeys(self.active)):
            raise ValueError("sources.active must contain unique source keys")
        inactive = sorted(set(self.selections) - set(self.active))
        if inactive:
            raise ValueError(
                f"sources.selections contains inactive source keys: {inactive}"
            )
        return self


class ScenarioCurrency(MappingModel):
    target: str = Field(min_length=3, max_length=3)
    target_year: int = Field(gt=0)
    inflation_index: str

    @model_validator(mode="after")
    def validate_currency_code(self) -> Self:
        if not self.target.isalpha() or not self.target.isupper():
            raise ValueError("currency.target must be a three-letter uppercase code")
        return self


class ScenarioEconomics(MappingModel):
    global_discount_rate: float = Field(ge=0.0, le=1.0)
    default_loan_rate: float = Field(ge=0.0, le=1.0)


class ScenarioOutputs(MappingModel):
    sqlite_name: str
    validation_report: str
    setup_log: str


class ScenarioValidation(MappingModel):
    behavior: Literal["error", "warn"] = "error"
    reference_sqlite: str | None = None
    compare_legacy: bool = False
    parameter_tolerances: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_reference_and_tolerances(self) -> Self:
        if self.compare_legacy and not self.reference_sqlite:
            raise ValueError(
                "validation.reference_sqlite is required when compare_legacy is true"
            )
        invalid = {
            key: value
            for key, value in self.parameter_tolerances.items()
            if value < 0
        }
        if invalid:
            raise ValueError(
                f"validation.parameter_tolerances must be non-negative: {invalid}"
            )
        return self


class ScenarioSwitches(MappingModel):
    legacy_equivalent: bool
    debug: bool = False
    download_sources: bool
    compile_sqlite: bool
    transform_parameters: bool
    include_existing_capacity: bool = True
    survival_curves: bool = False
    survival_curve_max_age: int = Field(default=30, gt=0)


class ScenarioRowNoteOverrides(MappingModel):
    technology: dict[str, str] = Field(default_factory=dict)
    parameters: dict[str, str] = Field(default_factory=dict)


class ScenarioPlanned(MappingModel):
    weather_year: int | None = Field(default=None, gt=0)
    timezone: str | None = None
    technology_progress: str | None = None
    demand_projection: str | None = None
    fuel_price_future: str | None = None
    utilization: str | None = None
    bev_charging: str | None = None
    adoption_constraints: str | None = None
    retirement_formulation: str | None = None
    emissions_scope: str | None = None
    sector_coupling: str | None = None
    capacity_limits: str | None = None


class ScenarioConfig(MappingModel):
    version: int
    scenario: ScenarioIdentity
    geography: ScenarioGeography
    periods: ScenarioPeriods
    sources: ScenarioSources
    currency: ScenarioCurrency
    economics: ScenarioEconomics
    outputs: ScenarioOutputs
    validation: ScenarioValidation
    switches: ScenarioSwitches
    row_note_overrides: ScenarioRowNoteOverrides = Field(
        default_factory=ScenarioRowNoteOverrides
    )
    planned: ScenarioPlanned = Field(default_factory=ScenarioPlanned)


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
    units: str | None = None
    required: bool = True
    data_quality: DataQuality = Field(default_factory=DataQuality)
    components: dict[str | int, SourceComponent] = Field(default_factory=dict)
    adapter: dict[str, Any] = Field(default_factory=dict)

    def component(self, key: str | int) -> SourceComponent:
        for candidate in (key, str(key)):
            if candidate in self.components:
                return self.components[candidate]
        if isinstance(key, str) and key.isdigit() and int(key) in self.components:
            return self.components[int(key)]
        raise KeyError(key)


class SourcesConfig(MappingModel):
    version: int
    sources: dict[str, SourceSpec]

    @model_validator(mode="after")
    def validate_registry(self) -> Self:
        if not self.sources:
            raise ValueError("sources.yaml must define at least one source")
        if len(self.sources) > 99:
            raise ValueError("sources.yaml supports at most 99 stable Txx source IDs")
        return self
