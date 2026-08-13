"""Fetch and source-normalize Ontario vehicle population Reports A, 4, and 5."""

import argparse
import hashlib
import io
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.config_models import SourceComponent, SourceSpec


SOURCE_ID = "ontario_ministry_transport_vehicle_population"
ZIP_MEMBER_SEPARATOR = "\t"


class CkanLookupRequest(BaseModel):
    """Validated request required before CKAN package metadata I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str
    package_id: str
    year: int = Field(gt=1900)
    selector: dict[str, Any]
    cache_path: Path

    @model_validator(mode="after")
    def validate_lookup(self) -> "CkanLookupRequest":
        _validate_http_url(self.base_url, label="CKAN base URL")
        if not self.package_id:
            raise ValueError("CKAN package ID is required")
        _validate_cache_path(self.cache_path)
        return self


class DiscoveredOntarioResource(BaseModel):
    """One annual Ontario CKAN ZIP resource."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    year: int = Field(gt=1900)
    resource_id: str = Field(min_length=1)
    resource_name: str = Field(min_length=1)
    url: str
    resource_format: str
    created: str = ""
    last_modified: str = ""

    @model_validator(mode="after")
    def validate_resource(self) -> "DiscoveredOntarioResource":
        _validate_http_url(self.url, label="Ontario vehicle archive URL")
        if self.resource_format.casefold() != "zip":
            raise ValueError(
                f"Ontario vehicle annual resource must be ZIP, got {self.resource_format}"
            )
        return self


class OntarioVehiclePopulationRequest(BaseModel):
    """One year-specific Ontario vehicle population archive request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: Literal["ontario_ministry_transport_vehicle_population"]
    year: int = Field(gt=1900)
    package_id: str = Field(min_length=1)
    resource_id: str = Field(min_length=1)
    resource_name: str = Field(min_length=1)
    url: str
    cache_path: Path
    report_a_members: tuple[str, ...] = Field(min_length=1)
    report4_member: str = Field(min_length=1)
    report5_member: str = Field(min_length=1)
    archive_depth: int = Field(default=1, ge=0, le=1)
    resource_created: str = ""
    resource_last_modified: str = ""

    @model_validator(mode="after")
    def validate_request(self) -> "OntarioVehiclePopulationRequest":
        _validate_http_url(self.url, label="Ontario vehicle archive URL")
        _validate_cache_path(self.cache_path)
        return self


@dataclass(frozen=True)
class ArchiveMember:
    """Resolved direct or one-level nested ZIP member."""

    outer_name: str
    inner_name: str | None = None

    @property
    def display_name(self) -> str:
        if self.inner_name is None:
            return self.outer_name
        return f"{self.outer_name}!{self.inner_name}"


def _validate_http_url(value: str, *, label: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{label} is invalid: {value}")


def _validate_cache_path(path: Path) -> None:
    if not path.is_absolute() or path.suffix.casefold() != ".zip":
        raise ValueError(f"Ontario vehicle cache path must be an absolute .zip path: {path}")


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load Ontario vehicle population source-normalization rules."""
    return load_harmonization_rules(bundle, "ontario_vehicle_population")


def configured_year(source: SourceSpec) -> int | None:
    """Return a configured numeric year, or ``None`` when latest is requested."""
    years = source.adapter.get("years", {})
    selected: Any
    if isinstance(years, dict):
        selected = years.get("default", "latest")
    else:
        selected = years
    if str(selected).casefold() == "latest":
        return None
    return int(selected)


def ckan_package_show_url(base_url: str, package_id: str) -> str:
    """Build a CKAN package_show URL."""
    return f"{base_url.rstrip('/')}/api/3/action/package_show?id={package_id}"


def fetch_ckan_package_metadata(
    base_url: str,
    package_id: str,
    *,
    timeout: int = 60,
) -> dict[str, Any]:
    """Fetch CKAN package metadata."""
    response = requests.get(
        ckan_package_show_url(base_url, package_id),
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success", False):
        raise ValueError(f"CKAN package_show failed for {package_id}")
    return payload


def package_resources(package_metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Return CKAN resources from either a full API payload or result mapping."""
    result = package_metadata.get("result", package_metadata)
    resources = result.get("resources", [])
    if not isinstance(resources, list):
        raise ValueError("CKAN package metadata has no resources list")
    return [resource for resource in resources if isinstance(resource, dict)]


def resource_field_text(resource: dict[str, Any], fields: list[str]) -> str:
    """Join selected CKAN resource fields for reusable text matching."""
    return " ".join(str(resource.get(field, "")) for field in fields)


def discover_ckan_resources(
    package_metadata: dict[str, Any],
    *,
    selector: dict[str, Any],
) -> list[DiscoveredOntarioResource]:
    """Discover and sort every unambiguous annual Ontario vehicle ZIP."""
    expected_format = str(selector.get("format", "zip")).casefold()
    year_fields = [str(field) for field in selector.get("year_fields", ["name", "url"])]
    year_pattern = re.compile(str(selector.get("year_pattern", r"(?<!\d)(20\d{2})(?!\d)")))
    discovered: dict[int, DiscoveredOntarioResource] = {}

    for resource in package_resources(package_metadata):
        resource_format = str(resource.get("format", "")).casefold()
        if expected_format and resource_format != expected_format:
            continue
        if not resource.get("url"):
            continue
        years = {
            int(match)
            for match in year_pattern.findall(resource_field_text(resource, year_fields))
        }
        if not years:
            continue
        if len(years) != 1:
            raise ValueError(
                "Ontario CKAN resource has ambiguous report years: "
                f"{resource.get('name', resource.get('id', ''))}: {sorted(years)}"
            )
        year = years.pop()
        candidate = DiscoveredOntarioResource(
            year=year,
            resource_id=str(resource.get("id", "")),
            resource_name=str(resource.get("name", "")),
            url=str(resource["url"]),
            resource_format=str(resource.get("format", "")),
            created=str(resource.get("created", "")),
            last_modified=str(resource.get("last_modified", "")),
        )
        if year in discovered:
            raise ValueError(
                f"Multiple CKAN resources matched year {year}: "
                f"{discovered[year].resource_name}, {candidate.resource_name}"
            )
        discovered[year] = candidate

    if not discovered:
        raise ValueError("No annual Ontario vehicle population ZIP resources were discovered")
    return [discovered[year] for year in sorted(discovered)]


def missing_resource_years(
    resources: list[DiscoveredOntarioResource],
) -> list[int]:
    """Return gaps between the oldest and latest discovered annual resources."""
    years = {resource.year for resource in resources}
    return [
        year
        for year in range(min(years), max(years) + 1)
        if year not in years
    ]


def select_ckan_resource(
    package_metadata: dict[str, Any],
    *,
    year: int,
    selector: dict[str, Any],
) -> dict[str, Any]:
    """Select one annual resource by parsed report year."""
    matches = [
        resource
        for resource in discover_ckan_resources(package_metadata, selector=selector)
        if resource.year == year
    ]
    if not matches:
        raise ValueError(f"No CKAN resource matched year {year}")
    match = matches[0]
    return {
        "id": match.resource_id,
        "name": match.resource_name,
        "format": match.resource_format,
        "url": match.url,
        "created": match.created,
        "last_modified": match.last_modified,
        "year": match.year,
    }


def render_cache_path(
    bundle: ConfigBundle,
    source: SourceSpec,
    *,
    year: int,
) -> Path:
    """Render the deterministic cache path for one annual ZIP."""
    template = source.adapter.get(
        "cache_path_template",
        source.adapter.get("path_template"),
    )
    if template is None:
        raise KeyError("Ontario vehicle population source missing cache_path_template")
    path = str(template).format(year=year)
    normalized_path = path.replace("\\", "/")
    for cache_root in ("inputs/cache/", "inputs/0_cache/"):
        if normalized_path.startswith(cache_root):
            path = normalized_path.removeprefix(cache_root)
            break
    return resolve_input_path(bundle, "cache", path)


def source_components(source: SourceSpec) -> dict[str | int, SourceComponent]:
    """Return Ontario vehicle population report components."""
    components = source.components
    required = ("A", 4, 5)
    for key in required:
        try:
            source.component(key)
        except KeyError as exc:
            raise ValueError(
                "Ontario vehicle population source must define Reports A, 4, and 5"
            ) from exc
    return components


def _component(source: SourceSpec, key: str | int) -> SourceComponent:
    component = source.component(key)
    if not isinstance(component, SourceComponent):
        raise TypeError(f"Ontario vehicle population component {key!r} is invalid")
    return component


def _report_members(
    source: SourceSpec,
    *,
    year: int,
) -> tuple[tuple[str, ...], str, str, int]:
    report_a = _component(source, "A")
    report4 = _component(source, 4)
    report5 = _component(source, 5)
    a_templates = report_a.adapter.get("raw_member_templates", [])
    if not isinstance(a_templates, list) or not a_templates:
        raise ValueError("Ontario Report A requires raw_member_templates")
    report_a_members = tuple(str(template).format(year=year) for template in a_templates)
    report4_member = str(report4.adapter["raw_member_template"]).format(year=year)
    report5_member = str(report5.adapter["raw_member_template"]).format(year=year)
    archive_depth = int(report_a.adapter.get("archive_depth", 1))
    return report_a_members, report4_member, report5_member, archive_depth


def request_from_resource(
    bundle: ConfigBundle,
    resource: DiscoveredOntarioResource,
) -> OntarioVehiclePopulationRequest:
    """Build one validated annual request from a discovered resource."""
    source = bundle.sources["sources"][SOURCE_ID]
    source_components(source)
    report_a_members, report4_member, report5_member, archive_depth = _report_members(
        source,
        year=resource.year,
    )
    access = source.adapter["access"]
    return OntarioVehiclePopulationRequest(
        source_id=SOURCE_ID,
        year=resource.year,
        package_id=str(access["package_id"]),
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        url=resource.url,
        cache_path=render_cache_path(bundle, source, year=resource.year),
        report_a_members=report_a_members,
        report4_member=report4_member,
        report5_member=report5_member,
        archive_depth=archive_depth,
        resource_created=resource.created,
        resource_last_modified=resource.last_modified,
    )


def build_request(
    bundle: ConfigBundle,
    *,
    year: int | None = None,
    package_metadata: dict[str, Any] | None = None,
) -> OntarioVehiclePopulationRequest:
    """Build one annual request; omitted year means latest discovered edition."""
    source = bundle.sources["sources"][SOURCE_ID]
    selected_year = year if year is not None else configured_year(source)
    access = source.adapter["access"]
    if selected_year is not None:
        lookup = CkanLookupRequest(
            base_url=str(access["ckan_base_url"]),
            package_id=str(access["package_id"]),
            year=selected_year,
            selector=access["resource_selector"],
            cache_path=render_cache_path(
                bundle,
                source,
                year=selected_year,
            ),
        )
        selected_year = lookup.year
    metadata = package_metadata or fetch_ckan_package_metadata(
        str(access["ckan_base_url"]),
        str(access["package_id"]),
    )
    resources = discover_ckan_resources(
        metadata,
        selector=access["resource_selector"],
    )
    if selected_year is None:
        resource = resources[-1]
    else:
        matched = [candidate for candidate in resources if candidate.year == selected_year]
        if not matched:
            raise ValueError(f"No CKAN resource matched year {selected_year}")
        resource = matched[0]
    return request_from_resource(bundle, resource)


def build_requests(
    bundle: ConfigBundle,
    package_metadata: dict[str, Any],
    *,
    year: int | None = None,
) -> tuple[list[OntarioVehiclePopulationRequest], list[int]]:
    """Build all discovered annual requests, or one explicit annual request."""
    source = bundle.sources["sources"][SOURCE_ID]
    access = source.adapter["access"]
    resources = discover_ckan_resources(
        package_metadata,
        selector=access["resource_selector"],
    )
    report_rules = module_rules(bundle)["reports"]["A"]
    excluded_years = {
        int(value) for value in report_rules.get("excluded_years", [])
    }
    gaps = [
        gap for gap in missing_resource_years(resources) if gap not in excluded_years
    ]
    if year is not None:
        if year in excluded_years:
            raise ValueError(
                f"Ontario Report A edition {year} is excluded by configuration"
            )
        resources = [resource for resource in resources if resource.year == year]
        if not resources:
            raise ValueError(f"No CKAN resource matched year {year}")
    else:
        resources = [
            resource
            for resource in resources
            if resource.year not in excluded_years
        ]
    return [request_from_resource(bundle, resource) for resource in resources], gaps


def sha256_file(path: Path) -> str:
    """Return a streaming SHA-256 digest."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_zip_integrity(path: Path) -> None:
    """Validate a non-empty ZIP and every member CRC."""
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size <= 0:
        raise ValueError(f"Ontario vehicle archive is empty: {path}")
    try:
        with ZipFile(path) as archive:
            bad_member = archive.testzip()
    except BadZipFile as exc:
        raise ValueError(f"Ontario vehicle archive is not a valid ZIP: {path}") from exc
    if bad_member is not None:
        raise ValueError(f"Ontario vehicle archive has a bad member {bad_member}: {path}")


def fetch_to_cache(
    request: OntarioVehiclePopulationRequest,
    *,
    timeout: int = 120,
) -> str:
    """Download one annual ZIP atomically, or validate and reuse its cache."""
    if request.cache_path.exists():
        validate_zip_integrity(request.cache_path)
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = request.cache_path.with_suffix(request.cache_path.suffix + ".part")
    if temporary.exists():
        temporary.unlink()
    try:
        response = requests.get(request.url, timeout=timeout)
        response.raise_for_status()
        temporary.write_bytes(response.content)
        validate_zip_integrity(temporary)
        os.replace(temporary, request.cache_path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return "downloaded"


def _basename_matches(names: list[str], candidates: tuple[str, ...]) -> list[str]:
    candidate_names = {Path(candidate).name.casefold() for candidate in candidates}
    return [
        name
        for name in names
        if Path(name.replace("\\", "/")).name.casefold() in candidate_names
    ]


def resolve_archive_member(
    cache_path: Path,
    candidates: tuple[str, ...],
    *,
    max_depth: int = 1,
) -> ArchiveMember:
    """Resolve one configured report member, allowing one nested ZIP."""
    with ZipFile(cache_path) as archive:
        names = archive.namelist()
        direct = _basename_matches(names, candidates)
        if len(direct) == 1:
            return ArchiveMember(direct[0])
        if len(direct) > 1:
            raise ValueError(
                f"Multiple ZIP members matched {', '.join(candidates)}: {', '.join(direct)}"
            )

        nested_matches: list[ArchiveMember] = []
        if max_depth >= 1:
            for nested_name in names:
                if not nested_name.casefold().endswith(".zip"):
                    continue
                try:
                    with ZipFile(io.BytesIO(archive.read(nested_name))) as nested:
                        matches = _basename_matches(nested.namelist(), candidates)
                except BadZipFile as exc:
                    raise ValueError(
                        f"Nested Ontario archive is not a valid ZIP: "
                        f"{cache_path}!{nested_name}"
                    ) from exc
                nested_matches.extend(
                    ArchiveMember(nested_name, match)
                    for match in matches
                )
        if len(nested_matches) == 1:
            return nested_matches[0]
        if len(nested_matches) > 1:
            displays = ", ".join(match.display_name for match in nested_matches)
            raise ValueError(
                f"Multiple nested ZIP members matched {', '.join(candidates)}: {displays}"
            )

    raise FileNotFoundError(
        f"None of {', '.join(candidates)} found in {cache_path}"
    )


def resolve_zip_member_name(cache_path: Path, member_name: str) -> str:
    """Resolve a direct ZIP member by case-insensitive unique basename."""
    return resolve_archive_member(
        cache_path,
        (member_name,),
        max_depth=0,
    ).display_name


def _read_archive_member(cache_path: Path, member: ArchiveMember) -> bytes:
    with ZipFile(cache_path) as archive:
        if member.inner_name is None:
            return archive.read(member.outer_name)
        nested_bytes = archive.read(member.outer_name)
    with ZipFile(io.BytesIO(nested_bytes)) as nested:
        return nested.read(member.inner_name)


def read_report_from_zip(
    cache_path: Path,
    member_name: str | ArchiveMember,
) -> pd.DataFrame:
    """Read one tab-delimited report from a direct or nested ZIP member."""
    member = (
        member_name
        if isinstance(member_name, ArchiveMember)
        else resolve_archive_member(cache_path, (member_name,), max_depth=1)
    )
    content = _read_archive_member(cache_path, member)
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = content.decode(encoding)
            return pd.read_csv(
                io.StringIO(text),
                sep=ZIP_MEMBER_SEPARATOR,
                dtype=str,
                keep_default_na=False,
            )
        except UnicodeDecodeError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def read_vehicle_population_txt(path: Path) -> pd.DataFrame:
    """Read the supplied local physical-format fixture."""
    return pd.read_csv(
        path,
        sep=ZIP_MEMBER_SEPARATOR,
        dtype=str,
        keep_default_na=False,
    )


def validate_source(
    request: OntarioVehiclePopulationRequest,
    *,
    require_auxiliary: bool = True,
) -> dict[str, ArchiveMember]:
    """Validate one cached ZIP and resolve its required report members."""
    validate_zip_integrity(request.cache_path)
    resolved = {
        "A": resolve_archive_member(
            request.cache_path,
            request.report_a_members,
            max_depth=request.archive_depth,
        )
    }
    if require_auxiliary:
        resolved["4"] = resolve_archive_member(
            request.cache_path,
            (request.report4_member,),
            max_depth=request.archive_depth,
        )
        resolved["5"] = resolve_archive_member(
            request.cache_path,
            (request.report5_member,),
            max_depth=request.archive_depth,
        )
    return resolved


def _canonicalize_headers(
    raw: pd.DataFrame,
    *,
    aliases: dict[str, str],
) -> pd.DataFrame:
    renamed: dict[str, str] = {}
    for column in raw.columns:
        stripped = str(column).strip()
        normalized = re.sub(r"[^A-Z0-9]+", "_", stripped.upper()).strip("_")
        renamed[column] = str(aliases.get(stripped, aliases.get(normalized, normalized)))
    frame = raw.rename(columns=renamed)
    duplicates = frame.columns[frame.columns.duplicated()].tolist()
    if duplicates:
        raise ValueError(
            "Report A header aliases produced duplicate columns: "
            + ", ".join(map(str, duplicates))
        )
    return frame


def _finding_rows(
    normalized: pd.DataFrame,
    *,
    report_year: int,
    issue_type: str,
    mask: pd.Series,
    detail: str,
    class_column: str,
    fit_active_column: str,
) -> list[dict[str, Any]]:
    selected = normalized.loc[mask.fillna(False)]
    if selected.empty:
        return []
    output: list[dict[str, Any]] = []
    for vehicle_class, rows in selected.groupby(class_column, dropna=False):
        stock = pd.to_numeric(rows[fit_active_column], errors="coerce").sum(min_count=1)
        output.append(
            {
                "report_year": report_year,
                "original_mto_class": vehicle_class,
                "issue_type": issue_type,
                "row_count": int(len(rows)),
                "fit_active_stock": stock,
                "detail": detail,
            }
        )
    return output


def _code_quality_masks(
    values: pd.Series,
    *,
    report_rules: dict[str, Any],
) -> tuple[pd.Series, pd.Series, pd.Series]:
    normalized_values = values.fillna("").astype(str).str.strip()
    blank = normalized_values.eq("")
    suppressed = pd.Series(False, index=normalized_values.index)
    for pattern in report_rules.get("suppressed_code_patterns", []):
        suppressed |= normalized_values.str.fullmatch(re.compile(str(pattern)))
    unknown_labels = {
        str(label).strip().upper()
        for label in report_rules.get("unknown_code_labels", [])
    }
    unknown = normalized_values.str.upper().isin(unknown_labels)
    return blank, suppressed, unknown


def make_model_key_inventory(
    normalized_frames: Iterable[pd.DataFrame],
    *,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Summarize unique nonsuppressed Report A make-model keys across editions.

    The output remains source-native: it describes MTO keys and their observed
    support without attaching or inferring a target vehicle class.  Future
    provincial vehicle-population adapters can publish the same review handoff
    after normalizing their source-specific columns.
    """
    report_rules = rules["reports"]["A"]
    class_column = str(report_rules["vehicle_class_column"])
    make_column = str(report_rules["make_column"])
    model_column = str(report_rules["model_column"])
    model_year_column = str(report_rules["model_year_column"])
    kept_classes = {
        str(value).strip().upper()
        for value in report_rules["kept_vehicle_classes"]
    }
    required_columns = [
        "source_id",
        "report_year",
        class_column,
        make_column,
        model_column,
        model_year_column,
        "FIT_ACTIVE",
    ]
    selected_frames: list[pd.DataFrame] = []
    for frame in normalized_frames:
        missing = sorted(set(required_columns) - set(frame.columns))
        if missing:
            raise ValueError(
                "Normalized vehicle-population frame missing inventory columns: "
                + ", ".join(missing)
            )
        selected = frame.loc[
            frame[class_column].astype(str).str.upper().isin(kept_classes),
            required_columns,
        ].copy()
        make_masks = _code_quality_masks(
            selected[make_column],
            report_rules=report_rules,
        )
        model_masks = _code_quality_masks(
            selected[model_column],
            report_rules=report_rules,
        )
        usable = ~(
            make_masks[0]
            | make_masks[1]
            | make_masks[2]
            | model_masks[0]
            | model_masks[1]
            | model_masks[2]
        )
        selected = selected.loc[usable].copy()
        if selected.empty:
            continue
        selected[make_column] = selected[make_column].astype(str).str.strip()
        selected[model_column] = selected[model_column].astype(str).str.strip()
        selected[class_column] = selected[class_column].astype(str).str.strip()
        selected["report_year"] = pd.to_numeric(
            selected["report_year"], errors="raise"
        ).astype(int)
        selected[model_year_column] = pd.to_numeric(
            selected[model_year_column], errors="coerce"
        )
        selected["FIT_ACTIVE"] = pd.to_numeric(
            selected["FIT_ACTIVE"], errors="coerce"
        ).fillna(0)
        selected_frames.append(selected)

    output_columns = [
        "source_id",
        "mto_make_code",
        "mto_model_code",
        "source_vehicle_classes",
        "first_report_year",
        "last_report_year",
        "observed_report_editions",
        "report_years",
        "model_year_from",
        "model_year_to",
        "observed_model_years",
        "source_rows_across_editions",
        "fit_active_stock_across_editions",
        "latest_report_year",
        "latest_fit_active_stock",
    ]
    if not selected_frames:
        return pd.DataFrame(columns=output_columns)

    combined = pd.concat(selected_frames, ignore_index=True)

    def joined_values(values: pd.Series) -> str:
        present = values.dropna().astype(str).str.strip()
        return " | ".join(sorted(set(present.loc[present.ne("")])))

    def joined_years(values: pd.Series) -> str:
        years = sorted(
            pd.to_numeric(values, errors="coerce")
            .dropna()
            .astype(int)
            .unique()
        )
        return " | ".join(map(str, years))

    group_columns = ["source_id", make_column, model_column]
    inventory = combined.groupby(
        group_columns,
        as_index=False,
        dropna=False,
    ).agg(
        source_vehicle_classes=(class_column, joined_values),
        first_report_year=("report_year", "min"),
        last_report_year=("report_year", "max"),
        observed_report_editions=("report_year", "nunique"),
        report_years=("report_year", joined_years),
        model_year_from=(model_year_column, "min"),
        model_year_to=(model_year_column, "max"),
        observed_model_years=(model_year_column, "nunique"),
        source_rows_across_editions=("report_year", "size"),
        fit_active_stock_across_editions=("FIT_ACTIVE", "sum"),
    )
    latest_report_year = int(combined["report_year"].max())
    latest_stock = (
        combined.loc[combined["report_year"].eq(latest_report_year)]
        .groupby(group_columns, as_index=False, dropna=False)["FIT_ACTIVE"]
        .sum(min_count=1)
        .rename(columns={"FIT_ACTIVE": "latest_fit_active_stock"})
    )
    inventory = inventory.merge(
        latest_stock,
        on=group_columns,
        how="left",
        validate="one_to_one",
    )
    inventory["latest_fit_active_stock"] = inventory[
        "latest_fit_active_stock"
    ].fillna(0)
    inventory["latest_report_year"] = latest_report_year
    inventory = inventory.rename(
        columns={
            make_column: "mto_make_code",
            model_column: "mto_model_code",
        }
    )
    return inventory.loc[:, output_columns].sort_values(
        ["mto_make_code", "mto_model_code"],
        kind="stable",
    ).reset_index(drop=True)


def report_a_cohort_usability(
    normalized: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> tuple[bool, str]:
    """Determine whether an edition contains any classifiable retained keys."""
    report_rules = rules["reports"]["A"]
    class_column = str(report_rules["vehicle_class_column"])
    make_column = str(report_rules["make_column"])
    model_column = str(report_rules["model_column"])
    model_year_column = str(report_rules["model_year_column"])
    kept_classes = {
        str(value).strip().upper()
        for value in report_rules["kept_vehicle_classes"]
    }
    retained = normalized[class_column].isin(kept_classes)
    make_masks = _code_quality_masks(
        normalized[make_column],
        report_rules=report_rules,
    )
    model_masks = _code_quality_masks(
        normalized[model_column],
        report_rules=report_rules,
    )
    unusable_make = make_masks[0] | make_masks[1] | make_masks[2]
    unusable_model = model_masks[0] | model_masks[1] | model_masks[2]
    classifiable = (
        retained
        & ~unusable_make
        & ~unusable_model
        & normalized[model_year_column].notna()
    )
    if classifiable.any():
        return True, "Retained make/model/model-year keys are available."
    return (
        False,
        "No retained row has a nonsuppressed make/model/model-year key; "
        "retain for audit but exclude from cohort transitions.",
    )


def normalize_report_a(
    raw: pd.DataFrame,
    *,
    source_id: str,
    year: int,
    raw_file: str,
    cached_zip: Path,
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Normalize one Report A-equivalent table at source grain."""
    report_rules = rules["reports"]["A"]
    aliases = {
        str(source): str(target)
        for source, target in report_rules["header_aliases"].items()
    }
    normalized = _canonicalize_headers(raw, aliases=aliases)
    required = [str(column) for column in report_rules["required_columns"]]
    missing = sorted(set(required) - set(normalized.columns))
    if missing:
        raise ValueError(f"Report A missing columns: {', '.join(missing)}")
    normalized = normalized.loc[:, required].copy()

    class_column = str(report_rules["vehicle_class_column"])
    make_column = str(report_rules["make_column"])
    model_column = str(report_rules["model_column"])
    model_year_column = str(report_rules["model_year_column"])
    status_columns = [str(column) for column in report_rules["status_columns"]]
    total_column = str(report_rules["total_column"])
    normalized[class_column] = normalized[class_column].astype(str).str.strip().str.upper()
    normalized[make_column] = normalized[make_column].astype(str).str.strip().str.upper()
    normalized[model_column] = normalized[model_column].astype(str).str.strip().str.upper()

    findings: list[dict[str, Any]] = []
    raw_model_year = normalized[model_year_column].copy()
    normalized[model_year_column] = pd.to_numeric(
        raw_model_year,
        errors="coerce",
    ).astype("Int64")
    invalid_year = normalized[model_year_column].isna() & raw_model_year.astype(str).str.strip().ne("")

    invalid_count = pd.Series(False, index=normalized.index)
    for column in [*status_columns, total_column]:
        source_values = normalized[column].copy()
        numeric = pd.to_numeric(source_values, errors="coerce")
        invalid_count |= numeric.isna() & source_values.astype(str).str.strip().ne("")
        normalized[column] = numeric.astype("Int64")

    kept_classes = {
        str(value).strip().upper()
        for value in report_rules["kept_vehicle_classes"]
    }
    kept_class_mask = normalized[class_column].isin(kept_classes)
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="excluded_vehicle_class",
            mask=~kept_class_mask,
            detail=(
                "Vehicle class discarded before Report A normalization; "
                f"kept classes are {', '.join(sorted(kept_classes))}."
            ),
            class_column=class_column,
            fit_active_column="FIT_ACTIVE",
        )
    )
    normalized = normalized.loc[kept_class_mask].copy()
    invalid_year = invalid_year.loc[normalized.index]
    invalid_count = invalid_count.loc[normalized.index]

    key_columns = [class_column, make_column, model_column, model_year_column]
    normalized["duplicate_source_key"] = normalized.duplicated(
        key_columns,
        keep=False,
    )
    normalized["STATUS_SUM"] = normalized[status_columns].sum(axis=1, min_count=1)
    normalized["STATUS_TOTAL_DIFFERENCE"] = (
        normalized["STATUS_SUM"] - normalized[total_column]
    )
    normalized["AGE"] = year - normalized[model_year_column]
    normalized.insert(0, "source_id", source_id)
    normalized.insert(1, "report_year", year)
    normalized.insert(2, "physical_report", raw_file)
    normalized.insert(3, "source_row", range(2, len(normalized) + 2))
    normalized.insert(4, "cached_zip", str(cached_zip))

    fit_active_column = "FIT_ACTIVE"
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="invalid_model_year",
            mask=invalid_year,
            detail="Nonblank model-year value could not be parsed.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="invalid_or_suppressed_status_count",
            mask=invalid_count,
            detail="One or more nonblank registration-status counts were nonnumeric.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="duplicate_source_key",
            mask=normalized["duplicate_source_key"],
            detail="Duplicate MTO class, make, model, and model-year source key.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="unreconciled_status_total",
            mask=normalized["STATUS_TOTAL_DIFFERENCE"].fillna(0).ne(0),
            detail="Sum of registration-status columns differs from source TOTAL.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="model_year_after_report_year",
            mask=normalized[model_year_column].gt(year),
            detail="Model year is later than the annual report year; row retained.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="pre_2000_stock",
            mask=normalized[model_year_column].lt(2000),
            detail="Pre-2000 model-year stock retained in the source-normalized audit table.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="stock_over_age_30",
            mask=normalized["AGE"].gt(30),
            detail="Stock older than 30 years retained; truncation is parameterization-owned.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )
    findings.extend(
        _finding_rows(
            normalized,
            report_year=year,
            issue_type="newest_model_year_cohort",
            mask=normalized[model_year_column].eq(year),
            detail="Report-year model cohort may be incomplete; row retained.",
            class_column=class_column,
            fit_active_column=fit_active_column,
        )
    )

    for column, label in ((make_column, "make"), (model_column, "model")):
        blank, suppressed, unknown = _code_quality_masks(
            normalized[column],
            report_rules=report_rules,
        )
        findings.extend(
            _finding_rows(
                normalized,
                report_year=year,
                issue_type=f"blank_{label}_code",
                mask=blank,
                detail=f"Blank MTO {label} code retained.",
                class_column=class_column,
                fit_active_column=fit_active_column,
            )
        )
        findings.extend(
            _finding_rows(
                normalized,
                report_year=year,
                issue_type=f"suppressed_{label}_code",
                mask=suppressed,
                detail=f"Suppressed MTO {label} code retained.",
                class_column=class_column,
                fit_active_column=fit_active_column,
            )
        )
        findings.extend(
            _finding_rows(
                normalized,
                report_year=year,
                issue_type=f"unknown_{label}_code",
                mask=unknown,
                detail=f"Unknown MTO {label} code retained.",
                class_column=class_column,
                fit_active_column=fit_active_column,
            )
        )

    if re.search(r"report[\s_-]*a(?:_|&|\.|$)", raw_file, flags=re.IGNORECASE) is None:
        findings.extend(
            _finding_rows(
                normalized,
                report_year=year,
                issue_type="source_layout_variant",
                mask=pd.Series(True, index=normalized.index),
                detail=(
                    f"Official annual member {raw_file!r} uses a verified "
                    "Report A-equivalent legacy name/header layout."
                ),
                class_column=class_column,
                fit_active_column=fit_active_column,
            )
        )
    cohort_usable, cohort_reason = report_a_cohort_usability(
        normalized,
        rules=rules,
    )
    if not cohort_usable:
        findings.extend(
            _finding_rows(
                normalized,
                report_year=year,
                issue_type="cohort_snapshot_unusable",
                mask=pd.Series(True, index=normalized.index),
                detail=cohort_reason,
                class_column=class_column,
                fit_active_column=fit_active_column,
            )
        )

    source_grain = [class_column, make_column, model_column, model_year_column]
    grouped = normalized.groupby(source_grain, dropna=False, sort=False)
    status_wide = grouped[[*status_columns, total_column]].sum(
        min_count=1
    ).reset_index()
    status_wide["source_row_count"] = grouped.size().to_numpy()
    status_wide.insert(0, "source_id", source_id)
    status_wide.insert(1, "report_year", year)
    status_long = status_wide.melt(
        id_vars=[
            "source_id",
            "report_year",
            class_column,
            make_column,
            model_column,
            model_year_column,
            "source_row_count",
        ],
        value_vars=[*status_columns, total_column],
        var_name="stock_status",
        value_name="native_count",
    )
    status_long = (
        status_long.sort_values(
            [
                "report_year",
                class_column,
                make_column,
                model_column,
                model_year_column,
                "stock_status",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    source_status = normalized.melt(
        id_vars=[class_column],
        value_vars=[*status_columns, total_column],
        var_name="stock_status",
        value_name="source_count",
    )
    source_status = (
        source_status.groupby(
            [class_column, "stock_status"],
            dropna=False,
            as_index=False,
        )["source_count"]
        .sum(min_count=1)
    )
    long_status_totals = (
        status_long.groupby(
            [class_column, "stock_status"],
            dropna=False,
            as_index=False,
        )["native_count"]
        .sum(min_count=1)
        .rename(columns={"native_count": "long_count"})
    )
    reconciliation = source_status.merge(
        long_status_totals,
        on=[class_column, "stock_status"],
        how="outer",
        validate="one_to_one",
    )
    reconciliation.insert(0, "report_year", year)
    reconciliation["difference"] = (
        reconciliation["long_count"] - reconciliation["source_count"]
    )
    reconciliation["reconciled"] = reconciliation["difference"].fillna(0).eq(0)

    findings_frame = pd.DataFrame(
        findings,
        columns=[
            "report_year",
            "original_mto_class",
            "issue_type",
            "row_count",
            "fit_active_stock",
            "detail",
        ],
    )
    return normalized, status_long, reconciliation, findings_frame


def normalize_report4(
    raw: pd.DataFrame,
    *,
    source_id: str,
    year: int,
    raw_file: str,
    cached_zip: Path,
    rules: dict[str, Any],
    kg_to_lb: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean Report 4 and aggregate commercial vehicles into EPA GVWR bins."""
    report_rules = rules["reports"][4]
    required = report_rules["required_columns"]
    missing = sorted(set(required) - set(raw.columns))
    if missing:
        raise ValueError(f"Report 4 missing columns: {', '.join(missing)}")

    weight_class_column = report_rules["weight_class_column"]
    kg_from_column = report_rules["kg_from_column"]
    kg_to_column = report_rules["kg_to_column"]
    count_column = report_rules["count_column"]
    kept_weight_class = str(report_rules["kept_weight_class"]).upper()
    cleaned = raw.loc[
        raw[weight_class_column].str.upper() == kept_weight_class,
        required,
    ].copy()
    cleaned[kg_from_column] = pd.to_numeric(cleaned[kg_from_column], errors="coerce")
    cleaned[kg_to_column] = pd.to_numeric(cleaned[kg_to_column], errors="coerce")
    cleaned[count_column] = pd.to_numeric(cleaned[count_column], errors="coerce")
    cleaned = cleaned.dropna(subset=[kg_from_column, kg_to_column, count_column])
    cleaned["LB_FROM"] = cleaned[kg_from_column] * kg_to_lb
    cleaned["LB_TO"] = cleaned[kg_to_column] * kg_to_lb
    gvwr_column = report_rules["epa_gvwr_column"]
    cleaned[gvwr_column] = pd.cut(
        cleaned["LB_TO"],
        bins=report_rules["epa_gvwr_bin_edges_lb"],
        labels=report_rules["epa_gvwr_labels"],
    )
    cleaned = cleaned.dropna(subset=[gvwr_column])
    cleaned.insert(0, "source_id", source_id)
    cleaned.insert(1, "year", year)
    cleaned.insert(2, "report", report_rules["report_label"])
    cleaned.insert(3, "raw_file", raw_file)
    cleaned["cached_zip"] = str(cached_zip)

    output_count_column = report_rules["output_count_column"]
    distribution = (
        cleaned.groupby(gvwr_column, observed=False, as_index=False)[count_column]
        .sum()
        .rename(columns={count_column: output_count_column})
    )
    total = distribution[output_count_column].sum()
    distribution[report_rules["share_column"]] = (
        distribution[output_count_column] / total
        if total
        else 0.0
    )
    distribution.insert(0, "source_id", source_id)
    distribution.insert(1, "year", year)
    distribution.insert(2, "report", report_rules["report_label"])
    distribution.insert(3, "raw_file", raw_file)
    distribution["cached_zip"] = str(cached_zip)
    return cleaned, distribution


def normalize_report5(
    raw: pd.DataFrame,
    *,
    source_id: str,
    year: int,
    raw_file: str,
    cached_zip: Path,
    rules: dict[str, Any],
    max_age: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retain Report 5 audit rows and produce its legacy truncated distribution."""
    report_rules = rules["reports"][5]
    required = report_rules["required_columns"]
    missing = sorted(set(required) - set(raw.columns))
    if missing:
        raise ValueError(f"Report 5 missing columns: {', '.join(missing)}")

    vehicle_class_column = report_rules["vehicle_class_column"]
    descriptor_column = report_rules["descriptor_column"]
    model_year_column = report_rules["model_year_column"]
    count_column = report_rules["count_column"]
    kept_descriptor = str(report_rules["kept_descriptor"]).upper()
    kept_classes = [
        str(vehicle_class).upper()
        for vehicle_class in report_rules["kept_vehicle_classes"]
    ]
    selected_max_age = int(
        max_age if max_age is not None else report_rules["max_age"]
    )

    cleaned = raw.loc[
        (raw[descriptor_column].str.upper() == kept_descriptor)
        & (raw[vehicle_class_column].str.upper().isin(kept_classes)),
        required,
    ].copy()
    cleaned[vehicle_class_column] = cleaned[vehicle_class_column].str.upper()
    cleaned[model_year_column] = pd.to_numeric(
        cleaned[model_year_column],
        errors="coerce",
    )
    cleaned[count_column] = pd.to_numeric(cleaned[count_column], errors="coerce")
    cleaned = cleaned.dropna(subset=[model_year_column, count_column])
    cleaned[model_year_column] = cleaned[model_year_column].astype(int)
    cleaned["MODEL_YEAR_AFTER_REPORT_YEAR"] = cleaned[model_year_column] > year
    age_column = report_rules["age_column"]
    cleaned[age_column] = year - cleaned[model_year_column]
    cleaned = cleaned.sort_values(
        [vehicle_class_column, age_column],
        kind="stable",
    ).reset_index(drop=True)
    cleaned.insert(0, "source_id", source_id)
    cleaned.insert(1, "year", year)
    cleaned.insert(2, "report", report_rules["report_label"])
    cleaned.insert(3, "raw_file", raw_file)
    cleaned["cached_zip"] = str(cached_zip)

    distribution = cleaned.loc[
        ~cleaned["MODEL_YEAR_AFTER_REPORT_YEAR"]
        & cleaned[age_column].le(selected_max_age)
    ].copy()
    output_count_column = report_rules["output_count_column"]
    distribution = distribution.rename(
        columns={count_column: output_count_column}
    )
    totals = distribution.groupby(vehicle_class_column)[
        output_count_column
    ].transform("sum")
    age_distribution_column = report_rules["age_distribution_column"]
    distribution[age_distribution_column] = (
        distribution[output_count_column] / totals
    )
    return cleaned, distribution[
        [
            "source_id",
            "year",
            "report",
            "raw_file",
            "cached_zip",
            vehicle_class_column,
            age_column,
            model_year_column,
            output_count_column,
            age_distribution_column,
        ]
    ]


def current_stock_input(
    normalized: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Aggregate latest retained stock by source class and MTO key."""
    report_rules = rules["reports"]["A"]
    class_column = str(report_rules["vehicle_class_column"])
    make_column = str(report_rules["make_column"])
    model_column = str(report_rules["model_column"])
    model_year_column = str(report_rules["model_year_column"])
    status_columns = [str(column) for column in report_rules["status_columns"]]
    total_column = str(report_rules["total_column"])
    kept_classes = {
        str(value).strip().upper()
        for value in report_rules["kept_vehicle_classes"]
    }
    selected = normalized.loc[
        normalized[class_column].isin(kept_classes)
    ].copy()
    group_columns = [
        "source_id",
        "report_year",
        class_column,
        make_column,
        model_column,
        model_year_column,
    ]
    return (
        selected.groupby(group_columns, dropna=False, as_index=False)[
            [*status_columns, total_column]
        ]
        .sum(min_count=1)
        .sort_values(
            ["report_year", make_column, model_column, model_year_column],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def write_dataframe_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Write one CSV and atomically publish it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    try:
        frame.to_csv(temporary, index=False)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_text_atomic(text: str, path: Path) -> None:
    """Write one UTF-8 text artifact and atomically publish it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _manifest_requests(
    bundle: ConfigBundle,
    manifest_path: Path,
    *,
    year: int | None,
) -> list[OntarioVehiclePopulationRequest]:
    if not manifest_path.is_file():
        raise FileNotFoundError(
            "Offline Ontario vehicle replay requires the source manifest: "
            f"{manifest_path}"
        )
    manifest = pd.read_csv(manifest_path, dtype=str, keep_default_na=False)
    excluded_years = {
        int(value)
        for value in module_rules(bundle)["reports"]["A"].get(
            "excluded_years",
            [],
        )
    }
    if year is not None and year in excluded_years:
        raise ValueError(
            f"Ontario Report A edition {year} is excluded by configuration"
        )
    required = {
        "year",
        "resource_id",
        "resource_name",
        "url",
        "resource_format",
        "resource_created",
        "resource_last_modified",
        "cache_sha256",
    }
    missing = sorted(required - set(manifest.columns))
    if missing:
        raise ValueError(
            "Ontario vehicle source manifest missing columns: "
            + ", ".join(missing)
        )
    requests_from_manifest: list[OntarioVehiclePopulationRequest] = []
    for row in manifest.to_dict("records"):
        report_year = int(row["year"])
        if report_year in excluded_years:
            continue
        if year is not None and report_year != year:
            continue
        resource = DiscoveredOntarioResource(
            year=report_year,
            resource_id=row["resource_id"],
            resource_name=row["resource_name"],
            url=row["url"],
            resource_format=row["resource_format"],
            created=row["resource_created"],
            last_modified=row["resource_last_modified"],
        )
        request = request_from_resource(bundle, resource)
        if not request.cache_path.is_file():
            raise FileNotFoundError(request.cache_path)
        actual_sha = sha256_file(request.cache_path)
        if actual_sha != row["cache_sha256"]:
            raise ValueError(
                f"Cached Ontario archive SHA-256 changed for {report_year}: "
                f"{actual_sha} != {row['cache_sha256']}"
            )
        requests_from_manifest.append(request)
    if not requests_from_manifest:
        raise ValueError(
            f"Ontario vehicle source manifest has no rows for requested year {year}"
        )
    return sorted(requests_from_manifest, key=lambda request: request.year)


def _manifest_row(
    request: OntarioVehiclePopulationRequest,
    *,
    resolved: dict[str, ArchiveMember],
    output_name: str,
    cohort_snapshot_usable: bool,
    cohort_snapshot_usability_reason: str,
) -> dict[str, Any]:
    return {
        "source_id": request.source_id,
        "year": request.year,
        "package_id": request.package_id,
        "resource_id": request.resource_id,
        "resource_name": request.resource_name,
        "resource_format": "ZIP",
        "resource_created": request.resource_created,
        "resource_last_modified": request.resource_last_modified,
        "url": request.url,
        "cached_zip": str(request.cache_path),
        "cache_bytes": request.cache_path.stat().st_size,
        "cache_sha256": sha256_file(request.cache_path),
        "cache_status": "validated",
        "report_a_member": resolved["A"].display_name,
        "report4_member": resolved.get("4", ArchiveMember("")).display_name,
        "report5_member": resolved.get("5", ArchiveMember("")).display_name,
        "normalized_report_a_output": output_name,
        "cohort_snapshot_usable": cohort_snapshot_usable,
        "cohort_snapshot_usability_reason": cohort_snapshot_usability_reason,
    }


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    year: int | None = None,
    download: bool = True,
    package_metadata: dict[str, Any] | None = None,
    max_age: int | None = None,
) -> Path:
    """Fetch/cache all usable editions and publish source-normalized artifacts."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    conversions = load_conversion_factors(bundle)
    kg_to_lb = float(conversions["mass"]["kg_to_lb"])
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    manifest_path = output_dir / str(rules["manifest_file"])
    gaps: list[int] = []

    if download or package_metadata is not None:
        source = bundle.sources["sources"][SOURCE_ID]
        access = source.adapter["access"]
        metadata = package_metadata or fetch_ckan_package_metadata(
            str(access["ckan_base_url"]),
            str(access["package_id"]),
        )
        requests_to_process, gaps = build_requests(
            bundle,
            metadata,
            year=year,
        )
    else:
        requests_to_process = _manifest_requests(
            bundle,
            manifest_path,
            year=year,
        )

    requests_to_process = sorted(
        requests_to_process,
        key=lambda request: request.year,
    )
    current_year = max(request.year for request in requests_to_process)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_a_rules = rules["reports"]["A"]
    for excluded_year in report_a_rules.get("excluded_years", []):
        excluded_output = output_dir / str(
            report_a_rules["normalized_output_template"]
        ).format(year=int(excluded_year))
        if excluded_output.is_file():
            excluded_output.unlink()
            logging.info(
                "Removed excluded Ontario Report A interim artifact: %s",
                excluded_output,
            )
    long_path = output_dir / str(rules["long_status_file"])
    long_temporary = long_path.with_suffix(long_path.suffix + ".tmp")
    if long_temporary.exists():
        long_temporary.unlink()
    normalized_current: pd.DataFrame | None = None
    inventory_frames: list[pd.DataFrame] = []
    inventory_columns = [
        "source_id",
        "report_year",
        str(report_a_rules["vehicle_class_column"]),
        str(report_a_rules["make_column"]),
        str(report_a_rules["model_column"]),
        str(report_a_rules["model_year_column"]),
        "FIT_ACTIVE",
    ]
    reconciliation_frames: list[pd.DataFrame] = []
    finding_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    report4_cleaned = pd.DataFrame()
    report4_distribution = pd.DataFrame()
    report5_cleaned = pd.DataFrame()
    report5_distribution = pd.DataFrame()

    try:
        for request_index, request in enumerate(requests_to_process):
            if download:
                cache_status = fetch_to_cache(request)
                logging.info(
                    "Ontario vehicle %s archive %s: %s",
                    request.year,
                    cache_status,
                    request.cache_path,
                )
            else:
                validate_zip_integrity(request.cache_path)
            resolved = validate_source(
                request,
                require_auxiliary=request.year == current_year,
            )
            report_a_raw = read_report_from_zip(request.cache_path, resolved["A"])
            normalized, status_long, reconciliation, findings = normalize_report_a(
                report_a_raw,
                source_id=request.source_id,
                year=request.year,
                raw_file=resolved["A"].display_name,
                cached_zip=request.cache_path,
                rules=rules,
            )
            cohort_usable, cohort_reason = report_a_cohort_usability(
                normalized,
                rules=rules,
            )
            output_name = rules["reports"]["A"][
                "normalized_output_template"
            ].format(year=request.year)
            write_dataframe_atomic(normalized, output_dir / output_name)
            inventory_frames.append(normalized.loc[:, inventory_columns].copy())
            status_long.to_csv(
                long_temporary,
                mode="a",
                header=request_index == 0,
                index=False,
            )
            reconciliation_frames.append(reconciliation)
            finding_frames.append(findings)
            manifest_rows.append(
                _manifest_row(
                    request,
                    resolved=resolved,
                    output_name=output_name,
                    cohort_snapshot_usable=cohort_usable,
                    cohort_snapshot_usability_reason=cohort_reason,
                )
            )
            if request.year == current_year:
                normalized_current = normalized
                report4_raw = read_report_from_zip(
                    request.cache_path,
                    resolved["4"],
                )
                report5_raw = read_report_from_zip(
                    request.cache_path,
                    resolved["5"],
                )
                report4_cleaned, report4_distribution = normalize_report4(
                    report4_raw,
                    source_id=request.source_id,
                    year=request.year,
                    raw_file=resolved["4"].display_name,
                    cached_zip=request.cache_path,
                    rules=rules,
                    kg_to_lb=kg_to_lb,
                )
                report5_cleaned, report5_distribution = normalize_report5(
                    report5_raw,
                    source_id=request.source_id,
                    year=request.year,
                    raw_file=resolved["5"].display_name,
                    cached_zip=request.cache_path,
                    rules=rules,
                    max_age=max_age,
                )
        os.replace(long_temporary, long_path)
    finally:
        if long_temporary.exists():
            long_temporary.unlink()

    reconciliation = pd.concat(reconciliation_frames, ignore_index=True)
    findings = pd.concat(finding_frames, ignore_index=True)
    for gap in gaps:
        findings.loc[len(findings)] = {
            "report_year": gap,
            "original_mto_class": "",
            "issue_type": "missing_annual_snapshot",
            "row_count": 0,
            "fit_active_stock": 0,
            "detail": "No annual ZIP resource was present in CKAN metadata.",
        }
    if normalized_current is None:
        raise AssertionError("Latest Ontario Report A was not normalized")
    current_stock = current_stock_input(normalized_current, rules=rules)
    write_dataframe_atomic(
        reconciliation,
        output_dir / str(rules["reconciliation_file"]),
    )
    write_dataframe_atomic(
        findings,
        output_dir / str(rules["findings_file"]),
    )
    write_dataframe_atomic(
        current_stock,
        output_dir / str(rules["current_stock_file"]),
    )
    if year is None:
        key_inventory = make_model_key_inventory(inventory_frames, rules=rules)
        write_dataframe_atomic(
            key_inventory,
            output_dir / str(rules["all_edition_make_model_keys_file"]),
        )
        logging.info(
            "Published %d unique nonsuppressed MTO make-model keys across %d editions",
            len(key_inventory),
            len(requests_to_process),
        )
    else:
        logging.info(
            "Skipped all-edition MTO key inventory during single-year override %d",
            year,
        )
    report4_rules = rules["reports"][4]
    report5_rules = rules["reports"][5]
    write_dataframe_atomic(
        report4_cleaned,
        output_dir
        / report4_rules["cleaned_output_template"].format(year=current_year),
    )
    write_dataframe_atomic(
        report4_distribution,
        output_dir
        / report4_rules["distribution_output_template"].format(year=current_year),
    )
    write_dataframe_atomic(
        report5_cleaned,
        output_dir
        / report5_rules["cleaned_output_template"].format(year=current_year),
    )
    write_dataframe_atomic(
        report5_distribution,
        output_dir
        / report5_rules["distribution_output_template"].format(year=current_year),
    )
    manifest = pd.DataFrame(manifest_rows).sort_values("year").reset_index(drop=True)
    write_dataframe_atomic(manifest, manifest_path)
    warning_lines = [
        f"{row.issue_type}: year={row.report_year}, "
        f"class={row.original_mto_class}, rows={row.row_count}, "
        f"fit_active={row.fit_active_stock}"
        for row in findings.itertuples(index=False)
    ]
    write_text_atomic(
        "\n".join(warning_lines) + ("\n" if warning_lines else ""),
        output_dir / str(rules["warnings_file"]),
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional single-edition override; omitted processes every usable edition.",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=None,
        help="Legacy Report 5 comparison only; Report A remains untruncated.",
    )
    parser.add_argument("--no-download", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    output_dir = fetch_and_normalize(
        args.scenario,
        year=args.year,
        download=not args.no_download,
        max_age=args.max_age,
    )
    logging.info(
        "Wrote Ontario vehicle population interim outputs to %s",
        output_dir,
    )


if __name__ == "__main__":
    main()
