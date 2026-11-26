"""Fetch and normalize NRCan CEUD transportation tables."""

from __future__ import annotations

import argparse
import hashlib
import logging
import math
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.config_models import SourceComponent, SourceSpec


SOURCE_PROVINCIAL = "nrcan_ceud_transport_provincial"
SOURCE_NATIONAL = "nrcan_ceud_transport_national"
SOURCE_RATINGS = "nrcan_fuel_consumption_ratings"
LOGGER = logging.getLogger(__name__)


class CeudWorkbookSnapshot(BaseModel):
    """Reviewed workbook bytes within a CEUD edition, independently of cache location."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0, strict=True)
    url: str | None = None
    cache_path: str | None = Field(default=None, min_length=1)


_SNAPSHOTS = TypeAdapter(dict[
    Annotated[int, Field(ge=1900, strict=True)],
    dict[
        Annotated[str, Field(pattern=r"^[a-z]{2,3}$")],
        dict[Annotated[int, Field(gt=0, strict=True)], CeudWorkbookSnapshot],
    ],
])


class CeudTableRequest(BaseModel):
    """One CEUD table file requested from the source registry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: Literal[
        "nrcan_ceud_transport_provincial",
        "nrcan_ceud_transport_national",
    ]
    region: str
    year: int
    table_id: int
    table_meta: SourceComponent
    url: str
    cache_path: Path
    output_region: str
    retry_attempts: int = Field(ge=0)
    retry_backoff_seconds: float = Field(ge=0, allow_inf_nan=False)
    expected_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int | None = Field(default=None, gt=0, strict=True)

    def artifact_description(self) -> str:
        return (
            f"CEUD source {self.source_id}/{self.table_id} "
            f"(edition {self.year}, region {self.output_region}, URL {self.url}, "
            f"cache {self.cache_path})"
        )

    @model_validator(mode="after")
    def validate_request(self) -> "CeudTableRequest":
        parsed = urlparse(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"CEUD URL is not an absolute HTTP(S) URL: {self.url}")
        if self.year < 1900 or self.table_id <= 0:
            raise ValueError("CEUD year and table_id must be positive source identifiers")
        if not self.cache_path.is_absolute() or self.cache_path.suffix.casefold() != ".xls":
            raise ValueError(f"CEUD cache path must be an absolute .xls path: {self.cache_path}")
        if not self.region or not self.output_region:
            raise ValueError("CEUD region fields cannot be blank")
        if (self.expected_sha256 is None) != (self.expected_bytes is None):
            raise ValueError("CEUD workbook identity requires both expected_sha256 and expected_bytes")
        return self


class FuelConsumptionRatingRequest(BaseModel):
    """One pinned English Fuel Consumption Ratings CSV request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: Literal["nrcan_fuel_consumption_ratings"]
    component_key: str
    component_meta: SourceComponent
    resource_id: str
    resource_title: str
    url: str
    cache_path: Path
    expected_md5: str
    expected_bytes: int
    expected_model_years: tuple[int, ...]
    required_columns: tuple[str, ...]
    required_non_null_columns: tuple[str, ...]
    output_file: str

    @model_validator(mode="after")
    def validate_request(self) -> "FuelConsumptionRatingRequest":
        parsed = urlparse(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "open.canada.ca"
            or not parsed.path.endswith(".csv")
        ):
            raise ValueError(
                "Fuel Consumption Ratings URL must be an official HTTPS CSV URL"
            )
        if not re.fullmatch(
            r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}",
            self.resource_id,
        ):
            raise ValueError(
                f"Fuel Consumption Ratings resource_id is invalid: {self.resource_id}"
            )
        if not re.fullmatch(r"[0-9a-f]{32}", self.expected_md5):
            raise ValueError("Fuel Consumption Ratings expected_md5 must be lowercase MD5")
        if self.expected_bytes <= 0:
            raise ValueError("Fuel Consumption Ratings expected_bytes must be positive")
        if (
            not self.cache_path.is_absolute()
            or self.cache_path.suffix.casefold() != ".csv"
        ):
            raise ValueError(
                "Fuel Consumption Ratings cache path must be an absolute .csv path: "
                f"{self.cache_path}"
            )
        if (
            Path(self.output_file).name != self.output_file
            or Path(self.output_file).suffix.casefold() != ".csv"
        ):
            raise ValueError(
                "Fuel Consumption Ratings output_file must be a CSV filename"
            )
        if (
            not self.expected_model_years
            or list(self.expected_model_years)
            != sorted(set(self.expected_model_years))
            or min(self.expected_model_years) < 1900
        ):
            raise ValueError(
                "Fuel Consumption Ratings model years must be sorted and unique"
            )
        if not self.required_columns or not self.required_non_null_columns:
            raise ValueError(
                "Fuel Consumption Ratings physical column contracts cannot be empty"
            )
        missing_non_null = sorted(
            set(self.required_non_null_columns) - set(self.required_columns)
        )
        if missing_non_null:
            raise ValueError(
                "Fuel Consumption Ratings non-null columns are not required columns: "
                f"{missing_non_null}"
            )
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load CEUD harmonization rules."""
    return load_harmonization_rules(bundle, "nrcan_ceud")


def ratings_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load Fuel Consumption Ratings acquisition and output rules."""
    return load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")


def clean_label(value: object, rules: dict[str, Any]) -> str | None:
    """Clean CEUD labels using configured legacy-equivalent rules."""
    if value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value):
        return None
    if str(value).strip().lower() in set(rules["null_labels"]):
        return None
    text = str(value).translate(str.maketrans("", "", str(rules["remove_label_characters"])))
    text = unicodedata.normalize("NFKD", text)
    allowed = str(rules["allowed_label_characters"])
    cleaned = "".join(char for char in text if char.isalnum() or char in allowed)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def table_label_to_string(label: object) -> str:
    """Render a table label consistently for CSV output."""
    if isinstance(label, list):
        return "|".join(str(item) for item in label)
    return str(label)


def extract_unit(raw_series: object) -> str | None:
    """Extract the last parenthesized unit token from a CEUD raw series label."""
    matches = re.findall(r"\(([^()]*)\)", str(raw_series))
    if not matches:
        return None
    unit = matches[-1].strip()
    return unit or None


def is_value_bearing_group_header(
    label: str,
    request: CeudTableRequest,
    rules: dict[str, Any],
) -> bool:
    """Return true for CEUD rows that are both a value row and a parent header."""
    configured = rules.get("group_header_value_rows", {})
    labels = configured.get(request.table_id, configured.get(str(request.table_id), []))
    return label in labels


def render_ceud_url(source: SourceSpec, *, year: int, region: str, table_id: int) -> str:
    """Render a CEUD URL from a source registry entry."""
    return str(source.adapter["url_template"]).format(
        year=year,
        region=region.lower(),
        table_id=table_id,
    )


def render_cache_path(
    bundle: ConfigBundle,
    source: SourceSpec,
    *,
    year: int,
    region: str,
    table_id: int,
) -> Path:
    """Render the deterministic raw cache path for one CEUD table."""
    template = source.adapter.get(
        "cache_path_template", source.adapter.get("path_template")
    )
    if template is None:
        raise KeyError("CEUD source missing cache_path_template")
    path = template.format(year=year, region=region.lower(), table_id=table_id)
    normalized_path = str(path).replace("\\", "/")
    for cache_root in ("inputs/cache/", "inputs/0_cache/"):
        if normalized_path.startswith(cache_root):
            path = normalized_path.removeprefix(cache_root)
            break
    return resolve_input_path(bundle, "cache", path)


def render_ratings_cache_path(
    bundle: ConfigBundle,
    component: SourceComponent,
) -> Path:
    """Resolve one pinned Fuel Consumption Ratings cache path."""
    configured_path = str(component.adapter["cache_path"])
    normalized_path = configured_path.replace("\\", "/")
    for cache_root in ("inputs/cache/", "inputs/0_cache/"):
        if normalized_path.startswith(cache_root):
            configured_path = normalized_path.removeprefix(cache_root)
            break
    return resolve_input_path(bundle, "cache", configured_path)


def iter_rating_requests(bundle: ConfigBundle) -> list[FuelConsumptionRatingRequest]:
    """Build exact English rating-resource requests from the source registry."""
    source = bundle.sources["sources"][SOURCE_RATINGS]
    rules = ratings_rules(bundle)
    outputs = rules["outputs"]
    component_keys = list(source.components)
    missing_outputs = sorted(set(component_keys) - set(outputs))
    unexpected_outputs = sorted(set(outputs) - set(component_keys))
    if missing_outputs or unexpected_outputs:
        raise ValueError(
            "Fuel Consumption Ratings output mapping does not match source components: "
            f"missing={missing_outputs}, unexpected={unexpected_outputs}"
        )

    requests_to_fetch: list[FuelConsumptionRatingRequest] = []
    resource_ids: list[str] = []
    for component_key, component in source.components.items():
        adapter = component.adapter
        request = FuelConsumptionRatingRequest(
            source_id=SOURCE_RATINGS,
            component_key=str(component_key),
            component_meta=component,
            resource_id=str(adapter["resource_id"]),
            resource_title=str(component.label),
            url=str(adapter["url"]),
            cache_path=render_ratings_cache_path(bundle, component),
            expected_md5=str(adapter["expected_md5"]),
            expected_bytes=int(adapter["expected_bytes"]),
            expected_model_years=tuple(
                int(year) for year in adapter["expected_model_years"]
            ),
            required_columns=tuple(
                str(column) for column in adapter["required_columns"]
            ),
            required_non_null_columns=tuple(
                str(column) for column in adapter["required_non_null_columns"]
            ),
            output_file=str(outputs[component_key]),
        )
        requests_to_fetch.append(request)
        resource_ids.append(request.resource_id)

    if len(resource_ids) != len(set(resource_ids)):
        raise ValueError("Fuel Consumption Ratings resource IDs must be unique")
    return requests_to_fetch


def scenario_source_year(bundle: ConfigBundle, source_id: str) -> int:
    """Resolve the required scenario-owned source year."""
    selection = bundle.scenario.sources.selections.get(source_id)
    if selection is not None and selection.year is not None:
        return selection.year
    raise ValueError(f"sources.selections.{source_id}.year is required")


def provincial_regions(source: SourceSpec, requested_regions: list[str]) -> list[str]:
    """Resolve requested provincial regions against the source registry."""
    allowed = [str(region).upper() for region in source.adapter["regions"]["allowed"]]
    if not requested_regions:
        raise ValueError("Select at least one CEUD provincial region")
    unknown = sorted(set(region.upper() for region in requested_regions) - set(allowed))
    if unknown:
        raise ValueError(f"Unknown CEUD provincial regions: {', '.join(unknown)}")
    return [region.upper() for region in requested_regions]


def iter_table_requests(
    bundle: ConfigBundle,
    *,
    regions: list[str] | None = None,
    include_national: bool = True,
    year: int | None = None,
) -> list[CeudTableRequest]:
    """Build concrete CEUD table requests from sources.yaml."""
    sources = bundle.sources["sources"]
    provincial = sources[SOURCE_PROVINCIAL]
    provincial_year = year if year is not None else scenario_source_year(bundle, SOURCE_PROVINCIAL)
    selected_regions = bundle.scenario.geography.regions if regions is None else regions
    requests_to_fetch = _edition_requests(
        bundle, SOURCE_PROVINCIAL, provincial_year,
        {region.lower(): region for region in provincial_regions(provincial, selected_regions)},
    )
    if include_national:
        national_year = year if year is not None else scenario_source_year(bundle, SOURCE_NATIONAL)
        requests_to_fetch.extend(_edition_requests(bundle, SOURCE_NATIONAL, national_year, {"ca": "national"}))

    if len({request.cache_path for request in requests_to_fetch}) != len(requests_to_fetch):
        raise ValueError("CEUD requests resolve to duplicate cache paths")
    return requests_to_fetch


def _edition_requests(
    bundle: ConfigBundle, source_id: str, year: int, regions: dict[str, str]
) -> list[CeudTableRequest]:
    source = bundle.sources.sources[source_id]
    snapshots = _SNAPSHOTS.validate_python(source.adapter.get("registered_snapshots", {}))
    edition = snapshots.get(year)
    cache_root = resolve_input_path(bundle, "cache").resolve()
    requests = []
    for region, output_region in regions.items():
        region_snapshots = edition.get(region, {}) if edition is not None else {}
        for table_id, table_meta in sorted(source.components.items()):
            table_id_int = int(table_id)
            snapshot = region_snapshots.get(table_id_int)
            key = f"{source_id} edition {year}, region {region}, table {table_id_int}"
            if edition is not None and snapshot is None and table_meta.required:
                raise ValueError(f"Missing registered CEUD workbook identity for {key}")
            if snapshot is not None and snapshot.cache_path is not None:
                relative = Path(snapshot.cache_path)
                if relative.anchor or ".." in relative.parts:
                    raise ValueError(f"Registered CEUD cache path must be cache-relative for {key}")
                cache_path = (cache_root / relative).resolve()
            else:
                cache_path = render_cache_path(bundle, source, year=year, region=region, table_id=table_id_int).resolve()
            if not cache_path.is_relative_to(cache_root):
                raise ValueError(f"CEUD cache path must stay inside the configured cache for {key}")
            requests.append(CeudTableRequest(
                source_id=source_id, region=region, output_region=output_region, year=year,
                table_id=table_id_int, table_meta=table_meta,
                retry_attempts=source.adapter["retry_attempts"],
                retry_backoff_seconds=source.adapter["retry_backoff_seconds"],
                url=snapshot.url if snapshot and snapshot.url is not None else
                    render_ceud_url(source, year=year, region=region, table_id=table_id_int),
                cache_path=cache_path,
                expected_sha256=snapshot.expected_sha256 if snapshot else None,
                expected_bytes=snapshot.expected_bytes if snapshot else None,
            ))
    return requests


def fetch_to_cache(
    request: CeudTableRequest, *, timeout: int = 60, skiprows: int | None = None
) -> str:
    """Publish a complete, readable CEUD workbook without caching failed responses."""
    if request.cache_path.exists():
        if request.expected_sha256 is not None:
            _validate_workbook_identity(request, request.cache_path)
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=request.cache_path.parent, suffix=".xls", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        retries = Retry(
            total=request.retry_attempts,
            connect=request.retry_attempts,
            read=request.retry_attempts,
            status=request.retry_attempts,
            backoff_factor=request.retry_backoff_seconds,
            allowed_methods=frozenset({"GET"}),
            status_forcelist=(429, 500, 502, 503, 504),
        )
        with requests.Session() as session:
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            with session.get(request.url, timeout=timeout) as response:
                response.raise_for_status()
                temporary.write_bytes(response.content)
                raw = getattr(response, "raw", None)
                history = getattr(getattr(raw, "retries", None), "history", ())
                if history:
                    logging.warning(
                        "CEUD %s recovered after %s transient HTTP retries", request.url, len(history)
                    )
        # A publisher/proxy can return an HTML error page with status 200. Do not
        # leave those bytes at the immutable cache path and poison every rerun.
        if request.expected_sha256 is not None:
            _validate_workbook_identity(request, temporary)
        try:
            if skiprows is None:
                with pd.ExcelFile(temporary):
                    pass
            else:
                raw = read_ceud_excel(temporary, skiprows=skiprows)
                _validate_source_shape(request, raw)
        except Exception as exc:
            raise ValueError(f"Unreadable or invalid download for {request.artifact_description()}: {exc}") from exc
        os.replace(temporary, request.cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    return "downloaded"


def bytes_md5(content: bytes) -> str:
    """Return a lowercase MD5 used by the Open Government resource registry."""
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def file_md5(path: Path) -> str:
    """Hash one cached file without loading it all into memory."""
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _republished_hint(request: FuelConsumptionRatingRequest) -> str:
    """Explain a pinned-snapshot mismatch and the two ways forward."""
    return (
        "NRCan has likely republished this resource. To rebuild the registered "
        f"version, place its cached copy at {request.cache_path}. To review and adopt "
        "the current release, run `uv run python -m fetching.source_updater "
        "--scenario <scenario.yaml> --source nrcan_fuel_consumption_ratings` "
        "and add --apply after reviewing its log and registry diff."
    )


def fetch_rating_to_cache(
    request: FuelConsumptionRatingRequest,
    *,
    timeout: int = 60,
) -> str:
    """Atomically download one pinned Ratings CSV unless already cached."""
    if request.cache_path.exists():
        return "cached"

    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = request.cache_path.with_suffix(f"{request.cache_path.suffix}.part")
    if temporary.exists():
        temporary.unlink()
    try:
        response = requests.get(request.url, timeout=timeout)
        response.raise_for_status()
        content = response.content
        if len(content) != request.expected_bytes:
            raise ValueError(
                f"{request.component_key} download has {len(content)} bytes; "
                f"expected {request.expected_bytes}. {_republished_hint(request)}"
            )
        actual_md5 = bytes_md5(content)
        if actual_md5 != request.expected_md5:
            raise ValueError(
                f"{request.component_key} download MD5 is {actual_md5}; "
                f"expected {request.expected_md5}. {_republished_hint(request)}"
            )
        temporary.write_bytes(content)
        temporary.replace(request.cache_path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return "downloaded"


def validate_rating_cache(request: FuelConsumptionRatingRequest) -> tuple[int, str]:
    """Validate the byte-level identity of one pinned cached CSV."""
    context = f"{request.source_id}/{request.component_key}"
    if not request.cache_path.is_file():
        raise FileNotFoundError(
            f"Fuel Consumption Ratings source {context} is missing: "
            f"{request.cache_path}"
        )
    actual_bytes = request.cache_path.stat().st_size
    if actual_bytes != request.expected_bytes:
        raise ValueError(
            f"Fuel Consumption Ratings source {context} has {actual_bytes} bytes; "
            f"expected {request.expected_bytes}"
        )
    actual_md5 = file_md5(request.cache_path)
    if actual_md5 != request.expected_md5:
        raise ValueError(
            f"Fuel Consumption Ratings source {context} MD5 is {actual_md5}; "
            f"expected {request.expected_md5}"
        )
    return actual_bytes, actual_md5


def read_rating_csv(
    request: FuelConsumptionRatingRequest,
    *,
    encoding_candidates: list[str],
) -> tuple[pd.DataFrame, str]:
    """Read a pinned Ratings CSV with the configured deterministic fallback order."""
    attempted: list[str] = []
    for encoding in encoding_candidates:
        attempted.append(str(encoding))
        try:
            return pd.read_csv(request.cache_path, encoding=encoding), encoding
        except UnicodeDecodeError:
            continue
    raise UnicodeError(
        f"Could not decode {request.component_key} with encodings: "
        f"{', '.join(attempted)}"
    )


def normalize_rating_dataframe(
    raw: pd.DataFrame,
    request: FuelConsumptionRatingRequest,
    *,
    enforce_coverage: bool = True,
) -> pd.DataFrame:
    """Validate ratings; only explicit updater review may discover new coverage."""
    if raw.empty:
        raise ValueError(f"{request.component_key} contains no data rows")

    normalized = raw.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    duplicate_columns = sorted(
        {
            column
            for column in normalized.columns
            if list(normalized.columns).count(column) > 1
        }
    )
    if duplicate_columns:
        raise ValueError(
            f"{request.component_key} has duplicate columns after trimming: "
            f"{duplicate_columns}"
        )

    missing_columns = sorted(
        set(request.required_columns) - set(normalized.columns)
    )
    if missing_columns:
        raise ValueError(
            f"{request.component_key} is missing columns: {missing_columns}"
        )

    model_years = pd.to_numeric(normalized["Model year"], errors="coerce")
    if model_years.isna().any() or (model_years % 1 != 0).any():
        raise ValueError(f"{request.component_key} has invalid Model year values")
    normalized["Model year"] = model_years.astype(int)
    actual_model_years = tuple(
        sorted(normalized["Model year"].drop_duplicates().tolist())
    )
    if enforce_coverage and actual_model_years != request.expected_model_years:
        raise ValueError(
            f"{request.component_key} covers model years {actual_model_years}; "
            f"expected {request.expected_model_years}"
        )

    for column in request.required_non_null_columns:
        blank = (
            normalized[column].isna()
            | normalized[column].fillna("").astype(str).str.strip().eq("")
        )
        if blank.any():
            raise ValueError(
                f"{request.component_key} has {int(blank.sum())} blank {column} values"
            )

    normalized.insert(0, "source_id", request.source_id)
    normalized.insert(1, "component", request.component_key)
    normalized.insert(2, "resource_id", request.resource_id)
    normalized.insert(3, "resource_title", request.resource_title)
    normalized.insert(4, "resource_version", request.component_meta.version or "")
    normalized.insert(5, "resource_url", request.url)
    normalized.insert(6, "source_row", range(2, len(normalized) + 2))
    normalized.insert(7, "cached_file", str(request.cache_path))
    return normalized


def validate_source(request: CeudTableRequest, raw: pd.DataFrame | None = None) -> None:
    """Validate the cached CEUD artifact before harmonization."""
    _validate_workbook_identity(request, request.cache_path)
    if raw is not None:
        _validate_source_shape(request, raw)


def _validate_workbook_identity(request: CeudTableRequest, path: Path) -> tuple[int, str]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing {request.artifact_description()}")
    size = path.stat().st_size
    if size <= 0:
        raise ValueError(f"Empty {request.artifact_description()}")
    if request.expected_bytes is not None and size != request.expected_bytes:
        raise ValueError(f"Byte count mismatch for {request.artifact_description()}: "
                         f"expected {request.expected_bytes}, got {size}. Restore or review the registered workbook.")
    digest = file_sha256(path)
    if request.expected_sha256 is not None and digest != request.expected_sha256:
        raise ValueError(f"SHA-256 mismatch for {request.artifact_description()}: "
                         f"expected {request.expected_sha256}, got {digest}. Restore or review the registered workbook.")
    return size, digest


def _validate_source_shape(request: CeudTableRequest, raw: pd.DataFrame) -> None:
    context = request.artifact_description()
    if raw.empty:
        raise ValueError(f"{context} contains no data rows")
    if raw.shape[1] < 3:
        raise ValueError(f"{context} has fewer than three columns")
    if not any(str(column).strip().isdigit() for column in raw.columns):
        raise ValueError(f"{context} has no integer year columns")


def read_ceud_excel(path: Path, *, skiprows: int) -> pd.DataFrame:
    """Read a raw CEUD Excel table using the configured metadata-row offset."""
    return pd.read_excel(path, skiprows=skiprows)


def normalize_ceud_dataframe(
    raw: pd.DataFrame,
    request: CeudTableRequest,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Normalize a parsed CEUD table into traceable long form."""
    if raw.shape[1] < 3:
        raise ValueError("CEUD table has fewer than three columns")

    df = raw.copy()
    label_column = "Unnamed: 1" if "Unnamed: 1" in df.columns else df.columns[int(rules["label_column_fallback_index"])]
    drop_columns = [column for column in rules["drop_columns_if_present"] if column in df.columns]
    if drop_columns:
        df = df.drop(columns=drop_columns)

    year_columns: dict[Any, int] = {}
    for column in df.columns:
        try:
            year_columns[column] = int(str(column).strip())
        except ValueError:
            continue
    if not year_columns:
        raise ValueError("CEUD table has no integer year columns")

    df = df[[label_column, *year_columns.keys()]].rename(columns=year_columns)
    df[label_column] = df[label_column].map(lambda value: clean_label(value, rules))

    header: str | None = None
    raw_series: list[str | None] = []
    first_year = min(year_columns.values())
    for _, row in df.iterrows():
        label = row[label_column]
        if label is None or pd.isna(label):
            header = None
            raw_series.append(None)
        elif is_value_bearing_group_header(label, request, rules):
            header = label
            raw_series.append(label)
        elif pd.isna(row[first_year]):
            header = label
            raw_series.append(label)
        elif header is not None:
            raw_series.append(f"{header}|{label}")
        else:
            raw_series.append(label)
    df["raw_series"] = raw_series
    df = df.dropna(subset=["raw_series"])
    df = df[~df["raw_series"].str.contains(str(rules["drop_raw_series_pattern"]), case=False, na=False)]
    df = df.dropna(subset=year_columns.values(), how="all")

    df["raw_series"] = add_legacy_table_label_prefixes(
        df["raw_series"],
        request.table_meta.get("label", ""),
        rules,
    )

    long = df.melt(
        id_vars=["raw_series"],
        value_vars=sorted(year_columns.values()),
        var_name="year",
        value_name="value",
    )
    long["value"] = pd.to_numeric(long["value"].replace("n.a.", pd.NA), errors="coerce")
    long = long.dropna(subset=["value"])
    series_parts = long["raw_series"].str.split("|", n=1, expand=True)
    long["series_group"] = series_parts[0]
    long["series_name"] = series_parts[1].fillna(series_parts[0]) if 1 in series_parts else series_parts[0]

    table_meta = request.table_meta
    long.insert(0, "source_id", request.source_id)
    long.insert(1, "region", request.output_region)
    long.insert(2, "table_id", request.table_id)
    long.insert(3, "table_label", table_label_to_string(table_meta.get("label", "")))
    long.insert(4, "short_name", table_meta.get("short_name", ""))
    long.insert(5, "applies_to", "|".join(table_meta.get("applies_to", [])))
    long.insert(6, "parameter_modules", "|".join(table_meta.get("parameter_modules", [])))
    long["year"] = long["year"].astype(int)
    long["unit"] = long["raw_series"].map(extract_unit).fillna(table_meta.get("units", "varies"))
    long["cached_file"] = str(request.cache_path)
    return long[
        [
            "source_id",
            "region",
            "table_id",
            "table_label",
            "short_name",
            "applies_to",
            "parameter_modules",
            "raw_series",
            "series_group",
            "series_name",
            "year",
            "value",
            "unit",
            "cached_file",
        ]
    ]


def add_legacy_table_label_prefixes(
    series: pd.Series,
    table_label: object,
    rules: dict[str, Any],
) -> pd.Series:
    """Apply configured legacy Activity/Energy label prefix behavior."""
    labels: list[str] = []
    activity_count = 0
    intensity_count = 0
    label_list = table_label if isinstance(table_label, list) else None
    markers = [str(marker) for marker in rules["table_label_prefix_markers"]]
    for raw_value in series:
        value = str(raw_value)
        needs_prefix = any(marker in value for marker in markers)
        if not needs_prefix:
            labels.append(value)
            continue

        if label_list:
            if "Activity" in value:
                prefix = label_list[activity_count % len(label_list)]
                activity_count += 1
            elif "Energy Intensity" in value:
                prefix = label_list[intensity_count % len(label_list)]
                intensity_count += 1
            else:
                prefix = label_list[0]
        else:
            prefix = table_label
        labels.append(f"{prefix}|{value}")
    return pd.Series(labels, index=series.index)


def write_outputs(
    rows: list[pd.DataFrame],
    manifest_rows: list[dict[str, Any]],
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Write normalized CEUD outputs, a manifest, and warning log."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if rows:
        all_rows = pd.concat(rows, ignore_index=True)
        for region, region_rows in all_rows.groupby("region"):
            suffix = str(region).lower()
            write_dataframe_atomic(region_rows, output_dir / rules["region_output_template"].format(region=suffix))
    else:
        write_text_atomic("", output_dir / rules["empty_output_file"])

    write_text_atomic("\n".join(warnings) + ("\n" if warnings else ""), output_dir / rules["warnings_file"])
    manifest = pd.DataFrame(manifest_rows)
    for column in ("bytes", "expected_bytes"):
        manifest[column] = pd.array([row.get(column) for row in manifest_rows], dtype="Int64")
    write_dataframe_atomic(manifest, output_dir / rules["manifest_file"])


def fetch_and_normalize(
    scenario_path: str | Path | ConfigBundle,
    *,
    regions: list[str] | None = None,
    include_national: bool = True,
    year: int | None = None,
    download: bool = True,
) -> Path:
    """Fetch/cache configured CEUD tables and write normalized interim CSVs."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    requests_to_fetch = iter_table_requests(bundle, regions=regions, include_national=include_national, year=year)
    rows: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    skiprows = int(rules["raw_excel_skiprows"])

    for request in requests_to_fetch:
        status = "cached"
        reason = ""
        size = digest = None
        try:
            if download and not request.cache_path.exists():
                status = fetch_to_cache(request, skiprows=skiprows)
            size, digest = _validate_workbook_identity(request, request.cache_path)
            raw = read_ceud_excel(request.cache_path, skiprows=skiprows)
            _validate_source_shape(request, raw)
            normalized = normalize_ceud_dataframe(raw, request, rules)
            rows.append(normalized)
        except Exception as exc:
            description = request.artifact_description()
            if request.table_meta.required:
                LOGGER.error("Failed to process required %s: %s", description, exc)
                raise
            status = "failed"
            reason = str(exc)
            warnings.append(f"{description}: {reason}")
            LOGGER.warning("Failed to process optional %s: %s", description, exc)

        manifest_rows.append(
            {
                "source_id": request.source_id,
                "region": request.output_region,
                "year": request.year,
                "table_id": request.table_id,
                "short_name": request.table_meta.get("short_name", ""),
                "url": request.url,
                "cached_file": str(request.cache_path),
                "status": status,
                "reason": reason,
                "bytes": size,
                "sha256": digest,
                "expected_bytes": request.expected_bytes,
                "expected_sha256": request.expected_sha256,
            }
        )

    write_outputs(rows, manifest_rows, warnings, output_dir, rules)
    LOGGER.info("CEUD workbook manifest: %s (%d validated, %d optional failures)",
                output_dir / rules["manifest_file"], len(rows), len(warnings))
    return output_dir


def write_rating_outputs(
    *,
    frames: dict[str, pd.DataFrame],
    manifest_rows: list[dict[str, Any]],
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Atomically publish Ratings interim tables, manifest, and warning log."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for output_file, frame in frames.items():
        write_dataframe_atomic(frame, output_dir / output_file)
    write_dataframe_atomic(
        pd.DataFrame(manifest_rows),
        output_dir / str(rules["manifest_file"]),
    )
    warnings_path = output_dir / str(rules["warnings_file"])
    warnings_path.write_text(
        "\n".join(warnings) + ("\n" if warnings else ""),
        encoding="utf-8",
    )


def fetch_and_normalize_ratings(
    scenario_path: str | Path | ConfigBundle,
    *,
    download: bool = True,
) -> Path:
    """Fetch/cache pinned Ratings resources and write source-normalized CSVs."""
    bundle = load_config_bundle(scenario_path)
    rules = ratings_rules(bundle)
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    encoding_candidates = [
        str(encoding) for encoding in rules["encoding_candidates"]
    ]
    frames: dict[str, pd.DataFrame] = {}
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    for request in iter_rating_requests(bundle):
        if download:
            cache_status = fetch_rating_to_cache(request)
        elif request.cache_path.exists():
            cache_status = "cached"
        else:
            raise FileNotFoundError(
                f"Fuel Consumption Ratings source {request.component_key} is missing "
                f"during --no-download execution: {request.cache_path}"
            )

        byte_count, actual_md5 = validate_rating_cache(request)
        raw, encoding = read_rating_csv(
            request,
            encoding_candidates=encoding_candidates,
        )
        normalized = normalize_rating_dataframe(raw, request)
        frames[request.output_file] = normalized
        model_years = normalized["Model year"]
        manifest_rows.append(
            {
                "source_id": request.source_id,
                "component": request.component_key,
                "short_name": request.component_meta.short_name,
                "resource_id": request.resource_id,
                "resource_title": request.resource_title,
                "resource_version": request.component_meta.version or "",
                "url": request.url,
                "cached_file": str(request.cache_path),
                "cache_status": cache_status,
                "encoding": encoding,
                "md5": actual_md5,
                "bytes": byte_count,
                "row_count": len(normalized),
                "model_year_min": int(model_years.min()),
                "model_year_max": int(model_years.max()),
                "output_file": request.output_file,
            }
        )

    write_rating_outputs(
        frames=frames,
        manifest_rows=manifest_rows,
        warnings=warnings,
        output_dir=output_dir,
        rules=rules,
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/default.yaml")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--regions", nargs="*", default=None)
    parser.add_argument("--skip-national", action="store_true")
    parser.add_argument(
        "--ratings-only",
        action="store_true",
        help="Fetch the pinned Fuel Consumption Ratings resources instead of CEUD tables.",
    )
    parser.add_argument("--no-download", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    if args.ratings_only:
        output_dir = fetch_and_normalize_ratings(
            args.scenario,
            download=not args.no_download,
        )
        logging.info(
            "Wrote NRCan Fuel Consumption Ratings interim outputs to %s",
            output_dir,
        )
    else:
        output_dir = fetch_and_normalize(
            args.scenario,
            regions=args.regions,
            include_national=not args.skip_national,
            year=args.year,
            download=not args.no_download,
        )
        logging.info("Wrote NRCan CEUD interim outputs to %s", output_dir)


if __name__ == "__main__":
    main()
