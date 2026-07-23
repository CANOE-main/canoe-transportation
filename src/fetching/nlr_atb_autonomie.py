"""Fetch, cache, and normalize 2024 Transportation ATB and 2022 ANL inputs."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import io
import logging
import os
import re
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.utils.cell import column_index_from_string, range_boundaries
from pydantic import BaseModel, ConfigDict, model_validator

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.config_models import SourceSpec


ATB_SOURCE_ID = "nlr_atb_transportation_2024"
ANL_SOURCE_ID = "anl_autonomie_bean_2022"


class NlrAtbAutonomieError(ValueError):
    """Raised when an ATB/ANL source contract is unavailable or changed."""


class ArchiveComponentRequest(BaseModel):
    """Resolved contract for one required member of the ATB archive."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    component_id: str
    member_patterns: tuple[str, ...]
    file_format: Literal["csv", "xlsx"]
    normalizer: str
    required_columns: tuple[str, ...] = ()
    adapter: dict[str, Any]

    @model_validator(mode="after")
    def validate_request(self) -> "ArchiveComponentRequest":
        if not self.component_id or not self.member_patterns or not self.normalizer:
            raise ValueError("ATB component identity, member patterns, and normalizer are required")
        if any(Path(pattern).is_absolute() for pattern in self.member_patterns):
            raise ValueError("ATB member patterns must be archive-relative")
        return self


class AtbArchiveRequest(BaseModel):
    """Resolved source/cache/component contract for the official ATB ZIP."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    url: str
    cache_path: Path
    expected_trajectories: tuple[str, ...]
    components: tuple[ArchiveComponentRequest, ...]

    @model_validator(mode="after")
    def validate_request(self) -> "AtbArchiveRequest":
        parsed = urlparse(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"ATB URL is invalid: {self.url}")
        if not self.cache_path.is_absolute() or self.cache_path.suffix.casefold() != ".zip":
            raise ValueError(f"ATB cache must be an absolute .zip path: {self.cache_path}")
        if not self.expected_trajectories or not self.components:
            raise ValueError("ATB trajectories and archive components are required")
        return self


class ManualWorkbookRequest(BaseModel):
    """Resolved contract for the manually registered ANL workbook."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    folder_url: str
    workbook_path: Path
    sheet_name: str
    cell_range: str
    expected_columns: tuple[str, ...]
    table_layout: dict[str, Any]
    output_file: str
    required: bool = False

    @model_validator(mode="after")
    def validate_request(self) -> "ManualWorkbookRequest":
        parsed = urlparse(self.folder_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"ANL Box URL is invalid: {self.folder_url}")
        if not self.workbook_path.is_absolute() or self.workbook_path.suffix.casefold() not in {
            ".xlsx",
            ".xlsm",
        }:
            raise ValueError(
                f"ANL workbook must be an absolute .xlsx/.xlsm path: {self.workbook_path}"
            )
        if (
            not self.sheet_name
            or not self.cell_range
            or not self.expected_columns
            or not self.table_layout
        ):
            raise ValueError(
                "ANL workbook sheet, range, columns, and table layout are required"
            )
        min_col, min_row, max_col, max_row = range_boundaries(self.cell_range)
        if min_col > max_col or min_row > max_row:
            raise ValueError(f"ANL workbook range is invalid: {self.cell_range}")
        if max_col - min_col + 1 != len(self.expected_columns):
            raise ValueError(
                "ANL expected columns must match the configured workbook range width"
            )
        actual_columns = tuple(
            chr(ord("A") + index) for index in range(min_col - 1, max_col)
        )
        if self.expected_columns != actual_columns:
            raise ValueError(
                "ANL expected columns must identify the configured workbook range"
            )
        return self


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load ATB/ANL normalization and output rules."""
    return load_harmonization_rules(bundle, "nlr_atb_autonomie")


def _source(bundle: ConfigBundle, source_id: str) -> SourceSpec:
    source = bundle.sources.sources.get(source_id)
    if not isinstance(source, SourceSpec):
        raise NlrAtbAutonomieError(f"sources.yaml missing {source_id}")
    return source


def configured_trajectory(bundle: ConfigBundle) -> str:
    """Resolve the scenario-selected ATB trajectory used only as an output marker."""
    source = _source(bundle, ATB_SOURCE_ID)
    expected = tuple(str(value) for value in source.adapter["expected_trajectories"])
    selection = bundle.scenario.sources.selections.get(ATB_SOURCE_ID)
    trajectory = selection.trajectory if selection is not None else None
    if trajectory is None:
        raise NlrAtbAutonomieError(
            f"Scenario must select a trajectory for {ATB_SOURCE_ID}"
        )
    if trajectory not in expected:
        raise NlrAtbAutonomieError(
            f"Unsupported ATB trajectory {trajectory!r}; expected one of {list(expected)}"
        )
    return trajectory


def build_atb_request(bundle: ConfigBundle) -> AtbArchiveRequest:
    """Build the configured official ATB archive request."""
    source = _source(bundle, ATB_SOURCE_ID)
    access = source.adapter.get("access", {})
    component_requests: list[ArchiveComponentRequest] = []
    for component_id, component in source.components.items():
        adapter = dict(component.adapter)
        component_requests.append(
            ArchiveComponentRequest(
                component_id=str(component_id),
                member_patterns=tuple(str(value) for value in adapter.pop("member_patterns")),
                file_format=str(adapter.pop("format")),
                normalizer=str(adapter.pop("normalizer")),
                required_columns=tuple(
                    str(value) for value in adapter.pop("required_columns", [])
                ),
                adapter=adapter,
            )
        )
    return AtbArchiveRequest(
        url=str(access.get("url", "")),
        cache_path=resolve_input_path(
            bundle, "cache", str(source.adapter.get("cache_path", ""))
        ),
        expected_trajectories=tuple(
            str(value) for value in source.adapter.get("expected_trajectories", [])
        ),
        components=tuple(component_requests),
    )


def build_manual_request(
    bundle: ConfigBundle, rules: dict[str, Any] | None = None
) -> ManualWorkbookRequest:
    """Build the registered manual ANL workbook request."""
    source = _source(bundle, ANL_SOURCE_ID)
    component = source.component("mhdv_maintenance_coefficients")
    adapter = source.adapter
    access = adapter.get("access", {})
    root = resolve_input_path(bundle, "external", str(adapter["external_subdir"]))
    selected_rules = rules or module_rules(bundle)
    workbook_rules = selected_rules["components"]["anl_bean"]
    return ManualWorkbookRequest(
        folder_url=str(access.get("folder_url", "")),
        workbook_path=root / str(adapter["expected_workbook"]),
        sheet_name=str(workbook_rules["workbook_sheet"]),
        cell_range=str(workbook_rules["workbook_range"]),
        expected_columns=tuple(
            str(value) for value in workbook_rules["expected_columns"]
        ),
        table_layout=dict(workbook_rules["table_layout"]),
        output_file=str(workbook_rules["output_file"]),
        required=bool(source.required or component.required),
    )


def fetch_archive_to_cache(
    request: AtbArchiveRequest,
    *,
    session: requests.Session | None = None,
    timeout: int = 120,
) -> str:
    """Download the official ATB ZIP atomically, or reuse an existing cache."""
    if request.cache_path.is_file():
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = request.cache_path.with_suffix(request.cache_path.suffix + ".part")
    client = session or requests.Session()
    try:
        response = client.get(request.url, stream=True, timeout=timeout)
        response.raise_for_status()
        with temporary.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        if not temporary.is_file() or temporary.stat().st_size == 0:
            raise NlrAtbAutonomieError(f"ATB download was empty: {request.url}")
        try:
            with ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise NlrAtbAutonomieError("ATB download contains a corrupt ZIP member")
        except BadZipFile as exc:
            raise NlrAtbAutonomieError(
                f"ATB download is not a valid ZIP: {request.url}"
            ) from exc
        os.replace(temporary, request.cache_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return "downloaded"


def discover_zip_members(
    archive_path: Path,
    components: tuple[ArchiveComponentRequest, ...],
) -> dict[str, str]:
    """Find one case-insensitive archive member for each configured component."""
    if not archive_path.is_file():
        raise FileNotFoundError(archive_path)
    try:
        with ZipFile(archive_path) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
    except BadZipFile as exc:
        raise NlrAtbAutonomieError(f"ATB cache is not a valid ZIP: {archive_path}") from exc

    discovered: dict[str, str] = {}
    for component in components:
        matches = {
            name
            for name in names
            if any(
                fnmatch.fnmatchcase(name.casefold(), pattern.casefold())
                for pattern in component.member_patterns
            )
        }
        if not matches:
            raise NlrAtbAutonomieError(
                f"ATB archive missing required component {component.component_id}; "
                f"expected one of {list(component.member_patterns)}"
            )
        if len(matches) > 1:
            raise NlrAtbAutonomieError(
                f"ATB archive component {component.component_id} is ambiguous: "
                f"{sorted(matches)}"
            )
        discovered[component.component_id] = matches.pop()
    return discovered


def _require_columns(
    frame: pd.DataFrame, required: tuple[str, ...] | list[str], context: str
) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise NlrAtbAutonomieError(
            f"{context} missing required columns {missing}; available columns are "
            f"{list(frame.columns)}"
        )
    if frame.empty:
        raise NlrAtbAutonomieError(f"{context} contains no rows")


def normalize_vehicles(
    frame: pd.DataFrame,
    *,
    request: AtbArchiveRequest,
    component: ArchiveComponentRequest,
    source_member: str,
    default_trajectory: str,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Retain configured cost/performance metrics and every ATB trajectory."""
    _require_columns(frame, component.required_columns, "ATB vehicles.csv")
    actual_trajectories = set(frame["scenario"].dropna().astype(str))
    expected_trajectories = set(request.expected_trajectories)
    if actual_trajectories != expected_trajectories:
        raise NlrAtbAutonomieError(
            "ATB vehicle trajectories changed: "
            f"expected {sorted(expected_trajectories)}, got {sorted(actual_trajectories)}"
        )
    vehicle_rules = rules["components"]["vehicles"]
    metric_keys = {str(key): str(value) for key, value in vehicle_rules["metric_keys"].items()}
    metric_units = {
        str(key): str(value) for key, value in vehicle_rules["metric_units"].items()
    }
    selected = frame[frame["metric"].isin(metric_keys)].copy()
    missing_metrics = sorted(set(metric_keys) - set(selected["metric"].astype(str)))
    if missing_metrics:
        raise NlrAtbAutonomieError(
            f"ATB vehicles.csv missing configured metrics {missing_metrics}"
        )
    selected["year"] = pd.to_numeric(selected["year"], errors="coerce")
    selected["value"] = pd.to_numeric(selected["value"], errors="coerce")
    if selected[["year", "value"]].isna().any().any():
        raise NlrAtbAutonomieError("ATB vehicles.csv contains invalid year/value rows")
    selected["year"] = selected["year"].astype(int)
    selected["trajectory"] = selected["scenario"].astype(str)
    selected["metric_key"] = selected["metric"].map(metric_keys)
    selected["unit"] = selected["metric"].map(metric_units)
    selected["is_default_trajectory"] = selected["trajectory"].eq(default_trajectory)
    selected["source_id"] = ATB_SOURCE_ID
    selected["source_member"] = source_member
    return selected.drop(columns=["scenario"]).sort_values(
        ["vehicle_class", "vehicle_powertrain", "vehicle_detail", "metric", "trajectory", "year"],
        na_position="last",
    ).reset_index(drop=True)


def normalize_vmt_ldv(
    frame: pd.DataFrame,
    *,
    component: ArchiveComponentRequest,
    source_member: str,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Normalize the row-oriented LDV age/VMT schedule."""
    _require_columns(frame, component.required_columns, "ATB vmt_ldv.csv")
    value_column = str(rules["components"]["vmt"]["value_column"])
    normalized = frame.rename(columns={"vmt(mi)": value_column}).copy()
    normalized["year_index"] = pd.to_numeric(normalized["year_index"], errors="coerce")
    normalized[value_column] = pd.to_numeric(normalized[value_column], errors="coerce")
    if normalized[["year_index", value_column]].isna().any().any():
        raise NlrAtbAutonomieError("ATB vmt_ldv.csv contains invalid age/VMT rows")
    normalized["year_index"] = normalized["year_index"].astype(int)
    normalized["vmt_source_component"] = component.component_id
    normalized["unit"] = str(rules["components"]["vmt"]["unit"])
    normalized["source_id"] = ATB_SOURCE_ID
    normalized["source_member"] = source_member
    return normalized


def normalize_vmt_mdhd(
    frame: pd.DataFrame,
    *,
    component: ArchiveComponentRequest,
    source_member: str,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Unpivot numeric MD/HD age columns into the common age/VMT shape."""
    _require_columns(frame, component.required_columns, "ATB vmt_mdhd.csv")
    pattern = re.compile(str(component.adapter["age_column_pattern"]))
    age_columns = [str(column) for column in frame.columns if pattern.fullmatch(str(column))]
    if not age_columns:
        raise NlrAtbAutonomieError("ATB vmt_mdhd.csv has no configured age columns")
    ages = sorted(int(column) for column in age_columns)
    if ages != list(range(ages[0], ages[-1] + 1)):
        raise NlrAtbAutonomieError(
            f"ATB vmt_mdhd.csv age columns are not contiguous: {ages}"
        )
    id_columns = [column for column in frame.columns if str(column) not in age_columns]
    value_column = str(rules["components"]["vmt"]["value_column"])
    normalized = frame.melt(
        id_vars=id_columns,
        value_vars=age_columns,
        var_name="year_index",
        value_name=value_column,
    )
    normalized["year_index"] = pd.to_numeric(normalized["year_index"], errors="coerce")
    normalized[value_column] = pd.to_numeric(normalized[value_column], errors="coerce")
    if normalized[["year_index", value_column]].isna().any().any():
        raise NlrAtbAutonomieError("ATB vmt_mdhd.csv contains invalid age/VMT rows")
    normalized["year_index"] = normalized["year_index"].astype(int)
    normalized["vmt_source_component"] = component.component_id
    normalized["unit"] = str(rules["components"]["vmt"]["unit"])
    normalized["source_id"] = ATB_SOURCE_ID
    normalized["source_member"] = source_member
    return normalized


def read_ldv_maintenance_workbook(
    content: bytes,
    *,
    component: ArchiveComponentRequest,
    source_member: str,
) -> dict[str, pd.DataFrame]:
    """Read and validate every configured LDV maintenance worksheet."""
    sheets = component.adapter.get("sheets")
    if not isinstance(sheets, dict) or not sheets:
        raise NlrAtbAutonomieError("ATB LDV maintenance sheet contracts are missing")
    workbook = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    missing_sheets = sorted(set(sheets) - set(workbook.sheet_names))
    if missing_sheets:
        raise NlrAtbAutonomieError(
            f"ATB maintenance_ldv.xlsx missing worksheets {missing_sheets}"
        )
    outputs: dict[str, pd.DataFrame] = {}
    for sheet_name, sheet_rules in sheets.items():
        frame = pd.read_excel(workbook, sheet_name=str(sheet_name))
        _require_columns(
            frame,
            [str(value) for value in sheet_rules["required_columns"]],
            f"ATB maintenance_ldv.xlsx[{sheet_name}]",
        )
        frame["workbook_sheet"] = str(sheet_name)
        frame["source_id"] = ATB_SOURCE_ID
        frame["source_member"] = source_member
        outputs[str(sheet_name)] = frame
    return outputs


def extract_bean_coefficients(request: ManualWorkbookRequest) -> pd.DataFrame:
    """Extract the configured ANL coefficient blocks to a source-labelled long table."""
    if not request.workbook_path.is_file():
        raise FileNotFoundError(
            f"Required manual ANL workbook missing: {request.workbook_path}. "
            f"Download the complete Box folder from {request.folder_url} into "
            f"{request.workbook_path.parent}."
        )
    try:
        workbook = load_workbook(
            request.workbook_path,
            read_only=True,
            data_only=True,
            keep_vba=request.workbook_path.suffix.casefold() == ".xlsm",
        )
    except Exception as exc:
        raise NlrAtbAutonomieError(
            f"Unable to read ANL workbook {request.workbook_path}: {exc}"
        ) from exc
    if request.sheet_name not in workbook.sheetnames:
        workbook.close()
        raise NlrAtbAutonomieError(
            f"ANL workbook missing worksheet {request.sheet_name!r}; "
            f"available worksheets are {workbook.sheetnames}"
        )
    min_col, min_row, max_col, max_row = range_boundaries(request.cell_range)
    worksheet = workbook[request.sheet_name]
    layout = request.table_layout
    header_row = int(layout["header_row"])
    label_column = str(layout["label_column"])
    powertrain_column = str(layout["powertrain_column"])
    vehicle_columns = {
        str(column): str(label)
        for column, label in dict(layout["vehicle_class_columns"]).items()
    }
    expected_powertrains = [str(value) for value in layout["expected_powertrains"]]
    if not min_row <= header_row <= max_row:
        workbook.close()
        raise NlrAtbAutonomieError("ANL header row falls outside the configured range")

    for column, expected_label in vehicle_columns.items():
        column_index = column_index_from_string(column)
        if not min_col <= column_index <= max_col:
            workbook.close()
            raise NlrAtbAutonomieError(
                f"ANL vehicle-class column {column} falls outside {request.cell_range}"
            )
        actual_label = worksheet[f"{column}{header_row}"].value
        if actual_label != expected_label:
            workbook.close()
            raise NlrAtbAutonomieError(
                f"ANL vehicle-class header changed at {column}{header_row}: "
                f"expected {expected_label!r}, got {actual_label!r}"
            )

    records: list[dict[str, Any]] = []
    for coefficient_key, block in dict(layout["blocks"]).items():
        first_row, last_row = (int(value) for value in block["rows"])
        if first_row < min_row or last_row > max_row or first_row > last_row:
            workbook.close()
            raise NlrAtbAutonomieError(
                f"ANL block {coefficient_key} falls outside {request.cell_range}"
            )
        source_label = str(block["source_label"])
        actual_label = worksheet[f"{label_column}{first_row}"].value
        if actual_label != source_label:
            workbook.close()
            raise NlrAtbAutonomieError(
                f"ANL coefficient label changed at {label_column}{first_row}: "
                f"expected {source_label!r}, got {actual_label!r}"
            )
        actual_powertrains = [
            worksheet[f"{powertrain_column}{row}"].value
            for row in range(first_row, last_row + 1)
        ]
        if actual_powertrains != expected_powertrains:
            workbook.close()
            raise NlrAtbAutonomieError(
                f"ANL powertrains changed for {coefficient_key}: "
                f"expected {expected_powertrains}, got {actual_powertrains}"
            )
        for row, powertrain in zip(
            range(first_row, last_row + 1),
            actual_powertrains,
            strict=True,
        ):
            for column, vehicle_class in vehicle_columns.items():
                value = worksheet[f"{column}{row}"].value
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError) as exc:
                    workbook.close()
                    raise NlrAtbAutonomieError(
                        f"ANL coefficient is not numeric at {column}{row}: {value!r}"
                    ) from exc
                records.append(
                    {
                        "coefficient_key": str(coefficient_key),
                        "source_coefficient_label": source_label,
                        "vehicle_powertrain": str(powertrain),
                        "vehicle_class": vehicle_class,
                        "value": numeric_value,
                        "source_row": row,
                        "source_column": column,
                    }
                )
    workbook.close()
    frame = pd.DataFrame(records)
    expected_count = (
        len(layout["blocks"])
        * len(expected_powertrains)
        * len(vehicle_columns)
    )
    if len(frame) != expected_count:
        raise NlrAtbAutonomieError(
            f"ANL coefficient extraction expected {expected_count} rows, got {len(frame)}"
        )
    frame["source_id"] = ANL_SOURCE_ID
    frame["source_workbook"] = str(request.workbook_path)
    frame["source_sheet"] = request.sheet_name
    frame["source_range"] = request.cell_range
    return frame


def file_sha256(path: Path) -> str:
    """Return a stable SHA-256 checksum for one physical source artifact."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_outputs(
    *,
    outputs: dict[str, pd.DataFrame],
    manifest_rows: list[dict[str, Any]],
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Write normalized source tables, manifest, and warnings."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, frame in outputs.items():
        frame.to_csv(output_dir / filename, index=False)
    pd.DataFrame(manifest_rows).to_csv(output_dir / rules["manifest_file"], index=False)
    (output_dir / rules["warnings_file"]).write_text(
        "\n".join(warnings) + ("\n" if warnings else ""),
        encoding="utf-8",
    )


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    download: bool = True,
    session: requests.Session | None = None,
) -> Path:
    """Fetch/cache ATB and write source-normalized ATB/available ANL outputs."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    request = build_atb_request(bundle)
    default_trajectory = configured_trajectory(bundle)
    output_dir = resolve_input_path(bundle, "interim", str(rules["interim_subdir"]))

    if download:
        cache_status = fetch_archive_to_cache(request, session=session)
    elif request.cache_path.is_file():
        cache_status = "cached"
    else:
        raise FileNotFoundError(
            f"ATB offline cache missing: {request.cache_path}. Run without --no-download "
            "or place the official ZIP at the configured cache path."
        )

    members = discover_zip_members(request.cache_path, request.components)
    archive_checksum = file_sha256(request.cache_path)
    outputs: dict[str, pd.DataFrame] = {}
    vmt_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    with ZipFile(request.cache_path) as archive:
        for component in request.components:
            member = members[component.component_id]
            normalizer = component.normalizer
            input_rows = 0
            output_files: list[str] = []
            selected_rows = 0
            if component.file_format == "xlsx":
                sheet_frames = read_ldv_maintenance_workbook(
                    archive.read(member), component=component, source_member=member
                )
                configured_outputs = rules["components"]["maintenance_ldv"]["outputs"]
                for sheet_name, frame in sheet_frames.items():
                    filename = str(configured_outputs[sheet_name])
                    outputs[filename] = frame
                    output_files.append(filename)
                    input_rows += len(frame)
                    selected_rows += len(frame)
            else:
                frame = pd.read_csv(archive.open(member))
                input_rows = len(frame)
                if normalizer == "vehicles":
                    normalized = normalize_vehicles(
                        frame,
                        request=request,
                        component=component,
                        source_member=member,
                        default_trajectory=default_trajectory,
                        rules=rules,
                    )
                    filename = str(rules["components"]["vehicles"]["output_file"])
                    outputs[filename] = normalized
                    output_files.append(filename)
                elif normalizer == "vmt_ldv":
                    normalized = normalize_vmt_ldv(
                        frame,
                        component=component,
                        source_member=member,
                        rules=rules,
                    )
                    vmt_frames.append(normalized)
                    output_files.append(str(rules["components"]["vmt"]["output_file"]))
                elif normalizer == "vmt_mdhd":
                    normalized = normalize_vmt_mdhd(
                        frame,
                        component=component,
                        source_member=member,
                        rules=rules,
                    )
                    vmt_frames.append(normalized)
                    output_files.append(str(rules["components"]["vmt"]["output_file"]))
                else:
                    raise NlrAtbAutonomieError(
                        f"No ATB normalizer configured for {normalizer!r}"
                    )
                selected_rows = len(normalized)
            manifest_rows.append(
                {
                    "source_id": ATB_SOURCE_ID,
                    "component_id": component.component_id,
                    "source_url": request.url,
                    "cached_file": str(request.cache_path),
                    "sha256": archive_checksum,
                    "cache_status": cache_status,
                    "source_member": member,
                    "input_rows": input_rows,
                    "selected_rows": selected_rows,
                    "output_files": "|".join(output_files),
                    "default_trajectory": default_trajectory,
                    "status": "ok",
                }
            )

    if not vmt_frames:
        raise NlrAtbAutonomieError("ATB archive produced no VMT schedules")
    vmt_filename = str(rules["components"]["vmt"]["output_file"])
    outputs[vmt_filename] = pd.concat(vmt_frames, ignore_index=True, sort=False).sort_values(
        ["vehicle_weight_category", "vehicle_class", "year_index"],
        na_position="last",
    ).reset_index(drop=True)

    manual = build_manual_request(bundle, rules)
    if manual.workbook_path.is_file():
        coefficients = extract_bean_coefficients(manual)
        outputs[manual.output_file] = coefficients
        _, manual_min_row, _, manual_max_row = range_boundaries(manual.cell_range)
        manifest_rows.append(
            {
                "source_id": ANL_SOURCE_ID,
                "component_id": "mhdv_maintenance_coefficients",
                "source_url": manual.folder_url,
                "cached_file": str(manual.workbook_path),
                "sha256": file_sha256(manual.workbook_path),
                "cache_status": "manual",
                "source_member": f"{manual.sheet_name}!{manual.cell_range}",
                "input_rows": manual_max_row - manual_min_row + 1,
                "selected_rows": len(coefficients),
                "output_files": manual.output_file,
                "default_trajectory": "",
                "status": "ok",
            }
        )
    else:
        message = (
            f"ANL manual input unavailable: {manual.workbook_path}. Download the complete "
            f"Box folder from {manual.folder_url} into {manual.workbook_path.parent}."
        )
        if manual.required:
            raise FileNotFoundError(message)
        warnings.append(message)
        manifest_rows.append(
            {
                "source_id": ANL_SOURCE_ID,
                "component_id": "mhdv_maintenance_coefficients",
                "source_url": manual.folder_url,
                "cached_file": str(manual.workbook_path),
                "sha256": "",
                "cache_status": "manual_missing",
                "source_member": f"{manual.sheet_name}!{manual.cell_range}",
                "input_rows": 0,
                "selected_rows": 0,
                "output_files": "",
                "default_trajectory": "",
                "status": "warning",
            }
        )

    write_outputs(
        outputs=outputs,
        manifest_rows=manifest_rows,
        warnings=warnings,
        output_dir=output_dir,
        rules=rules,
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
        help="Scenario YAML controlling configured paths and the default trajectory marker.",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Require and reuse the configured official ATB ZIP without network access.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    try:
        output_dir = fetch_and_normalize(
            args.scenario,
            download=not args.no_download,
        )
    except (FileNotFoundError, NlrAtbAutonomieError, requests.RequestException) as exc:
        raise SystemExit(f"NLR ATB/ANL adapter failed: {exc}") from exc
    logging.info("Wrote NLR ATB/ANL interim outputs to %s", output_dir)


if __name__ == "__main__":
    main()
