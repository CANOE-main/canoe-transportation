"""Fetch and normalize NRCan CEUD transportation tables."""

from __future__ import annotations

import argparse
import logging
import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from parameterization.utils import ConfigBundle, load_config_bundle, resolve_repo_path


SOURCE_PROVINCIAL = "nrcan_ceud_transport_provincial"
SOURCE_NATIONAL = "nrcan_ceud_transport_national"
INTERIM_DIR = "inputs/interim/fetched_nrcan_ceud_inputs"
WARNING_LOG = "warnings.log"
MANIFEST = "manifest.csv"


@dataclass(frozen=True)
class CeudTableRequest:
    """One CEUD table file requested from the source registry."""

    source_id: str
    region: str
    year: int
    table_id: int
    table_meta: dict[str, Any]
    url: str
    cache_path: Path
    output_region: str


def clean_label(value: object) -> str | None:
    """Clean CEUD labels using the legacy character filter."""
    if value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value):
        return None
    if str(value).strip().lower() in {"nan", "n.a.", "na"}:
        return None
    text = str(value).translate(str.maketrans("", "", "¹²³"))
    text = unicodedata.normalize("NFKD", text)
    cleaned = "".join(char for char in text if char.isalnum() or char in "- /()|%")
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


def is_value_bearing_group_header(label: str, request: CeudTableRequest) -> bool:
    """Return true for CEUD rows that are both a value row and a parent header."""
    return request.table_id == 7 and label == "Total Energy Use (PJ)"


def render_ceud_url(source: dict[str, Any], *, year: int, region: str, table_id: int) -> str:
    """Render a CEUD URL from a source registry entry."""
    return source["url_template"].format(year=year, region=region.lower(), table_id=table_id)


def render_cache_path(
    bundle: ConfigBundle,
    source: dict[str, Any],
    *,
    year: int,
    region: str,
    table_id: int,
) -> Path:
    """Render the deterministic raw cache path for one CEUD table."""
    path = source["path_template"].format(year=year, region=region.lower(), table_id=table_id)
    return resolve_repo_path(bundle.repo_root, path)


def configured_year(source: dict[str, Any]) -> int:
    """Return the default CEUD release year from either accepted config spelling."""
    year_config = source.get("years", source.get("year", {}))
    if isinstance(year_config, dict):
        return int(year_config["default"])
    return int(year_config)


def provincial_regions(source: dict[str, Any], requested_regions: list[str] | None) -> list[str]:
    """Resolve requested provincial regions against the source registry."""
    allowed = [str(region).upper() for region in source["regions"]["allowed"]]
    if requested_regions:
        unknown = sorted(set(region.upper() for region in requested_regions) - set(allowed))
        if unknown:
            raise ValueError(f"Unknown CEUD provincial regions: {', '.join(unknown)}")
        return [region.upper() for region in requested_regions]
    default = source["regions"].get("default")
    if default == "all_provinces":
        return allowed
    return [str(default).upper()]


def iter_table_requests(
    bundle: ConfigBundle,
    *,
    regions: list[str] | None = None,
    include_national: bool = True,
    year: int | None = None,
) -> list[CeudTableRequest]:
    """Build concrete CEUD table requests from sources.yaml."""
    sources = bundle.sources["sources"]
    requests_to_fetch: list[CeudTableRequest] = []

    provincial = sources[SOURCE_PROVINCIAL]
    provincial_year = year or configured_year(provincial)
    for region in provincial_regions(provincial, regions):
        for table_id, table_meta in sorted(provincial["tables"].items()):
            table_id_int = int(table_id)
            requests_to_fetch.append(
                CeudTableRequest(
                    source_id=SOURCE_PROVINCIAL,
                    region=region.lower(),
                    output_region=region,
                    year=provincial_year,
                    table_id=table_id_int,
                    table_meta=table_meta,
                    url=render_ceud_url(provincial, year=provincial_year, region=region, table_id=table_id_int),
                    cache_path=render_cache_path(
                        bundle,
                        provincial,
                        year=provincial_year,
                        region=region,
                        table_id=table_id_int,
                    ),
                )
            )

    if include_national:
        national = sources[SOURCE_NATIONAL]
        national_year = year or configured_year(national)
        for table_id, table_meta in sorted(national["tables"].items()):
            table_id_int = int(table_id)
            requests_to_fetch.append(
                CeudTableRequest(
                    source_id=SOURCE_NATIONAL,
                    region="ca",
                    output_region="national",
                    year=national_year,
                    table_id=table_id_int,
                    table_meta=table_meta,
                    url=render_ceud_url(national, year=national_year, region="ca", table_id=table_id_int),
                    cache_path=render_cache_path(
                        bundle,
                        national,
                        year=national_year,
                        region="ca",
                        table_id=table_id_int,
                    ),
                )
            )

    return requests_to_fetch


def fetch_to_cache(request: CeudTableRequest, *, timeout: int = 60) -> str:
    """Download a CEUD table unless it already exists in the raw cache."""
    if request.cache_path.exists():
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(request.url, timeout=timeout)
    response.raise_for_status()
    request.cache_path.write_bytes(response.content)
    return "downloaded"


def read_ceud_excel(path: Path) -> pd.DataFrame:
    """Read a raw CEUD Excel table using the legacy metadata-row offset."""
    return pd.read_excel(path, skiprows=10)


def normalize_ceud_dataframe(raw: pd.DataFrame, request: CeudTableRequest) -> pd.DataFrame:
    """Normalize a parsed CEUD table into traceable long form."""
    if raw.shape[1] < 3:
        raise ValueError("CEUD table has fewer than three columns")

    df = raw.copy()
    label_column = "Unnamed: 1" if "Unnamed: 1" in df.columns else df.columns[1]
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    year_columns: dict[Any, int] = {}
    for column in df.columns:
        try:
            year_columns[column] = int(str(column).strip())
        except ValueError:
            continue
    if not year_columns:
        raise ValueError("CEUD table has no integer year columns")

    df = df[[label_column, *year_columns.keys()]].rename(columns=year_columns)
    df[label_column] = df[label_column].map(clean_label)

    header: str | None = None
    raw_series: list[str | None] = []
    first_year = min(year_columns.values())
    for _, row in df.iterrows():
        label = row[label_column]
        if label is None or pd.isna(label):
            header = None
            raw_series.append(None)
        elif is_value_bearing_group_header(label, request):
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
    df = df[~df["raw_series"].str.contains("Shares|GHG", case=False, na=False)]
    df = df.dropna(subset=year_columns.values(), how="all")

    df["raw_series"] = add_legacy_table_label_prefixes(
        df["raw_series"],
        request.table_meta.get("label", ""),
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


def add_legacy_table_label_prefixes(series: pd.Series, table_label: object) -> pd.Series:
    """Apply the legacy Activity/Energy label prefix behavior, including table 36."""
    labels: list[str] = []
    activity_count = 0
    intensity_count = 0
    label_list = table_label if isinstance(table_label, list) else None
    for raw_value in series:
        value = str(raw_value)
        needs_prefix = any(
            marker in value
            for marker in ("Activity", "Energy Intensity", "Energy Use by Energy Source")
        )
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
) -> None:
    """Write normalized CEUD outputs, a manifest, and warning log."""
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = pd.DataFrame(manifest_rows)
    manifest.to_csv(output_dir / MANIFEST, index=False)

    if rows:
        all_rows = pd.concat(rows, ignore_index=True)
        for region, region_rows in all_rows.groupby("region"):
            suffix = str(region).lower()
            region_rows.to_csv(output_dir / f"nrcan_ceud_transport_{suffix}.csv", index=False)
    else:
        (output_dir / "nrcan_ceud_transport_empty.csv").write_text("", encoding="utf-8")

    (output_dir / WARNING_LOG).write_text("\n".join(warnings) + ("\n" if warnings else ""), encoding="utf-8")


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    regions: list[str] | None = None,
    include_national: bool = True,
    year: int | None = None,
    download: bool = True,
) -> Path:
    """Fetch/cache configured CEUD tables and write normalized interim CSVs."""
    bundle = load_config_bundle(scenario_path)
    output_dir = resolve_repo_path(bundle.repo_root, INTERIM_DIR)
    requests_to_fetch = iter_table_requests(bundle, regions=regions, include_national=include_national, year=year)
    rows: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    for request in requests_to_fetch:
        status = "missing"
        reason = ""
        try:
            if download:
                status = fetch_to_cache(request)
            elif request.cache_path.exists():
                status = "cached"
            else:
                raise FileNotFoundError(request.cache_path)
            normalized = normalize_ceud_dataframe(read_ceud_excel(request.cache_path), request)
            rows.append(normalized)
        except Exception as exc:
            status = "failed"
            reason = str(exc)
            warnings.append(
                f"{request.source_id} {request.output_region} table {request.table_id}: {reason}"
            )
            logging.warning("Failed to process %s %s table %s: %s", request.source_id, request.output_region, request.table_id, exc)

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
            }
        )

    write_outputs(rows, manifest_rows, warnings, output_dir)
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--regions", nargs="*", default=None)
    parser.add_argument("--skip-national", action="store_true")
    parser.add_argument("--no-download", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
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
