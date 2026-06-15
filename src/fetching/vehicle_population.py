"""Fetch and normalize Ontario vehicle population reports 4 and 5."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pandas as pd
import requests

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_input_path,
)


SOURCE_ID = "ontario_ministry_transport_vehicle_population"
ZIP_MEMBER_SEPARATOR = "\t"


@dataclass(frozen=True)
class OntarioVehiclePopulationRequest:
    """One year-specific Ontario vehicle population archive request."""

    source_id: str
    year: int
    package_id: str
    resource_id: str
    resource_name: str
    url: str
    cache_path: Path
    report4_member: str
    report5_member: str


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load Ontario vehicle population harmonization rules."""
    return load_harmonization_rules(bundle, "ontario_vehicle_population")


def configured_year(source: dict[str, Any]) -> int:
    """Return the default data year from a source registry entry."""
    years = source.get("years", {})
    if isinstance(years, dict):
        return int(years["default"])
    return int(years)


def ckan_package_show_url(base_url: str, package_id: str) -> str:
    """Build a CKAN package_show URL."""
    return f"{base_url.rstrip('/')}/api/3/action/package_show?id={package_id}"


def fetch_ckan_package_metadata(base_url: str, package_id: str, *, timeout: int = 60) -> dict[str, Any]:
    """Fetch CKAN package metadata."""
    response = requests.get(ckan_package_show_url(base_url, package_id), timeout=timeout)
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


def select_ckan_resource(
    package_metadata: dict[str, Any],
    *,
    year: int,
    selector: dict[str, Any],
) -> dict[str, Any]:
    """Select one CKAN resource matching generic selector rules and requested year."""
    expected_format = str(selector.get("format", "")).lower()
    year_fields = [str(field) for field in selector.get("year_fields", ["name", "url"])]
    candidates: list[dict[str, Any]] = []

    for resource in package_resources(package_metadata):
        resource_format = str(resource.get("format", "")).lower()
        if expected_format and resource_format != expected_format:
            continue
        if str(year) not in resource_field_text(resource, year_fields):
            continue
        if not resource.get("url"):
            continue
        candidates.append(resource)

    if not candidates:
        raise ValueError(f"No CKAN resource matched year {year}")
    if len(candidates) > 1:
        names = ", ".join(str(resource.get("name", resource.get("id", ""))) for resource in candidates)
        raise ValueError(f"Multiple CKAN resources matched year {year}: {names}")
    return candidates[0]


def render_cache_path(bundle: ConfigBundle, source: dict[str, Any], *, year: int) -> Path:
    """Render the deterministic cache path for one Ontario vehicle population ZIP."""
    template = source.get("cache_path_template", source.get("path_template"))
    if template is None:
        raise KeyError("Ontario vehicle population source missing cache_path_template")
    path = template.format(year=year)
    normalized_path = str(path).replace("\\", "/")
    for cache_root in ("inputs/cache/", "inputs/0_cache/"):
        if normalized_path.startswith(cache_root):
            path = normalized_path.removeprefix(cache_root)
            break
    return resolve_input_path(bundle, "cache", path)


def source_components(source: dict[str, Any]) -> dict[Any, Any]:
    """Return Ontario vehicle population report components."""
    components = source.get("reports", source.get("tables"))
    if not isinstance(components, dict):
        raise ValueError("Ontario vehicle population source must define reports 4 and 5")
    return components


def build_request(
    bundle: ConfigBundle,
    *,
    year: int | None = None,
    package_metadata: dict[str, Any] | None = None,
) -> OntarioVehiclePopulationRequest:
    """Build a concrete archive request from sources.yaml and CKAN metadata."""
    source = bundle.sources["sources"][SOURCE_ID]
    selected_year = year or configured_year(source)
    access = source["access"]
    metadata = package_metadata or fetch_ckan_package_metadata(access["ckan_base_url"], access["package_id"])
    resource = select_ckan_resource(metadata, year=selected_year, selector=access["resource_selector"])
    components = source_components(source)
    report4 = components.get(4, components.get("4"))
    report5 = components.get(5, components.get("5"))
    if not isinstance(report4, dict) or not isinstance(report5, dict):
        raise ValueError("Ontario vehicle population source must define reports 4 and 5")
    return OntarioVehiclePopulationRequest(
        source_id=SOURCE_ID,
        year=selected_year,
        package_id=access["package_id"],
        resource_id=str(resource.get("id", "")),
        resource_name=str(resource.get("name", "")),
        url=str(resource["url"]),
        cache_path=render_cache_path(bundle, source, year=selected_year),
        report4_member=report4["raw_member_template"].format(year=selected_year),
        report5_member=report5["raw_member_template"].format(year=selected_year),
    )


def fetch_to_cache(request: OntarioVehiclePopulationRequest, *, timeout: int = 120) -> str:
    """Download the requested ZIP unless it already exists."""
    if request.cache_path.exists():
        return "cached"
    request.cache_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(request.url, timeout=timeout)
    response.raise_for_status()
    request.cache_path.write_bytes(response.content)
    return "downloaded"


def read_report_from_zip(cache_path: Path, member_name: str) -> pd.DataFrame:
    """Read one tab-delimited report from a cached Ontario vehicle population ZIP."""
    resolved_member = resolve_zip_member_name(cache_path, member_name)
    with ZipFile(cache_path) as archive:
        with archive.open(resolved_member) as handle:
            return pd.read_csv(handle, sep=ZIP_MEMBER_SEPARATOR, dtype=str, keep_default_na=False)


def resolve_zip_member_name(cache_path: Path, member_name: str) -> str:
    """Resolve a ZIP member by exact name or unique basename match."""
    with ZipFile(cache_path) as archive:
        names = archive.namelist()
    if member_name in names:
        return member_name
    matches = [name for name in names if Path(name).name == member_name]
    if not matches:
        raise FileNotFoundError(f"{member_name} not found in {cache_path}")
    if len(matches) > 1:
        raise ValueError(f"Multiple ZIP members matched {member_name}: {', '.join(matches)}")
    return matches[0]


def read_vehicle_population_txt(path: Path) -> pd.DataFrame:
    """Read a local tab-delimited Ontario vehicle population TXT file."""
    return pd.read_csv(path, sep=ZIP_MEMBER_SEPARATOR, dtype=str, keep_default_na=False)


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
    cleaned = raw.loc[raw[weight_class_column].str.upper() == kept_weight_class, required].copy()
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
    distribution[report_rules["share_column"]] = distribution[output_count_column] / total if total else 0.0
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
    """Clean Report 5 and calculate class-normalized vehicle age distributions."""
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
    kept_classes = [str(vehicle_class).upper() for vehicle_class in report_rules["kept_vehicle_classes"]]
    selected_max_age = int(max_age if max_age is not None else report_rules["max_age"])

    cleaned = raw.loc[
        (raw[descriptor_column].str.upper() == kept_descriptor)
        & (raw[vehicle_class_column].str.upper().isin(kept_classes)),
        required,
    ].copy()
    cleaned[vehicle_class_column] = cleaned[vehicle_class_column].str.upper()
    cleaned[model_year_column] = pd.to_numeric(cleaned[model_year_column], errors="coerce")
    cleaned[count_column] = pd.to_numeric(cleaned[count_column], errors="coerce")
    cleaned = cleaned.dropna(subset=[model_year_column, count_column])
    cleaned[model_year_column] = cleaned[model_year_column].astype(int)
    cleaned = cleaned[cleaned[model_year_column] <= year]
    age_column = report_rules["age_column"]
    cleaned[age_column] = year - cleaned[model_year_column]
    cleaned = cleaned[cleaned[age_column] <= selected_max_age]
    cleaned = cleaned.sort_values([vehicle_class_column, age_column]).reset_index(drop=True)
    cleaned.insert(0, "source_id", source_id)
    cleaned.insert(1, "year", year)
    cleaned.insert(2, "report", report_rules["report_label"])
    cleaned.insert(3, "raw_file", raw_file)
    cleaned["cached_zip"] = str(cached_zip)

    output_count_column = report_rules["output_count_column"]
    distribution = cleaned.rename(columns={count_column: output_count_column}).copy()
    totals = distribution.groupby(vehicle_class_column)[output_count_column].transform("sum")
    age_distribution_column = report_rules["age_distribution_column"]
    distribution[age_distribution_column] = distribution[output_count_column] / totals
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


def write_outputs(
    *,
    request: OntarioVehiclePopulationRequest,
    cache_status: str,
    report4_cleaned: pd.DataFrame,
    report4_distribution: pd.DataFrame,
    report5_cleaned: pd.DataFrame,
    report5_distribution: pd.DataFrame,
    warnings: list[str],
    output_dir: Path,
    rules: dict[str, Any],
) -> None:
    """Write interim CSVs, manifest, and warning log."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report4_rules = rules["reports"][4]
    report5_rules = rules["reports"][5]
    outputs = {
        report4_rules["cleaned_output_template"].format(year=request.year): report4_cleaned,
        report4_rules["distribution_output_template"].format(year=request.year): report4_distribution,
        report5_rules["cleaned_output_template"].format(year=request.year): report5_cleaned,
        report5_rules["distribution_output_template"].format(year=request.year): report5_distribution,
    }
    for filename, frame in outputs.items():
        frame.to_csv(output_dir / filename, index=False)

    pd.DataFrame(
        [
            {
                "source_id": request.source_id,
                "year": request.year,
                "package_id": request.package_id,
                "resource_id": request.resource_id,
                "resource_name": request.resource_name,
                "url": request.url,
                "cached_zip": str(request.cache_path),
                "cache_status": cache_status,
                "report4_member": request.report4_member,
                "report5_member": request.report5_member,
                "outputs": "|".join(outputs),
            }
        ]
    ).to_csv(output_dir / rules["manifest_file"], index=False)
    (output_dir / rules["warnings_file"]).write_text("\n".join(warnings) + ("\n" if warnings else ""), encoding="utf-8")


def fetch_and_normalize(
    scenario_path: str | Path,
    *,
    year: int | None = None,
    download: bool = True,
    package_metadata: dict[str, Any] | None = None,
    max_age: int | None = None,
) -> Path:
    """Fetch/cache the Ontario ZIP and write Report 4/5 interim outputs."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    conversions = load_conversion_factors(bundle)
    kg_to_lb = float(conversions["mass"]["kg_to_lb"])
    request = build_request(bundle, year=year, package_metadata=package_metadata)
    output_dir = resolve_input_path(bundle, "interim", rules["interim_subdir"])
    warnings: list[str] = []

    if download:
        cache_status = fetch_to_cache(request)
    elif request.cache_path.exists():
        cache_status = "cached"
    else:
        raise FileNotFoundError(request.cache_path)

    report4_member = resolve_zip_member_name(request.cache_path, request.report4_member)
    report5_member = resolve_zip_member_name(request.cache_path, request.report5_member)
    report4_raw = read_report_from_zip(request.cache_path, report4_member)
    report5_raw = read_report_from_zip(request.cache_path, report5_member)
    report4_cleaned, report4_distribution = normalize_report4(
        report4_raw,
        source_id=request.source_id,
        year=request.year,
        raw_file=report4_member,
        cached_zip=request.cache_path,
        rules=rules,
        kg_to_lb=kg_to_lb,
    )
    report5_cleaned, report5_distribution = normalize_report5(
        report5_raw,
        source_id=request.source_id,
        year=request.year,
        raw_file=report5_member,
        cached_zip=request.cache_path,
        rules=rules,
        max_age=max_age,
    )
    write_outputs(
        request=request,
        cache_status=cache_status,
        report4_cleaned=report4_cleaned,
        report4_distribution=report4_distribution,
        report5_cleaned=report5_cleaned,
        report5_distribution=report5_distribution,
        warnings=warnings,
        output_dir=output_dir,
        rules=rules,
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/legacy_reproduction.yaml")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--max-age", type=int, default=None)
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
    logging.info("Wrote Ontario vehicle population interim outputs to %s", output_dir)


if __name__ == "__main__":
    main()
