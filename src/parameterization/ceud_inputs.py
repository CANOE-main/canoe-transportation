"""Explicit CEUD base-year parameter views; normalized source files remain immutable."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from utils import (
    ConfigBundle,
    file_sha256,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)


LOGGER = logging.getLogger(__name__)
SERIES_KEY = ["source_id", "region", "table_id", "raw_series"]
TABLE_KEY = ["source_id", "region", "table_id"]
ANNOTATIONS = [
    "ceud_value_status", "ceud_observed_year", "ceud_source_endpoint_year",
    "ceud_extension_policy",
]
AUDIT_COLUMNS = [
    *SERIES_KEY, "unit", "source_endpoint_year", "series_endpoint_year",
    "base_year", "policy", "status", "observed_rows", "missing_source_values",
    "extended_rows", "extension_start_year", "extension_end_year",
]


def extend_ceud_to_base_year(
    frame: pd.DataFrame,
    *,
    base_year: int,
    policy: Literal["error", "constant"] = "error",
    required_series: Sequence[tuple[int, str]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Append constant tail estimates without repairing source gaps or trimming history.

    A series is eligible only if it has a value at its table's observed endpoint.
    Historical fuel labels that disappear before that endpoint remain unchanged.
    Explicit required selectors additionally require continuous, nonmissing annual
    observations from their first year through the earlier of base year and source
    endpoint. These selectors are (table_id, raw_series), required in every region
    in the supplied frame; they do not make other series eligible.
    """
    if isinstance(base_year, bool) or not isinstance(base_year, (int, np.integer)):
        raise ValueError("CEUD base year must be an integer")
    if policy not in {"error", "constant"}:
        raise ValueError(f"Unsupported CEUD beyond-observed policy: {policy}")
    required_columns = {*SERIES_KEY, "year", "value", "unit"}
    if missing := sorted(required_columns - set(frame)):
        raise ValueError(f"CEUD parameter input lacks columns: {missing}")
    if set(ANNOTATIONS) & set(frame):
        raise ValueError("CEUD parameter inputs must originate from native observations")
    if frame[SERIES_KEY + ["unit"]].isna().any().any():
        raise ValueError("CEUD series identity and units must not be missing")
    years = pd.to_numeric(frame["year"], errors="raise")
    if not np.isfinite(years).all() or years.mod(1).ne(0).any():
        raise ValueError("CEUD observation years must be finite integers")
    values = pd.to_numeric(frame["value"], errors="raise")
    observed_values = values.loc[values.notna()]
    if not np.isfinite(observed_values).all() or observed_values.lt(0).any():
        raise ValueError("CEUD observed values must be finite and nonnegative")
    if frame.duplicated([*SERIES_KEY, "year"]).any():
        raise ValueError("CEUD series have duplicate observation years")
    required = set(required_series or ())
    if len(required) != len(required_series or ()):
        raise ValueError("CEUD required series selectors must be unique")
    for region, regional in frame.groupby(["source_id", "region"], sort=True):
        available = set(zip(regional.table_id, regional.raw_series, strict=True))
        if missing := sorted(required - available):
            raise ValueError(f"Missing required CEUD series for {region}: {missing}")
    if frame.empty and required:
        raise ValueError("Missing required CEUD series in empty input")

    result = frame.copy().reset_index(drop=True)
    result["ceud_value_status"] = np.where(values.isna(), "source_missing", "observed")
    result["ceud_observed_year"] = years.to_numpy(dtype=int)
    endpoints = result.groupby(TABLE_KEY, sort=False)["year"].transform("max")
    result["ceud_source_endpoint_year"] = endpoints
    result["ceud_extension_policy"] = "none"
    additions: list[pd.DataFrame] = []
    audit: list[dict] = []
    for identity, series in result.groupby(SERIES_KEY, sort=True):
        label = "/".join(map(str, identity))
        if series.unit.nunique() != 1:
            raise ValueError(f"CEUD series units change within {label}")
        source_endpoint = int(series.ceud_source_endpoint_year.iloc[0])
        finite = series.loc[series.value.notna()].sort_values("year")
        last_year = int(finite.year.iloc[-1]) if not finite.empty else None
        selected = (identity[2], identity[3]) in required
        if selected:
            through = min(base_year, source_endpoint)
            start = int(series.year.min())
            expected = set(range(start, through + 1))
            present = set(finite.loc[finite.year.le(through), "year"])
            if start > through or present != expected:
                raise ValueError(
                    f"Missing required CEUD observations for {label} through {through}: "
                    f"{sorted(expected - present)}"
                )
        record = {
            **dict(zip(SERIES_KEY, identity, strict=True)),
            "unit": series.unit.iloc[0],
            "source_endpoint_year": source_endpoint,
            "series_endpoint_year": last_year,
            "base_year": int(base_year),
            "policy": policy,
            "observed_rows": len(series),
            "missing_source_values": int(series.value.isna().sum()),
            "extended_rows": 0,
            "extension_start_year": None,
            "extension_end_year": None,
        }
        if finite.empty:
            record["status"] = "skipped_no_source_values"
        elif last_year != source_endpoint:
            record["status"] = "skipped_not_observed_at_source_endpoint"
        elif base_year <= source_endpoint:
            record["status"] = "within_observed_horizon"
        elif policy == "error":
            raise ValueError(
                f"CEUD base year {base_year} exceeds observed endpoint {source_endpoint} "
                f"for {label}; select nrcan_ceud_beyond_observed: constant explicitly"
            )
        else:
            extension_years = range(source_endpoint + 1, base_year + 1)
            endpoint = finite.tail(1)
            extension = endpoint.iloc[np.zeros(len(extension_years), dtype=int)].copy()
            extension["year"] = list(extension_years)
            extension["ceud_value_status"] = "constant"
            extension["ceud_observed_year"] = source_endpoint
            extension["ceud_extension_policy"] = policy
            additions.append(extension)
            record.update({
                "status": "extended", "extended_rows": len(extension),
                "extension_start_year": source_endpoint + 1,
                "extension_end_year": int(base_year),
            })
        audit.append(record)
    if additions:
        result = pd.concat([result, *additions], ignore_index=True)
    return result, pd.DataFrame(audit, columns=AUDIT_COLUMNS)


def ceud_input_path(bundle: ConfigBundle, region: str) -> Path:
    """Resolve the native normalized input, never the persisted parameter view."""
    rules = load_harmonization_rules(bundle, "nrcan_ceud")
    return resolve_input_path(
        bundle, "interim", rules["interim_subdir"],
        rules["region_output_template"].format(region=region.lower()),
    )


def _read_native(bundle: ConfigBundle, region: str) -> tuple[pd.DataFrame, Path]:
    path = ceud_input_path(bundle, region)
    frame = pd.read_csv(path)
    source = (
        "nrcan_ceud_transport_national" if region == "national"
        else "nrcan_ceud_transport_provincial"
    )
    if set(frame.source_id) != {source} or set(frame.region) != {region}:
        raise ValueError(f"CEUD source/region identity differs from {source}/{region}: {path}")
    return frame, path


def read_ceud_input(
    bundle: ConfigBundle,
    region: str,
    *,
    required_series: Sequence[tuple[int, str]] | None = None,
) -> pd.DataFrame:
    """Compute a parameter view through base_year without changing source evidence.

    Recompute from source on every call. Persisted audit files are never runtime
    authorities. Observations after base_year are retained in the full artifact
    produced by prepare_ceud_inputs but excluded from this parameter view.
    """
    native, _ = _read_native(bundle, region)
    frame, _ = extend_ceud_to_base_year(
        native, base_year=bundle.scenario.periods.base_year,
        policy=bundle.scenario.sources.nrcan_ceud_beyond_observed,
        required_series=required_series,
    )
    return frame.loc[frame.year.le(bundle.scenario.periods.base_year)].reset_index(drop=True)


def prepare_ceud_inputs(bundle: ConfigBundle) -> dict:
    """Persist complete annotated evidence and its source/selection manifest."""
    output = resolve_artifact_path(bundle, "ceud_parameter_inputs")
    native_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    rules = load_harmonization_rules(bundle, "ceud_inputs")
    regions = [*bundle.scenario.geography.regions, "national"]
    prepared: list[tuple[pd.DataFrame, Path]] = []
    audits: list[pd.DataFrame] = []
    sources: list[dict] = []
    # Validate all inputs before publishing any new parameter evidence.
    for region in regions:
        native, path = _read_native(bundle, region)
        frame, audit = extend_ceud_to_base_year(
            native, base_year=bundle.scenario.periods.base_year,
            policy=bundle.scenario.sources.nrcan_ceud_beyond_observed,
        )
        target = output / native_rules["region_output_template"].format(region=region.lower())
        prepared.append((frame, target))
        audits.append(audit)
        source_id = str(native.source_id.iloc[0])
        selection = bundle.scenario.sources.selections[source_id]
        sources.append({
            "region": region, "source_id": source_id,
            "selected_source_edition": selection.year,
            "native_path": str(path), "native_sha256": file_sha256(path),
            "observed_min_year": int(native.year.min()),
            "observed_max_year": int(native.year.max()),
            "native_rows": len(native), "parameter_evidence_rows": len(frame),
            "output_path": str(target),
        })
    combined = pd.concat(audits, ignore_index=True)
    for (frame, target), source in zip(prepared, sources, strict=True):
        write_dataframe_atomic(frame, target)
        source["output_sha256"] = file_sha256(target)
    audit_path = output / rules["audit_file"]
    write_dataframe_atomic(combined, audit_path)
    manifest = {
        "scenario": bundle.scenario.scenario.name,
        "scenario_path": str(bundle.scenario_path),
        "base_year": bundle.scenario.periods.base_year,
        "policy": bundle.scenario.sources.nrcan_ceud_beyond_observed,
        "extension_rule": "constant_after_table_observed_endpoint_only",
        "native_source_files_modified": False,
        "observed_history_preserved": True,
        "series_status_counts": combined.status.value_counts().to_dict(),
        "extended_rows": int(combined.extended_rows.sum()),
        "audit_path": str(audit_path), "audit_sha256": file_sha256(audit_path),
        "sources": sources,
    }
    write_text_atomic(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        output / rules["manifest_file"],
    )
    LOGGER.info(
        "CEUD parameter evidence: %s extension rows through %s; series statuses %s",
        manifest["extended_rows"], manifest["base_year"], manifest["series_status_counts"],
    )
    return manifest
