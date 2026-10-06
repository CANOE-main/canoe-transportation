"""Validate and normalize registered legacy profiles without acquiring or simulating."""

from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle,
    file_sha256,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)

SOURCE = "legacy_charging_profiles"
LOGGER = logging.getLogger(__name__)


class LegacyProfileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    registered_path: Path
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0)
    expected_rows: int = Field(gt=0)
    native_interval_seconds: Literal[60]
    calendar_year: int = Field(ge=1900, le=2100)
    calendar_timezone: str

    @model_validator(mode="after")
    def validate_location_and_calendar(self):
        if not self.registered_path.is_absolute():
            raise ValueError("Registered charging path must be absolute")
        ZoneInfo(self.calendar_timezone)
        return self


@dataclass(frozen=True)
class ProfileEvidence:
    hourly: pd.DataFrame
    audit: dict
    source_digest: str


def build_request(bundle: ConfigBundle, profile: str) -> LegacyProfileRequest:
    source = bundle.sources.sources[SOURCE]
    if source.status != "active":
        raise ValueError("Legacy charging profile source is inactive")
    payload = dict(source.component(profile).adapter)
    payload["registered_path"] = resolve_repo_path(
        bundle.repo_root, payload["registered_path"]
    )
    return LegacyProfileRequest.model_validate(payload)


def required_inputs(bundle: ConfigBundle) -> list[Path]:
    selection = bundle.scenario.charging_profiles
    if selection.travel_behavior_source == "none":
        return []
    paths = [build_request(bundle, selection.travel_behavior_source).registered_path]
    if selection.time_mapping is not None:
        paths.append(resolve_repo_path(bundle.repo_root, selection.time_mapping))
    return paths


def transform_hourly(
    raw: pd.DataFrame,
    *,
    year: int,
    timezone: str,
    value_column: str,
    precision: int,
) -> ProfileEvidence:
    """Reproduce compile_cft's hourly mean/peak on physical Toronto-calendar hours.

    cp_to_clustering's Jan 2–Dec 30 trim belongs to clustering, not capacity factors.
    No rolling filter, fill, composition multiplication or post-projection peak scaling.
    """
    if list(raw.columns) != [value_column] or raw.empty:
        raise ValueError("Charging source must contain the one registered value column")
    if not all(str(value).endswith("+00:00") for value in raw.index):
        raise ValueError("Charging timestamps must explicitly identify UTC")
    timestamps = pd.to_datetime(raw.index, utc=True, errors="raise")
    if timestamps.has_duplicates or not timestamps.is_monotonic_increasing:
        raise ValueError("Charging source timestamps must be unique and ordered")
    if not (timestamps[1:] - timestamps[:-1] == pd.Timedelta(minutes=1)).all():
        raise ValueError(
            "Charging source must have uninterrupted one-minute resolution"
        )
    values = pd.to_numeric(raw[value_column], errors="raise")
    local = timestamps.tz_convert(timezone)
    mask = local.year == year
    selected = pd.Series(values.to_numpy()[mask], index=local[mask], name=value_column)
    if (
        selected.empty
        or not np.isfinite(selected.to_numpy()).all()
        or (selected < 0).any()
    ):
        raise ValueError(
            "Selected charging year has missing/nonfinite/negative samples"
        )
    start = pd.Timestamp(year=year, month=1, day=1, tz=timezone)
    end = pd.Timestamp(year=year + 1, month=1, day=1, tz=timezone)
    expected_hours = int((end - start).total_seconds() / 3600)
    if (
        len(selected) != expected_hours * 60
        or selected.index[0] != start
        or selected.index[-1] != end - pd.Timedelta(minutes=1)
    ):
        raise ValueError("Charging source does not cover the full local calendar year")
    hourly = selected.resample("h").mean()
    if len(hourly) != expected_hours or not selected.resample("h").count().eq(60).all():
        raise ValueError("Charging hourly bins must each contain 60 finite samples")
    peak = float(hourly.max())
    if peak <= 0:
        raise ValueError("Charging profile annual peak must be positive")
    integral = float(selected.sum() / 60)
    if not np.isclose(hourly.sum(), integral, rtol=1e-12, atol=1e-8):
        raise ValueError("Hourly charging transformation changed the native integral")
    number = np.arange(expected_hours)
    output = pd.DataFrame(
        {
            "hour_index": number,
            "timestamp_utc": hourly.index.tz_convert("UTC").astype(str),
            "timestamp_local": hourly.index.astype(str),
            "local_day": hourly.index.strftime("D%j"),
            "local_hour": [f"H{x + 1:02d}" for x in hourly.index.hour],
            "season": [f"D{x // 24 + 1:03d}" for x in number],
            "tod": [f"H{x % 24 + 1:02d}" for x in number],
            "native_hourly_mean": hourly.to_numpy(),
            "factor": (hourly / peak).round(precision).to_numpy(),
        }
    )
    audit = {
        "native_interval_seconds": 60,
        "native_rows": len(raw),
        "excluded_outside_local_year": int((~mask).sum()),
        "excluded_nulls_outside_local_year": int(values[~mask].isna().sum()),
        "calendar_year": year,
        "calendar_timezone": timezone,
        "hourly_rows": len(output),
        "hourly_operation": "mean",
        "rolling_filter": False,
        "native_integral_in_hour_units": integral,
        "hourly_integral": float(hourly.sum()),
        "annual_hourly_peak": peak,
        "normalized_integral": float(output.factor.sum()),
        "precision": precision,
        "label_basis": "elapsed_hours_from_local_year_start",
        "repeated_wall_time_keys": int(
            output.duplicated(["local_day", "local_hour"]).sum()
        ),
        "embedded_range_shares": True,
        "market_share_reweighting": False,
        "clustered_legacy_parity": "not_comparable_without_the_existing_cluster_mapping",
        "limitations": [
            "Frozen inherited range/fleet/charger composition; no new-market reweighting",
            "TTS contains weekday-only travel behavior",
        ],
    }
    return ProfileEvidence(output, audit, "")


def normalize_profile(bundle: ConfigBundle, profile: str) -> ProfileEvidence:
    """Validate physical evidence before parsing; retain all native hourly audit values."""
    request = build_request(bundle, profile)
    path = request.registered_path
    if (
        path.stat().st_size != request.expected_bytes
        or file_sha256(path) != request.expected_sha256
    ):
        raise ValueError(
            f"Registered {profile} charging file differs from its frozen snapshot"
        )
    raw = pd.read_csv(path, index_col=0)
    if len(raw) != request.expected_rows:
        raise ValueError("Charging source row count differs from registration")
    rules = load_harmonization_rules(bundle, SOURCE)
    if (rules["hourly_operation"], rules["normalization"], rules["label_basis"]) != (
        "mean",
        "annual_hourly_peak",
        "elapsed_hours_from_local_year_start",
    ):
        raise ValueError(
            "Charging transformation requires an implemented, reviewed rule"
        )
    evidence = transform_hourly(
        raw,
        year=request.calendar_year,
        timezone=request.calendar_timezone,
        value_column=rules["value_column"],
        precision=rules["precision"],
    )
    audit = {
        **evidence.audit,
        "profile": profile,
        "sha256": request.expected_sha256,
        "source_units": bundle.sources.sources[SOURCE].units,
    }
    directory = resolve_artifact_path(bundle, "charging_profiles_interim")
    write_dataframe_atomic(
        evidence.hourly, directory / rules["hourly_file"].format(profile=profile)
    )
    write_text_atomic(
        json.dumps(audit, indent=2, sort_keys=True) + "\n",
        directory / rules["manifest_file"].format(profile=profile),
    )
    LOGGER.info(
        "Normalized %s charging: %s minute rows -> %s hours; peak=%s integral=%s; excluded=%s (nulls=%s)",
        profile,
        len(raw),
        len(evidence.hourly),
        audit["annual_hourly_peak"],
        audit["hourly_integral"],
        audit["excluded_outside_local_year"],
        audit["excluded_nulls_outside_local_year"],
    )
    LOGGER.warning(
        "Legacy charging composition is already embedded; do not apply new range shares to this profile"
    )
    return ProfileEvidence(evidence.hourly, audit, request.expected_sha256)
