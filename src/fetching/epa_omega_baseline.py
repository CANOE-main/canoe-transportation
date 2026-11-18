"""Validate the registered compact OMEGA baseline for LDV range shares."""

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)

SOURCE = "epa_omega_baseline"
COMPONENT = "baseline_sales_ranges"
LOGGER = logging.getLogger(__name__)


class OmegaBaselineRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    extract_path: Path
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_bytes: int = Field(gt=0)
    expected_rows: int = Field(gt=0)
    model_year: int = Field(ge=1980, le=2100)
    expected_sales_by_powertrain: dict[str, int]
    parent_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    notebook_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_registration(self):
        if not self.extract_path.is_absolute():
            raise ValueError("OMEGA extract path must be absolute")
        if set(self.expected_sales_by_powertrain) != {"BEV", "PHEV"} or any(
            n <= 0 for n in self.expected_sales_by_powertrain.values()
        ):
            raise ValueError(
                "OMEGA registration requires positive BEV/PHEV sales totals"
            )
        return self


class OmegaNormalizationRules(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    model_year: int = Field(ge=1980, le=2100)
    powertrains: tuple[str, ...]
    columns: tuple[str, ...]
    rename: dict[str, str]
    regulatory_class_map: dict[str, str]
    normalized_file: str
    manifest_file: str

    @model_validator(mode="after")
    def validate_native_contract(self):
        required = {
            "Unnamed: 0",
            "manufacturer_id",
            "vehicle_name",
            "model_year",
            "context_size_class",
            "reg_class_id",
            "powertrain_type",
            "onroad_charge_depleting_range_mi",
            "battery_kwh",
            "sales",
        }
        if (
            set(self.powertrains) != {"BEV", "PHEV"}
            or len(self.powertrains) != 2
            or not required <= set(self.columns)
            or len(set(self.columns)) != len(self.columns)
            or self.rename != {"Unnamed: 0": "source_row_id"}
            or set(self.regulatory_class_map) != {"car", "truck"}
            or set(self.regulatory_class_map.values()) != {"car", "light_truck"}
        ):
            raise ValueError(
                "OMEGA rules must retain the native plug-in row/range/sales contract"
            )
        return self


def module_rules(bundle: ConfigBundle) -> OmegaNormalizationRules:
    return OmegaNormalizationRules.model_validate(
        load_harmonization_rules(bundle, SOURCE)
    )


def build_request(bundle: ConfigBundle) -> OmegaBaselineRequest:
    source = bundle.sources.sources[SOURCE]
    if source.status != "active":
        raise ValueError("EPA OMEGA baseline source is inactive")
    payload = dict(source.component(COMPONENT).adapter)
    payload["extract_path"] = resolve_repo_path(bundle.repo_root, payload["extract_path"])
    request = OmegaBaselineRequest.model_validate(payload)
    if request.model_year != module_rules(bundle).model_year:
        raise ValueError("OMEGA source and normalization model years disagree")
    return request


def validate_extract(request: OmegaBaselineRequest) -> None:
    if (
        request.extract_path.stat().st_size != request.expected_bytes
        or file_sha256(request.extract_path) != request.expected_sha256
    ):
        raise ValueError(
            "OMEGA baseline extract differs from its registered immutable snapshot"
        )


def normalize_baseline(
    frame: pd.DataFrame, *, rules: OmegaNormalizationRules
) -> pd.DataFrame:
    """Retain native values/row identity; sales are weights, never catalogue-row counts."""
    required = {rules.rename.get(c, c) for c in rules.columns}
    if not required <= set(frame):
        raise ValueError(
            f"OMEGA baseline columns missing: {sorted(required - set(frame))}"
        )
    if (
        frame.source_row_id.isna().any()
        or frame.source_row_id.duplicated().any()
        or not frame.model_year.eq(rules.model_year).all()
        or not frame.powertrain_type.isin(rules.powertrains).all()
    ):
        raise ValueError("OMEGA baseline row identity, year or powertrain is invalid")
    result = frame.copy()
    for column in ("source_row_id", "sales"):
        values = pd.to_numeric(result[column], errors="raise")
        if (
            not np.isfinite(values).all()
            or (values < 0).any()
            or (values % 1 != 0).any()
        ):
            raise ValueError(f"OMEGA {column} must contain finite nonnegative integers")
        result[column] = values.astype("int64")
    result["market_class"] = result.reg_class_id.map(rules.regulatory_class_map)
    if result.market_class.isna().any():
        raise ValueError("OMEGA baseline has unmapped regulatory classes")
    result["powertrain"] = result.powertrain_type
    result["cd_range_miles"] = pd.to_numeric(
        result.onroad_charge_depleting_range_mi, errors="coerce"
    )
    result["range_status"] = np.where(
        np.isfinite(result.cd_range_miles) & result.cd_range_miles.gt(0),
        "valid",
        "invalid_cd_range",
    )
    LOGGER.debug(
        "OMEGA baseline: %s source records, %s MY%s sales, %s invalid CD ranges",
        len(result),
        int(result.sales.sum()),
        rules.model_year,
        int(result.range_status.ne("valid").sum()),
    )
    return result.sort_values("source_row_id").reset_index(drop=True)


def fetch_baseline(bundle: ConfigBundle, *, publish: bool = True) -> pd.DataFrame:
    """Read only the registered compact file; no network, parent archive or regeneration."""
    request, rules = build_request(bundle), module_rules(bundle)
    validate_extract(request)
    result = normalize_baseline(pd.read_csv(request.extract_path), rules=rules)
    totals = result.groupby("powertrain").sales.sum().to_dict()
    if (
        len(result) != request.expected_rows
        or totals != request.expected_sales_by_powertrain
    ):
        raise ValueError(
            "OMEGA baseline rows/sales differ from the legacy source registration"
        )
    if publish:
        directory = resolve_artifact_path(bundle, "ldv_range_interim")
        write_dataframe_atomic(result, directory / rules.normalized_file)
        write_text_atomic(
            json.dumps(
                {
                    "source": SOURCE,
                    "component": COMPONENT,
                    "sha256": request.expected_sha256,
                    "bytes": request.expected_bytes,
                    "rows": len(result),
                    "model_year": rules.model_year,
                    "sales_by_powertrain": totals,
                    "sales": int(result.sales.sum()),
                    "parent_sha256": request.parent_sha256,
                    "legacy_notebook_sha256": request.notebook_sha256,
                    "weight_basis": "OMEGA baseline sales; not final MY2024 actual production",
                    "range_field": "onroad_charge_depleting_range_mi",
                    "grain": "one original aggregated_vehicles source_row_id; no model-type join",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            directory / rules.manifest_file,
        )
    return result


def required_inputs(bundle: ConfigBundle) -> list[Path]:
    # Historical EV representatives always consume the registered OMEGA weights,
    # even when investable range variants are left unchanged.
    return [build_request(bundle).extract_path]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/default.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    bundle = load_config_bundle(args.scenario)
    frame = fetch_baseline(bundle)
    print(
        json.dumps(
            {"records": len(frame), "sales": int(frame.sales.sum())}, sort_keys=True
        )
    )


if __name__ == "__main__":
    main()
