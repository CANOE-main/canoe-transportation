"""Validate the compact OMEGA baseline used by the legacy LDV range notebook."""

import argparse
import hashlib
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
    parent_path: Path
    parent_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bytes: int = Field(gt=0)
    notebook_path: Path
    notebook_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_registration(self):
        if not all(
            p.is_absolute()
            for p in (self.extract_path, self.parent_path, self.notebook_path)
        ):
            raise ValueError("OMEGA source paths must be absolute")
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
    identity_file: str

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
    for key in ("extract_path", "parent_path", "notebook_path"):
        payload[key] = resolve_repo_path(bundle.repo_root, payload[key])
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
    LOGGER.info(
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


def prepare_baseline_extract(bundle: ConfigBundle) -> dict:
    """Opt-in exact extract registration/identity proof; never overwrite an existing file."""
    request, rules = build_request(bundle), module_rules(bundle)
    if (
        request.parent_path.stat().st_size != request.parent_bytes
        or file_sha256(request.parent_path) != request.parent_sha256
        or file_sha256(request.notebook_path) != request.notebook_sha256
    ):
        raise ValueError(
            "OMEGA parent/notebook differs from the registered legacy evidence"
        )
    notebook = json.loads(request.notebook_path.read_text(encoding="utf-8"))
    named_parent = str(
        request.parent_path.relative_to(request.notebook_path.parent)
    ).replace("\\", "/")
    if not any(named_parent in "".join(c.get("source", [])) for c in notebook["cells"]):
        raise ValueError("Legacy notebook does not name the registered OMEGA parent")
    parent = pd.read_csv(request.parent_path, usecols=list(rules.columns))
    extract = parent.loc[
        parent.model_year.eq(rules.model_year)
        & parent.powertrain_type.isin(rules.powertrains),
        list(rules.columns),
    ].rename(columns=rules.rename)
    encoded = extract.to_csv(index=False, lineterminator="\n").encode()
    if (
        hashlib.sha256(encoded).hexdigest() != request.expected_sha256
        or len(encoded) != request.expected_bytes
    ):
        raise ValueError(
            "Compact OMEGA extract does not reproduce the notebook's source market"
        )
    if not request.extract_path.exists():
        write_text_atomic(encoded.decode(), request.extract_path, newline="")
        LOGGER.info(
            "Registered compact OMEGA baseline extract from the pinned legacy parent"
        )
    validate_extract(request)
    evidence = fetch_baseline(bundle, publish=False)
    identity = {
        "matches_legacy_notebook_source": True,
        "matches_compact_extract_bytes": True,
        "parent_rows": len(parent),
        "extract_rows": len(extract),
        "model_year": rules.model_year,
        "sales_by_powertrain": evidence.groupby("powertrain").sales.sum().to_dict(),
        "parent_sha256": request.parent_sha256,
        "extract_sha256": request.expected_sha256,
        "notebook_sha256": request.notebook_sha256,
        "notebook_parent": named_parent,
    }
    write_text_atomic(
        json.dumps(identity, indent=2, sort_keys=True) + "\n",
        resolve_artifact_path(bundle, "omega_baseline_validation", rules.identity_file),
    )
    LOGGER.info(
        "Verified OMEGA extract matches the exact legacy notebook parent and market totals"
    )
    return identity


def required_inputs(bundle: ConfigBundle) -> list[Path]:
    if bundle.scenario.BEV_PHEV_range_representation.mode == "none":
        return []
    return [build_request(bundle).extract_path]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    parser.add_argument("--verify-legacy-identity", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    bundle = load_config_bundle(args.scenario)
    if args.verify_legacy_identity:
        print(json.dumps(prepare_baseline_extract(bundle), indent=2, sort_keys=True))
    else:
        frame = fetch_baseline(bundle)
        print(
            json.dumps(
                {"records": len(frame), "sales": int(frame.sales.sum())}, sort_keys=True
            )
        )


if __name__ == "__main__":
    main()
