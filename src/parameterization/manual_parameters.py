"""Validate compact manual parameters and resolve them to technology rows."""

import argparse
import os
import re
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
)
from validation.config_models import SourceComponent


RULE_KEY = "manual_parameters"
NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class ManualParameterError(ValueError):
    """Raised when a manual registry or selector contract is invalid."""


class ManualAdapterContract(BaseModel):
    """Validated component adapter for one manual-parameter source slice."""

    model_config = ConfigDict(extra="allow", strict=True)

    manual_parameter_path: NonEmptyString
    expected_columns: list[NonEmptyString] = Field(min_length=1)
    unique_key: list[NonEmptyString] = Field(min_length=1)
    source_selector: NonEmptyString
    expected_rows: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_columns(self) -> "ManualAdapterContract":
        missing = sorted(set(self.unique_key) - set(self.expected_columns))
        if missing:
            raise ValueError(
                "manual adapter unique_key is absent from expected_columns: "
                + ", ".join(missing)
            )
        return self


class TechnologySelectorTarget(BaseModel):
    """Trusted selector fields read from the backend-owned technology template."""

    model_config = ConfigDict(extra="ignore", strict=True)

    tech: NonEmptyString
    category: NonEmptyString
    sub_category: NonEmptyString


class ManualTechnologySelector(BaseModel):
    """One compact, user-maintained technology selector."""

    model_config = ConfigDict(extra="forbid", strict=True)

    technology_class: NonEmptyString
    powertrain: NonEmptyString | None = None
    parameter: NonEmptyString | None = None


@dataclass(frozen=True)
class ManualRegistration:
    """Internal association between a source component and its adapter."""

    source_id: str
    component_id: str
    component: SourceComponent
    adapter: ManualAdapterContract


def _normal_form(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _alias_lookup(rules: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for canonical, values in rules.get("powertrain_aliases", {}).items():
        canonical_key = _normal_form(str(canonical))
        for value in [canonical, *values]:
            key = _normal_form(str(value))
            existing = lookup.get(key)
            if existing is not None and existing != canonical_key:
                raise ManualParameterError(
                    f"Powertrain alias {value!r} maps to both "
                    f"{existing!r} and {canonical_key!r}"
                )
            lookup[key] = canonical_key
    return lookup


def _canonical_powertrain(value: str, aliases: dict[str, str]) -> str:
    normalized = _normal_form(value)
    return aliases.get(normalized, normalized)


def registered_manual_components(bundle: ConfigBundle) -> list[ManualRegistration]:
    """Return every component-level manual adapter after Pydantic validation."""
    registrations: list[ManualRegistration] = []
    for source_id, source in bundle.sources.sources.items():
        for component_id, component in source.components.items():
            if "manual_parameter_path" not in component.adapter:
                continue
            try:
                adapter = ManualAdapterContract.model_validate(component.adapter)
            except ValueError as exc:
                raise ManualParameterError(
                    f"Invalid manual adapter {source_id}.{component_id}: {exc}"
                ) from exc
            registrations.append(
                ManualRegistration(
                    source_id=str(source_id),
                    component_id=str(component_id),
                    component=component,
                    adapter=adapter,
                )
            )
    return sorted(
        registrations,
        key=lambda item: (
            item.adapter.manual_parameter_path,
            item.source_id,
            item.component_id,
        ),
    )


def validate_manual_registry(
    bundle: ConfigBundle,
    *,
    source_column: str,
    notes_column: str,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Validate every manual CSV, citation selector, and component ownership."""
    manual_dir = resolve_input_path(bundle, "manual")
    actual_files = {path.name for path in manual_dir.glob("*.csv")}
    registrations = registered_manual_components(bundle)
    registered_files = {
        registration.adapter.manual_parameter_path
        for registration in registrations
    }
    if actual_files != registered_files:
        raise ManualParameterError(
            "Manual file registry mismatch: "
            f"unregistered={sorted(actual_files - registered_files)}, "
            f"missing={sorted(registered_files - actual_files)}"
        )

    by_file: dict[str, list[ManualRegistration]] = defaultdict(list)
    for registration in registrations:
        by_file[registration.adapter.manual_parameter_path].append(registration)

    frames: dict[str, pd.DataFrame] = {}
    registry_rows: list[dict[str, Any]] = []
    for filename in sorted(by_file):
        file_registrations = by_file[filename]
        expected_column_contracts = {
            tuple(registration.adapter.expected_columns)
            for registration in file_registrations
        }
        unique_key_contracts = {
            tuple(registration.adapter.unique_key)
            for registration in file_registrations
        }
        if len(expected_column_contracts) != 1 or len(unique_key_contracts) != 1:
            raise ManualParameterError(
                f"Manual components disagree on the schema or key for {filename}"
            )
        expected_columns = list(next(iter(expected_column_contracts)))
        unique_key = list(next(iter(unique_key_contracts)))
        path = manual_dir / filename
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        if list(frame.columns) != expected_columns:
            raise ManualParameterError(
                f"{filename} columns {list(frame.columns)} do not match "
                f"registered columns {expected_columns}"
            )
        duplicated = frame.duplicated(unique_key, keep=False)
        if duplicated.any():
            duplicate_rows = [int(index) + 2 for index in frame.index[duplicated]]
            raise ManualParameterError(
                f"{filename} has duplicate registered keys at rows {duplicate_rows}"
            )
        if source_column not in frame or notes_column not in frame:
            raise ManualParameterError(
                f"{filename} must contain {source_column!r} and {notes_column!r}"
            )

        covered_rows: set[int] = set()
        registered_source_ids = pd.Series("", index=frame.index, dtype="string")
        registered_component_ids = pd.Series("", index=frame.index, dtype="string")
        for registration in file_registrations:
            adapter = registration.adapter
            selected = set(
                int(index)
                for index in frame.index[
                    frame[source_column].eq(adapter.source_selector)
                ]
            )
            if len(selected) != adapter.expected_rows:
                raise ManualParameterError(
                    f"{registration.source_id}.{registration.component_id} selects "
                    f"{len(selected)} rows from {filename}; expected "
                    f"{adapter.expected_rows}"
                )
            overlap = covered_rows.intersection(selected)
            if overlap:
                source_rows = sorted(index + 2 for index in overlap)
                raise ManualParameterError(
                    f"{filename} rows {source_rows} are selected by multiple components"
                )
            covered_rows.update(selected)
            registered_source_ids.loc[list(selected)] = registration.source_id
            registered_component_ids.loc[list(selected)] = (
                registration.component_id
            )
            if "technology_class" in frame:
                selected_classes = set(
                    frame.loc[list(selected), "technology_class"]
                )
                applies_to = set(map(str, registration.component.applies_to))
                if selected_classes != applies_to:
                    raise ManualParameterError(
                        f"{registration.source_id}.{registration.component_id} "
                        f"applies_to {sorted(applies_to)} does not match selected "
                        f"technology_class values {sorted(selected_classes)}"
                    )
            registry_rows.append(
                {
                    "source_id": registration.source_id,
                    "component_id": registration.component_id,
                    "manual_file": filename,
                    "selected_rows": len(selected),
                    "total_file_rows": len(frame),
                    "expected_columns": "|".join(expected_columns),
                    "unique_key": "|".join(unique_key),
                    "source_selector": adapter.source_selector,
                }
            )

        cited_rows = set(
            int(index)
            for index in frame.index[
                frame[source_column].astype(str).str.strip().ne("")
            ]
        )
        if covered_rows != cited_rows:
            raise ManualParameterError(
                f"{filename} cited-row coverage mismatch: "
                f"uncovered={sorted(index + 2 for index in cited_rows - covered_rows)}, "
                f"unexpected={sorted(index + 2 for index in covered_rows - cited_rows)}"
            )
        uncited = frame.loc[~frame.index.isin(cited_rows)]
        if not uncited[notes_column].astype(str).str.strip().ne("").all():
            raise ManualParameterError(
                f"{filename} has an uncited row without an explanatory note"
            )
        audited_frame = frame.copy()
        audited_frame.insert(
            0,
            "registered_component_id",
            registered_component_ids,
        )
        audited_frame.insert(
            0,
            "registered_source_id",
            registered_source_ids,
        )
        frames[filename] = audited_frame

    registry = pd.DataFrame(registry_rows).sort_values(
        ["manual_file", "source_id", "component_id"],
        kind="stable",
    )
    return registry.reset_index(drop=True), frames


def validate_technology_selectors(
    technology: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Validate and return the technology fields used by compact selectors."""
    columns = [
        str(rules["technology_id_column"]),
        str(rules["technology_category_column"]),
        str(rules["technology_sub_category_column"]),
    ]
    missing = sorted(set(columns) - set(technology.columns))
    if missing:
        raise ManualParameterError(
            "Technology template is missing selector columns: " + ", ".join(missing)
        )
    selected = technology.loc[:, columns].copy()
    selected.columns = ["tech", "category", "sub_category"]
    if selected["tech"].astype(str).str.strip().eq("").any():
        rows = [
            int(index) + 2
            for index in selected.index[
                selected["tech"].astype(str).str.strip().eq("")
            ]
        ]
        raise ManualParameterError(
            f"Technology template has blank technology IDs at rows {rows}"
        )
    if selected["tech"].duplicated().any():
        duplicates = sorted(
            selected.loc[selected["tech"].duplicated(False), "tech"]
        )
        raise ManualParameterError(
            f"Technology template has duplicate technology IDs: {duplicates}"
        )
    blank_category = selected["category"].astype(str).str.strip().eq("")
    blank_sub_category = selected["sub_category"].astype(str).str.strip().eq("")
    partial = blank_category ^ blank_sub_category
    if partial.any():
        rows = [int(index) + 2 for index in selected.index[partial]]
        raise ManualParameterError(
            "Technology selector metadata must provide both category and "
            f"sub_category; partial rows: {rows}"
        )
    # Backend structural technologies such as blending and dummy processes do
    # not participate in compact transport-parameter selection.
    selected = selected.loc[~(blank_category & blank_sub_category)].copy()
    validated: list[dict[str, str]] = []
    for row_number, row in enumerate(selected.to_dict("records"), start=2):
        try:
            target = TechnologySelectorTarget.model_validate(row)
        except ValueError as exc:
            raise ManualParameterError(
                f"Invalid technology selector fields at technology.csv row "
                f"{row_number}: {exc}"
            ) from exc
        validated.append(target.model_dump())
    result = pd.DataFrame(validated)
    return result.sort_values("tech", kind="stable").reset_index(drop=True)


def _selector_parts(
    row: pd.Series,
    *,
    rules: dict[str, Any],
) -> tuple[ManualTechnologySelector, str, int | None]:
    powertrain_column = str(rules["powertrain_column"])
    parameter_column = str(rules["parameter_column"])
    raw_powertrain = (
        str(row[powertrain_column]).strip()
        if powertrain_column in row.index and str(row[powertrain_column]).strip()
        else None
    )
    raw_parameter = (
        str(row[parameter_column]).strip()
        if parameter_column in row.index and str(row[parameter_column]).strip()
        else None
    )
    selector = ManualTechnologySelector.model_validate(
        {
            "technology_class": str(
                row[str(rules["technology_class_column"])]
            ),
            "powertrain": raw_powertrain,
            "parameter": raw_parameter,
        }
    )
    base = selector.powertrain or str(rules["wildcard_selector"])
    selector_year: int | None = None
    match = re.fullmatch(str(rules["projection_year_pattern"]), base)
    if match is not None:
        base = match.group("base")
        selector_year = int(match.group("year"))
    return selector, base, selector_year


def resolve_manual_parameters(
    frames: dict[str, pd.DataFrame],
    technology: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Expand compact manual selectors and retain unmatched rows as evidence."""
    aliases = _alias_lookup(rules)
    wildcard = _normal_form(str(rules["wildcard_selector"]))
    remainder = _normal_form(str(rules["remainder_selector"]))
    technology_classes = set(technology["category"])
    technology_class_column = str(rules["technology_class_column"])

    resolution_rows: list[dict[str, Any]] = []
    reconciliation_rows: list[dict[str, Any]] = []
    finding_rows: list[dict[str, Any]] = []

    for filename in sorted(frames):
        frame = frames[filename]
        if technology_class_column not in frame:
            for index, row in frame.iterrows():
                reconciliation_rows.append(
                    {
                        "manual_file": filename,
                        "manual_row": int(index) + 2,
                        "registered_source_id": row.get(
                            "registered_source_id",
                            "",
                        ),
                        "registered_component_id": row.get(
                            "registered_component_id",
                            "",
                        ),
                        "technology_class": "",
                        "powertrain": "",
                        "powertrain_base": "",
                        "parameter": "",
                        "selector_year": pd.NA,
                        "matched_technology_count": pd.NA,
                        "resolution_status": "not_technology_scoped",
                        "detail": (
                            "Manual file uses a separate vehicle-class mapping "
                            "contract."
                        ),
                    }
                )
            continue

        prepared: list[dict[str, Any]] = []
        for index, row in frame.iterrows():
            selector, powertrain_base, selector_year = _selector_parts(
                row,
                rules=rules,
            )
            if selector.technology_class not in technology_classes:
                raise ManualParameterError(
                    f"{filename} row {int(index) + 2} technology_class "
                    f"{selector.technology_class!r} is absent from "
                    "technology.category"
                )
            candidates = technology.loc[
                technology["category"].eq(selector.technology_class)
            ]
            prepared.append(
                {
                    "index": int(index),
                    "row": row,
                    "selector": selector,
                    "powertrain_base": powertrain_base,
                    "selector_year": selector_year,
                    "candidates": candidates,
                }
            )

        explicit_matches: dict[int, set[str]] = {}
        for item in prepared:
            selector = item["selector"]
            base_normal = _normal_form(item["powertrain_base"])
            candidates = item["candidates"]
            if base_normal == wildcard:
                matches = set(candidates["tech"])
            elif base_normal == remainder:
                matches = set()
            else:
                canonical = _canonical_powertrain(
                    item["powertrain_base"],
                    aliases,
                )
                matches = set(
                    candidates.loc[
                        candidates["sub_category"].map(
                            lambda value: _canonical_powertrain(
                                str(value),
                                aliases,
                            )
                        ).eq(canonical),
                        "tech",
                    ]
                )
            explicit_matches[item["index"]] = matches

        for item in prepared:
            selector = item["selector"]
            base_normal = _normal_form(item["powertrain_base"])
            matches = explicit_matches[item["index"]]
            method = "category_all" if base_normal == wildcard else "powertrain"
            if item["selector_year"] is not None:
                method = "year_qualified_powertrain"
            if base_normal == remainder:
                group_peers = [
                    peer
                    for peer in prepared
                    if peer["selector"].technology_class
                    == selector.technology_class
                    and peer["selector"].parameter == selector.parameter
                    and peer["index"] != item["index"]
                ]
                if any(
                    _normal_form(peer["powertrain_base"]) == wildcard
                    for peer in group_peers
                ):
                    raise ManualParameterError(
                        f"{filename} row {item['index'] + 2} uses remainder "
                        "alongside all for the same technology_class and parameter"
                    )
                excluded: set[str] = set()
                for peer in group_peers:
                    if _normal_form(peer["powertrain_base"]) != remainder:
                        excluded.update(explicit_matches[peer["index"]])
                matches = set(item["candidates"]["tech"]) - excluded
                method = "remainder"

            original = item["row"].to_dict()
            selector_year = item["selector_year"]
            for tech in sorted(matches):
                target = technology.loc[technology["tech"].eq(tech)].iloc[0]
                resolution_rows.append(
                    {
                        "manual_file": filename,
                        "manual_row": item["index"] + 2,
                        **original,
                        "powertrain_base": item["powertrain_base"],
                        "selector_year": selector_year,
                        "tech": target["tech"],
                        "technology_category": target["category"],
                        "technology_sub_category": target["sub_category"],
                        "resolution_method": method,
                    }
                )

            status = "matched" if matches else "unmatched"
            detail = (
                f"Resolved to {len(matches)} technology row(s)."
                if matches
                else (
                    "No technology.sub_category matches this configured "
                    "powertrain selector; row retained as unresolved evidence."
                )
            )
            reconciliation_rows.append(
                {
                    "manual_file": filename,
                    "manual_row": item["index"] + 2,
                    "registered_source_id": original.get(
                        "registered_source_id",
                        "",
                    ),
                    "registered_component_id": original.get(
                        "registered_component_id",
                        "",
                    ),
                    "technology_class": selector.technology_class,
                    "powertrain": selector.powertrain or "",
                    "powertrain_base": item["powertrain_base"],
                    "parameter": selector.parameter or "",
                    "selector_year": selector_year,
                    "matched_technology_count": len(matches),
                    "resolution_status": status,
                    "detail": detail,
                }
            )
            if not matches:
                finding_rows.append(
                    {
                        "manual_file": filename,
                        "manual_row": item["index"] + 2,
                        "registered_source_id": original.get(
                            "registered_source_id",
                            "",
                        ),
                        "registered_component_id": original.get(
                            "registered_component_id",
                            "",
                        ),
                        "issue_type": "unmatched_powertrain_selector",
                        "technology_class": selector.technology_class,
                        "powertrain": selector.powertrain or "",
                        "parameter": selector.parameter or "",
                        "detail": detail,
                    }
                )

    resolution = pd.DataFrame(resolution_rows)
    reconciliation = pd.DataFrame(reconciliation_rows)
    findings = pd.DataFrame(
        finding_rows,
        columns=[
            "manual_file",
            "manual_row",
            "registered_source_id",
            "registered_component_id",
            "issue_type",
            "technology_class",
            "powertrain",
            "parameter",
            "detail",
        ],
    )
    if not resolution.empty:
        conflict_columns = ["manual_file", "tech", "selector_year"]
        if "parameter" in resolution:
            conflict_columns.insert(2, "parameter")
        conflicts = resolution.duplicated(conflict_columns, keep=False)
        if conflicts.any():
            keys = (
                resolution.loc[conflicts, conflict_columns]
                .drop_duplicates()
                .to_dict("records")
            )
            raise ManualParameterError(
                f"Manual selectors create conflicting technology applications: {keys}"
            )
        resolution = resolution.sort_values(
            ["manual_file", "manual_row", "tech"],
            kind="stable",
        ).reset_index(drop=True)
        resolution["selector_year"] = pd.to_numeric(
            resolution["selector_year"],
            errors="coerce",
        ).astype("Int64")
    reconciliation["selector_year"] = pd.to_numeric(
        reconciliation["selector_year"],
        errors="coerce",
    ).astype("Int64")
    reconciliation["matched_technology_count"] = pd.to_numeric(
        reconciliation["matched_technology_count"],
        errors="coerce",
    ).astype("Int64")
    reconciliation = reconciliation.sort_values(
        ["manual_file", "manual_row"],
        kind="stable",
    ).reset_index(drop=True)
    findings = findings.sort_values(
        ["manual_file", "manual_row"],
        kind="stable",
    ).reset_index(drop=True)
    unmatched_policy = str(rules["unmatched_policy"])
    if unmatched_policy not in {"report", "error"}:
        raise ManualParameterError(
            f"Unsupported manual selector unmatched_policy {unmatched_policy!r}"
        )
    if unmatched_policy == "error" and not findings.empty:
        raise ManualParameterError(
            "Manual technology selectors remain unmatched: "
            + ", ".join(
                f"{row.manual_file}:{row.manual_row}"
                for row in findings.itertuples(index=False)
            )
        )
    return resolution, reconciliation, findings


def _write_dataframe_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv.tmp",
            dir=path.parent,
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            frame.to_csv(temporary, index=False, lineterminator="\n")
        os.replace(temporary_name, path)
    finally:
        if temporary_name is not None:
            temporary_path = Path(temporary_name)
            if temporary_path.exists():
                temporary_path.unlink()


def build_manual_parameter_artifacts(scenario_path: str | Path) -> Path:
    """Validate current manual inputs and publish selector resolution evidence."""
    bundle = load_config_bundle(scenario_path)
    rules = load_harmonization_rules(bundle, RULE_KEY)
    registry, frames = validate_manual_registry(
        bundle,
        source_column=str(rules["source_column"]),
        notes_column=str(rules["notes_column"]),
    )
    technology_path = resolve_input_path(
        bundle,
        "template",
        str(rules["technology_template_file"]),
    )
    technology = validate_technology_selectors(
        pd.read_csv(technology_path, dtype=str, keep_default_na=False),
        rules=rules,
    )
    resolution, reconciliation, findings = resolve_manual_parameters(
        frames,
        technology,
        rules=rules,
    )
    output_dir = resolve_input_path(
        bundle,
        "interim",
        str(rules["interim_subdir"]),
    )
    outputs = {
        str(rules["registry_file"]): registry,
        str(rules["resolution_file"]): resolution,
        str(rules["reconciliation_file"]): reconciliation,
        str(rules["findings_file"]): findings,
    }
    for filename, frame in outputs.items():
        _write_dataframe_atomic(frame, output_dir / filename)
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = build_manual_parameter_artifacts(args.scenario)
    print(f"Wrote resolved manual parameter artifacts to {output_dir}")


if __name__ == "__main__":
    main()
