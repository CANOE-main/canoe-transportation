"""GREET source contracts, physical extraction and deterministic offline evidence."""

from dataclasses import dataclass
import hashlib
import json
import logging
from math import isfinite
from pathlib import Path
import re
from typing import Annotated, Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (
    ConfigBundle, file_sha256, load_harmonization_rules, resolve_artifact_path,
    resolve_input_path, write_dataframe_atomic,
)


LOGGER = logging.getLogger(__name__)
SOURCE = "argonne_rd_greet_2025_rev1"
COMPONENT = "road_vehicle_cycle_emissions"
Cell = Annotated[str, Field(pattern=r"^[A-Z]+[1-9][0-9]*$")]


class NativeControl(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(min_length=1)
    cell: Cell


class FunctionCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    workbook: Literal["greet1", "greet2"]
    name: str = Field(min_length=1)
    arguments: list[str | int | float]
    expected: str | int | float


class LightweightGliderAudit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    worksheets: list[str] = Field(min_length=1)
    formula_cell: Cell
    header_row: int = Field(ge=1)
    columns: list[Annotated[str, Field(pattern=r"^[A-Z]+$")]] = Field(min_length=1)
    range_reference: str = Field(min_length=1)
    header_pattern: str = Field(min_length=1)


class GenerationSettings(BaseModel):
    """Source-native controls and bounded execution, validated before Excel I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    timeout_seconds: int = Field(ge=1)
    max_calculation_passes: int = Field(ge=2, le=10)
    relative_tolerance: float = Field(ge=0)
    absolute_tolerance: float = Field(ge=0)
    macro: str = Field(min_length=1)
    macro_arguments: list[str | bool | int | float]
    names: dict[str, NativeControl]
    range_dropdown_names: dict[str, str]
    range_labels: dict[Cell, str]
    range_unit: dict[str, str]
    imported_controls: dict[str, Cell]
    preserved_ldv_selectors: list[Cell]
    function_checks: list[FunctionCheck] = Field(min_length=1)
    lightweight_glider: LightweightGliderAudit
    files: dict[str, str]

    @model_validator(mode="after")
    def bounded_controls_and_files(self):
        if set(self.names) != {"simulation_year", "ldv_selector", "phev_range_miles", "bev_range_miles"}:
            raise ValueError("GREET requires four configured control roles")
        if set(self.imported_controls) != {"ldv_selector", "phev_range_miles", "bev_range_miles"}:
            raise ValueError("GREET requires all imported control echoes")
        if set(self.range_dropdown_names) != {"phev", "bev"}:
            raise ValueError("GREET requires both native range dropdowns")
        if re.fullmatch(r"[A-Z]+[1-9][0-9]*", self.range_unit["cell"]) is None:
            raise ValueError("GREET range-unit anchor must be a worksheet cell")
        if set(self.files) != {"results", "exclusions", "cases", "report"}:
            raise ValueError("GREET requires all evidence artifacts")
        if len(set(self.files.values())) != len(self.files) or any(
            Path(name).name != name or name in {"", ".", "..", "owned_excel.json", "execution.log"}
            for name in self.files.values()
        ):
            raise ValueError("GREET output names must be relative filenames")
        return self


class GreetRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    directory: str = Field(min_length=1)
    greet1_file: str
    greet2_file: str
    solved_simulation_year: int = Field(ge=1900, le=2100)
    ldv_vehicle_cohort_year: int = Field(ge=1900, le=2100)
    manifest_file: str
    automation: GenerationSettings

    @model_validator(mode="after")
    def relative_inputs(self):
        path = Path(self.directory)
        if path.is_absolute() or len(path.parts) != 1 or path.name in {".", ".."}:
            raise ValueError("GREET directory must be a single relative folder")
        for name in (self.greet1_file, self.greet2_file):
            if Path(name).name != name or Path(name).suffix != ".xlsm":
                raise ValueError("GREET inputs must be relative .xlsm filenames")
        if self.greet1_file == self.greet2_file:
            raise ValueError("Duplicate registered GREET workbook")
        if Path(self.manifest_file).name != self.manifest_file or self.manifest_file in {
            "", ".", "..", *self.automation.files.values(), "owned_excel.json", "execution.log",
        }:
            raise ValueError("GREET manifest must be a distinct relative filename")
        return self


class ResultManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1]
    source_id: str
    component: str
    profile_sha256: str
    source_hashes: dict[str, str]
    files: dict[str, str]


@dataclass(frozen=True)
class GreetEvidence:
    rows: pd.DataFrame
    exclusions: pd.DataFrame
    models: pd.DataFrame
    paths: tuple[Path, ...]
    raw_rows: pd.DataFrame
    audit: dict[str, Any]


def registered_workbooks(bundle: ConfigBundle) -> tuple[GreetRegistration, tuple[Path, ...]]:
    if SOURCE not in bundle.sources.sources or bundle.sources.sources[SOURCE].status != "active":
        raise ValueError("The GREET vehicle-cycle source must be active")
    component = bundle.sources.sources[SOURCE].component(COMPONENT)
    bundle.sources.resolved_data_quality(SOURCE, COMPONENT).row_fields()
    registration = GreetRegistration.model_validate(component.adapter)
    return registration, tuple(resolve_input_path(bundle, "external", registration.directory, name)
                               for name in (registration.greet1_file, registration.greet2_file))


def result_bank_paths(bundle: ConfigBundle) -> dict[str, Path]:
    registration, _ = registered_workbooks(bundle)
    root = resolve_artifact_path(bundle, "greet_vehicle_cycle_results")
    return {key: root / name for key, name in {
        **registration.automation.files, "manifest": registration.manifest_file,
    }.items()}


def required_inputs(bundle: ConfigBundle) -> list[Path]:
    """The coarse assembly consumes a complete source handoff, never starts Excel."""
    if not bundle.scenario.embodied_emissions:
        return []
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    atb_path = resolve_input_path(bundle, "interim", atb["interim_subdir"], atb["components"]["vehicles"]["output_file"])
    return [*registered_workbooks(bundle)[1], *result_bank_paths(bundle).values(), atb_path]


def generation_profile(bundle: ConfigBundle) -> str:
    """Exclude run paths/timing and material selection from the source identity."""
    from fetching.greet_automation import probe_archetypes

    registration, _ = registered_workbooks(bundle)
    ranges, atb = probe_archetypes(bundle)
    payload = {"registration": registration.model_dump(),
               "extraction": load_harmonization_rules(bundle, "greet_vehicle_cycle"),
               "archetypes": ranges, "atb_sha256": file_sha256(atb)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _sheet(book: Any, name: str, last_row: int, last_column: int) -> dict[str, Any]:
    if name not in book.sheetnames:
        raise ValueError(f"GREET workbook lacks worksheet {name}")
    return {cell.coordinate: cell.value
            for row in book[name].iter_rows(max_row=last_row, max_col=last_column)
            for cell in row if cell.value is not None}


def _expect(cells: dict, coordinate: str, expected: Any, label: str) -> Any:
    value = cells.get(coordinate)
    if isinstance(value, str):
        value = value.strip()
    if value != expected or (isinstance(expected, int) and isinstance(value, bool)):
        raise ValueError(f"Unexpected GREET {label}!{coordinate}: {value!r}; expected {expected!r}")
    return value


def extract_summary_rows(
    cells: dict[str, Any], *, family: str, model: dict[str, Any], rules: dict,
) -> tuple[list[dict], list[dict]]:
    """Validate labels/units beside configured anchors before retaining native totals."""
    spec, layout = rules[family], rules["summary_layout"]
    _expect(cells, layout["scope_cell"], spec["scope_label"], spec["worksheet"])
    starts = [block["first_row"] for block in spec["blocks"]]
    if any(b - a != rules["block_interval"] for a, b in zip(starts, starts[1:])):
        raise ValueError("GREET extraction blocks violate the configured row interval")
    records, excluded = [], []
    for block in spec["blocks"]:
        first = block["first_row"]
        _expect(cells, f"{layout['label_column']}{first + layout['block_label_offset']}", block["label"], spec["worksheet"])
        _expect(cells, f"{layout['unit_column']}{first + layout['unit_offset']}", rules["source_unit_label"], spec["worksheet"])
        _expect(cells, f"{spec['column']}{first + layout['total_offset']}", layout["total_label"], spec["worksheet"])
        for offset, gas in enumerate(rules["gas_rows"]):
            row = first + offset
            label_cell = f"{layout['label_column']}{row}"
            _expect(cells, label_cell, gas, spec["worksheet"])
            coordinate = f"{spec['column']}{row}"
            value = cells.get(coordinate)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
                raise ValueError(f"Invalid GREET numeric result: {spec['worksheet']}!{coordinate}: {value!r}")
            record = {**model, "source_id": SOURCE, "component": COMPONENT,
                      "family": family, "vehicle_class": model[family + "_class"],
                      "worksheet": spec["worksheet"], "cell": coordinate, "label_cell": label_cell,
                      "block_label": block["label"], "powertrain": block["powertrain"],
                      "materials": block.get("materials", "none"), "gas_label": gas,
                      "source_value": float(value), "source_units": rules["source_units"],
                      "scope": "vehicle_cycle_total_no_eol_credits"}
            if gas == rules["excluded_gas"]:
                excluded.append({**record, "reason": "combined_CO2_measure_excluded"})
            else:
                records.append(record)
    return records, excluded


def validate_generation_evidence(bundle, raw, exclusions, cases, report) -> dict:
    """Validate complete class/range/gas evidence and the full source audit handoff."""
    from fetching.greet_automation import probe_archetypes, probe_cases, validate_probe_results

    registration, paths = registered_workbooks(bundle)
    rules, settings = load_harmonization_rules(bundle, "greet_vehicle_cycle"), registration.automation
    expected_cases = probe_cases(rules, probe_archetypes(bundle)[0])
    if not report.get("ok") or not report.get("source_hashes_unchanged"):
        raise ValueError("GREET generation report has not passed source-preservation validation")
    if report.get("source_id") != SOURCE or report.get("component") != COMPONENT:
        raise ValueError("Unexpected GREET generation source/component")
    if report.get("profile_sha256") != generation_profile(bundle):
        raise ValueError("Stale GREET generation profile; explicitly regenerate the result bank")
    hashes = {p.relative_to(bundle.repo_root).as_posix(): file_sha256(p) for p in paths}
    if report.get("source_hashes") != hashes:
        raise ValueError("Stale GREET registered workbook hashes; explicitly regenerate the result bank")
    for key in expected_cases[0]:
        if cases[key].tolist() != [c[key] for c in expected_cases]:
            raise ValueError(f"Unexpected GREET case audit: {key}")
    if not cases.converged_vehicle_cycle_totals.eq(True).all() or not cases.calculation_passes.between(2, settings.max_calculation_passes).all():
        raise ValueError("Incomplete GREET convergence audit")
    validation = validate_probe_results(raw, settings, cases=expected_cases, rules=rules)
    if json.loads(json.dumps(validation)) != report.get("validation"):
        raise ValueError("GREET report disagrees with retained numeric evidence")
    for frame, gases in ((raw, [g for g in rules["gas_rows"] if g != rules["excluded_gas"]]),
                         (exclusions, [rules["excluded_gas"]])):
        if frame.empty or frame.source_value.map(lambda v: isinstance(v, bool) or not isinstance(v, (int, float)) or not isfinite(v) or v < 0).any():
            raise ValueError("Invalid GREET retained native values")
        expected = {}
        for case in expected_cases:
            for family in ("ldv", "mhdv"):
                spec = rules[family]
                for block in spec["blocks"]:
                    for gas in gases:
                        offset = rules["gas_rows"].index(gas)
                        row = block["first_row"] + offset
                        key = (case["case_id"], family, case[family + "_class"], block["powertrain"], block.get("materials", "none"), gas)
                        expected[key] = {**case, "worksheet": spec["worksheet"], "cell": f"{spec['column']}{row}",
                                         "label_cell": f"{rules['summary_layout']['label_column']}{row}", "block_label": block["label"]}
        keys = ["case_id", "family", "vehicle_class", "powertrain", "materials", "gas_label"]
        actual = list(frame[keys].itertuples(index=False, name=None))
        if len(set(actual)) != len(frame) or set(actual) != set(expected):
            raise ValueError("Incomplete GREET native anchor/exclusion coverage")
        invariants = {"source_id": SOURCE, "component": COMPONENT, "source_units": rules["source_units"],
                      "solved_simulation_year": registration.solved_simulation_year,
                      "ldv_vehicle_cohort_year": registration.ldv_vehicle_cohort_year,
                      "scope": "vehicle_cycle_total_no_eol_credits"}
        for number, path in enumerate(paths, 1):
            invariants[f"greet{number}_file"] = path.relative_to(bundle.repo_root).as_posix()
            invariants[f"greet{number}_sha256"] = hashes[path.relative_to(bundle.repo_root).as_posix()]
        for key, row in zip(actual, frame.to_dict("records"), strict=True):
            if any(row.get(col) != value for col, value in {**expected[key], **invariants}.items()):
                raise ValueError(f"Unexpected GREET retained control/anchor/source metadata: {key}")
        if frame is exclusions and not frame.reason.eq("combined_CO2_measure_excluded").all():
            raise ValueError("Unexpected GREET combined-CO2 exclusion reason")
    formula_audit = report.get("source_formula_audit", [])
    if {r.get("worksheet") for r in formula_audit} != set(settings.lightweight_glider.worksheets):
        raise ValueError("Missing GREET native glider formula audit")
    if report.get("lightweight_bev_range_mapping_ok") != all(r.get("ok") for r in formula_audit):
        raise ValueError("Inconsistent GREET lightweight glider audit")
    return validation


def normalize_generated_results(bundle: ConfigBundle) -> GreetEvidence:
    """Read the registered complete result bank; no Excel, mutation or network."""
    registration, workbooks = registered_workbooks(bundle)
    bank = result_bank_paths(bundle)
    for path in (*workbooks, *bank.values()):
        if not path.is_file():
            raise FileNotFoundError(f"Missing GREET evidence {path}; run uv run python -m fetching.greet_automation --scenario {bundle.scenario_path}")
    manifest = ResultManifest.model_validate_json(bank["manifest"].read_text(encoding="utf-8"))
    if (manifest.source_id, manifest.component, manifest.profile_sha256) != (SOURCE, COMPONENT, generation_profile(bundle)):
        raise ValueError("Stale GREET result-bank identity/profile; explicitly regenerate")
    hashes = {p.relative_to(bundle.repo_root).as_posix(): file_sha256(p) for p in workbooks}
    if manifest.source_hashes != hashes or manifest.files != {p.name: file_sha256(p) for k, p in bank.items() if k != "manifest"}:
        raise ValueError("Stale or altered GREET result-bank/source hashes; explicitly regenerate")
    raw = pd.read_csv(bank["results"], float_precision="round_trip")
    exclusions = pd.read_csv(bank["exclusions"], float_precision="round_trip")
    cases = pd.read_csv(bank["cases"], float_precision="round_trip")
    report = json.loads(bank["report"].read_text(encoding="utf-8"))
    validate_generation_evidence(bundle, raw, exclusions, cases, report)
    from fetching.greet_automation import inspect_lightweight_glider_formulas

    # Supplement the retained generation report with read-only physical evidence.
    # Older valid banks remain usable; their native defect audit is preserved.
    glider_audit = inspect_lightweight_glider_formulas(workbooks[1], registration.automation.lightweight_glider)
    retained = {a["worksheet"]: a for a in report["source_formula_audit"]}
    for audit in glider_audit:
        if any(retained[audit["worksheet"]].get(key) != audit[key]
               for key in ("original_formula", "range_checks", "ok", "correction_applied")) or any(
                   retained[audit["worksheet"]][key] != audit[key]
                   for key in ("glider_input_values", "glider_inputs_equal") if key in retained[audit["worksheet"]]):
            raise ValueError("GREET retained glider formula audit disagrees with the registered workbook")
    report["lightweight_glider_input_audit"] = glider_audit
    normalized = raw.copy()
    normalized["range_miles"] = 0
    for powertrain in ("phev", "bev"):
        mask = normalized.family.eq("ldv") & normalized.powertrain.eq(powertrain)
        normalized.loc[mask, "range_miles"] = normalized.loc[mask, powertrain + "_range_miles"]
    keys = ["family", "vehicle_class", "powertrain", "materials", "gas_label", "range_miles"]
    evidence_cases = normalized.groupby(keys, sort=True).case_id.agg(lambda c: json.dumps(sorted(c.tolist())))
    normalized = normalized.sort_values("case_id").drop_duplicates(keys).set_index(keys)
    normalized["evidence_case_ids"] = evidence_cases
    normalized = normalized.reset_index().sort_values(keys).reset_index(drop=True)
    rules = load_harmonization_rules(bundle, "greet_vehicle_cycle")
    interim = resolve_artifact_path(bundle, "greet_vehicle_cycle_interim")
    for key, frame in (("normalized", normalized), ("excluded", exclusions), ("models", cases)):
        write_dataframe_atomic(frame, interim / rules["files"][key])
    LOGGER.info("Normalized %s GREET range-specific gas rows; retained %s cases and %s exclusions", len(normalized), len(cases), len(exclusions))
    return GreetEvidence(normalized, exclusions, cases, (*workbooks, *bank.values()), raw, report)
