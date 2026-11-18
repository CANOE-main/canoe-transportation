"""Extract the pinned February 2024 all-segment Dunsky report.

The published HTML tables are the source. Row/column spans are expanded without
repairing inconsistent values. Configured equivalent aggregate inputs are extracted
for production; reviewed overrides remain in the manual charger table.
"""

from __future__ import annotations

import argparse
import hashlib
from html.parser import HTMLParser
import json
import logging
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Literal

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils import (ConfigBundle, load_config_bundle, load_yaml, resolve_artifact_path,
                   resolve_input_path, resolve_parameter_path, write_dataframe_atomic,
                   write_text_atomic)

LOGGER = logging.getLogger(__name__)
SOURCE = "dunsky_ev_charging_infrastructure"


class ReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    url: str = Field(pattern=r"^https://")
    cache_path: str
    expected_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_bytes: int = Field(gt=0)
    publication_date: str = Field(pattern=r"^2024-02$")

    @model_validator(mode="after")
    def bounded_cache(self):
        if Path(self.cache_path).is_absolute() or ".." in Path(self.cache_path).parts:
            raise ValueError("Dunsky cache path must be relative and bounded")
        return self


class ReportTables(HTMLParser):
    """Capture the report's ordinary HTML cells, retaining source coordinates."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables = {}
        self.table = None
        self.row = None
        self.cell = None
        self.caption = False
        self.in_header = False
        self.text_chunks = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "table":
            self.table = {"id": attrs.get("id"), "caption": "", "rows": []}
        elif self.table is not None:
            if tag == "caption":
                self.caption = True
            elif tag == "thead":
                self.in_header = True
            elif tag == "tr":
                self.row = {"header": self.in_header, "cells": []}
            elif tag in {"td", "th"} and self.row is not None:
                self.cell = {"text": "", "rowspan": int(attrs.get("rowspan", 1)),
                             "colspan": int(attrs.get("colspan", 1))}
            elif tag == "br" and self.cell is not None:
                self.cell["text"] += " "

    def handle_data(self, data):
        self.text_chunks.append(data)
        if self.table is not None and self.caption:
            self.table["caption"] += data
        if self.cell is not None:
            self.cell["text"] += data

    def handle_endtag(self, tag):
        if self.table is None:
            return
        if tag in {"td", "th"} and self.cell is not None:
            self.cell["text"] = " ".join(self.cell["text"].split())
            self.row["cells"].append(self.cell)
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.table["rows"].append(self.row)
            self.row = None
        elif tag == "thead":
            self.in_header = False
        elif tag == "caption":
            self.caption = False
        elif tag == "table":
            key = self.table["id"]
            if key:
                if key in self.tables:
                    raise ValueError(f"Duplicate report table ID: {key}")
                self.table["caption"] = " ".join(self.table["caption"].split())
                self.tables[key] = self.table
            self.table = None


def expand_cells(rows: list[dict]) -> list[list[str]]:
    grid: dict[tuple[int, int], str] = {}
    for i, row in enumerate(rows):
        j = 0
        for cell in row["cells"]:
            while (i, j) in grid:
                j += 1
            for dy in range(cell["rowspan"]):
                for dx in range(cell["colspan"]):
                    if (i + dy, j + dx) in grid:
                        raise ValueError("Overlapping report table spans")
                    grid[i + dy, j + dx] = cell["text"]
            j += cell["colspan"]
    width = max(j for _, j in grid) + 1
    if max(i for i, _ in grid) + 1 != len(rows):
        raise ValueError("Report span extends beyond table rows")
    if any((i, j) not in grid for i in range(len(rows)) for j in range(width)):
        raise ValueError("Ragged report table after span expansion")
    return [[grid[i, j] for j in range(width)] for i in range(len(rows))]


def normalize_report(content: bytes, settings: dict) -> dict:
    text = content.decode("utf-8")
    for token in settings["required_edition_tokens"]:
        if token not in text:
            raise ValueError(f"Dunsky report edition marker missing: {token}")
    parser = ReportTables()
    parser.feed(text)
    tables = {}
    for number, spec in settings["tables"].items():
        native = parser.tables[spec["html_id"]]
        if not re.fullmatch(spec["caption_pattern"], native["caption"]):
            raise ValueError(f"Dunsky table {number} caption changed")
        matrix = expand_cells(native["rows"])
        if len(matrix) != spec["rows"] or len(matrix[0]) != spec["columns"]:
            raise ValueError(f"Dunsky table {number} shape changed")
        tables[str(number)] = {**native, "matrix": matrix,
                               "header_rows": spec["header_rows"],
                               "evidence_kind": spec["evidence_kind"]}
    document = " ".join(" ".join(parser.text_chunks).split())
    context = {}
    for key, pattern in settings.get("context_patterns", {}).items():
        match = re.search(pattern, document)
        if not match:
            raise ValueError(f"Dunsky source context missing: {key}")
        context[key] = {"text": match[0], "values": match.groupdict()}
    return {"publication_date": "2024-02", "tables": tables, "context": context,
            "source_sha256": hashlib.sha256(content).hexdigest()}


def settings_for(bundle: ConfigBundle) -> dict:
    return load_yaml(resolve_parameter_path(bundle, "dunsky_charger_inputs.yaml"))


class ProductionTrajectory(BaseModel):
    """Bound report selectors and reviewed period semantics before extraction."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sub_category: Literal["ldv", "mhdv"]
    parameter: str = Field(min_length=1)
    table: int = Field(gt=0)
    labels: list[str] = Field(min_length=1)
    value_kind: Literal["annual_cell", "row_power", "cell_power"]
    periods: dict[str, list[int]] = Field(min_length=1)
    unit: Literal["kW", "CAD"]
    currency_year: str
    notes: str = Field(min_length=1)


class PreIntroductionDash(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    charger: str = Field(min_length=1)
    year: int = Field(gt=0)


class ProductionComposition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    table: int = Field(gt=0)
    scenario: str = Field(min_length=1)
    locations: list[str] = Field(min_length=1)
    population_chargers: list[str] = Field(min_length=1)
    years: list[int] = Field(min_length=1)
    round_digits: int = Field(ge=0, le=6)
    period: str = Field(min_length=1)
    parameters: dict[str, str] = Field(min_length=1)
    notes: dict[str, str] = Field(min_length=1)
    zero_dash_cells: list[PreIntroductionDash]

    @model_validator(mode="after")
    def complete_unique_selection(self):
        for values in (self.locations, self.population_chargers, self.years):
            if len(values) != len(set(values)):
                raise ValueError("Dunsky production composition selectors must be unique")
        if (set(self.parameters) - set(self.population_chargers)
                or len(set(self.parameters.values())) != len(self.parameters)
                or set(self.notes) != set(self.parameters.values())
                or any(not text.strip() for text in self.notes.values())):
            raise ValueError("Dunsky production composition parameters and notes differ")
        return self


def _source_number(value: str, *, dash_zero: bool = False) -> float:
    if value == "-" and dash_zero:
        return 0.0
    match = re.fullmatch(r"\$?([\d,]+(?:\.\d+)?)(?: \(\d+ kW average\))?", value)
    if not match:
        raise ValueError(f"Invalid production Dunsky numeric cell: {value!r}")
    result = float(match[1].replace(",", ""))
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"Invalid production Dunsky value: {value!r}")
    return result


def extract_production_inputs(evidence: dict, settings: dict, citation: str) -> pd.DataFrame:
    """Extract exact source equivalents, retaining native cells and derivations.

    Rounded MHDV shares use component counts; inconsistent printed subtotals never
    enter the denominator. Annual shares are averaged before the reviewed rounding.
    """
    rows = []

    def append(category, parameter, period, value, unit, currency_year, notes, cells, table):
        rows.append({"category": "charger", "sub_category": category,
                     "parameter": parameter, "unit": unit, "currency_year": currency_year,
                     "period": period, "value": str(value), "notes": notes,
                     "source -> data_source": citation, "source_table": table,
                     "source_cells": json.dumps(cells, ensure_ascii=False),
                     "source_sha256": evidence["source_sha256"]})

    for payload in settings["trajectories"]:
        spec = ProductionTrajectory.model_validate(payload)
        native = evidence["tables"][str(spec.table)]
        matrix = native["matrix"]
        matches = [(index, row) for index, row in enumerate(matrix)
                   if index >= native["header_rows"] and row[:len(spec.labels)] == spec.labels]
        if len(matches) != 1:
            raise ValueError(f"Dunsky production row changed: {spec.parameter}")
        index, row = matches[0]
        years = matrix[native["header_rows"] - 1]
        for period, selected_years in spec.periods.items():
            if not selected_years or len(selected_years) != len(set(selected_years)):
                raise ValueError(f"Invalid Dunsky production years: {spec.parameter}/{period}")
            columns = ([1] if spec.value_kind == "row_power" else
                       [years.index(str(year)) for year in selected_years])
            cells = [{"row": index + 1, "column": col + 1, "raw_value": row[col]} for col in columns]
            if spec.value_kind == "cell_power":
                powers = [re.fullmatch(r"\$[\d,]+ \((\d+) kW average\)", cell["raw_value"])
                          for cell in cells]
                if any(match is None for match in powers):
                    raise ValueError(f"Dunsky cost-matched rated power missing: {spec.parameter}/{period}")
                values = [float(match[1]) for match in powers]
            else:
                values = [_source_number(cell["raw_value"]) for cell in cells]
            if len(set(values)) != 1 or values[0] <= 0:
                raise ValueError(f"Dunsky production period no longer constant: {spec.parameter}/{period}")
            append(spec.sub_category, spec.parameter, period, values[0], spec.unit,
                   spec.currency_year, spec.notes, cells, spec.table)

    for payload in settings["compositions"]:
        spec = ProductionComposition.model_validate(payload).model_dump()
        native = evidence["tables"][str(spec["table"])]
        matrix = native["matrix"]
        selected = [(index, row) for index, row in enumerate(matrix)
                    if index >= native["header_rows"] and row[0] == spec["scenario"]
                    and row[1] in spec["locations"] and row[2] in spec["population_chargers"]]
        keys = [(row[1], row[2]) for _, row in selected]
        expected = {(location, kind) for location in spec["locations"] for kind in spec["population_chargers"]}
        if set(keys) != expected or len(keys) != len(expected):
            raise ValueError("Dunsky production composition coverage changed")
        yearly = {kind: [] for kind in spec["population_chargers"]}
        cells = []
        for year in spec["years"]:
            column = matrix[native["header_rows"] - 1].index(str(year))
            counts = {kind: 0.0 for kind in yearly}
            for index, row in selected:
                zero = {"charger": row[2], "year": year} in spec["zero_dash_cells"]
                counts[row[2]] += _source_number(row[column], dash_zero=zero)
                cells.append({"row": index + 1, "column": column + 1, "raw_value": row[column]})
            total = sum(counts.values())
            if total <= 0:
                raise ValueError("Dunsky production composition has no ports")
            for kind, count in counts.items():
                yearly[kind].append(count / total)
        for kind, parameter in spec["parameters"].items():
            values = yearly[kind]
            value = round(sum(values) / len(values), spec["round_digits"])
            append("mhdv", parameter, spec["period"], value, "", "", spec["notes"][parameter], cells, spec["table"])
    result = pd.DataFrame(rows)
    if len(result) != settings["expected_rows"] or result.duplicated(["category", "sub_category", "parameter", "period"]).any():
        raise ValueError("Dunsky production input coverage or keys differ from registration")
    return result


def infrastructure_frames(evidence: dict) -> dict[str, pd.DataFrame]:
    """Normalize source-native charging trajectories with reviewable cell coordinates."""
    def number(value: str, *, dash_zero: bool = False) -> float:
        if value.endswith("%"):
            return _source_number(value[:-1]) / 100
        return _source_number(value, dash_zero=dash_zero)

    def annual(table: int, dimensions: list[str], unit: str) -> pd.DataFrame:
        native = evidence["tables"][str(table)]
        matrix = native["matrix"]
        years = [int(value) for value in matrix[native["header_rows"] - 1][len(dimensions):]]
        records = []
        for row_index, row in enumerate(matrix[native["header_rows"]:], native["header_rows"] + 1):
            labels = dict(zip(dimensions, row[:len(dimensions)], strict=True))
            for column, year in enumerate(years, len(dimensions)):
                zero = table == 38 and labels.get("charger") == "UFC" and year == 2025
                records.append({**labels, "year": year,
                                "value": number(row[column], dash_zero=zero),
                                "raw_value": row[column], "unit": unit, "table": table,
                                "source_row": row_index, "source_column": column + 1,
                                "evidence_kind": native["evidence_kind"],
                                "source_url": evidence["url"] + "#" + native["id"]})
        frame = pd.DataFrame(records)
        if frame.duplicated([*dimensions, "year"]).any():
            raise ValueError(f"Duplicate Dunsky source trajectory cells: Table {table}")
        return frame

    ldv = annual(6, ["scenario", "charger"], "ports").assign(vehicle_class="LDV", geography="Canada")
    mhdv = annual(38, ["scenario", "location", "charger"], "ports").assign(vehicle_class="MHDV", geography="Canada")
    ldv_cost = annual(12, ["charger"], "CAD2022/port").assign(vehicle_class="LDV")
    ldv_cost["rated_power_kw"] = ldv_cost.raw_value.str.extract(r"\((\d+) kW average\)", expand=False).astype(float)
    mhdv_cost = annual(21, ["charger", "rated_power_kw"], "CAD2022/port").assign(vehicle_class="MHDV")
    mhdv_cost["rated_power_kw"] = mhdv_cost.rated_power_kw.map(number)
    matrix = evidence["tables"]["19"]["matrix"]
    operating = []
    for column in range(1, 7):
        power, hours, throughput = (number(matrix[row][column]) for row in (2, 3, 4))
        operating.append({"location": matrix[0][column], "charger": matrix[1][column],
                          "power_kw": power, "session_hours": hours, "vehicles_per_day": throughput,
                          "pre_2027_share": number(matrix[5][column]),
                          "post_2027_share": number(matrix[6][column]),
                          "table": 19, "source_column": column + 1,
                          "source_rows": "3,4,5,6,7", "source_url": evidence["url"] + "#" + evidence["tables"]["19"]["id"]})
    return {"ldv": ldv, "mhdv": mhdv,
            "power": annual(30, ["charger"], "kW/port"),
            "utilization": annual(31, ["charger"], "fraction"),
            "costs": pd.concat([ldv_cost, mhdv_cost], ignore_index=True),
            "operating": pd.DataFrame(operating)}


def production_inputs(bundle: ConfigBundle, *, evidence: dict | None = None) -> tuple[pd.DataFrame, Path]:
    if evidence is None:
        evidence, path = load_evidence(bundle)
    else:
        request = ReportRequest.model_validate(bundle.sources.sources[SOURCE].adapter["diagnostic_report"])
        path = resolve_input_path(bundle, "cache", request.cache_path)
    return extract_production_inputs(evidence, settings_for(bundle)["production"],
                                     bundle.sources.sources[SOURCE].citation), path


def load_evidence(bundle: ConfigBundle, *, download: bool = False, session=None) -> tuple[dict, Path]:
    request = ReportRequest.model_validate(bundle.sources.sources[SOURCE].adapter["diagnostic_report"])
    path = resolve_input_path(bundle, "cache", request.cache_path)
    if not path.exists():
        if not download:
            raise FileNotFoundError(f"Pinned Dunsky report missing: {path}; run uv run python -m fetching.dunsky_charger_inputs --download")
        response = (session or requests).get(request.url, timeout=90)
        response.raise_for_status()
        content = response.content
        if len(content) != request.expected_bytes or hashlib.sha256(content).hexdigest() != request.expected_sha256:
            raise ValueError("Downloaded Dunsky report differs from registered bytes; review a new edition explicitly")
        normalized = normalize_report(content, settings_for(bundle))
        extract_production_inputs(normalized, settings_for(bundle)["production"],
                                  bundle.sources.sources[SOURCE].citation)
        infrastructure_frames({**normalized, "url": request.url})
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
        try:
            temporary.write_bytes(content)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        LOGGER.info("Acquired registered Dunsky report: %s (%d bytes)", path, len(content))
    content = path.read_bytes()
    if len(content) != request.expected_bytes or hashlib.sha256(content).hexdigest() != request.expected_sha256:
        raise ValueError("Cached Dunsky report differs from registered identity")
    evidence = normalize_report(content, settings_for(bundle))
    evidence.update(source_key=SOURCE, component_key="published_report", url=request.url,
                    source_id=bundle.sources.sources[SOURCE].source_id,
                    data_quality=bundle.sources.resolved_data_quality(SOURCE, "published_report").row_fields())
    return evidence, path


def fetch_and_normalize(bundle: ConfigBundle | str | Path, *, download: bool = False) -> dict:
    if not isinstance(bundle, ConfigBundle):
        bundle = load_config_bundle(bundle)
    evidence, path = load_evidence(bundle, download=download)
    directory = resolve_artifact_path(bundle, "dunsky_interim")
    directory.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", directory / "published_tables.json", newline="\n")
    production, _ = production_inputs(bundle, evidence=evidence)
    write_dataframe_atomic(production, directory / "aggregate_production_inputs.csv")
    for name, frame in infrastructure_frames(evidence).items():
        write_dataframe_atomic(frame, directory / settings_for(bundle)["infrastructure_output_files"][name])
    LOGGER.info("Extracted %d pinned Dunsky tables from %s", len(evidence["tables"]), path)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="config/scenarios/default.yaml")
    parser.add_argument("--download", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    fetch_and_normalize(load_config_bundle(args.scenario), download=args.download)


if __name__ == "__main__":
    main()
