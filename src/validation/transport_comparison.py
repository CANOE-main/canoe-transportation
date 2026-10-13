"""Refreshable, read-only evidence for the transport legacy diagnostic notebook.

The SQLite parameters are the numeric authority.  Coverage, changed representations,
and incompatible units remain separate from numeric rankings.  The older family
comparators remain available alongside this broader row-level diagnostic.
"""

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
import json
from math import isclose, isfinite
from pathlib import Path
import re
import sqlite3
from typing import Any, Self

import pandas as pd
from pydantic import Field, model_validator

from utils import (
    ConfigBundle, file_sha256, find_repo_root, load_config_bundle, load_yaml,
    resolve_artifact_path, resolve_configured_path, resolve_repo_path,
    write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import MappingModel, ScenarioPeriods
from validation.legacy_compare import (
    compare_legacy_costs, compare_legacy_demand, compare_legacy_efficiency,
    compare_legacy_existing_capacity, compare_legacy_lifetime_tech,
)
from validation.sqlite_utils import open_sqlite_readonly, quote_identifier


class DifferenceDriver(MappingModel):
    scope: str = Field(min_length=1)
    legacy_basis: str = Field(min_length=1)
    refactored_basis: str = Field(min_length=1)
    explanation: str = Field(min_length=1)
    assumption_sections: list[str]
    evidence_paths: list[str]


class Explanation(MappingModel):
    summary: str = Field(min_length=1)
    assumption_sections: list[str]
    evidence_paths: list[str]
    drivers: list[DifferenceDriver] = Field(default_factory=list)


class ComparisonConfig(MappingModel):
    scenario: str = Field(min_length=1)
    reference_sqlite: str = Field(min_length=1)
    assumptions_path: str = Field(min_length=1)
    existing_range: tuple[int, int]
    future_range: tuple[int, int]
    top_n: int = Field(gt=0)
    explanations: dict[str, Explanation]

    @model_validator(mode="after")
    def ordered_ranges(self) -> Self:
        if not (self.existing_range[0] <= self.existing_range[1]
                < self.future_range[0] <= self.future_range[1]):
            raise ValueError("Existing and future diagnostic ranges must be ordered and disjoint")
        return self


@dataclass(frozen=True)
class Diagnostic:
    config: ComparisonConfig
    bundle: ConfigBundle
    summary: pd.DataFrame
    rows: pd.DataFrame
    metadata: dict[str, Any]
    development_comparisons: dict[str, Any]


@dataclass(frozen=True)
class TableSpec:
    table: str
    legacy_table: str
    keys: tuple[str, ...]
    value: str
    legacy_value: str = ""
    units: str = ""


# Version compatibility is an implementation contract, not a modeling selection.
TABLES = (
    TableSpec("existing_capacity", "ExistingCapacity", ("region", "tech", "vintage"), "capacity"),
    TableSpec("demand", "Demand", ("region", "commodity", "period"), "demand"),
    TableSpec("capacity_to_activity", "CapacityToActivity", ("region", "tech"), "c2a"),
    TableSpec("lifetime_tech", "LifetimeTech", ("region", "tech"), "lifetime", units="years"),
    TableSpec("efficiency", "Efficiency", ("region", "tech", "vintage", "input_comm", "output_comm"), "efficiency"),
    TableSpec("cost_invest", "CostInvest", ("region", "tech", "vintage"), "cost"),
    TableSpec("cost_fixed", "CostFixed", ("region", "tech", "vintage", "period"), "cost"),
    TableSpec("cost_variable", "CostVariable", ("region", "tech", "vintage", "period"), "cost"),
    TableSpec("limit_tech_input_split", "TechInputSplit", ("region", "tech", "period", "input_comm"), "proportion", "min_proportion", "fraction"),
    TableSpec("capacity_factor_tech", "CapacityFactorTech", ("region", "tech", "season", "tod"), "factor", units="fraction"),
    TableSpec("lifetime_survival_curve", "LifetimeSurvivalCurve", ("region", "tech", "vintage", "period"), "fraction", units="fraction"),
)
ANNUAL = TableSpec(
    "limit_annual_capacity_factor", "MaxAnnualCapacityFactor",
    ("region", "tech_or_group", "vintage", "output_comm", "operator"), "factor", units="fraction",
)
EXCLUDED = {
    "emission_embodied": "Absent from the legacy baseline; excluded by review scope",
    "growth_constraints": "Absent from the legacy baseline; excluded by review scope",
    "limit_new_capacity_share": "PEV market-share constraints excluded by review scope",
}
CHARGER_RATES = {
    "T_LDV_CHRG_EX": ("T_LDV_BEV_CHRG", "T_LDV_PHEV_CHRG"),
    "T_LDV_CHRG_N": ("T_LDV_BEV_CHRG", "T_LDV_PHEV_CHRG"),
    "T_MHDV_CHRG_EX": ("T_MDV_CHRG", "T_HDV_CHRG"),
    "T_MHDV_CHRG_N": ("T_MDV_CHRG", "T_HDV_CHRG"),
}
ROW_COLUMNS = (
    "table", "band", "status", "units", "candidate", "legacy", "delta",
    "absolute_difference", "relative_percent", "identifier", "keys", "notes",
    "legacy_notes", "legacy_reference", "data_source", "comparison_grain",
    "comparison_period", "legacy_lower", "legacy_upper",
)


def _read(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)", (table,),
    ).fetchone()
    if not exists:
        return []
    cursor = connection.execute(f"SELECT * FROM {quote_identifier(table)}")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor]


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


def _units(value: Any) -> str:
    text = str(value or "").strip()
    text = text.replace("bpkm", "bn passenger-km").replace("btkm", "bn tonne-km")
    text = text.replace("k units", "k vehicles")
    match = re.fullmatch(r"(\d{4}) ([A-Z]{3}) \(M\$/(.+?)\)", text)
    if match:
        year, currency, denominator = match.groups()
        # Legacy service capacity is annual service; both databases use that basis.
        denominator = denominator.removesuffix("/year")
        return f"$M {year}{currency} / {denominator}"
    return text


def _key(row: dict[str, Any], spec: TableSpec) -> tuple[Any, ...]:
    return tuple(row.get(key) for key in spec.keys)


def _index(rows: list[dict[str, Any]], spec: TableSpec) -> dict[tuple, list[dict]]:
    indexed: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        indexed[_key(row, spec)].append(row)
    return indexed


def _record(
    spec: TableSpec, current: dict | None, previous: dict | None,
    *, absolute_tolerance: float, relative_tolerance: float,
    status: str | None = None, grain: str = "exact parameter keys",
) -> dict[str, Any]:
    current = current or {}
    previous = previous or {}
    keys = {key: (current if current else previous).get(key) for key in spec.keys}
    candidate = _number(current.get(spec.value))
    legacy = _number(previous.get(spec.legacy_value or spec.value))
    current_unit = _units(current.get("units", spec.units))
    previous_unit = _units(previous.get("units", spec.units))
    inferred_unit = False
    if current and previous and not previous_unit and current_unit and spec.table in {
        "efficiency", "capacity_to_activity",
    }:
        # These legacy tables do not declare units; exact technology/edge keys
        # retain the service-per-PJ or activity-per-capacity parameter meaning.
        previous_unit = current_unit
        inferred_unit = True
    result = {
        "table": spec.table, **keys, "keys": json.dumps(keys, sort_keys=True),
        "identifier": "; ".join(f"{key}={value}" for key, value in keys.items()),
        "candidate": candidate, "legacy": legacy,
        "candidate_units": current_unit, "legacy_units": previous_unit,
        "units": current_unit or previous_unit or spec.units,
        "unit_basis": "legacy implicit parameter units" if inferred_unit else "declared units",
        "notes": current.get("notes", ""),
        "legacy_notes": previous.get("notes", previous.get("cf_tech_notes", "")),
        "legacy_reference": previous.get("reference", ""),
        "data_source": current.get("data_source", ""),
        "data_id": current.get("data_id", ""),
        "comparison_grain": grain, "delta": None,
        "absolute_difference": None, "relative_percent": None,
        "comparison_period": None, "legacy_lower": None, "legacy_upper": None,
    }
    if status is not None:
        result["status"] = status
    elif not current:
        result["status"] = "legacy_only"
    elif not previous:
        result["status"] = "candidate_only"
    elif candidate is None or legacy is None:
        result["status"] = "invalid_value"
    elif current_unit != previous_unit:
        result["status"] = "unit_mismatch"
    elif spec.table == "limit_tech_input_split" and current.get("operator") not in {"ge", "e"}:
        result["status"] = "representation_only"
    else:
        equal = isclose(candidate, legacy, abs_tol=absolute_tolerance, rel_tol=relative_tolerance)
        result.update(
            status="equal" if equal else "different", delta=candidate - legacy,
            absolute_difference=abs(candidate - legacy),
            relative_percent=100 * (candidate - legacy) / abs(legacy) if legacy else None,
        )
        if spec.table == "limit_tech_input_split":
            result["comparison_grain"] = "input energy-share value; legacy minimum bound"
            result["candidate_operator"] = current.get("operator")
    return result


def _charger_match(
    row: dict, reference: list[dict], spec: TableSpec, *, historical_end: int,
    absolute_tolerance: float, relative_tolerance: float,
) -> tuple[dict | None, list[dict], str, str | None]:
    """Bridge rates only after equal-rate proof; never sum or rebin capacity."""
    tech = row.get("tech")
    if spec.table == "capacity_factor_tech" and tech in {"T_LDV_CHRG_EX", "T_LDV_CHRG_N"}:
        matches = [old for old in reference if old.get("tech") == "T_LDV_BEV_CHRG"
                   and all(old.get(key) == row.get(key) for key in ("region", "season", "tod"))]
        grain = "shared LDV hourly shape bridge from legacy BEV charging profile"
    elif spec.table in {"cost_fixed", "cost_invest"} and tech in CHARGER_RATES:
        matches = [old for old in reference if old.get("tech") in CHARGER_RATES[tech]
                   and old.get("region") == row.get("region")
                   and (spec.table != "cost_fixed" or old.get("period") == row.get("period"))
                   and (old.get("vintage") == row.get("vintage") if tech.endswith("_N")
                        else isinstance(old.get("vintage"), int) and old["vintage"] <= historical_end)]
        grain = "charger class rate bridge; legacy constituent rates must agree"
    else:
        return None, [], "exact parameter keys", None
    if not matches:
        return None, [], grain, None
    if any(count > 1 for count in Counter(_key(old, spec) for old in matches).values()):
        return None, matches, grain, "ambiguous_key"
    values = [_number(old.get(spec.legacy_value or spec.value)) for old in matches]
    units = {_units(old.get("units", spec.units)) for old in matches}
    if (any(value is None for value in values) or len(units) != 1
            or any(not isclose(values[0], value, abs_tol=absolute_tolerance,
                               rel_tol=relative_tolerance) for value in values[1:])):
        return None, matches, grain, "representation_only"
    representative = dict(matches[0])
    representative["notes"] = " | ".join(sorted({str(old.get("notes", "")) for old in matches}))
    return representative, matches, grain, None


def _ordinary_rows(
    spec: TableSpec, current: list[dict], previous: list[dict], *,
    historical_end: int, curve_technologies: set[str], tolerances: dict[str, float],
) -> list[dict]:
    now, old = _index(current, spec), _index(previous, spec)
    bridge_fields = ("region", "season", "tod") if spec.table == "capacity_factor_tech" else (
        ("region", "period") if spec.table == "cost_fixed" else ("region",)
    )
    bridge_pools: dict[tuple, list[dict]] = defaultdict(list)
    for row in previous:
        bridge_pools[tuple(row.get(field) for field in bridge_fields)].append(row)
    consumed: set[tuple] = set()
    result = []
    for key in sorted(now, key=repr):
        candidates, references = now[key], old.get(key, [])
        status = "ambiguous_key" if len(candidates) > 1 or len(references) > 1 else None
        candidate = candidates[0]
        reference = references[0] if references else None
        grain = "exact parameter keys"
        bridge_rows: list[dict] = []
        if reference is not None:
            consumed.add(key)
        elif not status:
            reference, bridge_rows, grain, status = _charger_match(
                candidate, bridge_pools[tuple(candidate.get(field) for field in bridge_fields)],
                spec, historical_end=historical_end, **tolerances,
            )
            consumed.update(_key(old_row, spec) for old_row in bridge_rows)
        if spec.table == "lifetime_survival_curve":
            status = "representation_only"
            grain = "accepted survival curves replace legacy fixed lifetimes"
        record = _record(spec, candidate, reference, status=status, grain=grain, **tolerances)
        if bridge_rows:
            record["legacy_keys"] = json.dumps([dict(zip(spec.keys, _key(old_row, spec), strict=True))
                                                for old_row in bridge_rows], sort_keys=True)
            record["legacy_constituent_rows"] = len(bridge_rows)
        result.append(record)
    for key in sorted(old.keys() - consumed, key=repr):
        references = old[key]
        status = "ambiguous_key" if len(references) > 1 else None
        if spec.table == "lifetime_tech" and references[0].get("tech") in curve_technologies:
            status = "representation_only"
        result.append(_record(spec, None, references[0], status=status, **tolerances))
    return result


def _annual_rows(
    current: list[dict], minima: list[dict], maxima: list[dict], *,
    first_period: int, tolerances: dict[str, float],
) -> list[dict]:
    """Compare current cohort values with legacy operating-period bounds.

    Historical cohorts use their first modeled operating period. Future cohorts
    use their installation period. This is a stated projection, not a key alias.
    """
    old_keys = ("region", "tech", "period", "output_comm")
    bounds: dict[tuple, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for side, rows in (("lower", minima), ("upper", maxima)):
        for row in rows:
            bounds[tuple(row.get(key) for key in old_keys)][side].append(row)
    consumed = set()
    result = []
    for key, candidates in sorted(_index(current, ANNUAL).items(), key=lambda item: repr(item[0])):
        candidate = candidates[0]
        period = max(first_period, int(candidate["vintage"]))
        old_key = (candidate["region"], candidate["tech_or_group"], period, candidate["output_comm"])
        legacy = bounds.get(old_key, {})
        consumed.add(old_key)
        lower_rows, upper_rows = legacy.get("lower", []), legacy.get("upper", [])
        record = _record(ANNUAL, candidate, None, **tolerances)
        record.update(comparison_period=period, comparison_grain=(
            "cohort value at first modeled operation" if candidate["vintage"] < first_period
            else "new-cohort value at installation period"
        ))
        if len(candidates) > 1 or len(lower_rows) > 1 or len(upper_rows) > 1:
            record["status"] = "ambiguous_key"
        elif not lower_rows and not upper_rows:
            record["status"] = "candidate_only"
        else:
            lower = _number(lower_rows[0].get("factor")) if lower_rows else None
            upper = _number(upper_rows[0].get("factor")) if upper_rows else None
            value = record["candidate"]
            record.update(legacy_lower=lower, legacy_upper=upper)
            constituents = [*lower_rows, *upper_rows]
            record["legacy_notes"] = " | ".join(sorted({str(row.get("notes", "")) for row in constituents}))
            record["legacy_reference"] = " | ".join(sorted({str(row.get("reference", "")) for row in constituents}))
            operator = candidate.get("operator")
            if value is None or (lower is not None and upper is not None and lower > upper):
                record["status"] = "invalid_value"
            elif operator == "e" and lower is not None and upper is not None:
                nearest = lower if value < lower else upper if value > upper else value
                delta = value - nearest
                record.update(
                    legacy=nearest, delta=delta, absolute_difference=abs(delta),
                    relative_percent=100 * delta / abs(nearest) if nearest else None,
                    status="equal" if isclose(value, nearest, **_math_tolerances(tolerances)) else "different",
                )
                record["comparison_grain"] += "; distance outside legacy [minimum, maximum]"
            elif operator in {"le", "ge"} and (upper if operator == "le" else lower) is not None:
                bound = upper if operator == "le" else lower
                delta = value - bound
                record.update(
                    legacy=bound, delta=delta, absolute_difference=abs(delta),
                    relative_percent=100 * delta / abs(bound) if bound else None,
                    status="equal" if isclose(value, bound, **_math_tolerances(tolerances)) else "different",
                )
            else:
                record["status"] = "representation_only"
        result.append(record)
    for old_key in sorted(bounds.keys() - consumed, key=repr):
        for side, entries in bounds[old_key].items():
            old = entries[0]
            normalized = {**old, "tech_or_group": old["tech"], "operator": "ge" if side == "lower" else "le"}
            # Keep the legacy operating period visible; do not invent its vintage.
            record = _record(ANNUAL, None, normalized, **tolerances)
            record.update(period=old["period"], comparison_period=old["period"],
                          comparison_grain="unpaired legacy operating-period bound")
            keys = {**json.loads(record["keys"]), "period": old["period"]}
            record.update(keys=json.dumps(keys, sort_keys=True),
                          identifier="; ".join(f"{key}={value}" for key, value in keys.items()))
            result.append(record)
    return result


def _math_tolerances(tolerances: dict[str, float]) -> dict[str, float]:
    return {"abs_tol": tolerances["absolute_tolerance"], "rel_tol": tolerances["relative_tolerance"]}


def _band_rows(rows: pd.DataFrame, config: ComparisonConfig) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=ROW_COLUMNS)
    rows = rows.copy()
    rows["band"] = "All"
    for table, subset in rows.groupby("table", sort=False):
        axis = "vintage" if "vintage" in subset and subset.vintage.notna().any() else "period"
        if axis not in subset:
            continue
        years = pd.to_numeric(subset[axis], errors="coerce")
        if axis == "vintage" and "period" in subset:
            years = years.fillna(pd.to_numeric(subset.period, errors="coerce"))
        existing = years.between(*config.existing_range)
        future = years.between(*config.future_range)
        # Split only when this parameter table actually spans both requested bands.
        if existing.any() and future.any():
            rows.loc[subset.index[existing], "band"] = f"Existing ({config.existing_range[0]}–{config.existing_range[1]})"
            rows.loc[subset.index[future], "band"] = f"Future ({config.future_range[0]}–{config.future_range[1]})"
        outside = years.notna() & ~(existing | future)
        rows.loc[subset.index[outside], "band"] = "Outside requested ranges"
    return rows


def _review_identities(rows: pd.DataFrame) -> pd.DataFrame:
    """Identify a technology/region, or commodity/region for demand, for review."""
    result = rows.copy()
    result["entity"] = ""
    result["entity_kind"] = ""
    if "region" not in result:
        result["region"] = ""
    result["region"] = result["region"].fillna("").astype(str)
    for field in ("tech", "tech_or_group", "commodity", "identifier"):
        if field in result:
            values = result[field].fillna("").astype(str)
            available = result.entity.eq("") & values.ne("")
            result.loc[available, "entity"] = values[available]
            result.loc[available, "entity_kind"] = field
    return result


def ranked_observations(rows: pd.DataFrame, record: pd.Series) -> pd.DataFrame:
    """Recover all exact-key observations represented by a grouped ranking."""
    identified = _review_identities(rows)
    selected = pd.Series(True, index=identified.index)
    for field in ("table", "band", "units", "region", "entity_kind", "entity"):
        selected &= identified[field].fillna("").eq(record[field])
    return rows.loc[selected].copy()


def _same_values(rows: pd.DataFrame, record: pd.Series) -> pd.Series:
    matched = pd.Series(True, index=rows.index)
    for field in ("candidate", "legacy", "legacy_lower", "legacy_upper"):
        if field in rows:
            value = record[field]
            matched &= rows[field].isna() if pd.isna(value) else rows[field].eq(value)
    return matched


def _labels(rows: pd.DataFrame, field: str) -> str:
    if field not in rows:
        return ""
    values = rows[field].dropna().drop_duplicates().sort_values()
    return ", ".join(str(int(value)) if isinstance(value, (int, float)) and float(value).is_integer()
                     else str(value) for value in values)


def ranked_rows(rows: pd.DataFrame, top_n: int, metric: str = "relative_percent") -> pd.DataFrame:
    """Rank unique technology/region identifiers by their largest actual difference.

    Each row retains one exact-key representative, counts all observations in that
    identifier's table/band/unit group, and counts repetitions of the displayed
    candidate/legacy value pair. Unequal observations are never averaged. A group
    with finite percentages uses its largest percentage; zero-baseline observations
    remain counted and inspectable. Entirely zero-baseline groups rank separately.
    """
    if metric not in {"relative_percent", "absolute_difference"} or top_n <= 0:
        raise ValueError("Choose a supported ranking metric and positive top_n")
    if rows.empty:
        return rows.copy()
    identified = _review_identities(rows.loc[rows.band.ne("Outside requested ranges")])
    records = []
    identity_fields = ["table", "band", "units", "region", "entity_kind", "entity"]
    for _, observations in identified.groupby(identity_fields, dropna=False, sort=True):
        differences = observations.loc[observations.status.eq("different")].copy()
        if differences.empty:
            continue
        if metric == "relative_percent" and differences.relative_percent.notna().any():
            differences = differences.loc[differences.relative_percent.notna()].copy()
        differences["_magnitude"] = pd.to_numeric(differences[metric], errors="coerce").abs()
        differences["_magnitude"] = differences["_magnitude"].fillna(differences.absolute_difference)
        representative = differences.sort_values(
            ["_magnitude", "absolute_difference", "identifier"],
            ascending=[False, False, True], kind="stable",
        ).iloc[0].copy()
        identical = observations.loc[_same_values(observations, representative)]
        representative["representative_identifier"] = representative["identifier"]
        representative["identifier"] = (
            representative["entity"] if representative["entity_kind"] == "identifier"
            else "; ".join(part for part in (
                f"region={representative['region']}" if representative["region"] else "",
                f"{representative['entity_kind']}={representative['entity']}",
            ) if part)
        )
        representative["observation_count"] = len(identical)
        representative["different_observations"] = int(observations.status.eq("different").sum())
        representative["comparable_observations"] = int(observations.status.isin(["different", "equal"]).sum())
        representative["total_observations"] = len(observations)
        representative["zero_baseline_observations"] = int(
            (observations.status.eq("different") & observations.relative_percent.isna()).sum()
        )
        representative["value_vintages"] = _labels(identical, "vintage")
        representative["value_periods"] = _labels(identical, "period")
        records.append(representative)
    if not records:
        return identified.iloc[:0].assign(rank=pd.Series(dtype=int), ranking_basis=pd.Series(dtype=str))
    selected = pd.DataFrame(records)
    groups = ["table", "band"] + (["units"] if metric == "absolute_difference" else [])
    selected["_magnitude"] = pd.to_numeric(selected[metric], errors="coerce").abs()
    selected["ranking_basis"] = metric
    if metric == "relative_percent":
        zero_baseline = selected["_magnitude"].isna()
        selected.loc[zero_baseline, "ranking_basis"] = "legacy zero; absolute difference in " + selected.loc[zero_baseline, "units"]
        selected.loc[zero_baseline, "_magnitude"] = selected.loc[zero_baseline, "absolute_difference"]
        # Keep zero-baseline changes visible as a separate unit-specific ranking;
        # there is no finite percent denominator and no invented infinity.
        groups.append("ranking_basis")
    selected = selected.sort_values(
        [*groups, "_magnitude", "absolute_difference", "identifier"],
        ascending=[True] * len(groups) + [False, False, True], na_position="last", kind="stable",
    )
    selected["rank"] = selected.groupby(groups, dropna=False).cumcount() + 1
    return selected.loc[selected["rank"].le(top_n)].drop(columns="_magnitude").reset_index(drop=True)


def explanation_rows(config: ComparisonConfig, table: str | None = None) -> pd.DataFrame:
    """Flatten curated source/assumption drivers into portable review records."""
    records = []
    for name, explanation in config.explanations.items():
        if table is not None and name != table:
            continue
        for driver in explanation.drivers:
            record = driver.model_dump()
            record["table"] = name
            record["assumption_sections"] = "; ".join(driver.assumption_sections)
            record["evidence_paths"] = "; ".join(driver.evidence_paths)
            records.append(record)
    return pd.DataFrame(records, columns=[
        "table", "scope", "legacy_basis", "refactored_basis", "explanation",
        "assumption_sections", "evidence_paths",
    ])


def _summarize(rows: pd.DataFrame, config: ComparisonConfig) -> pd.DataFrame:
    summaries = []
    for (table, band), group in rows.groupby(["table", "band"], sort=True):
        counts = Counter(group.status)
        comparable = group.loc[group.status.isin(["different", "equal"])]
        identities = _review_identities(group)
        identity_fields = ["region", "entity_kind", "entity", "units"]
        explanation = config.explanations.get(table)
        summaries.append({
            "table": table, "band": band, "compared": len(comparable),
            "equal": counts["equal"], "different": counts["different"],
            "compared_identifiers": len(identities.loc[identities.status.isin(["different", "equal"])].drop_duplicates(identity_fields)),
            "different_identifiers": len(identities.loc[identities.status.eq("different")].drop_duplicates(identity_fields)),
            "candidate_only": counts["candidate_only"], "legacy_only": counts["legacy_only"],
            "noncomparable": sum(count for status, count in counts.items()
                                  if status not in {"equal", "different", "candidate_only", "legacy_only"}),
            "max_relative_percent": pd.to_numeric(comparable.relative_percent, errors="coerce").abs().max(),
            "max_absolute_difference": (comparable.absolute_difference.max()
                                        if comparable.units.nunique() == 1 else None),
            "absolute_difference_units": (comparable.units.iloc[0]
                                          if comparable.units.nunique() == 1 else "multiple units; see unit-specific rankings"),
            "explanation": explanation.summary if explanation else "Inspect row provenance and current assumptions.",
        })
    return pd.DataFrame(summaries)


def compare_parameters(
    candidate_path: Path, reference_path: Path, config: ComparisonConfig, *,
    periods: ScenarioPeriods, absolute_tolerance: float = 1e-9, relative_tolerance: float = 1e-6,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Compare already-built files without preparation, writes, or implicit zeros."""
    if periods.period_mode != "legacy":
        raise ValueError("Transport diagnostics require period_mode: legacy")
    if any(not isfinite(value) or value < 0 for value in (absolute_tolerance, relative_tolerance)):
        raise ValueError("Diagnostic tolerances must be finite and non-negative")
    tolerances = {"absolute_tolerance": absolute_tolerance, "relative_tolerance": relative_tolerance}
    with closing(open_sqlite_readonly(candidate_path)) as candidate, closing(open_sqlite_readonly(reference_path)) as reference:
        current = {spec.table: _read(candidate, spec.table) for spec in (*TABLES, ANNUAL)}
        previous = {spec.table: _read(reference, spec.legacy_table) for spec in (*TABLES, ANNUAL)}
        minima = _read(reference, "MinAnnualCapacityFactor")
        candidate_regions = {row.get("region") for entries in current.values() for row in entries if row.get("region")}
        reference_regions = {row.get("region") for entries in previous.values() for row in entries if row.get("region")}
        shared_regions = candidate_regions & reference_regions
        candidate_commodities = {row.get("commodity") for row in current["demand"]}
        historical = [int(row["period"]) for row in _read(reference, "TimePeriod") if row.get("flag") == "e"]
        historical_end = max(historical) if historical else config.existing_range[1]
        scope = {}

        def scoped(entries: list[dict], *, legacy: bool, table: str,
                   allowed_technologies: set[str] | None = None) -> list[dict]:
            result = []
            for row in entries:
                if row.get("region") not in shared_regions:
                    continue
                tech = row.get("tech", row.get("tech_or_group"))
                if legacy and tech is not None and tech not in (allowed_technologies or set()):
                    continue
                if legacy and table == "demand" and row.get("commodity") not in candidate_commodities:
                    continue
                result.append(row)
            return result

        table_technologies = {}
        for table in current:
            before_current, before_previous = len(current[table]), len(previous[table])
            current[table] = scoped(current[table], legacy=False, table=table)
            allowed = {row.get("tech", row.get("tech_or_group")) for row in current[table]}
            if table == "lifetime_tech":
                allowed.update(row["tech"] for row in current["lifetime_survival_curve"]
                               if row.get("region") in shared_regions)
            elif table in {"cost_fixed", "cost_invest"}:
                allowed.update(old for tech in list(allowed) for old in CHARGER_RATES.get(tech, ()))
            elif table == "capacity_factor_tech" and allowed & {"T_LDV_CHRG_EX", "T_LDV_CHRG_N"}:
                allowed.add("T_LDV_BEV_CHRG")
            table_technologies[table] = allowed
            previous[table] = scoped(previous[table], legacy=True, table=table,
                                     allowed_technologies=allowed)
            scope[table] = {
                "candidate_total": before_current, "candidate_in_scope": len(current[table]),
                "candidate_ignored": before_current - len(current[table]),
                "legacy_total": before_previous, "legacy_in_scope": len(previous[table]),
                "legacy_ignored": before_previous - len(previous[table]),
            }
        total_minima = len(minima)
        minima = scoped(minima, legacy=True, table=ANNUAL.table,
                        allowed_technologies=table_technologies[ANNUAL.table])
        scope[ANNUAL.table].update(
            legacy_upper_total=scope[ANNUAL.table]["legacy_total"],
            legacy_lower_total=total_minima, legacy_lower_in_scope=len(minima),
        )
        scope[ANNUAL.table]["legacy_total"] += total_minima
        scope[ANNUAL.table]["legacy_in_scope"] += len(minima)
        scope[ANNUAL.table]["legacy_ignored"] += total_minima - len(minima)
        curves = {row["tech"] for row in current["lifetime_survival_curve"]}
        records = []
        for spec in TABLES:
            records.extend(_ordinary_rows(
                spec, current[spec.table], previous[spec.table], historical_end=historical_end,
                curve_technologies=curves, tolerances=tolerances,
            ))
        records.extend(_annual_rows(current[ANNUAL.table], minima, previous[ANNUAL.table],
                                    first_period=periods.model[0], tolerances=tolerances))
        inventory = {}
        for label, connection in (("candidate", candidate), ("legacy", reference)):
            inventory[label] = {
                str(name): connection.execute(f"SELECT count(*) FROM {quote_identifier(name)}").fetchone()[0]
                for name, in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            }
    rows = _band_rows(pd.DataFrame(records), config)
    for column in ROW_COLUMNS:
        if column not in rows:
            rows[column] = None
    for column in ("candidate", "legacy", "delta", "absolute_difference", "relative_percent"):
        rows[column] = pd.to_numeric(rows[column], errors="coerce")
    return _summarize(rows, config), rows, {
        "scope": scope, "inventory": inventory, "shared_regions": sorted(shared_regions),
        "candidate_regions_without_legacy": sorted(candidate_regions - reference_regions),
        "legacy_existing_end": historical_end, "candidate_existing_end": periods.existing[-1],
        "excluded": EXCLUDED, "periods": periods.audit(),
        "annual_factor_alignment": "Existing cohorts at first model period; future cohorts at vintage. Equality values rank by distance outside legacy interval.",
    }


def _verify_snapshot(bundle: ConfigBundle, database: Path, report: dict) -> dict[str, Any]:
    """Reject stale prospective or differently configured published artifacts."""
    if bundle.scenario.periods.period_mode != "legacy":
        raise ValueError("Rebuild the diagnostic scenario with period_mode: legacy")
    if not report.get("ok") or report.get("scenario") != bundle.scenario.scenario.name:
        raise ValueError("Diagnostic build report does not match the selected scenario")
    if Path(report.get("database", "")).resolve() != database.resolve():
        raise ValueError("Diagnostic build report names a different database")
    if not report.get("database_sha256") or report["database_sha256"] != file_sha256(database):
        raise ValueError("SQLite fingerprint does not match its build report; rebuild the diagnostic scenario")
    expected_periods = bundle.scenario.periods.model_dump(mode="json")
    if any(report.get("periods", {}).get(key) != value for key, value in expected_periods.items()):
        raise ValueError("Diagnostic database period grid is stale; rebuild the selected scenario")
    configuration = report.get("config", {})
    expected_files = {"scenario": bundle.scenario_path, "paths": bundle.paths_path, "sources": bundle.sources_path}
    checked = []
    for name, path in expected_files.items():
        evidence = configuration.get(name, {})
        if Path(evidence.get("path", "")).resolve() != path.resolve() or evidence.get("sha256") != file_sha256(path):
            raise ValueError(f"Diagnostic database has stale {name} configuration; rebuild the selected scenario")
        checked.append(str(path))
    for family in ("parameters", "manual_inputs"):
        evidence_rows = configuration.get(family)
        if not isinstance(evidence_rows, list) or not evidence_rows:
            raise ValueError(f"Build report lacks {family} fingerprints; rebuild the diagnostic scenario")
        for evidence in evidence_rows:
            path = resolve_repo_path(bundle.repo_root, evidence["path"])
            if not path.is_file() or file_sha256(path) != evidence.get("sha256"):
                raise ValueError(f"Diagnostic database input changed: {path}; rebuild the selected scenario")
            checked.append(str(path))
    with closing(open_sqlite_readonly(database)) as connection:
        times = _read(connection, "time_period")
        existing = sorted(int(row["period"]) for row in times if row.get("flag") == "e")
        if not set(bundle.scenario.periods.existing).issubset(existing):
            raise ValueError("SQLite existing periods do not match the legacy diagnostic grid")
        capacity_vintages = {int(row["vintage"]) for row in _read(connection, "existing_capacity")}
        if not capacity_vintages.issubset(bundle.scenario.periods.existing):
            raise ValueError("SQLite capacity vintages lie outside the diagnostic scenario grid")
        actual_model = sorted({int(row["period"]) for row in _read(connection, "demand")})
        if actual_model != bundle.scenario.periods.model:
            raise ValueError("SQLite demand periods do not match the diagnostic scenario")
        for table, expected_count in report.get("touched_table_row_counts", {}).items():
            count = connection.execute(f"SELECT count(*) FROM {quote_identifier(table)}").fetchone()[0]
            if count != expected_count:
                raise ValueError(f"SQLite {table} count does not match the published build report")
    return {
        "status": "verified", "checked_files": checked,
        "build_timestamp_utc": report.get("timestamp_utc"),
        "unused_template_existing_labels": sorted(set(existing) - set(bundle.scenario.periods.existing)),
    }


def _assumption_excerpts(path: Path, config: ComparisonConfig) -> dict[str, list[dict[str, str]]]:
    text = path.read_text(encoding="utf-8")
    sections = {}
    for section in re.split(r"(?m)^## ", text)[1:]:
        heading, _, body = section.partition("\n")
        sections[heading.strip()] = body.strip()
    def normalize(value):
        return value.replace("`", "").strip().casefold()
    return {
        table: [{"heading": heading, "text": body} for heading, body in sections.items()
                if any(normalize(requested) == normalize(heading)
                       or normalize(requested) in normalize(heading)
                       for requested in explanation.assumption_sections)]
        for table, explanation in config.explanations.items()
    }


def load_diagnostic(config_path: str | Path = "config/parameters/transport_comparison.yaml") -> Diagnostic:
    """Reload SQLite values, source annotations, and live assumption text from disk."""
    root = find_repo_root(Path(config_path).resolve().parent)
    path = resolve_repo_path(root, config_path)
    config = ComparisonConfig.model_validate(load_yaml(path))
    bundle = load_config_bundle(config.scenario, repo_root=root)
    database = resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    reference = resolve_repo_path(root, config.reference_sqlite)
    report_path = resolve_repo_path(root, bundle.scenario.outputs.validation_report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    freshness = _verify_snapshot(bundle, database, report)
    tolerances = {
        "absolute_tolerance": bundle.scenario.comparison.absolute_tolerance,
        "relative_tolerance": bundle.scenario.comparison.relative_tolerance,
    }
    summary, rows, metadata = compare_parameters(database, reference, config, periods=bundle.scenario.periods, **tolerances)
    assumptions = resolve_repo_path(root, config.assumptions_path)
    metadata.update(
        freshness=freshness, assumption_excerpts=_assumption_excerpts(assumptions, config),
        files={label: {"path": str(file), "sha256": file_sha256(file)} for label, file in (
            ("candidate", database), ("legacy", reference), ("build_report", report_path),
            ("diagnostic_config", path), ("assumptions", assumptions),
        )},
        tolerances=tolerances,
        source_id_mapping=report.get("source_id_mapping", {}),
    )
    development = {}
    for name, compare in (
        ("existing_capacity", compare_legacy_existing_capacity), ("demand", compare_legacy_demand),
        ("lifetime_tech", compare_legacy_lifetime_tech), ("efficiency", compare_legacy_efficiency),
        ("costs", compare_legacy_costs),
    ):
        development[name] = compare(database, reference, **tolerances)
    return Diagnostic(config, bundle, summary, rows, metadata, development)


def write_report(diagnostic: Diagnostic) -> Path:
    """Explicitly publish portable review evidence to its configured output route."""
    destination = resolve_artifact_path(diagnostic.bundle, "transport_comparison_report")
    frames = {
        "summary.csv": diagnostic.summary,
        "rows.csv": diagnostic.rows,
        "ranked_relative.csv": ranked_rows(diagnostic.rows, max(1, len(diagnostic.rows))),
        "ranked_absolute.csv": ranked_rows(diagnostic.rows, max(1, len(diagnostic.rows)), "absolute_difference"),
        "explanations.csv": explanation_rows(diagnostic.config),
    }
    for filename, frame in frames.items():
        write_dataframe_atomic(frame, destination / filename)
    snapshot = {
        "config": diagnostic.config.model_dump(mode="json"),
        "metadata": diagnostic.metadata,
        "development_comparisons": diagnostic.development_comparisons,
        "exports": {name: {"rows": len(frame), "sha256": file_sha256(destination / name)}
                    for name, frame in frames.items()},
    }
    # Publish the manifest last so it identifies the complete exported tables.
    write_text_atomic(json.dumps(snapshot, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
                      destination / "snapshot.json")
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/parameters/transport_comparison.yaml")
    parser.add_argument("--write-report", action="store_true", help="Publish CSV and JSON review evidence")
    arguments = parser.parse_args()
    diagnostic = load_diagnostic(arguments.config)
    print(diagnostic.summary.drop(columns=["explanation"]).to_string(index=False))
    if arguments.write_report:
        print(f"Review evidence: {write_report(diagnostic)}")


if __name__ == "__main__":
    main()
