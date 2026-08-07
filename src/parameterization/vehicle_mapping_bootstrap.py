"""Bootstrap reviewed Ontario vehicle-size mappings from cached source evidence.

This is an explicit maintainer tool, not a runtime ETL stage. Ordinary fetching
and parameterization commands only read ``vehicle_size_class_map.csv``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from fetching.vehicle_population import write_dataframe_atomic
from parameterization.road_aggregation import (
    ONTARIO_RULE_KEY,
    apply_vehicle_mapping,
    assign_rating_model_families,
    generate_mapping_candidates,
    load_rating_evidence,
    mapping_coverage,
    module_rules,
    normalize_vehicle_text,
    validate_vehicle_mapping,
)
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    resolve_parameter_path,
)

LOGGER = logging.getLogger(__name__)
RATINGS_RULE_KEY = "nrcan_fuel_consumption_ratings"
DEFAULT_SCENARIO = "config/scenarios/legacy_reproduction.yaml"

DEFAULT_MODEL_MATCH_PRIORITY = {
    "exact_normalized_model": 0,
    "canonical_model_plus_mto_suffix": 1,
    "normalized_model_prefix": 2,
    "anchored_consonant_abbreviation": 3,
}
def _make_match_method(
    mto_make: object,
    rating_make: object,
    *,
    canonical_aliases: dict[str, str],
    minimum_prefix_length: int,
) -> str | None:
    source = normalize_vehicle_text(mto_make)
    target = normalize_vehicle_text(rating_make)
    alias = canonical_aliases.get(source)
    if alias is not None and normalize_vehicle_text(alias) == target:
        return "configured_make_alias"
    if source == target:
        return "exact_normalized_make"
    if len(source) >= minimum_prefix_length and target.startswith(source):
        return "normalized_make_prefix"
    return None


def _model_match_method(
    mto_model: object,
    rating_model: object,
    *,
    minimum_prefix_length: int,
) -> str | None:
    source = normalize_vehicle_text(mto_model)
    target = normalize_vehicle_text(rating_model)
    if not source or not target:
        return None
    if source == target:
        return "exact_normalized_model"
    if (
        len(target) >= 2
        and len(source) - len(target) in {1, 2}
        and source.endswith(target)
    ):
        return "mto_prefix_plus_canonical_model"
    if (
        len(target) >= 2
        and len(source) - len(target) in {1, 2}
        and source.startswith(target)
        and any(character.isalpha() for character in target)
        and any(character.isdigit() for character in target)
    ):
        return "canonical_model_plus_mto_suffix"
    if len(source) >= minimum_prefix_length and target.startswith(source):
        return "normalized_model_prefix"
    if (
        len(source) >= minimum_prefix_length
        and len(target) > len(source)
        and all(character.isalnum() for character in source)
    ):
        # MTO commonly removes vowels and sometimes internal consonants from a
        # model-family name (RNG -> Ranger; FRT -> Frontier).  Anchor the first
        # two code characters to the consonant/digit skeleton before allowing
        # an ordered subsequence.  This prevents an arbitrary match such as
        # CRG -> C-MAX Energi, whose skeleton starts CM rather than CR.
        target_skeleton = "".join(
            character
            for character in target
            if character.isdigit() or character not in "AEIOU"
        )
        target_characters = iter(target_skeleton)
        if (
            source[:2] == target_skeleton[:2]
            and all(character in target_characters for character in source)
        ):
            return "anchored_consonant_abbreviation"
    return None


def _fit_active_stock_rows(
    current_stock: pd.DataFrame,
    *,
    exclude_future_model_years: bool,
    minimum_model_year: int | None = None,
    positive_only: bool = True,
) -> pd.DataFrame:
    required = {
        "report_year",
        "MAKE",
        "MODEL",
        "MODEL_YEAR",
        "FIT_ACTIVE",
    }
    missing = sorted(required - set(current_stock.columns))
    if missing:
        raise ValueError(
            "Ontario current-stock input missing columns: " + ", ".join(missing)
        )
    stock = current_stock.copy()
    for column in ["report_year", "MODEL_YEAR", "FIT_ACTIVE"]:
        stock[column] = pd.to_numeric(stock[column], errors="coerce")
    stock = stock.dropna(
        subset=["report_year", "MAKE", "MODEL", "MODEL_YEAR", "FIT_ACTIVE"]
    )
    if positive_only:
        stock = stock.loc[stock["FIT_ACTIVE"].gt(0)].copy()
    if minimum_model_year is not None:
        stock = stock.loc[stock["MODEL_YEAR"].ge(minimum_model_year)].copy()
    if exclude_future_model_years:
        stock = stock.loc[
            stock["MODEL_YEAR"].le(stock["report_year"])
        ].copy()
    stock[["report_year", "MODEL_YEAR"]] = stock[
        ["report_year", "MODEL_YEAR"]
    ].astype(int)
    return (
        stock.groupby(
            ["MAKE", "MODEL", "MODEL_YEAR"],
            as_index=False,
            dropna=False,
        )
        .agg(
            FIT_ACTIVE=("FIT_ACTIVE", "sum"),
            first_report_year=("report_year", "min"),
            last_report_year=("report_year", "max"),
            edition_count=("report_year", "nunique"),
        )
        .sort_values(
            ["FIT_ACTIVE", "MAKE", "MODEL", "MODEL_YEAR"],
            ascending=[False, True, True, True],
            kind="stable",
        )
    )


def _covered_by_seed(
    seed: pd.DataFrame,
) -> set[tuple[str, str]]:
    """Return reviewed MTO make/model keys irrespective of vintage."""
    return {
        (str(row.mto_make_code), str(row.mto_model_code))
        for row in seed.itertuples(index=False)
    }


def reviewed_crosswalk_seed(
    reviewed_mapping: pd.DataFrame,
    *,
    policy: str,
) -> pd.DataFrame:
    """Select version-controlled crosswalks that a refresh must preserve."""
    crosswalk = reviewed_mapping.loc[
        reviewed_mapping["entry_type"].eq("mto_crosswalk")
    ].copy()
    if policy == "preserve_all":
        return crosswalk
    if policy == "rebuild":
        return crosswalk.iloc[0:0].copy()
    raise ValueError(f"Unsupported reviewed crosswalk policy: {policy}")


def reviewed_label_hints(
    reviewed_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Return curated MTO-to-model labels without retaining old class claims."""
    columns = [
        "mto_make_code",
        "mto_model_code",
        "model_year_from",
        "model_year_to",
        "canonical_make",
        "canonical_model",
    ]
    hints = reviewed_mapping.loc[
        reviewed_mapping["entry_type"].eq("mto_crosswalk"), columns
    ].copy()
    hints["model_year_from"] = pd.to_numeric(
        hints["model_year_from"], errors="coerce"
    )
    hints["model_year_to"] = pd.to_numeric(
        hints["model_year_to"], errors="coerce"
    )
    return hints.drop_duplicates().reset_index(drop=True)


def _label_hint_for_year(
    hints_by_key: dict[tuple[str, str], list[dict[str, Any]]],
    key: tuple[str, str, int],
) -> dict[str, Any] | None:
    """Select a reviewed label hint for an MTO vintage, if unambiguous."""
    candidates = hints_by_key.get(key[:2], [])
    ranged = [
        row
        for row in candidates
        if (
            pd.isna(row["model_year_from"])
            or int(row["model_year_from"]) <= key[2]
        )
        and (
            pd.isna(row["model_year_to"])
            or key[2] <= int(row["model_year_to"])
        )
    ]
    if not ranged:
        labels = {
            (str(row["canonical_make"]), str(row["canonical_model"]))
            for row in candidates
        }
        if len(labels) != 1:
            return None
        canonical_make, canonical_model = next(iter(labels))
        return {
            "canonical_make": canonical_make,
            "canonical_model": canonical_model,
        }
    labels = {
        (str(row["canonical_make"]), str(row["canonical_model"]))
        for row in ranged
    }
    if len(labels) != 1:
        return None
    canonical_make, canonical_model = next(iter(labels))
    return {
        "canonical_make": canonical_make,
        "canonical_model": canonical_model,
    }


def automatically_supported_years(
    current_stock: pd.DataFrame,
    rating_evidence: pd.DataFrame,
    *,
    canonical_aliases: dict[str, str] | None = None,
    seed_mapping: pd.DataFrame | None = None,
    label_hints: pd.DataFrame | None = None,
    model_match_priority: dict[str, int] | None = None,
    evidence_source_priority: list[str] | None = None,
    minimum_make_prefix_length: int = 3,
    minimum_model_prefix_length: int = 3,
    minimum_model_year: int | None = None,
    exclude_future_model_years: bool = True,
) -> pd.DataFrame:
    """Return stock years with unambiguous make/model/class evidence."""
    rating_evidence = assign_rating_model_families(rating_evidence)
    rating_evidence = (
        rating_evidence.groupby(
            [
                "Model year",
                "canonical_make",
                "canonical_model",
                "Vehicle class",
                "nlr_atb_class",
                "nrcan_ceud_class",
                "evidence_source",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            Model=(
                "Model",
                lambda values: " | ".join(
                    sorted(set(map(str, values)), key=lambda value: value.casefold())
                ),
            ),
            source_row_count=("Model", "size"),
        )
    )
    aliases = {
        normalize_vehicle_text(source): str(target)
        for source, target in (canonical_aliases or {}).items()
    }
    match_priority = model_match_priority or DEFAULT_MODEL_MATCH_PRIORITY
    source_priority = evidence_source_priority or [
        "nrcan_fuel_consumption_ratings",
        "fueleconomy_gov_vehicle_data",
    ]
    stock = _fit_active_stock_rows(
        current_stock,
        exclude_future_model_years=exclude_future_model_years,
        minimum_model_year=minimum_model_year,
    )
    seed = seed_mapping if seed_mapping is not None else pd.DataFrame()
    covered = _covered_by_seed(seed) if not seed.empty else set()
    hints_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if label_hints is not None and not label_hints.empty:
        for hint in label_hints.to_dict("records"):
            hint_key = (
                str(hint["mto_make_code"]).strip().upper(),
                str(hint["mto_model_code"]).strip().upper(),
            )
            hints_by_key.setdefault(hint_key, []).append(hint)

    ratings_by_make: dict[str, list[dict[str, Any]]] = {}
    for rating in rating_evidence.to_dict("records"):
        normalized_make = normalize_vehicle_text(rating["canonical_make"])
        for prefix_length in range(1, len(normalized_make) + 1):
            ratings_by_make.setdefault(
                normalized_make[:prefix_length],
                [],
            ).append(rating)
    accepted_rows: list[dict[str, Any]] = []
    for stock_row in stock.to_dict("records"):
        key = (
            str(stock_row["MAKE"]).strip().upper(),
            str(stock_row["MODEL"]).strip().upper(),
            int(stock_row["MODEL_YEAR"]),
        )
        if key[:2] in covered:
            continue
        label_hint = _label_hint_for_year(hints_by_key, key)
        match_make = (
            str(label_hint["canonical_make"]) if label_hint else key[0]
        )
        match_model = (
            str(label_hint["canonical_model"]) if label_hint else key[1]
        )
        normalized_make = normalize_vehicle_text(match_make)
        make_lookup = normalize_vehicle_text(
            aliases.get(normalized_make, normalized_make)
        )
        compatible = ratings_by_make.get(make_lookup, [])
        if not compatible:
            continue

        candidates: list[dict[str, Any]] = []
        for rating in compatible:
            make_method = _make_match_method(
                match_make,
                rating["canonical_make"],
                canonical_aliases=aliases,
                minimum_prefix_length=minimum_make_prefix_length,
            )
            if make_method is None:
                continue
            model_method = _model_match_method(
                match_model,
                rating["canonical_model"],
                minimum_prefix_length=minimum_model_prefix_length,
            )
            if model_method is None or model_method not in match_priority:
                continue
            if label_hint is not None:
                model_method = "reviewed_canonical_label"
            candidates.append(
                {
                    **rating,
                    "make_match_method": make_method,
                    "model_match_method": model_method,
                }
            )
        if not candidates:
            continue

        best_priority = min(
            match_priority[row["model_match_method"]]
            for row in candidates
        )
        best = [
            row
            for row in candidates
            if match_priority[row["model_match_method"]] == best_priority
        ]
        exact_year = [
            row for row in best if int(row["Model year"]) == key[2]
        ]
        if exact_year:
            best = exact_year
            year_resolution = "exact_mto_model_year"
        else:
            nearest_distance = min(
                abs(int(row["Model year"]) - key[2]) for row in best
            )
            best = [
                row
                for row in best
                if abs(int(row["Model year"]) - key[2]) == nearest_distance
            ]
            year_resolution = "nearest_source_model_year"
        if best[0]["model_match_method"] == "anchored_consonant_abbreviation":
            canonical_families = {
                normalize_vehicle_text(row["canonical_model"]) for row in best
            }
            if len(canonical_families) != 1:
                continue
        shortest_model_length = min(
            len(normalize_vehicle_text(row["canonical_model"])) for row in best
        )
        best = [
            row
            for row in best
            if len(normalize_vehicle_text(row["canonical_model"]))
            == shortest_model_length
        ]
        preferred_source = next(
            (
                source
                for source in source_priority
                if any(
                    str(row.get("evidence_source")) == source
                    for row in best
                )
            ),
            str(best[0].get("evidence_source")),
        )
        preferred = [
            row
            for row in best
            if str(row.get("evidence_source")) == preferred_source
        ]
        aggregate_hierarchies = {
            (str(row["nlr_atb_class"]), str(row["nrcan_ceud_class"]))
            for row in preferred
        }
        if len(aggregate_hierarchies) != 1:
            continue
        nrcan_class_counts: dict[str, int] = {}
        for row in preferred:
            source_class = str(row["Vehicle class"])
            nrcan_class_counts[source_class] = (
                nrcan_class_counts.get(source_class, 0) + 1
            )
        nrcan_class = min(
            nrcan_class_counts,
            key=lambda source_class: (
                -nrcan_class_counts[source_class],
                source_class.casefold(),
            ),
        )

        representative = min(
            [row for row in preferred if str(row["Vehicle class"]) == nrcan_class],
            key=lambda row: (
                len(normalize_vehicle_text(row["canonical_model"])),
                str(row["canonical_model"]),
                str(row["canonical_make"]),
            ),
        )
        nlr_class, ceud_class = next(iter(aggregate_hierarchies))
        accepted_rows.append(
            {
                "first_report_year": int(stock_row["first_report_year"]),
                "last_report_year": int(stock_row["last_report_year"]),
                "edition_count": int(stock_row["edition_count"]),
                "mto_make_code": key[0],
                "mto_model_code": key[1],
                "model_year": key[2],
                "fit_active_stock": float(stock_row["FIT_ACTIVE"]),
                "canonical_make": representative["canonical_make"],
                "canonical_model": representative["canonical_model"],
                "nrcan_vehicle_class": nrcan_class,
                "nlr_atb_class": nlr_class,
                "nrcan_ceud_class": ceud_class,
                "make_match_method": representative["make_match_method"],
                "model_match_method": representative["model_match_method"],
                "supporting_rating_rows": sum(
                    int(row["source_row_count"]) for row in best
                ),
                "supporting_model_labels": " | ".join(
                    sorted(
                        {str(row["Model"]) for row in best},
                        key=lambda value: (value.casefold(), value),
                    )
                ),
                "supporting_evidence_sources": " | ".join(
                    sorted({str(row["evidence_source"]) for row in best})
                ),
                "year_resolution": year_resolution,
            }
        )
    return pd.DataFrame(accepted_rows)


def reviewed_strong_candidate_years(
    current_stock: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    """Return current exact/prefix candidates explicitly approved for review."""
    if candidates.empty:
        return pd.DataFrame()
    top = candidates.loc[
        pd.to_numeric(candidates["candidate_rank"], errors="coerce").eq(1)
        & candidates["candidate_status"].eq("ranked_candidate")
        & candidates["match_method"].isin(
            ["exact_normalized", "normalized_prefix"]
        )
    ].copy()
    if top.empty:
        return pd.DataFrame()
    top = top.drop(
        columns=[
            "fit_active_stock",
            "source_rows",
            "observed_model_year_from",
            "observed_model_year_to",
        ],
        errors="ignore",
    )
    stock = _fit_active_stock_rows(
        current_stock,
        exclude_future_model_years=False,
        minimum_model_year=None,
        positive_only=False,
    ).rename(columns={"MAKE": "mto_make_code", "MODEL": "mto_model_code"})
    supported = stock.merge(
        top,
        on=["mto_make_code", "mto_model_code"],
        how="inner",
        validate="many_to_one",
    )
    if supported.empty:
        return pd.DataFrame()
    supported = supported.rename(
        columns={
            "MODEL_YEAR": "model_year",
            "FIT_ACTIVE": "fit_active_stock",
            "rating_model_labels": "supporting_model_labels",
            "evidence_source": "supporting_evidence_sources",
        }
    )
    supported["supporting_rating_rows"] = 1
    supported["make_match_method"] = "reviewed_current_strong_candidate"
    supported["model_match_method"] = "reviewed_canonical_label"
    supported["year_resolution"] = "reviewed_current_strong_candidate"
    columns = [
        "first_report_year",
        "last_report_year",
        "edition_count",
        "mto_make_code",
        "mto_model_code",
        "model_year",
        "fit_active_stock",
        "canonical_make",
        "canonical_model",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
        "make_match_method",
        "model_match_method",
        "supporting_rating_rows",
        "supporting_model_labels",
        "supporting_evidence_sources",
        "year_resolution",
    ]
    return supported.loc[:, columns].reset_index(drop=True)


def fill_stable_family_years(
    supported: pd.DataFrame,
    *,
    hierarchy_dominance_share: float,
) -> pd.DataFrame:
    """Fill internal vintage gaps for keys with one stable canonical family."""
    if supported.empty:
        return supported.copy()
    output: list[pd.DataFrame] = []
    hierarchy = [
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    ]
    for _, key_rows in supported.groupby(
        ["mto_make_code", "mto_model_code"], sort=False
    ):
        key_rows = key_rows.sort_values("model_year", kind="stable").copy()
        families = key_rows[["canonical_make", "canonical_model"]].drop_duplicates()
        if len(families) != 1:
            output.append(key_rows)
            continue
        exposure = (
            key_rows.groupby(hierarchy, as_index=False, dropna=False)[
                "fit_active_stock"
            ]
            .sum()
            .sort_values("fit_active_stock", ascending=False, kind="stable")
        )
        total_exposure = float(exposure["fit_active_stock"].sum())
        if total_exposure and (
            float(exposure.iloc[0]["fit_active_stock"]) / total_exposure
            >= hierarchy_dominance_share
        ):
            dominant = exposure.iloc[0]
            for column in hierarchy:
                key_rows[column] = dominant[column]
        indexed = key_rows.drop_duplicates("model_year", keep="last").set_index(
            "model_year"
        )
        complete_years = range(
            int(indexed.index.min()), int(indexed.index.max()) + 1
        )
        filled = indexed.reindex(complete_years)
        missing = filled["mto_make_code"].isna()
        filled = filled.ffill().bfill()
        filled.index.name = "model_year"
        filled = filled.reset_index()
        filled.loc[missing.to_numpy(), "fit_active_stock"] = 0.0
        filled.loc[missing.to_numpy(), "edition_count"] = 0
        filled.loc[missing.to_numpy(), "year_resolution"] = (
            "stable_canonical_family_gap_fill"
        )
        output.append(filled)
    return pd.concat(output, ignore_index=True, sort=False)


def apply_reviewed_class_overrides(
    supported: pd.DataFrame,
    overrides: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Apply narrowly reviewed hierarchy decisions after candidate inference."""
    output = supported.copy()
    allowed = {
        "canonical_make",
        "canonical_model",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    }
    for mto_key, values in overrides.items():
        make_code, model_code = str(mto_key).split("/", maxsplit=1)
        unsupported = sorted(set(values) - allowed)
        if unsupported:
            raise ValueError(
                f"Unsupported reviewed class override fields for {mto_key}: "
                + ", ".join(unsupported)
            )
        mask = output["mto_make_code"].eq(make_code) & output[
            "mto_model_code"
        ].eq(model_code)
        for column, value in values.items():
            output.loc[mask, column] = str(value)
    return output


def collapse_supported_years(
    supported: pd.DataFrame,
    *,
    canonical_model_overrides: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Collapse contiguous, identically supported MTO years into ranges."""
    columns = [
        "entry_type",
        "mto_make_code",
        "mto_model_code",
        "model_year_from",
        "model_year_to",
        "canonical_make",
        "canonical_model",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
        "match_method",
        "mapping_status",
        "evidence_source",
        "supporting_rating_rows",
        "supporting_model_labels",
        "review_notes",
    ]
    if supported.empty:
        return pd.DataFrame(columns=columns)
    supported = supported.copy()
    if "supporting_evidence_sources" not in supported:
        supported["supporting_evidence_sources"] = (
            "nrcan_fuel_consumption_ratings"
        )
    if "year_resolution" not in supported:
        supported["year_resolution"] = "nearest_source_model_year"

    signature = [
        "mto_make_code",
        "mto_model_code",
        "canonical_make",
        "canonical_model",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
        "make_match_method",
        "model_match_method",
    ]
    range_rows: list[dict[str, Any]] = []
    for group_key, rows in supported.groupby(signature, dropna=False, sort=False):
        ordered = rows.sort_values("model_year", kind="stable").copy()
        ordered["_range_id"] = ordered["model_year"].diff().ne(1).cumsum()
        for _, contiguous in ordered.groupby("_range_id", sort=False):
            record = dict(zip(signature, group_key, strict=True))
            record.update(
                {
                    "model_year_from": int(contiguous["model_year"].min()),
                    "model_year_to": int(contiguous["model_year"].max()),
                    "fit_active_stock": contiguous["fit_active_stock"].sum(),
                    "supporting_rating_rows": int(
                        contiguous["supporting_rating_rows"].max()
                    ),
                    "supporting_model_labels": " | ".join(
                        sorted(
                            {
                                label
                                for value in contiguous["supporting_model_labels"]
                                for label in str(value).split(" | ")
                                if label
                            },
                            key=lambda value: (value.casefold(), value),
                        )
                    ),
                    "supporting_evidence_sources": " | ".join(
                        sorted(
                            {
                                source
                                for value in contiguous[
                                    "supporting_evidence_sources"
                                ]
                                for source in str(value).split(" | ")
                                if source
                            }
                        )
                    ),
                    "year_resolution": " | ".join(
                        sorted(set(map(str, contiguous["year_resolution"])))
                    ),
                }
            )
            range_rows.append(record)
    ranges = pd.DataFrame(range_rows).sort_values(
        ["mto_make_code", "mto_model_code", "model_year_from"],
        kind="stable",
    )
    overrides = canonical_model_overrides or {}
    ranges["canonical_model"] = ranges.apply(
        lambda row: overrides.get(
            f"{row['mto_make_code']}/{row['mto_model_code']}",
            row["canonical_model"],
        ),
        axis=1,
    )
    ranges.insert(0, "entry_type", "mto_crosswalk")
    ranges["match_method"] = (
        "automatic_"
        + ranges["make_match_method"]
        + "_"
        + ranges["model_match_method"]
    )
    ranges["mapping_status"] = "reviewed"
    ranges["evidence_source"] = ranges["supporting_evidence_sources"].map(
        lambda value: f"{value}; Ontario Vehicle Population Report A historical fit-active stock"
    )
    ranges["review_notes"] = ranges.apply(
        lambda row: (
            "Deterministic cached-evidence bootstrap; "
            f"{int(round(row.fit_active_stock)):,} historical fit-active exposure; "
            f"{int(row.supporting_rating_rows)} supporting source rows; "
            f"year resolution={row.year_resolution}; MTO model year selects this "
            "non-overlapping range and all best model-label matches agree on the "
            "recorded NLR/CEUD hierarchy."
        ),
        axis=1,
    )
    return ranges.loc[:, columns].reset_index(drop=True)


def build_bootstrap_mapping(
    bundle: ConfigBundle,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build the reviewed map and its automatically supported year evidence."""
    rules = module_rules(bundle)
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    output_dir = resolve_input_path(
        bundle,
        "interim",
        ontario_rules["interim_subdir"],
    )
    manifest = pd.read_csv(output_dir / str(ontario_rules["manifest_file"]))
    historical_frames: list[pd.DataFrame] = []
    usecols = [
        "report_year",
        "MAKE",
        "MODEL",
        "MODEL_YEAR",
        "FIT_ACTIVE",
    ]
    for row in manifest.sort_values("year").itertuples(index=False):
        if hasattr(row, "cohort_snapshot_usable") and str(
            row.cohort_snapshot_usable
        ).strip().lower() not in {"true", "1", "yes"}:
            continue
        historical_frames.append(
            pd.read_csv(
                output_dir / str(row.normalized_report_a_output),
                usecols=usecols,
                low_memory=False,
            )
        )
    if not historical_frames:
        raise ValueError("No usable historical Report A editions were found")
    historical_stock = pd.concat(historical_frames, ignore_index=True)
    current_stock = pd.read_csv(
        output_dir / str(ontario_rules["current_stock_file"]),
        low_memory=False,
    )
    mapping_path = resolve_parameter_path(
        bundle,
        rules["vehicle_size_class_map_file"],
    )
    reviewed_mapping = pd.read_csv(
        mapping_path,
        dtype=str,
        keep_default_na=False,
    )
    if "entry_type" not in reviewed_mapping.columns:
        reviewed_mapping.insert(0, "entry_type", "mto_crosswalk")
    if "supporting_rating_rows" not in reviewed_mapping.columns:
        reviewed_mapping["supporting_rating_rows"] = ""
    if "supporting_model_labels" not in reviewed_mapping.columns:
        reviewed_mapping["supporting_model_labels"] = reviewed_mapping[
            "canonical_model"
        ]
    reviewed_mapping = reviewed_mapping.loc[:, rules["mapping_columns"]]
    bootstrap_rules = rules["mapping_bootstrap"]
    seed = reviewed_crosswalk_seed(
        reviewed_mapping,
        policy=str(bootstrap_rules["reviewed_crosswalk_policy"]),
    )
    label_hints = reviewed_label_hints(reviewed_mapping)
    configured_hints: list[dict[str, Any]] = []
    for mto_key, hint in bootstrap_rules.get(
        "reviewed_model_label_hints", {}
    ).items():
        make_code, model_code = str(mto_key).split("/", maxsplit=1)
        configured_hints.append(
            {
                "mto_make_code": make_code,
                "mto_model_code": model_code,
                "model_year_from": pd.NA,
                "model_year_to": pd.NA,
                "canonical_make": str(hint["canonical_make"]),
                "canonical_model": str(hint["canonical_model"]),
            }
        )
    if configured_hints:
        configured_keys = {
            (str(row["mto_make_code"]), str(row["mto_model_code"]))
            for row in configured_hints
        }
        label_hints = label_hints.loc[
            ~label_hints[["mto_make_code", "mto_model_code"]]
            .apply(tuple, axis=1)
            .isin(configured_keys)
        ].copy()
        label_hints = pd.concat(
            [label_hints, pd.DataFrame(configured_hints)],
            ignore_index=True,
        ).drop_duplicates()
    seed = validate_vehicle_mapping(
        seed,
        rules=rules,
        rating_class_rules=rating_rules["vehicle_class_harmonization"],
    )
    ratings = load_rating_evidence(bundle, rules=rules)
    supported = automatically_supported_years(
        historical_stock,
        ratings,
        canonical_aliases={
            str(source): str(target)
            for source, target in rules.get("canonical_aliases", {}).items()
        },
        seed_mapping=None,
        label_hints=label_hints,
        model_match_priority={
            str(method): int(priority)
            for method, priority in bootstrap_rules[
                "accepted_model_match_methods"
            ].items()
        },
        evidence_source_priority=[
            str(source)
            for source in bootstrap_rules["evidence_source_priority"]
        ],
        minimum_make_prefix_length=int(
            bootstrap_rules["minimum_make_prefix_length"]
        ),
        minimum_model_prefix_length=int(
            bootstrap_rules["minimum_model_prefix_length"]
        ),
        minimum_model_year=int(
            bootstrap_rules.get(
                "candidate_minimum_model_year",
                bootstrap_rules["minimum_model_year"],
            )
        ),
        exclude_future_model_years=bool(
            bootstrap_rules["exclude_future_model_years"]
        ),
    )
    if bool(bootstrap_rules.get("accept_reviewed_strong_candidates", False)):
        current_candidates = generate_mapping_candidates(
            current_stock,
            ratings,
            canonical_aliases={
                str(source): str(target)
                for source, target in rules.get("canonical_aliases", {}).items()
            },
        )
        strong_supported = reviewed_strong_candidate_years(
            current_stock,
            current_candidates,
        )
        if not strong_supported.empty:
            supported_keys = set(
                supported[["mto_make_code", "mto_model_code", "model_year"]]
                .apply(tuple, axis=1)
                .tolist()
            )
            strong_supported = strong_supported.loc[
                ~strong_supported[
                    ["mto_make_code", "mto_model_code", "model_year"]
                ]
                .apply(tuple, axis=1)
                .isin(supported_keys)
            ]
            supported = pd.concat(
                [supported, strong_supported], ignore_index=True, sort=False
            )
    supported = fill_stable_family_years(
        supported,
        hierarchy_dominance_share=float(
            bootstrap_rules["stable_family_hierarchy_dominance_share"]
        ),
    )
    supported = apply_reviewed_class_overrides(
        supported,
        {
            str(key): {str(column): str(value) for column, value in values.items()}
            for key, values in bootstrap_rules.get(
                "reviewed_class_overrides", {}
            ).items()
        },
    )
    supported_for_generation = supported
    if not seed.empty and not supported.empty:
        seed_ranges: dict[tuple[str, str], list[tuple[int, int]]] = {}
        for row in seed.itertuples(index=False):
            seed_ranges.setdefault(
                (str(row.mto_make_code), str(row.mto_model_code)), []
            ).append((int(row.model_year_from), int(row.model_year_to)))
        already_reviewed = supported.apply(
            lambda row: any(
                start <= int(row["model_year"]) <= end
                for start, end in seed_ranges.get(
                    (str(row["mto_make_code"]), str(row["mto_model_code"])),
                    [],
                )
            ),
            axis=1,
        )
        supported_for_generation = supported.loc[~already_reviewed].copy()
    generated = collapse_supported_years(
        supported_for_generation,
        canonical_model_overrides={
            str(key): str(value)
            for key, value in bootstrap_rules.get(
                "canonical_model_overrides",
                {},
            ).items()
        },
    )
    combined = pd.concat([seed, generated], ignore_index=True)
    combined = validate_vehicle_mapping(
        combined,
        rules=rules,
        rating_class_rules=rating_rules["vehicle_class_harmonization"],
    )
    combined = combined.sort_values(
        [
            "entry_type",
            "canonical_make",
            "canonical_model",
            "nrcan_vehicle_class",
            "mto_make_code",
            "mto_model_code",
            "model_year_from",
        ],
        kind="stable",
    ).reset_index(drop=True)
    return combined, supported


def bootstrap_coverage(
    bundle: ConfigBundle,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Measure the proposed mapping against latest Report A fit-active stock."""
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    output_dir = resolve_input_path(
        bundle,
        "interim",
        ontario_rules["interim_subdir"],
    )
    current_stock = pd.read_csv(
        output_dir / str(ontario_rules["current_stock_file"]),
        low_memory=False,
    )
    mapped = apply_vehicle_mapping(
        current_stock,
        mapping,
        accepted_statuses={
            str(status) for status in rules["accepted_mapping_statuses"]
        },
    )
    return mapping_coverage(mapped)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Requested reviewed CSV output path.",
    )
    parser.add_argument(
        "--replace-reviewed-config",
        action="store_true",
        help="Required when --output is the configured reviewed mapping path.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    bundle = load_config_bundle(args.scenario)
    mapping, supported = build_bootstrap_mapping(bundle)
    configured_path = resolve_parameter_path(
        bundle,
        module_rules(bundle)["vehicle_size_class_map_file"],
    ).resolve()
    output_path = (
        args.output
        if args.output.is_absolute()
        else bundle.repo_root / args.output
    ).resolve()
    if output_path == configured_path and not args.replace_reviewed_config:
        raise ValueError(
            "Refusing to replace reviewed configuration without "
            "--replace-reviewed-config"
        )
    write_dataframe_atomic(mapping, output_path)
    bootstrap_file = module_rules(bundle).get("bootstrap_evidence_file")
    if bootstrap_file:
        ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
        interim_dir = resolve_input_path(
            bundle,
            "interim",
            ontario_rules["interim_subdir"],
        )
        if supported.empty:
            raise ValueError(
                "Historical mapping bootstrap evidence is unexpectedly empty"
            )
        write_dataframe_atomic(supported, interim_dir / str(bootstrap_file))
    coverage = bootstrap_coverage(bundle, mapping)
    LOGGER.info(
        "Wrote %d reviewed mapping ranges (%d automatically supported years) to %s",
        len(mapping),
        len(supported),
        output_path,
    )
    for row in coverage.itertuples(index=False):
        LOGGER.info(
            "%s coverage: %s/%s (%.3f%%)",
            row.measure,
            row.mapped,
            row.total,
            float(row.coverage) * 100,
        )


if __name__ == "__main__":
    main()
