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
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
    write_dataframe_atomic,
)
from utils.vehicle_labels import (
    candidate_matches_any,
    is_unresolved_vehicle_label,
    reconcile_candidate_passes,
)

LOGGER = logging.getLogger(__name__)
RATINGS_RULE_KEY = "nrcan_fuel_consumption_ratings"
DEFAULT_SCENARIO = "config/scenarios/legacy_reproduction.yaml"
MANUAL_DERIVED_COLUMNS = [
    "canonical_make",
    "candidate_pass_agreement",
    "agreed_model_candidate",
    "manual_promotion_eligible",
    "manual_evidence_disposition",
    "notes",
    "source -> data_source",
]

DEFAULT_MODEL_MATCH_PRIORITY = {
    "exact_normalized_model": 0,
    "canonical_model_plus_mto_suffix": 1,
    "normalized_model_prefix": 2,
    "anchored_consonant_abbreviation": 3,
}


def prepare_manual_evidence(
    manual: pd.DataFrame,
    *,
    canonical_make_aliases: dict[str, str],
    source_selector: str,
    reaudit_keys: set[tuple[str, str]] | None = None,
) -> pd.DataFrame:
    """Validate and deterministically derive reviewed two-pass evidence fields."""
    required = {
        "mto_make_code",
        "mto_model_code",
        "latest_fit_active_stock",
        "highest_confidence_candidate",
        "vpic_second_pass_candidate",
    }
    missing = sorted(required - set(manual.columns))
    if missing:
        raise ValueError("Manual MTO evidence is missing columns: " + ", ".join(missing))
    output = manual.drop(columns=MANUAL_DERIVED_COLUMNS, errors="ignore").copy()
    for column in ["mto_make_code", "mto_model_code"]:
        output[column] = output[column].astype(str).str.strip().str.upper()
    if output.duplicated(["mto_make_code", "mto_model_code"]).any():
        raise ValueError("Manual MTO evidence must contain one row per make/model key")
    stock = pd.to_numeric(output["latest_fit_active_stock"], errors="raise")
    if not stock.gt(0).all():
        raise ValueError("Manual MTO evidence contains non-positive latest stock")
    aliases = {
        str(key).strip().upper(): str(value).strip()
        for key, value in canonical_make_aliases.items()
    }
    output["canonical_make"] = output["mto_make_code"].map(aliases).fillna(
        output["mto_make_code"]
    )
    agreements = output.apply(
        lambda row: reconcile_candidate_passes(
            row["highest_confidence_candidate"],
            row["vpic_second_pass_candidate"],
        ),
        axis=1,
    )
    output["candidate_pass_agreement"] = agreements.map(lambda value: value.status)
    output["agreed_model_candidate"] = agreements.map(
        lambda value: value.agreed_candidate or ""
    )
    reaudited = output[["mto_make_code", "mto_model_code"]].apply(
        tuple, axis=1
    ).isin(reaudit_keys or set())
    output["manual_promotion_eligible"] = reaudited.map(
        {True: "false", False: "true"}
    )
    output["manual_evidence_disposition"] = reaudited.map(
        {
            True: "superseded_high_stock_reaudit",
            False: "promotion_eligible",
        }
    )
    output["notes"] = (
        "Latest-active reviewed MTO key; pass agreement is recomputed using "
        "case-insensitive model-family equivalence."
    )
    output["source -> data_source"] = source_selector
    return output


def load_manual_evidence(
    bundle: ConfigBundle,
    *,
    bootstrap_rules: dict[str, Any],
    canonical_make_aliases: dict[str, str],
    reaudit_keys: set[tuple[str, str]],
) -> pd.DataFrame:
    """Load the registered manual inventory and verify persisted derived fields."""
    path = resolve_input_path(
        bundle,
        "manual",
        str(bootstrap_rules["manual_evidence_file"]),
    )
    persisted = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected = prepare_manual_evidence(
        persisted,
        canonical_make_aliases=canonical_make_aliases,
        source_selector=str(bootstrap_rules["manual_evidence_source_selector"]),
        reaudit_keys=reaudit_keys,
    )
    if list(persisted.columns) != list(expected.columns):
        raise ValueError(
            "Manual MTO evidence columns differ from the reviewed schema: "
            f"{list(persisted.columns)} != {list(expected.columns)}"
        )
    for column in MANUAL_DERIVED_COLUMNS:
        if not persisted[column].equals(expected[column]):
            raise ValueError(f"Manual MTO evidence has stale derived column: {column}")
    return expected


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


def derive_mapping_model_year_floor(
    historical_stock: pd.DataFrame,
    *,
    maximum_transition_age: int,
) -> int:
    """Return the oldest vintage that can enter the observed transition window."""
    report_years = sorted(
        pd.to_numeric(historical_stock["report_year"], errors="raise")
        .astype(int)
        .unique()
    )
    transition_starts = [year for year in report_years if year + 1 in report_years]
    if not transition_starts:
        raise ValueError("Cannot derive a mapping floor without Report A editions")
    return min(transition_starts) - int(maximum_transition_age)


def latest_stock_reaudit_keys(
    current_stock: pd.DataFrame,
    *,
    minimum_model_year: int | None,
    quantile: float,
) -> tuple[set[tuple[str, str]], float, pd.DataFrame]:
    """Select the tied upper quantile of latest positive-stock MTO keys."""
    if not 0 <= quantile <= 1:
        raise ValueError("High-stock re-audit quantile must be in [0, 1]")
    stock = _fit_active_stock_rows(
        current_stock,
        exclude_future_model_years=False,
        minimum_model_year=minimum_model_year,
    )
    by_key = (
        stock.groupby(["MAKE", "MODEL"], as_index=False)["FIT_ACTIVE"]
        .sum()
        .sort_values(["FIT_ACTIVE", "MAKE", "MODEL"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    cutoff = float(by_key["FIT_ACTIVE"].quantile(quantile))
    selected = by_key.loc[by_key["FIT_ACTIVE"].ge(cutoff)].copy()
    keys = set(selected[["MAKE", "MODEL"]].apply(tuple, axis=1))
    return keys, cutoff, selected


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
    *,
    minimum_similarity: float = 0.7,
) -> pd.DataFrame:
    """Return unambiguous rank-one candidates approved by the similarity rule."""
    if candidates.empty:
        return pd.DataFrame()
    if not 0 <= minimum_similarity <= 1:
        raise ValueError("Candidate model-similarity threshold must be in [0, 1]")
    similarity = pd.to_numeric(candidates["model_similarity"], errors="coerce")
    top = candidates.loc[
        pd.to_numeric(candidates["candidate_rank"], errors="coerce").eq(1)
        & candidates["candidate_status"].eq("ranked_candidate")
        & similarity.ge(minimum_similarity)
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
    rating_rows = (
        supported["rating_rows"]
        if "rating_rows" in supported
        else pd.Series(1, index=supported.index)
    )
    supported["supporting_rating_rows"] = pd.to_numeric(
        rating_rows, errors="coerce"
    ).fillna(1).astype(int)
    supported["make_match_method"] = "approved_current_candidate"
    supported["candidate_match_method"] = supported["match_method"].astype(str)
    supported["candidate_model_similarity"] = pd.to_numeric(
        supported["model_similarity"], errors="raise"
    )
    supported["model_match_method"] = (
        "approved_similarity_" + supported["candidate_match_method"]
    )
    supported["year_resolution"] = "approved_current_candidate_similarity"
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
        "candidate_match_method",
        "candidate_model_similarity",
    ]
    return supported.loc[:, columns].reset_index(drop=True)


def _manual_label_hints(manual: pd.DataFrame) -> pd.DataFrame:
    promotion_eligible = manual["manual_promotion_eligible"].eq("true")
    promoted = manual.loc[
        promotion_eligible
        & manual["candidate_pass_agreement"].eq("agreement")
        & ~manual["agreed_model_candidate"].map(is_unresolved_vehicle_label)
    ].copy()
    if promoted.empty:
        return pd.DataFrame(
            columns=[
                "mto_make_code",
                "mto_model_code",
                "model_year_from",
                "model_year_to",
                "canonical_make",
                "canonical_model",
            ]
        )
    promoted["model_year_from"] = pd.NA
    promoted["model_year_to"] = pd.NA
    promoted["canonical_model"] = promoted["agreed_model_candidate"]
    return promoted.loc[
        :,
        [
            "mto_make_code",
            "mto_model_code",
            "model_year_from",
            "model_year_to",
            "canonical_make",
            "canonical_model",
        ],
    ]


def _classless_supported_years(
    stock: pd.DataFrame,
    manual_row: pd.Series,
    *,
    vehicle_scope: str,
) -> pd.DataFrame:
    stock = stock.loc[
        stock["MAKE"].eq(manual_row["mto_make_code"])
        & stock["MODEL"].eq(manual_row["mto_model_code"])
    ].copy()
    if stock.empty:
        return pd.DataFrame()
    stock = stock.rename(
        columns={
            "MAKE": "mto_make_code",
            "MODEL": "mto_model_code",
            "MODEL_YEAR": "model_year",
            "FIT_ACTIVE": "fit_active_stock",
        }
    )
    stock["canonical_make"] = manual_row["canonical_make"]
    stock["canonical_model"] = manual_row["agreed_model_candidate"]
    stock["vehicle_scope"] = vehicle_scope
    stock["nrcan_vehicle_class"] = ""
    stock["nlr_atb_class"] = ""
    stock["nrcan_ceud_class"] = ""
    stock["make_match_method"] = "reviewed_manual_make"
    stock["model_match_method"] = "reviewed_two_pass_agreement"
    stock["supporting_rating_rows"] = 0
    stock["supporting_model_labels"] = manual_row["agreed_model_candidate"]
    stock["supporting_evidence_sources"] = "reviewed_mto_make_model_evidence"
    stock["year_resolution"] = "reviewed_manual_classless_family"
    return stock


def load_vpic_scope_evidence(bundle: ConfigBundle) -> pd.DataFrame:
    """Load optional cached vPIC classifications without causing network access."""
    rules = load_harmonization_rules(bundle, "vpic_vehicle_types")
    path = resolve_input_path(
        bundle,
        "interim",
        str(rules["interim_subdir"]),
        str(rules["output_file"]),
    )
    if not path.is_file():
        return pd.DataFrame(
            columns=["mto_make_code", "mto_model_code", "vehicle_scope"]
        )
    evidence = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"mto_make_code", "mto_model_code", "vehicle_scope"}
    missing = sorted(required - set(evidence.columns))
    if missing:
        raise ValueError("vPIC evidence is missing columns: " + ", ".join(missing))
    return evidence


def load_vpic_temporal_evidence(bundle: ConfigBundle) -> pd.DataFrame:
    """Load optional cached make-model-year corroboration without network access."""
    rules = load_harmonization_rules(bundle, "vpic_model_years")
    path = resolve_input_path(
        bundle,
        "interim",
        str(rules["interim_subdir"]),
        str(rules["output_file"]),
    )
    columns = [
        "mto_make_code",
        "mto_model_code",
        "model_year",
        "canonical_make",
        "canonical_model",
        "temporal_validation_status",
    ]
    if not path.is_file():
        return pd.DataFrame(columns=columns)
    evidence = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = sorted(set(columns) - set(evidence.columns))
    if missing:
        raise ValueError("vPIC temporal evidence is missing columns: " + ", ".join(missing))
    return evidence


def build_temporal_request_rows(
    supported: pd.DataFrame,
    current_stock: pd.DataFrame,
    *,
    reaudit_keys: set[tuple[str, str]],
    minimum_model_year: int,
) -> pd.DataFrame:
    """Publish proposed mapped key-years for the dedicated temporal adapter."""
    columns = [
        "mto_make_code",
        "mto_model_code",
        "model_year",
        "canonical_make",
        "canonical_model",
        "exact_public_year_evidence",
        "high_stock_reaudit",
        "latest_fit_active_stock",
    ]
    if supported.empty:
        return pd.DataFrame(columns=columns)
    requests = supported.loc[
        pd.to_numeric(supported["model_year"], errors="coerce").ge(
            minimum_model_year
        )
    ].copy()
    if requests.empty:
        return pd.DataFrame(columns=columns)
    requests["model_year"] = pd.to_numeric(
        requests["model_year"], errors="raise"
    ).astype(int)
    requests["exact_public_year_evidence"] = requests["year_resolution"].astype(
        str
    ).str.contains("exact_mto_model_year", regex=False)
    request_keys = requests[["mto_make_code", "mto_model_code"]].apply(tuple, axis=1)
    requests["high_stock_reaudit"] = request_keys.isin(reaudit_keys)
    latest = _fit_active_stock_rows(
        current_stock,
        exclude_future_model_years=False,
        minimum_model_year=None,
    )
    latest = latest.groupby(["MAKE", "MODEL"], as_index=False)["FIT_ACTIVE"].sum()
    requests = requests.merge(
        latest,
        left_on=["mto_make_code", "mto_model_code"],
        right_on=["MAKE", "MODEL"],
        how="left",
        validate="many_to_one",
    ).rename(columns={"FIT_ACTIVE": "latest_fit_active_stock"})
    requests["latest_fit_active_stock"] = requests["latest_fit_active_stock"].fillna(0)
    for column in ["exact_public_year_evidence", "high_stock_reaudit"]:
        requests[column] = requests[column].map({True: "true", False: "false"})
    return (
        requests.loc[:, columns]
        .drop_duplicates()
        .sort_values(
            ["canonical_make", "model_year", "canonical_model", "mto_make_code", "mto_model_code"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def apply_high_stock_temporal_gate(
    supported: pd.DataFrame,
    temporal_evidence: pd.DataFrame,
    *,
    reaudit_keys: set[tuple[str, str]],
    minimum_model_year: int,
) -> pd.DataFrame:
    """Require affirmative year evidence for the high-stock re-audit cohort."""
    output = supported.copy()
    if output.empty:
        output["temporal_validation_status"] = pd.Series(dtype=str)
        return output
    evidence_status = {
        (
            str(row.mto_make_code),
            str(row.mto_model_code),
            int(row.model_year),
            str(row.canonical_make),
            str(row.canonical_model),
        ): str(row.temporal_validation_status)
        for row in temporal_evidence.itertuples(index=False)
    }
    statuses: list[str] = []
    keep: list[bool] = []
    for row in output.itertuples(index=False):
        key = (str(row.mto_make_code), str(row.mto_model_code))
        year = int(row.model_year)
        exact = "exact_mto_model_year" in str(row.year_resolution)
        status = evidence_status.get(
            (
                key[0],
                key[1],
                year,
                str(row.canonical_make),
                str(row.canonical_model),
            ),
            "not_requested",
        )
        statuses.append(status)
        approved_candidate = (
            str(row.year_resolution) == "approved_current_candidate_similarity"
        )
        requires_gate = key in reaudit_keys and year >= minimum_model_year
        keep.append(
            not requires_gate or exact or status == "confirmed" or approved_candidate
        )
    output["temporal_validation_status"] = statuses
    return output.loc[keep].reset_index(drop=True)


def reconcile_manual_supported_years(
    historical_stock: pd.DataFrame,
    pipeline_supported: pd.DataFrame,
    manual_supported: pd.DataFrame,
    manual: pd.DataFrame,
    vpic_evidence: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply the reviewed two-pass hierarchy and publish gated vPIC requests."""
    pipeline = pipeline_supported.copy()
    pipeline["vehicle_scope"] = "ldv"
    manual_classed = manual_supported.copy()
    if not manual_classed.empty:
        manual_classed["vehicle_scope"] = "ldv"
    promotion_eligible = manual["manual_promotion_eligible"].eq("true")
    approved_candidate_rows = pipeline["year_resolution"].eq(
        "approved_current_candidate_similarity"
    )
    approved_candidate_keys = set(
        pipeline.loc[
            approved_candidate_rows,
            ["mto_make_code", "mto_model_code"],
        ].apply(tuple, axis=1)
    )
    manual_keys = set(
        manual.loc[promotion_eligible, ["mto_make_code", "mto_model_code"]]
        .apply(tuple, axis=1)
    )
    pipeline_keys = pipeline[["mto_make_code", "mto_model_code"]].apply(
        tuple, axis=1
    )
    retained = pipeline.loc[
        ~pipeline_keys.isin(manual_keys)
        | pipeline_keys.isin(approved_candidate_keys)
    ].copy()
    vpic_scopes = {
        (str(row.mto_make_code), str(row.mto_model_code)): str(row.vehicle_scope)
        for row in vpic_evidence.itertuples(index=False)
    }
    selected_frames: list[pd.DataFrame] = [retained]
    reconciliation_rows: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    all_stock = _fit_active_stock_rows(
        historical_stock,
        exclude_future_model_years=False,
        minimum_model_year=None,
    )
    eligible_vpic_stock = all_stock.loc[
        all_stock["MODEL_YEAR"].ge(1996)
        & all_stock["MODEL_YEAR"].le(all_stock["last_report_year"])
    ].copy()
    for _, manual_row in manual.iterrows():
        key = (manual_row["mto_make_code"], manual_row["mto_model_code"])
        if manual_row["manual_promotion_eligible"] != "true":
            reconciliation_rows.append(
                {
                    "mto_make_code": key[0],
                    "mto_model_code": key[1],
                    "canonical_make": manual_row["canonical_make"],
                    "candidate_pass_agreement": manual_row["candidate_pass_agreement"],
                    "agreed_model_candidate": manual_row["agreed_model_candidate"],
                    "pipeline_model_families": "",
                    "selected_model_family": "",
                    "promotion_status": "superseded",
                    "class_evidence_status": "not_applicable",
                    "vehicle_scope": "",
                    "reconciliation_reason": "superseded_high_stock_reaudit",
                }
            )
            continue
        if key in approved_candidate_keys:
            approved_rows = pipeline.loc[
                pipeline["mto_make_code"].eq(key[0])
                & pipeline["mto_model_code"].eq(key[1])
            ]
            selected_families = " | ".join(
                sorted(set(approved_rows["canonical_model"].astype(str)))
            )
            reconciliation_rows.append(
                {
                    "mto_make_code": key[0],
                    "mto_model_code": key[1],
                    "canonical_make": manual_row["canonical_make"],
                    "candidate_pass_agreement": manual_row[
                        "candidate_pass_agreement"
                    ],
                    "agreed_model_candidate": manual_row[
                        "agreed_model_candidate"
                    ],
                    "pipeline_model_families": selected_families,
                    "selected_model_family": selected_families,
                    "promotion_status": "promoted",
                    "class_evidence_status": "ldv_class_resolved",
                    "vehicle_scope": "ldv",
                    "reconciliation_reason": (
                        "approved_candidate_similarity_supersedes_manual_arbitration"
                    ),
                }
            )
            continue
        base_rows = pipeline.loc[
            pipeline["mto_make_code"].eq(key[0])
            & pipeline["mto_model_code"].eq(key[1])
        ].copy()
        selected = base_rows.iloc[0:0].copy()
        promotion_status = "rejected"
        class_status = "not_applicable"
        selected_family = ""
        reason = "three_way_conflict_or_unresolved_manual_candidate"
        agreement = manual_row["candidate_pass_agreement"]
        agreed_candidate = manual_row["agreed_model_candidate"]
        if agreement == "agreement" and not is_unresolved_vehicle_label(
            agreed_candidate
        ):
            selected_family = str(agreed_candidate)
            selected = manual_classed.loc[
                manual_classed["mto_make_code"].eq(key[0])
                & manual_classed["mto_model_code"].eq(key[1])
            ].copy()
            promotion_status = "promoted"
            if selected.empty:
                class_status = "missing_ldv_class"
                scope = vpic_scopes.get(key, "non_ldv_unclassified")
                selected = _classless_supported_years(
                    all_stock,
                    manual_row,
                    vehicle_scope=scope,
                )
                reason = "two_pass_agreement_without_ldv_class"
                eligible_years = eligible_vpic_stock.loc[
                    eligible_vpic_stock["MAKE"].eq(key[0])
                    & eligible_vpic_stock["MODEL"].eq(key[1])
                ]
                if not eligible_years.empty:
                    request_rows.append(
                        {
                            "mto_make_code": key[0],
                            "mto_model_code": key[1],
                            "canonical_make": manual_row["canonical_make"],
                            "canonical_model": agreed_candidate,
                            "query_model_year": int(eligible_years["MODEL_YEAR"].max()),
                            "latest_fit_active_stock": manual_row[
                                "latest_fit_active_stock"
                            ],
                            "promotion_status": "promoted",
                            "class_evidence_status": "missing_ldv_class",
                        }
                    )
            else:
                class_status = "ldv_class_resolved"
                reason = "two_pass_agreement_with_ldv_class"
        elif agreement == "disagreement" and not base_rows.empty:
            matches = base_rows["canonical_model"].map(
                lambda candidate: candidate_matches_any(
                    candidate,
                    manual_row["highest_confidence_candidate"],
                    manual_row["vpic_second_pass_candidate"],
                )
            )
            selected = base_rows.loc[matches].copy()
            if not selected.empty:
                promotion_status = "promoted"
                class_status = "ldv_class_resolved"
                selected_family = " | ".join(
                    sorted(set(selected["canonical_model"].astype(str)))
                )
                reason = "pipeline_family_matches_one_disagreeing_pass"
        if not selected.empty:
            selected_frames.append(selected)
        pipeline_families = " | ".join(
            sorted(set(base_rows.get("canonical_model", pd.Series(dtype=str)).astype(str)))
        )
        reconciliation_rows.append(
            {
                "mto_make_code": key[0],
                "mto_model_code": key[1],
                "canonical_make": manual_row["canonical_make"],
                "candidate_pass_agreement": agreement,
                "agreed_model_candidate": agreed_candidate,
                "pipeline_model_families": pipeline_families,
                "selected_model_family": selected_family,
                "promotion_status": promotion_status,
                "class_evidence_status": class_status,
                "vehicle_scope": (
                    str(selected["vehicle_scope"].iloc[0]) if not selected.empty else ""
                ),
                "reconciliation_reason": reason,
            }
        )
    combined = pd.concat(selected_frames, ignore_index=True, sort=False)
    reconciliation = pd.DataFrame(reconciliation_rows)
    request_columns = [
        "mto_make_code",
        "mto_model_code",
        "canonical_make",
        "canonical_model",
        "query_model_year",
        "latest_fit_active_stock",
        "promotion_status",
        "class_evidence_status",
    ]
    requests = pd.DataFrame(request_rows, columns=request_columns)
    return combined, reconciliation, requests


def fill_stable_family_years(
    supported: pd.DataFrame,
    *,
    hierarchy_dominance_share: float,
    excluded_keys: set[tuple[str, str]] | None = None,
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
        key = (
            str(key_rows["mto_make_code"].iloc[0]),
            str(key_rows["mto_model_code"].iloc[0]),
        )
        if key in (excluded_keys or set()):
            output.append(key_rows)
            continue
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


def apply_reviewed_scope_overrides(
    supported: pd.DataFrame,
    overrides: dict[str, str],
) -> pd.DataFrame:
    """Apply reviewed non-LDV decisions and remove incompatible LDV hierarchy."""
    output = supported.copy()
    allowed = {"mhdv", "non_ldv_unclassified"}
    for mto_key, scope in overrides.items():
        if scope not in allowed:
            raise ValueError(f"Unsupported reviewed scope override for {mto_key}: {scope}")
        make_code, model_code = str(mto_key).split("/", maxsplit=1)
        mask = output["mto_make_code"].eq(make_code) & output[
            "mto_model_code"
        ].eq(model_code)
        output.loc[mask, "vehicle_scope"] = scope
        output.loc[
            mask,
            ["nrcan_vehicle_class", "nlr_atb_class", "nrcan_ceud_class"],
        ] = ""
    return output


def apply_canonical_model_overrides(
    supported: pd.DataFrame,
    overrides: dict[str, str],
) -> pd.DataFrame:
    """Apply reviewed canonical spelling before temporal corroboration."""
    output = supported.copy()
    for mto_key, canonical_model in overrides.items():
        make_code, model_code = str(mto_key).split("/", maxsplit=1)
        mask = output["mto_make_code"].eq(make_code) & output[
            "mto_model_code"
        ].eq(model_code)
        output.loc[mask, "canonical_model"] = str(canonical_model)
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
        "vehicle_scope",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
        "match_method",
        "mapping_status",
        "evidence_source",
        "supporting_rating_rows",
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
    if "vehicle_scope" not in supported:
        supported["vehicle_scope"] = "ldv"

    signature = [
        "mto_make_code",
        "mto_model_code",
        "canonical_make",
        "canonical_model",
        "vehicle_scope",
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
    ranges["review_notes"] = ranges["vehicle_scope"].map(
        {
            "mhdv": "Public evidence supports a mapped non-LDV truck family.",
            "non_ldv_unclassified": (
                "Public evidence supports the family but not an LDV class hierarchy."
            ),
        }
    ).fillna("")
    return ranges.loc[:, columns].reset_index(drop=True)


def build_bootstrap_mapping(
    bundle: ConfigBundle,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
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
    if "vehicle_scope" not in reviewed_mapping.columns:
        reviewed_mapping.insert(
            reviewed_mapping.columns.get_loc("nrcan_vehicle_class"),
            "vehicle_scope",
            "ldv",
        )
    reviewed_mapping = reviewed_mapping.loc[:, rules["mapping_columns"]]
    bootstrap_rules = rules["mapping_bootstrap"]
    lifetime_rules = load_harmonization_rules(bundle, "lifetimes_survival")
    mapping_floor = derive_mapping_model_year_floor(
        historical_stock,
        maximum_transition_age=int(lifetime_rules["maximum_transition_age"]),
    )
    reaudit_keys, reaudit_cutoff, reaudit_inventory = latest_stock_reaudit_keys(
        current_stock,
        minimum_model_year=None,
        quantile=float(bootstrap_rules["high_stock_reaudit_quantile"]),
    )
    LOGGER.info(
        "High-stock re-audit selects %d keys at FIT_ACTIVE >= %s; mapping floor=%d",
        len(reaudit_keys),
        f"{reaudit_cutoff:g}",
        mapping_floor,
    )
    canonical_make_aliases = {
        str(source): str(target)
        for source, target in {
            **rules.get("canonical_aliases", {}),
            **bootstrap_rules.get("manual_make_aliases", {}),
        }.items()
    }
    reviewed_make_names = (
        reviewed_mapping.loc[
            reviewed_mapping["entry_type"].eq("mto_crosswalk")
            & reviewed_mapping["canonical_make"].ne(""),
            ["mto_make_code", "canonical_make"],
        ]
        .drop_duplicates()
        .groupby("mto_make_code")["canonical_make"]
        .agg(lambda values: values.iloc[0] if values.nunique() == 1 else "")
    )
    canonical_make_aliases.update(
        {
            str(code): str(make)
            for code, make in reviewed_make_names.items()
            if str(make)
        }
    )
    manual = load_manual_evidence(
        bundle,
        bootstrap_rules=bootstrap_rules,
        canonical_make_aliases=canonical_make_aliases,
        reaudit_keys=reaudit_keys,
    )
    seed = reviewed_crosswalk_seed(
        reviewed_mapping,
        policy=str(bootstrap_rules["reviewed_crosswalk_policy"]),
    )
    label_hints = reviewed_label_hints(reviewed_mapping)
    label_hints = label_hints.loc[
        ~label_hints[["mto_make_code", "mto_model_code"]]
        .apply(tuple, axis=1)
        .isin(reaudit_keys)
    ].copy()
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
        minimum_model_year=mapping_floor,
        exclude_future_model_years=bool(
            bootstrap_rules["exclude_future_model_years"]
        ),
    )
    manual_supported = automatically_supported_years(
        historical_stock,
        ratings,
        canonical_aliases=canonical_make_aliases,
        seed_mapping=None,
        label_hints=_manual_label_hints(manual),
        model_match_priority={
            str(method): int(priority)
            for method, priority in bootstrap_rules[
                "accepted_model_match_methods"
            ].items()
        },
        evidence_source_priority=[
            str(source) for source in bootstrap_rules["evidence_source_priority"]
        ],
        minimum_make_prefix_length=int(
            bootstrap_rules["minimum_make_prefix_length"]
        ),
        minimum_model_prefix_length=2,
        minimum_model_year=mapping_floor,
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
            minimum_similarity=float(
                bootstrap_rules["accepted_candidate_model_similarity"]
            ),
        )
        if not strong_supported.empty and configured_hints:
            supported_keys = set(
                supported[["mto_make_code", "mto_model_code"]].apply(tuple, axis=1)
            )
            configured_supported_keys = configured_keys & supported_keys
            configured_approved = strong_supported.loc[
                strong_supported[["mto_make_code", "mto_model_code"]]
                .apply(tuple, axis=1)
                .isin(configured_supported_keys)
            ].copy()
            for key, approved_rows in configured_approved.groupby(
                ["mto_make_code", "mto_model_code"], sort=False
            ):
                approved_row = approved_rows.iloc[0]
                configured_mask = supported["mto_make_code"].eq(key[0]) & supported[
                    "mto_model_code"
                ].eq(key[1])
                supported.loc[
                    configured_mask, "year_resolution"
                ] = "approved_current_candidate_similarity"
                supported.loc[
                    configured_mask, "candidate_match_method"
                ] = approved_row["candidate_match_method"]
                supported.loc[
                    configured_mask, "candidate_model_similarity"
                ] = approved_row["candidate_model_similarity"]
            strong_supported = strong_supported.loc[
                ~strong_supported[["mto_make_code", "mto_model_code"]]
                .apply(tuple, axis=1)
                .isin(configured_supported_keys)
            ].copy()
        if not strong_supported.empty:
            strong_supported = strong_supported.loc[
                pd.to_numeric(strong_supported["model_year"], errors="coerce").ge(
                    mapping_floor
                )
            ].copy()
        if not strong_supported.empty:
            approved_key_years = set(
                strong_supported[
                    ["mto_make_code", "mto_model_code", "model_year"]
                ].apply(tuple, axis=1)
            )
            supported = supported.loc[
                ~supported[
                    ["mto_make_code", "mto_model_code", "model_year"]
                ]
                .apply(tuple, axis=1)
                .isin(approved_key_years)
            ]
            supported = pd.concat(
                [supported, strong_supported], ignore_index=True, sort=False
            )
    supported, reconciliation, eligible_requests = reconcile_manual_supported_years(
        historical_stock,
        supported,
        manual_supported,
        manual,
        load_vpic_scope_evidence(bundle),
    )
    supported = supported.loc[
        pd.to_numeric(supported["model_year"], errors="coerce").ge(mapping_floor)
    ].copy()
    supported = apply_reviewed_class_overrides(
        supported,
        {
            str(key): {str(column): str(value) for column, value in values.items()}
            for key, values in bootstrap_rules.get(
                "reviewed_class_overrides", {}
            ).items()
        },
    )
    supported = apply_reviewed_scope_overrides(
        supported,
        {
            str(key): str(value)
            for key, value in bootstrap_rules.get(
                "reviewed_scope_overrides", {}
            ).items()
        },
    )
    canonical_model_overrides = {
        str(key): str(value)
        for key, value in bootstrap_rules.get("canonical_model_overrides", {}).items()
    }
    supported = apply_canonical_model_overrides(
        supported,
        canonical_model_overrides,
    )
    temporal_requests = build_temporal_request_rows(
        supported,
        current_stock,
        reaudit_keys=reaudit_keys,
        minimum_model_year=int(
            bootstrap_rules["temporal_validation_minimum_model_year"]
        ),
    )
    supported = apply_high_stock_temporal_gate(
        supported,
        load_vpic_temporal_evidence(bundle),
        reaudit_keys=reaudit_keys,
        minimum_model_year=int(
            bootstrap_rules["temporal_validation_minimum_model_year"]
        ),
    )
    supported = fill_stable_family_years(
        supported,
        hierarchy_dominance_share=float(
            bootstrap_rules["stable_family_hierarchy_dominance_share"]
        ),
        excluded_keys=reaudit_keys,
    )
    for mto_key, scope in bootstrap_rules.get(
        "reviewed_scope_overrides", {}
    ).items():
        make_code, model_code = str(mto_key).split("/", maxsplit=1)
        mask = reconciliation["mto_make_code"].eq(make_code) & reconciliation[
            "mto_model_code"
        ].eq(model_code)
        reconciliation.loc[mask, "vehicle_scope"] = str(scope)
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
        canonical_model_overrides=canonical_model_overrides,
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
    reconciliation["mapping_model_year_floor"] = mapping_floor
    reconciliation["high_stock_reaudit_cutoff"] = reaudit_cutoff
    reconciliation["high_stock_reaudit_key_count"] = len(reaudit_inventory)
    return combined, supported, reconciliation, eligible_requests, temporal_requests


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
    (
        mapping,
        supported,
        reconciliation,
        eligible_requests,
        temporal_requests,
    ) = build_bootstrap_mapping(bundle)
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
        interim_dir = resolve_artifact_path(bundle, "vehicle_mapping_review")
        if supported.empty:
            raise ValueError(
                "Historical mapping bootstrap evidence is unexpectedly empty"
            )
        write_dataframe_atomic(supported, interim_dir / str(bootstrap_file))
        write_dataframe_atomic(
            reconciliation,
            interim_dir / str(module_rules(bundle)["manual_reconciliation_file"]),
        )
        write_dataframe_atomic(
            eligible_requests,
            interim_dir / str(module_rules(bundle)["vpic_eligible_request_file"]),
        )
        write_dataframe_atomic(
            temporal_requests,
            interim_dir / str(module_rules(bundle)["vpic_temporal_request_file"]),
        )
    coverage = bootstrap_coverage(bundle, mapping)
    LOGGER.info(
        "Wrote %d reviewed mapping ranges (%d automatically supported years) to %s",
        len(mapping),
        len(supported),
        output_path,
    )
    for row in coverage.itertuples(index=False):
        LOGGER.info(
            "%s LDV/non-LDV/unmapped/out-of-scope: %s/%s/%s/%s of %s",
            row.measure,
            row.mapped_ldv,
            row.mapped_non_ldv,
            row.unmapped,
            row.out_of_scope,
            row.total,
        )


if __name__ == "__main__":
    main()
