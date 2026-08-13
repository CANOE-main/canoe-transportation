"""Apply reviewed LDV mappings and derive Ontario Report A aggregation weights."""

import argparse
import logging
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

from fetching.vehicle_population import write_dataframe_atomic
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    resolve_parameter_path,
)
from utils.vehicle_labels import is_unresolved_vehicle_label


ONTARIO_RULE_KEY = "ontario_vehicle_population"
ROAD_RULE_KEY = "road_aggregation"
RATINGS_RULE_KEY = "nrcan_fuel_consumption_ratings"
FUELECONOMY_RULE_KEY = "fueleconomy_vehicle_data"
WARDS_SOURCE_ID = "wards_intelligence_2022_sales_shares"


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load road-aggregation rules."""
    return load_harmonization_rules(bundle, ROAD_RULE_KEY)


def normalize_vehicle_text(value: object) -> str:
    """Return deterministic ASCII alphanumeric make/model text."""
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Z0-9]+", "", ascii_text.upper())


def nrcan_to_nlr_map(class_rules: dict[str, Any]) -> dict[str, str]:
    """Invert the existing NRCan-to-NLR harmonization after uniqueness checks."""
    inverse: dict[str, str] = {}
    for nlr_class, nrcan_classes in class_rules["target_to_nrcan"].items():
        for nrcan_class in nrcan_classes:
            source_class = str(nrcan_class)
            if source_class in inverse:
                raise ValueError(
                    f"NRCan class {source_class!r} maps to multiple NLR classes"
                )
            inverse[source_class] = str(nlr_class)
    return inverse


def ceud_class_for_nlr(nlr_class: str) -> str:
    """Map the established five NLR LDV classes to NRCan CEUD classes."""
    if nlr_class in {"Compact", "Midsize"}:
        return "Car"
    if nlr_class in {"Small SUV", "Midsize SUV", "Pickup"}:
        return "Light Truck"
    raise ValueError(f"Unsupported NLR LDV class: {nlr_class}")


def load_rating_evidence(
    bundle: ConfigBundle,
    *,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Load unified NRCan and FuelEconomy.gov vehicle-class evidence."""
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    rating_dir = resolve_input_path(
        bundle,
        "interim",
        rating_rules["interim_subdir"],
    )
    frames: list[pd.DataFrame] = []
    required = ["Model year", "Make", "Model", "Vehicle class"]
    for filename in rules["nrcan_rating_files"]:
        path = rating_dir / str(filename)
        if not path.is_file():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path, low_memory=False)
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(
                f"NRCan rating evidence {path.name} missing columns: "
                + ", ".join(missing)
            )
        selected = frame.loc[:, required].copy()
        selected["evidence_source"] = "nrcan_fuel_consumption_ratings"
        frames.append(selected)

    fueleconomy_rules = load_harmonization_rules(
        bundle,
        FUELECONOMY_RULE_KEY,
    )
    fueleconomy_dir = resolve_input_path(
        bundle,
        "interim",
        rules["fueleconomy_interim_subdir"],
    )
    fueleconomy_file = str(fueleconomy_rules["output_file"])
    if fueleconomy_file != str(rules["fueleconomy_evidence_file"]):
        raise ValueError(
            "Road aggregation and FuelEconomy.gov output filenames disagree"
        )
    fueleconomy_path = fueleconomy_dir / fueleconomy_file
    if not fueleconomy_path.is_file():
        raise FileNotFoundError(fueleconomy_path)
    fueleconomy = pd.read_csv(fueleconomy_path, low_memory=False)
    fueleconomy_required = {
        "Model year",
        "Make",
        "Model",
        "nrcan_vehicle_class",
        "class_normalization_status",
    }
    missing_fueleconomy = sorted(fueleconomy_required - set(fueleconomy.columns))
    if missing_fueleconomy:
        raise ValueError(
            f"FuelEconomy.gov evidence {fueleconomy_path.name} missing columns: "
            + ", ".join(missing_fueleconomy)
        )
    fueleconomy = fueleconomy.loc[
        fueleconomy["class_normalization_status"].eq("mapped"),
        ["Model year", "Make", "Model", "nrcan_vehicle_class"],
    ].rename(columns={"nrcan_vehicle_class": "Vehicle class"})
    fueleconomy["evidence_source"] = "fueleconomy_gov_vehicle_data"
    frames.append(fueleconomy)
    evidence = pd.concat(frames, ignore_index=True)
    evidence["Model year"] = pd.to_numeric(
        evidence["Model year"],
        errors="coerce",
    ).astype("Int64")
    evidence = evidence.dropna(subset=["Model year", "Make", "Model", "Vehicle class"])
    evidence["Model year"] = evidence["Model year"].astype(int)
    evidence["normalized_make"] = evidence["Make"].map(normalize_vehicle_text)
    evidence["normalized_model"] = evidence["Model"].map(normalize_vehicle_text)

    class_map = nrcan_to_nlr_map(rating_rules["vehicle_class_harmonization"])
    evidence["nlr_atb_class"] = evidence["Vehicle class"].map(class_map)
    unresolved = sorted(
        evidence.loc[evidence["nlr_atb_class"].isna(), "Vehicle class"]
        .dropna()
        .astype(str)
        .unique()
    )
    configured_unresolved = {
        str(value)
        for value in rating_rules["vehicle_class_harmonization"].get(
            "unresolved_nrcan_classes",
            [],
        )
    }
    unexpected = sorted(set(unresolved) - configured_unresolved)
    if unexpected:
        raise ValueError(
            "NRCan rating evidence has unexpected unmapped classes: "
            + ", ".join(unexpected)
        )
    evidence = evidence.dropna(subset=["nlr_atb_class"]).copy()
    evidence["nrcan_ceud_class"] = evidence["nlr_atb_class"].map(
        ceud_class_for_nlr
    )
    evidence = evidence.drop_duplicates(
        ["Model year", "Make", "Model", "Vehicle class", "evidence_source"]
    ).reset_index(drop=True)
    return assign_rating_model_families(evidence)


def _is_model_family_prefix(base: str, label: str) -> bool:
    """Return whether an observed Ratings label extends a family label."""
    base_text = re.sub(r"\s+", " ", str(base).strip())
    label_text = re.sub(r"\s+", " ", str(label).strip())
    if base_text.casefold() == label_text.casefold():
        return True
    if len(normalize_vehicle_text(base_text)) < 2:
        return False
    if not label_text.casefold().startswith(base_text.casefold()):
        return False
    remainder = label_text[len(base_text) :]
    return bool(remainder) and not remainder[0].isalnum()


_RATING_VARIANT_MARKERS = frozenset(
    {
        "2WD",
        "4MATIC",
        "4WD",
        "ASPEC",
        "AT4",
        "AT4X",
        "AWD",
        "BASE",
        "BOSS",
        "CABRIOLET",
        "CONVERTIBLE",
        "COUPE",
        "CUSTOM",
        "DENALI",
        "EDITION",
        "EV",
        "EXT",
        "FFV",
        "FWD",
        "HATCHBACK",
        "HYBRID",
        "ISS",
        "LE",
        "LIMITED",
        "LONG",
        "LT",
        "LX",
        "MAX",
        "MUD",
        "NO",
        "OFF",
        "PACKAGE",
        "PERFORMANCE",
        "PLUG",
        "PLUGIN",
        "PREMIUM",
        "PRIME",
        "PRO",
        "QUATTRO",
        "RANGE",
        "RAPTOR",
        "RST",
        "SE",
        "SEL",
        "SEDAN",
        "SPORT",
        "SPORTBACK",
        "START",
        "STD",
        "STOP",
        "TECHNOLOGY",
        "TERRAIN",
        "TIRE",
        "TIRES",
        "TOURING",
        "TRAIL",
        "TREMOR",
        "TRD",
        "VAN",
        "WAGON",
        "WITH",
        "WITHOUT",
        "WOODLAND",
        "WT",
        "XLE",
        "XDRIVE",
        "XSE",
        "ZR2",
    }
)


def _rating_model_stem(label: object) -> str:
    """Strip source-native drivetrain/trim suffixes from a Ratings label."""
    text = re.sub(r"\s+", " ", str(label).strip())
    tokens = list(re.finditer(r"[A-Za-z0-9]+", text))
    for token in tokens[1:]:
        marker = token.group(0).upper()
        if marker in _RATING_VARIANT_MARKERS:
            stem = text[: token.start()].rstrip(" -/(")
            if len(normalize_vehicle_text(stem)) >= 2:
                return stem
    return text


def _minimal_class_distinguishing_model(
    row: pd.Series,
    family_rows: pd.DataFrame,
) -> str:
    """Retain the shortest model prefix needed for a same-year class split."""
    initial = str(row["_initial_family"])
    same_year = family_rows.loc[
        family_rows["Model year"].eq(row["Model year"])
    ]
    if same_year["Vehicle class"].nunique(dropna=False) <= 1:
        return initial
    tokens = re.findall(r"[A-Za-z0-9]+", str(row["Model"]))
    initial_normalized = normalize_vehicle_text(initial)
    for token_count in range(1, len(tokens) + 1):
        candidate = " ".join(tokens[:token_count])
        normalized_candidate = normalize_vehicle_text(candidate)
        if (
            len(normalized_candidate) < len(initial_normalized)
            or not normalized_candidate.startswith(initial_normalized)
        ):
            continue
        matching = same_year.loc[
            same_year["normalized_model"].str.startswith(normalized_candidate)
        ]
        if set(matching["Vehicle class"].astype(str)) == {
            str(row["Vehicle class"])
        }:
            return candidate
    return initial


def assign_rating_model_families(evidence: pd.DataFrame) -> pd.DataFrame:
    """Assign source labels to minimal, model-year-aware make/model families."""
    required = {
        "Model year",
        "Make",
        "Model",
        "Vehicle class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    }
    missing = sorted(required - set(evidence.columns))
    if missing:
        raise ValueError(
            "Ratings family evidence missing columns: " + ", ".join(missing)
        )
    enriched = evidence.copy()
    if "evidence_source" not in enriched:
        enriched["evidence_source"] = "nrcan_fuel_consumption_ratings"
    if "normalized_make" not in enriched:
        enriched["normalized_make"] = enriched["Make"].map(normalize_vehicle_text)
    if "normalized_model" not in enriched:
        enriched["normalized_model"] = enriched["Model"].map(normalize_vehicle_text)
    enriched["_model_stem"] = enriched["Model"].map(_rating_model_stem)

    make_names = (
        enriched[["normalized_make", "Make"]]
        .drop_duplicates()
        .sort_values(
            ["normalized_make", "Make"],
            key=lambda values: values.astype(str).str.casefold(),
            kind="stable",
        )
        .drop_duplicates("normalized_make")
        .set_index("normalized_make")["Make"]
        .to_dict()
    )
    family_by_stem: dict[tuple[str, str], str] = {}
    for normalized_make, rows in enriched.groupby("normalized_make", dropna=False):
        labels = sorted(
            rows["_model_stem"].dropna().astype(str).unique(),
            key=lambda value: (len(normalize_vehicle_text(value)), value.casefold()),
        )
        for label in labels:
            roots = [base for base in labels if _is_model_family_prefix(base, label)]
            family_by_stem[(str(normalized_make), str(label))] = min(
                roots,
                key=lambda value: (
                    len(normalize_vehicle_text(value)),
                    value.casefold(),
                ),
            )
    enriched["canonical_make"] = enriched["normalized_make"].map(make_names)
    enriched["_initial_family"] = enriched.apply(
        lambda row: family_by_stem[
            (
                str(row["normalized_make"]),
                str(row["_model_stem"]),
            )
        ],
        axis=1,
    )
    enriched["canonical_model"] = enriched["_initial_family"]
    conflicting = (
        enriched.groupby(
            ["normalized_make", "_initial_family", "Model year"],
            dropna=False,
        )["Vehicle class"]
        .transform("nunique")
        .gt(1)
    )
    family_columns = ["normalized_make", "_initial_family"]
    for family_key, conflict_rows in enriched.loc[conflicting].groupby(
        family_columns,
        dropna=False,
    ):
        family_rows = enriched.loc[
            enriched["normalized_make"].eq(family_key[0])
            & enriched["_initial_family"].eq(family_key[1])
        ]
        enriched.loc[conflict_rows.index, "canonical_model"] = conflict_rows.apply(
            _minimal_class_distinguishing_model,
            axis=1,
            family_rows=family_rows,
        )
    enriched["normalized_canonical_model"] = enriched["canonical_model"].map(
        normalize_vehicle_text
    )
    return enriched.drop(columns=["_model_stem", "_initial_family"])


def build_rating_model_catalog(
    rating_evidence: pd.DataFrame,
    *,
    mapping_columns: list[str],
) -> pd.DataFrame:
    """Build exhaustive evidence-only rows for normalized Ratings families."""
    evidence = assign_rating_model_families(rating_evidence)
    grouped = (
        evidence.groupby(
            [
                "canonical_make",
                "canonical_model",
                "Vehicle class",
                "nlr_atb_class",
                "nrcan_ceud_class",
            ],
            as_index=False,
            dropna=False,
        )
        .agg(
            model_year_from=("Model year", "min"),
            model_year_to=("Model year", "max"),
            supporting_rating_rows=("Model year", "size"),
            supporting_model_labels=(
                "Model",
                lambda values: " | ".join(
                    sorted(
                        set(map(str, values)),
                        key=lambda value: (value.casefold(), value),
                    )
                ),
            ),
            evidence_source=(
                "evidence_source",
                lambda values: " | ".join(sorted(set(map(str, values)))),
            ),
        )
        .rename(columns={"Vehicle class": "nrcan_vehicle_class"})
    )
    grouped.insert(0, "entry_type", "ratings_catalog")
    grouped.insert(1, "mto_make_code", "")
    grouped.insert(2, "mto_model_code", "")
    grouped["vehicle_scope"] = "ldv"
    grouped["match_method"] = "ratings_family_token_prefix"
    grouped["mapping_status"] = "evidence_only"
    grouped["review_notes"] = (
        "Generated normalized source-family evidence; not a runtime MTO key."
    )
    return grouped.loc[:, mapping_columns].sort_values(
        ["canonical_make", "canonical_model", "nrcan_vehicle_class"],
        kind="stable",
    ).reset_index(drop=True)


def validate_vehicle_mapping(
    mapping: pd.DataFrame,
    *,
    rules: dict[str, Any],
    rating_class_rules: dict[str, Any],
) -> pd.DataFrame:
    """Validate the human-reviewed mapping without changing its contents."""
    expected = [str(column) for column in rules["mapping_columns"]]
    if list(mapping.columns) != expected:
        raise ValueError(
            "vehicle_size_class_map.csv columns differ from the reviewed schema: "
            f"{list(mapping.columns)} != {expected}"
        )
    if mapping.empty:
        return mapping.copy()

    validated = mapping.copy()
    validated["entry_type"] = validated["entry_type"].astype(str).str.strip()
    allowed_entry_types = {"ratings_catalog", "mto_crosswalk"}
    unexpected_entry_types = sorted(
        set(validated["entry_type"]) - allowed_entry_types
    )
    if unexpected_entry_types:
        raise ValueError(
            "Reviewed mapping has unsupported entry types: "
            + ", ".join(unexpected_entry_types)
        )
    for column in ["mto_make_code", "mto_model_code"]:
        validated[column] = validated[column].astype(str).str.strip().str.upper()
    catalog = validated["entry_type"].eq("ratings_catalog")
    crosswalk = validated["entry_type"].eq("mto_crosswalk")
    if validated.loc[catalog, ["mto_make_code", "mto_model_code"]].ne("").any().any():
        raise ValueError("Ratings catalogue rows must not contain MTO keys")
    if validated.loc[crosswalk, ["mto_make_code", "mto_model_code"]].eq("").any().any():
        raise ValueError("MTO crosswalk rows require nonblank make/model keys")
    if ~validated.loc[catalog, "mapping_status"].eq("evidence_only").all():
        raise ValueError("Ratings catalogue rows must be evidence_only")
    for column in ["model_year_from", "model_year_to"]:
        validated[column] = pd.to_numeric(
            validated[column],
            errors="raise",
        ).astype(int)
    invalid_ranges = validated["model_year_from"] > validated["model_year_to"]
    if invalid_ranges.any():
        raise ValueError("Reviewed mapping has model_year_from after model_year_to")

    allowed_scopes = {"ldv", "mhdv", "non_ldv_unclassified"}
    validated["vehicle_scope"] = validated["vehicle_scope"].fillna("").astype(str)
    unexpected_scopes = sorted(set(validated["vehicle_scope"]) - allowed_scopes)
    if unexpected_scopes:
        raise ValueError(
            "Reviewed mapping has unsupported vehicle_scope values: "
            + ", ".join(unexpected_scopes)
        )
    if ~validated.loc[catalog, "vehicle_scope"].eq("ldv").all():
        raise ValueError("Ratings catalogue rows must have vehicle_scope='ldv'")
    non_ldv = validated["vehicle_scope"].isin(
        {"mhdv", "non_ldv_unclassified"}
    )
    hierarchy_columns = [
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    ]
    if validated.loc[non_ldv, hierarchy_columns].ne("").any().any():
        raise ValueError("Non-LDV mapping rows must leave the LDV hierarchy blank")
    if validated.loc[non_ldv, ["canonical_make", "canonical_model"]].eq("").any().any():
        raise ValueError("Non-LDV mapping rows require canonical make/model evidence")

    inverse = nrcan_to_nlr_map(rating_class_rules)
    for row in validated.itertuples(index=False):
        if row.vehicle_scope != "ldv":
            continue
        expected_nlr = inverse.get(str(row.nrcan_vehicle_class))
        if expected_nlr is None:
            raise ValueError(
                f"Reviewed mapping uses unsupported NRCan class "
                f"{row.nrcan_vehicle_class!r}"
            )
        if str(row.nlr_atb_class) != expected_nlr:
            raise ValueError(
                f"Reviewed mapping hierarchy mismatch for "
                f"{row.mto_make_code}/{row.mto_model_code}: "
                f"{row.nrcan_vehicle_class!r} -> {expected_nlr!r}, not "
                f"{row.nlr_atb_class!r}"
            )
        expected_ceud = ceud_class_for_nlr(expected_nlr)
        if str(row.nrcan_ceud_class) != expected_ceud:
            raise ValueError(
                f"Reviewed mapping CEUD mismatch for "
                f"{row.mto_make_code}/{row.mto_model_code}: "
                f"{expected_ceud!r} expected"
            )

    overlapping_keys: list[str] = []
    for key, rows in validated.loc[crosswalk].groupby(
        ["mto_make_code", "mto_model_code"],
        sort=False,
    ):
        ordered = rows.sort_values(
            ["model_year_from", "model_year_to"],
            kind="stable",
        )
        prior_end: int | None = None
        for row in ordered.itertuples(index=False):
            start = int(row.model_year_from)
            end = int(row.model_year_to)
            if prior_end is not None and start <= prior_end:
                overlapping_keys.append("/".join(map(str, key)))
                break
            prior_end = end
    if overlapping_keys:
        raise ValueError(
            "Reviewed mapping has overlapping model-year ranges for MTO keys: "
            + ", ".join(overlapping_keys[:10])
        )
    duplicate_catalog = validated.loc[catalog].duplicated(
        ["canonical_make", "canonical_model", "nrcan_vehicle_class"],
        keep=False,
    )
    if duplicate_catalog.any():
        raise ValueError(
            "Ratings catalogue must contain one row per normalized family and "
            "NRCan class hierarchy"
        )
    return validated


def _model_similarity(mto_model: str, canonical_model: str) -> tuple[float, str]:
    if mto_model == canonical_model:
        return 1.0, "exact_normalized"
    if canonical_model.startswith(mto_model) or mto_model.startswith(canonical_model):
        return 0.95, "normalized_prefix"
    if mto_model and mto_model in canonical_model:
        return 0.9, "normalized_substring"
    return (
        SequenceMatcher(None, mto_model, canonical_model).ratio(),
        "string_similarity",
    )


def generate_mapping_candidates(
    current_stock: pd.DataFrame,
    rating_evidence: pd.DataFrame,
    *,
    candidates_per_key: int = 5,
    canonical_aliases: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Generate ranked evidence without accepting or overwriting mappings."""
    required_stock = {"MAKE", "MODEL", "MODEL_YEAR", "FIT_ACTIVE"}
    missing = sorted(required_stock - set(current_stock.columns))
    if missing:
        raise ValueError(
            "Ontario current-stock input missing columns: " + ", ".join(missing)
        )
    stock = current_stock.copy()
    stock["MODEL_YEAR"] = pd.to_numeric(stock["MODEL_YEAR"], errors="coerce")
    stock["FIT_ACTIVE"] = pd.to_numeric(stock["FIT_ACTIVE"], errors="coerce")
    stock["normalized_mto_make"] = stock["MAKE"].map(normalize_vehicle_text)
    stock["normalized_mto_model"] = stock["MODEL"].map(normalize_vehicle_text)
    stock_keys = (
        stock.groupby(
            ["MAKE", "MODEL", "normalized_mto_make", "normalized_mto_model"],
            dropna=False,
            as_index=False,
        )
        .agg(
            observed_model_year_from=("MODEL_YEAR", "min"),
            observed_model_year_to=("MODEL_YEAR", "max"),
            fit_active_stock=("FIT_ACTIVE", "sum"),
            source_rows=("FIT_ACTIVE", "size"),
        )
        .sort_values("fit_active_stock", ascending=False, kind="stable")
    )
    rating_evidence = assign_rating_model_families(rating_evidence)
    rating_groups = (
        rating_evidence.groupby(
            [
                "normalized_make",
                "normalized_canonical_model",
                "canonical_make",
                "canonical_model",
                "Vehicle class",
                "nlr_atb_class",
                "nrcan_ceud_class",
            ],
            as_index=False,
        )
        .agg(
            rating_model_year_from=("Model year", "min"),
            rating_model_year_to=("Model year", "max"),
            rating_rows=("Model year", "size"),
            rating_model_labels=(
                "Model",
                lambda values: " | ".join(
                    sorted(
                        set(map(str, values)),
                        key=lambda value: (value.casefold(), value),
                    )
                ),
            ),
            evidence_source=(
                "evidence_source",
                lambda values: " | ".join(sorted(set(map(str, values)))),
            ),
        )
    )
    by_make_prefix: dict[str, list[dict[str, Any]]] = {}
    for record in rating_groups.to_dict("records"):
        normalized_make = str(record["normalized_make"])
        for prefix_length in range(1, len(normalized_make) + 1):
            by_make_prefix.setdefault(
                normalized_make[:prefix_length],
                [],
            ).append(record)

    output: list[dict[str, Any]] = []
    normalized_aliases = {
        normalize_vehicle_text(source): normalize_vehicle_text(target)
        for source, target in (canonical_aliases or {}).items()
    }
    for stock_row in stock_keys.to_dict("records"):
        source_make_code = str(stock_row["normalized_mto_make"])
        make_code = normalized_aliases.get(source_make_code, source_make_code)
        model_code = str(stock_row["normalized_mto_model"])
        possible = by_make_prefix.get(make_code, [])
        ranked: list[dict[str, Any]] = []
        for rating in possible:
            model_score, method = _model_similarity(
                model_code,
                str(rating["normalized_canonical_model"]),
            )
            overlap_start = max(
                int(stock_row["observed_model_year_from"]),
                int(rating["rating_model_year_from"]),
            )
            overlap_end = min(
                int(stock_row["observed_model_year_to"]),
                int(rating["rating_model_year_to"]),
            )
            overlap_years = max(0, overlap_end - overlap_start + 1)
            ranked.append(
                {
                    "mto_make_code": stock_row["MAKE"],
                    "mto_model_code": stock_row["MODEL"],
                    "observed_model_year_from": int(
                        stock_row["observed_model_year_from"]
                    ),
                    "observed_model_year_to": int(
                        stock_row["observed_model_year_to"]
                    ),
                    "fit_active_stock": stock_row["fit_active_stock"],
                    "source_rows": stock_row["source_rows"],
                    "canonical_make": rating["canonical_make"],
                    "canonical_model": rating["canonical_model"],
                    "nrcan_vehicle_class": rating["Vehicle class"],
                    "nlr_atb_class": rating["nlr_atb_class"],
                    "nrcan_ceud_class": rating["nrcan_ceud_class"],
                    "match_method": method,
                    "model_similarity": model_score,
                    "rating_model_year_from": rating["rating_model_year_from"],
                    "rating_model_year_to": rating["rating_model_year_to"],
                    "overlap_years": overlap_years,
                    "rating_model_labels": rating["rating_model_labels"],
                    "evidence_source": rating["evidence_source"],
                }
            )
        ranked.sort(
            key=lambda row: (
                -float(row["model_similarity"]),
                str(row["canonical_make"]),
                str(row["canonical_model"]),
                str(row["nrcan_vehicle_class"]),
            )
        )
        if not ranked:
            output.append(
                {
                    "mto_make_code": stock_row["MAKE"],
                    "mto_model_code": stock_row["MODEL"],
                    "observed_model_year_from": int(
                        stock_row["observed_model_year_from"]
                    ),
                    "observed_model_year_to": int(
                        stock_row["observed_model_year_to"]
                    ),
                    "fit_active_stock": stock_row["fit_active_stock"],
                    "source_rows": stock_row["source_rows"],
                    "candidate_rank": pd.NA,
                    "candidate_status": "no_make_prefix_match",
                    "canonical_make": pd.NA,
                    "canonical_model": pd.NA,
                    "nrcan_vehicle_class": pd.NA,
                    "nlr_atb_class": pd.NA,
                    "nrcan_ceud_class": pd.NA,
                    "match_method": pd.NA,
                    "model_similarity": pd.NA,
                    "rating_model_year_from": pd.NA,
                    "rating_model_year_to": pd.NA,
                    "overlap_years": 0,
                    "rating_model_labels": pd.NA,
                    "evidence_source": (
                        "NRCan Fuel Consumption Ratings and FuelEconomy.gov"
                    ),
                }
            )
            continue
        top_score = float(ranked[0]["model_similarity"])
        tied_top = sum(
            float(candidate["model_similarity"]) == top_score
            for candidate in ranked
        )
        for rank, candidate in enumerate(
            ranked[:candidates_per_key],
            start=1,
        ):
            candidate["candidate_rank"] = rank
            candidate["candidate_status"] = (
                "ambiguous_top_score"
                if rank == 1 and tied_top > 1
                else "ranked_candidate"
            )
            output.append(candidate)
    candidates = pd.DataFrame(output)
    return candidates.sort_values(
        ["fit_active_stock", "mto_make_code", "mto_model_code", "candidate_rank"],
        ascending=[False, True, True, True],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)


def apply_vehicle_mapping(
    current_stock: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    accepted_statuses: set[str],
) -> pd.DataFrame:
    """Apply accepted MTO make/model mappings selected by stock model year."""
    stock = current_stock.copy()
    stock["_stock_row_id"] = range(len(stock))
    stock["MODEL_YEAR"] = pd.to_numeric(
        stock["MODEL_YEAR"],
        errors="coerce",
    ).astype("Int64")
    crosswalk = mapping.loc[
        mapping["entry_type"].eq("mto_crosswalk")
    ].copy()
    if crosswalk.empty:
        for column in [
            "canonical_make",
            "canonical_model",
            "vehicle_scope",
            "nrcan_vehicle_class",
            "nlr_atb_class",
            "nrcan_ceud_class",
            "match_method",
            "mapping_status",
            "evidence_source",
            "review_notes",
        ]:
            stock[column] = pd.NA
        stock["mapping_status"] = "unmatched"
        stock["mapping_outcome"] = "unmapped"
        stock["mapping_accepted"] = False
        return stock.drop(columns="_stock_row_id")

    joined = stock.merge(
        crosswalk,
        left_on=["MAKE", "MODEL"],
        right_on=["mto_make_code", "mto_model_code"],
        how="left",
        validate="many_to_many",
    )
    for column in ["model_year_from", "model_year_to"]:
        joined[column] = pd.to_numeric(joined[column], errors="coerce").astype(
            "Int64"
        )
    in_range = (
        joined["mto_make_code"].notna()
        & joined["MODEL_YEAR"].ge(joined["model_year_from"])
        & joined["MODEL_YEAR"].le(joined["model_year_to"])
    )
    matched = joined.loc[in_range].copy()
    duplicates = matched.duplicated("_stock_row_id", keep=False)
    if duplicates.any():
        raise ValueError(
            "Reviewed mapping produced multiple model-year matches for one stock row"
        )
    unmatched_ids = stock.loc[
        ~stock["_stock_row_id"].isin(matched["_stock_row_id"]),
        "_stock_row_id",
    ]
    unmatched = stock.loc[stock["_stock_row_id"].isin(unmatched_ids)].copy()
    for column in crosswalk.columns:
        if column not in unmatched:
            unmatched[column] = pd.NA
    joined = pd.concat([matched, unmatched], ignore_index=True, sort=False)
    joined["mapping_status"] = joined["mapping_status"].fillna("unmatched")
    if "vehicle_scope" not in joined:
        joined["vehicle_scope"] = "ldv"
    resolved = joined["mapping_status"].isin(accepted_statuses)
    scope = joined["vehicle_scope"].fillna("")
    joined["mapping_outcome"] = "unmapped"
    joined.loc[resolved & scope.eq("ldv"), "mapping_outcome"] = "mapped_ldv"
    joined.loc[
        resolved & scope.isin({"mhdv", "non_ldv_unclassified"}),
        "mapping_outcome",
    ] = "mapped_non_ldv"
    joined["mapping_accepted"] = joined["mapping_outcome"].eq("mapped_ldv")
    joined.loc[
        joined["mapping_outcome"].eq("mapped_non_ldv"),
        ["nrcan_vehicle_class", "nlr_atb_class", "nrcan_ceud_class"],
    ] = pd.NA
    rejected_class_columns = [
        "canonical_make",
        "canonical_model",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    ]
    joined.loc[joined["mapping_outcome"].eq("unmapped"), rejected_class_columns] = pd.NA
    return (
        joined.sort_values("_stock_row_id", kind="stable")
        .drop(columns="_stock_row_id")
        .reset_index(drop=True)
    )


def mapping_coverage(mapped: pd.DataFrame) -> pd.DataFrame:
    """Report LDV, non-LDV, and unresolved fleet coverage."""
    frame = mapped.copy()
    frame["FIT_ACTIVE"] = pd.to_numeric(frame["FIT_ACTIVE"], errors="coerce")
    groups = [
        (str(vehicle_class), rows)
        for vehicle_class, rows in frame.groupby("VEHICLE_CLASS", dropna=False)
    ]
    groups.append(("ALL", frame))
    output: list[dict[str, Any]] = []
    for vehicle_class, rows in groups:
        rows_total = len(rows)
        stock_total = rows["FIT_ACTIVE"].sum(min_count=1)
        values = {
            "rows": rows_total,
            "fit_active_stock": stock_total,
        }
        for measure, total_value in values.items():
            category_values: dict[str, float | int] = {}
            for category in ["mapped_ldv", "mapped_non_ldv", "unmapped"]:
                selected = rows["mapping_outcome"].eq(category)
                category_values[category] = (
                    int(selected.sum())
                    if measure == "rows"
                    else rows.loc[selected, "FIT_ACTIVE"].sum()
                )
            output.append(
                {
                    "scope": "latest_fit_active_retained_stock",
                    "vehicle_class": vehicle_class,
                    "measure": measure,
                    **category_values,
                    "total": total_value,
                    "mapped_ldv_share": category_values["mapped_ldv"] / total_value
                    if total_value
                    else pd.NA,
                    "mapped_non_ldv_share": category_values["mapped_non_ldv"]
                    / total_value
                    if total_value
                    else pd.NA,
                    "unmapped_share": category_values["unmapped"] / total_value
                    if total_value
                    else pd.NA,
                }
            )
    return pd.DataFrame(output)


def unresolved_mapping_reasons(
    mapped: pd.DataFrame,
    candidates: pd.DataFrame,
    mapping: pd.DataFrame,
    manual_evidence: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attribute unresolved stock to auditable evidence limitations."""
    outcomes = mapped.get(
        "mapping_outcome",
        pd.Series(
            pd.NA,
            index=mapped.index,
            dtype="string",
        ),
    ).fillna(
        mapped.get("mapping_accepted", pd.Series(False, index=mapped.index)).map(
            {True: "mapped_ldv", False: "unmapped"}
        )
    )
    unresolved = mapped.loc[outcomes.eq("unmapped")].copy()
    unresolved["FIT_ACTIVE"] = pd.to_numeric(
        unresolved["FIT_ACTIVE"],
        errors="coerce",
    ).fillna(0)
    if unresolved.empty:
        return unresolved, pd.DataFrame(
            columns=[
                "unresolved_reason",
                "unresolved_rows",
                "unresolved_fit_active_stock",
                "share_of_unresolved_rows",
                "share_of_unresolved_stock",
                "share_of_total_rows",
                "share_of_total_stock",
            ]
        )

    ranked = candidates.copy()
    if "rating_model_labels" not in ranked.columns:
        ranked["rating_model_labels"] = pd.NA
    ranked["_candidate_rank"] = pd.to_numeric(
        ranked["candidate_rank"],
        errors="coerce",
    ).fillna(float("inf"))
    top = (
        ranked.sort_values(
            [
                "mto_make_code",
                "mto_model_code",
                "_candidate_rank",
            ],
            kind="stable",
        )
        .groupby(
            ["mto_make_code", "mto_model_code"],
            as_index=False,
            dropna=False,
        )
        .head(1)
        .loc[
            :,
            [
                "mto_make_code",
                "mto_model_code",
                "candidate_status",
                "canonical_make",
                "canonical_model",
                "nrcan_vehicle_class",
                "nlr_atb_class",
                "match_method",
                "model_similarity",
                "rating_model_year_from",
                "rating_model_year_to",
                "overlap_years",
                "rating_model_labels",
            ],
        ]
        .rename(
            columns={
                "canonical_make": "candidate_canonical_make",
                "canonical_model": "candidate_canonical_model",
                "nrcan_vehicle_class": "candidate_nrcan_vehicle_class",
                "nlr_atb_class": "candidate_nlr_atb_class",
                "match_method": "candidate_match_method",
                "model_similarity": "candidate_model_similarity",
            }
        )
    )
    unresolved = unresolved.merge(
        top,
        left_on=["MAKE", "MODEL"],
        right_on=["mto_make_code", "mto_model_code"],
        how="left",
        validate="many_to_one",
    )
    manual_columns = [
        "mto_make_code",
        "mto_model_code",
        "canonical_make",
        "candidate_pass_agreement",
        "agreed_model_candidate",
        "highest_confidence_candidate",
        "vpic_second_pass_candidate",
    ]
    if manual_evidence is not None and not manual_evidence.empty:
        manual = manual_evidence.loc[:, manual_columns].rename(
            columns={"canonical_make": "manual_canonical_make"}
        )
        unresolved = unresolved.merge(
            manual,
            left_on=["MAKE", "MODEL"],
            right_on=["mto_make_code", "mto_model_code"],
            how="left",
            validate="many_to_one",
            suffixes=("", "_manual"),
        )
    else:
        for column in manual_columns[2:]:
            output_column = (
                "manual_canonical_make" if column == "canonical_make" else column
            )
            unresolved[output_column] = pd.NA
    make_text = unresolved["MAKE"].fillna("").astype(str).str.strip().str.upper()
    model_text = unresolved["MODEL"].fillna("").astype(str).str.strip().str.upper()
    suppressed = (
        make_text.eq("")
        | model_text.eq("")
        | make_text.str.fullmatch(r"\*+|UNKNOWN|UNK|N/A", na=False)
        | model_text.str.fullmatch(r"\*+|UNKNOWN|UNK|N/A", na=False)
    )

    unresolved["unresolved_reason"] = ""
    unresolved["unresolved_reason_detail"] = ""

    def assign(mask: pd.Series, reason: str, detail: str) -> None:
        selected = unresolved["unresolved_reason"].eq("") & mask.fillna(False)
        unresolved.loc[selected, "unresolved_reason"] = reason
        unresolved.loc[selected, "unresolved_reason_detail"] = detail

    assign(
        suppressed,
        "suppressed_or_unknown_code",
        "MTO make or model is blank, suppressed, or explicitly unknown.",
    )
    usable_manual_agreement = unresolved["candidate_pass_agreement"].eq(
        "agreement"
    ) & ~unresolved["agreed_model_candidate"].map(is_unresolved_vehicle_label)
    assign(
        usable_manual_agreement,
        "weak_model_label_agreement",
        "The two manual passes agree on a model family, but the promotion hierarchy did not produce an accepted mapping for this row.",
    )
    assign(
        unresolved["candidate_pass_agreement"].notna(),
        "ambiguous_top_candidate",
        "Manual candidates remain unresolved or conflict with the pipeline family; no family is promoted.",
    )
    assign(
        unresolved["candidate_status"].eq("no_make_prefix_match")
        | unresolved["candidate_canonical_make"].isna(),
        "no_normalized_make_agreement",
        "No configured alias or normalized make-prefix agreement exists in the Ratings evidence.",
    )
    assign(
        unresolved["candidate_status"].eq("ambiguous_top_score"),
        "ambiguous_top_candidate",
        "Multiple top-scoring labels or class interpretations remain; no class is accepted automatically.",
    )
    assign(
        unresolved["candidate_match_method"].isin(
            ["exact_normalized", "normalized_prefix"]
        ),
        "unreviewed_high_confidence_candidate",
        "A strong model-label candidate exists, but it is not yet an accepted reviewed MTO make/model mapping.",
    )
    assign(
        unresolved["candidate_match_method"].isin(
            ["normalized_substring", "string_similarity"]
        ),
        "weak_model_label_agreement",
        "Only substring or general string-similarity evidence exists; this is insufficient for reliable acceptance.",
    )
    assign(
        unresolved["unresolved_reason"].eq(""),
        "no_model_label_candidate",
        "A normalized make exists, but the evidence catalogue produced no model-family candidate for this MTO label.",
    )

    summary = (
        unresolved.groupby(
            ["VEHICLE_CLASS", "unresolved_reason"],
            as_index=False,
        )
        .agg(
            unresolved_rows=("FIT_ACTIVE", "size"),
            unresolved_fit_active_stock=("FIT_ACTIVE", "sum"),
        )
        .sort_values(
            "unresolved_fit_active_stock",
            ascending=False,
            kind="stable",
        )
    )
    unresolved_rows = summary.groupby("VEHICLE_CLASS")[
        "unresolved_rows"
    ].transform("sum")
    unresolved_stock = summary.groupby("VEHICLE_CLASS")[
        "unresolved_fit_active_stock"
    ].transform("sum")
    total_by_class = mapped.assign(
        FIT_ACTIVE=pd.to_numeric(mapped["FIT_ACTIVE"], errors="coerce")
    ).groupby("VEHICLE_CLASS").agg(
        total_rows=("FIT_ACTIVE", "size"),
        total_stock=("FIT_ACTIVE", "sum"),
    )
    summary = summary.merge(
        total_by_class,
        left_on="VEHICLE_CLASS",
        right_index=True,
        how="left",
        validate="many_to_one",
    )
    summary["share_of_unresolved_rows"] = summary["unresolved_rows"] / unresolved_rows
    summary["share_of_unresolved_stock"] = (
        summary["unresolved_fit_active_stock"] / unresolved_stock
    )
    summary["share_of_total_rows"] = summary["unresolved_rows"] / summary["total_rows"]
    summary["share_of_total_stock"] = (
        summary["unresolved_fit_active_stock"] / summary["total_stock"]
    )
    summary = summary.drop(columns=["total_rows", "total_stock"])
    if "rating_model_labels" not in unresolved.columns:
        unresolved["rating_model_labels"] = pd.NA
    detail_columns = [
        "report_year",
        "VEHICLE_CLASS",
        "MAKE",
        "MODEL",
        "MODEL_YEAR",
        "FIT_ACTIVE",
        "unresolved_reason",
        "unresolved_reason_detail",
        "candidate_status",
        "candidate_canonical_make",
        "candidate_canonical_model",
        "candidate_nrcan_vehicle_class",
        "candidate_nlr_atb_class",
        "candidate_match_method",
        "candidate_model_similarity",
        "rating_model_year_from",
        "rating_model_year_to",
        "overlap_years",
        "rating_model_labels",
        "manual_canonical_make",
        "candidate_pass_agreement",
        "agreed_model_candidate",
        "highest_confidence_candidate",
        "vpic_second_pass_candidate",
    ]
    detail = unresolved.loc[:, detail_columns].sort_values(
        "FIT_ACTIVE",
        ascending=False,
        kind="stable",
    )
    return detail.reset_index(drop=True), summary.reset_index(drop=True)


def latest_unresolved_key_worklist(
    unresolved_detail: pd.DataFrame,
) -> pd.DataFrame:
    """Publish one prioritized row per nonsuppressed latest MTO key."""
    columns = [
        "VEHICLE_CLASS",
        "MAKE",
        "MODEL",
        "latest_report_year",
        "fit_active_stock",
        "model_year_from",
        "model_year_to",
        "observed_model_years",
        "model_year_list",
        "unresolved_reason",
        "unresolved_reason_detail",
        "candidate_canonical_make",
        "candidate_canonical_model",
        "candidate_nrcan_vehicle_class",
        "candidate_nlr_atb_class",
        "candidate_match_method",
        "candidate_model_similarity",
        "rating_model_year_from",
        "rating_model_year_to",
        "overlap_years",
        "rating_model_labels",
    ]
    if unresolved_detail.empty:
        return pd.DataFrame(columns=columns)
    detail = unresolved_detail.loc[
        ~unresolved_detail["unresolved_reason"].eq(
            "suppressed_or_unknown_code"
        )
    ].copy()
    if detail.empty:
        return pd.DataFrame(columns=columns)
    detail["FIT_ACTIVE"] = pd.to_numeric(
        detail["FIT_ACTIVE"], errors="coerce"
    ).fillna(0)
    detail["MODEL_YEAR"] = pd.to_numeric(
        detail["MODEL_YEAR"], errors="coerce"
    ).astype("Int64")
    detail["report_year"] = pd.to_numeric(
        detail["report_year"], errors="coerce"
    ).astype("Int64")

    def first_present(values: pd.Series) -> object:
        present = values.dropna()
        present = present.loc[present.astype(str).str.strip().ne("")]
        return present.iloc[0] if len(present) else pd.NA

    worklist = (
        detail.groupby(["VEHICLE_CLASS", "MAKE", "MODEL"], as_index=False)
        .agg(
            latest_report_year=("report_year", "max"),
            fit_active_stock=("FIT_ACTIVE", "sum"),
            model_year_from=("MODEL_YEAR", "min"),
            model_year_to=("MODEL_YEAR", "max"),
            observed_model_years=("MODEL_YEAR", "nunique"),
            model_year_list=(
                "MODEL_YEAR",
                lambda values: "["
                + ", ".join(
                    map(
                        str,
                        sorted(
                            set(
                                pd.to_numeric(values, errors="coerce")
                                .dropna()
                                .astype(int)
                            )
                        ),
                    )
                )
                + "]",
            ),
            unresolved_reason=("unresolved_reason", first_present),
            unresolved_reason_detail=(
                "unresolved_reason_detail",
                first_present,
            ),
            candidate_canonical_make=(
                "candidate_canonical_make",
                first_present,
            ),
            candidate_canonical_model=(
                "candidate_canonical_model",
                first_present,
            ),
            candidate_nrcan_vehicle_class=(
                "candidate_nrcan_vehicle_class",
                first_present,
            ),
            candidate_nlr_atb_class=(
                "candidate_nlr_atb_class",
                first_present,
            ),
            candidate_match_method=("candidate_match_method", first_present),
            candidate_model_similarity=(
                "candidate_model_similarity",
                "max",
            ),
            rating_model_year_from=("rating_model_year_from", first_present),
            rating_model_year_to=("rating_model_year_to", first_present),
            overlap_years=("overlap_years", first_present),
            rating_model_labels=("rating_model_labels", first_present),
        )
        .sort_values(
            ["fit_active_stock", "VEHICLE_CLASS", "MAKE", "MODEL"],
            ascending=[False, True, True, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    return worklist.loc[:, columns]


def derive_aggregation_weights(
    mapped: pd.DataFrame,
    *,
    vintage_bin_years: int = 5,
    vintage_alignment: str = "ceiling",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive NRCan and NLR class weights from accepted fit-active stock."""
    accepted = mapped.loc[mapped["mapping_accepted"]].copy()
    if not accepted.empty:
        latest_report_year = pd.to_numeric(
            accepted["report_year"], errors="coerce"
        ).max()
        accepted = accepted.loc[
            pd.to_numeric(accepted["report_year"], errors="coerce").eq(
                latest_report_year
            )
        ].copy()
    accepted["FIT_ACTIVE"] = pd.to_numeric(
        accepted["FIT_ACTIVE"],
        errors="coerce",
    )

    if vintage_bin_years <= 0:
        raise ValueError("vintage_bin_years must be positive")
    if vintage_alignment != "ceiling":
        raise ValueError(f"Unsupported vintage alignment: {vintage_alignment}")
    accepted["vintage"] = (
        (
            pd.to_numeric(accepted["MODEL_YEAR"], errors="coerce")
            + vintage_bin_years
            - 1
        )
        // vintage_bin_years
        * vintage_bin_years
    ).astype("Int64")

    def _weights(class_column: str) -> pd.DataFrame:
        columns = [
            "report_year",
            "weight_basis",
            "model_year",
            "vintage",
            "nrcan_ceud_class",
            class_column,
            "fit_active_stock",
            "aggregation_weight",
        ]
        if accepted.empty:
            return pd.DataFrame(columns=columns)
        annual = (
            accepted.groupby(
                [
                    "report_year",
                    "MODEL_YEAR",
                    "nrcan_ceud_class",
                    class_column,
                ],
                dropna=False,
                as_index=False,
            )["FIT_ACTIVE"]
            .sum(min_count=1)
            .rename(
                columns={
                    "MODEL_YEAR": "model_year",
                    "FIT_ACTIVE": "fit_active_stock",
                }
            )
        )
        annual.insert(1, "weight_basis", "model_year")
        annual.insert(3, "vintage", pd.NA)
        vintage = (
            accepted.groupby(
                ["report_year", "vintage", "nrcan_ceud_class", class_column],
                dropna=False,
                as_index=False,
            )["FIT_ACTIVE"]
            .sum(min_count=1)
            .rename(columns={"FIT_ACTIVE": "fit_active_stock"})
        )
        vintage.insert(1, "weight_basis", "five_year_vintage")
        vintage.insert(2, "model_year", pd.NA)
        pooled = (
            accepted.groupby(
                ["report_year", "nrcan_ceud_class", class_column],
                dropna=False,
                as_index=False,
            )["FIT_ACTIVE"]
            .sum(min_count=1)
            .rename(columns={"FIT_ACTIVE": "fit_active_stock"})
        )
        pooled.insert(1, "weight_basis", "all_vintages")
        pooled.insert(2, "model_year", pd.NA)
        pooled.insert(3, "vintage", pd.NA)
        weights = pd.concat([annual, vintage, pooled], ignore_index=True)
        totals = weights.groupby(
            [
                "report_year",
                "weight_basis",
                "model_year",
                "vintage",
                "nrcan_ceud_class",
            ],
            dropna=False,
        )["fit_active_stock"].transform("sum")
        weights["aggregation_weight"] = weights["fit_active_stock"] / totals
        return weights.loc[:, columns].sort_values(
            [
                "report_year",
                "weight_basis",
                "model_year",
                "vintage",
                "nrcan_ceud_class",
                class_column,
            ],
            kind="stable",
            na_position="last",
        )

    return _weights("nrcan_vehicle_class"), _weights("nlr_atb_class")


def derive_fleet_composition_weights(
    mapped: pd.DataFrame,
    *,
    minimum_model_year: int = 2000,
) -> pd.DataFrame:
    """Derive latest-snapshot fit-active weights at the full class-age grain."""
    columns = [
        "report_year",
        "vehicle_class",
        "model_year",
        "age",
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
        "fit_active_stock",
        "share_of_mapped_fleet",
        "share_within_vehicle_class_age",
        "age_share_within_nrcan_class",
        "age_share_within_nlr_class",
        "age_share_within_ceud_class",
    ]
    accepted = mapped.loc[mapped["mapping_accepted"]].copy()
    if accepted.empty:
        return pd.DataFrame(columns=columns)
    accepted["report_year"] = pd.to_numeric(
        accepted["report_year"], errors="coerce"
    )
    accepted["MODEL_YEAR"] = pd.to_numeric(
        accepted["MODEL_YEAR"], errors="coerce"
    )
    accepted["FIT_ACTIVE"] = pd.to_numeric(
        accepted["FIT_ACTIVE"], errors="coerce"
    )
    latest_report_year = int(accepted["report_year"].max())
    accepted = accepted.loc[
        accepted["report_year"].eq(latest_report_year)
        & accepted["MODEL_YEAR"].ge(minimum_model_year)
        & accepted["FIT_ACTIVE"].gt(0)
    ].copy()
    if accepted.empty:
        return pd.DataFrame(columns=columns)
    accepted["age"] = accepted["report_year"] - accepted["MODEL_YEAR"]
    weights = (
        accepted.groupby(
            [
                "report_year",
                "VEHICLE_CLASS",
                "MODEL_YEAR",
                "age",
                "nrcan_vehicle_class",
                "nlr_atb_class",
                "nrcan_ceud_class",
            ],
            as_index=False,
            dropna=False,
        )["FIT_ACTIVE"]
        .sum(min_count=1)
        .rename(
            columns={
                "VEHICLE_CLASS": "vehicle_class",
                "MODEL_YEAR": "model_year",
                "FIT_ACTIVE": "fit_active_stock",
            }
        )
    )
    weights["share_of_mapped_fleet"] = (
        weights["fit_active_stock"] / weights["fit_active_stock"].sum()
    )
    weights["share_within_vehicle_class_age"] = weights[
        "fit_active_stock"
    ] / weights.groupby(
        ["vehicle_class", "age"], dropna=False
    )["fit_active_stock"].transform("sum")
    for class_column, output_column in [
        ("nrcan_vehicle_class", "age_share_within_nrcan_class"),
        ("nlr_atb_class", "age_share_within_nlr_class"),
        ("nrcan_ceud_class", "age_share_within_ceud_class"),
    ]:
        weights[output_column] = weights["fit_active_stock"] / weights.groupby(
            ["vehicle_class", class_column], dropna=False
        )["fit_active_stock"].transform("sum")
    return weights.loc[:, columns].sort_values(
        ["vehicle_class", "age", "nrcan_vehicle_class", "nlr_atb_class"],
        kind="stable",
    ).reset_index(drop=True)


def load_wards_legacy_comparison(bundle: ConfigBundle) -> pd.DataFrame:
    """Load complete Wards class shares for non-Ontario and legacy comparison."""
    source = bundle.sources["sources"][WARDS_SOURCE_ID]
    component = source.component("vehicle_class_market_shares")
    path = resolve_input_path(
        bundle,
        "manual",
        component.adapter["manual_parameter_path"],
    )
    wards = pd.read_csv(path)
    wards.insert(0, "region_scope", "outside_ontario_or_legacy_comparison")
    wards.insert(1, "weight_source", WARDS_SOURCE_ID)
    return wards


def build_road_aggregation_artifacts(
    scenario_path: str | Path,
) -> Path:
    """Generate candidates, apply reviewed mappings, and publish weights."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    output_dir = resolve_input_path(
        bundle,
        "interim",
        ontario_rules["interim_subdir"],
    )
    current_path = output_dir / str(ontario_rules["current_stock_file"])
    if not current_path.is_file():
        raise FileNotFoundError(current_path)
    current_stock = pd.read_csv(current_path, low_memory=False)

    mapping_path = resolve_parameter_path(
        bundle,
        rules["vehicle_size_class_map_file"],
    )
    mapping = pd.read_csv(mapping_path, dtype=str, keep_default_na=False)
    validated_mapping = validate_vehicle_mapping(
        mapping,
        rules=rules,
        rating_class_rules=rating_rules["vehicle_class_harmonization"],
    )
    ratings = load_rating_evidence(bundle, rules=rules)
    candidates = generate_mapping_candidates(
        current_stock,
        ratings,
        canonical_aliases={
            str(source): str(target)
            for source, target in rules.get("canonical_aliases", {}).items()
        },
    )
    mapped = apply_vehicle_mapping(
        current_stock,
        validated_mapping,
        accepted_statuses={
            str(status)
            for status in rules["accepted_mapping_statuses"]
        },
    )
    coverage = mapping_coverage(mapped)
    unresolved_detail, unresolved_summary = unresolved_mapping_reasons(
        mapped,
        candidates,
        validated_mapping,
        pd.read_csv(
            resolve_input_path(
                bundle,
                "manual",
                str(rules["mapping_bootstrap"]["manual_evidence_file"]),
            ),
            dtype=str,
            keep_default_na=False,
        ),
    )
    unresolved_worklist = latest_unresolved_key_worklist(unresolved_detail)
    unresolved = (
        mapped.loc[mapped["mapping_outcome"].eq("unmapped")]
        .sort_values("FIT_ACTIVE", ascending=False, kind="stable")
        .head(int(rules["highest_stock_unresolved_limit"]))
    )
    nrcan_weights, nlr_weights = derive_aggregation_weights(
        mapped,
        vintage_bin_years=int(rules["vintage_bin_years"]),
        vintage_alignment=str(rules["vintage_alignment"]),
    )
    fleet_weights = derive_fleet_composition_weights(
        mapped,
        minimum_model_year=int(
            rules["mapping_bootstrap"]["minimum_model_year"]
        ),
    )
    evidence_catalog = build_rating_model_catalog(
        ratings,
        mapping_columns=[str(column) for column in rules["mapping_columns"]],
    )
    wards = load_wards_legacy_comparison(bundle)
    top = (
        mapped.loc[mapped["mapping_accepted"]]
        .groupby(
            [
                "report_year",
                "VEHICLE_CLASS",
                "canonical_make",
                "canonical_model",
                "MODEL_YEAR",
                "nrcan_vehicle_class",
                "nlr_atb_class",
                "nrcan_ceud_class",
            ],
            as_index=False,
            dropna=False,
        )["FIT_ACTIVE"]
        .sum(min_count=1)
        .sort_values(
            ["nrcan_ceud_class", "FIT_ACTIVE"],
            ascending=[True, False],
            kind="stable",
        )
        .groupby("nrcan_ceud_class", group_keys=False)
        .head(int(rules["top_observations_per_ceud_class"]))
    )

    outputs = {
        rules["candidate_file"]: candidates,
        rules["coverage_file"]: coverage,
        rules["unresolved_file"]: unresolved,
        rules["unresolved_reason_detail_file"]: unresolved_detail,
        rules["unresolved_reason_summary_file"]: unresolved_summary,
        rules["latest_unresolved_worklist_file"]: unresolved_worklist,
        rules["mapped_current_stock_file"]: mapped,
        rules["nrcan_weights_file"]: nrcan_weights,
        rules["nlr_weights_file"]: nlr_weights,
        rules["fleet_composition_weights_file"]: fleet_weights,
        rules["vehicle_class_evidence_file"]: evidence_catalog,
        rules["wards_comparison_file"]: wards,
        rules["top_observations_file"]: top,
    }
    for filename, frame in outputs.items():
        write_dataframe_atomic(frame, output_dir / str(filename))
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    output_dir = build_road_aggregation_artifacts(args.scenario)
    logging.info("Wrote Ontario road-aggregation artifacts to %s", output_dir)


if __name__ == "__main__":
    main()
