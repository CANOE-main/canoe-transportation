"""Apply reviewed LDV mappings and derive Ontario Report A aggregation weights."""

import argparse
import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

from parameterization.vehicle_reference import load_vehicle_reference, reference_mto_mapping, resolve_source_labels

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
)
from utils.vehicle_labels import is_unresolved_vehicle_label, normalize_vehicle_text


ONTARIO_RULE_KEY = "ontario_vehicle_population"
ROAD_RULE_KEY = "road_aggregation"
RATINGS_RULE_KEY = "nrcan_fuel_consumption_ratings"
FUELECONOMY_RULE_KEY = "fueleconomy_vehicle_data"
WARDS_SOURCE_ID = "wards_intelligence_sales_shares"


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load road-aggregation rules."""
    return load_harmonization_rules(bundle, ROAD_RULE_KEY)


def nrcan_to_nlr_map(class_rules: dict[str, Any]) -> dict[str, str]:
    """Invert the existing NRCan-to-NLR harmonization after uniqueness checks."""
    if class_rules["unmapped_policy"] != "error":
        raise ValueError("Unsupported NRCan class unmapped_policy; expected error")
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


def ceud_class_for_nlr(nlr_class: str, class_rules: dict[str, Any]) -> str:
    """Map the established five NLR LDV classes to NRCan CEUD classes."""
    try:
        return str(class_rules["target_to_ceud"][nlr_class])
    except KeyError as exc:
        raise ValueError(f"Unsupported NLR LDV class: {nlr_class}") from exc


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
        for value in rating_rules["vehicle_class_harmonization"]["unresolved_nrcan_classes"]
    }
    unexpected = sorted(set(unresolved) - configured_unresolved)
    if unexpected:
        raise ValueError(
            "NRCan rating evidence has unexpected unmapped classes: "
            + ", ".join(unexpected)
        )
    evidence = evidence.dropna(subset=["nlr_atb_class"]).copy()
    evidence["nrcan_ceud_class"] = evidence["nlr_atb_class"].map(
        lambda value: ceud_class_for_nlr(value, rating_rules["vehicle_class_harmonization"])
    )
    evidence = evidence.drop_duplicates(
        ["Model year", "Make", "Model", "Vehicle class", "evidence_source"]
    ).reset_index(drop=True)
    reference = load_vehicle_reference(bundle)
    composition_rules = load_harmonization_rules(bundle, "efficiencies")["ratings"]["composition"]
    linked = []
    for source, group in evidence.groupby("evidence_source", sort=False):
        resolved = resolve_source_labels(
            group, reference, source=source, make_column="Make", model_column="Model",
            year_column="Model year", class_column="Vehicle class",
            require_registered=source == "nrcan_fuel_consumption_ratings",
        )
        # Unreviewed auxiliary labels remain candidate-discovery evidence. Generic
        # trim parsing does not attach an accepted canonical ID or approve a link.
        pending = resolved.reference_link_status.eq("no_applicable_source_link")
        if pending.any():
            from parameterization.vehicle_reference import catalogue_family_labels

            labels = catalogue_family_labels(
                resolved.loc[pending].drop(columns=["family_make", "family_model", "family_key"]),
                composition_rules,
            )
            for field in ("family_make", "family_model", "family_key"):
                resolved.loc[pending, field] = labels[field]
            resolved.loc[pending, "reference_link_status"] = "unreviewed_auxiliary_label"
        resolved["canonical_make"] = resolved.family_make
        resolved["canonical_model"] = resolved.family_model
        linked.append(resolved)
    return assign_rating_model_families(pd.concat(linked, ignore_index=True))


def assign_rating_model_families(
    evidence: pd.DataFrame, *, composition_rules: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Use the production canonical identity policy without inferring family roots.

    Already labelled evidence may be reused by pure candidate helpers. Unlabelled
    source rows require the caller's policy rather than a different fallback heuristic.
    """
    required = {"Model year", "Make", "Model", "Vehicle class", "nlr_atb_class", "nrcan_ceud_class"}
    if missing := required - set(evidence):
        raise ValueError("Ratings family evidence missing columns: " + ", ".join(sorted(missing)))
    enriched = evidence.copy()
    if "evidence_source" not in enriched:
        enriched["evidence_source"] = "nrcan_fuel_consumption_ratings"
    if {"canonical_make", "canonical_model"} <= set(enriched):
        pass
    elif composition_rules is not None:
        from parameterization.vehicle_reference import catalogue_family_labels

        labels = catalogue_family_labels(enriched, composition_rules)
        enriched["canonical_make"] = labels.family_make
        enriched["canonical_model"] = labels.family_model
    elif not {"canonical_make", "canonical_model"} <= set(enriched):
        raise ValueError("Unlabelled Ratings evidence requires canonical composition_rules")
    enriched["normalized_make"] = enriched["Make"].map(normalize_vehicle_text)
    enriched["normalized_model"] = enriched["Model"].map(normalize_vehicle_text)
    enriched["normalized_canonical_model"] = enriched["canonical_model"].map(normalize_vehicle_text)
    # Case-only source spellings are one identity, with a stable readable label.
    keys = ["normalized_make", "normalized_canonical_model"]
    labels = (enriched[keys + ["canonical_make", "canonical_model"]].drop_duplicates()
              .sort_values([*keys, "canonical_make", "canonical_model"], kind="stable")
              .drop_duplicates(keys).set_index(keys))
    normalized = pd.MultiIndex.from_frame(enriched[keys])
    enriched["canonical_make"] = labels.canonical_make.reindex(normalized).to_numpy()
    enriched["canonical_model"] = labels.canonical_model.reindex(normalized).to_numpy()
    return enriched


def build_rating_model_catalog(
    rating_evidence: pd.DataFrame, *, mapping_columns: list[str],
    composition_rules: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Summarize exact contiguous public-evidence years using canonical identities.

    EPA-only class evidence is retained as such; it does not establish NRCan rating
    availability. Missing years and source-coverage changes split intervals.
    """
    evidence = assign_rating_model_families(rating_evidence, composition_rules=composition_rules)
    identities = ["canonical_make", "canonical_model", "Vehicle class", "nlr_atb_class", "nrcan_ceud_class"]
    annual = evidence.groupby([*identities, "Model year"], as_index=False, dropna=False).agg(
        supporting_rating_rows=("Model", "size"),
        evidence_source=("evidence_source", lambda values: " | ".join(sorted(set(map(str, values))))),
    )
    records = []
    for _, group in annual.groupby(identities, sort=True, dropna=False):
        current = None
        for row in group.sort_values("Model year").to_dict("records"):
            year = int(row["Model year"])
            if current is not None and year == current["model_year_to"] + 1 and row["evidence_source"] == current["evidence_source"]:
                current["model_year_to"] = year
                current["supporting_rating_rows"] += int(row["supporting_rating_rows"])
                continue
            if current is not None:
                records.append(current)
            current = {field: row[field] for field in identities}
            nrcan = "nrcan_fuel_consumption_ratings" in row["evidence_source"].split(" | ")
            current.update(
                entry_type="ratings_catalog", mto_make_code="", mto_model_code="",
                model_year_from=year, model_year_to=year, vehicle_scope="ldv",
                match_method="configured_canonical_identity", mapping_status="evidence_only",
                supporting_rating_rows=int(row["supporting_rating_rows"]), evidence_source=row["evidence_source"],
                review_notes=("Exact contiguous NRCan rating years; identity/class evidence only."
                              if nrcan else "EPA-only identity/class evidence; no NRCan rating availability claim."),
            )
        if current is not None:
            records.append(current)
    result = pd.DataFrame(records).rename(columns={"Vehicle class": "nrcan_vehicle_class"})
    if result.empty:
        return pd.DataFrame(columns=mapping_columns)
    return result.loc[:, mapping_columns].sort_values(
        ["canonical_make", "canonical_model", "nrcan_vehicle_class", "model_year_from"], kind="stable",
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
            "Internal reviewed vehicle mapping columns differ from the validated schema: "
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
    identity_unresolved = validated["vehicle_scope"].eq("identity_unresolved")
    for column in ["canonical_make", "canonical_model"]:
        validated[column] = validated[column].fillna("").astype(str).str.strip()
    if ((validated.canonical_make.eq("") | validated.canonical_model.eq("")) & ~identity_unresolved).any():
        raise ValueError("Reviewed mapping rows require nonblank canonical make/model identities unless identity is explicitly unresolved")
    if (identity_unresolved & validated.canonical_model.ne("")).any():
        raise ValueError("Unresolved model identity must leave canonical_model blank")
    for column in ["model_year_from", "model_year_to"]:
        years = pd.to_numeric(
            validated[column],
            errors="raise",
        )
        if years.isna().any() or years.mod(1).ne(0).any():
            raise ValueError("Reviewed mapping model-year bounds must be finite integers")
        validated[column] = years.astype(int)
    invalid_ranges = validated["model_year_from"] > validated["model_year_to"]
    if invalid_ranges.any():
        raise ValueError("Reviewed mapping has model_year_from after model_year_to")

    allowed_scopes = {
        "ldv", "mhdv", "non_ldv_unclassified", "ldv_class_unresolved", "scope_unresolved", "identity_unresolved",
    }
    validated["vehicle_scope"] = validated["vehicle_scope"].fillna("").astype(str)
    unexpected_scopes = sorted(set(validated["vehicle_scope"]) - allowed_scopes)
    if unexpected_scopes:
        raise ValueError(
            "Reviewed mapping has unsupported vehicle_scope values: "
            + ", ".join(unexpected_scopes)
        )
    if ~validated.loc[catalog, "vehicle_scope"].eq("ldv").all():
        raise ValueError("Ratings catalogue rows must have vehicle_scope='ldv'")
    non_ldv = ~validated["vehicle_scope"].eq("ldv")
    hierarchy_columns = [
        "nrcan_vehicle_class",
        "nlr_atb_class",
        "nrcan_ceud_class",
    ]
    if validated.loc[non_ldv, hierarchy_columns].ne("").any().any():
        raise ValueError("Non-LDV or unresolved-class mapping rows must leave the LDV hierarchy blank")

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
        expected_ceud = ceud_class_for_nlr(expected_nlr, rating_class_rules)
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


def _model_similarity(
    mto_model: str,
    canonical_model: str,
    *,
    scores: dict[str, float],
) -> tuple[float, str]:
    if mto_model == canonical_model:
        return float(scores["exact_normalized"]), "exact_normalized"
    if canonical_model.startswith(mto_model) or mto_model.startswith(canonical_model):
        return float(scores["normalized_prefix"]), "normalized_prefix"
    if mto_model and mto_model in canonical_model:
        return float(scores["normalized_substring"]), "normalized_substring"
    return (
        SequenceMatcher(None, mto_model, canonical_model).ratio(),
        "string_similarity",
    )


def generate_mapping_candidates(
    current_stock: pd.DataFrame,
    rating_evidence: pd.DataFrame,
    *,
    candidates_per_key: int,
    similarity_scores: dict[str, float],
    canonical_aliases: dict[str, str] | None = None,
    composition_rules: dict[str, Any] | None = None,
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
    rating_evidence = assign_rating_model_families(rating_evidence, composition_rules=composition_rules)
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
                scores=similarity_scores,
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
    """Apply only year-applicable reviewed identities, preserving their native keys."""
    stock = current_stock.copy()
    stock["_stock_row_id"] = range(len(stock))
    stock["_reviewed_make_code"] = stock["MAKE"].astype(str).str.strip().str.upper()
    stock["_reviewed_model_code"] = stock["MODEL"].astype(str).str.strip().str.upper()
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
        stock["identity_authority"] = ""
        stock["identity_supported"] = False
        stock["raw_canonical_make"] = stock.canonical_make
        stock["raw_canonical_model"] = stock.canonical_model
        return stock.drop(columns=["_stock_row_id", "_reviewed_make_code", "_reviewed_model_code"])

    joined = stock.merge(
        crosswalk,
        left_on=["_reviewed_make_code", "_reviewed_model_code"],
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
    mapping_floor = int(pd.to_numeric(crosswalk["model_year_from"]).min())
    outside_scope = joined["MODEL_YEAR"].lt(mapping_floor).fillna(False)
    joined.loc[outside_scope, "mapping_outcome"] = "out_of_scope"
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
    unresolved_scope = resolved & scope.isin({"ldv_class_unresolved", "scope_unresolved", "identity_unresolved"})
    joined.loc[
        joined["mapping_outcome"].isin({"unmapped", "out_of_scope"}),
        ["nrcan_vehicle_class", "nlr_atb_class", "nrcan_ceud_class"],
    ] = pd.NA
    joined.loc[
        joined["mapping_outcome"].isin({"unmapped", "out_of_scope"}) & ~unresolved_scope,
        ["canonical_make", "canonical_model"],
    ] = pd.NA
    # The reviewed crosswalk owns positive and explicitly unresolved identity
    # decisions; a historical candidate method is not an acceptance gate.
    joined["identity_authority"] = ""
    joined.loc[resolved, "identity_authority"] = "reviewed_vehicle_reference"
    joined["identity_supported"] = resolved & ~scope.eq("identity_unresolved")
    joined["raw_canonical_make"] = joined.canonical_make
    joined["raw_canonical_model"] = joined.canonical_model
    return (
        joined.sort_values("_stock_row_id", kind="stable")
        .drop(columns=["_stock_row_id", "_reviewed_make_code", "_reviewed_model_code"])
        .reset_index(drop=True)
    )


def mapping_coverage(mapped: pd.DataFrame) -> pd.DataFrame:
    """Report LDV, non-LDV, unresolved, and out-of-scope coverage."""
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
            for category in [
                "mapped_ldv",
                "mapped_non_ldv",
                "unmapped",
                "out_of_scope",
            ]:
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
                    "out_of_scope_share": category_values["out_of_scope"]
                    / total_value
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
        "manual_promotion_eligible",
        "manual_evidence_disposition",
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

    def assign(mask: pd.Series, reason: str, detail: str | pd.Series) -> None:
        selected = unresolved["unresolved_reason"].eq("") & mask.fillna(False)
        unresolved.loc[selected, "unresolved_reason"] = reason
        unresolved.loc[selected, "unresolved_reason_detail"] = detail

    for scope, reason in [
        ("identity_unresolved", "reviewed_identity_unresolved"),
        ("ldv_class_unresolved", "reviewed_family_class_unresolved"),
        ("scope_unresolved", "reviewed_family_scope_unresolved"),
    ]:
        selected = unresolved.get("vehicle_scope", pd.Series("", index=unresolved.index)).eq(scope)
        unresolved.loc[selected, "unresolved_reason"] = reason
        notes = unresolved.get("review_notes", pd.Series("", index=unresolved.index))
        unresolved.loc[selected, "unresolved_reason_detail"] = notes.loc[selected]
    assign(
        suppressed,
        "suppressed_or_unknown_code",
        "MTO make or model is blank, suppressed, or explicitly unknown.",
    )
    reviewed = mapping.loc[
        mapping["entry_type"].eq("mto_crosswalk")
        & mapping["mapping_status"].eq("reviewed")
    ]
    intervals = {
        key: sorted(set(zip(group.model_year_from.astype(int), group.model_year_to.astype(int))))
        for key, group in reviewed.groupby(["mto_make_code", "mto_model_code"], sort=False)
    }
    interval_details = pd.Series("", index=unresolved.index)
    for row in unresolved.itertuples():
        ranges = intervals.get((str(row.MAKE).strip().upper(), str(row.MODEL).strip().upper()), [])
        if ranges and pd.notna(row.MODEL_YEAR) and not any(start <= row.MODEL_YEAR <= end for start, end in ranges):
            years = ", ".join(str(start) if start == end else f"{start}-{end}" for start, end in ranges)
            interval_details.loc[row.Index] = (
                f"Reviewed source-link years: {years}. No reviewed interval covers MY{int(row.MODEL_YEAR)}. "
                "Check model identity and year evidence; a label match does not extend applicability."
            )
    assign(
        interval_details.ne(""),
        "no_applicable_reviewed_interval",
        interval_details,
    )
    promotion_eligible = (
        unresolved["manual_promotion_eligible"]
        .fillna(False)
        .astype(str)
        .str.casefold()
        .isin({"true", "1", "yes"})
    )
    usable_manual_agreement = (
        promotion_eligible
        & unresolved["candidate_pass_agreement"].eq("agreement")
        & ~unresolved["agreed_model_candidate"].map(is_unresolved_vehicle_label)
    )
    assign(
        usable_manual_agreement,
        "weak_model_label_agreement",
        "The two manual passes agree on a model family, but the promotion hierarchy did not produce an accepted mapping for this row.",
    )
    assign(
        promotion_eligible & unresolved["candidate_pass_agreement"].notna(),
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
        "An exact or prefix label candidate exists, but no reviewed source link establishes its identity, year and class.",
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
    vintage_bin_years: int,
    vintage_alignment: str,
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
    minimum_model_year: int,
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
    if "vehicle_scope" not in wards:
        wards.insert(1, "vehicle_scope", "ldv")
    wards.insert(0, "region_scope", "outside_ontario_or_legacy_comparison")
    wards.insert(1, "weight_source", WARDS_SOURCE_ID)
    return wards


def build_road_aggregation_artifacts(
    scenario_path: str | Path,
) -> Path:
    """Apply the reviewed mapping and publish runtime aggregation products."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    source_dir = resolve_artifact_path(bundle, "ontario_vehicle_population")
    output_dir = resolve_artifact_path(bundle, "road_aggregation")
    validation_dir = resolve_artifact_path(bundle, "vehicle_mapping_review")
    current_path = source_dir / str(ontario_rules["current_stock_file"])
    if not current_path.is_file():
        raise FileNotFoundError(current_path)
    current_stock = pd.read_csv(current_path, low_memory=False)

    validated_mapping = reference_mto_mapping(
        load_vehicle_reference(bundle), rules, rating_rules["vehicle_class_harmonization"],
    )
    mapped = apply_vehicle_mapping(
        current_stock,
        validated_mapping,
        accepted_statuses={
            str(status)
            for status in rules["accepted_mapping_statuses"]
        },
    )
    logging.info("Applied reviewed MTO crosswalk: %s accepted LDV rows, %s unresolved/out-of-scope rows",
                 int(mapped.mapping_accepted.sum()), int((~mapped.mapping_accepted).sum()))
    coverage = mapping_coverage(mapped)
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
    wards = load_wards_legacy_comparison(bundle)
    outputs = {
        rules["mapped_current_stock_file"]: mapped,
        rules["nrcan_weights_file"]: nrcan_weights,
        rules["nlr_weights_file"]: nlr_weights,
        rules["fleet_composition_weights_file"]: fleet_weights,
        rules["wards_comparison_file"]: wards,
    }
    for filename, frame in outputs.items():
        write_dataframe_atomic(frame, output_dir / str(filename))
    write_dataframe_atomic(
        coverage,
        validation_dir / str(rules["coverage_file"]),
    )
    return output_dir


def build_mapping_diagnostic_artifacts(scenario_path: str | Path) -> Path:
    """Explicitly regenerate mapping candidates and review diagnostics."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    source_dir = resolve_artifact_path(bundle, "ontario_vehicle_population")
    output_dir = resolve_artifact_path(bundle, "vehicle_mapping_review")
    current_stock = pd.read_csv(
        source_dir / str(ontario_rules["current_stock_file"]),
        low_memory=False,
    )
    mapping = reference_mto_mapping(
        load_vehicle_reference(bundle), rules, rating_rules["vehicle_class_harmonization"],
    )
    ratings = load_rating_evidence(bundle, rules=rules)
    candidates = generate_mapping_candidates(
        current_stock,
        ratings,
        composition_rules=load_harmonization_rules(bundle, "efficiencies")["ratings"]["composition"],
        candidates_per_key=int(
            rules["mapping_bootstrap"]["candidate_rows_per_key"]
        ),
        similarity_scores={
            str(name): float(score)
            for name, score in rules["mapping_bootstrap"][
                "candidate_similarity_scores"
            ].items()
        },
        canonical_aliases={
            str(source): str(target)
            for source, target in rules["canonical_aliases"].items()
        },
    )
    mapped = apply_vehicle_mapping(
        current_stock,
        mapping,
        accepted_statuses={
            str(status) for status in rules["accepted_mapping_statuses"]
        },
    )
    coverage = mapping_coverage(mapped)
    # Residual inference evidence is optional development context. It cannot
    # establish or replace a runtime identity from the reviewed crosswalk.
    manual_evidence = None
    manual_filename = rules["mapping_bootstrap"].get("manual_evidence_file")
    if manual_filename:
        manual_path = resolve_input_path(bundle, "manual", str(manual_filename))
        if manual_path.is_file():
            manual_evidence = pd.read_csv(manual_path, dtype=str, keep_default_na=False)
        else:
            logging.info("Skipped unavailable development-only MTO residual evidence: %s", manual_path)
    unresolved_detail, unresolved_summary = unresolved_mapping_reasons(mapped, candidates, mapping, manual_evidence)
    unresolved = (
        mapped.loc[mapped["mapping_outcome"].eq("unmapped")]
        .sort_values("FIT_ACTIVE", ascending=False, kind="stable")
        .head(int(rules["highest_stock_unresolved_limit"]))
    )
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
        rules["latest_unresolved_worklist_file"]: latest_unresolved_key_worklist(
            unresolved_detail
        ),
        rules["vehicle_class_evidence_file"]: build_rating_model_catalog(
            ratings,
            mapping_columns=[str(column) for column in rules["mapping_columns"]],
            composition_rules=load_harmonization_rules(bundle, "efficiencies")["ratings"]["composition"],
        ),
        rules["top_observations_file"]: top,
    }
    for filename, frame in outputs.items():
        write_dataframe_atomic(frame, output_dir / str(filename))
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/default.yaml",
    )
    parser.add_argument(
        "--mapping-diagnostics",
        action="store_true",
        help="Regenerate candidate/rating/review evidence instead of runtime products.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    if args.mapping_diagnostics:
        output_dir = build_mapping_diagnostic_artifacts(args.scenario)
        logging.info("Wrote Ontario mapping diagnostics to %s", output_dir)
    else:
        output_dir = build_road_aggregation_artifacts(args.scenario)
        logging.info("Wrote Ontario road-aggregation artifacts to %s", output_dir)


if __name__ == "__main__":
    main()
