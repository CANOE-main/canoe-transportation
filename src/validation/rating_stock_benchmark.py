"""Opt-in MTO-to-rating cohort benchmark; never a production parameter input.

MTO observes model families and model years, not propulsion or electric range.
Resolved matches are an incomplete diagnostic subset and are not population weights.
"""

from __future__ import annotations

import logging
from math import isfinite
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from parameterization.road_aggregation import load_rating_evidence, normalize_vehicle_text
from utils import (
    ConfigBundle,
    load_harmonization_rules,
    resolve_artifact_path,
    write_dataframe_atomic,
)


LOGGER = logging.getLogger(__name__)


def derive_rating_stock_weights(
    classified: pd.DataFrame,
    mapped_stock: pd.DataFrame,
    *,
    rules: dict[str, Any],
    model_catalog: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Match exact reviewed MTO cohorts, retaining unresolved propulsion evidence.

    The returned frames are one disposition per input stock row and source-rating
    allocations for resolved rows only. Report A does not observe powertrain or
    trim: a family spanning powertrains or PEV buckets therefore receives no
    allocation. Within a resolved cohort, distinct configured rating variants
    share its stock equally; repeated catalogue rows split their variant's share.
    This is an explicit within-model proxy, not observed trim registrations.

    ``model_catalog`` may be the existing ``load_rating_evidence`` result. Without
    it, classified rows must already contain canonical make/model labels.
    The function performs no I/O and does not amend reviewed mappings.
    """
    if rules["variant_allocation"] != "equal_distinct_variants":
        raise ValueError("Unsupported resolved rating variant allocation")
    tolerance = float(rules["weight_sum_tolerance"])
    if not isfinite(tolerance) or tolerance <= 0:
        raise ValueError("Rating stock weight tolerance must be positive and finite")
    identity = list(rules["rating_variant_identity_columns"])
    source_identity = ["component", "source_row"]
    provenance_columns = {
        "source_id", "component", "source_row", "resource_id", "resource_title",
        "resource_version", "resource_url", "cached_file",
    }
    if not identity or len(identity) != len(set(identity)) or set(identity) & provenance_columns:
        raise ValueError("Rating variant identity must use distinct vehicle fields, not source-row metadata")

    stock_required = {
        "report_year", "MODEL_YEAR", "MAKE", "MODEL", "FIT_ACTIVE", "mapping_accepted",
        "canonical_make", "canonical_model", "nrcan_vehicle_class", "nlr_atb_class",
        "nrcan_ceud_class",
    }
    rating_required = {
        *source_identity, *identity, "year", "rating_class", "powertrain", "included",
        "native_consumption", "Model",
    }
    for label, frame, required in (
        ("Mapped stock", mapped_stock, stock_required),
        ("Classified ratings", classified, rating_required),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{label} missing weighting columns: {', '.join(missing)}")

    stock = mapped_stock.copy().reset_index(drop=True)
    stock.insert(0, "mto_stock_row_id", range(len(stock)))
    stock["FIT_ACTIVE"] = pd.to_numeric(stock.FIT_ACTIVE, errors="raise")
    if (stock.FIT_ACTIVE.isna().any()
            or stock.FIT_ACTIVE.map(lambda value: not isfinite(value) or value < 0).any()
            or stock.FIT_ACTIVE.mod(1).ne(0).any()):
        raise ValueError("MTO fit-active counts must be finite nonnegative integers")
    if not stock.mapping_accepted.isin([True, False]).all():
        raise ValueError("MTO mapping_accepted must contain booleans")
    stock["_join_year"] = pd.to_numeric(stock.MODEL_YEAR, errors="coerce")
    accepted = stock.mapping_accepted
    if (stock.loc[accepted, "_join_year"].isna().any()
            or stock.loc[accepted, "_join_year"].mod(1).ne(0).any()):
        raise ValueError("Accepted MTO mappings need an integer model year")

    ratings = classified.copy()
    if not ratings.included.isin([True, False]).all():
        raise ValueError("Classified ratings included must contain booleans")
    if ratings[source_identity].isna().any().any():
        raise ValueError("Classified ratings need source component/row identities")
    ratings["source_duplicate_count"] = ratings.groupby(source_identity, dropna=False).source_row.transform("size")
    ratings = ratings.drop_duplicates().copy()
    if ratings.duplicated(source_identity).any():
        raise ValueError("Conflicting values for a classified rating source identity")
    if model_catalog is not None:
        catalog_keys = ["Model year", "Make", "Model", "Vehicle class"]
        canonical = ["canonical_make", "canonical_model"]
        missing = sorted(set(catalog_keys + canonical) - set(model_catalog.columns))
        if missing:
            raise ValueError("Rating model catalogue missing columns: " + ", ".join(missing))
        missing = sorted(set(catalog_keys) - set(ratings.columns))
        if missing:
            raise ValueError("Classified ratings missing catalogue keys: " + ", ".join(missing))
        catalog = model_catalog[catalog_keys + canonical].drop_duplicates()
        if catalog.duplicated(catalog_keys).any():
            raise ValueError("Conflicting canonical families for a rating model catalogue key")
        ratings = ratings.drop(columns=canonical, errors="ignore").merge(
            catalog, on=catalog_keys, how="left", validate="many_to_one",
        )
    elif not {"canonical_make", "canonical_model"}.issubset(ratings):
        raise ValueError("Classified ratings need canonical labels or a model catalogue")

    keys = ["_join_year", "_join_make", "_join_model", "_join_class"]
    ratings["_join_year"] = pd.to_numeric(ratings.year, errors="raise")
    if ratings._join_year.isna().any() or ratings._join_year.mod(1).ne(0).any():
        raise ValueError("Classified ratings need an integer model year")
    for frame, class_column in ((stock, "nrcan_vehicle_class"), (ratings, "rating_class")):
        for field in ("make", "model"):
            frame[f"_join_{field}"] = frame[f"canonical_{field}"].fillna("").map(normalize_vehicle_text)
        frame["_join_class"] = frame[class_column].fillna("").astype(str)
    eligible = ratings.included & ratings[["_join_make", "_join_model", "_join_class"]].ne("").all(axis=1)
    ratings = ratings.loc[eligible].copy()
    if ratings.powertrain.isna().any() or ratings.powertrain.astype(str).str.strip().eq("").any():
        raise ValueError("Included ratings need a classified powertrain")
    ratings["rating_powertrain_family"] = ratings.powertrain.map(
        lambda value: str(value).split("_", 1)[0]
        if str(value).split("_", 1)[0] in {"bev", "phev"} else str(value)
    )
    variant_keys = list(dict.fromkeys(keys + ["powertrain"] + identity))
    variants = ratings.groupby(variant_keys, dropna=False, sort=True)
    metric_fields = [field for field in (
        "Model", "native_consumption", "native_unit", "consumption_mj_per_vkm", "range_km",
    ) if field in ratings and field not in variant_keys]
    if metric_fields and variants[metric_fields].nunique(dropna=False).gt(1).any().any():
        raise ValueError("Configured rating variant identity merges conflicting consumption or range evidence")
    ratings["rating_variant_id"] = variants.ngroup().map(lambda value: f"rating_variant_{value:08d}")
    ratings["rating_source_row_share"] = 1.0 / ratings.groupby("rating_variant_id").source_row.transform("size")
    ratings["_source_identity"] = ratings.component.astype(str) + ":" + ratings.source_row.astype(str)

    def labels(values: pd.Series) -> str:
        return " | ".join(sorted(set(values.astype(str))))

    cohorts = ratings.groupby(keys, dropna=False, as_index=False).agg(
        candidate_rating_rows=("source_row", "size"),
        candidate_variant_count=("rating_variant_id", "nunique"),
        candidate_powertrain_count=("powertrain", "nunique"),
        candidate_powertrain_family_count=("rating_powertrain_family", "nunique"),
        candidate_powertrains=("powertrain", labels),
        candidate_powertrain_families=("rating_powertrain_family", labels),
        candidate_variant_ids=("rating_variant_id", labels),
        candidate_rating_ids=("_source_identity", labels),
    )
    disposition = stock.merge(cohorts, on=keys, how="left", validate="many_to_one")
    count_fields = [column for column in cohorts if column.endswith(("_count", "_rows"))]
    label_fields = [column for column in cohorts if column not in keys + count_fields]
    disposition[count_fields] = disposition[count_fields].fillna(0).astype(int)
    disposition[label_fields] = disposition[label_fields].fillna("")
    disposition["match_status"] = "mapping_not_accepted"
    accepted = disposition.mapping_accepted
    disposition.loc[accepted, "match_status"] = "unmatched_ratings"
    matched = accepted & disposition.candidate_variant_count.gt(0)
    disposition.loc[matched, "match_status"] = "resolved"
    disposition.loc[matched & disposition.candidate_powertrain_count.gt(1), "match_status"] = "ambiguous_range_bucket"
    disposition.loc[matched & disposition.candidate_powertrain_family_count.gt(1), "match_status"] = "ambiguous_powertrain"
    resolved = disposition.match_status.eq("resolved")
    disposition["matched_fit_active_stock"] = disposition.FIT_ACTIVE.where(resolved, 0)
    disposition["unresolved_fit_active_stock"] = disposition.FIT_ACTIVE.where(~resolved, 0)
    disposition["resolved_powertrain"] = disposition.candidate_powertrains.where(resolved, "")
    disposition["resolved_powertrain_family"] = disposition.candidate_powertrain_families.where(resolved, "")

    stock_fields = {
        "report_year": "mto_report_year", "MODEL_YEAR": "mto_model_year",
        "FIT_ACTIVE": "mto_fit_active_stock", "MAKE": "mto_make_code", "MODEL": "mto_model_code",
        "canonical_make": "mto_canonical_make", "canonical_model": "mto_canonical_model",
        "nrcan_vehicle_class": "mto_nrcan_vehicle_class", "nlr_atb_class": "mto_nlr_atb_class",
        "nrcan_ceud_class": "mto_nrcan_ceud_class",
    }
    selected = disposition.loc[resolved, keys + ["mto_stock_row_id", "candidate_variant_count"] + list(stock_fields)].rename(columns=stock_fields)
    allocations = selected.merge(ratings, on=keys, how="inner", validate="many_to_many")
    allocations["rating_variant_share"] = 1.0 / allocations.candidate_variant_count
    allocations["allocated_fit_active_stock"] = (
        allocations.mto_fit_active_stock * allocations.rating_variant_share * allocations.rating_source_row_share
    )
    allocations["variant_allocation"] = rules["variant_allocation"]
    actual = allocations.groupby("mto_stock_row_id").allocated_fit_active_stock.sum()
    expected = disposition.loc[resolved].set_index("mto_stock_row_id").FIT_ACTIVE
    if not expected.index.equals(actual.index) or (
        actual.sub(expected).abs() > tolerance * expected.abs().clip(lower=1)
    ).any():
        raise ValueError("Resolved MTO stock is not conserved by rating allocations")
    if len(disposition) != len(mapped_stock) or not disposition.mto_stock_row_id.is_unique:
        raise ValueError("Rating weighting lost or duplicated MTO stock dispositions")
    for raw_field, audit_field in stock_fields.items():
        disposition[audit_field] = disposition[raw_field]
    return (
        disposition.drop(columns=keys),
        allocations.drop(columns=keys + ["_source_identity"]).sort_values(
            ["mto_stock_row_id", "component", "source_row"], kind="stable",
        ).reset_index(drop=True),
    )


def _production_rating_statistic(audit: pd.DataFrame) -> str:
    """Reject stale or mixed production methods before using their fallback values."""
    if (audit.empty or "aggregation_method" not in audit
            or not audit.aggregation_method.eq("distinct_model_class_weighted").all()):
        raise ValueError("Prepare production distinct-model/class rating aggregation before benchmarking")
    if ("rating_statistic" not in audit or audit.rating_statistic.isna().any()
            or audit.rating_statistic.nunique() != 1
            or audit.rating_statistic.iloc[0] not in {"mean", "median"}):
        raise ValueError("Production rating audit must record one mean or median rating statistic")
    return str(audit.rating_statistic.iloc[0])


def aggregate_stock_weighted_ratings(
    allocations: pd.DataFrame, production_audit: pd.DataFrame, *, rules: dict,
) -> pd.DataFrame:
    """Weight resolved model variants and size classes by the same MTO cohort.

    Each model-year stock count enters once. Size-class shares are conditional on
    the resolved powertrain/range cohort, rather than pooled across all fuels and
    ages. A wholly uncovered cohort retains the production distinct-model fallback;
    partly covered cohorts use only resolved stock and expose their denominator.
    """
    policy = rules["stock_weighting"]
    if policy["method"] != "resolved_mto_model_year_stock":
        raise ValueError("Unsupported rating stock-weighting method")
    if policy["unresolved_stock"] != "exclude_and_report":
        raise ValueError("Unsupported ambiguous rating stock allocation")
    if policy["uncovered_cohort"] != "distinct_model_class_weighted":
        raise ValueError("Unsupported uncovered rating cohort policy")
    _production_rating_statistic(production_audit)
    cohort = ["ceud_class", "year", "powertrain"]
    fallback = production_audit.copy()
    fallback["aggregation_method"] = "distinct_model_fallback_no_resolved_stock"
    if allocations.empty:
        return fallback
    if (not np.isfinite(allocations.allocated_fit_active_stock).all()
            or allocations.allocated_fit_active_stock.lt(0).any()):
        raise ValueError("Invalid allocated MTO rating stock")
    selected = allocations.loc[allocations.allocated_fit_active_stock.gt(0)].copy()
    if selected.empty:
        return fallback
    if (selected.mto_report_year.nunique() != 1
            or not selected.year.eq(selected.mto_model_year).all()):
        raise ValueError("Invalid MTO rating stock edition, count or model year")
    selected["ceud_class"] = selected.mto_nrcan_ceud_class
    if selected[cohort + ["rating_class"]].isna().any().any():
        raise ValueError("Resolved rating stock lacks a class/powertrain cohort")
    if "range_km" not in selected:
        selected["range_km"] = np.nan
    for metric in ("native_consumption", "consumption_mj_per_vkm", "range_km"):
        selected["weighted_" + metric] = selected[metric] * selected.allocated_fit_active_stock
    keys = cohort + ["rating_class"]
    grouped = selected.groupby(keys, sort=True, dropna=False)
    if grouped.native_unit.nunique().ne(1).any():
        raise ValueError("Mixed native units in a resolved rating cohort")
    weighted = grouped.agg(
        matched_stock=("allocated_fit_active_stock", "sum"),
        native_unit=("native_unit", "first"),
        observations=("rating_variant_id", "nunique"),
        matched_models=("mto_stock_row_id", "nunique"),
    )
    for source, target in (
        ("native_consumption", "native_class_mean"),
        ("consumption_mj_per_vkm", "class_mean_mj_per_vkm"),
        ("range_km", "range_class_mean_km"),
    ):
        # Preserve absent PHEV MJ values and absent non-PEV ranges as missing.
        present = grouped[source].count()
        sizes = grouped.size()
        if ((present > 0) & (present < sizes)).any():
            raise ValueError(f"Partly missing {source} in a resolved rating cohort")
        weighted[target] = grouped["weighted_" + source].sum(min_count=1) / weighted.matched_stock
    weighted = weighted.reset_index()
    weighted["cohort_matched_stock"] = weighted.groupby(cohort).matched_stock.transform("sum")
    weighted["weight"] = weighted.matched_stock / weighted.cohort_matched_stock
    weighted["original_weight"] = weighted.weight
    weighted["observed_weight_sum"] = 1.0
    weighted["aggregation_method"] = "mto_stock_weighted"
    covered = pd.MultiIndex.from_frame(weighted[cohort].drop_duplicates())
    fallback = fallback.loc[~pd.MultiIndex.from_frame(fallback[cohort]).isin(covered)]
    return pd.concat([weighted, fallback], ignore_index=True).sort_values(keys).reset_index(drop=True)


def build_rating_stock_benchmark(bundle: ConfigBundle) -> dict[str, Path]:
    """Export opt-in match evidence using already-prepared production ratings.

    No source preparation or network calls occur here. The output directory is a
    diagnostic route, and the promoted aggregate is read solely as a comparison
    and an explicitly labelled fallback for completely uncovered cohorts.
    """
    rules = load_harmonization_rules(bundle, "rating_stock_benchmark")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    production = resolve_artifact_path(bundle, "efficiencies_interim")
    mapped_path = (
        resolve_artifact_path(bundle, "road_aggregation")
        / aggregation["mapped_current_stock_file"]
    )
    source_paths = {
        "classified": production / efficiency_rules["files"]["ratings"],
        "production_ratings": production / efficiency_rules["files"]["rating_weights"],
        "mapped": mapped_path,
    }
    missing = [str(path) for path in source_paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Prepare the diagnostic scenario prerequisites before benchmarking ratings: "
            + ", ".join(missing)
        )
    classified, production_audit, mapped = (
        pd.read_csv(source_paths[key], low_memory=False)
        for key in ("classified", "production_ratings", "mapped")
    )
    statistic = _production_rating_statistic(production_audit)
    if statistic != bundle.scenario.efficiencies.rating_statistic:
        raise ValueError("Production rating audit statistic differs from the selected scenario rating_statistic")
    population_year = bundle.scenario.existing_capacity.vehicle_population_year
    if set(mapped.report_year) != {population_year}:
        raise ValueError("MTO benchmark stock edition differs from the prepared population selection")
    model_catalog = load_rating_evidence(bundle, rules=aggregation)
    matches, allocations = derive_rating_stock_weights(
        classified, mapped, model_catalog=model_catalog, rules=rules["stock_weighting"],
    )
    weighted = aggregate_stock_weighted_ratings(allocations, production_audit, rules=rules)
    output = resolve_artifact_path(bundle, "rating_stock_benchmark")
    paths = {key: output / filename for key, filename in rules["files"].items()}
    if set(paths) != {"matches", "allocations", "weighted"}:
        raise ValueError("Rating benchmark files must name matches, allocations and weighted")
    if len({path.resolve() for path in paths.values()}) != len(paths):
        raise ValueError("Rating benchmark output files must be distinct")
    protected = [
        production.resolve(),
        resolve_artifact_path(bundle, "efficiencies_processed").resolve(),
        resolve_artifact_path(bundle, "road_aggregation").resolve(),
    ]
    if any(path.resolve().is_relative_to(root) for path in paths.values() for root in protected):
        raise ValueError("Rating stock benchmark must not overwrite production artifact routes")
    for key, frame in (("matches", matches), ("allocations", allocations), ("weighted", weighted)):
        write_dataframe_atomic(frame, paths[key])
    LOGGER.info(
        "Diagnostic rating benchmark: resolved stock=%s of accepted stock=%s; unresolved stock=%s",
        float(allocations.allocated_fit_active_stock.sum()),
        float(matches.loc[matches.mapping_accepted, "FIT_ACTIVE"].sum()),
        float(matches.unresolved_fit_active_stock.sum()),
    )
    return paths


