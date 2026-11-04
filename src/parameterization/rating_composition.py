"""Observed-family composition for annual NRCan consumption cohorts.

Counts describe surviving Ontario registrations or incentivized transactions.
They never establish propulsion for an ambiguous MTO family, unobserved trim
shares, complete vehicle-market coverage, or real-world fuel consumption.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
from hashlib import sha256
from math import isfinite
import re
from typing import Any

import numpy as np
import pandas as pd

from utils.vehicle_labels import normalize_vehicle_text
from parameterization.vehicle_reference import (
    _normal_text, _canonical_labels,
    MTO_SOURCE, VehicleReference, attach_catalogue_identities, resolve_source_labels,
    canonical_family as canonical_family,
    canonical_izev_observation, catalogue_family, catalogue_family_labels,
    normalize_family as normalize_family,
)


@dataclass(frozen=True)
class RatingComposition:
    class_audit: pd.DataFrame
    observations: pd.DataFrame
    variants: pd.DataFrame
    coverage: pd.DataFrame
    matches: pd.DataFrame


def median_support_weights(frame: pd.DataFrame, field: str) -> pd.Series:
    """Central distinct-trim coefficients, splitting repeated physical source rows.

    These reproduce a scalar median and component totals. They are mathematical
    representative-rating coefficients, never observed trim-market shares.
    """
    values = frame.drop_duplicates("rating_variant_id").sort_values([field, "rating_variant_id"])
    values = values.loc[pd.to_numeric(values[field], errors="coerce").notna()]
    result = pd.Series(0., index=frame.index)
    if values.empty:
        return result
    central = values.iloc[[(len(values) - 1) // 2, len(values) // 2]][field]
    central_mass = central.value_counts(normalize=True)
    tied_count = values.groupby(field, dropna=False)[field].transform("size")
    counts = pd.Series((values[field].map(central_mass).fillna(0.) / tied_count).to_numpy(),
                       index=values.rating_variant_id)
    source_counts = frame.groupby("rating_variant_id").rating_variant_id.transform("size")
    return frame.rating_variant_id.map(counts).fillna(0.).astype(float) / source_counts


def _family_tokens(value: object) -> tuple[str, ...]:
    return tuple(re.findall(r"[A-Z0-9]+", str(value).upper()))


@lru_cache(maxsize=16384)
def _compatible(observed: str, candidate: str) -> bool:
    """A complete token prefix is a family relation, not arbitrary fuzzy text."""
    if normalize_vehicle_text(observed) == normalize_vehicle_text(candidate):
        return True
    return _description_prefix(observed, candidate) or _description_prefix(candidate, observed)


def _labels(values: pd.Series) -> str:
    return " | ".join(sorted({str(value) for value in values if pd.notna(value)}))


def _source_references(values: pd.Series) -> str:
    """Keep row reconciliation reproducible without copying long transaction lists."""
    unique = sorted({str(value) for value in values if pd.notna(value)})
    if len(unique) <= 20 or any(":" not in value for value in unique):
        return " | ".join(unique)
    bounds: dict[str, list[int]] = {}
    for identifier in unique:
        component, number = identifier.rsplit(":", 1)
        bounds.setdefault(component, []).append(int(number))
    ranges = " | ".join(
        f"{component}:rows {min(numbers)}-{max(numbers)}; n={len(numbers)}"
        for component, numbers in sorted(bounds.items())
    )
    digest = sha256("\n".join(unique).encode("utf-8")).hexdigest()
    return f"{ranges}; row_identity_sha256={digest}"


def _validate_policy(spec: dict) -> None:
    retired = {"unresolved_mto_keys", "unresolved_mto_reason", "unresolved_mto_cohorts"} & set(spec)
    if retired:
        raise ValueError("Retired MTO exclusion policies are unsupported; migrate identity/configuration decisions to vehicle_source_links.csv: "
                         + ", ".join(sorted(retired)))
    supported = {
        "method": "observed_family_counts", "unmatched": "exclude_and_report",
        "ambiguous": "exclude_and_report", "uncovered_cohort": "distinct_model_median",
        "variant_allocation": "family_median", "family_statistic": "median",
    }
    for key, value in supported.items():
        if key in spec and spec[key] != value:
            raise ValueError(f"Unsupported empirical composition {key}: {spec[key]!r}")
    if spec.get("incumbent_powertrains", ["gasoline"]) != ["gasoline"]:
        raise ValueError("MTO empirical composition is supported for gasoline only")
    if spec.get("empirical_subclasses", "median_fill_original_weights") != "median_fill_original_weights":
        raise ValueError("Missing empirical size subclasses must retain median catalogue consumption and outer weights")
    transactions = spec["izev_transactions"]
    if (not isinstance(transactions["column"], str) or not transactions["column"].strip()
            or transactions["accepted_values"] != ["Purchase", "Lease"]):
        raise ValueError("iZEV weights require configured purchase and lease observations")
    matching = spec["izev_matching"]
    if matching.get("trim_priority") != "explicit_configuration_then_family_median":
        raise ValueError("Unsupported iZEV trim matching policy")
    if matching.get("range_assignment", "family_median") != "family_median":
        raise ValueError("iZEV range assignment requires the eligible-family median")
    family_matching = spec.get("family_matching", {})
    for field in ("minimum_similarity", "minimum_family_margin"):
        value = family_matching.get(field)
        if value is not None and (isinstance(value, bool) or not isfinite(value) or not 0 <= value <= 1):
            raise ValueError(f"Invalid family matching {field}")
    class_policy = family_matching.get("class_resolution", {})
    if class_policy:
        minimum = class_policy.get("minimum_predominant_share")
        groups = class_policy.get("compatible_class_groups")
        if (isinstance(minimum, bool) or not isinstance(minimum, (float, int))
                or not isfinite(minimum) or not .5 < minimum <= 1
                or not isinstance(groups, list) or not groups
                or any(not isinstance(group, list) or len(group) < 2
                       or len(group) != len(set(group))
                       or any(not isinstance(label, str) or not label.strip() for label in group)
                       for group in groups)):
            raise ValueError("Predominant rating class requires a majority threshold and compatible body-class groups")
    dispersion = family_matching.get("consumption_dispersion", {})
    if dispersion:
        maximum = dispersion.get("maximum_relative_p90_p10_span")
        if (isinstance(maximum, bool) or not isinstance(maximum, (float, int))
                or not isfinite(maximum) or maximum <= 0):
            raise ValueError("Family consumption dispersion requires a positive relative P90-P10 span")
    try:
        for pattern in family_matching.get("trim_suffix_patterns", []):
            re.compile(pattern)
        for rule in family_matching.get("family_patterns", []):
            if not str(rule["make"]).strip() or not str(rule["canonical_model"]).strip():
                raise ValueError("Family matching labels must be nonblank")
            re.compile(rule["pattern"])
    except (KeyError, TypeError, re.error) as error:
        raise ValueError("Invalid configured family matching pattern") from error
    if "family_candidate_rewrites" in matching:
        raise ValueError("iZEV family candidate rewrites were replaced by shared configured family patterns")
    if any(not isinstance(alias.get("exact_only", False), bool)
           for alias in [*spec.get("aliases", []), *spec.get("izev_aliases", [])]):
        raise ValueError("Family alias exact_only must be a boolean")
    observation_aliases = spec.get("izev_observation_aliases_by_powertrain", {})
    if not isinstance(observation_aliases, dict) or set(observation_aliases) - {"bev", "phev"}:
        raise ValueError("Observation-only iZEV aliases require a bev/phev dictionary")
    for aliases in observation_aliases.values():
        if not isinstance(aliases, list):
            raise ValueError("Observation-only iZEV aliases must be lists")
        for alias in aliases:
            if (not isinstance(alias, dict) or set(alias) - {"make", "canonical_model", "models", "exact_only"}
                    or any(not isinstance(alias.get(field), str) or not alias[field].strip()
                           for field in ("make", "canonical_model"))
                    or not isinstance(alias.get("models"), list) or not alias["models"]
                    or any(not isinstance(model, str) or not model.strip() for model in alias["models"])
                    or not isinstance(alias.get("exact_only", False), bool)):
                raise ValueError("Invalid observation-only iZEV alias identity/models/exact_only")
    pooling = spec["izev_pool_selection"]
    minimum = pooling["minimum_observations"]
    if (pooling.get("method") != "provincial_observation_count" or isinstance(minimum, bool)
            or not isinstance(minimum, int) or minimum < 1):
        raise ValueError("iZEV pool selection requires a positive integer observation threshold")
    provinces = pooling["national_provinces"]
    if (not isinstance(provinces, list) or not provinces or len(provinces) != len(set(provinces))
            or any(not isinstance(value, str) or not value.strip() for value in provinces)):
        raise ValueError("National iZEV geography requires distinct recipient-province codes")
    selection = spec["mto_selection"]
    minimum = selection["minimum_resolved_count_share"]
    if (selection["method"] != "identity_supported_coverage" or isinstance(minimum, bool)
            or not isfinite(minimum) or not 0 < minimum <= 1):
        raise ValueError("MTO selection requires an explicit identity-supported share of resolved counts")
    supported_routes = {
        "direct_methods": {"automatic_exact_normalized_make_exact_normalized_model",
                           "automatic_configured_make_alias_exact_normalized_model",
                           "automatic_approved_current_candidate_approved_similarity_exact_normalized"},
        "external_methods": {"reviewed_external_model_vin_family_association"},
    }
    for key, allowed in supported_routes.items():
        values = selection[key]
        if not isinstance(values, list) or len(values) != len(set(values)) or set(values) - allowed:
            raise ValueError(f"Unsupported positive MTO identity routes: {key}")
    if any(isinstance(spec[key], bool) or not isinstance(spec[key], int)
           for key in ("izev_model_year_min", "izev_model_year_max")):
        raise ValueError("iZEV supported model-year bounds must be integers")
    if spec["izev_model_year_min"] > spec["izev_model_year_max"]:
        raise ValueError("iZEV supported model-year window is reversed")
    if spec["izev_historical_composition"] != {"method": "pooled_supported_model_year_window"}:
        raise ValueError("Historical iZEV composition must pool the configured supported model-year window")
    if spec["mto_selection"].get("eligible_fuel_codes", ["X", "Z"]) != ["X", "Z"]:
        raise ValueError("MTO composition requires conventional gasoline fuel codes X/Z")


def _region_provinces(region: str, spec: dict) -> tuple[str, ...]:
    """Resolve one configured region to disjoint recipient-province codes."""
    selected = spec.get("province_map", {}).get(region, region)
    values = [selected] if isinstance(selected, str) else selected
    if (not isinstance(values, list) or not values
            or any(not isinstance(value, str) or not value.strip() for value in values)
            or len(values) != len(set(values))):
        raise ValueError(f"iZEV geography {region} requires distinct nonblank recipient-province codes")
    return tuple(values)


def _transaction_selection(records: pd.DataFrame, spec: dict) -> pd.Series:
    column = spec["izev_transactions"]["column"]
    if column not in records:
        raise ValueError(f"iZEV composition missing purchase/lease column: {column}")
    accepted = {value.casefold() for value in spec["izev_transactions"]["accepted_values"]}
    return records[column].fillna("").astype(str).str.strip().str.casefold().isin(accepted)


def _rating_variants(classified: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """Identify distinct variants and split duplicate catalogue rows' shares."""
    required = {
        "included", "year", "powertrain", "rating_class", "native_unit", "Make", "Model",
        "component", "source_row", "native_consumption", "consumption_mj_per_vkm",
    }
    if missing := required - set(classified):
        raise ValueError(f"Empirical rating evidence missing columns: {sorted(missing)}")
    if not classified.included.isin([True, False]).all():
        raise ValueError("Empirical rating inclusion flags must be complete booleans")
    result = classified.loc[classified.included].copy().reset_index(drop=True)
    if "range_km" not in result:
        result["range_km"] = np.nan
    if result[["Make", "Model", "year", "powertrain", "rating_class", "native_unit"]].isna().any().any():
        raise ValueError("Included empirical ratings require complete cohort and model identities")
    if result[["Make", "Model"]].astype(str).apply(lambda values: values.str.strip().eq("")).any().any():
        raise ValueError("Included empirical ratings require nonblank make/model labels")
    if result.duplicated(["component", "source_row"]).any():
        raise ValueError("Empirical ratings have repeated source-row identities")
    if ("ambiguous_epa_match" in result
            and not result.ambiguous_epa_match.fillna(False).isin([True, False]).all()):
        raise ValueError("EPA powertrain ambiguity flags must be booleans when present")
    if "canonical_description" in result:
        result["_trim_model_key"] = result.canonical_description.map(normalize_vehicle_text)
    else:
        canonical = _canonical_labels(result.Make, result.Model, spec)
        result["_trim_model_key"] = [normalize_vehicle_text(value[1]) for value in canonical]
    result = catalogue_family_labels(result, spec)
    result["_make_key"] = result.family_make.map(normalize_vehicle_text)
    result["_model_key"] = result.family_model.map(normalize_vehicle_text)
    result["_powertrain_family"] = result.powertrain.str.split("_").str[0]
    result["catalogue_powertrain"] = result.powertrain
    identity = list(dict.fromkeys([
        "year", "powertrain", "rating_class", "native_unit", "_make_key", "_model_key",
        *("_trim_model_key" if field == "Model" else "_make_key" if field == "Make" else field
          for field in spec["variant_identity_columns"]),
    ]))
    missing = set(identity) - set(result)
    # Source adapters need not emit fields inapplicable to their fuel type.
    for field in missing:
        result[field] = np.nan
    grouped = result.groupby(identity, dropna=False, sort=True)
    metrics = [field for field in ("native_consumption", "consumption_mj_per_vkm", "range_km") if field not in identity]
    if metrics and grouped[metrics].nunique(dropna=False).gt(1).any().any():
        raise ValueError("Configured variant identity merges conflicting rating consumption/range")
    result["rating_variant_id"] = grouped.ngroup().map(lambda number: f"empirical_variant_{number:08d}")
    result["source_rows_per_variant"] = result.groupby("rating_variant_id").source_row.transform("size")
    return result


def distinct_catalogue_variants(classified: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """Public distinct-trim grain shared by production and diagnostic medians."""
    return _rating_variants(classified, spec)


def _observed_families(
    mto: pd.DataFrame, izev: pd.DataFrame, spec: dict, *, izev_target_year: int | None = None,
    reference: VehicleReference | None = None,
) -> pd.DataFrame:
    """Coalesce genuine identity duplicates before ranking; retain native keys."""
    frames = []
    columns = ["source_kind", "province", "year", "rating_class", "observed_powertrain",
               "source_count", "raw_make", "raw_model", "raw_observation_ids",
               "mapped_canonical_make", "mapped_canonical_model", "match_make", "match_model",
               "request_year_min", "request_year_max", "source_row_min", "source_row_max",
               "composition_source", "composition_components", "raw_nrcan_vehicle_classes",
               "identity_exclusion_reason", "transaction_eligible", "transaction_types",
               "identity_supported", "identity_evidence_methods", "identity_support_reason",
               "composition_basis", "source_model_year_min", "source_model_year_max", "source_model_years",
               "vehicle_id", "reference_family_model", "reference_description", "reference_link_status"]
    if not mto.empty:
        required = {"report_year", "MODEL_YEAR", "MAKE", "MODEL", "FIT_ACTIVE", "mapping_accepted",
                    "canonical_make", "canonical_model", "nrcan_vehicle_class", "match_method"}
        if missing := required - set(mto):
            raise ValueError(f"Mapped MTO composition missing columns: {sorted(missing)}")
        if not mto.mapping_accepted.isin([True, False]).all():
            raise ValueError("MTO mapping_accepted must contain complete booleans")
        counts = pd.to_numeric(mto.FIT_ACTIVE, errors="raise")
        if counts.isna().any() or not np.isfinite(counts).all() or counts.lt(0).any() or counts.mod(1).ne(0).any():
            raise ValueError("MTO empirical counts must be finite nonnegative integers")
        if mto.report_year.nunique() != 1:
            raise ValueError("MTO empirical composition requires one report-year snapshot")
        selected = mto.loc[mto.mapping_accepted & counts.gt(0)].copy()
        selected["source_count"] = counts.loc[selected.index]
        selected["raw_make"] = selected.get("raw_canonical_make", selected.canonical_make)
        selected["raw_model"] = selected.get("raw_canonical_model", selected.canonical_model)
        selected["mapped_canonical_make"] = selected.canonical_make
        selected["mapped_canonical_model"] = selected.canonical_model
        selected["match_make"], selected["match_model"] = selected.canonical_make, selected.canonical_model
        selected["raw_observation_ids"] = selected.MAKE.astype(str) + "/" + selected.MODEL.astype(str)
        selected["identity_exclusion_reason"] = ""
        selected["identity_evidence_methods"] = selected.match_method.fillna("").astype(str)
        direct = selected.identity_evidence_methods.isin(spec["mto_selection"]["direct_methods"])
        external = selected.identity_evidence_methods.isin(spec["mto_selection"]["external_methods"])
        reviewed = selected.get("identity_authority", pd.Series("", index=selected.index)).eq("reviewed_vehicle_reference")
        if "identity_supported" in selected:
            if not selected.loc[reviewed, "identity_supported"].isin([True, False]).all():
                raise ValueError("Reviewed MTO identity_supported flags must be complete booleans")
            reviewed &= selected.identity_supported.fillna(False)
        if "identity_authority" in selected:
            # Current reviewed crosswalk flags govern acceptance. Legacy route
            # metadata cannot override an explicit authority decision.
            direct[:] = False
            external[:] = False
        selected["identity_supported"] = reviewed | direct | external
        selected["identity_support_reason"] = np.select(
            [reviewed, direct, external],
            ["reviewed_year_scoped_crosswalk_identity", "exact_model_code_identity", "documented_model_vin_family_association"],
            default="mapping_route_does_not_identify_model_family",
        )
        selected["year"] = pd.to_numeric(selected.MODEL_YEAR, errors="raise")
        selected["composition_basis"] = "observed_model_year_stock"
        selected["source_model_year_min"] = selected["source_model_year_max"] = selected.year
        selected["source_model_years"] = selected.year.astype(str)
        selected["rating_class"] = selected.nrcan_vehicle_class
        selected["raw_nrcan_vehicle_classes"] = selected.get("raw_nrcan_vehicle_classes", selected.nrcan_vehicle_class)
        selected["source_kind"], selected["province"], selected["observed_powertrain"] = "mto", "ON", ""
        selected["transaction_eligible"], selected["transaction_types"] = False, "not_observed"
        selected["composition_source"] = "ontario_ministry_transport_vehicle_population"
        selected["composition_components"] = "A"
        selected["request_year_min"], selected["request_year_max"] = np.nan, np.nan
        selected["source_row_min"], selected["source_row_max"] = np.nan, np.nan
        selected["vehicle_id"], selected["reference_family_model"] = "", ""
        selected["reference_description"], selected["reference_link_status"] = "", ""
        if reference is not None:
            labels = resolve_source_labels(selected, reference, source=MTO_SOURCE,
                make_column="MAKE", model_column="MODEL", year_column="MODEL_YEAR", require_registered=True)
            selected["vehicle_id"] = labels.vehicle_id
            selected["reference_family_model"] = labels.family_model
            selected["reference_description"] = labels.canonical_description
            selected["reference_link_status"] = labels.reference_link_status
        frames.append(selected[columns])
    if not izev.empty:
        required = {"model_year", "request_year", "province", "make", "model", "powertrain", "count", "source_row"}
        if missing := required - set(izev):
            raise ValueError(f"iZEV composition missing columns: {sorted(missing)}")
        selected = izev.copy()
        selected["transaction_eligible"] = _transaction_selection(selected, spec)
        selected["transaction_types"] = selected[spec["izev_transactions"]["column"]].fillna("").astype(str)
        selected["source_count"] = pd.to_numeric(selected["count"], errors="raise")
        if (selected.source_count.isna().any() or not np.isfinite(selected.source_count).all()
                or selected.source_count.le(0).any() or selected.source_count.mod(1).ne(0).any()):
            raise ValueError("iZEV empirical counts must be finite positive integers")
        if selected.powertrain.isna().any() or selected.powertrain.astype(str).str.strip().eq("").any():
            raise ValueError("iZEV empirical composition requires complete observed powertrain labels")
        source_year = pd.to_numeric(selected.model_year, errors="raise")
        if source_year.isna().any() or source_year.mod(1).ne(0).any():
            raise ValueError("Empirical composition needs observed integer vehicle model years")
        selected["year"] = source_year if izev_target_year is None else izev_target_year
        selected["composition_basis"] = "observed_model_year" if izev_target_year is None else "historical_pooled_model_year_window"
        selected["source_model_year_min"] = selected["source_model_year_max"] = source_year
        selected["source_model_years"] = source_year.astype(int).astype(str)
        selected["request_year_min"] = pd.to_numeric(selected.request_year, errors="raise")
        selected["request_year_max"] = selected.request_year_min
        selected["raw_make"], selected["raw_model"] = selected.make, selected.model
        selected["match_make"], selected["match_model"] = selected.make, selected.model
        selected["mapped_canonical_make"], selected["mapped_canonical_model"] = "", ""
        selected["composition_source"] = "transport_canada_izev"
        selected["composition_components"] = selected.get("component", pd.Series("", index=selected.index)).astype(str)
        selected["raw_observation_ids"] = selected.composition_components + ":" + selected.source_row.astype(str)
        selected["source_row_min"] = pd.to_numeric(selected.source_row, errors="raise")
        selected["source_row_max"] = selected.source_row_min
        selected["rating_class"], selected["source_kind"] = "", "izev"
        selected["raw_nrcan_vehicle_classes"] = ""
        selected["identity_exclusion_reason"] = ""
        selected["identity_supported"] = True
        selected["identity_evidence_methods"] = "iZEV_reported_vehicle_description"
        selected["identity_support_reason"] = "source_observed_vehicle_description"
        selected["observed_powertrain"] = selected.powertrain
        selected["vehicle_id"], selected["reference_family_model"] = "", ""
        selected["reference_description"], selected["reference_link_status"] = "", ""
        if reference is not None:
            labels = resolve_source_labels(selected, reference, source="transport_canada_izev",
                make_column="make", model_column="model", year_column="model_year", powertrain_column="powertrain")
            selected["vehicle_id"] = labels.vehicle_id.fillna("")
            selected["reference_family_model"] = labels.family_model
            selected["reference_description"] = labels.canonical_description
            selected["reference_link_status"] = labels.reference_link_status
            selected["match_make"] = labels.family_make
            selected["match_model"] = labels.canonical_description
            selected["mapped_canonical_make"], selected["mapped_canonical_model"] = labels.family_make, labels.family_model
            selected["identity_exclusion_reason"] = labels.identity_exclusion_reason
            selected["identity_supported"] = labels.vehicle_id.notna()
        frames.append(selected[columns])
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    if combined.year.isna().any() or combined.year.mod(1).ne(0).any():
        raise ValueError("Empirical composition needs observed integer vehicle model years")
    if combined[["raw_make", "raw_model", "province"]].isna().any().any():
        raise ValueError("Empirical composition requires complete make/model and province")
    canonical = (_canonical_labels(combined.match_make, combined.match_model, spec) if reference is None else
                 list(zip(combined.match_make, combined.reference_description, strict=True)))
    if reference is None and (spec.get("izev_aliases") or spec.get("izev_observation_aliases_by_powertrain")):
        incentive_rows = combined.source_kind.eq("izev")
        descriptions = list(combined.loc[incentive_rows, ["raw_make", "raw_model", "observed_powertrain"]]
                            .astype(str).itertuples(index=False, name=None))
        translations = {description: canonical_izev_observation(*description, spec) for description in set(descriptions)}
        for index, description in zip(combined.index[incentive_rows], descriptions, strict=True):
            canonical[index] = translations[description]
    combined["family_make"] = [value[0] for value in canonical]
    combined["family_model"] = [value[1] for value in canonical]
    combined["_make_key"] = combined.family_make.map(normalize_vehicle_text)
    combined["_model_key"] = combined.family_model.map(normalize_vehicle_text)
    if combined[["_make_key", "_model_key"]].eq("").any().any():
        raise ValueError("Empirical composition requires nonblank make/model identities")
    keys = ["source_kind", "province", "year", "rating_class", "observed_powertrain", "_make_key", "_model_key",
            "identity_exclusion_reason", "transaction_eligible", "identity_supported", "vehicle_id", "reference_link_status"]
    grouped = combined.groupby(keys, as_index=False, dropna=False, sort=True).agg(
        family_make=("family_make", "first"), family_model=("family_model", "first"),
        raw_make=("raw_make", _labels), raw_model=("raw_model", _labels),
        mapped_canonical_make=("mapped_canonical_make", _labels), mapped_canonical_model=("mapped_canonical_model", _labels),
        raw_observation_ids=("raw_observation_ids", _source_references), source_count=("source_count", "sum"),
        raw_identity_count=("raw_model", "nunique"), source_rows=("source_count", "size"),
        request_year_min=("request_year_min", "min"), request_year_max=("request_year_max", "max"),
        composition_source=("composition_source", "first"), composition_components=("composition_components", _labels),
        source_row_min=("source_row_min", "min"), source_row_max=("source_row_max", "max"),
        raw_nrcan_vehicle_classes=("raw_nrcan_vehicle_classes", _labels),
        transaction_types=("transaction_types", _labels),
        identity_evidence_methods=("identity_evidence_methods", _labels),
        identity_support_reason=("identity_support_reason", _labels),
        composition_basis=("composition_basis", "first"),
        source_model_year_min=("source_model_year_min", "min"),
        source_model_year_max=("source_model_year_max", "max"),
        source_model_years=("source_model_years", _labels),
        reference_family_model=("reference_family_model", "first"),
    )
    grouped["duplicate_identity_merged"] = grouped.raw_identity_count.gt(1)
    grouped["empirical_observation_id"] = [f"empirical_observation_{number:08d}" for number in range(len(grouped))]
    return grouped


def _historical_observations(records: pd.DataFrame, baseline: pd.DataFrame, spec: dict,
                             reference: VehicleReference | None = None) -> pd.DataFrame:
    """Pool native window records once, then pair each target to its own edition."""
    minimum, maximum = spec["izev_model_year_min"], spec["izev_model_year_max"]
    targets: dict[int, set[str]] = {}
    for year, powertrain in baseline.loc[
        baseline.year.lt(minimum) & baseline.powertrain.str.startswith(("bev_", "phev_")), ["year", "powertrain"],
    ].itertuples(index=False, name=None):
        targets.setdefault(int(year), set()).add(str(powertrain).split("_")[0])
    if not targets or records.empty:
        return pd.DataFrame()
    window = records.loc[
        records.model_year.between(minimum, maximum)
        & records.powertrain.isin(["bev", "phev"]) & _transaction_selection(records, spec)
    ]
    if window.empty:
        return pd.DataFrame()
    pooled = _observed_families(pd.DataFrame(), window, spec, izev_target_year=min(targets), reference=reference)
    pooled["source_model_year_min"], pooled["source_model_year_max"] = minimum, maximum
    frames = []
    for target in sorted(targets):
        observations = pooled.loc[pooled.observed_powertrain.isin(targets[target])].copy()
        observations["year"] = target
        observations["empirical_observation_id"] = [
            f"empirical_observation_historical_{target}_{minimum}_{maximum}_{number:08d}"
            for number in range(len(observations))
        ]
        frames.append(observations)
    return pd.concat(frames, ignore_index=True)


def _source_grain_mto_classes(mto: pd.DataFrame, ratings: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """Reuse the accepted historic SUV rollup at the source's coarser grain."""
    if mto.empty:
        return mto
    historical = rules["historical_suv"]
    split = set(historical["combined_weight_classes"])
    unsplit = historical["source_label"]
    source_classes = ratings.groupby("year").rating_class.agg(set)
    years = {year for year, classes in source_classes.items() if unsplit in classes and not split & classes}
    result = mto.copy()
    result["raw_nrcan_vehicle_classes"] = result.nrcan_vehicle_class
    coarse = pd.to_numeric(result.MODEL_YEAR, errors="raise").isin(years) & result.nrcan_vehicle_class.isin(split)
    result.loc[coarse, "nrcan_vehicle_class"] = unsplit
    return result


def _description_prefix(prefix: str, label: str) -> bool:
    """Compare normalized text only at complete model/trim-token boundaries."""
    key, cumulative = normalize_vehicle_text(prefix), ""
    for token in _family_tokens(label):
        cumulative += token
        if cumulative == key:
            return True
    return False


def _body_kind(value: str) -> str:
    text = _normal_text(value).upper()
    for pattern, kind in ((r"\b(?:HATCHBACK|5[ -]?(?:DR|DOOR))\b", "hatchback"),
                          (r"\b(?:SEDAN|4[ -]?(?:DR|DOOR))\b", "sedan"),
                          (r"\bSPORTBACK\b", "sportback"), (r"\bCONVERTIBLE\b", "convertible"),
                          (r"\bWAGON\b", "wagon"), (r"\bCOUPE\b", "coupe")):
        if re.search(pattern, text):
            return kind
    return ""


def _drive_kind(value: str) -> str:
    text = _normal_text(value).upper()
    if re.search(r"\b(?:AWD|4WD|4X4|QUATTRO|ALL4|XDRIVE|4MATIC)\b", text):
        return "all_wheels"
    if re.search(r"\bFWD\b", text):
        return "front_wheels"
    if re.search(r"\bRWD\b", text):
        return "rear_wheels"
    return ""


def _family_structure_compatible(observed: str, candidate: str) -> bool:
    # Numeric model identities are hard boundaries even when text is similar.
    if tuple(re.findall(r"\d+", observed)) != tuple(re.findall(r"\d+", candidate)):
        return False
    # An added character on an otherwise complete model token is a different
    # identity (Falcon/FalconX), rather than a recognized whitespace trim suffix.
    left_tokens, right_tokens = _family_tokens(observed), _family_tokens(candidate)
    if len(left_tokens) == len(right_tokens):
        for left_token, right_token in zip(left_tokens, right_tokens, strict=True):
            if left_token != right_token and (left_token.startswith(right_token) or right_token.startswith(left_token)):
                return False
    left, right = _body_kind(observed), _body_kind(candidate)
    return not (left and right and left != right)


def _candidate_families(candidates: pd.DataFrame, description: str, spec: dict, *,
                        family_boundaries: set[str] | None = None) -> tuple[pd.DataFrame, str]:
    if candidates.empty:
        return candidates, ""
    key = normalize_vehicle_text(description)
    families = candidates[["_candidate_family", "_candidate_key"]].drop_duplicates()
    structural = families._candidate_family.map(lambda label: _family_structure_compatible(description, str(label)))
    makes = set(candidates._make_key) if "_make_key" in candidates else set()
    reviewed_families = (family_boundaries if family_boundaries is not None else
        {normalize_vehicle_text(rule["canonical_model"])
         for rule in spec.get("family_matching", {}).get("family_patterns", [])
         if normalize_vehicle_text(rule["make"]) in makes})
    if key in reviewed_families:
        # Known body/nameplate families remain separate even when one is absent
        # from this edition (Rogue vs Rogue Sport, Corolla vs Corolla Cross).
        structural &= ~families._candidate_key.isin(reviewed_families - {key})
    families = families.loc[structural]
    exact = families.loc[families._candidate_key.eq(key)]
    if not exact.empty:
        return candidates.loc[candidates._candidate_key.isin(exact._candidate_key)], "canonical_family"
    prefix = families.loc[families._candidate_family.map(lambda label: _compatible(description, str(label)))]
    if not prefix.empty:
        return candidates.loc[candidates._candidate_key.isin(prefix._candidate_key)], "recognized_family_extensions"
    policy = spec.get("family_matching", {})
    minimum = policy.get("minimum_similarity")
    if minimum is None or families.empty:
        return candidates.iloc[:0], ""
    families = families.assign(similarity=families._candidate_key.map(lambda value: SequenceMatcher(None, key, value).ratio()))
    families = families.loc[families.similarity.ge(minimum)].sort_values(["similarity", "_candidate_key"], ascending=[False, True])
    if families.empty:
        return candidates.iloc[:0], ""
    best = float(families.similarity.iloc[0])
    competing = families.loc[(best - families.similarity).lt(policy.get("minimum_family_margin", 0)) | families.similarity.eq(best)]
    selected = candidates.loc[candidates._candidate_key.isin(competing._candidate_key)].copy()
    selected["family_similarity"] = selected._candidate_key.map(families.set_index("_candidate_key").similarity)
    return selected, "fuzzy_family_candidates"


def _constrain_configurations(candidates: pd.DataFrame, description: str, family: str) -> tuple[pd.DataFrame, str]:
    """Use explicit source configuration only; absent trim details permit a median."""
    selected = candidates
    text = _normal_text(description)
    constraints = [("drivetrain", _drive_kind(text), candidates.Model.map(_drive_kind)),
                   ("body", _body_kind(text), candidates.Model.map(_body_kind))]
    for name, wanted, values in constraints:
        if wanted:
            if not values.ne("").any():
                return selected.iloc[:0], "unverified_explicit_" + name
            selected = selected.loc[values.loc[selected.index].eq(wanted)]
            if selected.empty:
                return selected, "contradictory_" + name
    for phrase in ("Long Range", "Standard Range", "Extended Range", "Performance", "Plaid", "Turbo"):
        pattern = r"(?i)\b" + r"\s*".join(phrase.split()) + r"\b"
        if re.search(pattern, text):
            selected = selected.loc[selected.Model.astype(str).str.contains(pattern, regex=True)]
            if selected.empty:
                return selected, "unverified_explicit_trim"
    battery = re.search(r"(?i)(\d+(?:\.\d+)?)\s*kwh\b", text)
    if battery:
        capacities = selected.Model.astype(str).str.extract(r"(?i)(\d+(?:\.\d+)?)\s*kwh\b", expand=False).astype(float)
        selected = selected.loc[np.isclose(capacities, float(battery[1]), atol=.05)]
        if selected.empty:
            return selected, "unverified_explicit_battery_capacity"
    # Parse attributes after the supported model identity. In FIAT 500L the L
    # belongs to the model token; it is not a 500-litre engine specification.
    attribute_text = text
    cumulative = ""
    for token in re.finditer(r"[A-Z0-9]+", text.upper()):
        cumulative += token[0]
        if cumulative == normalize_vehicle_text(family):
            attribute_text = text[token.end():]
            break
    engine = re.search(r"(?i)\b(\d+(?:\.\d+)?)\s*(?:L|litre|liter)\b", attribute_text)
    if engine:
        values = pd.to_numeric(selected.get("Engine size (L)", pd.Series(np.nan, index=selected.index)), errors="coerce")
        selected = selected.loc[np.isclose(values, float(engine[1]), atol=.05)]
        if selected.empty:
            return selected, "unverified_explicit_engine"
    # Configured family reduction must not erase an explicitly named numeric
    # trim (Q4 45 vs Q4 55, Model S 75D vs 100D). Model-family numbers remain
    # structural boundaries upstream; additional numbers constrain variants.
    family_numbers = list(re.findall(r"\d+(?:\.\d+)?", family))
    numeric_text = re.sub(r"(?i)\b\d+(?:\.\d+)?\s*(?:kwh|L|litre|liter)\b", "", text)
    numeric_text = re.sub(r"(?i)\b(?:4WD|4X4|2WD|4MATIC|ALL4|[45][ -]?(?:DR|DOOR))\b", "", numeric_text)
    explicit_numbers = list(re.findall(r"\d+(?:\.\d+)?", numeric_text))
    for number in family_numbers:
        if number in explicit_numbers:
            explicit_numbers.remove(number)
    for number in explicit_numbers:
        selected = selected.loc[selected.Model.astype(str).map(
            lambda label: number in re.findall(r"\d+(?:\.\d+)?", label))]
        if selected.empty:
            return selected, "unverified_explicit_numeric_trim"
    # Shared family patterns may deliberately erase a trim suffix. Preserve any
    # remaining explicit tokens (SE/XSE, Plus, Preferred, S, 75D) rather than
    # silently broadening that observation to every trim in the family.
    matched_family = family
    if not selected.empty and selected._candidate_key.nunique() == 1:
        candidate_family = str(selected._candidate_family.iloc[0])
        if _compatible(family, candidate_family):
            matched_family = candidate_family
    residual = _residual_trim_tokens(text, matched_family)
    for token in residual:
        selected = selected.loc[selected.Model.astype(str).map(lambda label: token in _family_tokens(label))]
        if selected.empty:
            return selected, "unverified_explicit_trim_tokens"
    return selected, ""


def _residual_trim_tokens(description: str, family: str) -> tuple[str, ...]:
    """Find explicit suffix evidence after already-checked equivalent attributes."""
    handled = [
        r"(?i)\b\d+(?:\.\d+)?\s*(?:kwh|L|litre|liter)\b",
        r"(?i)\b(?:AWD|FWD|RWD|4WD|4X4|QUATTRO|ALL4|XDRIVE|4MATIC)\b",
        r"(?i)\b(?:HATCHBACK|SEDAN|SPORTBACK|CONVERTIBLE|WAGON|COUPE|[45][ -]?(?:DR|DOOR))\b",
        r"(?i)\b(?:Long\s*Range|Standard\s*Range|Extended\s*Range|Performance|Plaid|Turbo)\b",
    ]
    source, canonical = description, family
    for pattern in handled:
        source, canonical = re.sub(pattern, " ", source), re.sub(pattern, " ", canonical)
    source_tokens, family_tokens = list(_family_tokens(source)), _family_tokens(canonical)
    family_key, cumulative = "".join(family_tokens), ""
    if not family_key:
        return tuple(source_tokens)
    # Formatting may join/split model tokens (Model3 / Model 3).
    for index, token in enumerate(source_tokens):
        cumulative += token
        if cumulative == family_key:
            return tuple(source_tokens[index + 1:])
    # A numeric trim can occur inside a named family (Q4 45 e-tron).
    for token in family_tokens:
        if token in source_tokens:
            source_tokens.remove(token)
    return tuple(source_tokens)


def _representative_range(chosen: pd.DataFrame, family: str, range_rules: Any, conversions: dict | None) -> tuple[str, float]:
    distinct = chosen.drop_duplicates("rating_variant_id")
    median = float(pd.to_numeric(distinct.range_km, errors="coerce").median())
    if range_rules is None or conversions is None:
        if chosen.powertrain.nunique() == 1:
            return str(chosen.powertrain.iloc[0]), median
        raise ValueError("Family-median range allocation requires the owning range rules and conversions")
    if not np.isfinite(median) or median <= 0:
        raise ValueError("Family-median range allocation requires positive native range evidence")
    from parameterization.ldv_ev_ranges import range_bucket

    return range_bucket(family.upper(), median / conversions["length"]["mile_to_km"], range_rules), median


def _phev_assigned_components(chosen: pd.DataFrame, powertrain: str, conversions: dict | None,
                              uf_by_powertrain: dict | None) -> pd.DataFrame:
    result = chosen.copy()
    if result.powertrain.eq(powertrain).all():
        return result
    required = {"cd_electricity_kwh_per_100km", "cd_fuel_l_per_100km", "cs_fuel_l_per_100km"}
    if conversions is None or uf_by_powertrain is None or powertrain not in uf_by_powertrain or required - set(result):
        raise ValueError("PHEV family range reassignment requires CD/CS components and assigned-bucket utility factor")
    uf = float(uf_by_powertrain[powertrain])
    if not np.isfinite(uf) or not 0 <= uf <= 1:
        raise ValueError("Assigned PHEV utility factor is invalid")
    components = result[list(required)].apply(pd.to_numeric, errors="raise")
    if components.isna().any().any() or not np.isfinite(components).all().all() or components.lt(0).any().any():
        raise ValueError("Assigned PHEV family components must be finite nonnegative values")
    hhv: float | pd.Series = conversions["fuel_hhv"]["gasoline_mj_per_litre"]
    if "Fuel type 2" in result:
        hhv = result["Fuel type 2"].map({
            "X": conversions["fuel_hhv"]["gasoline_mj_per_litre"],
            "Z": conversions["fuel_hhv"]["gasoline_mj_per_litre"],
            "D": conversions["fuel_hhv"].get("diesel_mj_per_litre"),
        })
        if hhv.isna().any():
            raise ValueError("PHEV family reassignment requires a supported native liquid-fuel identity")
    per_km = conversions["service"]["per_100_km_to_per_km"]
    result["fleet_utility_factor"] = uf
    result["utility_factor_basis"] = "assigned_family_median_range_bucket"
    result["utility_weighted_electricity_mj_per_vkm"] = uf * result.cd_electricity_kwh_per_100km * conversions["energy"]["kwh_to_mj"] * per_km
    result["utility_weighted_fuel_mj_per_vkm"] = (uf * result.cd_fuel_l_per_100km + (1 - uf) * result.cs_fuel_l_per_100km) * hhv * per_km
    result["consumption_mj_per_vkm"] = result.utility_weighted_electricity_mj_per_vkm + result.utility_weighted_fuel_mj_per_vkm
    return result


def _representative_class(chosen: pd.DataFrame, spec: dict) -> tuple[str, str, float]:
    """Resolve minor same-year class disagreements without pretending market shares.

    Every distinct compatible configuration remains in the consumption median.
    A predominant class changes only the family's class allocation. Explicit
    sedan/hatchback or other body conflicts cannot be suppressed by frequency.
    """
    distinct = chosen.drop_duplicates("rating_variant_id")
    counts = distinct.rating_class.value_counts()
    winner, share = str(counts.index[0]), float(counts.iloc[0] / counts.sum())
    if len(counts) == 1:
        return winner, "unanimous_target_year_class", share
    policy = spec.get("family_matching", {}).get("class_resolution", {})
    body_labels = {value for value in distinct.Model.map(_body_kind) if value}
    compatible = any(set(counts.index).issubset(group) for group in policy.get("compatible_class_groups", []))
    if (policy and compatible and len(body_labels) <= 1
            and share >= policy["minimum_predominant_share"]):
        return winner, "predominant_distinct_target_year_configurations", share
    return "", "conflicting_target_year_classes_or_bodies", share


def _relative_consumption_span(chosen: pd.DataFrame) -> float:
    """Central 80-percent catalogue spread relative to its median, not uncertainty."""
    values = pd.to_numeric(chosen.drop_duplicates("rating_variant_id").consumption_mj_per_vkm,
                           errors="raise").dropna()
    if values.empty:
        return np.nan
    median = float(values.median())
    if not np.isfinite(values).all() or values.le(0).any():
        raise ValueError("Representative family consumption must be finite and positive")
    return float((values.quantile(.9) - values.quantile(.1)) / median)


def _match_observations(
    observed: pd.DataFrame, ratings: pd.DataFrame, spec: dict, *,
    range_rules: Any = None, conversions: dict | None = None, uf_by_powertrain: dict | None = None,
    catalogue_evidence: pd.DataFrame | None = None,
    reference: VehicleReference | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    result, matches, all_candidates = [], {}, {}
    family_boundaries = {}
    if reference is not None:
        for row in reference.canonical.loc[reference.canonical.family_boundary].itertuples(index=False):
            family_boundaries.setdefault(normalize_vehicle_text(row.canonical_make), set()).add(
                normalize_vehicle_text(row.canonical_model))
    candidate_cache: dict[tuple, tuple[pd.DataFrame, str, str, pd.DataFrame]] = {}
    ratings = ratings.copy()
    izev_spec = {**spec, "aliases": [*spec.get("aliases", []), *spec.get("izev_aliases", [])]}
    evidence_input = ratings if catalogue_evidence is None else catalogue_evidence
    izev_labels = ({pair: catalogue_family(*pair, izev_spec)[1]
                    for pair in set(zip(evidence_input.Make, evidence_input.Model, strict=True))}
                   if reference is None else {})
    ratings["_izev_family_model"] = (ratings.family_model if reference is not None else
        [izev_labels[pair] for pair in zip(ratings.Make, ratings.Model, strict=True)])
    catalog = {(int(year), make): group for (year, make), group in ratings.groupby(["year", "_make_key"], sort=False)}
    # Retain excluded fuels/propulsions for explanations, not for allocation.
    evidence = catalogue_family_labels(evidence_input, spec)
    evidence["_make_key"] = evidence.family_make.map(normalize_vehicle_text)
    evidence["_izev_family_model"] = (evidence.family_model if reference is not None else
        [izev_labels[pair] for pair in zip(evidence.Make, evidence.Model, strict=True)])
    # Conventional X/Z rows with unverified/conflicting complementary evidence
    # remain source evidence, but are neither proven gasoline nor proven HEV.
    # Native BEV/PHEV frames need not carry this conventional-only flag.
    evidence["unresolved_propulsion_evidence"] = evidence.get(
        "propulsion_resolved", pd.Series(True, index=evidence.index)).eq(False).fillna(False)
    unresolved = evidence.unresolved_propulsion_evidence & evidence.powertrain.eq("gasoline")
    unresolved &= ~evidence.Model.astype(str).str.contains(r"(?i)\b(?:hybrid|mhev|hev|phev|bev|electric|plug[ -]?in)\b")
    if "Fuel type" in evidence:
        unresolved &= evidence["Fuel type"].isin(spec["mto_selection"].get("eligible_fuel_codes", ["X", "Z"]))
    unresolved_catalog = {
        (int(year), make): group
        for (year, make), group in evidence.loc[unresolved].groupby(["year", "_make_key"], sort=False)
    }
    evidence = evidence[["year", "_make_key", "family_model", "_izev_family_model",
                         "unresolved_propulsion_evidence", *(["vehicle_id"] if reference is not None else [])]].drop_duplicates()
    evidence_catalog = {make: group for make, group in evidence.groupby("_make_key", sort=False)}
    for observation in observed.to_dict("records"):
        item = dict(observation)
        cache_key = tuple(item[field] for field in ("source_kind", "year", "_make_key", "family_model", "rating_class", "observed_powertrain", "vehicle_id"))
        source_spec = izev_spec if item["source_kind"] == "izev" else spec
        source_family = (item["reference_family_model"] if reference is not None else
                         catalogue_family(item["family_make"], item["family_model"], source_spec)[1])
        if cache_key not in candidate_cache:
            candidates = catalog.get((int(item["year"]), item["_make_key"]), ratings.iloc[:0]).copy()
            if reference is not None:
                candidates = candidates.loc[candidates.vehicle_id.eq(item["vehicle_id"])].copy()
            if item["source_kind"] == "mto":
                eligible = candidates.powertrain.eq("gasoline") & ~candidates.Model.astype(str).str.contains(r"(?i)\b(?:hybrid|mhev|hev|phev|bev|electric|plug[ -]?in)\b")
                if "Fuel type" in candidates:
                    codes = candidates["Fuel type"]
                    eligible &= codes.isna() | codes.isin(spec["mto_selection"].get("eligible_fuel_codes", ["X", "Z"]))
                candidates = candidates.loc[eligible].copy()
                candidates["_candidate_family"] = candidates.family_model
            else:
                candidates = candidates.loc[candidates._powertrain_family.eq(item["observed_powertrain"])].copy()
                candidates["_candidate_family"] = candidates._izev_family_model
            candidates["_candidate_key"] = candidates._candidate_family.map(normalize_vehicle_text)
            candidates, method = _candidate_families(candidates, source_family, source_spec)
            unresolved_identity = evidence.iloc[:0].copy()
            if item["source_kind"] == "mto" and not candidates.empty:
                # Eligibility cannot silently settle an unspecified body/family
                # identity when a corresponding native sibling has unresolved
                # propulsion. Positive HEV/diesel/E85 alternatives do not enter.
                unresolved_identity = unresolved_catalog.get(
                    (int(item["year"]), item["_make_key"]), unresolved_identity).copy()
                unresolved_identity["_candidate_family"] = unresolved_identity.family_model
                unresolved_identity["_candidate_key"] = unresolved_identity._candidate_family.map(normalize_vehicle_text)
                unresolved_identity, identity_method = _candidate_families(unresolved_identity, source_family, source_spec,
                    family_boundaries=family_boundaries.get(item["_make_key"], set()) if reference is not None else None)
                if identity_method in {"canonical_family", "recognized_family_extensions"}:
                    unresolved_identity, _ = _constrain_configurations(
                        unresolved_identity, item["family_model"], source_family)
                else:
                    unresolved_identity = unresolved_identity.iloc[:0]
            absence = "unmatched_ratings"
            if candidates.empty:
                known = evidence_catalog.get(item["_make_key"], evidence.iloc[:0]).copy()
                if reference is not None:
                    known = known.loc[known.vehicle_id.eq(item["vehicle_id"])]
                known["_candidate_family"] = (known._izev_family_model if item["source_kind"] == "izev"
                                               else known.family_model)
                known["_candidate_key"] = known._candidate_family.map(normalize_vehicle_text)
                known, known_method = _candidate_families(known, source_family, source_spec)
                if known_method in {"canonical_family", "recognized_family_extensions"} and not known.empty:
                    target = known.loc[known.year.eq(item["year"])]
                    if target.empty:
                        absence = "target_year_rating_unavailable"
                    elif item["source_kind"] == "mto" and target.unresolved_propulsion_evidence.any():
                        absence = "unresolved_propulsion_evidence"
                    else:
                        absence = "ineligible_observed_propulsion"
                if reference is not None:
                    canonical = reference.canonical
                    excluded_year = (canonical.vehicle_id.eq(item["vehicle_id"])
                        & canonical.model_year_from.le(item["year"]) & canonical.model_year_to.ge(item["year"])
                        & canonical.evidence_status.eq("model_not_produced"))
                    if excluded_year.any():
                        absence = "model_year_incompatible"
            candidate_cache[cache_key] = candidates, method, absence, unresolved_identity
        candidates, method, absence, unresolved_identity = candidate_cache[cache_key]
        all_candidates[item["empirical_observation_id"]] = candidates
        item.update(match_status=absence, match_method=method, resolved_powertrain="",
            candidate_variant_count=int(candidates.rating_variant_id.nunique()), candidate_rating_classes=_labels(candidates.rating_class),
            candidate_powertrains=_labels(candidates.powertrain), candidate_models=_labels(candidates.Model), selected=False,
            candidate_families=_labels(candidates._candidate_family) if "_candidate_family" in candidates else "",
            matched_variant_count=0, range_assignment="not_assigned", representative_statistic="family_median",
            observed_family_native_mean=np.nan, observed_family_native_median=np.nan, observed_family_mj_per_vkm=np.nan,
            observed_family_range_median_km=np.nan, matched_family_make="", matched_family_model="",
            representative_bucket_has_native_trim=True, configuration_status="unspecified_configurations_use_family_median",
            class_resolution_status="not_resolved", class_predominant_share=np.nan,
            consumption_relative_p90_p10_span=np.nan,
            family_candidate_count=int(candidates._candidate_key.nunique()) if "_candidate_key" in candidates else 0)
        if not candidates.empty:
            chosen, conflict = _constrain_configurations(candidates, item["family_model"], source_family)
            item["matched_variant_count"] = int(chosen.rating_variant_id.nunique())
            rating_class, class_status, class_share = _representative_class(chosen, spec) if not chosen.empty else ("", "not_resolved", np.nan)
            item.update(class_resolution_status=class_status, class_predominant_share=class_share)
            if conflict:
                item["match_status"] = "conflicting_or_unverified_configuration"
                item["configuration_status"] = conflict
            elif not unresolved_identity.empty and not unresolved_identity._candidate_key.isin(chosen._candidate_key).all():
                item["match_status"] = "ambiguous_model_family"
                item["configuration_status"] = "unresolved_propulsion_competing_family"
                families = pd.concat([chosen._candidate_family, unresolved_identity._candidate_family])
                item["candidate_families"] = _labels(families)
                item["family_candidate_count"] = families.map(normalize_vehicle_text).nunique()
                item["candidate_models"] = _labels(pd.concat([candidates.Model, unresolved_identity.Model]))
            elif chosen._candidate_key.nunique() > 1:
                item["match_status"] = "ambiguous_model_family"
            elif method == "fuzzy_family_candidates":
                item["match_status"] = "unverified_model_family"
            elif not rating_class:
                item["match_status"] = "ambiguous_rating_class"
            elif item["source_kind"] == "mto" and rating_class != item["rating_class"]:
                item["match_status"] = "conflicting_reviewed_rating_class"
            elif (item["source_kind"] == "mto" and "ambiguous_epa_match" in chosen and chosen.ambiguous_epa_match.fillna(False).any()):
                item["match_status"] = "ambiguous_powertrain_evidence"
            else:
                item["match_status"] = "resolved"
                item["rating_class"] = rating_class
                item["matched_family_make"] = str(chosen.family_make.iloc[0])
                item["matched_family_model"] = str(chosen._candidate_family.iloc[0])
                assigned = str(chosen.powertrain.iloc[0])
                if item["source_kind"] == "izev":
                    assigned, median_range = _representative_range(chosen, item["observed_powertrain"], range_rules, conversions)
                    item["observed_family_range_median_km"] = median_range
                    item["range_assignment"] = "family_median_range" if chosen.rating_variant_id.nunique() > 1 else "single_configuration_range"
                    item["representative_bucket_has_native_trim"] = bool(chosen.powertrain.eq(assigned).any())
                    if item["observed_powertrain"] == "phev":
                        chosen = _phev_assigned_components(chosen, assigned, conversions, uf_by_powertrain)
                item["resolved_powertrain"] = assigned
                span = _relative_consumption_span(chosen)
                item["consumption_relative_p90_p10_span"] = span
                maximum_span = spec.get("family_matching", {}).get("consumption_dispersion", {}).get("maximum_relative_p90_p10_span")
                if maximum_span is not None and span > maximum_span:
                    item["match_status"] = "family_consumption_heterogeneity"
                distinct = chosen.drop_duplicates("rating_variant_id")
                item["observed_family_native_median"] = float(distinct.native_consumption.median())
                item["observed_family_native_mean"] = item["observed_family_native_median"]
                item["observed_family_mj_per_vkm"] = float(distinct.consumption_mj_per_vkm.median()) if distinct.consumption_mj_per_vkm.notna().any() else np.nan
                chosen = chosen.copy()
                chosen["catalogue_rating_class"] = chosen.rating_class
                chosen["rating_class"] = rating_class
                chosen["powertrain"] = assigned
                chosen["range_assignment"] = item["range_assignment"]
                if item["match_status"] == "resolved":
                    matches[item["empirical_observation_id"]] = chosen
        if (item["source_kind"] == "izev" and item["composition_basis"] == "observed_model_year" and not
                spec["izev_model_year_min"] <= item["year"] <= spec["izev_model_year_max"]):
            item["match_status"] = "outside_supported_program_window"
            matches.pop(item["empirical_observation_id"], None)
        if item["source_kind"] == "izev" and item["observed_powertrain"] not in {"bev", "phev"}:
            item["match_status"] = "unsupported_observed_powertrain"
            matches.pop(item["empirical_observation_id"], None)
        if item["identity_exclusion_reason"] and item["match_status"] != "outside_supported_program_window":
            item["match_status"] = "unresolved_mapping_identity"
            matches.pop(item["empirical_observation_id"], None)
        if item["source_kind"] == "mto" and item.get("reference_link_status") == "configuration_unresolved":
            item["match_status"] = "conflicting_or_unverified_configuration"
            item["configuration_status"] = "unverified_source_body_or_performance"
            matches.pop(item["empirical_observation_id"], None)
        if item["source_kind"] == "izev" and not item["transaction_eligible"]:
            item["match_status"] = "unsupported_transaction_type"
            matches.pop(item["empirical_observation_id"], None)
        reasons = {
            "resolved": "Supported target-year family; compatible distinct configurations use representative medians.",
            "unmatched_ratings": "No supported family correspondence in the captured NRCan catalogue; identity or rating availability needs evidence.",
            "target_year_rating_unavailable": "Family is present in other captured NRCan years, but its target-year eligible rating is unavailable; catalogue absence does not establish model nonexistence.",
            "model_year_incompatible": "Reviewed identity evidence establishes that this model did not exist in the target model year.",
            "ineligible_observed_propulsion": ("Target-year family has no eligible conventional-gasoline X/Z configuration for this gasoline-only MTO estimator."
                                               if item["source_kind"] == "mto" else "Target-year family has no eligible configuration for the source-reported propulsion."),
            "unresolved_propulsion_evidence": "Target-year family ratings exist, but complementary configuration evidence is unavailable or conflicting; X/Z fuel codes alone do not establish conventional gasoline.",
            "conflicting_or_unverified_configuration": (
                "The source observation does not establish the specific body or performance variant represented by the ratings; its family and stock class remain supported."
                if item["configuration_status"] == "unverified_source_body_or_performance" else
                "Explicit source configuration is conflicting or unavailable in the target-year ratings: " + item["configuration_status"].replace("_", " ") + "."),
            "ambiguous_model_family": (
                "Description also corresponds to a target-year body/family whose complementary propulsion evidence is unresolved; eligible ratings alone cannot establish that the observation belongs to the remaining family."
                if item["configuration_status"] == "unresolved_propulsion_competing_family"
                else "Description corresponds to multiple model families; trim similarity does not establish family identity."),
            "unverified_model_family": "Only similar model names were found; similarity alone does not establish the family correspondence.",
            "ambiguous_rating_class": "Eligible target-year configurations have conflicting classes or body identities without a supported predominant class.",
            "conflicting_reviewed_rating_class": "Eligible target-year family class conflicts with the authoritative MTO mapping interval.",
            "ambiguous_powertrain_evidence": "Complementary propulsion evidence conflicts at the compatible configuration level.",
            "family_consumption_heterogeneity": "Compatible target-year configurations have material consumption dispersion beyond the configured representative-family limit.",
            "outside_supported_program_window": "Observation model year is outside the configured complete iZEV composition window.",
            "unsupported_observed_powertrain": "Source-reported propulsion is outside the BEV/PHEV iZEV estimator.",
            "unresolved_mapping_identity": str(item["identity_exclusion_reason"]),
            "unsupported_transaction_type": "Source transaction is outside the configured purchase/lease observations.",
        }
        item["match_reason"] = reasons[item["match_status"]]
        result.append(item)
    return pd.DataFrame(result), matches, all_candidates


def _select_families(observations: pd.DataFrame, spec: dict) -> pd.DataFrame:
    result = observations.copy()
    eligible = result.match_status.eq("resolved")
    for field in ("resolved_gasoline_count", "identity_supported_count", "identity_supported_resolved_share"):
        result[field] = np.nan
    result["minimum_resolved_count_share"] = spec["mto_selection"]["minimum_resolved_count_share"]
    result["identity_coverage_met"] = False
    result["selection_status"] = result.match_status
    for _, group in result.loc[eligible].groupby(
        ["source_kind", "province", "year", "rating_class", "resolved_powertrain", "composition_basis"], sort=True,
    ):
        selected = group.index
        if group.source_kind.iloc[0] == "mto":
            resolved = float(group.source_count.sum())
            supported = float(group.loc[group.identity_supported, "source_count"].sum())
            share = supported / resolved if resolved else 0.
            coverage_met = supported > 0 and share >= spec["mto_selection"]["minimum_resolved_count_share"]
            result.loc[group.index, ["resolved_gasoline_count", "identity_supported_count", "identity_supported_resolved_share"]] = [resolved, supported, share]
            result.loc[group.index, "identity_coverage_met"] = coverage_met
            result.loc[group.index[~group.identity_supported], "selection_status"] = "unsupported_mapping_identity"
            result.loc[group.index[group.identity_supported], "selection_status"] = "insufficient_identity_supported_mass"
            selected = group.index[group.identity_supported] if coverage_met else group.index[:0]
        result.loc[selected, "selected"] = True
    result.loc[result.selected, "selection_status"] = "selected"
    return result


def _izev_pool_choices(records: pd.DataFrame, baseline: pd.DataFrame, regions: list[str], spec: dict):
    """Choose one geography before inspecting any subclass or range matches."""
    policy = spec["izev_pool_selection"]
    national = tuple(policy["national_provinces"])
    transactions = records.iloc[:0]
    counts = {}
    if not records.empty:
        required = {"province", "model_year", "powertrain", "count"}
        if missing := required - set(records):
            raise ValueError(f"iZEV composition missing columns: {sorted(missing)}")
        transactions = records.loc[_transaction_selection(records, spec)].copy()
        transactions["count"] = pd.to_numeric(transactions["count"], errors="raise")
        counts = transactions.groupby(["province", "model_year", "powertrain"])["count"].sum().to_dict()
    targets = {
        (int(year), str(powertrain).split("_")[0])
        for year, powertrain in baseline[["year", "powertrain"]].itertuples(index=False, name=None)
        if str(powertrain).startswith(("bev_", "phev_"))
    }
    if not records.empty:
        targets.update((int(year), powertrain) for year, powertrain in
                       records.loc[records.powertrain.isin(["bev", "phev"]), ["model_year", "powertrain"]]
                       .drop_duplicates().itertuples(index=False, name=None))
    minimum, maximum = spec["izev_model_year_min"], spec["izev_model_year_max"]
    historical_targets = {
        (int(year), str(powertrain).split("_")[0])
        for year, powertrain in baseline.loc[baseline.year.lt(minimum), ["year", "powertrain"]].itertuples(index=False, name=None)
        if str(powertrain).startswith(("bev_", "phev_"))
    }
    choices, needed = {}, set()
    for region in regions:
        provinces = _region_provinces(region, spec)
        needed.update(provinces)
        for year, powertrain in sorted(targets):
            provincial_count = sum(counts.get((province, year, powertrain), 0) for province in provinces)
            use_national = (spec["izev_model_year_min"] <= year <= spec["izev_model_year_max"]
                            and provincial_count < policy["minimum_observations"])
            selected = national if use_national else provinces
            needed.update(selected)
            choices[region, year, powertrain, "observed_model_year"] = {
                "pool_geography": "national" if use_national else "provincial",
                "pool_provinces": " | ".join(selected), "_pool_provinces": selected,
                "provincial_observation_count": float(provincial_count),
                "pool_observation_count": float(sum(counts.get((province, year, powertrain), 0) for province in selected)),
                "minimum_observations": policy["minimum_observations"],
                "composition_basis": "observed_model_year",
                "source_model_year_min": year, "source_model_year_max": year,
                "source_model_years": str(year) if any(counts.get((province, year, powertrain), 0) for province in selected) else "",
            }
        for year, powertrain in sorted(historical_targets):
            provincial_count = sum(counts.get((province, source_year, powertrain), 0)
                                   for province in provinces for source_year in range(minimum, maximum + 1))
            use_national = provincial_count < policy["minimum_observations"]
            selected = national if use_national else provinces
            needed.update(selected)
            choices[region, year, powertrain, "historical_pooled_model_year_window"] = {
                "pool_geography": "national" if use_national else "provincial",
                "pool_provinces": " | ".join(selected), "_pool_provinces": selected,
                "provincial_observation_count": float(provincial_count),
                "pool_observation_count": float(sum(counts.get((province, source_year, powertrain), 0)
                                                     for province in selected for source_year in range(minimum, maximum + 1))),
                "minimum_observations": policy["minimum_observations"],
                "composition_basis": "historical_pooled_model_year_window",
                "source_model_year_min": minimum, "source_model_year_max": maximum,
                "source_model_years": " | ".join(str(source_year) for source_year in range(minimum, maximum + 1)
                                                   if any(counts.get((province, source_year, powertrain), 0) for province in selected)),
            }
    selected_records = records.loc[records.province.isin(needed)] if not records.empty else records
    return selected_records, choices


def _regional_observations(disposition: pd.DataFrame, region: str, choices: dict, spec: dict) -> pd.DataFrame:
    if disposition.empty:
        return disposition.copy()
    provinces = _region_provinces(region, spec)
    records = []
    for item in disposition.to_dict("records"):
        if item["source_kind"] == "mto":
            pool = {"pool_geography": "mto_ontario_proxy", "pool_provinces": "ON",
                    "provincial_observation_count": np.nan, "pool_observation_count": np.nan,
                    "minimum_observations": spec["izev_pool_selection"]["minimum_observations"]}
        else:
            pool = choices.get((region, int(item["year"]), item["observed_powertrain"], item["composition_basis"]), {
                "pool_geography": "provincial", "pool_provinces": " | ".join(provinces),
                "_pool_provinces": provinces, "provincial_observation_count": 0.,
                "pool_observation_count": 0., "minimum_observations": spec["izev_pool_selection"]["minimum_observations"],
            })
            if item["province"] not in pool["_pool_provinces"]:
                continue
        records.append({**item, **{key: value for key, value in pool.items()
                                  if not key.startswith("_") and key != "source_model_years"},
                        "region": region, "relative_contribution": 0.})
    return pd.DataFrame(records)


def aggregate_empirical_ratings(
    classified: pd.DataFrame,
    class_weights: pd.DataFrame,
    mto_stock: pd.DataFrame,
    izev_records: pd.DataFrame,
    *,
    rules: dict[str, Any],
    composition_rules: dict[str, Any],
    regions: list[str],
    weight_basis: str,
    range_rules: Any = None, conversions: dict | None = None, uf_by_powertrain: dict | None = None,
    reference: VehicleReference | None = None,
) -> RatingComposition:
    """Use counted observations where supported and distinct-model medians elsewhere.

    Purchase/lease geography is selected for a whole model-year/BEV-or-PHEV pool.
    Precoverage vintages reuse the whole supported iZEV window's composition and
    pair those descriptions anew with each target NRCan edition's consumption.
    Unsupported empirical subclasses retain nested family-median consumption
    and their original outer weights. A clear electrified family is assigned by
    its median eligible native range. Conflicting identities stay unallocated;
    counts remain incentive/stock composition proxies.
    """
    from parameterization.road_efficiencies import aggregate_ratings

    if reference is not None:
        overrides = {"aliases", "izev_aliases", "izev_observation_aliases_by_powertrain"} & set(composition_rules)
        if "family_patterns" in composition_rules.get("family_matching", {}):
            overrides.add("family_matching.family_patterns")
        if overrides:
            raise ValueError("Production vehicle identity overrides must migrate to vehicle_source_links.csv and vehicle_reference.csv: "
                             + ", ".join(sorted(overrides)))
    _validate_policy(composition_rules)
    if reference is not None:
        classified = attach_catalogue_identities(classified, reference)
    if not regions or len(regions) != len(set(regions)):
        raise ValueError("Empirical ratings require distinct selected regions")
    selected_class_weights = class_weights.loc[class_weights.weight_basis.eq(weight_basis)]
    if selected_class_weights.groupby("nrcan_vehicle_class").nrcan_ceud_class.nunique().gt(1).any():
        raise ValueError("An NRCan size subclass cannot allocate empirical counts to multiple CEUD classes")
    if not mto_stock.empty and "report_year" in mto_stock and set(mto_stock.report_year) != set(
        class_weights.loc[class_weights.weight_basis.eq(weight_basis), "report_year"]
    ):
        raise ValueError("MTO family counts and reviewed class weights use different report editions")
    ratings = _rating_variants(classified, composition_rules)
    baseline = aggregate_ratings(classified, class_weights, rules=rules, weight_basis=weight_basis, statistic="median")
    baseline["representative_bucket_has_native_trim"] = True
    if baseline.empty:
        baseline = pd.DataFrame(columns=["year", "powertrain", "rating_class", "ceud_class"])
    selected_incentives, choices = _izev_pool_choices(izev_records, baseline, regions, composition_rules)
    observed = _observed_families(
        _source_grain_mto_classes(mto_stock, ratings, rules), selected_incentives, composition_rules,
        reference=reference,
    )
    historical = _historical_observations(selected_incentives, baseline, composition_rules, reference)
    if not historical.empty:
        observed = pd.concat([observed, historical], ignore_index=True)
    if observed.empty:
        disposition, matches, candidate_matches = pd.DataFrame(), {}, {}
    else:
        disposition, matches, candidate_matches = _match_observations(observed, ratings, composition_rules,
            range_rules=range_rules, conversions=conversions, uf_by_powertrain=uf_by_powertrain,
            catalogue_evidence=classified, reference=reference)
        disposition = _select_families(disposition, composition_rules)
    class_records, regional_observations, allocations, coverage_records, match_audits = [], [], [], [], []
    for region in regions:
        regional = _regional_observations(disposition, region, choices, composition_rules)
        regional_baseline = baseline.copy()
        if not regional.empty:
            new_cells = []
            selected_observations = regional.loc[regional.selected & regional.match_status.eq("resolved")]
            for (year, size, powertrain), group in selected_observations.groupby(["year", "rating_class", "resolved_powertrain"]):
                present = (regional_baseline.year.eq(year) & regional_baseline.rating_class.eq(size)
                    & regional_baseline.powertrain.eq(powertrain))
                if present.any():
                    continue
                family = str(powertrain).split("_", 1)[0]
                templates = baseline.loc[baseline.year.eq(year) & baseline.rating_class.eq(size)
                    & baseline.powertrain.str.startswith(family + "_")]
                if templates.empty:
                    raise ValueError("Representative range cohort lacks an underlying rated size-class weight")
                template = templates.iloc[0].to_dict()
                template.update(powertrain=powertrain, native_class_mean=float(group.observed_family_native_median.median()),
                    class_mean_mj_per_vkm=float(group.observed_family_mj_per_vkm.median()),
                    range_class_mean_km=float(group.observed_family_range_median_km.median()),
                    representative_bucket_has_native_trim=False, distinct_models=group.matched_family_model.nunique(),
                    rating_statistic="median", observations=int(group.matched_variant_count.sum()))
                new_cells.append(template)
            if new_cells:
                regional_baseline = pd.concat([regional_baseline, pd.DataFrame(new_cells)], ignore_index=True)
        region_records = []
        region_allocations = []
        pool_groups = {}
        if not regional.empty:
            mto_groups = regional.loc[regional.source_kind.eq("mto")].groupby(["year", "rating_class"], sort=False)
            incentive_groups = regional.loc[regional.source_kind.eq("izev")].groupby(
                ["year", "observed_powertrain", "composition_basis"], sort=False,
            )
            pool_groups.update({("mto", year, rating_class): group for (year, rating_class), group in mto_groups})
            pool_groups.update({("izev", year, powertrain, basis): group
                                for (year, powertrain, basis), group in incentive_groups})
        for base in regional_baseline.to_dict("records"):
            item = dict(base, region=region)
            family = str(base["powertrain"]).split("_")[0]
            kind = "izev" if family in {"bev", "phev"} else "mto"
            composition_basis = (
                "historical_pooled_model_year_window" if kind == "izev" and base["year"] < composition_rules["izev_model_year_min"]
                else "observed_model_year" if kind == "izev" else "observed_model_year_stock"
            )
            pool_choice = choices.get((region, int(base["year"]), family, composition_basis), {
                "pool_geography": "mto_ontario_proxy", "pool_provinces": "ON",
                "provincial_observation_count": np.nan, "pool_observation_count": np.nan,
                "minimum_observations": composition_rules["izev_pool_selection"]["minimum_observations"],
                "composition_basis": composition_basis,
                "source_model_year_min": base["year"], "source_model_year_max": base["year"],
                "source_model_years": str(int(base["year"])),
            })
            item.update(
                aggregation_method="catalogue_fallback_median_no_empirical_support",
                composition_source="transport_canada_izev" if kind == "izev" else "ontario_ministry_transport_vehicle_population",
                composition_components="", composition_geography=pool_choice["pool_provinces"] if kind == "izev" else "ON",
                **{key: value for key, value in pool_choice.items() if not key.startswith("_")},
                empirical_families=0, empirical_variant_count=0, observed_source_count=0.0,
                resolved_source_count=0.0, selected_source_count=0.0,
                matched_source_share=np.nan, selected_source_share=np.nan, selected_resolved_share=np.nan,
                denominator_basis=("selected_geography_model_year_powertrain_observations" if kind == "izev"
                                   else "mapped_class_model_year_stock_all_propulsion_unobserved"),
                coverage_status="no_matching_observations", rating_statistic="median",
                representative_statistic="family_median",
                propulsion_basis="iZEV_reported_powertrain" if kind == "izev" else "canonical_family_gasoline_compatibility_not_observed_fuel",
                range_assignment="not_assigned",
                resolved_gasoline_count=0. if family == "gasoline" else np.nan,
                identity_supported_count=0. if family == "gasoline" else np.nan,
                identity_supported_resolved_share=np.nan, identity_supported_all_mapped_share=np.nan,
                minimum_resolved_count_share=composition_rules["mto_selection"]["minimum_resolved_count_share"] if family == "gasoline" else np.nan,
                identity_coverage_met=False if family == "gasoline" else None,
                largest_family_share=np.nan, effective_family_count=np.nan,
            )
            if family in {"hev", "diesel"}:
                item.update(aggregation_method="catalogue_distinct_model_median", rating_statistic="median",
                            coverage_status="configured_distinct_model_median",
                            composition_source="nrcan_fuel_consumption_ratings", composition_geography="NRCan catalogue",
                            pool_geography="catalogue", pool_provinces="", composition_basis="catalogue_only",
                            source_model_year_min=np.nan, source_model_year_max=np.nan, source_model_years="")
            elif family in {"gasoline", "bev", "phev"} and not regional.empty:
                pool = pool_groups.get(
                    ((kind, base["year"], base["rating_class"]) if kind == "mto"
                     else (kind, base["year"], family, composition_basis)),
                    regional.iloc[:0],
                )
                if kind == "izev":
                    pool = pool.loc[pool.transaction_eligible]
                resolved = pool.loc[
                    pool.match_status.eq("resolved")
                    & pool.rating_class.eq(base["rating_class"])
                    & pool.resolved_powertrain.eq(base["powertrain"])
                ]
                selected = resolved.loc[resolved.selected]
                source_count, resolved_count, selected_count = (float(frame.source_count.sum()) for frame in (pool, resolved, selected))
                item.update(
                    observed_source_count=source_count, resolved_source_count=resolved_count,
                    selected_source_count=selected_count,
                    matched_source_share=resolved_count / source_count if source_count else np.nan,
                    selected_source_share=selected_count / source_count if source_count else np.nan,
                    selected_resolved_share=selected_count / resolved_count if resolved_count else np.nan,
                )
                if kind == "mto":
                    supported_count = float(resolved.loc[resolved.identity_supported, "source_count"].sum())
                    supported_share = supported_count / resolved_count if resolved_count else 0.
                    coverage_met = supported_count > 0 and supported_share >= composition_rules["mto_selection"]["minimum_resolved_count_share"]
                    item.update(resolved_gasoline_count=resolved_count, identity_supported_count=supported_count,
                                identity_supported_resolved_share=supported_share,
                                identity_supported_all_mapped_share=supported_count / source_count if source_count else np.nan,
                                identity_coverage_met=coverage_met)
                    if resolved_count > 0 and not coverage_met:
                        item["aggregation_method"] = "catalogue_fallback_median_insufficient_identity_support"
                        item["coverage_status"] = "insufficient_identity_supported_mass"
                if not selected.empty:
                    variant_frames = []
                    for observation in selected.to_dict("records"):
                        contribution = observation["source_count"] / selected_count
                        regional.loc[regional.empirical_observation_id.eq(observation["empirical_observation_id"]), "relative_contribution"] = contribution
                        variants = matches[observation["empirical_observation_id"]].copy()
                        variants["region"] = region
                        variants["province"] = observation["province"]
                        for field in ("pool_geography", "pool_provinces", "provincial_observation_count",
                                      "pool_observation_count", "minimum_observations"):
                            variants[field] = pool_choice[field]
                        for field in ("identity_supported", "identity_evidence_methods", "identity_support_reason",
                                      "identity_supported_resolved_share", "identity_coverage_met", "composition_basis",
                                      "source_model_year_min", "source_model_year_max", "source_model_years",
                              "representative_statistic", "representative_bucket_has_native_trim", "configuration_status",
                              "matched_family_make", "matched_family_model", "family_candidate_count", "match_reason",
                              "class_resolution_status", "class_predominant_share", "consumption_relative_p90_p10_span"):
                            variants[field] = observation[field]
                        variants["empirical_observation_id"] = observation["empirical_observation_id"]
                        variants["empirical_family_make"] = observation["family_make"]
                        variants["empirical_family_model"] = observation["family_model"]
                        variants["composition_source"] = observation["composition_source"]
                        variants["composition_components"] = observation["composition_components"]
                        variants["family_source_count"] = observation["source_count"]
                        variants["family_variant_count"] = variants.rating_variant_id.nunique()
                        variants["family_median_weight"] = median_support_weights(variants, "consumption_mj_per_vkm")
                        variants["native_median_weight"] = median_support_weights(variants, "native_consumption")
                        variants["range_median_weight"] = median_support_weights(variants, "range_km")
                        variants["representative_statistic"] = "family_median"
                        variants["family_native_median"] = observation["observed_family_native_median"]
                        variants["family_mj_per_vkm"] = observation["observed_family_mj_per_vkm"]
                        variants["family_range_median_km"] = observation["observed_family_range_median_km"]
                        variants["representative_bucket_has_native_trim"] = observation["representative_bucket_has_native_trim"]
                        variants["allocated_count"] = observation["source_count"] * variants.family_median_weight
                        variants["variant_weight"] = contribution * variants.family_median_weight
                        regional.loc[regional.empirical_observation_id.eq(observation["empirical_observation_id"]), "observed_family_native_mean"] = observation["observed_family_native_median"]
                        variant_frames.append(variants)
                    weighted = pd.concat(variant_frames, ignore_index=True)
                    if abs(weighted.variant_weight.sum() - 1.0) > rules["tolerances"]["weight_sum"]:
                        raise ValueError("Empirical variant contributions do not sum to one")
                    for field, output in (("observed_family_native_median", "native_class_mean"),
                                          ("observed_family_mj_per_vkm", "class_mean_mj_per_vkm"),
                                          ("observed_family_range_median_km", "range_class_mean_km")):
                        values = selected[field]
                        if values.notna().any() and values.isna().any():
                            raise ValueError(f"Partly missing empirical family median: {field}")
                        item[output] = float((values * selected.source_count / selected_count).sum(min_count=1))
                    item.update(
                        aggregation_method=("izev_historical_composition_proxy" if composition_basis == "historical_pooled_model_year_window"
                                            else "izev_incentive_weighted" if kind == "izev" else "mto_identity_supported_weighted"),
                        empirical_families=len(selected[["matched_family_make", "matched_family_model"]].drop_duplicates()),
                        empirical_variant_count=int(weighted.rating_variant_id.nunique()),
                        rating_statistic="empirical_count_weighted_mean",
                        range_assignment=_labels(selected.range_assignment),
                        matched_stock=selected_count, cohort_matched_stock=selected_count,
                        composition_components=" | ".join(sorted({
                            part.strip() for value in selected.composition_components
                            for part in value.split("|") if part.strip()
                        })),
                        coverage_status="conditional_identity_supported_coverage" if kind == "mto" else "conditional_empirical_coverage",
                    )
                    family_counts = selected.groupby(["matched_family_make", "matched_family_model"]).source_count.sum()
                    shares = family_counts / family_counts.sum()
                    item["largest_family_share"] = float(shares.max())
                    item["effective_family_count"] = float(1. / shares.pow(2).sum())
                    allocations.append(weighted)
                    region_allocations.append(weighted)
                elif source_count and item["coverage_status"] != "insufficient_identity_supported_mass":
                    item["coverage_status"] = "observed_but_unresolved_or_outside_window"
            region_records.append(item)
        if region_records:
            region_frame = pd.DataFrame(region_records)
            region_frame["used_in_aggregation"] = True
            for _, group in region_frame.groupby(["ceud_class", "year", "powertrain"], sort=True):
                total = float(group.original_weight.sum())
                if total <= 0:
                    raise ValueError("Observed NRCan size classes have no positive configured weight")
                region_frame.loc[group.index, "observed_weight_sum"] = total
                region_frame.loc[group.index, "weight"] = group.original_weight / total
            class_records.extend(region_frame.to_dict("records"))
            coverage_records.extend(region_frame.to_dict("records"))
        if not regional.empty:
            weighted_rows = pd.concat(region_allocations, ignore_index=True) if region_allocations else pd.DataFrame()
            weighted_by_observation = (
                {identity: group for identity, group in weighted_rows.groupby("empirical_observation_id", sort=False)}
                if not weighted_rows.empty else {}
            )
            for observation in regional.to_dict("records"):
                candidates = candidate_matches.get(observation["empirical_observation_id"])
                if candidates is None or candidates.empty:
                    continue
                audit = candidates.copy()
                for field in ("region", "province", "empirical_observation_id", "family_make", "family_model",
                              "source_kind", "source_count", "transaction_eligible", "transaction_types", "match_status",
                              "selection_status", "range_assignment", "pool_geography", "pool_provinces",
                              "provincial_observation_count", "pool_observation_count", "minimum_observations",
                              "candidate_variant_count", "matched_variant_count", "composition_source", "composition_components",
                              "match_reason", "configuration_status", "class_resolution_status",
                              "class_predominant_share", "consumption_relative_p90_p10_span",
                              "identity_supported", "identity_evidence_methods", "identity_support_reason",
                              "identity_exclusion_reason",
                              "resolved_gasoline_count", "identity_supported_count", "identity_supported_resolved_share",
                              "minimum_resolved_count_share", "identity_coverage_met", "composition_basis",
                              "source_model_year_min", "source_model_year_max", "source_model_years"):
                    audit[field] = observation[field]
                audit["empirical_raw_make"], audit["empirical_raw_model"] = observation["raw_make"], observation["raw_model"]
                audit["assigned_powertrain"] = observation["resolved_powertrain"]
                actual = weighted_by_observation.get(observation["empirical_observation_id"])
                if actual is None or actual.empty:
                    audit["variant_weight"], audit["allocated_count"] = 0., 0.
                else:
                    audit = audit.merge(actual[["component", "source_row", "variant_weight", "allocated_count"]],
                                        on=["component", "source_row"], how="left", validate="one_to_one")
                    audit[["variant_weight", "allocated_count"]] = audit[["variant_weight", "allocated_count"]].fillna(0.)
                audit["selected"] = audit.variant_weight.gt(0)
                match_audits.append(audit)
            regional_observations.append(regional)
    private = ["_make_key", "_model_key", "_powertrain_family", "_izev_family_model", "_trim_model_key", "_candidate_family", "_candidate_key"]
    return RatingComposition(
        class_audit=pd.DataFrame(class_records),
        observations=pd.concat(regional_observations, ignore_index=True).drop(columns=private, errors="ignore") if regional_observations else pd.DataFrame(),
        variants=(pd.concat(allocations, ignore_index=True) if allocations else
                  ratings.iloc[:0].assign(region="", empirical_observation_id="", empirical_family_make="",
                                          empirical_family_model="", composition_source="", composition_components="",
                                          composition_basis="", source_model_year_min=np.nan, source_model_year_max=np.nan,
                                          source_model_years="",
                                          family_source_count=0., family_variant_count=0, allocated_count=0., variant_weight=0.)
                  ).drop(columns=private, errors="ignore"),
        coverage=pd.DataFrame(coverage_records),
        matches=(pd.concat(match_audits, ignore_index=True) if match_audits else
                 ratings.iloc[:0].assign(region="", empirical_observation_id="", match_status="",
                                         composition_basis="", source_model_year_min=np.nan, source_model_year_max=np.nan,
                                         source_model_years="",
                                         selected=False, assigned_powertrain="", variant_weight=0., allocated_count=0.)
                 ).drop(columns=private, errors="ignore"),
    )
