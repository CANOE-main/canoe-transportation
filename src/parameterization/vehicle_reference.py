"""Read-only reviewed vehicle identities and source linkages.

The two configured tables contain accepted identity/applicability decisions.
Consumption, eligible trims, representative ranges and population weights remain
downstream calculations from registered evidence; builds never rewrite the maps.
"""

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Annotated, Literal
import unicodedata

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter, model_validator

from utils.vehicle_labels import normalize_vehicle_text

MTO_SOURCE = "ontario_ministry_transport_vehicle_population"

def _normal_text(value: object) -> str:
    """Normalize formatting while retaining model numbers and configuration words."""
    return " ".join("".join(char for char in unicodedata.normalize("NFKD", str(value))
                            if not unicodedata.combining(char)).split())

def catalogue_family(make: object, model: object, spec: dict) -> tuple[str, str]:
    """Return the configured make-model family, preserving meaningful body identities.

    Ordered, reviewed manufacturer patterns resolve named families before generic
    recognized suffixes are removed. This is classification, not a fuzzy alias.
    """
    make_label, model_label = canonical_family(make, model, spec)
    matching = spec.get("family_matching", {})
    previous = None
    while previous != model_label:
        # A recognized suffix can hide an otherwise reviewed family identity.
        # Recheck after stripping it; retain the full native Model for later
        # explicit configuration constraints and distinct-trim identification.
        for rule in matching.get("family_patterns", []):
            if (normalize_vehicle_text(rule["make"]) == normalize_vehicle_text(make_label)
                    and re.fullmatch(rule["pattern"], model_label)):
                return make_label, str(rule["canonical_model"])
        previous = model_label
        for pattern in matching.get("trim_suffix_patterns", []):
            model_label = re.sub(pattern, "", model_label).strip()
    return make_label, model_label or canonical_family(make, model, spec)[1]

def catalogue_family_labels(frame: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """Copy source rows with shared family identities for nested-median estimates."""
    result = frame.copy()
    family_columns = {"family_make", "family_model", "family_key"}
    if family_columns.issubset(result.columns):
        if result[list(family_columns)].isna().any().any():
            raise ValueError("Pre-resolved catalogue family identities must be complete")
        return result
    pairs = list(zip(result.Make.astype(str), result.Model.astype(str), strict=True))
    labels = {pair: catalogue_family(*pair, spec) for pair in set(pairs)}
    resolved = [labels[pair] for pair in pairs]
    result["family_make"] = [item[0] for item in resolved]
    result["family_model"] = [item[1] for item in resolved]
    result["family_key"] = [normalize_vehicle_text(make) + "/" + normalize_vehicle_text(model)
                            for make, model in resolved]
    return result

def canonical_family(make: object, model: object, spec: dict) -> tuple[str, str]:
    """Apply declared identity aliases while preserving readable source labels."""
    make_label, model_label = (_normal_text(value) for value in (make, model))
    make_key, model_key = map(normalize_vehicle_text, (make_label, model_label))
    matches = []
    for alias in spec.get("aliases", []):
        if normalize_vehicle_text(alias["make"]) != make_key:
            continue
        for value in [alias["canonical_model"], *alias["models"]]:
            native = str(value)
            exact = model_key == normalize_vehicle_text(native)
            prefix = model_label.casefold().startswith(native.casefold())
            suffix = model_label[len(native):] if prefix else ""
            if exact or (not alias.get("exact_only", False) and prefix and suffix and not suffix[0].isalnum()):
                matches.append((len(normalize_vehicle_text(native)), str(alias["make"]),
                                str(alias["canonical_model"]) + (suffix if not exact else "")))
    best = max((item[0] for item in matches), default=0)
    identities = {(make, model) for length, make, model in matches if length == best}
    if len(identities) > 1:
        raise ValueError(f"Conflicting empirical family aliases: {make_label} {model_label}")
    return next(iter(identities)) if identities else (make_label, model_label)

def normalize_family(make: object, model: object, spec: dict) -> tuple[str, str]:
    """Return the same deterministic identity for configured naming duplicates."""
    return tuple(normalize_vehicle_text(value) for value in canonical_family(make, model, spec))

def canonical_izev_observation(make: object, model: object, powertrain: str, spec: dict) -> tuple[str, str]:
    """Resolve an incentive description using its reported propulsion context.

    These aliases translate source observations only. They neither relabel native
    catalogue rows nor establish propulsion for MTO registrations.
    """
    shared = {"aliases": [*spec.get("aliases", []), *spec.get("izev_aliases", [])]}
    identity = canonical_family(make, model, shared)
    scoped = spec.get("izev_observation_aliases_by_powertrain", {}).get(powertrain, [])
    return canonical_family(*identity, {"aliases": scoped}) if scoped else identity

def _canonical_labels(make: pd.Series, model: pd.Series, spec: dict) -> list[tuple[str, str]]:
    # The iZEV extract has hundreds of thousands of transactions but only hundreds
    # of labels. Resolve each spelling once instead of repeating alias scans.
    pairs = list(zip(make.astype(str), model.astype(str), strict=True))
    resolved = {pair: canonical_family(*pair, spec) for pair in set(pairs)}
    return [resolved[pair] for pair in pairs]


@dataclass(frozen=True)
class VehicleReference:
    """Validated, count-free configuration and its provenance paths."""

    canonical: pd.DataFrame
    links: pd.DataFrame
    paths: tuple[Path, ...] = ()


_Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
_VehicleId = Annotated[str, Field(pattern=r"^[A-Z0-9]+/[A-Z0-9]+$")]
_Year = Annotated[int, Field(gt=0)]
_Powertrain = Literal["gasoline", "diesel", "hev", "phev", "bev", "unknown"]
_Scope = Literal["ldv", "mhdv", "non_ldv_unclassified", "ldv_class_unresolved", "scope_unresolved", "identity_unresolved"]


class _ReferenceRow(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _CanonicalRow(_ReferenceRow):
    vehicle_id: _VehicleId
    canonical_make: _Text
    canonical_model: _Text
    family_boundary: bool
    model_year_from: _Year
    model_year_to: _Year
    vehicle_scope: _Scope
    nrcan_vehicle_class: str
    powertrain: _Powertrain
    evidence_status: Literal["catalogue_supported", "propulsion_unresolved", "reviewed_identity", "source_reported", "model_not_produced"]
    evidence_source: _Text
    notes: str

    @model_validator(mode="after")
    def supported_scope(self):
        if self.model_year_from > self.model_year_to:
            raise ValueError("Canonical vehicle model-year interval is reversed")
        if self.evidence_status == "propulsion_unresolved" and self.powertrain != "unknown":
            raise ValueError("Unresolved propulsion cannot establish a powertrain")
        if self.evidence_status == "model_not_produced" and self.powertrain != "unknown":
            raise ValueError("Model nonexistence is an identity decision, not a powertrain")
        return self


class _LinkRow(_ReferenceRow):
    source: _Text
    source_make: str
    source_model: str
    model_year_from: _Year
    model_year_to: _Year
    observed_powertrain: Literal["", "gasoline", "diesel", "hev", "phev", "bev", "unknown"]
    source_vehicle_class: str
    vehicle_id: _VehicleId | None
    vehicle_scope: _Scope | Literal[""]
    link_status: _Text
    evidence_source: _Text
    source_notes: str
    wards_notes: str
    canonical_description: str

    @model_validator(mode="after")
    def supported_interval(self):
        if self.model_year_from > self.model_year_to:
            raise ValueError("Source-link model-year interval is reversed")
        if self.vehicle_id is None and self.link_status != "unresolved_identity":
            raise ValueError("Unlinked source descriptions must explicitly retain unresolved identity")
        if self.source == MTO_SOURCE and self.observed_powertrain:
            raise ValueError("MTO source links cannot establish observed propulsion")
        return self


CANONICAL_COLUMNS = list(_CanonicalRow.model_fields)
SOURCE_LINK_COLUMNS = list(_LinkRow.model_fields)


def vehicle_id(make: object, model: object, composition_rules: dict) -> str:
    """Pure identity helper for review tools and fixture policies."""
    make, model = catalogue_family(make, model, composition_rules)
    return normalize_vehicle_text(make) + "/" + normalize_vehicle_text(model)


def _validated_frame(frame: pd.DataFrame, model: type[BaseModel]) -> pd.DataFrame:
    expected = list(model.model_fields)
    if list(frame.columns) != expected:
        raise ValueError(f"Vehicle reference columns differ from the authoritative schema: {list(frame.columns)} != {expected}")
    records = frame.astype(object).where(frame.notna(), None).to_dict("records")
    for record in records:
        for field, value in record.items():
            if value is None and field != "vehicle_id":
                record[field] = ""
        if record.get("vehicle_id") == "":
            record["vehicle_id"] = None
    rows = TypeAdapter(list[model]).validate_python(records)
    result = pd.DataFrame([row.model_dump() for row in rows], columns=expected)
    for field in ("model_year_from", "model_year_to"):
        result[field] = result[field].astype("int64")
    return result


def _overlapping(frame: pd.DataFrame, keys: list[str]) -> bool:
    ordered = frame.sort_values([*keys, "model_year_from", "model_year_to"])
    preceding_end = ordered.groupby(keys, dropna=False).model_year_to.cummax().shift()
    same_scope = ordered[keys].eq(ordered[keys].shift()).all(axis=1)
    return bool((same_scope & ordered.model_year_from.le(preceding_end)).any())


def _covers_interval(intervals, first: int, last: int) -> bool:
    cursor = first
    for start, end in intervals:
        if end < cursor:
            continue
        if start > cursor:
            return False
        cursor = max(cursor, end + 1)
        if cursor > last:
            return True
    return False


def load_vehicle_reference_frames(
    canonical: pd.DataFrame, links: pd.DataFrame, *, paths=(),
) -> VehicleReference:
    """Validate the same authoritative contract for disk and captured configuration."""
    reference = VehicleReference(
        _validated_frame(canonical, _CanonicalRow),
        _validated_frame(links, _LinkRow),
        tuple(Path(path) for path in paths),
    )
    validate_vehicle_reference(reference)
    return reference


def load_vehicle_reference(bundle) -> VehicleReference:
    """Read the two versioned parameter tables without generating or rewriting them."""
    from utils import load_harmonization_rules, resolve_parameter_path

    rules = load_harmonization_rules(bundle, "vehicle_reference")
    paths = tuple(resolve_parameter_path(bundle, rules[field])
                  for field in ("canonical_file", "source_links_file"))
    return load_vehicle_reference_frames(
        *(pd.read_csv(path, dtype=str, keep_default_na=False) for path in paths), paths=paths,
    )


def validate_vehicle_reference(reference: VehicleReference) -> None:
    """Reject conflicting scope/link decisions before population counts are joined."""
    canonical = _validated_frame(reference.canonical, _CanonicalRow)
    links = _validated_frame(reference.links, _LinkRow)
    if canonical.empty:
        raise ValueError("Canonical vehicle reference must not be empty")
    labels = canonical[["vehicle_id", "canonical_make", "canonical_model"]].drop_duplicates()
    if labels.vehicle_id.duplicated().any():
        raise ValueError("Canonical identity has conflicting make/model labels")
    if canonical.groupby("vehicle_id").family_boundary.nunique().gt(1).any():
        raise ValueError("Canonical identity has conflicting reviewed family boundaries")
    if any(row.vehicle_id != normalize_vehicle_text(row.canonical_make) + "/" + normalize_vehicle_text(row.canonical_model)
           for row in labels.itertuples(index=False)):
        raise ValueError("Canonical vehicle_id must agree with its make/model labels")
    scope_key = ["vehicle_id", "vehicle_scope", "nrcan_vehicle_class", "powertrain", "evidence_status"]
    if _overlapping(canonical, scope_key):
        raise ValueError("Canonical vehicle reference has overlapping identical scopes")
    ids = set(canonical.vehicle_id)
    if not set(links.vehicle_id.dropna()) <= ids:
        raise ValueError("Source link contains an unknown canonical identity foreign key")
    negative = canonical.loc[canonical.evidence_status.eq("model_not_produced")]
    positive = canonical.loc[~canonical.evidence_status.eq("model_not_produced")]
    for row in negative.itertuples(index=False):
        overlap = positive.vehicle_id.eq(row.vehicle_id) & positive.model_year_from.le(row.model_year_to) & positive.model_year_to.ge(row.model_year_from)
        if overlap.any():
            raise ValueError(f"Model nonexistence conflicts with supported applicability: {row.vehicle_id}")
    source_key = ["source", "source_make", "source_model", "observed_powertrain", "source_vehicle_class"]
    if _overlapping(links, source_key):
        raise ValueError("Source links have overlapping applicability for a source description")
    mto = links.loc[links.source.eq(MTO_SOURCE)]
    if _overlapping(mto, ["source_make", "source_model"]):
        raise ValueError("Overlapping reviewed MTO source-link intervals")
    intervals = {}
    for (identifier, vehicle_class), group in positive.groupby(["vehicle_id", "nrcan_vehicle_class"], sort=False):
        intervals[identifier, vehicle_class] = sorted(set(zip(group.model_year_from, group.model_year_to, strict=True)))
    for identifier, group in positive.groupby("vehicle_id", sort=False):
        intervals[identifier, None] = sorted(set(zip(group.model_year_from, group.model_year_to, strict=True)))
    for row in links.loc[links.vehicle_id.notna()].itertuples(index=False):
        scopes = intervals.get((row.vehicle_id, row.source_vehicle_class or None), [])
        if not _covers_interval(scopes, row.model_year_from, row.model_year_to):
            raise ValueError(f"Source link lacks canonical year/class applicability: {row.vehicle_id} {row.model_year_from}-{row.model_year_to}")
    # A blank source attribute is a wildcard. It must not conceal a competing
    # conditional identity or description when the source supplies that attribute.
    keyed = links.reset_index().rename(columns={"index": "_link_row"})
    pairs = keyed.merge(keyed, on=["source", "source_make", "source_model"], suffixes=("_left", "_right"))
    overlap = (pairs._link_row_left.lt(pairs._link_row_right)
        & pairs.model_year_from_left.le(pairs.model_year_to_right)
        & pairs.model_year_to_left.ge(pairs.model_year_from_right))
    for field in ("observed_powertrain", "source_vehicle_class"):
        left, right = pairs[field + "_left"], pairs[field + "_right"]
        overlap &= left.eq("") | right.eq("") | left.eq(right)
    conflict = pd.Series(False, index=pairs.index)
    for field in ("vehicle_id", "vehicle_scope", "link_status", "canonical_description"):
        conflict |= pairs[field + "_left"].fillna("").ne(pairs[field + "_right"].fillna(""))
    if (overlap & conflict).any():
        raise ValueError("Source links have conflicting wildcard applicability")


def reference_mto_mapping(reference: VehicleReference, road_rules: dict, rating_class_rules: dict) -> pd.DataFrame:
    """Translate accepted MTO links into the established internal mapping contract."""
    from parameterization.road_aggregation import (
        ceud_class_for_nlr, nrcan_to_nlr_map, validate_vehicle_mapping,
    )

    names = reference.canonical.drop_duplicates("vehicle_id").set_index("vehicle_id")
    nlr = nrcan_to_nlr_map(rating_class_rules)
    records = []
    for row in reference.links.loc[reference.links.source.eq(MTO_SOURCE)].itertuples(index=False):
        identified = row.vehicle_id is not None and not pd.isna(row.vehicle_id)
        vehicle_class = row.source_vehicle_class if row.vehicle_scope == "ldv" else ""
        nlr_class = nlr.get(vehicle_class, "") if vehicle_class else ""
        notes = "; ".join(value for value in (row.source_notes, row.wards_notes) if value)
        records.append({
            "entry_type": "mto_crosswalk", "mto_make_code": row.source_make, "mto_model_code": row.source_model,
            "model_year_from": row.model_year_from, "model_year_to": row.model_year_to,
            "canonical_make": names.loc[row.vehicle_id, "canonical_make"] if identified else "",
            "canonical_model": names.loc[row.vehicle_id, "canonical_model"] if identified else "",
            "vehicle_scope": row.vehicle_scope, "nrcan_vehicle_class": vehicle_class,
            "nlr_atb_class": nlr_class,
            "nrcan_ceud_class": ceud_class_for_nlr(nlr_class, rating_class_rules) if nlr_class else "",
            "mapping_status": "reviewed", "match_method": "reviewed_identity_and_applicability",
            "supporting_rating_rows": "", "evidence_source": row.evidence_source, "review_notes": notes,
        })
    frame = pd.DataFrame(records, columns=road_rules["mapping_columns"])
    return validate_vehicle_mapping(frame, rules=road_rules, rating_class_rules=rating_class_rules)


def resolve_source_labels(
    rows: pd.DataFrame, reference: VehicleReference, *, source: str,
    make_column: str, model_column: str, year_column: str,
    powertrain_column: str | None = None, class_column: str | None = None,
    require_registered: bool = False,
) -> pd.DataFrame:
    """Attach one reviewed identity to each row, preserving counts and native labels.

    Exact source spellings are compared case-insensitively after whitespace
    normalization. Applicability uses the source observation's model year, never
    the historical proxy target year. Class/powertrain constraints disambiguate
    links only when those attributes are actually observed by that source.
    """
    result = rows.copy()
    required = {make_column, model_column, year_column}
    required.update(field for field in (powertrain_column, class_column) if field)
    if missing := required - set(result):
        raise ValueError(f"Source identity resolution missing fields: {sorted(missing)}")
    source_links = reference.links.loc[reference.links.source.eq(source)]
    if source_links.empty:
        if require_registered and not result.empty:
            raise ValueError(f"No applicable reviewed {source} source links are registered")
        result["vehicle_id"] = None
        result["family_make"] = result[make_column].astype(str)
        result["family_model"] = result[model_column].astype(str)
        result["canonical_description"] = result["family_model"]
        result["reference_link_status"] = "no_applicable_source_link"
        result["identity_exclusion_reason"] = "No reviewed source links are registered for this source."
        result["family_key"] = result.family_make.map(normalize_vehicle_text) + "/" + result.family_model.map(normalize_vehicle_text)
        return result
    names = {row.vehicle_id: (row.canonical_make, row.canonical_model)
             for row in reference.canonical.drop_duplicates("vehicle_id").itertuples(index=False)}
    facts = {}
    for row in reference.canonical.itertuples(index=False):
        facts.setdefault(row.vehicle_id, []).append((row.model_year_from, row.model_year_to, row.evidence_status))
    candidates = {}
    normalization = {}

    def normalized(value):
        value = str(value)
        if value not in normalization:
            normalization[value] = _normal_text(value).casefold()
        return normalization[value]

    for row in source_links.itertuples(index=False):
        candidates.setdefault((normalized(row.source_make), normalized(row.source_model)), []).append(row)
    cache = {}
    resolved = []
    fields = [make_column, model_column, year_column, *([powertrain_column] if powertrain_column else []),
              *([class_column] if class_column else [])]
    for values in result[fields].itertuples(index=False, name=None):
        make, model, year = values[:3]
        year = int(year)
        powertrain = str(values[3]).split("_", 1)[0] if powertrain_column else ""
        vehicle_class = str(values[-1]) if class_column else ""
        key = (normalized(make), normalized(model), year, powertrain, vehicle_class)
        if key not in cache:
            selected = [row for row in candidates.get(key[:2], [])
                if row.model_year_from <= year <= row.model_year_to
                and (not powertrain_column or not row.observed_powertrain or row.observed_powertrain == powertrain)
                and (not class_column or not row.source_vehicle_class or row.source_vehicle_class == vehicle_class)]
            if not selected:
                if require_registered:
                    raise ValueError(f"No applicable reviewed {source} source link: {make} {model} MY{year}")
                cache[key] = (None, str(make), str(model), str(model),
                              "no_applicable_source_link", "No applicable reviewed source-link interval for this observation.")
            else:
                decisions = {(row.vehicle_id, row.vehicle_scope, row.link_status, row.canonical_description)
                             for row in selected}
                if len(decisions) != 1:
                    raise ValueError(f"Conflicting reviewed source links: {source} {make} {model} MY{year}")
                identifier, _, status, description = next(iter(decisions))
                if identifier is None or pd.isna(identifier):
                    notes = "; ".join(dict.fromkeys(row.source_notes for row in selected))
                    cache[key] = (None, str(make), str(model), description or str(model), "unresolved_identity",
                                  notes or "Source identity remains unresolved in the reviewed reference.")
                else:
                    applicable = [status for first, last, status in facts[identifier] if first <= year <= last]
                    if not applicable or "model_not_produced" in applicable:
                        raise ValueError(f"Source link conflicts with canonical model-year applicability: {identifier} MY{year}")
                    cache[key] = (identifier, *names[identifier], description or str(model), status, "")
        resolved.append(cache[key])
    fields = ["vehicle_id", "family_make", "family_model", "canonical_description", "reference_link_status", "identity_exclusion_reason"]
    for index, field in enumerate(fields):
        result[field] = [values[index] for values in resolved]
    result["family_key"] = [identifier if identifier else normalize_vehicle_text(make) + "/" + normalize_vehicle_text(model)
                            for identifier, make, model, *_ in resolved]
    return result


def attach_catalogue_identities(classified: pd.DataFrame, reference: VehicleReference) -> pd.DataFrame:
    """Attach reviewed NRCan identities before any family/cohort calculation."""
    result = resolve_source_labels(classified, reference, source="nrcan_fuel_consumption_ratings",
        make_column="Make", model_column="Model", year_column="year",
        powertrain_column="powertrain", class_column="rating_class", require_registered=True)
    supported = set()
    for row in reference.canonical.loc[~reference.canonical.evidence_status.eq("model_not_produced")].itertuples(index=False):
        for year in range(row.model_year_from, row.model_year_to + 1):
            supported.add((row.vehicle_id, year, row.nrcan_vehicle_class, row.powertrain))
    # The existing classifier establishes configuration propulsion from native
    # and complementary evidence. Reviewed family availability validates that
    # result; it cannot classify a trim or infer population fuel composition.
    base = result.powertrain.astype(str).str.split("_").str[0]
    unresolved = result.get("propulsion_resolved", pd.Series(True, index=result.index)).eq(False).fillna(False)
    effective = base.mask(unresolved, "unknown")
    tracked = base.isin({"gasoline", "diesel", "hev", "phev", "bev", "unknown"})
    for row, powertrain in zip(result.loc[tracked].itertuples(index=False), effective.loc[tracked], strict=True):
        if (row.vehicle_id, row.year, row.rating_class, powertrain) not in supported:
            raise ValueError(f"Classified propulsion conflicts with reviewed canonical availability: {row.vehicle_id} MY{row.year} {row.rating_class} {powertrain}")
    return result
