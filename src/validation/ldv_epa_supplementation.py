"""Opt-in, read-only assessment of EPA consumption as an NRCan supplement.

The captured empirical composition remains the reference. Catalogue screening is
not physical vehicle verification, and this diagnostic never promotes a rating.
Run ``uv run python -m validation.ldv_epa_supplementation --write-report`` to
recreate its audit tables; no scenario, source fetch or database build is run.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import re
from zipfile import ZipFile

import numpy as np
import pandas as pd

from parameterization.rating_composition import (
    _compatible, _rating_variants, aggregate_empirical_ratings, normalize_family,
)
from parameterization.road_aggregation import normalize_vehicle_text
from utils import (
    file_sha256, load_conversion_factors, load_harmonization_rules, load_yaml,
    resolve_input_path, write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import PathsConfig
from validation.notebook_evidence import load_notebook_evidence


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SupplementationAssessment:
    frames: dict[str, pd.DataFrame]
    summary: dict


def range_bucket(miles: float, family: str, ranges: dict) -> str:
    """Use the captured production boundaries, including PHEV's open 35-mile edge."""
    if not np.isfinite(miles) or miles <= 0 or family not in {"bev", "phev"}:
        return ""
    for bucket in ranges["buckets"][family.upper()]:
        upper = bucket["upper"]
        if upper is None or (miles <= upper if bucket["upper_inclusive"] else miles < upper):
            return bucket["sub_category"]
    raise ValueError("Configured range buckets have no open final bucket")


def _transmission(value: object) -> str:
    """Translate source notation only; missing gearbox/drive detail is not invented."""
    label = str(value)
    if match := re.search(r"\(([AMS]\d+)\)", label):
        return match[1]
    if match := re.search(r"(?i)(Manual|Automatic)\s+(\d+)-spd", label):
        return ("M" if match[1].casefold() == "manual" else "A") + match[2]
    if "variable gear ratios" in label.casefold():
        return "AV"
    return label if re.fullmatch(r"[AMS]\d+|AV\d*", label) else ""


def prepare_epa(raw: pd.DataFrame, spec: dict, classes: dict, ranges: dict, conversions: dict) -> pd.DataFrame:
    """Convert independent native rates to MJ/vkm without substituting MPGe for MPG."""
    required = {"id", "year", "make", "model", "VClass", "atvType", "fuelType1", "fuelType2",
                "comb08", "comb08U", "combA08U", "combE", "combinedCD", "phevBlended",
                "range", "rangeA", "cylinders", "displ", "trany", "baseModel", "drive"}
    if missing := required - set(raw):
        raise ValueError(f"EPA diagnostic fields missing: {sorted(missing)}")
    result = raw.copy()
    if result.id.duplicated().any():
        raise ValueError("EPA vehicle ids are not unique")
    if not result.phevBlended.dropna().isin([True, False]).all():
        raise ValueError("EPA blended-mode flags must be booleans when present")
    result["family"] = np.select(
        [result.atvType.eq("EV"), result.atvType.eq("Plug-in Hybrid"), result.atvType.eq("Hybrid"),
         result.fuelType1.eq("Diesel"), result.fuelType1.isin(["Regular Gasoline", "Premium Gasoline", "Midgrade Gasoline"])],
        ["bev", "phev", "hev", "diesel", "gasoline"], default="unsupported",
    )
    keys = [normalize_family(make, model, spec) for make, model in zip(result.make, result.model, strict=True)]
    result["make_key"], result["model_key"] = zip(*keys, strict=True)
    result["rating_class"] = result.VClass.map(classes)
    result["transmission_key"] = result.trany.map(_transmission)
    result["family_holdout"] = result.make_key + "/" + result.baseModel.fillna(result.model).map(normalize_vehicle_text)
    distance = conversions["length"]["mile_to_km"]
    gallon = conversions["volume"]["us_gallon_to_litre"]
    wh = conversions["energy"]["wh_to_mj"]
    kwh = conversions["energy"]["kwh_to_mj"]
    fuel_hhv = np.where(result.family.eq("diesel"), conversions["fuel_hhv"]["diesel_mj_per_litre"],
                        conversions["fuel_hhv"]["gasoline_mj_per_litre"])
    mpg = pd.to_numeric(result.comb08U, errors="raise")
    label_mpg = pd.to_numeric(result.comb08, errors="raise")
    result["mpg_basis"] = np.where(mpg.gt(0), "unrounded_adjusted", "rounded_adjusted_fallback")
    mpg = mpg.where(mpg.gt(0), label_mpg.where(label_mpg.gt(0)))
    result["cs_fuel_mj_per_vkm"] = gallon * fuel_hhv / (mpg * distance)
    result.loc[~result.family.isin(["gasoline", "hev", "diesel", "phev"]), "cs_fuel_mj_per_vkm"] = np.nan
    result["cd_electricity_mj_per_vkm"] = pd.to_numeric(result.combE, errors="raise") * kwh / (100 * distance)
    result["cd_fuel_mj_per_vkm"] = pd.to_numeric(result.combinedCD, errors="raise") * gallon * fuel_hhv / (100 * distance)
    # Native EPA zero CD fuel is retained as a reported value, not inferred from MPGe.
    # A missing blended fuel rate stays missing; nonblended operation has no CD fuel.
    result.loc[result.phevBlended.eq(False) & result.cd_fuel_mj_per_vkm.isna(), "cd_fuel_mj_per_vkm"] = 0.
    result.loc[~result.family.isin(["bev", "phev"]), "cd_electricity_mj_per_vkm"] = np.nan
    result.loc[~result.family.eq("phev"), "cd_fuel_mj_per_vkm"] = np.nan
    result["epa_mj_per_vkm"] = result.cs_fuel_mj_per_vkm.where(result.family.isin(["gasoline", "hev", "diesel"]))
    bev = result.family.eq("bev")
    result.loc[bev, "epa_mj_per_vkm"] = result.loc[bev, "cd_electricity_mj_per_vkm"]
    result["label_equivalent_cd_mj_per_vkm"] = conversions["energy"]["wh_per_gge"] * wh / (
        pd.to_numeric(result.combA08U, errors="raise").where(result.combA08U.gt(0)) * distance)
    result.loc[bev, "label_equivalent_cd_mj_per_vkm"] = conversions["energy"]["wh_per_gge"] * wh / (mpg.loc[bev] * distance)
    # MPGe uses the fixed EPA GGE energy, not CANOE's gasoline HHV.
    equivalent_fuel = pd.to_numeric(result.combinedCD, errors="raise") * conversions["energy"]["wh_per_gge"] * wh / (100 * distance)
    native_equivalent = result.cd_electricity_mj_per_vkm + equivalent_fuel.where(result.family.eq("phev"), 0.)
    result["native_label_fraction"] = native_equivalent / result.label_equivalent_cd_mj_per_vkm - 1
    native_range = pd.to_numeric(result["range"], errors="coerce")
    secondary_range = pd.to_numeric(result.rangeA, errors="coerce")
    result["range_km"] = np.where(bev, native_range, secondary_range) * distance
    result["powertrain"] = [range_bucket(float(miles), family, ranges) if family in {"bev", "phev"} else family
                             for miles, family in zip(result.range_km / distance, result.family, strict=True)]
    result["fuel_grade"] = result.fuelType1.map({"Regular Gasoline": "X", "Premium Gasoline": "Z", "Diesel": "D"}).fillna("")
    return result


def catalogue_pairs(ratings: pd.DataFrame, epa: pd.DataFrame, spec: dict, conversions: dict) -> pd.DataFrame:
    """Retain exact catalogue-name candidates and flag stronger observable agreement.

    Even a reciprocal unique screen leaves market-specific equipment, wheels,
    certification test groups and physical correspondence unverified.
    """
    variants = _rating_variants(ratings, spec).drop_duplicates("rating_variant_id").copy()
    variants["make_key"], variants["model_key"] = variants["_make_key"], variants["_model_key"]
    variants["family"] = variants.powertrain.str.split("_").str[0]
    fields = ["year", "Make", "Model", "rating_class", "family", "powertrain", "make_key", "model_key",
              "rating_variant_id", "component", "source_row", "consumption_mj_per_vkm", "native_consumption",
              "Engine size (L)", "Cylinders", "Transmission", "Fuel type", "Fuel type 1", "Fuel type 2", "range_km", "fleet_utility_factor",
              "cd_electricity_kwh_per_100km", "cd_fuel_l_per_100km", "cs_fuel_l_per_100km"]
    merged = variants[fields].merge(epa, on=["year", "make_key", "model_key", "rating_class", "family"],
                                    how="inner", suffixes=("_nrcan", "_epa"))
    if merged.empty:
        return merged
    fuel = merged.family.isin(["gasoline", "hev", "diesel", "phev"])
    engine = pd.to_numeric(merged["Engine size (L)"], errors="coerce").round(1).eq(merged.displ.round(1))
    cylinders = merged.Cylinders.eq(merged.cylinders)
    transmission = merged.Transmission.map(_transmission).eq(merged.transmission_key) & merged.transmission_key.ne("")
    # NRCan PHEVs label electricity as fuel 1 and CS gasoline as fuel 2.
    nrcan_grade = merged["Fuel type"].where(~merged.family.eq("phev"), merged["Fuel type 2"])
    grade = nrcan_grade.eq(merged.fuel_grade)
    # The sum of half a native kilometre and half a native mile is the maximum
    # discrepancy attributable solely to independently rounded range labels.
    rounding = (1 + conversions["length"]["mile_to_km"]) / 2
    range_rounding = (merged.range_km_nrcan - merged.range_km_epa).abs().le(rounding)
    bucket = merged.powertrain_nrcan.eq(merged.powertrain_epa)
    merged["observed_configuration_agreement"] = np.where(fuel, engine & cylinders & transmission & grade,
                                                         transmission & range_rounding & bucket)
    phev = merged.family.eq("phev")
    merged.loc[phev, "observed_configuration_agreement"] &= bucket.loc[phev] & range_rounding.loc[phev]
    uf = pd.to_numeric(merged.fleet_utility_factor, errors="coerce")
    merged.loc[phev, "epa_mj_per_vkm"] = (
        uf * (merged.cd_electricity_mj_per_vkm + merged.cd_fuel_mj_per_vkm)
        + (1 - uf) * merged.cs_fuel_mj_per_vkm
    ).loc[phev]
    merged["relative_difference"] = merged.epa_mj_per_vkm / merged.consumption_mj_per_vkm - 1
    merged["difference_mj_per_vkm"] = merged.epa_mj_per_vkm - merged.consumption_mj_per_vkm
    merged["cd_electricity_difference_mj_per_vkm"] = (merged.cd_electricity_mj_per_vkm
        - merged.cd_electricity_kwh_per_100km * conversions["energy"]["kwh_to_mj"] / 100)
    merged["cd_fuel_difference_mj_per_vkm"] = merged.cd_fuel_mj_per_vkm - merged.cd_fuel_l_per_100km * conversions["fuel_hhv"]["gasoline_mj_per_litre"] / 100
    merged["cs_fuel_difference_mj_per_vkm"] = merged.cs_fuel_mj_per_vkm - merged.cs_fuel_l_per_100km * conversions["fuel_hhv"]["gasoline_mj_per_litre"] / 100
    agreed = merged.loc[merged.observed_configuration_agreement]
    per_nrcan = agreed.groupby("rating_variant_id").id.nunique()
    per_epa = agreed.groupby("id").rating_variant_id.nunique()
    merged["reciprocal_unique_screen"] = (merged.observed_configuration_agreement
        & merged.rating_variant_id.map(per_nrcan).eq(1) & merged.id.map(per_epa).eq(1)
        & merged.epa_mj_per_vkm.gt(0) & np.isfinite(merged.epa_mj_per_vkm))
    merged["physical_correspondence_verified"] = False
    merged["regime"] = pd.cut(merged.year, [0, 2007, 2010, 2014, 2016, 9999],
        labels=["through_2007_adjusted_2008", "2008_2010", "2011_2014_adjusted_2017", "2015_2016_adjusted_2017", "2017_onward"]).astype(str)
    return merged


def observation_dispositions(observed: pd.DataFrame, ratings: pd.DataFrame, epa: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """Reconcile each empirical description once; EPA trims never multiply counts."""
    result = observed.copy()
    nrcan = ratings.loc[ratings.included].copy()
    identities = [normalize_family(make, model, spec) for make, model in zip(nrcan.Make, nrcan.Model, strict=True)]
    nrcan["make_key"], nrcan["model_key"] = zip(*identities, strict=True)
    nrcan["family"] = nrcan.powertrain.str.split("_").str[0]
    nrcan["first_token"] = nrcan.Model.astype(str).str.upper().str.split().str[0]
    nrcan["model_tokens"] = nrcan.Model.map(lambda value: tuple(re.findall(r"[A-Z0-9]+", str(value).upper())))
    nrcan_makes = {make: group for make, group in nrcan.groupby("make_key")}
    nrcan_neighbours = {key: group for key, group in nrcan.groupby(["make_key", "year", "first_token"])}
    epa_makes = {make: group for make, group in epa.groupby("make_key")}
    family_cache = {}
    records = []
    for item in result.to_dict("records"):
        make_key = normalize_vehicle_text(item["family_make"])
        model = str(item["family_model"])
        own = nrcan_makes.get(make_key, nrcan.iloc[:0])
        other = epa_makes.get(make_key, epa.iloc[:0])
        family = item["observed_powertrain"] if item["source_kind"] == "izev" else "unobserved"
        cache_key = (make_key, model)
        if cache_key not in family_cache:
            own_labels = {label: _compatible(model, str(label)) for label in own.Model.unique()}
            other_labels = {label: _compatible(model, str(label)) for label in other.model.unique()}
            own_family = own.loc[own.Model.map(own_labels).astype(bool)]
            other_family = other.loc[other.model.map(other_labels).astype(bool)]
            family_cache[cache_key] = own_family, other_family
        own_family, other_family = family_cache[cache_key]
        own_year = own_family.loc[own_family.year.eq(item["year"])]
        candidates = other_family.loc[other_family.year.eq(item["year"])]
        if item["source_kind"] == "izev":
            candidates = candidates.loc[candidates.family.eq(family)]
            own_year = own_year.loc[own_year.family.eq(family)]
        else:
            candidates = candidates.loc[candidates.rating_class.eq(item["rating_class"])]
        first_token = re.split(r"\W+", model.upper())[0]
        name_neighbours = nrcan_neighbours.get((make_key, item["year"], first_token), nrcan.iloc[:0])
        status = item["match_status"]
        if status == "unmatched_ratings" and own_year.empty and name_neighbours.empty:
            tokens = tuple(re.findall(r"[A-Z0-9]+", model.upper()))
            own_edition = own.loc[own.year.eq(item["year"])]
            # Whole observed tokens inside a longer own-year label (GTI in Golf
            # GTI) establish a naming-review lead, never an accepted identity.
            if tokens:
                interior = own_edition.model_tokens.map(lambda values: any(
                    values[index:index + len(tokens)] == tokens for index in range(len(values) - len(tokens) + 1)))
                name_neighbours = own_edition.loc[interior.astype(bool)]
        if item["selected"]:
            reason = "already_selected_nrcan"
        elif status != "unmatched_ratings":
            reason = status
        elif not own_year.empty:
            reason = "existing_nrcan_class_or_description_mismatch"
        elif not name_neighbours.empty:
            reason = ("existing_nrcan_powertrain_name_conflict" if item["source_kind"] == "izev"
                      and not name_neighbours.family.eq(family).any() else "existing_nrcan_name_or_trim_ambiguity")
        elif not own_family.empty:
            reason = "missing_own_year_nrcan_family_rating"
        else:
            reason = "no_nrcan_family_identity_unverified"
        classes = sorted(candidates.rating_class.dropna().unique())
        powers = sorted(candidates.powertrain.dropna().unique())
        identity = bool(item["identity_supported"])
        permitted = (status == "unmatched_ratings" and identity and reason == "missing_own_year_nrcan_family_rating"
                     and len(classes) == 1 and len(powers) == 1 and powers[0] != ""
                     and ((family in {"bev", "phev"} and bool(item["transaction_eligible"]))
                          or (item["source_kind"] == "mto" and powers == ["gasoline"])))
        item.update(coverage_disposition=reason, epa_candidate_rows=len(candidates),
                    epa_candidate_models=" | ".join(sorted(candidates.model.unique())),
                    epa_candidate_classes=" | ".join(classes), epa_candidate_powertrains=" | ".join(powers),
                    nrcan_neighbour_models=" | ".join(sorted(name_neighbours.Model.unique())),
                    candidate_recovery_upper_bound=int(item["source_count"]) if permitted else 0,
                    pre_range_candidate_count=(int(item["source_count"]) if status == "unmatched_ratings"
                        and identity and reason == "missing_own_year_nrcan_family_rating" and not candidates.empty else 0),
                    accepted_recovery_count=0, physical_correspondence_verified=False,
                    supplement_consumption_mj_per_vkm=np.nan)
        if permitted and family != "phev" and candidates.epa_mj_per_vkm.notna().all():
            # Diagnostic candidate range only, never an eligible production rating.
            item["candidate_consumption_min_mj_per_vkm"] = float(candidates.epa_mj_per_vkm.min())
            item["candidate_consumption_max_mj_per_vkm"] = float(candidates.epa_mj_per_vkm.max())
        records.append(item)
    return pd.DataFrame(records)


def coverage_tables(observations: pd.DataFrame, classes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep all-fuel MTO, PEV pool and range-specific denominators distinct."""
    rows = observations.copy()
    rows["family"] = np.where(rows.source_kind.eq("mto"), "propulsion_unobserved", rows.observed_powertrain)
    keys = ["region", "source_kind", "composition_basis", "year", "family", "pool_geography", "pool_provinces"]
    rows["selected_count"] = rows.source_count.where(rows.selected, 0)
    rows["resolved_count"] = rows.source_count.where(rows.match_status.eq("resolved"), 0)
    rows["unmatched_count"] = rows.source_count.where(rows.match_status.eq("unmatched_ratings"), 0)
    rows["ambiguous_count"] = rows.source_count.where(rows.match_status.str.startswith("ambiguous"), 0)
    rows["identity_unverified_count"] = rows.source_count.where(~rows.identity_supported, 0)
    pools = rows.groupby(keys, dropna=False, as_index=False).agg(
        denominator_count=("source_count", "sum"), empirical_descriptions=("source_count", "size"),
        existing_selected_count=("selected_count", "sum"), resolved_count=("resolved_count", "sum"),
        unmatched_count=("unmatched_count", "sum"), ambiguous_count=("ambiguous_count", "sum"),
        identity_unverified_count=("identity_unverified_count", "sum"),
        candidate_recovery_upper_bound=("candidate_recovery_upper_bound", "sum"), accepted_recovery_count=("accepted_recovery_count", "sum"))
    prerange = rows.groupby(keys, dropna=False, as_index=False).pre_range_candidate_count.sum()
    pools = pools.merge(prerange, on=keys, validate="one_to_one")
    pools["existing_selected_share"] = pools.existing_selected_count / pools.denominator_count
    pools["supplemented_selected_count"] = pools.existing_selected_count + pools.accepted_recovery_count
    pools["supplemented_selected_share"] = pools.supplemented_selected_count / pools.denominator_count
    pools["denominator_definition"] = np.where(pools.source_kind.eq("mto"),
        "mapped model-year stock, all propulsion unobserved; Ontario stock proxy in every region",
        "purchase/lease source counts in the unchanged selected model-year/family/geographic pool")
    pools["additivity_note"] = np.where(pools.composition_basis.eq("historical_pooled_model_year_window"),
        "same source window reused for target years; do not sum across years", "national pools can overlap across recipient regions; do not sum regions")
    cohorts = classes.copy()
    cohorts["accepted_recovery_count"] = 0
    cohorts["supplemented_selected_count"] = cohorts.selected_source_count
    cohorts["supplemented_class_mj_per_vkm"] = cohorts.class_mean_mj_per_vkm
    cohorts["accepted_class_change_mj_per_vkm"] = 0.
    cohorts["candidate_recovery_upper_bound"] = 0
    for item in observations.loc[observations.candidate_recovery_upper_bound.gt(0)].to_dict("records"):
        selected = (cohorts.region.eq(item["region"]) & cohorts.year.eq(item["year"])
                    & cohorts.rating_class.eq(item["epa_candidate_classes"]) & cohorts.powertrain.eq(item["epa_candidate_powertrains"]))
        cohorts.loc[selected, "candidate_recovery_upper_bound"] += item["candidate_recovery_upper_bound"]
    return pools, cohorts


def holdout_validation(pairs: pd.DataFrame) -> pd.DataFrame:
    """Hold out entire families across years, or an entire model year, by regime.

    The optional diagnostic multiplicative correction is estimated only from the
    remaining families/years. These are catalogue transport tests, not physical
    identity validation or a random vehicle split.
    """
    screened = pairs.loc[pairs.reciprocal_unique_screen].copy()
    records = []
    for method, field in (("family", "family_holdout"), ("model_year", "year")):
        for (family, regime), group in screened.groupby(["family", "regime"]):
            for key, test in group.groupby(field):
                train = group.loc[group[field].ne(key)]
                if train.empty:
                    continue
                ratio = float((train.epa_mj_per_vkm / train.consumption_mj_per_vkm).median())
                for item in test.to_dict("records"):
                    actual, predicted = item["consumption_mj_per_vkm"], item["epa_mj_per_vkm"]
                    records.append(dict(holdout=method, held_out_group=str(key), family=family, regime=regime,
                        family_holdout=item["family_holdout"], year=item["year"], rating_variant_id=item["rating_variant_id"],
                        epa_id=item["id"], train_pairs=len(train), train_families=train.family_holdout.nunique(),
                        train_years=train.year.nunique(), training_epa_over_nrcan_ratio=ratio,
                        nrcan_mj_per_vkm=actual, epa_mj_per_vkm=predicted,
                        uncorrected_fraction=predicted / actual - 1,
                        corrected_fraction=predicted / ratio / actual - 1,
                        physical_correspondence_verified=False))
    return pd.DataFrame(records)


def cohort_sensitivity(classes: pd.DataFrame, observations: pd.DataFrame, variants: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    """Simulate NRCan gaps only for complete, screened observed-family matches.

    Unmatched descriptions retain NRCan. Each observation's original source count
    and within-family variant allocation remain fixed; this is a sensitivity, not
    recovered coverage or an eligibility decision.
    """
    screened = pairs.loc[pairs.reciprocal_unique_screen]
    mapped = screened.set_index("rating_variant_id")["epa_mj_per_vkm"]
    if mapped.index.duplicated().any():
        raise ValueError("Screened EPA substitutions are not unique per NRCan variant")
    selected = observations.loc[observations.selected].copy()
    selected["sensitivity_mj_per_vkm"] = selected.observed_family_mj_per_vkm
    selected["screened_replacement_count"] = 0
    contributions = variants.copy()
    contributions["epa_value"] = contributions.rating_variant_id.map(mapped)
    for (region, identity), group in contributions.groupby(["region", "empirical_observation_id"], sort=False):
        if group.epa_value.isna().any():
            continue
        shares = 1 / group.family_variant_count / group.source_rows_per_variant
        value = float((group.epa_value * shares).sum())
        mask = selected.region.eq(region) & selected.empirical_observation_id.eq(identity)
        selected.loc[mask, "sensitivity_mj_per_vkm"] = value
        selected.loc[mask, "screened_replacement_count"] = selected.loc[mask, "source_count"]
    result = classes.copy()
    result["sensitivity_class_mj_per_vkm"] = result.class_mean_mj_per_vkm
    result["screened_replacement_count"] = 0
    for (region, year, size, powertrain), group in selected.groupby(["region", "year", "rating_class", "resolved_powertrain"]):
        mask = result.region.eq(region) & result.year.eq(year) & result.rating_class.eq(size) & result.powertrain.eq(powertrain)
        weight = group.source_count / group.source_count.sum()
        native = float((group.observed_family_mj_per_vkm * weight).sum())
        if not np.allclose(result.loc[mask, "class_mean_mj_per_vkm"], native, atol=1e-10, rtol=1e-10):
            raise ValueError("Empirical mean does not reconcile with the captured class estimate")
        result.loc[mask, "sensitivity_class_mj_per_vkm"] = float((group.sensitivity_mj_per_vkm * weight).sum())
        result.loc[mask, "screened_replacement_count"] = group.screened_replacement_count.sum()
    result["baseline_contribution"] = result.weight * result.class_mean_mj_per_vkm
    result["sensitivity_contribution"] = result.weight * result.sensitivity_class_mj_per_vkm
    aggregate = result.groupby(["region", "ceud_class", "year", "powertrain"], as_index=False).agg(
        nrcan_mj_per_vkm=("baseline_contribution", lambda values: values.sum(min_count=1)),
        screened_sensitivity_mj_per_vkm=("sensitivity_contribution", lambda values: values.sum(min_count=1)),
        empirical_count=("selected_source_count", "sum"), screened_replacement_count=("screened_replacement_count", "sum"))
    aggregate["screened_sensitivity_fraction"] = aggregate.screened_sensitivity_mj_per_vkm / aggregate.nrcan_mj_per_vkm - 1
    aggregate["accepted_supplement_mj_per_vkm"] = aggregate.nrcan_mj_per_vkm
    aggregate["total_energy_available"] = aggregate.nrcan_mj_per_vkm.notna()
    aggregate["accepted_supplement_fraction"] = np.where(aggregate.total_energy_available, 0., np.nan)
    aggregate["accepted_recovered_count"] = 0
    return aggregate


def _agreement_summary(pairs: pd.DataFrame) -> pd.DataFrame:
    records = []
    for tier, subset in (("exact_catalogue_name_class_fuel", pairs), ("observable_configuration_agreement", pairs.loc[pairs.observed_configuration_agreement]),
                         ("reciprocal_unique_screen", pairs.loc[pairs.reciprocal_unique_screen])):
        for (family, regime), group in subset.groupby(["family", "regime"]):
            records.append(dict(screen=tier, family=family, regime=regime, catalogue_pairs=len(group),
                nrcan_variants=group.rating_variant_id.nunique(), epa_rows=group.id.nunique(),
                mean_fraction=float(group.relative_difference.mean()), median_fraction=float(group.relative_difference.median()),
                median_absolute_fraction=float(group.relative_difference.abs().median()),
                p95_absolute_fraction=float(group.relative_difference.abs().quantile(.95)),
                min_fraction=float(group.relative_difference.min()), max_fraction=float(group.relative_difference.max()),
                physical_verified_pairs=0))
    return pd.DataFrame(records)


def candidate_benefit_bounds(observations: pd.DataFrame, classes: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    """Bound optimistic MTO gap filling without changing empirical count weights.

    This deliberately assumes candidate Canadian identity/class/fuel equivalence
    for sensitivity only. EPA trim rates supply a min/max envelope, never extra
    empirical weight. Existing source-native family means are reconstructed from
    the recorded distinct NRCan variants when a documentary gate would reopen.
    """
    candidates = observations.loc[observations.source_kind.eq("mto") & observations.candidate_recovery_upper_bound.gt(0)]
    records = []
    for (region, year, size, powertrain), group in candidates.groupby(
            ["region", "year", "epa_candidate_classes", "epa_candidate_powertrains"]):
        selected = classes.loc[classes.region.eq(region) & classes.year.eq(year)
            & classes.rating_class.eq(size) & classes.powertrain.eq(powertrain)]
        if len(selected) != 1 or powertrain != "gasoline":
            raise ValueError("Candidate benefit requires one existing gasoline class cohort")
        base = selected.iloc[0]
        resolved, supported = float(base.resolved_gasoline_count), float(base.identity_supported_count)
        count = float(group.candidate_recovery_upper_bound.sum())
        threshold = float(base.minimum_resolved_count_share)
        current_gate = supported > 0 and supported / resolved >= threshold if resolved else False
        new_share = (supported + count) / (resolved + count)
        new_gate = new_share >= threshold
        baseline = float(base.class_mean_mj_per_vkm)
        supported_mean = baseline if current_gate else np.nan
        if new_gate and not current_gate and supported:
            documentary = matches.loc[matches.region.eq(region) & matches.year.eq(year) & matches.rating_class.eq(size)
                & matches.source_kind.eq("mto") & matches.identity_supported & matches.match_status.eq("resolved")
                & matches.assigned_powertrain.eq("gasoline")].drop_duplicates(["empirical_observation_id", "rating_variant_id"])
            families = documentary.groupby("empirical_observation_id").agg(
                family_mean=("consumption_mj_per_vkm", "mean"), source_count=("source_count", "first"))
            if not np.isclose(families.source_count.sum(), supported):
                raise ValueError("Recorded documentary family means do not reconcile with the identity gate")
            supported_mean = float((families.family_mean * families.source_count).sum() / supported)
        low, high = baseline, baseline
        if new_gate:
            complete = group[["candidate_consumption_min_mj_per_vkm", "candidate_consumption_max_mj_per_vkm"]].notna().all().all()
            if not complete or (supported and not np.isfinite(supported_mean)):
                low, high = np.nan, np.nan
            else:
                existing = supported * supported_mean if supported else 0.
                low = (existing + (group.source_count * group.candidate_consumption_min_mj_per_vkm).sum()) / (supported + count)
                high = (existing + (group.source_count * group.candidate_consumption_max_mj_per_vkm).sum()) / (supported + count)
        siblings = classes.loc[classes.region.eq(region) & classes.year.eq(year)
            & classes.ceud_class.eq(base.ceud_class) & classes.powertrain.eq(powertrain)]
        aggregate = float((siblings.weight * siblings.class_mean_mj_per_vkm).sum(min_count=1))
        aggregate_low = aggregate + base.weight * (low - baseline)
        aggregate_high = aggregate + base.weight * (high - baseline)
        records.append(dict(region=region, year=year, rating_class=size, ceud_class=base.ceud_class, powertrain=powertrain,
            candidate_families=" | ".join(sorted(group.family_make + " " + group.family_model)),
            candidate_source_count=count, existing_resolved_gasoline_count=resolved, existing_identity_supported_count=supported,
            existing_identity_supported_share=supported / resolved if resolved else np.nan, minimum_share=threshold,
            optimistic_identity_supported_share=new_share, existing_gate_met=current_gate, optimistic_gate_met=new_gate,
            gate_crossed=(new_gate and not current_gate), existing_selected_count=supported if current_gate else 0.,
            optimistic_selected_count=supported + count if new_gate else 0.,
            existing_documentary_family_mean_mj_per_vkm=supported_mean, baseline_class_mj_per_vkm=baseline,
            optimistic_class_min_mj_per_vkm=low, optimistic_class_max_mj_per_vkm=high,
            baseline_cohort_mj_per_vkm=aggregate, optimistic_cohort_min_mj_per_vkm=aggregate_low,
            optimistic_cohort_max_mj_per_vkm=aggregate_high,
            optimistic_cohort_min_fraction=aggregate_low / aggregate - 1,
            optimistic_cohort_max_fraction=aggregate_high / aggregate - 1,
            accepted_recovered_count=0, physical_correspondence_verified=False,
            interpretation="Conditional candidate envelope only: Canadian identity/class/fuel correspondence remains unverified; unchanged source counts, documentary gate and outer class weights"))
    return pd.DataFrame(records)


def _native_label_summary(epa: pd.DataFrame) -> pd.DataFrame:
    records = []
    for (family, year), group in epa.loc[epa.family.isin(["bev", "phev"])].groupby(["family", "year"]):
        values = group.native_label_fraction.dropna()
        records.append(dict(family=family, year=year, epa_rows=len(group), comparable_native_label_rows=len(values),
            median_fraction=values.median(), p95_absolute_fraction=values.abs().quantile(.95),
            min_fraction=values.min(), max_fraction=values.max(),
            interpretation="Native combined electricity plus reported CD fuel at EPA GGE energy versus unrounded adjusted MPGe; no CANOE UF or gasoline HHV applied to MPGe"))
    return pd.DataFrame(records)


def _configured_regions(repo_root: Path) -> tuple[str, ...]:
    """Use live diagnostic geography selections; captured model policy stays fixed."""
    paths = PathsConfig.model_validate(load_yaml(repo_root / "config/paths.yaml"))
    rules = load_yaml(repo_root / paths.config.parameters / "rules.yaml")
    regions = rules["parameterization"]["ldv_efficiency_diagnostic"]["epa_assessment_regions"]
    if (not isinstance(regions, list) or not regions
            or any(not isinstance(value, str) or not value.strip() for value in regions)
            or len(regions) != len(set(regions))):
        raise ValueError("EPA assessment requires distinct nonblank configured diagnostic regions")
    return tuple(regions)


def assess_supplementation(*, repo_root: Path | None = None, regions: tuple[str, ...] | None = None) -> SupplementationAssessment:
    """Read pinned evidence and reconstruct the unchanged geographic policy in memory."""
    recorded = load_notebook_evidence("efficiency", repo_root=repo_root)
    LOGGER.info("Loaded captured efficiency build %s", recorded.metadata["build_id"])
    bundle = recorded.bundle
    if regions is None:
        regions = _configured_regions(bundle.repo_root)
    rules = load_harmonization_rules(bundle, "efficiencies")
    spec = rules["ratings"]["composition"]
    conversions = load_conversion_factors(bundle)
    ranges = load_harmonization_rules(bundle, "ldv_ev_ranges")
    rating_class_map = load_harmonization_rules(bundle, "fueleconomy_vehicle_data")["vclass_to_nrcan"]
    adapter = bundle.sources.sources["fueleconomy_gov_vehicle_data"].components["vehicles"].adapter
    cache = resolve_input_path(bundle, "cache", adapter["cache_path"])
    if cache.stat().st_size != adapter["expected_bytes"] or file_sha256(cache) != adapter["expected_sha256"]:
        raise ValueError("Pinned EPA cache differs from the captured source registration")
    with ZipFile(cache) as archive:
        raw_epa = pd.read_csv(archive.open(adapter["archive_member"]), low_memory=False)
    epa = prepare_epa(raw_epa, spec, rating_class_map, ranges, conversions)
    LOGGER.info("Validated and converted %s pinned EPA rows", len(epa))
    ratings = pd.read_csv(recorded.paths["ratings"], low_memory=False)
    stock = pd.read_csv(recorded.paths["benchmark_mto_mapped"], low_memory=False)
    incentives = pd.read_csv(recorded.paths["benchmark_izev_records"], low_memory=False, usecols=[
        "source_id", "component", "source_row", "model_year", "request_year", "province",
        "make", "model", "powertrain", "count", spec["izev_transactions"]["column"],
    ])
    class_weights = pd.read_csv(recorded.paths["rating_class_weights"], low_memory=False)
    weight_basis = load_harmonization_rules(bundle, "road_aggregation")["ldv_weight_basis"]
    LOGGER.info("Replaying current empirical composition for %s (native source rows unchanged)", regions)
    composition = aggregate_empirical_ratings(ratings, class_weights, stock, incentives, rules=rules,
        composition_rules=spec, regions=list(regions), weight_basis=weight_basis)
    # Recomputed ON rows must reconcile with the published capture before making
    # claims about other regional pools reconstructed from the same native inputs.
    captured = pd.read_csv(recorded.paths["composition_coverage"], low_memory=False)
    keys = ["region", "year", "powertrain", "rating_class"]
    controls = captured.loc[captured.region.isin(regions)]
    if controls.empty:
        raise ValueError("At least one requested region must have a captured control")
    replay_control = composition.coverage.loc[composition.coverage.region.isin(controls.region.unique())]
    identity_check = replay_control[keys].merge(controls[keys], on=keys, how="outer", indicator=True, validate="one_to_one")
    if not identity_check._merge.eq("both").all() or identity_check.empty:
        raise ValueError("Current composition replay has different captured cohort coverage")
    shared = replay_control.merge(controls, on=keys, suffixes=("_replay", "_capture"), validate="one_to_one")
    for field in ("class_mean_mj_per_vkm", "selected_source_count", "observed_source_count", "weight"):
        if not np.allclose(shared[f"{field}_replay"], shared[f"{field}_capture"], atol=1e-10, rtol=1e-10, equal_nan=True):
            raise ValueError(f"Current composition replay differs from captured {field}")
    LOGGER.info("Replayed %s empirical descriptions in %s; captured control agrees", len(composition.observations), regions)
    pairs = catalogue_pairs(ratings, epa, spec, conversions)
    LOGGER.info("Screened %s catalogue pairs", len(pairs))
    dispositions = observation_dispositions(composition.observations, ratings, epa, spec)
    cohort_keys = ["region", "year", "rating_class", "powertrain"]
    candidates = dispositions.assign(rating_class=dispositions.epa_candidate_classes, powertrain=dispositions.epa_candidate_powertrains)
    dispositions["within_existing_rated_cohort"] = pd.MultiIndex.from_frame(candidates[cohort_keys]).isin(
        pd.MultiIndex.from_frame(composition.coverage[cohort_keys]))
    dispositions.loc[~dispositions.within_existing_rated_cohort, "candidate_recovery_upper_bound"] = 0
    supported_years = set(composition.coverage.year)
    outside_years = dispositions.match_status.eq("unmatched_ratings") & ~dispositions.year.isin(supported_years)
    dispositions.loc[outside_years, "coverage_disposition"] = "outside_captured_rating_year_support"
    LOGGER.info("Reconciled empirical rating gaps")
    pools, cohorts = coverage_tables(dispositions, composition.coverage)
    LOGGER.info("Built pool and cohort coverage tables")
    holdouts = holdout_validation(pairs)
    LOGGER.info("Completed family and model-year holdouts")
    sensitivity = cohort_sensitivity(composition.coverage, composition.observations, composition.variants, pairs)
    LOGGER.info("Completed fixed-weight cohort sensitivity")
    benefit = candidate_benefit_bounds(dispositions, composition.coverage, composition.matches)
    frames = dict(observation_decisions=dispositions, pool_coverage=pools, cohort_coverage=cohorts,
                  catalogue_pairs=pairs, agreement_summary=_agreement_summary(pairs), holdouts=holdouts,
                  cohort_sensitivity=sensitivity, native_label_consistency=_native_label_summary(epa), candidate_benefit_bounds=benefit)
    summary = dict(
        decision="retain_nrcan_only_consumption", build=recorded.metadata,
        regions=list(regions), captured_regions=sorted(captured.region.unique()),
        supported_model_year_window=[spec["izev_model_year_min"], spec["izev_model_year_max"]],
        regional_reconstruction="Current aggregate_empirical_ratings in memory, captured config and native inputs; ON reconciled with captured class means/counts/weights",
        accepted_recovered_count=0, physically_verified_pairs=0,
        strict_consumption_change_mj_per_vkm=0.,
        limitations=["No independent physical correspondence or Canadian configuration verification exists in these inputs.",
            "Catalogue screens and counterfactual holdouts test agreement only; selection on shared catalogue names is not representativeness validation.",
            "MTO propulsion is unobserved. Documentary identity gates cannot be bypassed by locating a US rating.",
            "Historical proxy counts repeat their supported source window across target vintages and are not additive.",
            "National pools overlap between recipient regions; MTO Ontario proxies are not independent provincial observations.",
            "No EPA consumption source component, DQ acceptance or production configuration is created by this analysis."],
        epa_input=dict(path=str(cache), sha256=file_sha256(cache), rows=len(raw_epa)),
        analysis_code={name: file_sha256(path) for name, path in {
            "validation.ldv_epa_supplementation": Path(__file__),
            "parameterization.rating_composition": bundle.repo_root / "src/parameterization/rating_composition.py",
        }.items()},
        evidence_inputs={name: dict(path=str(recorded.paths[name]), sha256=file_sha256(recorded.paths[name]))
            for name in ("ratings", "composition_coverage", "composition_observations", "benchmark_mto_mapped", "benchmark_izev_records", "rating_class_weights")},
        commands=["uv run python -m validation.ldv_epa_supplementation --write-report"],
        screen="Exact configured normalized year/make/model + mapped class + powertrain; stronger screen checks engine/cylinders/transmission/fuel grade or native range-rounding agreement and bucket; reciprocal unique both catalogues. Physical correspondence remains unverified.",
        holdouts="Entire EPA baseModel family across years, and entire model year, independently within powertrain and through2007/2008-10/2011-14/2015-16/2017+ revision groups; optional median ratio fitted to training only.",
        conversions=conversions,
    )
    return SupplementationAssessment(frames=frames, summary=summary)


def _report_directory(repo_root: Path | None = None) -> Path:
    from utils import find_repo_root

    root = (repo_root or find_repo_root()).resolve()
    paths = PathsConfig.model_validate(load_yaml(root / "config/paths.yaml"))
    return root / paths.artifacts["ldv_epa_supplementation_report"].path


def load_report(*, repo_root: Path | None = None) -> SupplementationAssessment:
    """Read an explicit report only after checking its evidence/code/export identities.

    Notebook readers do not rerun composition or write artifacts. Missing or stale
    evidence requires the explicit assessment command shown in the error.
    """
    directory = _report_directory(repo_root)
    command = "uv run python -m validation.ldv_epa_supplementation --write-report"
    path = directory / "summary.json"
    if not path.is_file():
        raise FileNotFoundError(f"EPA assessment report is unavailable. Run: {command}")
    summary = json.loads(path.read_text(encoding="utf-8"))
    recorded = load_notebook_evidence("efficiency", repo_root=repo_root)
    if summary["build"]["build_id"] != recorded.metadata["build_id"]:
        raise ValueError(f"EPA assessment belongs to a different captured build. Run: {command}")
    if tuple(summary["regions"]) != _configured_regions(recorded.bundle.repo_root):
        raise ValueError(f"EPA assessment diagnostic regions changed. Run: {command}")
    code_paths = {"validation.ldv_epa_supplementation": Path(__file__),
                  "parameterization.rating_composition": recorded.bundle.repo_root / "src/parameterization/rating_composition.py"}
    if any(summary["analysis_code"].get(name) != file_sha256(source) for name, source in code_paths.items()):
        raise ValueError(f"EPA assessment code changed. Run: {command}")
    for name, expected in summary["evidence_inputs"].items():
        if file_sha256(recorded.paths[name]) != expected["sha256"]:
            raise ValueError(f"EPA assessment input {name} changed. Run: {command}")
    adapter = recorded.bundle.sources.sources["fueleconomy_gov_vehicle_data"].components["vehicles"].adapter
    cache = resolve_input_path(recorded.bundle, "cache", adapter["cache_path"])
    if (file_sha256(cache) != summary["epa_input"]["sha256"]
            or summary["epa_input"]["sha256"] != adapter["expected_sha256"]):
        raise ValueError(f"EPA assessment pinned cache changed. Run: {command}")
    frames = {}
    for name, expected in summary["exports"].items():
        target = (directory / expected["file"]).resolve()
        if not target.is_relative_to(directory.resolve()):
            raise ValueError("EPA assessment export escapes its report directory")
        if not target.is_file():
            raise FileNotFoundError(f"EPA assessment export {name} is unavailable. Run: {command}")
        if file_sha256(target) != expected["sha256"]:
            raise ValueError(f"EPA assessment export {name} changed. Run: {command}")
        frame = pd.read_csv(target, low_memory=False)
        if len(frame) != expected["rows"]:
            raise ValueError(f"EPA assessment export {name} has a different row count. Run: {command}")
        frames[name] = frame
    return SupplementationAssessment(frames=frames, summary=summary)


def write_report(assessment: SupplementationAssessment, *, repo_root: Path | None = None) -> Path:
    """Write only opt-in review artifacts to their live configured route."""
    directory = _report_directory(repo_root)
    exports = {}
    for name, frame in assessment.frames.items():
        path = directory / f"{name}.csv"
        write_dataframe_atomic(frame, path)
        exports[name] = dict(file=path.name, rows=len(frame), sha256=file_sha256(path))
    write_text_atomic(json.dumps({**assessment.summary, "exports": exports}, indent=2, allow_nan=False) + "\n", directory / "summary.json")
    return directory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    assessment = assess_supplementation()
    print(json.dumps({key: assessment.summary[key] for key in ("decision", "regions", "accepted_recovered_count", "physically_verified_pairs")}, indent=2))
    print(assessment.frames["agreement_summary"].to_string(index=False))
    if args.write_report:
        print(f"Supplementation report: {write_report(assessment)}")


if __name__ == "__main__":
    main()
