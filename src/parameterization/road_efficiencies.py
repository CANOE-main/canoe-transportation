"""Road efficiency calculations on normalized evidence; no SQLite or downloads."""

from __future__ import annotations

from functools import cache
import logging
from math import isfinite
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fetching.nlr_atb_autonomie import configured_trajectory
from utils import (
    ConfigBundle,
    load_conversion_factors,
    load_harmonization_rules,
    resolve_input_path,
)


def positive(value: Any, label: str) -> float:
    value = float(value)
    if not isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be finite and positive: {value}")
    return value


def interpolate(
    frame: pd.DataFrame,
    year: int,
    value: str,
    *,
    year_col: str = "year",
    allow_zero: bool = False,
) -> float:
    """Linear interpolation of consumption, never positional matching/extrapolation."""
    ordered = frame.sort_values(year_col)
    if ordered.empty or ordered[year_col].duplicated().any():
        raise ValueError(f"Missing or ambiguous {value} series")
    years = pd.to_numeric(ordered[year_col], errors="raise").to_numpy(dtype=float)
    values = pd.to_numeric(ordered[value], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(values).all() or (
        (values < 0).any() if allow_zero else (values <= 0).any()
    ):
        raise ValueError(f"Invalid {value} series")
    if year < years.min() or year > years.max():
        raise ValueError(
            f"No extrapolation for {value}: {year} outside {years.min()}..{years.max()}"
        )
    return float(np.interp(year, years, values))


def observed_value(
    frame: pd.DataFrame, year: int, value: str, *, year_col: str = "year", allow_zero: bool = False,
) -> float | None:
    """Read one exact source observation without filling absent model years."""
    selected = frame.loc[frame[year_col].eq(year)]
    if len(selected) > 1:
        raise ValueError(f"Ambiguous {value} observation for {year}")
    if selected.empty or pd.isna(selected.iloc[0][value]):
        return None
    if allow_zero:
        observed = float(selected.iloc[0][value])
        if not isfinite(observed) or observed < 0:
            raise ValueError(f"{value} observation for {year} must be finite and non-negative: {observed}")
        return observed
    return positive(selected.iloc[0][value], f"{value} observation for {year}")


def require_observed_value(
    frame: pd.DataFrame, year: int, value: str, *, year_col: str = "year", allow_zero: bool = False,
) -> float:
    """Require the source year used by an annual calculation."""
    observed = observed_value(frame, year, value, year_col=year_col, allow_zero=allow_zero)
    if observed is None:
        raise ValueError(f"Missing exact {value} observation for {year}")
    return observed


def source_point(
    frame: pd.DataFrame, year: int, value: str, *, year_col: str = "year"
) -> tuple[float, int]:
    """Hold the previous source observation, clamping to the first before coverage."""
    ordered = frame.copy()
    ordered[year_col] = pd.to_numeric(ordered[year_col], errors="raise")
    if ordered.empty or ordered[year_col].duplicated().any():
        raise ValueError(f"Missing or ambiguous {value} source points")
    ordered = ordered.sort_values(year_col)
    years = ordered[year_col]
    if not np.isfinite(years).all() or years.mod(1).ne(0).any():
        raise ValueError(f"Invalid {year_col} source points")
    available = ordered.loc[years.le(year)]
    selected = available.iloc[-1] if not available.empty else ordered.iloc[0]
    source_year = int(selected[year_col])
    return positive(selected[value], f"{value} source point {source_year}"), source_year


def ceud_series(
    frame: pd.DataFrame, table: int, series: str, unit: str
) -> pd.DataFrame:
    selected = frame.loc[frame.table_id.eq(table) & frame.raw_series.eq(series)].copy()
    if (
        selected.empty
        or set(selected.unit) != {unit}
        or selected.year.duplicated().any()
    ):
        raise ValueError(
            f"Missing, ambiguous, or wrong-unit CEUD series: {table}/{series}/{unit}"
        )
    selected["value"] = selected.value.map(lambda v: positive(v, series))
    return selected[["year", "value"]].sort_values("year")


def derive_load_factors(
    ceud: pd.DataFrame,
    *,
    rules: dict,
    activity_rules: dict,
    conversions: dict,
) -> pd.DataFrame:
    records = []
    scale = conversions["service"]["million_service_per_thousand_vehicles"]
    for region, regional in ceud.groupby("region", sort=True):
        for mode, spec in rules["road_classes"].items():
            activity = activity_rules[mode]
            a = ceud_series(
                regional, activity["table_id"], activity["raw_series"], "millions"
            )
            s = ceud_series(regional, spec["stock_table"], spec["stock"], "thousands")
            d = ceud_series(regional, spec["stock_table"], spec["distance"], "km")
            if set(a.year) != set(s.year) or set(a.year) != set(d.year):
                raise ValueError(
                    f"CEUD load-factor year coverage differs: {region}/{mode}"
                )
            joined = (
                a.rename(columns={"value": "activity_millions"})
                .merge(
                    s.rename(columns={"value": "stock_thousands"}),
                    on="year",
                    validate="one_to_one",
                )
                .merge(
                    d.rename(columns={"value": "distance_km"}),
                    on="year",
                    validate="one_to_one",
                )
            )
            joined["load_factor"] = (
                joined.activity_millions
                * scale
                / (joined.stock_thousands * joined.distance_km)
            )
            joined["region"], joined["mode"] = region, mode
            (
                joined["activity_series"],
                joined["stock_series"],
                joined["distance_series"],
            ) = activity["raw_series"], spec["stock"], spec["distance"]
            joined["unit"] = (
                "passengers/vehicle"
                if spec["service"] == "passenger"
                else "tonnes/vehicle"
            )
            records.append(joined)
    return pd.concat(records, ignore_index=True).sort_values(["region", "mode", "year"])


def parse_nrcan_phev_cd_components(raw: pd.Series, *, contract: dict) -> pd.DataFrame:
    """Read explicit CD electricity and fuel rates without treating Le as electricity."""
    pattern = contract["combined_cd_components_pattern"]
    # Python's regex conditional enforces the paired brackets in the mixed-fuel form.
    text = raw.astype("string[python]")
    matched = text.str.fullmatch(pattern, na=False)
    if not matched.all():
        raise ValueError(
            "Unsupported NRCan PHEV CD component text in "
            f"{int((~matched).sum())} rows; indices={raw.index[~matched].tolist()[:10]}"
        )
    columns = ["cd_electricity_kwh_per_100km", "cd_fuel_l_per_100km"]
    parsed = text.str.extract(pattern)
    if missing := set(columns) - set(parsed.columns):
        raise ValueError(f"NRCan PHEV CD component pattern is missing captures: {sorted(missing)}")
    result = parsed[columns].copy()
    # The configured pure-electric syntax explicitly contains no liquid-fuel term.
    result["cd_fuel_l_per_100km"] = result.cd_fuel_l_per_100km.fillna("0")
    result = result.astype(float)
    values = result.to_numpy()
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("NRCan PHEV CD components must be finite and nonnegative")
    return result


def apply_rating_outlier_filter(frame: pd.DataFrame, *, rules: dict) -> pd.DataFrame:
    """Screen equal-weight make/model means once; retain every native source row."""
    spec = rules["ratings"]["outlier_filter"]
    cohort = ["year", "rating_class", "powertrain", "native_unit"]
    supported = {
        "method": "tukey_iqr", "metric": "consumption_mj_per_vkm",
        "cohort_fields": cohort, "variant_aggregation": "mean", "tails": "both",
        "quartile_interpolation": "linear", "insufficient_support": "retain", "zero_iqr": "retain",
        "missing_energy_metric": "retain_native_audit",
    }
    for key, expected in supported.items():
        if spec.get(key) != expected:
            raise ValueError(f"Unsupported NRCan outlier_filter.{key}: {spec.get(key)!r}")
    minimum, multiplier = spec.get("minimum_models"), spec.get("iqr_multiplier")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 1:
        raise ValueError("NRCan outlier minimum_models must be a positive integer")
    if (isinstance(multiplier, bool) or not isinstance(multiplier, (int, float))
            or not np.isfinite(multiplier) or multiplier <= 0):
        raise ValueError("NRCan outlier iqr_multiplier must be finite and positive")
    unconverted = spec.get("unconverted_powertrains")
    if (not isinstance(unconverted, list) or any(not isinstance(value, str) for value in unconverted)
            or len(unconverted) != len(set(unconverted)) or set(unconverted) - set(rules["ldv_archetypes"])):
        raise ValueError("NRCan outlier unconverted_powertrains must name distinct configured powertrains")
    identities = [rules["ratings"]["fields"][name] for name in ("make", "model")]
    keys, metric = [*cohort, *identities], spec["metric"]
    if missing := {"included", metric, *keys} - set(frame):
        raise ValueError(f"NRCan outlier filtering requires columns: {sorted(missing)}")
    if not pd.api.types.is_bool_dtype(frame.included) or frame.included.isna().any():
        raise ValueError("NRCan outlier filtering requires complete boolean inclusion flags")
    result = frame.copy()
    result["included_before_filter"] = result.included
    observed = result.loc[result.included, [*keys, metric]].copy()
    if observed[keys].isna().any().any():
        raise ValueError("NRCan outlier filtering requires complete eligible cohort and identity keys")
    for column in [*cohort[1:], *identities]:
        if observed[column].astype("string[python]").str.strip().eq("").any():
            raise ValueError(f"NRCan outlier filtering requires nonblank eligible {column}")
    observed[metric] = pd.to_numeric(observed[metric], errors="raise")
    unavailable = result.included_before_filter & result.powertrain.isin(unconverted) & result[metric].isna()
    observed = observed.loc[~(observed.powertrain.isin(unconverted) & observed[metric].isna())]
    if not np.isfinite(observed[metric]).all() or observed[metric].le(0).any():
        raise ValueError("NRCan outlier filtering requires finite positive eligible total consumption")

    mean = "outlier_model_mean_mj_per_vkm"
    count = "outlier_cohort_model_count"
    q1, q3 = "outlier_q1_mj_per_vkm", "outlier_q3_mj_per_vkm"
    lower, upper = "outlier_lower_fence_mj_per_vkm", "outlier_upper_fence_mj_per_vkm"
    models = observed.groupby(keys, sort=True)[metric].mean().rename(mean).to_frame()
    grouped = models.groupby(level=cohort)[mean]
    models[count] = grouped.transform("size").astype("Int64")
    models[q1] = grouped.transform(lambda values: values.quantile(.25, interpolation=spec["quartile_interpolation"]))
    models[q3] = grouped.transform(lambda values: values.quantile(.75, interpolation=spec["quartile_interpolation"]))
    iqr = models[q3] - models[q1]
    models[lower], models[upper] = models[q1] - multiplier * iqr, models[q3] + multiplier * iqr
    models["outlier_status"] = "within_fences"
    enough = models[count].ge(minimum)
    models.loc[~enough, "outlier_status"] = "insufficient_models"
    models.loc[enough & iqr.eq(0), "outlier_status"] = "zero_iqr"
    supported_cohort = enough & iqr.gt(0)
    models.loc[supported_cohort & models[mean].lt(models[lower]), "outlier_status"] = "below_lower_fence"
    models.loc[supported_cohort & models[mean].gt(models[upper]), "outlier_status"] = "above_upper_fence"
    result = result.join(models, on=keys, validate="many_to_one")
    ineligible = ~result.included_before_filter
    result.loc[ineligible | unavailable, [mean, count, q1, q3, lower, upper]] = pd.NA
    result.loc[ineligible, "outlier_status"] = "not_eligible"
    result.loc[unavailable, "outlier_status"] = "unavailable_energy_metric"
    result["outlier_excluded"] = result.outlier_status.isin(["below_lower_fence", "above_upper_fence"])
    result["filter_reason"] = result.outlier_status.where(result.outlier_excluded, "")
    result["included"] = result.included_before_filter & ~result.outlier_excluded
    cohort_audit = models.reset_index().drop_duplicates(cohort)
    logging.getLogger(__name__).info(
        "NRCan outlier filter: %s of %s eligible source rows excluded; %s sparse and %s zero-IQR cohorts retained; %s native-only rows retained without energy screening; all %s rows retained in audit",
        int(result.outlier_excluded.sum()), int(result.included_before_filter.sum()),
        int(cohort_audit[count].lt(minimum).sum()),
        int((cohort_audit[count].ge(minimum) & cohort_audit[q1].eq(cohort_audit[q3])).sum()),
        int(unavailable.sum()), len(result),
    )
    return result


def classify_ratings(
    frames: list[pd.DataFrame],
    epa: pd.DataFrame,
    *,
    rules: dict,
    conversions: dict,
    phev_contract: dict | None = None,
    uf_by_powertrain: dict[str, float] | None = None,
    range_rules=None,
) -> pd.DataFrame:
    """Retain native rows; exact EPA hybrid evidence supplements the legacy name rule."""
    spec = rules["ratings"]
    if spec["range_mapping"] != "omega_bucket_boundaries" or range_rules is None:
        raise ValueError("NRCan EV ratings require the configured OMEGA range-bucket contract")
    from parameterization.ldv_ev_ranges import range_bucket
    fields = spec["fields"]
    supported = {
        "evidence_join": "exact_normalized_year_make_model",
        "ambiguous_evidence": "retain_name_rule_and_report",
        "phev_baseline": "nrcan_cd_cs_atb_bucket_uf",
    }
    for key, expected in supported.items():
        if spec[key] != expected:
            raise ValueError(f"Unsupported ratings.{key}: {spec[key]!r}")

    def norm(values):
        return (
            values.fillna("")
            .astype(str)
            .str.lower()
            .str.replace(spec["normalization_pattern"], "", regex=True)
        )

    evidence = epa[spec["evidence_columns"]].copy()
    evidence["make_key"], evidence["model_key"] = (
        norm(evidence.make),
        norm(evidence.model),
    )
    evidence["is_hybrid"] = evidence.atvType.eq(spec["hybrid_evidence_label"])
    grouped = evidence.groupby(["year", "make_key", "model_key"]).is_hybrid.agg(
        ["min", "max"]
    )
    results = []
    for source in frames:
        frame = source.copy()
        frame["year"] = pd.to_numeric(frame[fields["year"]], errors="raise").astype(int)
        frame["rating_class"] = frame[fields["class"]]
        frame["classification_method"] = "source_fuel"
        if fields["electricity"] in frame:
            frame["legacy_range_label"] = pd.cut(
                frame[fields["bev_range"]],
                spec["bev_range_edges_km"],
                labels=spec["bev_range_labels"],
            ).astype("string")
            frame["range_km"] = pd.to_numeric(frame[fields["bev_range"]], errors="raise")
            frame["powertrain"] = frame.range_km.map(
                lambda value: range_bucket("BEV", value / conversions["length"]["mile_to_km"], range_rules)
            )
            frame["native_consumption"] = pd.to_numeric(
                frame[fields["electricity"]], errors="raise"
            )
            frame["native_unit"] = "kWh/100 km"
            frame["consumption_mj_per_vkm"] = (
                frame.native_consumption
                * conversions["energy"]["kwh_to_mj"]
                * conversions["service"]["per_100_km_to_per_km"]
            )
            frame["classification_method"] = "omega_range_bucket"
        elif fields["phev_range"] in frame:
            if (
                phev_contract is None
                or phev_contract["combined_cd_parse"] != "leading_and_explicit_components"
            ):
                raise ValueError(
                    "NRCan PHEV requires the configured explicit-component audit contract"
                )
            frame["legacy_range_label"] = pd.cut(
                frame[fields["phev_range"]],
                spec["phev_range_edges_km"],
                labels=spec["phev_range_labels"],
            ).astype("string")
            frame["range_km"] = pd.to_numeric(frame[fields["phev_range"]], errors="raise")
            frame["powertrain"] = frame.range_km.map(
                lambda value: range_bucket("PHEV", value / conversions["length"]["mile_to_km"], range_rules)
            )
            if uf_by_powertrain is None or (missing := set(frame.powertrain) - set(uf_by_powertrain)):
                raise ValueError(
                    "NRCan PHEV requires explicit ATB utility factors for every range bucket"
                    + (f": {sorted(missing)}" if uf_by_powertrain is not None else "")
                )
            uf = pd.to_numeric(frame.powertrain.map(uf_by_powertrain), errors="raise")
            if not np.isfinite(uf).all() or not uf.between(0, 1).all():
                raise ValueError("NRCan PHEV utility factors must be finite and lie in [0, 1]")
            cs = pd.to_numeric(
                frame[fields["consumption"]], errors="raise"
            )
            if cs.isna().any() or not np.isfinite(cs).all() or cs.le(0).any():
                raise ValueError("NRCan PHEV CS consumption is missing or invalid")
            frame[phev_contract["combined_cs_output_numeric_field"]] = cs
            raw = frame[phev_contract["combined_cd_raw_column"]].astype(str)
            frame[phev_contract["combined_cd_output_raw_field"]] = raw
            frame[phev_contract["combined_cd_output_numeric_field"]] = pd.to_numeric(
                raw.str.extract(
                    phev_contract["combined_cd_leading_numeric_pattern"], expand=False
                ),
                errors="raise",
            )
            cd = frame[phev_contract["combined_cd_output_numeric_field"]]
            if cd.isna().any() or not np.isfinite(cd).all() or cd.lt(0).any():
                raise ValueError("NRCan PHEV CD leading value is missing or invalid")
            components = parse_nrcan_phev_cd_components(raw, contract=phev_contract)
            frame[components.columns] = components
            frame["cs_fuel_l_per_100km"] = cs
            frame["fleet_utility_factor"] = uf
            frame["utility_factor_basis"] = "ATB_range_bucket"
            cs_fuel = frame[fields["phev_fuel"]].map(spec["fuel_codes"])
            hhv = cs_fuel.map({
                fuel: conversions["fuel_hhv"][fuel + "_mj_per_litre"]
                for fuel in ("gasoline", "diesel")
            })
            if hhv.isna().any():
                raise ValueError("NRCan PHEV requires a supported charge-sustaining liquid fuel")
            per_km = conversions["service"]["per_100_km_to_per_km"]
            frame["utility_weighted_electricity_mj_per_vkm"] = (
                uf * frame.cd_electricity_kwh_per_100km * conversions["energy"]["kwh_to_mj"] * per_km
            )
            frame["utility_weighted_fuel_mj_per_vkm"] = (
                uf * frame.cd_fuel_l_per_100km + (1 - uf) * cs
            ) * hhv * per_km
            frame["native_consumption"] = cd
            frame["native_unit"] = "Le/100 km (CD)"
            frame["consumption_mj_per_vkm"] = (
                frame.utility_weighted_electricity_mj_per_vkm + frame.utility_weighted_fuel_mj_per_vkm
            )
            frame["classification_method"] = "omega_range_bucket_cd_cs_atb_uf"
        else:
            frame["powertrain"] = frame[fields["fuel"]].map(spec["fuel_codes"])
            frame["make_key"], frame["model_key"] = (
                norm(frame[fields["make"]]),
                norm(frame[fields["model"]]),
            )
            frame = frame.join(
                grouped, on=["year", "make_key", "model_key"], validate="many_to_one"
            )
            named = frame[fields["model"]].str.contains(
                spec["hybrid_pattern"], na=False
            )
            matched = frame["min"].eq(True) & frame["max"].eq(True)
            ambiguous = frame["min"].eq(False) & frame["max"].eq(True)
            gasoline = frame.powertrain.eq("gasoline")
            frame.loc[gasoline & (named | matched), "powertrain"] = "hev"
            frame.loc[gasoline & named, "classification_method"] = "hybrid_name"
            frame.loc[gasoline & matched, "classification_method"] = "exact_epa_hybrid"
            frame["ambiguous_epa_match"] = ambiguous
            frame["native_consumption"] = pd.to_numeric(
                frame[fields["consumption"]], errors="raise"
            )
            frame["native_unit"] = "L/100 km"
            hhv = frame.powertrain.replace({"hev": "gasoline"}).map(
                {
                    f: conversions["fuel_hhv"][f + "_mj_per_litre"]
                    for f in ("gasoline", "diesel")
                }
            )
            frame["consumption_mj_per_vkm"] = (
                frame.native_consumption
                * hhv
                * conversions["service"]["per_100_km_to_per_km"]
            )
            frame = frame.drop(columns=["min", "max"])
        if frame.powertrain.isna().any():
            raise ValueError(
                f"Unclassified NRCan rows: {frame.loc[frame.powertrain.isna(), ['component', 'source_row']].to_dict('records')[:10]}"
            )
        frame["included"] = ~frame.powertrain.isin(
            spec["excluded_fuels"]
        ) & ~frame.rating_class.isin(spec["ignored_classes"])
        if (frame.native_consumption <= 0).any() or not np.isfinite(
            frame.native_consumption
        ).all():
            raise ValueError("Nonpositive/nonfinite source rating")
        results.append(frame)
    classified = pd.concat(results, ignore_index=True).sort_values(["component", "source_row"])
    return apply_rating_outlier_filter(classified, rules=rules)


def distinct_rating_models(
    classified: pd.DataFrame, *, rules: dict, value_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Average rated variants within each exact make/model and annual class cohort."""
    fields = rules["ratings"]["fields"]
    cohort = ["year", "powertrain", "rating_class", "native_unit"]
    identities = [fields["make"], fields["model"]]
    values = list(dict.fromkeys([
        "native_consumption", "consumption_mj_per_vkm", "range_km", *value_columns,
    ]))
    required = {"included", *cohort, *identities, *values} - {"range_km"}
    if missing := required - set(classified):
        raise ValueError(f"Distinct-model rating evidence is missing columns: {sorted(missing)}")
    observed = classified.loc[classified.included].copy()
    missing_identity = observed[identities].isna().any(axis=1)
    for field in identities:
        missing_identity |= observed[field].astype(str).str.strip().eq("")
    if missing_identity.any():
        raise ValueError(
            "Distinct-model ratings require nonblank make and model identities; "
            f"missing in {int(missing_identity.sum())} included rows"
        )
    if observed[cohort].isna().any().any():
        raise ValueError("Distinct-model ratings require complete annual class/powertrain/unit keys")
    if observed.groupby(cohort[:-1]).native_unit.nunique().gt(1).any():
        raise ValueError("Conflicting native units within an annual rating class/powertrain cohort")
    if "range_km" not in observed:
        observed["range_km"] = np.nan
    return observed.groupby([*cohort, *identities], as_index=False, sort=True).agg(
        **{field: (field, "mean") for field in values},
        catalogue_rows=("native_consumption", "size"),
    )


def rating_class_statistics(
    classified: pd.DataFrame, *, rules: dict, statistic: str,
    value_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Apply the selected statistic equally across distinct models within each cohort."""
    if statistic not in {"mean", "median"}:
        raise ValueError(f"Unsupported distinct-model rating statistic: {statistic!r}")
    models = distinct_rating_models(classified, rules=rules, value_columns=value_columns)
    values = list(dict.fromkeys([
        "native_consumption", "consumption_mj_per_vkm", "range_km", *value_columns,
    ]))
    annual = models.groupby(
        ["year", "powertrain", "rating_class", "native_unit"], as_index=False, sort=True,
    ).agg(
        **{field: (field, statistic) for field in values},
        observations=("catalogue_rows", "sum"),
        distinct_models=("catalogue_rows", "size"),
    )
    annual["rating_statistic"] = statistic
    return annual


def aggregate_ratings(
    classified: pd.DataFrame, weights: pd.DataFrame, *, rules: dict, weight_basis: str,
    statistic: str,
) -> pd.DataFrame:
    """Summarize distinct models within classes, then reweight observed classes."""
    weights = weights.loc[weights.weight_basis.eq(weight_basis)].copy()
    if weights.empty or weights.report_year.nunique() != 1:
        raise ValueError("Expected one reviewed NRCan class-weight edition")
    if weights.duplicated(["nrcan_ceud_class", "nrcan_vehicle_class"]).any():
        raise ValueError("Duplicate NRCan aggregation weights")
    suv = rules["historical_suv"]
    annual = rating_class_statistics(classified, rules=rules, statistic=statistic)
    records = []
    for ceud_class, class_weights in weights.groupby("nrcan_ceud_class", sort=True):
        weight_map = class_weights.set_index(
            "nrcan_vehicle_class"
        ).aggregation_weight.to_dict()
        if (
            any(not isfinite(w) or w < 0 for w in weight_map.values())
            or abs(sum(weight_map.values()) - 1) > rules["tolerances"]["weight_sum"]
        ):
            raise ValueError(f"Invalid NRCan aggregation weights: {ceud_class}")
        for (year, powertrain), group in annual.groupby(
            ["year", "powertrain"], sort=True
        ):
            selected_weights = weight_map.copy()
            if suv["source_label"] in set(group.rating_class):
                if set(suv["combined_weight_classes"]) & set(group.rating_class):
                    raise ValueError(
                        "Unsplit and split SUV observations coexist; allocation needs review"
                    )
                selected_weights[suv["source_label"]] = sum(
                    selected_weights.pop(c, 0) for c in suv["combined_weight_classes"]
                )
            group = group.copy()
            group["raw_weight"] = group.rating_class.map(selected_weights).fillna(0)
            group = group.loc[group.raw_weight.gt(0)]
            total = float(group.raw_weight.sum())
            if total <= 0:
                continue
            for item in group.itertuples(index=False):
                records.append(
                    {
                        "ceud_class": ceud_class,
                        "year": year,
                        "powertrain": powertrain,
                        "rating_class": item.rating_class,
                        "native_class_mean": item.native_consumption,
                        "class_mean_mj_per_vkm": item.consumption_mj_per_vkm,
                        "native_unit": item.native_unit,
                        "observations": item.observations,
                        "distinct_models": item.distinct_models,
                        "rating_statistic": statistic,
                        "original_weight": item.raw_weight,
                        "observed_weight_sum": total,
                        "weight": item.raw_weight / total,
                        "range_class_mean_km": item.range_km,
                        "aggregation_method": "distinct_model_class_weighted",
                        "matched_stock": 0.0,
                        "cohort_matched_stock": 0.0,
                    }
                )
    return pd.DataFrame(records)


def select_atb_consumption(
    vehicles: pd.DataFrame,
    phev: pd.DataFrame,
    *,
    trajectory: str,
    rules: dict,
    conversions: dict,
) -> pd.DataFrame:
    """Consume derived PHEV products, never their final output-table fuel economy."""
    records = []
    for family, archetypes in (
        ("ldv", rules["ldv_archetypes"]),
        ("mhdv", rules["mhdv_archetypes"]),
    ):
        category = rules["atb"]["weight_categories"][family]
        for powertrain, spec in archetypes.items():
            is_phev = powertrain.startswith("phev")
            source = phev if is_phev else vehicles
            selected = source.loc[
                source.trajectory.eq(trajectory)
                & source.vehicle_weight_category.eq(category)
            ]
            if "range_mi" in spec:
                selected = selected.loc[selected.electric_range_mi.eq(spec["range_mi"])]
            else:
                selected = selected.loc[
                    selected.vehicle_detail.str.strip().eq(spec["detail"])
                ]
            if not is_phev:
                selected = selected.loc[
                    selected.metric_key.isin(rules["atb"]["metric_basis"])
                ]
            if selected.empty or selected.duplicated(["year", "vehicle_class"]).any():
                raise ValueError(
                    f"Missing or ambiguous ATB archetype {family}/{powertrain}/{trajectory}"
                )
            for item in selected.to_dict("records"):
                if is_phev:
                    basis = item["fuel_equivalent_basis"]
                    energy = positive(
                        item["total_utility_weighted_energy_wh_equivalent_per_mi"],
                        "PHEV energy",
                    )
                    share = float(item["electricity_input_share"])
                    if (
                        not 0 <= share <= 1
                        or abs(share + item["liquid_fuel_input_share"] - 1)
                        > rules["tolerances"]["split_sum"]
                    ):
                        raise ValueError("Invalid source-derived PHEV energy shares")
                    source_value, source_unit = energy, "Wh-equivalent/mi"
                else:
                    basis = rules["atb"]["metric_basis"][item["metric_key"]]
                    source_value = positive(item["value"], "ATB fuel economy")
                    source_unit = item["unit"]
                    if source_unit != "mi/" + basis:
                        raise ValueError("ATB equivalent-gallon unit mismatch")
                    energy = (
                        conversions["energy"][
                            rules["atb"]["source_energy_basis"][basis]
                        ]
                        / source_value
                    )
                    share = np.nan
                native_energy_mj = energy * conversions["energy"]["wh_to_mj"]
                model_energy_mj, energy_basis = (
                    native_energy_mj,
                    "ATB_equivalent_energy",
                )
                source_share = share
                if is_phev:
                    fuel = rules["atb"]["equivalent_gallon_hhv_fuel"][basis]
                    fuel_mj = (
                        item[
                            "utility_weighted_fuel_consumption_gallon_equivalent_per_mi"
                        ]
                        * conversions["fuel_hhv"][fuel + "_mj_per_litre"]
                        * conversions["volume"]["us_gallon_to_litre"]
                    )
                    electric_mj = (
                        item["utility_weighted_electricity_consumption_wh_per_mi"]
                        * conversions["energy"]["wh_to_mj"]
                    )
                    if any(not isfinite(v) or v < 0 for v in (fuel_mj, electric_mj)):
                        raise ValueError(
                            "Invalid utility-weighted PHEV component energy"
                        )
                    model_energy_mj = fuel_mj + electric_mj
                    share = electric_mj / positive(
                        model_energy_mj, "PHEV HHV total energy"
                    )
                    energy_basis = "GREET_fuel_HHV_plus_electricity"
                elif powertrain in rules["atb"]["fuel_equivalent_hhv_powertrains"]:
                    fuel = rules["atb"]["equivalent_gallon_hhv_fuel"][basis]
                    model_energy_mj = (
                        conversions["fuel_hhv"][fuel + "_mj_per_litre"]
                        * conversions["volume"]["us_gallon_to_litre"]
                        / source_value
                    )
                    energy_basis = "GREET_" + fuel + "_HHV_equivalent"
                records.append(
                    {
                        "family": family,
                        "powertrain": powertrain,
                        "year": item["year"],
                        "trajectory": trajectory,
                        "vehicle_class": item["vehicle_class"],
                        "vehicle_detail": item["vehicle_detail"],
                        "basis": basis,
                        "energy_basis": energy_basis,
                        "source_value": source_value,
                        "source_unit": source_unit,
                        "native_energy_mj_per_mi": native_energy_mj,
                        "model_total_energy_mj_per_mi": model_energy_mj,
                        "source_fuel_consumption_gallon_equivalent_per_mi": item.get(
                            "utility_weighted_fuel_consumption_gallon_equivalent_per_mi"
                        ),
                        "source_electricity_consumption_wh_per_mi": item.get(
                            "utility_weighted_electricity_consumption_wh_per_mi"
                        ),
                        "source_fleet_utility_factor": item.get("fleet_utility_factor"),
                        "consumption_mj_per_vkm": model_energy_mj
                        / conversions["length"]["mile_to_km"],
                        "source_electricity_input_share": source_share,
                        "electricity_input_share": share,
                        "source_reference": item.get(
                            "vehicle_input_reference", item.get("reference")
                        ),
                        "proxy": spec.get("proxy", ""),
                        "source_member": item.get(
                            "source_vehicle_input_member", item.get("source_member")
                        ),
                    }
                )
    return pd.DataFrame(records).sort_values(
        ["family", "powertrain", "vehicle_class", "year"]
    )


def aggregate_atb(
    atb: pd.DataFrame, class_weights: dict[str, float], *, tolerance: float
) -> pd.DataFrame:
    if (
        not class_weights
        or any(not isfinite(w) or w < 0 for w in class_weights.values())
        or abs(sum(class_weights.values()) - 1) > tolerance
    ):
        raise ValueError("Invalid ATB class weights")
    selected = atb.loc[atb.vehicle_class.isin(class_weights)].copy()
    selected["weight"] = selected.vehicle_class.map(class_weights)
    for key, group in selected.groupby(["powertrain", "year"]):
        if (
            set(group.vehicle_class) != set(class_weights)
            or group.vehicle_class.duplicated().any()
        ):
            raise ValueError(f"Incomplete ATB vocational coverage for {key}")
    selected["weighted_consumption"] = selected.consumption_mj_per_vkm * selected.weight
    return selected


class ProjectionAnchorUnavailable(ValueError):
    """No observed or approved-proxy rating support for a projected LDV class."""


class RoadEfficiencyEvidence:
    """Run-local numerical evidence, shared by existing and future vintages."""

    def __init__(
        self,
        *,
        ceud: pd.DataFrame,
        loads: pd.DataFrame,
        rating_audit: pd.DataFrame,
        atb_audit: pd.DataFrame,
        gcam: pd.DataFrame,
        regen: pd.DataFrame,
        base_year: int,
        rules: dict,
        projection_years: tuple[int, ...] = (),
        projection_reference_year: int | None = None,
        cohort_stock: pd.DataFrame | None = None,
        conversions: dict | None = None,
    ):
        self.ceud, self.loads, self.rules = ceud, loads, rules
        self.conversions = conversions
        self.base_year, self.gcam, self.regen = base_year, gcam, regen
        self.pev_projection_anchors = rules["pev_projection_anchors"]
        self.projection_years = tuple(projection_years)
        self.projection_reference_year = projection_reference_year
        self.cohort_stock = None if cohort_stock is None else cohort_stock.copy(deep=True)
        for key, expected in {
            "ldv_projection_baseline": "latest_observed_cohort",
            "ldv_projection_reference": "first_model_period",
            "ldv_future_load_factor": "anchor_cohort",
            "projection_missing_stock": "configured_analogue",
        }.items():
            if key in rules and rules[key] != expected:
                raise ValueError(f"Unsupported efficiency {key}: {rules[key]}")
        if rules["source_point_selection"] != "previous_observation_clamped":
            raise ValueError("Unsupported efficiency source-point selection")
        if set(self.pev_projection_anchors) != {"bev", "phev"} or any(
            anchor not in rules["ldv_archetypes"] or not anchor.startswith(family + "_")
            for family, anchor in self.pev_projection_anchors.items()
        ):
            raise ValueError("PEV projection anchors must name one configured range per family")
        if rules["historical_rating_observations"] != "through_base_year":
            raise ValueError("Unsupported historical ratings observation window")
        if (
            rules["historical_rating_baseline"]
            != "latest_own_observation_indexed_to_base_year"
        ):
            raise ValueError("Unsupported historical ratings baseline treatment")
        rated = rating_audit.loc[rating_audit.year.le(base_year)].copy()
        rated["weighted_native"] = rated.native_class_mean * rated.weight
        rated["weighted_mj"] = rated.class_mean_mj_per_vkm * rated.weight
        self.ratings = rated.groupby(
            ["ceud_class", "powertrain", "year"], as_index=False
        ).agg(
            native=("weighted_native", "sum"),
            mj=("weighted_mj", lambda s: s.sum(min_count=1)),
        )
        self.atb = atb_audit.groupby(
            ["region", "mode", "powertrain", "year"], as_index=False
        ).weighted_consumption.sum()
        self.findings: set[tuple[str, str, str, int, str]] = set()
        # Instance-local caches cannot retain evidence from unrelated preparation runs.
        self.atb_value = cache(self.atb_value)
        self.rating = cache(self.rating)
        self.projection_anchor = cache(self.projection_anchor)
        self.derive_consumption = cache(self.derive_consumption)

    def atb_value(self, region: str, mode: str, powertrain: str, year: int) -> float:
        series = self.atb.loc[
            self.atb.region.eq(region)
            & self.atb["mode"].eq(mode)
            & self.atb.powertrain.eq(powertrain)
        ]
        value = observed_value(series, year, "weighted_consumption")
        if value is None:
            raise ValueError(f"Missing exact ATB consumption: {region}/{mode}/{powertrain}/{year}")
        return value

    def rating(
        self, mode: str, powertrain: str, year: int, column: str
    ) -> float | None:
        weights = self.rules["road_classes"][mode]["weights"]
        series = self.ratings.loc[
            self.ratings.ceud_class.eq(weights) & self.ratings.powertrain.eq(powertrain)
        ]
        return observed_value(series, year, column)

    def period_observations(
        self, region: str, mode: str, powertrain: str, years: list[int], *,
        allow_analogue_when_empty: bool = False,
    ) -> dict[int, dict]:
        """Use observed years; a wholly empty period may use its reviewed proxy."""
        if not years or len(set(years)) != len(years):
            raise ValueError("A consumption period requires distinct source years")
        observed = {
            year: result for year in years
            if (result := self.derive_consumption(region, mode, powertrain, year)) is not None
        }
        if observed or not allow_analogue_when_empty:
            return observed
        return {
            year: result
            for year in years
            if (result := self.derive_consumption(region, mode, powertrain, year, allow_analogue=True)) is not None
        }

    def historical_rating_ratio(
        self, mode: str, powertrain: str, year: int, anchor: int, *, missing_ok: bool = False,
    ) -> tuple[float, str] | None:
        candidates, current = [], powertrain
        while current not in candidates:
            candidates.append(current)
            current = self.rules["ldv_archetypes"][current]["analogue"]
        incumbent = self.rules["ldv_incumbent_fallback"]
        if incumbent not in candidates:
            candidates.append(incumbent)
        for candidate in candidates:
            a, b = (
                self.rating(mode, candidate, year, "native"),
                self.rating(mode, candidate, anchor, "native"),
            )
            if a is not None and b is not None:
                return a / b, candidate
        if missing_ok:
            return None
        raise ValueError(
            f"No approved rating index: {mode}/{powertrain}/{year}/{anchor}"
        )

    def ldv_baseline(
        self, region: str, mode: str, powertrain: str, seen: tuple = ()
    ) -> tuple[float, str]:
        if powertrain in seen:
            raise ValueError(f"Cyclic/missing LDV rating analogue: {mode}/{powertrain}")
        value = self.rating(mode, powertrain, self.base_year, "mj")
        if value is not None:
            return value, powertrain
        observed = self.ratings.loc[
            self.ratings.ceud_class.eq(self.rules["road_classes"][mode]["weights"])
            & self.ratings.powertrain.eq(powertrain)
            & self.ratings.mj.notna()
        ]
        if not observed.empty:
            latest = observed.loc[observed.year.idxmax()]
            index, evidence = self.historical_rating_ratio(
                mode, powertrain, self.base_year, int(latest.year)
            )
            self.findings.add(
                (
                    region,
                    mode,
                    powertrain,
                    self.base_year,
                    f"latest_own_rating_year={int(latest.year)};base_year_index={evidence}",
                )
            )
            return float(latest.mj) * index, evidence
        analogue = self.rules["ldv_archetypes"][powertrain]["analogue"]
        if analogue in (*seen, powertrain):
            analogue = self.rules["ldv_incumbent_fallback"]
        base, evidence = self.ldv_baseline(region, mode, analogue, (*seen, powertrain))
        return base * self.atb_value(
            region, mode, powertrain, self.base_year
        ) / self.atb_value(region, mode, analogue, self.base_year), evidence

    def projection_anchor(self, region: str, mode: str, powertrain: str) -> dict:
        """Use observed annual consumption and raw cohort counts for projection.

        PEV ranges share only the future anchor; their observed histories remain
        independent. Missing annual ratings reduce coverage rather than becoming
        synthetic observations. A wholly empty bin uses the explicit analogue policy.
        """
        pathway = self.rules["road_classes"][mode]["pathway"]
        if pathway not in {"ldv", "mhdv"}:
            raise ValueError(f"Cohort projection anchors apply only to LDV and MD/HD trucks: {mode}")
        label = "LDV" if pathway == "ldv" else "MD/HD"
        years = self.projection_years
        if (not years or len(set(years)) != len(years)
                or any(not isinstance(year, (int, np.integer)) or isinstance(year, bool)
                       or year < 1 or year > self.base_year for year in years)):
            raise ValueError(f"{label} projection requires distinct observed cohort years through base_year")
        reference = self.projection_reference_year
        if (not isinstance(reference, (int, np.integer)) or isinstance(reference, bool)
                or reference <= self.base_year):
            raise ValueError(f"{label} projection requires its first model period after base_year")
        required = {"region", "road_class", "powertrain", "vintage_year", "raw_capacity"}
        if self.cohort_stock is None or not required.issubset(self.cohort_stock):
            raise ValueError(f"{label} projection requires registered raw cohort stock evidence")
        family = powertrain.split("_", 1)[0]
        anchor_powertrain = self.pev_projection_anchors.get(family, powertrain) if pathway == "ldv" else powertrain
        if anchor_powertrain not in self.rules[pathway + "_archetypes"]:
            raise ValueError(f"Unconfigured {label} projection powertrain: {anchor_powertrain}")
        observations = self.period_observations(
            region, mode, anchor_powertrain, sorted(years), allow_analogue_when_empty=(
                self.rules["missing_annual_rating"] == "observed_years_with_explicit_period_fallback"),
        )
        if not observations:
            raise ProjectionAnchorUnavailable(f"No observed or approved-proxy {label} projection anchor: {region}/{mode}/{anchor_powertrain}")

        region_map = self.rules.get("region_output_map", {})
        output_region = region_map.get(region, region)
        stock = self.cohort_stock.loc[
            self.cohort_stock.region.map(lambda value: region_map.get(value, value)).eq(output_region)
            & self.cohort_stock.road_class.eq(mode)
        ].copy()
        for column in ("vintage_year", "raw_capacity"):
            stock[column] = pd.to_numeric(stock[column], errors="raise")
            if not np.isfinite(stock[column]).all():
                raise ValueError(f"Non-finite {label} projection stock {column}: {region}/{mode}")
        if stock.vintage_year.mod(1).ne(0).any() or stock.raw_capacity.lt(0).any():
            raise ValueError(f"Invalid {label} projection cohort stock: {region}/{mode}")
        stock = stock.loc[stock.vintage_year.isin(years)]
        candidates, current = [], anchor_powertrain
        if pathway == "ldv":
            while current not in candidates:
                candidates.append(current)
                current = self.rules["ldv_archetypes"][current]["analogue"]
            incumbent = self.rules["ldv_incumbent_fallback"]
        else:
            candidates.append(current)
            incumbent = self.rules["mhdv_fleet_baseline"]["reference_fuel"]
        if incumbent not in candidates:
            candidates.append(incumbent)
        stock_fuels = list(dict.fromkeys(
            candidate.split("_", 1)[0] if candidate.startswith(("bev_", "phev_")) else candidate
            for candidate in candidates
        ))
        stock_powertrain, cohort_counts = None, {}
        for candidate in stock_fuels:
            counts = stock.loc[stock.powertrain.eq(candidate)].groupby("vintage_year").raw_capacity.sum()
            if float(counts.sum()) > 0:
                stock_powertrain = candidate
                cohort_counts = {int(year): float(counts.get(year, 0)) for year in sorted(years)}
                break
        if stock_powertrain is None:
            raise ValueError(f"No positive own or configured-analogue cohort stock: {region}/{mode}/{anchor_powertrain}")
        included = sorted(year for year in observations if cohort_counts[year] > 0)
        included_stock = sum(cohort_counts[year] for year in included)
        if included_stock <= 0:
            raise ValueError(f"No overlap between observed ratings and positive cohort stock: {region}/{mode}/{anchor_powertrain}")
        weights = {year: cohort_counts[year] / included_stock for year in included}
        total_stock = sum(cohort_counts.values())
        own_stock_powertrain = anchor_powertrain.split("_", 1)[0] if family in self.pev_projection_anchors else anchor_powertrain
        stock_proxy = stock_powertrain != own_stock_powertrain
        coverage = included_stock / total_stock
        if stock_proxy:
            self.findings.add((region, mode, anchor_powertrain, self.base_year,
                               f"projection_cohort_stock_analogue={stock_powertrain}"))
        if coverage < 1 - self.rules["tolerances"]["weight_sum"]:
            self.findings.add((region, mode, anchor_powertrain, self.base_year,
                               f"projection_cohort_stock_coverage={coverage:.12g}"))
        return {
            "powertrain": anchor_powertrain,
            "native_intensity": positive(sum(weights[year] * observations[year]["native_intensity"]
                                             for year in included), "projection anchor consumption"),
            "load_factor": positive(sum(weights[year] * observations[year]["load_factor"]
                                        for year in included), "projection anchor load factor"),
            "candidate_years": [int(year) for year in sorted(years)],
            "included_years": [int(year) for year in included],
            "weights": weights,
            "observations": {year: observations[year] for year in included},
            "cohort_stock": cohort_counts,
            "cohort_stock_coverage": coverage,
            "stock_powertrain": stock_powertrain,
            "stock_proxy": stock_proxy,
            "atb_reference_year": int(reference),
        }

    def derive_consumption(
        self, region: str, mode: str, powertrain: str, year: int, *,
        allow_analogue: bool = False,
    ) -> dict | None:
        """Keep energy in the numerator until the caller's final service conversion."""
        spec, base = self.rules["road_classes"][mode], self.base_year
        pathway = spec["pathway"]
        load = None if pathway in {"ldv", "mhdv"} and year > base else require_observed_value(
            self.loads.loc[self.loads.region.eq(region) & self.loads["mode"].eq(mode)],
            min(year, base),
            "load_factor",
        )
        native, native_unit, index, analogue = None, "MJ/vkm", 1.0, powertrain
        index_reference = powertrain
        source_years, projection_details = {}, {}
        if pathway == "ldv":
            if year <= base:
                observed = self.rating(mode, powertrain, year, "mj")
                if observed is not None:
                    consumption, treatment = observed, "NRCan_observed_class_weighted"
                else:
                    if not allow_analogue:
                        return None
                    proxy = self.historical_rating_ratio(
                        mode, powertrain, year, base, missing_ok=True,
                    )
                    if proxy is None:
                        return None
                    index, analogue = proxy
                    baseline, _ = self.ldv_baseline(region, mode, powertrain)
                    consumption = baseline * index
                    treatment = "NRCan_missing_year_approved_analogue_index"
            else:
                anchor = self.projection_anchor(region, mode, powertrain)
                analogue = index_reference = anchor["powertrain"]
                index = self.atb_value(region, mode, powertrain, year) / self.atb_value(
                    region, mode, anchor["powertrain"], anchor["atb_reference_year"],
                )
                consumption = anchor["native_intensity"] * index
                load = anchor["load_factor"]
                treatment = "NRCan_cohort_anchor_ATB_relative_projection"
                projection_details = {
                    "projection_anchor_powertrain": anchor["powertrain"],
                    "projection_source_years": anchor["included_years"],
                    "projection_reference_year": anchor["atb_reference_year"],
                    "projection_stock_powertrain": anchor["stock_powertrain"],
                    "projection_stock_coverage": anchor["cohort_stock_coverage"],
                    "projection_stock_proxy": anchor["stock_proxy"],
                }
            native = consumption
        elif pathway == "mhdv":
            truck = self.rules["mhdv_fleet_baseline"]
            for key, expected in {
                "emerging_ratio_year": "first_model_period",
                "future_baseline": "latest_observed_cohort",
                "future_load_factor": "anchor_cohort",
                "missing_stock": "reference_fuel_cohort",
            }.items():
                if truck[key] != expected:
                    raise ValueError(f"Unsupported MD/HD fleet baseline {key}: {truck[key]}")
            reference = self.projection_reference_year
            if year <= base:
                observed = powertrain in truck["observed_fuels"][mode]
                fuel = powertrain if observed else truck["reference_fuel"]
                series = ceud_series(
                    self.ceud.loc[self.ceud.region.eq(region)],
                    spec["stock_table"],
                    spec["cs_prefix"] + "|" + self.rules["fuel_series"][fuel],
                    "L/100 km",
                )
                if self.conversions is None:
                    raise ValueError("MD/HD fleet consumption requires configured fuel heating values")
                hhv = positive(self.conversions["fuel_hhv"][fuel + "_mj_per_litre"], "CEUD fuel HHV")
                consumption = require_observed_value(series, year, "value") * hhv * self.conversions["service"]["per_100_km_to_per_km"]
                if not observed:
                    if not isinstance(reference, (int, np.integer)) or isinstance(reference, bool) or reference <= base:
                        raise ValueError("MD/HD emerging history requires its first model period after base_year")
                    index = self.atb_value(region, mode, powertrain, reference) / self.atb_value(region, mode, fuel, reference)
                    consumption *= index
                    projection_details["projection_reference_year"] = int(reference)
                analogue = fuel
                projection_details["source_fuel"] = fuel
                treatment = "CEUD_observed_fleet_consumption" if observed else "CEUD_diesel_fleet_ATB_relative_powertrain"
            else:
                anchor = self.projection_anchor(region, mode, powertrain)
                index_powertrain = truck["projection_index_powertrains"].get(powertrain, powertrain)
                index = self.atb_value(region, mode, index_powertrain, year) / self.atb_value(region, mode, index_powertrain, anchor["atb_reference_year"])
                consumption = anchor["native_intensity"] * index
                load = anchor["load_factor"]
                analogue = index_reference = index_powertrain
                treatment = "CEUD_cohort_anchor_ATB_relative_projection"
                projection_details = {
                    "projection_anchor_powertrain": anchor["powertrain"],
                    "projection_source_years": anchor["included_years"],
                    "projection_reference_year": anchor["atb_reference_year"],
                    "projection_stock_powertrain": anchor["stock_powertrain"],
                    "projection_stock_coverage": anchor["cohort_stock_coverage"],
                    "projection_stock_proxy": anchor["stock_proxy"],
                    "projection_index_powertrain": index_powertrain,
                }
            native = consumption
        elif pathway in {"bus", "intercity"}:
            selected_mode = (
                self.rules["intercity"]["baseline_class"]
                if pathway == "intercity"
                else mode
            )
            source_year = max(base, year)
            if pathway == "intercity" and powertrain != "hev":
                keys = self.rules["intercity"]["source_keys"]
                reference = self.regen.loc[
                    self.regen.source_technology_key.eq(keys["diesel"])
                    & self.regen.metric.eq("efficiency")
                ]
                target = self.regen.loc[
                    self.regen.source_technology_key.eq(keys[powertrain])
                    & self.regen.metric.eq("efficiency")
                ]
                # Fuel economy is converted to consumption before the relative
                # comparison; gallon-equivalent cancels.
                reference = reference.assign(consumption=1 / reference.source_value)
                target = target.assign(consumption=1 / target.source_value)
                target_consumption, target_year = source_point(
                    target, source_year, "consumption", year_col="source_year"
                )
                reference_consumption, reference_year = source_point(
                    reference, source_year, "consumption", year_col="source_year"
                )
                index = target_consumption / reference_consumption
                source_years = {"relative_source_year": target_year,
                                "relative_reference_year": reference_year}
                consumption = (
                    self.atb_value(region, selected_mode, "diesel", source_year) * index
                )
            else:
                consumption = self.atb_value(
                    region, selected_mode, powertrain, source_year
                )
            if year <= base:
                series = ceud_series(
                    self.ceud.loc[self.ceud.region.eq(region)],
                    spec["intensity_table"],
                    spec["intensity"],
                    "MJ/Pkm",
                )
                historic, reference = (
                    require_observed_value(series, year, "value"),
                    require_observed_value(series, base, "value"),
                )
                base_load = require_observed_value(
                    self.loads.loc[
                        self.loads.region.eq(region) & self.loads["mode"].eq(mode)
                    ],
                    base,
                    "load_factor",
                )
                index = historic / reference
                consumption *= index * load / base_load
            native, treatment = (
                consumption,
                "ATB_bus_CEUD_service_intensity_backcast"
                if pathway == "bus"
                else "ATB_transit_REGEN_intercity_relative",
            )
        elif pathway == "motorcycle":
            series = ceud_series(
                self.ceud.loc[self.ceud.region.eq(region)],
                spec["intensity_table"],
                spec["intensity"],
                "MJ/Pkm",
            )
            intensity = require_observed_value(series, min(year, base), "value")
            if year > base:
                source = self.gcam.loc[self.gcam.source_variable.eq("intensity")]
                if set(source.source_unit) != {"MJ/vkm"}:
                    raise ValueError("Unexpected GCAM motorcycle units")
                incumbent = source.loc[
                    source.source_technology.eq(
                        self.rules["motorcycle"][self.rules["motorcycle"]["gasoline_fuel"]]
                    )
                ]
                target = source.loc[
                    source.source_technology.eq(self.rules["motorcycle"][powertrain])
                ]
                target_consumption, target_year = source_point(
                    target, year, "source_value", year_col="source_year"
                )
                reference_consumption, reference_year = source_point(
                    incumbent, base, "source_value", year_col="source_year"
                )
                index = target_consumption / reference_consumption
                source_years = {"index_source_year": target_year, "index_reference_year": reference_year}
            native = intensity * index * load
            treatment = "CEUD_service_intensity_GCAM_vehicle_intensity_index"
        else:
            raise ValueError(f"Unsupported road pathway: {pathway}")
        if analogue != index_reference:
            self.findings.add(
                (
                    region,
                    mode,
                    powertrain,
                    year,
                    f"rating_or_historical_index_analogue={analogue}",
                )
            )
        return {
            "load_factor": load,
            "native_intensity": positive(native, "energy consumption"),
            "native_unit": native_unit,
            "consumption_index": index,
            "analogue": analogue,
            "treatment": treatment,
            **projection_details,
            **({"source_point_selection": self.rules["source_point_selection"], **source_years}
               if source_years else {}),
        }


def prepare_bus_annual_efficiency_evidence(
    bundle: ConfigBundle, provincial: pd.DataFrame
) -> tuple[pd.DataFrame, tuple[Path, ...]]:
    """Derive annual bus efficiencies from the same source-backed road calculation.

    Capacity preparation needs annual values before the capacity-gated Efficiency
    rows can be built. This entrypoint uses the existing numerical evidence class
    without invoking the full efficiency builder or reading its generated CSV.
    """
    rules = load_harmonization_rules(bundle, "efficiencies")
    conversions = load_conversion_factors(bundle)
    demand = load_harmonization_rules(bundle, "road_stocks_and_demands")["demand"]
    loads = derive_load_factors(
        provincial,
        rules=rules,
        activity_rules=demand["activity_series"],
        conversions=conversions,
    )
    atb_rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    atb_dir = resolve_input_path(bundle, "interim", atb_rules["interim_subdir"])
    vehicle_path = atb_dir / atb_rules["components"]["vehicles"]["output_file"]
    phev_path = atb_dir / atb_rules["components"]["phev_efficiency"]["output_file"]
    vehicles, phev = pd.read_csv(vehicle_path), pd.read_csv(phev_path)
    if (
        set(vehicles["source_id"]) != {"nlr_atb_transportation"}
        or set(phev["source_id"]) != {"nlr_atb_transportation"}
    ):
        raise ValueError("Bus ATB evidence has an unexpected source identity")
    aliases = atb_rules["components"]["phev_efficiency"]["reconciliation"][
        "scenario_aliases"
    ]
    phev["trajectory"] = phev["trajectory"].replace(aliases)
    atb = select_atb_consumption(
        vehicles,
        phev,
        trajectory=configured_trajectory(bundle),
        rules=rules,
        conversions=conversions,
    )
    bus_specs = {
        mode: spec
        for mode, spec in rules["road_classes"].items()
        if spec["pathway"] in {"bus", "intercity"}
    }
    if set(bus_specs) != {"school_buses", "urban_transit", "inter_city_buses"}:
        raise ValueError("Bus efficiency classes differ from the supported CEUD classes")
    parts = []
    for region in bundle.scenario.geography.regions:
        for mode, spec in bus_specs.items():
            classes = spec["atb_classes"]
            weights = {name: 1 / len(classes) for name in classes}
            part = aggregate_atb(
                atb.loc[atb["family"].eq("mhdv")],
                weights,
                tolerance=rules["tolerances"]["weight_sum"],
            )
            part["region"], part["mode"] = region, mode
            parts.append(part)
    assorted = load_harmonization_rules(bundle, "assorted_sources")
    regen_path = resolve_input_path(
        bundle,
        "interim",
        assorted["interim_subdir"],
        assorted["epri_us_regen"]["output_file"],
    )
    regen = pd.read_csv(regen_path)
    if (
        set(regen["source_id"]) != {assorted["epri_us_regen"]["source_id"]}
        or set(regen.loc[regen["metric"].eq("efficiency"), "native_unit"])
        != {"mpg-e"}
    ):
        raise ValueError("Bus REGEN efficiency evidence has invalid source or units")
    empty_ratings = pd.DataFrame(
        columns=[
            "year", "native_class_mean", "weight", "class_mean_mj_per_vkm",
            "ceud_class", "powertrain",
        ]
    )
    evidence = RoadEfficiencyEvidence(
        ceud=provincial,
        loads=loads,
        rating_audit=empty_ratings,
        atb_audit=pd.concat(parts, ignore_index=True),
        gcam=pd.DataFrame(),
        regen=regen,
        base_year=bundle.scenario.periods.base_year,
        rules=rules,
    )
    technology = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    owners = technology.loc[
        technology["category"].isin(bus_specs)
        & technology["tech"].str.endswith(rules["existing_suffix"])
    ]
    if owners.empty or owners["tech"].duplicated().any():
        raise ValueError("Missing or duplicate existing bus technology owners")
    years = range(
        int(provincial["year"].min()), bundle.scenario.periods.base_year + 1
    )
    records = []
    for region in bundle.scenario.geography.regions:
        for owner in owners.itertuples(index=False):
            for year in years:
                consumption = evidence.derive_consumption(
                    region, owner.category, owner.sub_category, year
                )
                value = consumption["load_factor"] / consumption["native_intensity"]
                if not np.isfinite(value) or value <= 0:
                    raise ValueError(
                        f"Invalid annual bus efficiency: {(region, owner.tech, year)}"
                    )
                records.append(
                    {
                        "ceud_region": region,
                        "region": rules["region_output_map"].get(region, region),
                        "road_class": owner.category,
                        "tech": owner.tech,
                        "powertrain": owner.sub_category,
                        "year": year,
                        "efficiency": float(value),
                        "units": rules["service_units"]["passenger"],
                    }
                )
    result = pd.DataFrame(records)
    if result.duplicated(["region", "tech", "year"]).any():
        raise ValueError("Duplicate annual bus efficiency owner/year")
    return result, (vehicle_path, phev_path, regen_path)
