"""Derive Ontario LDV existing-stock age cohorts from mapped Report A stock."""

import argparse
import logging
from math import isfinite
from pathlib import Path
from typing import Any
from collections.abc import Mapping

import pandas as pd

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    write_dataframe_atomic,
)


ROAD_RULE_KEY = "road_aggregation"
STOCK_RULE_KEY = "road_stocks_and_demands"
LIFETIME_RULE_KEY = "road_lifetimes_survival"


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load Ontario stock-and-demand rules."""
    return load_harmonization_rules(bundle, STOCK_RULE_KEY)


def reconcile_road_capacity_to_activity(
    manual: pd.DataFrame,
    technology: pd.DataFrame,
    *,
    road_classes: set[str],
    activity_units: Mapping[str, str],
) -> pd.DataFrame:
    """Expand reviewed road category values through template-owned technologies."""
    required_manual = {"category", "sub_category", "units", "c2a", "notes"}
    required_technology = {"tech", "category"}
    if required_manual - set(manual) or required_technology - set(technology):
        raise ValueError("Capacity-to-activity input or technology template lacks columns")
    selected = manual.loc[manual["category"].isin(road_classes)].copy()
    if (
        set(selected["category"]) != road_classes
        or selected.duplicated("category").any()
        or not selected["sub_category"].eq("all").all()
        or selected[["category", "units", "notes"]].isna().any().any()
        or selected[["category", "units", "notes"]].astype(str).map(
            lambda value: not value.strip()
        ).any().any()
    ):
        raise ValueError("Road capacity-to-activity categories are incomplete or ambiguous")
    selected["c2a"] = pd.to_numeric(selected["c2a"], errors="raise")
    if selected["c2a"].map(lambda value: not isfinite(value) or value <= 0).any():
        raise ValueError("Road capacity-to-activity values must be finite and positive")
    for row in selected.itertuples(index=False):
        expected_unit = str(activity_units[row.category]) + "/k units"
        if row.units != expected_unit:
            raise ValueError(f"Capacity-to-activity unit mismatch for {row.category}")
    owners = technology.loc[technology["category"].isin(road_classes), ["tech", "category"]]
    if (
        owners["tech"].isna().any()
        or owners["tech"].duplicated().any()
        or set(owners["category"]) != road_classes
    ):
        raise ValueError("Technology template does not uniquely cover road classes")
    expanded = owners.merge(selected, on="category", validate="many_to_one")
    return expanded.sort_values("tech", kind="stable").reset_index(drop=True)


def derive_road_annual_utilization(
    provincial: pd.DataFrame,
    *,
    regions: list[str],
    years: list[int],
    activity_series: Mapping[str, Mapping[str, Any]],
    stock_series: Mapping[str, Mapping[str, Any]],
    capacity_by_class: Mapping[str, float],
    region_output_map: Mapping[str, str],
    activity_to_billion_factor: float,
) -> pd.DataFrame:
    """Calculate CEUD annual activity/stock/capacity ratios at road-class grain."""
    if (
        not years
        or len(set(years)) != len(years)
        or set(activity_series) != set(stock_series)
        or set(activity_series) != set(capacity_by_class)
        or not isfinite(activity_to_billion_factor)
        or activity_to_billion_factor <= 0
    ):
        raise ValueError("Invalid CEUD road utilization selectors or years")
    required = {"region", "year", "table_id", "raw_series", "unit", "value"}
    if required - set(provincial):
        raise ValueError("CEUD road utilization input lacks columns")
    records: list[dict[str, Any]] = []
    for region in regions:
        for road_class, activity_selector in activity_series.items():
            stock_selector = stock_series[road_class]
            c2a = float(capacity_by_class[road_class])
            if not isfinite(c2a) or c2a <= 0:
                raise ValueError(f"Invalid capacity-to-activity for {road_class}")
            for year in years:
                local = provincial.loc[
                    provincial["region"].eq(region) & provincial["year"].eq(year)
                ]
                activity = local.loc[
                    local["table_id"].eq(int(activity_selector["table_id"]))
                    & local["raw_series"].eq(activity_selector["raw_series"])
                ]
                stock = local.loc[
                    local["table_id"].eq(int(stock_selector["table_id"]))
                    & local["raw_series"].eq(stock_selector["raw_series"])
                ]
                if len(activity) != 1 or len(stock) != 1:
                    raise ValueError(f"Missing or duplicate CEUD activity/stock: {(region, road_class, year)}")
                if activity.iloc[0]["unit"] != "millions" or stock.iloc[0]["unit"] != "thousands":
                    raise ValueError(f"Unexpected CEUD activity/stock unit: {(region, road_class, year)}")
                activity_bn = float(activity.iloc[0]["value"]) * activity_to_billion_factor
                stock_k = float(stock.iloc[0]["value"])
                if not isfinite(activity_bn) or activity_bn < 0 or not isfinite(stock_k) or stock_k <= 0:
                    raise ValueError(f"Invalid CEUD activity/stock: {(region, road_class, year)}")
                records.append({
                    "ceud_region": region,
                    "region": region_output_map.get(region, region),
                    "road_class": road_class,
                    "year": year,
                    "activity_bn": activity_bn,
                    "stock_k_vehicles": stock_k,
                    "c2a": c2a,
                    "utilization": activity_bn / (stock_k * c2a),
                    "unit": "dimensionless",
                })
    return pd.DataFrame(records).sort_values(
        ["region", "road_class", "year"], kind="stable"
    ).reset_index(drop=True)


def aggregate_normalized_mileage_profiles(
    mileage: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    mile_to_km: float,
) -> pd.DataFrame:
    """Apply an explicitly selected road weight slice to complete ATB age profiles."""
    mileage_columns = {"vehicle_class", "year_index", "vmt_mi", "unit"}
    weight_columns = {"nrcan_ceud_class", "nlr_atb_class", "aggregation_weight"}
    if mileage_columns - set(mileage) or weight_columns - set(weights):
        raise ValueError("Mileage or road aggregation input lacks columns")
    if not isfinite(mile_to_km) or mile_to_km <= 0:
        raise ValueError("Invalid configured mile-to-kilometre conversion")
    if weights.empty or weights.duplicated(["nrcan_ceud_class", "nlr_atb_class"]).any():
        raise ValueError("Road aggregation weights are missing or duplicated")
    selected = mileage.loc[mileage["vehicle_class"].isin(weights["nlr_atb_class"])].copy()
    if (
        set(selected["vehicle_class"]) != set(weights["nlr_atb_class"])
        or selected.duplicated(["vehicle_class", "year_index"]).any()
        or set(selected["unit"]) != {"mi/vehicle-year"}
    ):
        raise ValueError("ATB mileage profiles do not uniquely cover weighted classes")
    numeric_ages = pd.to_numeric(selected["year_index"], errors="raise")
    if numeric_ages.isna().any() or numeric_ages.mod(1).ne(0).any():
        raise ValueError("ATB mileage ages must be integers")
    selected["year_index"] = numeric_ages.astype(int)
    selected["vmt_mi"] = pd.to_numeric(selected["vmt_mi"], errors="raise")
    if (selected["year_index"] < 0).any() or selected["vmt_mi"].map(
        lambda value: not isfinite(value) or value < 0
    ).any():
        raise ValueError("ATB mileage ages or values are invalid")
    age_sets = [set(group["year_index"]) for _, group in selected.groupby("vehicle_class")]
    if any(ages != age_sets[0] for ages in age_sets[1:]):
        raise ValueError("ATB classes have unequal age coverage; a reviewed treatment is required")
    expected_ages = set(range(max(age_sets[0]) + 1))
    if age_sets[0] != expected_ages:
        raise ValueError("ATB mileage profile ages are not contiguous from zero")
    weighted = weights.copy()
    weighted["aggregation_weight"] = pd.to_numeric(
        weighted["aggregation_weight"], errors="raise"
    )
    if weighted["aggregation_weight"].map(
        lambda value: not isfinite(value) or value <= 0
    ).any():
        raise ValueError("Road aggregation weights must be finite and positive")
    sums = weighted.groupby("nrcan_ceud_class")["aggregation_weight"].sum()
    if not sums.map(lambda value: abs(value - 1.0) < 1e-8).all():
        raise ValueError("Road aggregation weights do not sum to one by CEUD class")
    joined = weighted.merge(
        selected, left_on="nlr_atb_class", right_on="vehicle_class", validate="one_to_many"
    )
    joined["weighted_vmt_km"] = (
        joined["aggregation_weight"] * joined["vmt_mi"] * mile_to_km
    )
    result = joined.groupby(
        ["nrcan_ceud_class", "year_index"], as_index=False
    )["weighted_vmt_km"].sum()
    maxima = result.groupby("nrcan_ceud_class")["weighted_vmt_km"].transform("max")
    if maxima.le(0).any():
        raise ValueError("ATB mileage profile has no positive maximum")
    result["normalized_utilization"] = result["weighted_vmt_km"] / maxima
    if not result.groupby("nrcan_ceud_class")["normalized_utilization"].max().eq(1.0).all():
        raise ValueError("Normalized ATB mileage profile does not peak at one")
    return result.rename(columns={"year_index": "age"}).sort_values(
        ["nrcan_ceud_class", "age"], kind="stable"
    ).reset_index(drop=True)


def flat_road_capacity_factor_records(
    baseline: pd.DataFrame,
    technology: pd.DataFrame,
    *,
    periods: list[int],
    commodity_by_class: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Expand a validated class baseline to period-indexed v4 row payloads."""
    if not periods or len(periods) != len(set(periods)):
        raise ValueError("Flat utilization periods must be unique and nonempty")
    if {"region", "road_class", "utilization"} - set(baseline):
        raise ValueError("Flat utilization baseline lacks columns")
    if {"tech", "category"} - set(technology):
        raise ValueError("Flat utilization technology owners lack columns")
    if baseline.empty or baseline.duplicated(["region", "road_class"]).any():
        raise ValueError("Flat utilization baseline is empty or duplicated")
    if set(baseline["road_class"]) != set(commodity_by_class):
        raise ValueError("Flat utilization classes differ from demand commodities")
    if any(
        set(group["road_class"]) != set(commodity_by_class)
        for _, group in baseline.groupby("region")
    ):
        raise ValueError("Flat utilization class coverage differs by region")
    if set(technology["category"]) != set(commodity_by_class) or technology["tech"].duplicated().any():
        raise ValueError("Flat utilization technology ownership is incomplete")
    if baseline["utilization"].map(
        lambda value: not isfinite(value) or value < 0 or value > 1
    ).any():
        raise ValueError("Flat utilization exceeds the schema's unit interval")
    expanded = baseline.merge(
        technology[["tech", "category"]], left_on="road_class", right_on="category",
        validate="many_to_many",
    )
    return [
        {
            "region": row.region,
            "tech_or_group": row.tech,
            "vintage": period,
            "output_comm": commodity_by_class[row.road_class],
            "operator": "e",
            "factor": float(row.utilization),
            "notes": f"CEUD road {row.road_class} annual utilization; flat trajectory",
        }
        for row in expanded.sort_values(
            ["region", "road_class", "tech"], kind="stable"
        ).itertuples(index=False)
        for period in sorted(periods)
    ]


def mean_age_utilization_for_period(
    profile: pd.DataFrame,
    *,
    baseline_utilization: float,
    vintage: int,
    period: int,
    step: int,
) -> float:
    """Average each annual age-scaled value across a start-labeled model period."""
    if (
        step <= 0
        or vintage > period
        or not isfinite(baseline_utilization)
        or not 0 <= baseline_utilization <= 1
    ):
        raise ValueError("Invalid vintage, period, or baseline utilization")
    if {"age", "normalized_utilization"} - set(profile):
        raise ValueError("Normalized age profile lacks columns")
    if profile["age"].duplicated().any():
        raise ValueError("Normalized age profile has duplicate ages")
    ages = list(range(period + 1 - vintage, period + step + 1 - vintage))
    selected = profile.set_index("age").reindex(ages)
    if selected["normalized_utilization"].isna().any():
        raise ValueError(
            f"Age profile does not cover vintage {vintage}, period {period}, ages {ages}"
        )
    values = pd.to_numeric(selected["normalized_utilization"], errors="raise")
    if values.map(lambda value: not isfinite(value) or not 0 <= value <= 1).any():
        raise ValueError("Normalized age profile contains invalid utilization")
    return float(baseline_utilization * values.mean())


def median_lifetime_map(medians: pd.DataFrame) -> dict[str, float]:
    """Return unique CEUD-class median-equivalent ages."""
    selected = medians.loc[medians["target_system"].eq("nrcan_ceud")].copy()
    if selected.empty:
        raise ValueError("No NRCan CEUD median-equivalent lifetimes are available")
    duplicates = selected.duplicated("target_class", keep=False)
    if duplicates.any():
        raise ValueError("Duplicate NRCan CEUD median-equivalent lifetime rows")
    if selected["median_equivalent_age"].isna().any():
        missing = selected.loc[
            selected["median_equivalent_age"].isna(),
            "target_class",
        ].tolist()
        raise ValueError(f"Missing median-equivalent ages for: {missing}")
    return {
        str(row.target_class): float(row.median_equivalent_age)
        for row in selected.itertuples(index=False)
    }


def fixed_existing_lifetimes(
    medians: pd.DataFrame,
    manual: pd.DataFrame,
    rules: Mapping[str, Any],
) -> dict[str, float]:
    """Resolve reviewed median and manual lifetimes for CEUD road classes."""
    sources = rules["fixed_lifetime_sources"]
    if set(sources) != set(rules["ceud_stock_series"]):
        raise ValueError("Fixed lifetime sources do not cover every road class")
    lifetimes: dict[str, float] = {}
    for road_class, selector in sources.items():
        kind = selector["kind"]
        if kind == "ceud_median":
            selected = medians.loc[
                medians["target_system"].eq("nrcan_ceud")
                & medians["target_class"].eq(selector["target_class"])
            ]
        elif kind == "source_median_equal":
            selected = medians.loc[
                medians["source_id"].eq(selector["source_id"])
                & medians["target_system"].eq("source_class")
                & medians["target_class"].isin(selector["source_classes"])
            ]
            if set(selected["target_class"]) != set(selector["source_classes"]):
                raise ValueError(f"Incomplete source median classes for {road_class}")
        elif kind == "manual":
            selected = manual.loc[
                manual["category"].eq(selector["category"])
                & manual["sub_category"].eq("all")
            ].rename(columns={"lifetime": "median_equivalent_age"})
        else:
            raise ValueError(f"Unknown fixed lifetime source for {road_class}: {kind}")
        values = pd.to_numeric(selected["median_equivalent_age"], errors="raise")
        if (
            selected.empty
            or (kind != "source_median_equal" and len(selected) != 1)
            or values.nunique() != 1
        ):
            raise ValueError(f"Ambiguous fixed lifetime for {road_class}")
        lifetime = float(values.iloc[0])
        if not isfinite(lifetime) or lifetime <= 0:
            raise ValueError(f"Invalid fixed lifetime for {road_class}")
        lifetimes[road_class] = lifetime
    return lifetimes


def accepted_curve_ages(
    curves: pd.DataFrame, rules: Mapping[str, Any]
) -> dict[str, set[int]]:
    """Return supported ages from the accepted CEUD road survival curves."""
    result: dict[str, set[int]] = {}
    for road_class in rules["survival_curve_classes"]:
        selector = rules["fixed_lifetime_sources"][road_class]
        target_class = selector.get("target_class", selector.get("curve_source_class"))
        selected = curves.loc[curves["source_class"].eq(target_class)]
        if selected.empty or selected["age"].duplicated().any():
            raise ValueError(f"Missing or duplicate accepted curve for {road_class}")
        result[road_class] = set(
            selected.loc[
                pd.to_numeric(selected["survival_probability"], errors="raise").gt(0),
                "age",
            ].astype(int)
        )
    return result


def derive_ldv_age_distributions(
    mapped_stock: pd.DataFrame,
    *,
    survival_curves: bool,
    maximum_age: int,
    median_lifetimes: dict[str, float],
    historical_review_minimum_model_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the scenario-owned cutoff and report all excluded stock."""
    required = {
        "report_year",
        "MODEL_YEAR",
        "FIT_ACTIVE",
        "nrcan_ceud_class",
        "nlr_atb_class",
        "mapping_accepted",
    }
    missing = sorted(required - set(mapped_stock.columns))
    if missing:
        raise ValueError("Mapped Ontario stock missing columns: " + ", ".join(missing))
    accepted = mapped_stock.loc[mapped_stock["mapping_accepted"]].copy()
    accepted["report_year"] = pd.to_numeric(
        accepted["report_year"],
        errors="raise",
    ).astype(int)
    accepted["MODEL_YEAR"] = pd.to_numeric(
        accepted["MODEL_YEAR"],
        errors="coerce",
    ).astype("Int64")
    accepted["FIT_ACTIVE"] = pd.to_numeric(
        accepted["FIT_ACTIVE"],
        errors="coerce",
    )
    accepted["age"] = accepted["report_year"] - accepted["MODEL_YEAR"]
    accepted["before_historical_review_year"] = accepted["MODEL_YEAR"].lt(
        historical_review_minimum_model_year
    )
    accepted["negative_age"] = accepted["age"].lt(0)
    if survival_curves:
        accepted["cutoff_age"] = float(maximum_age)
        accepted["cutoff_basis"] = "scenario_survival_curve_max_age"
    else:
        accepted["cutoff_age"] = accepted["nrcan_ceud_class"].map(median_lifetimes)
        missing_medians = sorted(
            accepted.loc[
                accepted["cutoff_age"].isna(),
                "nrcan_ceud_class",
            ]
            .dropna()
            .astype(str)
            .unique()
        )
        if missing_medians:
            raise ValueError(
                "Missing median-equivalent lifetime for mapped classes: "
                + ", ".join(missing_medians)
            )
        accepted["cutoff_basis"] = "source_derived_median_equivalent_age"
    accepted["over_cutoff"] = accepted["age"].gt(accepted["cutoff_age"])
    accepted["retained"] = (
        ~accepted["negative_age"] & ~accepted["over_cutoff"] & accepted["age"].notna()
    )

    findings: list[dict[str, Any]] = []
    for ceud_class, rows in accepted.groupby("nrcan_ceud_class", dropna=False):
        retained = rows.loc[rows["retained"]]
        findings.append(
            {
                "report_year": int(rows["report_year"].max()),
                "nrcan_ceud_class": ceud_class,
                "cutoff_basis": rows["cutoff_basis"].iloc[0],
                "cutoff_age": rows["cutoff_age"].iloc[0],
                "source_rows": len(rows),
                "source_fit_active_stock": rows["FIT_ACTIVE"].sum(),
                "negative_age_rows": int(rows["negative_age"].sum()),
                "negative_age_fit_active_stock": rows.loc[
                    rows["negative_age"],
                    "FIT_ACTIVE",
                ].sum(),
                "historical_review_minimum_model_year": (
                    historical_review_minimum_model_year
                ),
                "before_historical_review_year_rows": int(
                    rows["before_historical_review_year"].sum()
                ),
                "before_historical_review_year_fit_active_stock": rows.loc[
                    rows["before_historical_review_year"],
                    "FIT_ACTIVE",
                ].sum(),
                "over_cutoff_rows": int(rows["over_cutoff"].sum()),
                "over_cutoff_fit_active_stock": rows.loc[
                    rows["over_cutoff"],
                    "FIT_ACTIVE",
                ].sum(),
                "retained_rows": len(retained),
                "retained_fit_active_stock": retained["FIT_ACTIVE"].sum(),
            }
        )

    retained = accepted.loc[accepted["retained"]].copy()
    age_distribution = (
        retained.groupby(
            [
                "report_year",
                "nrcan_ceud_class",
                "nlr_atb_class",
                "MODEL_YEAR",
                "age",
                "cutoff_basis",
                "cutoff_age",
            ],
            as_index=False,
            dropna=False,
        )["FIT_ACTIVE"]
        .sum(min_count=1)
        .rename(
            columns={
                "MODEL_YEAR": "model_year",
                "FIT_ACTIVE": "fit_active_stock",
            }
        )
    )
    totals = age_distribution.groupby(
        ["report_year", "nrcan_ceud_class", "nlr_atb_class"]
    )["fit_active_stock"].transform("sum")
    age_distribution["age_distribution"] = age_distribution["fit_active_stock"] / totals
    return (
        age_distribution.sort_values(
            ["report_year", "nrcan_ceud_class", "nlr_atb_class", "age"],
            kind="stable",
        ).reset_index(drop=True),
        pd.DataFrame(findings),
    )


def build_existing_stock_age_artifacts(
    scenario_path: str | Path | ConfigBundle,
) -> Path:
    """Publish scenario-dependent Ontario LDV existing-stock age cohorts."""
    bundle = (
        scenario_path
        if isinstance(scenario_path, ConfigBundle)
        else load_config_bundle(scenario_path)
    )
    rules = module_rules(bundle)["ontario_report_a"]
    road_rules = load_harmonization_rules(bundle, ROAD_RULE_KEY)
    lifetime_rules = load_harmonization_rules(bundle, LIFETIME_RULE_KEY)
    output_dir = resolve_artifact_path(bundle, "road_stocks_and_demands")
    mapped = pd.read_csv(
        resolve_artifact_path(bundle, "road_aggregation")
        / road_rules["mapped_current_stock_file"],
        low_memory=False,
    )
    medians = pd.read_csv(
        resolve_artifact_path(bundle, "road_lifetimes_survival")
        / lifetime_rules["median_lifetimes_file"]
    )
    age_distribution, findings = derive_ldv_age_distributions(
        mapped,
        survival_curves=bundle.scenario.switches.survival_curves,
        maximum_age=bundle.scenario.switches.survival_curve_max_age,
        median_lifetimes=median_lifetime_map(medians),
        historical_review_minimum_model_year=int(
            rules["historical_review_minimum_model_year"]
        ),
    )
    write_dataframe_atomic(
        age_distribution,
        output_dir / str(rules["age_distribution_file"]),
    )
    write_dataframe_atomic(
        findings,
        resolve_artifact_path(bundle, "lifetime_validation")
        / str(rules["truncation_findings_file"]),
    )
    return output_dir


def distribute_existing_road_capacity(
    *,
    provincial: pd.DataFrame,
    ldv_age: pd.DataFrame,
    report5_age: pd.DataFrame,
    ldv_registrations: pd.DataFrame,
    truck_registrations: pd.DataFrame,
    dashboard: pd.DataFrame,
    regions: list[str],
    base_year: int,
    first_model_period: int,
    survival_curves: bool,
    survival_curve_max_age: int,
    fixed_lifetimes_by_class: Mapping[str, float],
    curve_ages_by_class: Mapping[str, set[int]],
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply reviewed age and registration shares to CEUD base-year road stocks."""
    periods = [int(period) for period in rules["vintage_periods"]]
    if periods != sorted(set(periods)) or periods[-1] != base_year:
        raise ValueError("Road vintage periods must be unique and end at the base year")
    if (
        rules["first_model_period_rule"] != "positive_surviving_capacity"
        or rules["ineligible_cohort_redistribution"]
        != "proportional_to_eligible_capacity"
        or first_model_period <= base_year
    ):
        raise ValueError("Unsupported road first-model-period eligibility rule")
    if set(fixed_lifetimes_by_class) != set(rules["ceud_stock_series"]):
        raise ValueError("Missing fixed lifetime for one or more road classes")
    curve_classes = set(rules["survival_curve_classes"])
    if survival_curves and set(curve_ages_by_class) != curve_classes:
        raise ValueError("Accepted curves do not cover configured road classes")
    historical_fuels = set(rules["historical_fuel_types"])
    if historical_fuels != {"Gasoline", "Diesel"}:
        raise ValueError("Historical fuel rule must select gasoline and diesel")
    if ldv_age["report_year"].nunique() != 1 or int(
        ldv_age["report_year"].iloc[0]
    ) != int(rules["age_evidence_year"]):
        raise ValueError("Mapped LDV age evidence is not the configured MTO edition")
    ldv_age = ldv_age.copy()
    ldv_age["age"] = pd.to_numeric(ldv_age["age"], errors="raise").astype(int)
    age_map: dict[str, pd.DataFrame] = {}
    for name, ceud_class in (
        ("cars", "Car"),
        ("passenger_light_trucks", "Light Truck"),
        ("freight_light_trucks", "Light Truck"),
    ):
        selected = ldv_age.loc[ldv_age["nrcan_ceud_class"].eq(ceud_class)]
        if selected.empty:
            raise ValueError(f"No mapped MTO age evidence for {name}")
        by_age = selected.groupby("age", as_index=False)["fit_active_stock"].sum()
        age_map[name] = by_age.assign(
            age_share=by_age["fit_active_stock"] / by_age["fit_active_stock"].sum()
        )
    for name, source_class in rules["report5_age_classes"].items():
        selected = report5_age.loc[
            report5_age["VEHICLE_CLASS"].eq(source_class)
            & report5_age["year"].eq(int(rules["age_evidence_year"]))
        ]
        if selected.empty or selected["AGE"].duplicated().any():
            raise ValueError(f"Missing or duplicate Report 5 age rows for {name}")
        by_age = (
            selected[["AGE", "AGE_DIST"]]
            .rename(columns={"AGE": "age", "AGE_DIST": "age_share"})
            .copy()
        )
        by_age["age"] = pd.to_numeric(by_age["age"], errors="raise").astype(int)
        age_map[name] = by_age
    for name, ages in age_map.items():
        if (
            (ages["age"] < 0).any()
            or ages["age"].duplicated().any()
            or not abs(float(ages["age_share"].sum()) - 1) < 1e-8
        ):
            raise ValueError(f"Invalid MTO age distribution for {name}")

    stock_rows: list[dict[str, Any]] = []
    for region in regions:
        local = provincial.loc[
            provincial["region"].eq(region) & provincial["year"].eq(base_year)
        ]
        for name, selector in rules["ceud_stock_series"].items():
            source = local.loc[
                local["table_id"].eq(int(selector["table_id"]))
                & local["raw_series"].eq(selector["raw_series"])
            ]
            if len(source) != 1 or source.iloc[0]["unit"] != "thousands":
                raise ValueError(
                    f"Missing or wrong-unit CEUD road stock: {(region, name)}"
                )
            stock = float(source.iloc[0]["value"])
            if not isfinite(stock) or stock < 0:
                raise ValueError(f"Invalid CEUD road stock: {(region, name)}")
            eligible_age_rows: list[tuple[int, int, float, bool]] = []
            for age_row in age_map[name].itertuples(index=False):
                actual_year = base_year - int(age_row.age)
                year = max(periods[0], actual_year)
                if survival_curves and name in curve_classes:
                    first_period_age = first_model_period - actual_year
                    eligible = (
                        actual_year >= periods[0]
                        and first_period_age <= survival_curve_max_age
                        and first_period_age in curve_ages_by_class[name]
                    )
                else:
                    eligible = (
                        actual_year + float(fixed_lifetimes_by_class[name])
                        > first_model_period
                    )
                eligible_age_rows.append(
                    (int(age_row.age), year, float(age_row.age_share), eligible)
                )
            eligible_share = sum(
                share for _, _, share, eligible in eligible_age_rows if eligible
            )
            if stock > 0 and eligible_share <= 0:
                raise ValueError(f"No eligible MTO age cohorts for {(region, name)}")
            factor = 1.0 / eligible_share if eligible_share > 0 else 0.0
            for age, year, age_share, eligible in eligible_age_rows:
                vintage = next(period for period in periods if period >= year)
                stock_rows.append(
                    {
                        "ceud_region": region,
                        "region": rules["region_output_map"].get(region, region),
                        "road_class": name,
                        "age": age,
                        "vintage_year": year,
                        "vintage": vintage,
                        "age_share": age_share,
                        "eligible_first_model_period": eligible,
                        "redistribution_factor": factor if eligible else 0.0,
                        "stock_k_vehicles": stock,
                        "raw_cohort_k_vehicles": stock * age_share,
                        "cohort_k_vehicles": stock * age_share * factor
                        if eligible
                        else 0.0,
                        "age_source": "MTO Report A mapped"
                        if name in rules["statcan_ldv_source_types"]
                        else "MTO Report 5",
                    }
                )
    cohorts = pd.DataFrame(stock_rows)
    active_cohorts = cohorts.loc[cohorts["cohort_k_vehicles"].gt(0)]
    share_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    fuel_technologies = rules["fuel_technology"]
    share_proxy = rules["share_region_proxy"]
    dashboard_by_region = dashboard.set_index("source_region")["ytd_market_share"]

    for (region, name, vintage_year, vintage), group in active_cohorts.groupby(
        ["ceud_region", "road_class", "vintage_year", "vintage"], sort=True
    ):
        if name in rules["statcan_ldv_source_types"]:
            source_region = share_proxy.get(region, region)
        elif name in {"medium_trucks", "heavy_trucks"}:
            source_region = "BC" if region == "BCT" else region
        else:
            source_region = region
        source_year: int | None = None
        historical_backfill = False
        counts: dict[str, float]
        if name in rules["statcan_ldv_source_types"]:
            selected = ldv_registrations.loc[
                ldv_registrations["scenario_region"].eq(source_region)
                & ldv_registrations["vehicle_type"].isin(
                    rules["statcan_ldv_source_types"][name]
                )
            ]
        elif name == "medium_trucks":
            selected = truck_registrations.loc[
                truck_registrations["scenario_region"].eq(source_region)
                & truck_registrations["vehicle_type"].isin(
                    rules["statcan_medium_source_types"]
                )
            ].copy()
            selected["reference_year"] = selected["reference_period"].astype(int)
        elif name == "heavy_trucks":
            selected = truck_registrations.loc[
                truck_registrations["scenario_region"].eq(source_region)
                & truck_registrations["vehicle_type"].eq(
                    rules["statcan_heavy_source_type"]
                )
            ].copy()
            selected["reference_year"] = selected["reference_period"].astype(int)
        else:
            selected = pd.DataFrame()
        if name == "motorcycles":
            counts = {next(iter(fuel_technologies[name])): 1.0}
            source_table_ids = ""
        else:
            selected = selected.loc[selected["scaled_value"].notna()].copy()
            available = sorted(
                int(year)
                for year in selected["reference_year"].unique()
                if int(year) <= base_year
            )
            if not available:
                raise ValueError(
                    f"No fuel registrations for {(region, name)} via {source_region}"
                )
            historical_backfill = vintage_year < available[0]
            source_year = max(
                (year for year in available if year <= vintage_year),
                default=available[0],
            )
            selected = selected.loc[selected["reference_year"].eq(source_year)]
            source_table_ids = (
                "|".join(sorted(set(selected["source_table_id"])))
                if "source_table_id" in selected
                else "23-10-0308-01"
            )
            counts = selected.groupby("fuel_type")["scaled_value"].sum().to_dict()
            if not counts or any(
                not isfinite(float(value)) or float(value) < 0
                for value in counts.values()
            ):
                raise ValueError(
                    f"Invalid fuel registrations for {(region, name, source_year)}"
                )
        total = sum(counts.values())
        supported = fuel_technologies[name]
        eligible_fuels = set(supported)
        vintage_excluded_fuels = (
            set(rules.get("base_year_only_fuels", {}).get(name, []))
            if vintage != base_year else set()
        )
        eligible_fuels -= vintage_excluded_fuels
        if historical_backfill:
            eligible_fuels &= historical_fuels
        selected_total = sum(
            value for fuel, value in counts.items() if fuel in eligible_fuels
        )
        if total <= 0 or selected_total <= 0:
            raise ValueError(
                f"No supported fuel registrations for {(region, name, source_year)}"
            )
        for fuel, value in counts.items():
            if fuel not in eligible_fuels and value > 0:
                excluded_rows.append(
                    {
                        "ceud_region": region,
                        "road_class": name,
                        "vintage_year": vintage_year,
                        "source_region": source_region,
                        "source_year": source_year,
                        "fuel_type": fuel,
                        "excluded_source_share": value / total,
                        "exclusion_reason": (
                            "configured_base_year_only_fuel"
                            if fuel in vintage_excluded_fuels else
                            "pre_evidence_non_combustion"
                            if historical_backfill and fuel in supported else
                            "unsupported_existing_technology"
                        ),
                    }
                )
        shares = {
            fuel: value / selected_total
            for fuel, value in counts.items()
            if fuel in eligible_fuels
        }
        if name == "medium_trucks" and vintage == base_year:
            dashboard_label = (
                "British Columbia"
                if region == "BCT"
                else (
                    "Newfoundland and Labrador"
                    if region == "NL"
                    else ("Prince Edward Island" if region == "PE" else None)
                )
            )
            if dashboard_label is None:
                dashboard_label = {
                    "AB": "Alberta",
                    "MB": "Manitoba",
                    "NB": "New Brunswick",
                    "NS": "Nova Scotia",
                    "ON": "Ontario",
                    "QC": "Quebec",
                    "SK": "Saskatchewan",
                }[region]
            if dashboard_label not in dashboard_by_region or pd.isna(
                dashboard_by_region[dashboard_label]
            ):
                raise ValueError(f"No dashboard share for {region}")
            bev_share = float(dashboard_by_region[dashboard_label])
            combustion = sum(shares.get(fuel, 0.0) for fuel in ("Gasoline", "Diesel"))
            if combustion <= 0:
                raise ValueError(f"No combustion mix for dashboard override: {region}")
            shares = {
                fuel: (1 - bev_share) * shares.get(fuel, 0.0) / combustion
                for fuel in ("Gasoline", "Diesel")
            }
            shares["Battery electric"] = bev_share
        if not abs(sum(shares.values()) - 1) < 1e-9:
            raise ValueError(
                f"Fuel shares do not sum to one for {(region, name, vintage_year)}"
            )
        for fuel, share in shares.items():
            share_rows.append(
                {
                    "ceud_region": region,
                    "region": rules["region_output_map"].get(region, region),
                    "road_class": name,
                    "vintage_year": vintage_year,
                    "vintage": vintage,
                    "fuel_type": fuel,
                    "tech": supported[fuel],
                    "fuel_share": share,
                    "source_region": source_region,
                    "source_year": source_year,
                    "source_table_ids": source_table_ids,
                    "proxy_geography": source_region != region,
                    "dashboard_override": name == "medium_trucks"
                    and vintage == base_year,
                    "historical_combustion_backfill": historical_backfill,
                    "cohort_k_vehicles": group["cohort_k_vehicles"].sum(),
                    "capacity": group["cohort_k_vehicles"].sum() * share,
                }
            )
    shares_frame = pd.DataFrame(share_rows)
    capacity = shares_frame.groupby(
        ["region", "road_class", "tech", "vintage"], as_index=False
    )["capacity"].sum()
    capacity["units"] = "k vehicles"
    return cohorts, shares_frame, pd.DataFrame(excluded_rows), capacity


def distribute_existing_bus_capacity(
    *,
    provincial: pd.DataFrame,
    report5_age: pd.DataFrame,
    annual_efficiency: pd.DataFrame,
    lifetimes: Mapping[tuple[str, str], float],
    regions: list[str],
    base_year: int,
    first_model_period: int,
    road_rules: Mapping[str, Any],
    rules: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Allocate CEUD bus stocks with annual fuel activity and MTO age shares."""
    supported = {
        "activity_share_method": "annual_energy_times_efficiency",
        "pre_ceud_year_rule": "oldest_available_activity_share",
        "first_model_period_rule": "positive_surviving_capacity",
        "ineligible_cohort_redistribution": "proportional_to_eligible_capacity",
        "unallocatable_technology_rule": "redistribute_within_class_proportionally",
    }
    if any(rules[key] != value for key, value in supported.items()):
        raise ValueError("Unsupported bus capacity allocation policy")
    periods = [int(period) for period in road_rules["vintage_periods"]]
    if periods != sorted(set(periods)) or periods[-1] != base_year:
        raise ValueError("Bus vintage periods must end at the base year")
    age = report5_age.loc[report5_age["VEHICLE_CLASS"].eq(rules["age_class"])].copy()
    if (
        age.empty
        or set(age["year"]) != {int(road_rules["age_evidence_year"])}
        or age["AGE"].duplicated().any()
        or age["AGE_DIST"].isna().any()
        or (age["AGE_DIST"] < 0).any()
        or abs(float(age["AGE_DIST"].sum()) - 1) > 1e-8
    ):
        raise ValueError("Invalid selected MTO Report 5 BUS age distribution")
    if first_model_period <= base_year:
        raise ValueError("The first model period must follow bus base-year stock")
    source_to_powertrain = {
        member: powertrain
        for powertrain, members in rules["fuel_source_members"].items()
        for member in members
    }
    if len(source_to_powertrain) != sum(
        len(members) for members in rules["fuel_source_members"].values()
    ):
        raise ValueError("Bus CEUD fuel members map to multiple powertrains")
    excluded_members = set(rules["excluded_fuel_members"])
    if excluded_members & set(source_to_powertrain):
        raise ValueError("Bus fuel cannot be both included and excluded")
    required_members = set(rules["required_fuel_members"])
    if not required_members.issubset(source_to_powertrain):
        raise ValueError("Required bus fuel members lack powertrain mappings")
    first_year = periods[0]
    years = set(range(first_year, base_year + 1))
    stock_records = []
    fuel_records = []
    excluded_records = []
    expected_owners = set()
    for road_class, spec in rules["classes"].items():
        prefix = spec["energy_prefix"]
        owners = spec["fuel_technology"]
        expected_owners.update(owners.values())
        allowed_members = {
            member
            for powertrain in owners
            for member in rules["fuel_source_members"][powertrain]
        }
        required_for_class = {
            member for member in allowed_members if member in required_members
        }
        for region in regions:
            local = provincial.loc[provincial["region"].eq(region)]
            selected_stock = local.loc[
                local["year"].eq(base_year)
                & local["table_id"].eq(int(spec["stock"]["table_id"]))
                & local["raw_series"].eq(spec["stock"]["raw_series"])
            ]
            if len(selected_stock) != 1 or selected_stock.iloc[0]["unit"] != "thousands":
                raise ValueError(f"Missing CEUD bus stock for {(region, road_class)}")
            stock = float(selected_stock.iloc[0]["value"])
            if not isfinite(stock) or stock < 0:
                raise ValueError(f"Invalid CEUD bus stock for {(region, road_class)}")
            stock_records.append(
                {
                    "ceud_region": region,
                    "region": road_rules["region_output_map"].get(region, region),
                    "road_class": road_class,
                    "stock_k_vehicles": stock,
                }
            )
            source = local.loc[
                local["table_id"].eq(int(spec["energy_table"]))
                & local["raw_series"].str.startswith(prefix)
                & local["year"].isin(years)
            ].copy()
            source["fuel_member"] = source["raw_series"].str.removeprefix(prefix)
            if source.duplicated(["year", "fuel_member"]).any():
                raise ValueError(f"Duplicate CEUD bus fuel cells: {(region, road_class)}")
            if set(source["unit"]) != {"PJ"}:
                raise ValueError(f"Bus fuel units must be PJ: {(region, road_class)}")
            if source["value"].map(lambda value: not isfinite(value) or value < 0).any():
                raise ValueError(f"Invalid CEUD bus fuel use: {(region, road_class)}")
            for member in required_for_class:
                if set(source.loc[source["fuel_member"].eq(member), "year"]) != years:
                    raise ValueError(
                        f"Incomplete CEUD bus fuel history: {(region, road_class, member)}"
                    )
            unknown = source.loc[
                ~source["fuel_member"].isin(allowed_members | excluded_members)
                & source["value"].gt(0)
            ]
            if not unknown.empty:
                raise ValueError(
                    f"Unmapped positive CEUD bus fuel: {(region, road_class, sorted(unknown['fuel_member'].unique()))}"
                )
            total_energy = source.groupby("year")["value"].sum()
            for record in source.itertuples(index=False):
                member = record.fuel_member
                if member in allowed_members:
                    fuel_records.append(
                        {
                            "ceud_region": region,
                            "road_class": road_class,
                            "year": int(record.year),
                            "powertrain": source_to_powertrain[member],
                            "fuel_member": member,
                            "energy_pj": float(record.value),
                        }
                    )
                elif member in excluded_members and record.value > 0:
                    excluded_records.append(
                        {
                            "ceud_region": region,
                            "road_class": road_class,
                            "year": int(record.year),
                            "fuel_member": member,
                            "excluded_energy_pj": float(record.value),
                            "excluded_energy_share": float(record.value)
                            / float(total_energy.loc[record.year]),
                        }
                    )
    stocks = pd.DataFrame(stock_records)
    fuel = pd.DataFrame(fuel_records)
    fuel["is_blend"] = fuel["fuel_member"].isin(
        {member for members in rules["fuel_source_members"].values() for member in members[1:]}
    )
    fuel["blend_energy_pj"] = fuel["energy_pj"].where(fuel["is_blend"], 0.0)
    energy = fuel.groupby(
        ["ceud_region", "road_class", "year", "powertrain"], as_index=False
    )[["energy_pj", "blend_energy_pj"]].sum()
    efficiency = annual_efficiency.loc[
        annual_efficiency["road_class"].isin(rules["classes"])
    ].copy()
    if (
        set(efficiency["tech"]) != expected_owners
        or efficiency.duplicated(["ceud_region", "tech", "year"]).any()
        or set(efficiency["units"]) != {"bn passenger-km/PJ"}
    ):
        raise ValueError("Bus annual efficiency owner, key, or unit mismatch")
    expected_keys = {
        (region, tech, year)
        for region in regions
        for tech in expected_owners
        for year in years
    }
    if {
        (r.ceud_region, r.tech, r.year) for r in efficiency.itertuples(index=False)
    } != expected_keys:
        raise ValueError("Bus annual efficiency lacks regional or year coverage")
    shares = efficiency.merge(
        energy,
        on=["ceud_region", "road_class", "year", "powertrain"],
        how="left",
        validate="one_to_one",
    )
    shares[["energy_pj", "blend_energy_pj"]] = shares[
        ["energy_pj", "blend_energy_pj"]
    ].fillna(0.0)
    shares["activity_bn_passenger_km"] = shares["energy_pj"] * shares["efficiency"]
    group_keys = ["ceud_region", "road_class", "year"]
    totals = shares.groupby(group_keys)["activity_bn_passenger_km"].transform("sum")
    if totals.le(0).any():
        raise ValueError("Bus fuel activity has no supported powertrain in a CEUD year")
    shares["activity_share"] = shares["activity_bn_passenger_km"] / totals
    age = age[["AGE", "AGE_DIST"]].rename(
        columns={"AGE": "age", "AGE_DIST": "age_share"}
    )
    cohorts = stocks.merge(age, how="cross")
    cohorts["vintage_year"] = base_year - cohorts["age"].astype(int)
    cohorts["source_year"] = cohorts["vintage_year"].clip(lower=first_year)
    cohorts["vintage"] = cohorts["source_year"].map(
        lambda year: next(period for period in periods if period >= year)
    )
    cohorts = cohorts.merge(
        shares.rename(columns={"year": "source_year"}).drop(columns="region"),
        on=["ceud_region", "road_class", "source_year"],
        how="left",
        validate="many_to_many",
    )
    if cohorts["tech"].isna().any():
        raise ValueError("MTO bus cohort lacks a CEUD/efficiency activity share")
    life = pd.DataFrame(
        [
            {"region": region, "tech": tech, "lifetime_years": value}
            for (region, tech), value in lifetimes.items()
            if tech in expected_owners
        ]
    )
    if (
        life.duplicated(["region", "tech"]).any()
        or {(r.region, r.tech) for r in life.itertuples(index=False)}
        != {(road_rules["region_output_map"].get(region, region), tech)
            for region in regions for tech in expected_owners}
        or life["lifetime_years"].map(lambda x: not isfinite(x) or x <= 0).any()
    ):
        raise ValueError("StatCan bus lifetimes lack complete native owner coverage")
    cohorts = cohorts.merge(life, on=["region", "tech"], validate="many_to_one")
    cohorts["raw_cohort_k_vehicles"] = (
        cohorts["stock_k_vehicles"] * cohorts["age_share"] * cohorts["activity_share"]
    )
    cohorts["eligible_first_model_period"] = (
        cohorts["vintage_year"] + cohorts["lifetime_years"] > first_model_period
    )
    key = ["region", "road_class", "tech"]
    original = cohorts.groupby(key)["raw_cohort_k_vehicles"].transform("sum")
    eligible = cohorts["raw_cohort_k_vehicles"].where(
        cohorts["eligible_first_model_period"], 0.0
    )
    surviving = eligible.groupby([cohorts[column] for column in key]).transform("sum")
    cohorts["redistribution_factor"] = original.div(surviving.where(surviving.gt(0), 1.0))
    cohorts["capacity"] = eligible * cohorts["redistribution_factor"]
    summary = cohorts.assign(eligible_stock=eligible).groupby(key, as_index=False).agg(
        original_stock=("raw_cohort_k_vehicles", "sum"),
        eligible_stock=("eligible_stock", "sum"),
    )
    transfers = []
    for (region, road_class), group in summary.groupby(["region", "road_class"]):
        orphaned = group.loc[group.original_stock.gt(0) & group.eligible_stock.le(0)]
        if orphaned.empty:
            continue
        recipients = group.loc[group.eligible_stock.gt(0)]
        recipient_total = float(recipients.original_stock.sum())
        if recipient_total <= 0:
            raise ValueError(
                f"No eligible bus technology can receive stock: {(region, road_class)}"
            )
        orphan_total = float(orphaned.original_stock.sum())
        for donor in orphaned.itertuples(index=False):
            for recipient in recipients.itertuples(index=False):
                transfers.append(
                    {
                        "region": region,
                        "road_class": road_class,
                        "from_tech": donor.tech,
                        "to_tech": recipient.tech,
                        "capacity_k_vehicles": (
                            float(donor.original_stock)
                            * float(recipient.original_stock)
                            / recipient_total
                        ),
                        "reason": "no_first_period_eligible_cohort",
                    }
                )
        selected = cohorts.region.eq(region) & cohorts.road_class.eq(road_class)
        cohorts.loc[selected, "capacity"] *= 1 + orphan_total / recipient_total
    actual = cohorts.groupby(["ceud_region", "road_class"])["capacity"].sum()
    expected = stocks.set_index(["ceud_region", "road_class"])["stock_k_vehicles"]
    if not (actual - expected).abs().le(1e-7).all():
        raise ValueError("Bus vintage/powertrain allocation does not conserve CEUD stock")
    capacity = cohorts.groupby(
        ["region", "road_class", "tech", "vintage"], as_index=False
    )["capacity"].sum()
    capacity["units"] = road_rules["output_units"]
    return (
        cohorts,
        shares,
        pd.DataFrame(
            excluded_records,
            columns=[
                "ceud_region", "road_class", "year", "fuel_member",
                "excluded_energy_pj", "excluded_energy_share",
            ],
        ),
        pd.DataFrame(
            transfers,
            columns=[
                "region", "road_class", "from_tech", "to_tech",
                "capacity_k_vehicles", "reason",
            ],
        ),
        capacity,
    )


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
    output_dir = build_existing_stock_age_artifacts(args.scenario)
    logging.info("Wrote Ontario LDV age-cohort artifacts to %s", output_dir)


def derive_road_baseline_demand(
    provincial: pd.DataFrame, *, regions: list[str], base_year: int,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Select CEUD provincial road activity and convert millions to billions."""
    factor = float(rules["activity_to_billion_factor"])
    if not isfinite(factor) or factor <= 0:
        raise ValueError("Road activity conversion factor must be positive and finite")
    rows: list[dict[str, Any]] = []
    for service, selector in rules["activity_series"].items():
        for region in regions:
            selected = provincial.loc[
                provincial["region"].eq(region)
                & provincial["year"].eq(base_year)
                & provincial["table_id"].eq(int(selector["table_id"]))
                & provincial["raw_series"].eq(selector["raw_series"])
            ]
            if len(selected) != 1 or selected.iloc[0]["unit"] != "millions":
                raise ValueError(f"Missing or duplicate CEUD road activity for {(region, service, base_year)}")
            value = float(selected.iloc[0]["value"])
            if not isfinite(value) or value < 0:
                raise ValueError(f"Invalid CEUD road activity for {(region, service)}")
            rows.append({
                "ceud_region": region,
                "region": rules["region_output_map"].get(region, region),
                "service": service,
                "commodity": selector["commodity"],
                "units": selector["units"],
                "baseline_demand": value * factor,
            })
    return pd.DataFrame(rows)


def extrapolate_passenger_ldv_demand(
    provincial: pd.DataFrame,
    baseline: pd.DataFrame,
    *,
    regions: list[str],
    base_year: int,
    periods: list[int],
    step: int,
    gdp_indices: Mapping[int, float],
    rules: Mapping[str, Any],
) -> tuple[dict[tuple[str, int, str], float], list[dict[str, Any]]]:
    """Apply provincial car CAGR and assign the GDP-indexed LDV residual to trucks."""
    fit_rules = rules["car_extrapolation"]
    if fit_rules["method"] != "endpoint_cagr_anchored_to_base_year":
        raise ValueError("Unsupported car extrapolation method")
    start_year = int(fit_rules["history_start_year"])
    if start_year >= base_year:
        raise ValueError("Car CAGR history must start before the base year")
    car_selector = rules["activity_series"]["cars"]
    factor = float(rules["activity_to_billion_factor"])
    projected: dict[tuple[str, int, str], float] = {}
    audit: list[dict[str, Any]] = []
    for ceud_region in regions:
        history = provincial.loc[
            provincial["region"].eq(ceud_region)
            & provincial["table_id"].eq(int(car_selector["table_id"]))
            & provincial["raw_series"].eq(car_selector["raw_series"])
            & provincial["year"].isin([start_year, base_year])
        ].sort_values("year")
        if len(history) != 2 or history["year"].tolist() != [start_year, base_year]:
            raise ValueError(f"Missing or duplicate CEUD car CAGR endpoints for {ceud_region}")
        if set(history["unit"]) != {"millions"}:
            raise ValueError(f"CEUD car history unit changed for {ceud_region}")
        activity = (pd.to_numeric(history["value"], errors="raise") * factor).tolist()
        if any(not isfinite(value) or value <= 0 for value in activity):
            raise ValueError(f"CEUD car CAGR endpoints must be finite and positive for {ceud_region}")
        annual_cagr = (activity[-1] / activity[0]) ** (
            1.0 / (base_year - start_year)
        ) - 1.0
        native_region = rules["region_output_map"].get(ceud_region, ceud_region)
        car_rows = baseline.loc[
            baseline["region"].eq(native_region) & baseline["service"].eq("cars")
        ]
        truck_rows = baseline.loc[
            baseline["region"].eq(native_region)
            & baseline["service"].eq("passenger_light_trucks")
        ]
        if len(car_rows) != 1 or len(truck_rows) != 1:
            raise ValueError(f"Missing passenger LDV baseline for {ceud_region}")
        car_base = float(car_rows.iloc[0]["baseline_demand"])
        truck_base = float(truck_rows.iloc[0]["baseline_demand"])
        if abs(car_base - activity[-1]) > 1e-9:
            raise ValueError(f"CEUD car history does not match baseline for {ceud_region}")
        for period in periods:
            end_year = period + step
            total = (car_base + truck_base) * float(gdp_indices[period])
            car = car_base * (1.0 + annual_cagr) ** (end_year - base_year)
            if not isfinite(total) or total <= 0 or not isfinite(car) or car <= 0:
                raise ValueError(
                    f"Invalid passenger LDV projection for {(ceud_region, period)}"
                )
            if car > total:
                raise ValueError(
                    f"Car projection exceeds GDP-indexed passenger LDV total for {(ceud_region, period)}"
                )
            truck = total - car
            projected[native_region, period, "cars"] = car
            projected[native_region, period, "passenger_light_trucks"] = truck
            audit.append({
                "ceud_region": ceud_region,
                "region": native_region,
                "period": period,
                "projection_year": end_year,
                "history_start_year": start_year,
                "annual_car_cagr": annual_cagr,
                "car_demand": car,
                "passenger_light_truck_demand": truck,
                "combined_gdp_indexed_demand": total,
            })
    return projected, audit


if __name__ == "__main__":
    main()
