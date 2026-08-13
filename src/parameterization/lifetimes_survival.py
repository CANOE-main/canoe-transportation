"""Derive apparent cohort retention and source-based road-vehicle lifetimes."""

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from fetching.vehicle_population import write_dataframe_atomic
from parameterization.road_aggregation import (
    apply_vehicle_mapping,
    validate_vehicle_mapping,
)
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    resolve_parameter_path,
)


ONTARIO_RULE_KEY = "ontario_vehicle_population"
ROAD_RULE_KEY = "road_aggregation"
RATINGS_RULE_KEY = "nrcan_fuel_consumption_ratings"
LIFETIME_RULE_KEY = "lifetimes_survival"
ASSORTED_RULE_KEY = "assorted_sources"


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load cohort and lifetime rules."""
    return load_harmonization_rules(bundle, LIFETIME_RULE_KEY)


def aggregate_report_a_snapshots(
    normalized_frames: list[pd.DataFrame],
    mapping: pd.DataFrame,
    *,
    accepted_statuses: set[str],
    status_columns: list[str],
    status_buckets: dict[str, list[str]] | None = None,
    passenger_class: str = "PASSENGER",
    commercial_class: str = "COMMERCIAL",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate mapped passenger and commercial NLR-class snapshots."""
    snapshots: dict[str, list[pd.DataFrame]] = {
        passenger_class: [],
        commercial_class: [],
    }
    for normalized in normalized_frames:
        for vehicle_class in [passenger_class, commercial_class]:
            selected = normalized.loc[
                normalized["VEHICLE_CLASS"].eq(vehicle_class)
            ].copy()
            mapped = apply_vehicle_mapping(
                selected,
                mapping,
                accepted_statuses=accepted_statuses,
            )
            accepted = mapped.loc[mapped["mapping_accepted"]].copy()
            if accepted.empty:
                continue
            id_vars = ["report_year", "MODEL_YEAR", "nlr_atb_class"]
            if status_buckets is None:
                status_long = accepted.melt(
                    id_vars=id_vars,
                    value_vars=status_columns,
                    var_name="stock_status",
                    value_name="cohort_count",
                )
            else:
                bucket_frames: list[pd.DataFrame] = []
                for bucket, columns in status_buckets.items():
                    missing = sorted(set(columns) - set(status_columns))
                    if missing:
                        raise ValueError(
                            f"Status bucket {bucket!r} uses unknown Report A "
                            f"statuses: {', '.join(missing)}"
                        )
                    bucket_frame = accepted.loc[:, id_vars].copy()
                    bucket_frame["stock_status"] = str(bucket)
                    bucket_frame["cohort_count"] = accepted[columns].sum(
                        axis=1,
                        min_count=1,
                    )
                    bucket_frames.append(bucket_frame)
                status_long = pd.concat(bucket_frames, ignore_index=True)
            snapshot = (
                status_long.groupby(
                    [
                        "report_year",
                        "MODEL_YEAR",
                        "nlr_atb_class",
                        "stock_status",
                    ],
                    dropna=False,
                    as_index=False,
                )["cohort_count"]
                .sum(min_count=1)
                .rename(
                    columns={
                        "MODEL_YEAR": "model_year",
                        "nlr_atb_class": "cohort_class",
                    }
                )
            )
            snapshot.insert(
                0,
                "population_group",
                f"mapped_{vehicle_class.lower()}",
            )
            snapshots[vehicle_class].append(snapshot)

    columns = [
        "population_group",
        "report_year",
        "model_year",
        "cohort_class",
        "stock_status",
        "cohort_count",
    ]
    passenger_frame = (
        pd.concat(snapshots[passenger_class], ignore_index=True)
        if snapshots[passenger_class]
        else pd.DataFrame(columns=columns)
    )
    commercial_frame = (
        pd.concat(snapshots[commercial_class], ignore_index=True)
        if snapshots[commercial_class]
        else pd.DataFrame(columns=columns)
    )
    sort_columns = [
        "population_group",
        "report_year",
        "model_year",
        "cohort_class",
        "stock_status",
    ]
    return (
        passenger_frame.sort_values(sort_columns, kind="stable").reset_index(
            drop=True
        ),
        commercial_frame.sort_values(sort_columns, kind="stable").reset_index(
            drop=True
        ),
    )


def raw_mto_key_snapshots(
    normalized_frames: list[pd.DataFrame],
    *,
    passenger_class: str,
    commercial_class: str,
    minimum_model_year: int | None,
    suppressed_code_patterns: list[str],
    unknown_code_labels: list[str],
) -> pd.DataFrame:
    """Build fit-active make-model-vintage snapshots before class mapping.

    A null model-year floor retains every source-reported vintage.  This is the
    survival-evidence interface and is intentionally independent of the 2000
    existing-fleet aggregation floor.
    """
    frames: list[pd.DataFrame] = []
    unknown = {str(label).strip().upper() for label in unknown_code_labels}
    for normalized in normalized_frames:
        selected = normalized.loc[
            normalized["VEHICLE_CLASS"].isin(
                [passenger_class, commercial_class]
            )
        ].copy()
        selected["MODEL_YEAR"] = pd.to_numeric(
            selected["MODEL_YEAR"], errors="coerce"
        )
        selected["FIT_ACTIVE"] = pd.to_numeric(
            selected["FIT_ACTIVE"], errors="coerce"
        )
        eligible = selected["MODEL_YEAR"].notna() & selected["FIT_ACTIVE"].gt(0)
        if minimum_model_year is not None:
            eligible &= selected["MODEL_YEAR"].ge(minimum_model_year)
        selected = selected.loc[eligible].copy()
        for column in ["MAKE", "MODEL"]:
            values = selected[column].astype("string").str.strip()
            suppressed = values.isna() | values.eq("") | values.str.upper().isin(
                unknown
            )
            for pattern in suppressed_code_patterns:
                suppressed |= values.str.fullmatch(
                    str(pattern), na=False
                )
            selected = selected.loc[~suppressed].copy()
        if selected.empty:
            continue
        snapshot = (
            selected.groupby(
                [
                    "VEHICLE_CLASS",
                    "report_year",
                    "MAKE",
                    "MODEL",
                    "MODEL_YEAR",
                ],
                as_index=False,
                dropna=False,
            )["FIT_ACTIVE"]
            .sum(min_count=1)
            .rename(
                columns={
                    "VEHICLE_CLASS": "vehicle_class",
                    "MAKE": "mto_make_code",
                    "MODEL": "mto_model_code",
                    "MODEL_YEAR": "model_year",
                    "FIT_ACTIVE": "cohort_count",
                }
            )
        )
        snapshot.insert(
            0,
            "population_group",
            "raw_" + snapshot["vehicle_class"].str.lower(),
        )
        snapshot["stock_status"] = "FIT_ACTIVE"
        frames.append(snapshot)
    columns = [
        "population_group",
        "vehicle_class",
        "report_year",
        "mto_make_code",
        "mto_model_code",
        "model_year",
        "stock_status",
        "cohort_count",
    ]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True).loc[:, columns].sort_values(
        [
            "population_group",
            "mto_make_code",
            "mto_model_code",
            "model_year",
            "report_year",
        ],
        kind="stable",
    ).reset_index(drop=True)


def mto_key_transition_observations(
    snapshots: pd.DataFrame,
    *,
    implausible_change_ratio: float | None = None,
    ignored_missing_years: set[int] | None = None,
    maximum_transition_age: int | None = None,
) -> tuple[pd.DataFrame, list[int]]:
    """Estimate eligible FIT_ACTIVE make-model-vintage annual transitions.

    Both endpoints must exist in consecutive editions.  The estimator never
    manufactures zero endpoints and never clips growth (negative retirement).
    An optional age ceiling applies to the starting age of the transition, not
    to the source model year, so older vintages can still contribute evidence.
    """
    key = [
        "population_group",
        "vehicle_class",
        "mto_make_code",
        "mto_model_code",
        "model_year",
        "stock_status",
    ]
    columns = [
        *key,
        "report_year",
        "next_report_year",
        "age",
        "cohort_count_t",
        "cohort_count_t1",
        "apparent_retirements",
        "annual_survival_factor",
        "annual_retirement_rate",
        "apparent_retention",
        "apparent_retirement",
        "absolute_change",
        "zero_denominator",
        "retention_above_one",
        "negative_age",
        "newest_model_year_cohort",
        "implausible_change",
    ]
    if snapshots.empty:
        return pd.DataFrame(columns=columns), []
    proxy_count = int(
        snapshots["stock_status"].astype(str).eq("NON_FIT_ACTIVE_PROXY").sum()
    )
    if proxy_count:
        raise AssertionError(
            "MTO survival estimator input contains NON_FIT_ACTIVE_PROXY rows: "
            f"{proxy_count}"
        )
    if not snapshots["stock_status"].astype(str).eq("FIT_ACTIVE").all():
        unexpected = sorted(set(map(str, snapshots["stock_status"])))
        raise AssertionError(
            "MTO survival estimator requires strictly FIT_ACTIVE observations; "
            f"found {unexpected}"
        )
    years = sorted(pd.to_numeric(snapshots["report_year"]).astype(int).unique())
    ignored = ignored_missing_years or set()
    missing_years = [
        year
        for year in range(min(years), max(years) + 1)
        if year not in years and year not in ignored
    ]
    available = set(years)
    observations: list[pd.DataFrame] = []
    for report_year in years:
        next_report_year = report_year + 1
        if next_report_year not in available:
            continue
        left = snapshots.loc[
            snapshots["report_year"].eq(report_year), [*key, "cohort_count"]
        ].rename(columns={"cohort_count": "cohort_count_t"})
        right = snapshots.loc[
            snapshots["report_year"].eq(next_report_year),
            [*key, "cohort_count"],
        ].rename(columns={"cohort_count": "cohort_count_t1"})
        paired = left.merge(right, on=key, how="inner", validate="one_to_one")
        paired["report_year"] = report_year
        paired["next_report_year"] = next_report_year
        paired["age"] = report_year - paired["model_year"]
        eligible = paired["cohort_count_t"].gt(0) & paired["age"].ge(0)
        if maximum_transition_age is not None:
            eligible &= paired["age"].le(maximum_transition_age)
        paired = paired.loc[eligible].copy()
        if paired.empty:
            continue
        paired["zero_denominator"] = False
        paired["apparent_retirements"] = (
            paired["cohort_count_t"] - paired["cohort_count_t1"]
        )
        paired["annual_survival_factor"] = (
            paired["cohort_count_t1"] / paired["cohort_count_t"]
        )
        paired["annual_retirement_rate"] = (
            paired["apparent_retirements"] / paired["cohort_count_t"]
        )
        paired["apparent_retention"] = paired["annual_survival_factor"]
        paired["apparent_retirement"] = paired["annual_retirement_rate"]
        paired["absolute_change"] = (
            paired["cohort_count_t1"] - paired["cohort_count_t"]
        )
        paired["retention_above_one"] = paired["apparent_retention"].gt(1)
        paired["negative_age"] = False
        paired["newest_model_year_cohort"] = False
        paired["implausible_change"] = (
            pd.NA
            if implausible_change_ratio is None
            else (
                paired["apparent_retention"].gt(implausible_change_ratio)
                | paired["apparent_retention"].lt(
                    1 / implausible_change_ratio
                )
            )
        )
        observations.append(paired.loc[:, columns])
    if not observations:
        return pd.DataFrame(columns=columns), missing_years
    return pd.concat(observations, ignore_index=True).sort_values(
        [
            "population_group",
            "mto_make_code",
            "mto_model_code",
            "model_year",
            "report_year",
        ],
        kind="stable",
    ).reset_index(drop=True), missing_years


def map_mto_key_transitions(
    observations: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    accepted_statuses: set[str],
) -> pd.DataFrame:
    """Attach accepted classes only after raw key transitions are estimated."""
    if observations.empty:
        return observations.copy()
    stock = observations.rename(
        columns={
            "mto_make_code": "MAKE",
            "mto_model_code": "MODEL",
            "model_year": "MODEL_YEAR",
        }
    ).copy()
    stock["FIT_ACTIVE"] = stock["cohort_count_t"]
    mapped = apply_vehicle_mapping(
        stock,
        mapping,
        accepted_statuses=accepted_statuses,
    )
    result = mapped.rename(
        columns={
            "MAKE": "mto_make_code",
            "MODEL": "mto_model_code",
            "MODEL_YEAR": "model_year",
        }
    ).drop(columns="FIT_ACTIVE")
    result = result.loc[:, ~result.columns.duplicated(keep="first")]
    duplicate_columns = [
        column for column in ["mto_make_code.1", "mto_model_code.1"]
        if column in result.columns
    ]
    return result.drop(columns=duplicate_columns).reset_index(drop=True)


def annotate_latest_snapshot_presence(
    mapped_observations: pd.DataFrame,
    raw_snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """Mark whether each make-model-vintage series exists in the latest snapshot."""
    output = mapped_observations.copy()
    if output.empty:
        output["present_in_latest_snapshot"] = pd.Series(dtype=bool)
        output["latest_snapshot_year"] = pd.Series(dtype="Int64")
        return output
    latest_snapshot_year = int(
        pd.to_numeric(raw_snapshots["report_year"], errors="raise").max()
    )
    series_key = [
        "vehicle_class",
        "mto_make_code",
        "mto_model_code",
        "model_year",
    ]
    latest_series = (
        raw_snapshots.loc[
            pd.to_numeric(raw_snapshots["report_year"], errors="raise").eq(
                latest_snapshot_year
            ),
            series_key,
        ]
        .drop_duplicates()
        .assign(present_in_latest_snapshot=True)
    )
    output = output.merge(
        latest_series,
        on=series_key,
        how="left",
        validate="many_to_one",
    )
    output["present_in_latest_snapshot"] = (
        output["present_in_latest_snapshot"].fillna(False).astype(bool)
    )
    output["latest_snapshot_year"] = latest_snapshot_year
    return output


def _pooled_retention(
    frame: pd.DataFrame,
    group_columns: list[str],
) -> pd.DataFrame:
    usable = frame.copy()
    if "mapping_accepted" in usable:
        usable = usable.loc[usable["mapping_accepted"].fillna(False)].copy()
    if "zero_denominator" in usable:
        usable = usable.loc[~usable["zero_denominator"]].copy()
    if usable.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "cohort_count_t_sum",
                "cohort_count_t1_sum",
                "observation_count",
                "apparent_retention_pooled",
                "apparent_retirement_pooled",
            ]
        )
    pooled = usable.groupby(
        group_columns, as_index=False, dropna=False
    ).agg(
        cohort_count_t_sum=("cohort_count_t", "sum"),
        cohort_count_t1_sum=("cohort_count_t1", "sum"),
        apparent_retirements=("apparent_retirements", "sum"),
        observation_count=("cohort_count_t", "size"),
    )
    pooled["apparent_retention_pooled"] = (
        pooled["cohort_count_t1_sum"] / pooled["cohort_count_t_sum"]
    )
    pooled["apparent_retirement_pooled"] = (
        1 - pooled["apparent_retention_pooled"]
    )
    pooled["retention_above_one"] = pooled[
        "apparent_retention_pooled"
    ].gt(1)
    return pooled


def aggregate_mto_survival_stages(
    mapped_observations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Pool raw transitions through NLR and CEUD exposure-weighted stages."""
    nlr_vintage = _pooled_retention(
        mapped_observations,
        [
            "nlr_atb_class",
            "nrcan_ceud_class",
            "model_year",
            "age",
        ],
    )
    nlr_class = _pooled_retention(
        mapped_observations,
        [
            "nlr_atb_class",
            "nrcan_ceud_class",
            "age",
        ],
    )
    ceud_vintage = _pooled_retention(
        nlr_vintage.rename(
            columns={
                "cohort_count_t_sum": "cohort_count_t",
                "cohort_count_t1_sum": "cohort_count_t1",
            }
        ).assign(zero_denominator=False),
        [
            "nrcan_ceud_class",
            "model_year",
            "age",
        ],
    )
    usable = mapped_observations.copy()
    if "mapping_accepted" in usable:
        usable = usable.loc[usable["mapping_accepted"].fillna(False)].copy()
    usable = usable.loc[
        usable["nrcan_ceud_class"].isin(["Car", "Light Truck"])
    ].copy()
    ceud_class = (
        usable.groupby(["nrcan_ceud_class", "age"], as_index=False)
        .agg(
            fit_active_exposure=("cohort_count_t", "sum"),
            apparent_retirements=("apparent_retirements", "sum"),
            number_of_vintages=("model_year", "nunique"),
            number_of_transitions=("cohort_count_t", "size"),
        )
        .rename(columns={"nrcan_ceud_class": "vehicle_class"})
    )
    ceud_class["annual_retirement_rate"] = (
        ceud_class["apparent_retirements"]
        / ceud_class["fit_active_exposure"]
    )
    ceud_class["annual_survival_factor"] = (
        1 - ceud_class["annual_retirement_rate"]
    )
    curve_rows: list[pd.DataFrame] = []
    for vehicle_class, rows in ceud_class.groupby("vehicle_class", sort=False):
        observed = rows.set_index("age").sort_index()
        first_observed_age = int(observed.index.min())
        ages = pd.Index(
            range(0, int(observed.index.max()) + 2), name="age"
        )
        curve = observed.reindex(ages).reset_index()
        curve["vehicle_class"] = vehicle_class
        curve["cumulative_survival"] = pd.NA
        survival = 1.0
        continuous = True
        for index, row in curve.iterrows():
            curve.loc[index, "cumulative_survival"] = (
                survival if continuous else pd.NA
            )
            if int(row["age"]) < first_observed_age:
                continue
            rate = row["annual_retirement_rate"]
            if pd.isna(rate):
                continuous = False
            elif continuous:
                survival *= 1 - float(rate)
        curve["cumulative_survival"] = pd.to_numeric(
            curve["cumulative_survival"], errors="coerce"
        )
        curve["cumulative_scrappage"] = 1 - curve["cumulative_survival"]
        curve_rows.append(curve)
    if curve_rows:
        ceud_class = pd.concat(curve_rows, ignore_index=True)
    final_columns = [
        "vehicle_class",
        "age",
        "annual_retirement_rate",
        "annual_survival_factor",
        "cumulative_survival",
        "cumulative_scrappage",
        "fit_active_exposure",
        "number_of_vintages",
        "number_of_transitions",
    ]
    ceud_class = ceud_class.loc[:, final_columns]
    return nlr_vintage, nlr_class, ceud_vintage, ceud_class


def mto_survival_scope_comparison(
    mapped_observations: pd.DataFrame,
) -> pd.DataFrame:
    """Compare all historical transitions with latest-survivor conditioning."""
    if "present_in_latest_snapshot" not in mapped_observations.columns:
        raise ValueError(
            "Mapped transitions require present_in_latest_snapshot annotation"
        )
    scope_frames: list[pd.DataFrame] = []
    scopes = [
        (
            "all_historical_transitions",
            mapped_observations,
        ),
        (
            "latest_snapshot_survivors",
            mapped_observations.loc[
                mapped_observations["present_in_latest_snapshot"]
                .fillna(False)
                .astype(bool)
            ],
        ),
    ]
    for aggregation_scope, scope_observations in scopes:
        *_, curves = aggregate_mto_survival_stages(scope_observations)
        curves.insert(0, "aggregation_scope", aggregation_scope)
        scope_frames.append(curves)
    return pd.concat(scope_frames, ignore_index=True, sort=False)


def transition_mapping_coverage(mapped_observations: pd.DataFrame) -> pd.DataFrame:
    """Measure historical starting exposure mapped to relevant CEUD classes."""
    frame = mapped_observations.copy()
    frame["cohort_count_t"] = pd.to_numeric(
        frame["cohort_count_t"], errors="coerce"
    ).fillna(0)
    relevant = frame["nrcan_ceud_class"].isin(["Car", "Light Truck"])
    accepted = frame.get(
        "mapping_accepted", pd.Series(True, index=frame.index)
    ).fillna(False) & relevant
    outcomes = frame.get(
        "mapping_outcome", pd.Series("mapped_ldv", index=frame.index)
    )
    non_ldv = outcomes.eq("mapped_non_ldv")
    unresolved = outcomes.eq("unmapped")
    rows: list[dict[str, object]] = []
    for source_category, group in frame.groupby("vehicle_class", sort=True):
        group_accepted = accepted.loc[group.index]
        exposure = float(group["cohort_count_t"].sum())
        mapped_exposure = float(
            group.loc[group_accepted, "cohort_count_t"].sum()
        )
        non_ldv_exposure = float(
            group.loc[non_ldv.loc[group.index], "cohort_count_t"].sum()
        )
        unmapped_exposure = float(
            group.loc[unresolved.loc[group.index], "cohort_count_t"].sum()
        )
        rows.append(
            {
                "source_category": str(source_category),
                "fit_active_exposure": exposure,
                "mapped_fit_active_exposure": mapped_exposure,
                "mapped_non_ldv_fit_active_exposure": non_ldv_exposure,
                "unmapped_fit_active_exposure": unmapped_exposure,
                "mapped_exposure_share": (
                    mapped_exposure / exposure if exposure else pd.NA
                ),
                "mapped_non_ldv_exposure_share": (
                    non_ldv_exposure / exposure if exposure else pd.NA
                ),
                "unmapped_exposure_share": (
                    unmapped_exposure / exposure if exposure else pd.NA
                ),
                "number_of_transitions": len(group),
                "mapped_transitions": int(group_accepted.sum()),
                "mapped_non_ldv_transitions": int(non_ldv.loc[group.index].sum()),
                "unmapped_transitions": int(unresolved.loc[group.index].sum()),
            }
        )
    return pd.DataFrame(rows)


def evaluate_mto_survival_decision(
    curves: pd.DataFrame,
    coverage: pd.DataFrame,
    legacy_curves: pd.DataFrame,
    *,
    criteria: dict[str, float | int],
) -> pd.DataFrame:
    """Evaluate transparent class-specific usability gates without clipping."""
    source_for_class = {"Car": "PASSENGER", "Light Truck": "COMMERCIAL"}
    rows: list[dict[str, object]] = []
    tolerance = float(criteria["monotonic_tolerance"])
    for vehicle_class in ["Car", "Light Truck"]:
        class_rows = curves.loc[
            curves["vehicle_class"].eq(vehicle_class)
        ].sort_values("age")
        observed = class_rows.loc[
            class_rows["annual_retirement_rate"].notna()
        ].copy()
        source_category = source_for_class[vehicle_class]
        coverage_rows = coverage.loc[
            coverage["source_category"].eq(source_category),
            "mapped_exposure_share",
        ]
        mapped_share = (
            float(coverage_rows.iloc[0]) if not coverage_rows.empty else 0.0
        )
        bounded = observed["annual_retirement_rate"].between(0, 1)
        bounded_share = float(bounded.mean()) if len(observed) else 0.0
        ages = sorted(map(int, observed["age"]))
        internal_gaps = (
            len(set(range(min(ages), max(ages) + 1)) - set(ages))
            if ages
            else 0
        )
        cumulative = class_rows["cumulative_survival"].dropna()
        monotone = bool(
            cumulative.diff().dropna().le(tolerance).all()
        ) if len(cumulative) else False
        min_vintages = int(observed["number_of_vintages"].min()) if len(observed) else 0
        min_transitions = (
            int(observed["number_of_transitions"].min()) if len(observed) else 0
        )
        legacy_ages = set(
            pd.to_numeric(
                legacy_curves.loc[
                    legacy_curves["source_class"].eq(vehicle_class), "age"
                ],
                errors="coerce",
            ).dropna().astype(int)
        )
        overlap_ages = len(set(ages) & legacy_ages)
        gates = {
            "mapped_exposure_gate": mapped_share
            >= float(criteria["minimum_mapped_exposure_share"]),
            "bounded_rate_gate": bounded_share
            >= float(criteria["minimum_in_bounds_rate_share"]),
            "continuous_age_gate": internal_gaps
            <= int(criteria["maximum_internal_age_gaps"]),
            "monotone_survival_gate": monotone,
            "vintage_support_gate": min_vintages
            >= int(criteria["minimum_vintages_per_age"]),
            "transition_support_gate": min_transitions
            >= int(criteria["minimum_transitions_per_age"]),
            "legacy_comparison_gate": overlap_ages
            >= int(criteria["minimum_legacy_overlap_ages"]),
        }
        rows.append(
            {
                "vehicle_class": vehicle_class,
                "mapped_fit_active_exposure_share": mapped_share,
                "observed_class_age_rates": len(observed),
                "in_bounds_rate_share": bounded_share,
                "internal_age_gaps": internal_gaps,
                "minimum_vintages_per_age": min_vintages,
                "minimum_transitions_per_age": min_transitions,
                "legacy_overlap_ages": overlap_ages,
                **gates,
                "all_gates_pass": all(gates.values()),
            }
        )
    decision = pd.DataFrame(rows)
    usable = bool(decision["all_gates_pass"].all())
    outcome = (
        "usable MTO survival curves"
        if usable
        else "diagnostic MTO evidence with NHTSA survival retained"
    )
    decision["decision_outcome"] = outcome
    decision["parameterization_schedule"] = (
        "MTO cohort-transition curves" if usable else "legacy NHTSA schedules"
    )
    return decision


def cohort_transition_observations(
    snapshots: pd.DataFrame,
    *,
    implausible_change_ratio: float | None = None,
    ignored_missing_years: set[int] | None = None,
) -> tuple[pd.DataFrame, list[int]]:
    """Pair consecutive annual snapshots and retain raw apparent retention."""
    if snapshots.empty:
        columns = [
            "population_group",
            "cohort_class",
            "stock_status",
            "report_year",
            "next_report_year",
            "model_year",
            "age",
            "cohort_count_t",
            "cohort_count_t1",
            "apparent_retention",
            "apparent_retirement",
            "absolute_change",
            "zero_denominator",
            "retention_above_one",
            "negative_age",
            "newest_model_year_cohort",
            "implausible_change",
        ]
        return pd.DataFrame(columns=columns), []

    years = sorted(pd.to_numeric(snapshots["report_year"]).astype(int).unique())
    ignored = ignored_missing_years or set()
    missing_years = [
        year
        for year in range(min(years), max(years) + 1)
        if year not in years and year not in ignored
    ]
    available = set(years)
    pairs = [
        (year, year + 1)
        for year in years
        if year + 1 in available
    ]
    observations: list[pd.DataFrame] = []
    key = ["population_group", "cohort_class", "stock_status", "model_year"]
    for report_year, next_year in pairs:
        left = snapshots.loc[
            snapshots["report_year"].eq(report_year),
            [*key, "cohort_count"],
        ].rename(columns={"cohort_count": "cohort_count_t"})
        right = snapshots.loc[
            snapshots["report_year"].eq(next_year),
            [*key, "cohort_count"],
        ].rename(columns={"cohort_count": "cohort_count_t1"})
        paired = left.merge(
            right,
            on=key,
            how="outer",
            validate="one_to_one",
        )
        paired["cohort_count_t"] = paired["cohort_count_t"].fillna(0)
        paired["cohort_count_t1"] = paired["cohort_count_t1"].fillna(0)
        paired.insert(3, "report_year", report_year)
        paired.insert(4, "next_report_year", next_year)
        paired["age"] = report_year - paired["model_year"]
        paired["zero_denominator"] = paired["cohort_count_t"].eq(0)
        paired["apparent_retention"] = (
            paired["cohort_count_t1"] / paired["cohort_count_t"]
        ).where(~paired["zero_denominator"])
        paired["apparent_retirement"] = 1 - paired["apparent_retention"]
        paired["absolute_change"] = (
            paired["cohort_count_t1"] - paired["cohort_count_t"]
        )
        paired["retention_above_one"] = paired["apparent_retention"].gt(1)
        paired["negative_age"] = paired["age"].lt(0)
        paired["newest_model_year_cohort"] = paired["model_year"].ge(report_year)
        if implausible_change_ratio is None:
            paired["implausible_change"] = pd.NA
        else:
            paired["implausible_change"] = (
                paired["apparent_retention"].gt(implausible_change_ratio)
                | paired["apparent_retention"].lt(
                    1 / implausible_change_ratio
                )
            )
        observations.append(paired)
    if not observations:
        return cohort_transition_observations(
            snapshots.iloc[0:0],
            implausible_change_ratio=implausible_change_ratio,
            ignored_missing_years=ignored,
        )[0], missing_years
    combined = pd.concat(observations, ignore_index=True)
    return (
        combined.sort_values(
            [
                "population_group",
                "cohort_class",
                "stock_status",
                "report_year",
                "model_year",
            ],
            kind="stable",
        ).reset_index(drop=True),
        missing_years,
    )


def pool_cohort_retention(observations: pd.DataFrame) -> pd.DataFrame:
    """Pool cohort counts by class and age; never average percentages."""
    usable = observations.loc[~observations["zero_denominator"]].copy()
    if usable.empty:
        return pd.DataFrame(
            columns=[
                "population_group",
                "cohort_class",
                "stock_status",
                "age",
                "cohort_count_t_sum",
                "cohort_count_t1_sum",
                "observation_count",
                "apparent_retention_pooled",
                "apparent_retirement_pooled",
                "retention_above_one",
            ]
        )
    pooled = (
        usable.groupby(
            ["population_group", "cohort_class", "stock_status", "age"],
            as_index=False,
        )
        .agg(
            cohort_count_t_sum=("cohort_count_t", "sum"),
            cohort_count_t1_sum=("cohort_count_t1", "sum"),
            observation_count=("cohort_count_t", "size"),
        )
    )
    pooled["apparent_retention_pooled"] = (
        pooled["cohort_count_t1_sum"] / pooled["cohort_count_t_sum"]
    )
    pooled["apparent_retirement_pooled"] = (
        1 - pooled["apparent_retention_pooled"]
    )
    pooled["retention_above_one"] = pooled["apparent_retention_pooled"].gt(1)
    return pooled.sort_values(
        ["population_group", "cohort_class", "stock_status", "age"],
        kind="stable",
    ).reset_index(drop=True)


def transition_findings(
    observations: pd.DataFrame,
    *,
    missing_years: list[int],
    implausible_threshold: float | None,
) -> pd.DataFrame:
    """Summarize data-quality flags without changing observations."""
    rows: list[dict[str, Any]] = []
    flags = {
        "zero_cohort_denominator": "zero_denominator",
        "cohort_retention_above_one": "retention_above_one",
        "negative_cohort_age": "negative_age",
        "newest_model_year_cohort": "newest_model_year_cohort",
    }
    for issue_type, column in flags.items():
        selected = observations.loc[observations[column].fillna(False)]
        rows.append(
            {
                "issue_type": issue_type,
                "observation_count": len(selected),
                "cohort_count_t": selected["cohort_count_t"].sum(),
                "cohort_count_t1": selected["cohort_count_t1"].sum(),
                "detail": "Raw observations retained.",
            }
        )
    tail = observations.loc[observations["age"].gt(30)]
    rows.append(
        {
            "issue_type": "vintage_tail_over_age_30",
            "observation_count": len(tail),
            "cohort_count_t": tail["cohort_count_t"].sum(),
            "cohort_count_t1": tail["cohort_count_t1"].sum(),
            "detail": "Counts quantify the untrimmed vintage tail; no low-count threshold applied.",
        }
    )
    rows.append(
        {
            "issue_type": "missing_annual_snapshot",
            "observation_count": len(missing_years),
            "cohort_count_t": pd.NA,
            "cohort_count_t1": pd.NA,
            "detail": "|".join(map(str, missing_years)),
        }
    )
    rows.append(
        {
            "issue_type": "implausibly_large_change",
            "observation_count": (
                int(observations["implausible_change"].fillna(False).sum())
                if implausible_threshold is not None
                else pd.NA
            ),
            "cohort_count_t": pd.NA,
            "cohort_count_t1": pd.NA,
            "detail": (
                f"Configured retention-ratio threshold: {implausible_threshold}"
                if implausible_threshold is not None
                else "Threshold intentionally unconfigured; raw changes and ratios are published."
            ),
        }
    )
    return pd.DataFrame(rows)


def transform_source_survival_curves(
    nhtsa: pd.DataFrame,
    eia: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Preserve source curves and convert EIA scrappage rates cumulatively."""
    nhtsa_source = nhtsa.rename(
        columns={
            "source_vehicle_class_label": "source_class",
            "vehicle_age": "age",
            "survival_rate": "source_value",
        }
    ).copy()
    nhtsa_source["source_measure"] = "survival_probability"
    nhtsa_source["source_unit"] = nhtsa_source["unit"]
    nhtsa_source = nhtsa_source[
        [
            "source_id",
            "source_class",
            "age",
            "source_measure",
            "source_value",
            "source_unit",
        ]
    ]
    eia_source = eia.rename(
        columns={
            "source_vehicle_class_label": "source_class",
            "vehicle_age": "age",
            "annual_scrappage_rate": "source_value",
        }
    ).copy()
    eia_source["source_measure"] = "annual_scrappage_rate"
    eia_source["source_unit"] = eia_source["unit"]
    eia_source = eia_source[
        [
            "source_id",
            "source_class",
            "age",
            "source_measure",
            "source_value",
            "source_unit",
        ]
    ]
    source_curves = pd.concat(
        [nhtsa_source, eia_source],
        ignore_index=True,
    ).sort_values(["source_id", "source_class", "age"], kind="stable")

    transformed_nhtsa = nhtsa_source.rename(
        columns={"source_value": "survival_probability"}
    ).copy()
    transformed_nhtsa["transformation"] = "source_survival_probability"
    transformed_frames = [transformed_nhtsa]
    for source_class, rows in eia_source.groupby("source_class"):
        ordered = rows.sort_values("age").copy()
        age_zero = pd.DataFrame(
            [
                {
                    "source_id": ordered["source_id"].iloc[0],
                    "source_class": source_class,
                    "age": 0,
                    "source_measure": "annual_scrappage_rate",
                    "survival_probability": 1.0,
                    "source_unit": "dimensionless",
                    "transformation": "age0_equals_1",
                }
            ]
        )
        ordered["survival_probability"] = (
            1 - pd.to_numeric(ordered["source_value"], errors="raise")
        ).cumprod()
        ordered["transformation"] = (
            "cumulative_product_of_one_minus_annual_scrappage_rate"
        )
        ordered["source_unit"] = "dimensionless"
        transformed_frames.extend(
            [
                age_zero,
                ordered[
                    [
                        "source_id",
                        "source_class",
                        "age",
                        "source_measure",
                        "survival_probability",
                        "source_unit",
                        "transformation",
                    ]
                ],
            ]
        )
    transformed = pd.concat(transformed_frames, ignore_index=True)
    return (
        source_curves.reset_index(drop=True),
        transformed.sort_values(
            ["source_id", "source_class", "age"],
            kind="stable",
        ).reset_index(drop=True),
    )


def retention_source_comparison(
    pooled: pd.DataFrame,
    source_curves: pd.DataFrame,
    *,
    eia_classes: list[str],
) -> pd.DataFrame:
    """Compare mapped NLR retention with NHTSA and NEMS source retention."""
    observed = pooled.loc[
        pooled["population_group"].isin(
            [
                "mapped_passenger",
                "mapped_commercial",
                "raw_passenger",
                "raw_commercial",
            ]
        )
        & pooled["stock_status"].eq("FIT_ACTIVE")
        & pooled["age"].ge(0)
    ].copy()
    observed = (
        observed.groupby(["cohort_class", "age"], as_index=False)
        .agg(
            cohort_count_t_sum=("cohort_count_t_sum", "sum"),
            cohort_count_t1_sum=("cohort_count_t1_sum", "sum"),
            observation_count=("observation_count", "sum"),
        )
    )
    observed_frame = pd.DataFrame(
        {
            "series_family": "ontario_report_a",
            "series_class": observed["cohort_class"],
            "age": observed["age"],
            "one_year_retention": (
                observed["cohort_count_t1_sum"]
                / observed["cohort_count_t_sum"]
            ),
            "exposed_stock": observed["cohort_count_t_sum"],
            "observation_count": observed["observation_count"],
            "interpretation": (
                "apparent fit-active registration retention; includes migration "
                "and registration-status changes"
            ),
        }
    )
    nhtsa = source_curves.loc[
        source_curves["source_id"].eq("nhtsa_cafe_2024_ldv_survival")
        & source_curves["source_class"].isin(["Cars", "Vans/SUVs", "Pickups"])
        & source_curves["source_measure"].eq("survival_probability")
    ].copy()
    nhtsa["source_value"] = pd.to_numeric(nhtsa["source_value"], errors="raise")
    nhtsa = nhtsa.sort_values(["source_class", "age"], kind="stable")
    nhtsa["next_survival"] = nhtsa.groupby("source_class")["source_value"].shift(-1)
    nhtsa["one_year_retention"] = nhtsa["next_survival"] / nhtsa["source_value"]
    nhtsa_frame = pd.DataFrame(
        {
            "series_family": "nhtsa_cafe_2024_ldv_survival",
            "series_class": nhtsa["source_class"],
            "age": nhtsa["age"],
            "one_year_retention": nhtsa["one_year_retention"],
            "exposed_stock": pd.NA,
            "observation_count": pd.NA,
            "interpretation": "next-age survival divided by current-age survival",
        }
    ).dropna(subset=["one_year_retention"])
    eia = source_curves.loc[
        source_curves["source_id"].eq("eia_nems_hd_truck_scrappage")
        & source_curves["source_class"].isin(eia_classes)
        & source_curves["source_measure"].eq("annual_scrappage_rate")
    ].copy()
    eia_frame = pd.DataFrame(
        {
            "series_family": "eia_nems_hd_truck_scrappage",
            "series_class": eia["source_class"],
            "age": eia["age"],
            "one_year_retention": 1
            - pd.to_numeric(eia["source_value"], errors="raise"),
            "exposed_stock": pd.NA,
            "observation_count": pd.NA,
            "interpretation": "one minus sourced annual scrappage rate",
        }
    )
    comparison = pd.concat(
        [observed_frame, nhtsa_frame, eia_frame],
        ignore_index=True,
    )
    return comparison.sort_values(
        ["series_family", "series_class", "age"],
        kind="stable",
    ).reset_index(drop=True)


def legacy_wards_survival_curves(
    transformed: pd.DataFrame,
    wards: pd.DataFrame,
    *,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Aggregate legacy Car/Light Truck curves with reviewed Wards shares."""
    legacy_rules = rules["legacy_survival"]
    nhtsa = transformed.loc[
        transformed["source_id"].eq("nhtsa_cafe_2024_ldv_survival")
    ].copy()
    car = nhtsa.loc[
        nhtsa["source_class"].eq(str(legacy_rules["car_source_class"]))
    ].copy()
    car["source_id"] = "wards_weighted_nhtsa_legacy"
    car["source_class"] = "Car"
    car["included_weight"] = 1.0
    car["weight_year"] = pd.NA
    car["transformation"] = "NHTSA Cars used directly for legacy Car"

    weight_year = int(pd.to_numeric(wards["year"], errors="raise").max())
    latest = wards.loc[pd.to_numeric(wards["year"]).eq(weight_year)].copy()
    source_weights: list[dict[str, Any]] = []
    for source_class, nlr_classes in legacy_rules[
        "light_truck_source_classes"
    ].items():
        weight = latest.loc[
            latest["nlr_atb_class"].isin(nlr_classes),
            "market_share",
        ].sum()
        source_weights.append(
            {"source_class": str(source_class), "source_weight": float(weight)}
        )
    weights = pd.DataFrame(source_weights)
    weights["source_weight"] = weights["source_weight"] / weights[
        "source_weight"
    ].sum()
    light = nhtsa.merge(weights, on="source_class", how="inner", validate="many_to_one")
    light["weighted_survival"] = light["survival_probability"] * light["source_weight"]
    light = (
        light.groupby("age", as_index=False)
        .agg(
            survival_probability=("weighted_survival", "sum"),
            included_weight=("source_weight", "sum"),
        )
    )
    light["source_id"] = "wards_weighted_nhtsa_legacy"
    light["source_class"] = "Light Truck"
    light["source_measure"] = "survival_probability"
    light["source_unit"] = "dimensionless"
    light["weight_year"] = weight_year
    light["transformation"] = (
        "Latest complete Wards class shares aggregate NHTSA Vans/SUVs and Pickups"
    )
    columns = [
        "source_id", "source_class", "age", "source_measure",
        "survival_probability", "source_unit", "transformation",
        "included_weight", "weight_year",
    ]
    return pd.concat([car.loc[:, columns], light.loc[:, columns]], ignore_index=True)


def nlr_source_survival_curves(
    transformed: pd.DataFrame,
    mappings: pd.DataFrame,
) -> pd.DataFrame:
    """Publish the five NLR LDV source anchors for the new MTO approach."""
    source_map = mappings.loc[
        mappings["target_system"].eq("NLR ATB"),
        ["source_class", "target_class", "nrcan_ceud_class"],
    ]
    nhtsa = transformed.loc[
        transformed["source_id"].eq("nhtsa_cafe_2024_ldv_survival")
    ].copy()
    anchored = source_map.merge(nhtsa, on="source_class", how="inner", validate="many_to_many")
    anchored["source_id"] = "nlr_atb_source_anchor"
    anchored["source_class"] = anchored["target_class"]
    anchored["transformation"] = (
        "NHTSA source class retained at NLR ATB LDV class grain for MTO calibration"
    )
    return anchored[
        [
            "source_id", "source_class", "age", "source_measure",
            "survival_probability", "source_unit", "transformation",
            "nrcan_ceud_class",
        ]
    ]


def survival_class_mappings(rules: dict[str, Any]) -> pd.DataFrame:
    """Publish explicit source-to-target class mappings and comparison scope."""
    rows: list[dict[str, Any]] = []
    for source_class, nlr_classes in rules["nhtsa_class_to_nlr"].items():
        for nlr_class in nlr_classes:
            ceud_class = (
                "Car"
                if str(nlr_class) in {"Compact", "Midsize"}
                else "Light Truck"
            )
            rows.append(
                {
                    "source_family": "nhtsa_cafe_2024_ldv_survival",
                    "source_class": source_class,
                    "target_system": "NLR ATB",
                    "target_class": nlr_class,
                    "nrcan_ceud_class": ceud_class,
                    "mapping_scope": "Ontario LDV weighted aggregation",
                }
            )
    for source_class in rules["nhtsa_comparison_classes"]:
        rows.append(
            {
                "source_family": "nhtsa_cafe_2024_ldv_survival",
                "source_class": source_class,
                "target_system": "comparison",
                "target_class": "medium-duty comparison",
                "nrcan_ceud_class": pd.NA,
                "mapping_scope": "comparison only",
            }
        )
    for source_class in rules["eia_source_classes"]:
        rows.append(
            {
                "source_family": "eia_nems_hd_truck_scrappage",
                "source_class": source_class,
                "target_system": "comparison",
                "target_class": source_class,
                "nrcan_ceud_class": pd.NA,
                "mapping_scope": "source class only; no fabricated Wards MHDV weighting",
            }
        )
    return pd.DataFrame(rows)


def aggregate_ceud_survival_curves(
    transformed: pd.DataFrame,
    nlr_weights: pd.DataFrame,
    mappings: pd.DataFrame,
) -> pd.DataFrame:
    """Weight NHTSA LDV curves using latest Ontario NLR class weights."""
    weights = nlr_weights.loc[
        nlr_weights["weight_basis"].eq("all_vintages")
    ].copy()
    source_map = mappings.loc[
        mappings["target_system"].eq("NLR ATB"),
        ["source_class", "target_class", "nrcan_ceud_class"],
    ].rename(columns={"target_class": "nlr_atb_class"})
    weights = weights.merge(
        source_map,
        on=["nlr_atb_class", "nrcan_ceud_class"],
        how="inner",
        validate="many_to_one",
    )
    nhtsa = transformed.loc[
        transformed["source_id"].eq("nhtsa_cafe_2024_ldv_survival")
    ].copy()
    weighted = weights.merge(
        nhtsa[["source_class", "age", "survival_probability"]],
        on="source_class",
        how="inner",
        validate="many_to_many",
    )
    weighted["weighted_survival"] = (
        weighted["aggregation_weight"] * weighted["survival_probability"]
    )
    aggregated = (
        weighted.groupby(
            ["report_year", "nrcan_ceud_class", "age"],
            as_index=False,
        )
        .agg(
            survival_probability=("weighted_survival", "sum"),
            included_weight=("aggregation_weight", "sum"),
        )
    )
    aggregated["survival_probability"] = (
        aggregated["survival_probability"] / aggregated["included_weight"]
    )
    aggregated["source_id"] = "ontario_report_a_weighted_nhtsa_cafe"
    aggregated["source_class"] = aggregated["nrcan_ceud_class"]
    aggregated["source_measure"] = "survival_probability"
    aggregated["source_unit"] = "dimensionless"
    aggregated["transformation"] = (
        "Ontario latest fit-active NLR weights applied to NHTSA source curves"
    )
    return aggregated[
        [
            "source_id",
            "source_class",
            "age",
            "source_measure",
            "survival_probability",
            "source_unit",
            "transformation",
            "report_year",
            "included_weight",
        ]
    ]


def median_equivalent_lifetimes(
    curves: pd.DataFrame,
    *,
    interpolation: str = "none",
) -> pd.DataFrame:
    """Return first age at or below 0.5, with optional linear interpolation."""
    if interpolation not in {"none", "linear"}:
        raise ValueError(f"Unsupported median interpolation: {interpolation}")
    rows: list[dict[str, Any]] = []
    for (source_id, source_class), group in curves.groupby(
        ["source_id", "source_class"]
    ):
        ordered = group.sort_values("age")
        at_or_below = ordered.loc[ordered["survival_probability"].le(0.5)]
        if at_or_below.empty:
            median_age = pd.NA
            lower_age = pd.NA
            upper_age = pd.NA
        else:
            upper = at_or_below.iloc[0]
            upper_age = float(upper["age"])
            prior = ordered.loc[ordered["age"].lt(upper["age"])].tail(1)
            lower_age = (
                float(prior["age"].iloc[0])
                if not prior.empty
                else upper_age
            )
            median_age = upper_age
            if interpolation == "linear" and not prior.empty:
                lower_survival = float(prior["survival_probability"].iloc[0])
                upper_survival = float(upper["survival_probability"])
                if lower_survival != upper_survival:
                    fraction = (
                        (lower_survival - 0.5)
                        / (lower_survival - upper_survival)
                    )
                    median_age = lower_age + fraction * (upper_age - lower_age)
        if source_id == "wards_weighted_nhtsa_legacy":
            target_system = "nrcan_ceud"
        elif source_id == "nlr_atb_source_anchor":
            target_system = "nlr_atb"
        else:
            target_system = "source_class"
        rows.append(
            {
                "source_id": source_id,
                "target_system": target_system,
                "target_class": source_class,
                "median_equivalent_age": median_age,
                "first_age_at_or_below_0_5": upper_age,
                "previous_age_above_0_5": lower_age,
                "interpolation_method": interpolation,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["target_system", "target_class", "source_id"],
        kind="stable",
    )


def _load_normalized_frames(
    output_dir: Path,
    manifest: pd.DataFrame,
    *,
    status_columns: list[str],
) -> list[pd.DataFrame]:
    usecols = [
        "report_year",
        "VEHICLE_CLASS",
        "MAKE",
        "MODEL",
        "MODEL_YEAR",
        *status_columns,
    ]
    frames: list[pd.DataFrame] = []
    for row in manifest.sort_values("year").itertuples(index=False):
        if hasattr(row, "cohort_snapshot_usable") and str(
            row.cohort_snapshot_usable
        ).strip().lower() not in {"true", "1", "yes"}:
            continue
        path = output_dir / str(row.normalized_report_a_output)
        frames.append(pd.read_csv(path, usecols=usecols, low_memory=False))
    return frames


def build_lifetime_artifacts(scenario_path: str | Path) -> Path:
    """Publish Report A transitions, transformed curves, and medians."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)
    ontario_rules = load_harmonization_rules(bundle, ONTARIO_RULE_KEY)
    road_rules = load_harmonization_rules(bundle, ROAD_RULE_KEY)
    rating_rules = load_harmonization_rules(bundle, RATINGS_RULE_KEY)
    assorted_rules = load_harmonization_rules(bundle, ASSORTED_RULE_KEY)
    output_dir = resolve_input_path(
        bundle,
        "interim",
        ontario_rules["interim_subdir"],
    )
    manifest = pd.read_csv(output_dir / ontario_rules["manifest_file"])
    report_rules = ontario_rules["reports"]["A"]
    status_columns = [str(value) for value in report_rules["status_columns"]]
    normalized_frames = _load_normalized_frames(
        output_dir,
        manifest,
        status_columns=status_columns,
    )

    mapping_path = resolve_parameter_path(
        bundle,
        road_rules["vehicle_size_class_map_file"],
    )
    mapping = pd.read_csv(mapping_path, dtype=str, keep_default_na=False)
    mapping = validate_vehicle_mapping(
        mapping,
        rules=road_rules,
        rating_class_rules=rating_rules["vehicle_class_harmonization"],
    )
    accepted_statuses = {
        str(status)
        for status in road_rules["accepted_mapping_statuses"]
    }
    threshold = rules.get("implausible_change_ratio")
    raw_snapshots = raw_mto_key_snapshots(
        normalized_frames,
        passenger_class=str(report_rules["passenger_class"]),
        commercial_class=str(report_rules["commercial_class"]),
        minimum_model_year=(
            int(rules["minimum_transition_model_year"])
            if rules.get("minimum_transition_model_year") is not None
            else None
        ),
        suppressed_code_patterns=[
            str(pattern)
            for pattern in report_rules.get("suppressed_code_patterns", [])
        ],
        unknown_code_labels=[
            str(label) for label in report_rules.get("unknown_code_labels", [])
        ],
    )
    raw_key_observations, raw_missing_years = mto_key_transition_observations(
        raw_snapshots,
        implausible_change_ratio=(
            float(threshold) if threshold is not None else None
        ),
        ignored_missing_years={
            int(year) for year in report_rules.get("excluded_years", [])
        },
        maximum_transition_age=(
            int(rules["maximum_transition_age"])
            if rules.get("maximum_transition_age") is not None
            else None
        ),
    )
    mapped_key_observations = map_mto_key_transitions(
        raw_key_observations,
        mapping,
        accepted_statuses=accepted_statuses,
    )
    mapped_key_observations = annotate_latest_snapshot_presence(
        mapped_key_observations,
        raw_snapshots,
    )
    transition_coverage = transition_mapping_coverage(
        mapped_key_observations
    )
    (
        nlr_vintage_retention,
        nlr_class_retention,
        ceud_vintage_retention,
        ceud_class_retention,
    ) = aggregate_mto_survival_stages(mapped_key_observations)
    survival_scope_comparison = mto_survival_scope_comparison(
        mapped_key_observations
    )
    mapped_snapshot, commercial_snapshot = aggregate_report_a_snapshots(
        normalized_frames,
        mapping,
        accepted_statuses=accepted_statuses,
        status_columns=status_columns,
        status_buckets={
            str(bucket): [str(status) for status in statuses]
            for bucket, statuses in rules["status_buckets"].items()
        },
        passenger_class=str(report_rules["passenger_class"]),
        commercial_class=str(report_rules["commercial_class"]),
    )
    all_snapshots = pd.concat(
        [mapped_snapshot, commercial_snapshot],
        ignore_index=True,
    )
    observations, _missing_years = cohort_transition_observations(
        all_snapshots,
        implausible_change_ratio=(
            float(threshold)
            if threshold is not None
            else None
        ),
        ignored_missing_years={
            int(year)
            for year in report_rules.get("excluded_years", [])
        },
    )
    pooled = nlr_class_retention.rename(
        columns={"nlr_atb_class": "cohort_class"}
    ).assign(population_group="all_mapped", stock_status="FIT_ACTIVE")
    findings = transition_findings(
        raw_key_observations,
        missing_years=raw_missing_years,
        implausible_threshold=(
            float(threshold)
            if threshold is not None
            else None
        ),
    )

    assorted_dir = resolve_input_path(
        bundle,
        "interim",
        assorted_rules["interim_subdir"],
    )
    nhtsa = pd.read_csv(
        assorted_dir / assorted_rules["nhtsa_cafe"]["output_file"]
    )
    eia = pd.read_csv(
        assorted_dir / assorted_rules["eia_nems"]["output_file"]
    )
    source_curves, transformed = transform_source_survival_curves(nhtsa, eia)
    retention_comparison = retention_source_comparison(
        pooled,
        source_curves,
        eia_classes=[
            str(value)
            for value in rules["eia_retention_comparison_classes"]
        ],
    )
    class_mappings = survival_class_mappings(rules)
    wards = pd.read_csv(output_dir / road_rules["wards_comparison_file"])
    legacy_curves = legacy_wards_survival_curves(
        transformed,
        wards,
        rules=rules,
    )
    mto_survival_decision = evaluate_mto_survival_decision(
        ceud_class_retention,
        transition_coverage,
        legacy_curves,
        criteria={
            str(key): value
            for key, value in rules["mto_survival_decision_criteria"].items()
        },
    )
    nlr_curves = nlr_source_survival_curves(transformed, class_mappings)
    transformed_with_aggregates = pd.concat(
        [transformed, legacy_curves, nlr_curves],
        ignore_index=True,
        sort=False,
    )
    medians = median_equivalent_lifetimes(
        transformed_with_aggregates,
        interpolation=str(rules["interpolation"]),
    )

    outputs = {
        rules["raw_key_snapshot_file"]: raw_snapshots,
        rules["raw_key_transition_file"]: raw_key_observations,
        rules["mapped_key_transition_file"]: mapped_key_observations,
        rules["nlr_class_vintage_retention_file"]: nlr_vintage_retention,
        rules["nlr_class_retention_file"]: nlr_class_retention,
        rules["ceud_class_vintage_retention_file"]: ceud_vintage_retention,
        rules["ceud_class_retention_file"]: ceud_class_retention,
        rules["ceud_scope_comparison_file"]: survival_scope_comparison,
        rules["transition_mapping_coverage_file"]: transition_coverage,
        rules["mto_survival_decision_file"]: mto_survival_decision,
        rules["cohort_snapshot_file"]: mapped_snapshot,
        rules["commercial_snapshot_file"]: commercial_snapshot,
        rules["transition_observations_file"]: observations,
        rules["pooled_estimates_file"]: pooled,
        rules["transition_findings_file"]: findings,
        rules["retention_comparison_file"]: retention_comparison,
        rules["legacy_survival_curves_file"]: legacy_curves,
        rules["nlr_survival_curves_file"]: nlr_curves,
        rules["source_curves_file"]: source_curves,
        rules["transformed_curves_file"]: transformed_with_aggregates,
        rules["class_mapping_file"]: class_mappings,
        rules["median_lifetimes_file"]: medians,
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
    output_dir = build_lifetime_artifacts(args.scenario)
    logging.info("Wrote cohort and lifetime artifacts to %s", output_dir)


if __name__ == "__main__":
    main()
