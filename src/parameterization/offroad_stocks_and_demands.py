"""Estimate off-road demand capacity and surviving annual vintage cohorts."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any

import pandas as pd


ENERGY_KEY = ["region", "mode", "tech", "year"]
INTENSITY_KEY = ["mode", "year"]
CAPACITY_UNITS = {"bn passenger-km", "bn tonne-km"}


def derive_offroad_baseline_demand(
    provincial: pd.DataFrame, national: pd.DataFrame, *,
    regions: list[str], base_year: int, rules: dict[str, Any],
) -> pd.DataFrame:
    """Use total provincial mode energy and national same-year intensity."""
    energy_rows: list[dict[str, Any]] = []
    intensity_rows: list[dict[str, Any]] = []
    for service, selector in rules["energy_intensity_series"].items():
        intensity = national.loc[
            national["year"].eq(base_year)
            & national["table_id"].eq(int(selector["national_table"]))
            & national["raw_series"].eq(selector["intensity_series"])
        ]
        expected_unit = "MJ/Pkm" if selector["units"] == "bn passenger-km" else "MJ/Tkm"
        if len(intensity) != 1 or intensity.iloc[0]["unit"] != expected_unit:
            raise ValueError(f"Missing or duplicate national CEUD intensity for {service}")
        intensity_rows.append({"mode": service, "year": base_year,
                               "intensity_mj_per_km": intensity.iloc[0]["value"],
                               "units": selector["units"]})
        for region in regions:
            energy = provincial.loc[
                provincial["region"].eq(region)
                & provincial["year"].eq(base_year)
                & provincial["table_id"].eq(int(selector["provincial_table"]))
                & provincial["raw_series"].eq(selector["energy_series"])
            ]
            if len(energy) != 1 or energy.iloc[0]["unit"] != "PJ":
                raise ValueError(f"Missing or duplicate provincial CEUD energy for {(region, service)}")
            energy_rows.append({"region": rules["region_output_map"].get(region, region),
                                "ceud_region": region, "mode": service,
                                "tech": selector["commodity"], "year": base_year,
                                "energy_pj": energy.iloc[0]["value"]})
    annual = estimate_demand_unit_capacity(pd.DataFrame(energy_rows), pd.DataFrame(intensity_rows))
    annual = annual.merge(
        pd.DataFrame(energy_rows)[["region", "mode", "tech", "ceud_region"]],
        on=["region", "mode", "tech"], validate="one_to_one",
    )
    return annual.rename(columns={"mode": "service", "tech": "commodity",
                                  "capacity": "baseline_demand"})[
        ["ceud_region", "region", "service", "commodity", "units", "baseline_demand"]
    ]


def estimate_demand_unit_capacity(
    energy: pd.DataFrame,
    intensity: pd.DataFrame,
) -> pd.DataFrame:
    """Divide provincial PJ by same-year national MJ/km in billion-km units.

    The energy input already identifies the native incumbent technology. Marine fuel
    rows therefore retain separate technologies while sharing their mode intensity.
    Numerically, 1 PJ / (1 MJ/km) = 1 billion km.
    """
    energy_columns = {*ENERGY_KEY, "energy_pj"}
    intensity_columns = {*INTENSITY_KEY, "intensity_mj_per_km", "units"}
    if missing := sorted(energy_columns - set(energy.columns)):
        raise ValueError(f"Off-road energy is missing columns: {missing}")
    if missing := sorted(intensity_columns - set(intensity.columns)):
        raise ValueError(f"Off-road intensity is missing columns: {missing}")
    if energy.duplicated(ENERGY_KEY).any():
        raise ValueError("Duplicate off-road energy region/mode/tech/year rows")
    if intensity.duplicated(INTENSITY_KEY).any():
        raise ValueError("Duplicate national off-road intensity mode/year rows")
    if energy.empty:
        raise ValueError("Off-road energy has no rows")

    selected_energy = energy.loc[:, [*ENERGY_KEY, "energy_pj"]].copy()
    selected_intensity = intensity.loc[
        :, [*INTENSITY_KEY, "intensity_mj_per_km", "units"]
    ].copy()
    for frame, column, allow_zero in (
        (selected_energy, "energy_pj", True),
        (selected_intensity, "intensity_mj_per_km", False),
    ):
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        invalid = ~frame[column].map(isfinite) | (
            frame[column] < 0 if allow_zero else frame[column] <= 0
        )
        if invalid.any():
            raise ValueError(
                f"Off-road {column} must be finite and positive where required"
            )
    unknown_units = sorted(set(selected_intensity["units"]) - CAPACITY_UNITS)
    if unknown_units:
        raise ValueError(f"Unsupported off-road capacity units: {unknown_units}")
    if selected_energy[["region", "mode", "tech"]].isna().any().any():
        raise ValueError("Off-road energy has blank region, mode, or technology")

    joined = selected_energy.merge(
        selected_intensity,
        on=INTENSITY_KEY,
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    unmatched = joined["_merge"].ne("both")
    if unmatched.any():
        keys = joined.loc[unmatched, INTENSITY_KEY].drop_duplicates().to_dict("records")
        raise ValueError(f"Missing same-year national off-road intensity for {keys}")
    joined = joined.drop(columns="_merge")
    joined["capacity"] = joined["energy_pj"] / joined["intensity_mj_per_km"]
    return joined.sort_values(ENERGY_KEY, kind="stable").reset_index(drop=True)


def estimate_linear_cohort_additions(
    annual_capacity: pd.DataFrame,
    *,
    lifetimes_by_mode: Mapping[str, float],
    base_year: int,
    reconcile_annual_capacity: bool = False,
) -> pd.DataFrame:
    """Estimate annual additions and their surviving capacity in the base year.

    A cohort loses 1/lifetime of its effective original capacity per year. When
    annual reconciliation is enabled, excess surviving capacity is retired
    proportionally before additions, so each year's fleet equals its observed
    proxy. Input histories must be contiguous through the base year.
    """
    required = {*ENERGY_KEY, "capacity", "units"}
    if missing := sorted(required - set(annual_capacity.columns)):
        raise ValueError(f"Annual off-road capacity is missing columns: {missing}")
    if annual_capacity.empty:
        raise ValueError("Annual off-road capacity has no rows")
    if annual_capacity.duplicated(ENERGY_KEY).any():
        raise ValueError(
            "Duplicate annual off-road capacity region/mode/tech/year rows"
        )
    if annual_capacity[["region", "mode", "tech", "units"]].isna().any().any():
        raise ValueError("Annual off-road capacity has blank keys or units")
    for mode in annual_capacity["mode"].unique():
        lifetime = lifetimes_by_mode.get(str(mode))
        if lifetime is None or not isfinite(float(lifetime)) or float(lifetime) <= 0:
            raise ValueError(f"Missing positive off-road lifetime for {mode!r}")

    rows: list[dict[str, object]] = []
    for (region, mode, tech), history in annual_capacity.groupby(
        ["region", "mode", "tech"], sort=True
    ):
        ordered = history.sort_values("year", kind="stable")
        years = pd.to_numeric(ordered["year"], errors="raise").astype(int).tolist()
        if years != list(range(years[0], base_year + 1)):
            raise ValueError(
                f"Off-road history for {(region, mode, tech)} must be contiguous through "
                f"base year {base_year}: {years}"
            )
        units = set(ordered["units"])
        if len(units) != 1 or not units.issubset(CAPACITY_UNITS):
            raise ValueError(f"Off-road units change within {(region, mode, tech)}")
        lifetime = float(lifetimes_by_mode[str(mode)])
        additions: list[dict[str, float | int]] = []
        group_rows: list[dict[str, object]] = []
        for source_row in ordered.itertuples(index=False):
            year = int(source_row.year)
            capacity = float(source_row.capacity)
            if not isfinite(capacity) or capacity < 0:
                raise ValueError(
                    f"Invalid annual off-road capacity for {(region, mode, tech, year)}"
                )
            residual = sum(
                float(cohort["effective_original"])
                * max(0.0, 1.0 - (year - int(cohort["year"])) / lifetime)
                for cohort in additions
            )
            residual_before_adjustment = residual
            early_retired = 0.0
            retirement_factor = 1.0
            if reconcile_annual_capacity and residual > capacity:
                retirement_factor = capacity / residual
                early_retired = residual - capacity
                for cohort in additions:
                    cohort["effective_original"] = (
                        float(cohort["effective_original"]) * retirement_factor
                    )
                residual = capacity
            addition = max(0.0, capacity - residual)
            additions.append({"year": year, "effective_original": addition})
            group_rows.append(
                {
                    "region": region,
                    "mode": mode,
                    "tech": tech,
                    "vintage_year": year,
                    "estimated_annual_capacity": capacity,
                    "residual_before_additions": residual_before_adjustment,
                    "residual_after_early_retirement": residual,
                    "capacity_additions": addition,
                    "early_retired_capacity": early_retired,
                    "early_retirement_factor": retirement_factor,
                    "units": source_row.units,
                    "lifetime_years": lifetime,
                }
            )
        for row, cohort in zip(group_rows, additions, strict=True):
            row["capacity_at_base_year"] = float(cohort["effective_original"]) * max(
                0.0, 1.0 - (base_year - int(cohort["year"])) / lifetime
            )
        if reconcile_annual_capacity:
            observed = float(ordered.iloc[-1]["capacity"])
            surviving = sum(float(row["capacity_at_base_year"]) for row in group_rows)
            if abs(surviving - observed) > 1e-8:
                raise ValueError(
                    f"Annual cohort reconciliation did not conserve {(region, mode, tech)}"
                )
        rows.extend(group_rows)
    return pd.DataFrame(rows)


def redistribute_eligible_offroad_cohorts(
    cohorts: pd.DataFrame,
    base_year_capacity: pd.DataFrame,
    *,
    first_model_period: int,
) -> pd.DataFrame:
    """Conserve each base-year proxy after excluding fully retired old cohorts."""
    keys = ["region", "mode", "tech"]
    expected = {*keys, "vintage_year", "lifetime_years", "capacity_at_base_year"}
    if missing := sorted(expected - set(cohorts.columns)):
        raise ValueError(f"Off-road cohorts are missing columns: {missing}")
    if missing := sorted({*keys, "capacity"} - set(base_year_capacity.columns)):
        raise ValueError(f"Base-year capacity is missing columns: {missing}")
    if base_year_capacity.duplicated(keys).any():
        raise ValueError("Duplicate off-road base-year capacity keys")
    result = cohorts.copy()
    result = result.merge(
        base_year_capacity[keys + ["capacity"]].rename(
            columns={"capacity": "base_year_proxy"}
        ),
        on=keys,
        how="left",
        validate="many_to_one",
    )
    if result["base_year_proxy"].isna().any():
        raise ValueError("Missing off-road base-year capacity proxy")
    result["raw_capacity_at_base_year"] = result["capacity_at_base_year"]
    result["eligible_first_model_period"] = (
        result["vintage_year"] + result["lifetime_years"] > first_model_period
    )
    eligible = result["capacity_at_base_year"].where(
        result["eligible_first_model_period"], 0.0
    )
    totals = eligible.groupby([result[key] for key in keys]).transform("sum")
    positive_proxy_without_cohorts = result["base_year_proxy"].gt(0) & totals.le(0)
    if positive_proxy_without_cohorts.any():
        failed = result.loc[positive_proxy_without_cohorts, keys].drop_duplicates()
        raise ValueError(
            f"No eligible off-road cohorts for positive base-year proxy: {failed.to_dict('records')}"
        )
    result["redistribution_factor"] = result["base_year_proxy"].div(
        totals.where(totals.gt(0), 1.0)
    )
    result["capacity_at_base_year"] = eligible * result["redistribution_factor"]
    actual = result.groupby(keys)["capacity_at_base_year"].sum()
    expected_proxy = result.groupby(keys)["base_year_proxy"].first()
    if not (actual - expected_proxy).abs().le(1e-8).all():
        raise ValueError("Off-road cohorts do not conserve base-year capacity proxy")
    return result


def build_offroad_existing_capacity(
    *,
    provincial: pd.DataFrame,
    national: pd.DataFrame,
    manual_lifetimes: pd.DataFrame,
    source_selector: str,
    regions: list[str],
    base_year: int,
    first_model_period: int,
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Select CEUD mode series, apply same-year intensities and cohort retirement."""
    periods = [int(period) for period in rules["vintage_periods"]]
    if periods != sorted(set(periods)) or periods[-1] != base_year:
        raise ValueError("Off-road vintage periods must be unique and end at base year")
    if rules["retirement_rule"] != "linear_cohort_annual_reconciliation":
        raise ValueError("Unsupported off-road retirement rule")
    if (
        rules["first_model_period_rule"] != "positive_surviving_capacity"
        or rules["ineligible_cohort_redistribution"]
        != "proportional_to_eligible_capacity"
        or rules["marine_powertrain_split"] != "base_year_energy"
    ):
        raise ValueError("Unsupported off-road vintage or marine allocation rule")
    if first_model_period <= base_year:
        raise ValueError("The first model period must follow the base year")
    energy_rows: list[dict[str, Any]] = []
    intensity_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    modes: set[str] = set()
    for key, selector in rules["capacity_series"].items():
        mode = selector.get("mode", key)
        modes.add(mode)
        selected_national = national.loc[
            national["table_id"].eq(int(selector["national_table"]))
            & national["raw_series"].eq(selector["intensity_series"])
        ]
        if len(selected_national) != base_year - periods[0] + 1 or set(
            selected_national["year"]
        ) != set(range(periods[0], base_year + 1)):
            raise ValueError(f"Incomplete national intensity for {mode}")
        expected_unit = "MJ/Pkm" if selector["units"] == "bn passenger-km" else "MJ/Tkm"
        if set(selected_national["unit"]) != {expected_unit}:
            raise ValueError(f"National intensity unit changed for {mode}")
        for row in selected_national.itertuples(index=False):
            intensity_rows.append(
                {
                    "mode": mode,
                    "year": int(row.year),
                    "intensity_mj_per_km": row.value,
                    "units": selector["units"],
                }
            )
        for region in regions:
            selected = provincial.loc[
                provincial["region"].eq(region)
                & provincial["table_id"].eq(int(selector["provincial_table"]))
                & provincial["raw_series"].eq(selector["energy_series"])
            ]
            if (
                len(selected) != base_year - periods[0] + 1
                or set(selected["year"]) != set(range(periods[0], base_year + 1))
                or set(selected["unit"]) != {"PJ"}
            ):
                raise ValueError(
                    f"Incomplete provincial PJ energy for {(region, mode, key)}"
                )
            for row in selected.itertuples(index=False):
                energy_rows.append(
                    {
                        "region": rules["region_output_map"].get(region, region),
                        "ceud_region": region,
                        "mode": mode,
                        "tech": selector["tech"],
                        "year": int(row.year),
                        "energy_pj": row.value,
                    }
                )
            if "excluded_energy_series" in selector:
                excluded = provincial.loc[
                    provincial["region"].eq(region)
                    & provincial["table_id"].eq(int(selector["provincial_table"]))
                    & provincial["raw_series"].eq(selector["excluded_energy_series"])
                ]
                if set(excluded["year"]) != set(range(periods[0], base_year + 1)):
                    raise ValueError(
                        f"Missing excluded air fuel audit for {(region, mode)}"
                    )
                excluded_by_year = excluded.set_index("year")["value"]
                for row in selected.itertuples(index=False):
                    excluded_pj = float(excluded_by_year.loc[int(row.year)])
                    total_pj = float(row.value) + excluded_pj
                    excluded_rows.append(
                        {
                            "ceud_region": region,
                            "mode": mode,
                            "year": int(row.year),
                            "excluded_energy_series": selector[
                                "excluded_energy_series"
                            ],
                            "excluded_energy_pj": excluded_pj,
                            "excluded_energy_share": excluded_pj / total_pj
                            if total_pj > 0
                            else 0.0,
                        }
                    )
    energy = pd.DataFrame(energy_rows)
    intensity = pd.DataFrame(intensity_rows).drop_duplicates(["mode", "year"])
    if len(intensity) != len(modes) * (base_year - periods[0] + 1):
        raise ValueError(
            "National intensity selection has incomplete mode/year coverage"
        )
    annual = estimate_demand_unit_capacity(energy, intensity)
    expected_regions = {
        rules["region_output_map"].get(region, region) for region in regions
    }
    if set(annual["region"]) != expected_regions:
        raise ValueError("Off-road region coverage mismatch")
    lifetime_rows = manual_lifetimes.loc[
        manual_lifetimes["category"].isin(modes)
        & manual_lifetimes["source -> data_source"].eq(source_selector)
    ]
    if (
        lifetime_rows.duplicated("category").any()
        or set(lifetime_rows["category"]) != modes
    ):
        raise ValueError(
            "Manual off-road lifetimes do not cover each mode exactly once"
        )
    lifetimes = dict(
        zip(lifetime_rows["category"], lifetime_rows["lifetime"], strict=True)
    )
    marine_mode = str(rules["marine_turnover_pool"])
    marine_annual = annual.loc[annual["mode"].eq(marine_mode)]
    if marine_annual["tech"].nunique() != 2:
        raise ValueError("Marine turnover pool must contain both incumbent fuels")
    marine_pool = marine_annual.groupby(
        ["region", "mode", "year", "units"], as_index=False
    )["capacity"].sum()
    marine_pool["tech"] = f"{marine_mode}_pool"
    turnover_annual = pd.concat(
        [annual.loc[annual["mode"].ne(marine_mode)], marine_pool],
        ignore_index=True,
    )
    pooled_cohorts = estimate_linear_cohort_additions(
        turnover_annual,
        lifetimes_by_mode=lifetimes,
        base_year=base_year,
        reconcile_annual_capacity=True,
    )
    marine_shares = marine_annual.loc[
        marine_annual["year"].eq(base_year), ["region", "mode", "tech", "capacity"]
    ].copy()
    marine_totals = marine_shares.groupby(["region", "mode"])["capacity"].transform(
        "sum"
    )
    marine_shares["base_year_fuel_share"] = marine_shares["capacity"].div(
        marine_totals.where(marine_totals.gt(0), 1.0)
    )
    marine_cohorts = (
        pooled_cohorts.loc[pooled_cohorts["mode"].eq(marine_mode)]
        .drop(columns="tech")
        .merge(
            marine_shares[["region", "mode", "tech", "base_year_fuel_share"]],
            on=["region", "mode"],
            how="left",
            validate="many_to_many",
        )
    )
    if marine_cohorts["tech"].isna().any():
        raise ValueError("Missing base-year marine fuel allocation")
    marine_cohorts["turnover_pool"] = marine_mode
    marine_cohorts["turnover_pool_annual_capacity"] = marine_cohorts[
        "estimated_annual_capacity"
    ]
    marine_cohorts = marine_cohorts.merge(
        marine_annual[["region", "mode", "tech", "year", "capacity"]].rename(
            columns={"year": "vintage_year", "capacity": "fuel_annual_capacity"}
        ),
        on=["region", "mode", "tech", "vintage_year"],
        how="left",
        validate="many_to_one",
    )
    if marine_cohorts["fuel_annual_capacity"].isna().any():
        raise ValueError("Missing annual marine fuel capacity audit")
    marine_cohorts["estimated_annual_capacity"] = marine_cohorts["fuel_annual_capacity"]
    marine_cohorts = marine_cohorts.drop(columns="fuel_annual_capacity")
    for column in (
        "capacity_additions",
        "capacity_at_base_year",
        "early_retired_capacity",
    ):
        marine_cohorts[column] *= marine_cohorts["base_year_fuel_share"]
    other_cohorts = pooled_cohorts.loc[pooled_cohorts["mode"].ne(marine_mode)].copy()
    other_cohorts["turnover_pool"] = other_cohorts["tech"]
    other_cohorts["turnover_pool_annual_capacity"] = other_cohorts[
        "estimated_annual_capacity"
    ]
    other_cohorts["base_year_fuel_share"] = 1.0
    cohorts = pd.concat([other_cohorts, marine_cohorts], ignore_index=True)
    cohorts = redistribute_eligible_offroad_cohorts(
        cohorts,
        annual.loc[
            annual["year"].eq(base_year), ["region", "mode", "tech", "capacity"]
        ],
        first_model_period=first_model_period,
    )
    cohorts["vintage"] = cohorts["vintage_year"].map(
        lambda year: next(period for period in periods if period >= year)
    )
    capacity = (
        cohorts.groupby(["region", "mode", "tech", "vintage", "units"], as_index=False)[
            "capacity_at_base_year"
        ]
        .sum()
        .rename(columns={"capacity_at_base_year": "capacity"})
    )
    return annual, cohorts, capacity, pd.DataFrame(excluded_rows)
