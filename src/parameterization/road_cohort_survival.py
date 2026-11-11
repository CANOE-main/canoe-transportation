"""Pure annual survival reconciliation and acquisition-weighted cohort arithmetic.

Source schedules remain evidence. The approved constant-retirement tail and any
explicit age cap are applied without writing parameter artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil, isfinite, log

import pandas as pd


# Current Temoa create_survival_curve rejects supplied interior fractions below
# this value. This is an upstream interface limit, not a chosen retirement rate.
TEMOA_MIN_INTERIOR_SURVIVAL = 0.0001


@dataclass(frozen=True)
class ReconciledSurvival:
    annual: pd.DataFrame
    audit: pd.DataFrame


@dataclass(frozen=True)
class CompositeCohorts:
    capacity: pd.DataFrame
    curves: pd.DataFrame
    lifetimes: pd.DataFrame
    physical_stock: pd.DataFrame
    audit: pd.DataFrame
    tail_losses: pd.DataFrame


def _validate_schedule(frame: pd.DataFrame, *, identity: str) -> pd.DataFrame:
    required = {"age", "survival_probability"}
    if not required <= set(frame) or frame.empty:
        raise ValueError(f"Missing annual survival schedule: {identity}")
    ordered = frame.sort_values("age").copy()
    ages = pd.to_numeric(ordered.age, errors="raise")
    values = pd.to_numeric(ordered.survival_probability, errors="raise")
    if (ages.isna().any() or ages.mod(1).ne(0).any()
            or ages.tolist() != list(range(int(ages.max()) + 1))):
        raise ValueError(f"Annual survival ages must be complete, unique integers from zero: {identity}")
    if (values.map(lambda value: not isfinite(value) or not 0 <= value <= 1).any()
            or values.iloc[0] != 1 or values.diff().dropna().gt(1e-12).any()):
        raise ValueError(f"Survival must begin at one and be finite, bounded and nonincreasing: {identity}")
    ordered["age"] = ages.astype(int)
    ordered["survival_probability"] = values.astype(float)
    return ordered


def reconcile_survival_schedules(
    selected: pd.DataFrame,
    *,
    tail_derivation: str,
    threshold: float,
    hard_zero_age: int | None = None,
) -> ReconciledSurvival:
    """Extend the last observed annual retirement rate, then end at zero.

    Native points are retained unless an explicit hard zero age removes them.
    Continue the last conditional retention until the first value at or below
    ``threshold``; retain that positive value and put zero in the next year.
    A derived positive value below Temoa's numerical floor instead becomes the
    zero endpoint. The optional age cap can only shorten the derived lifetime.
    """
    if tail_derivation != "constant_retirement":
        raise ValueError("Road survival tail_derivation must be constant_retirement")
    if not isfinite(threshold) or not TEMOA_MIN_INTERIOR_SURVIVAL <= threshold < 1:
        raise ValueError("Survival tail threshold must be finite and supported by Temoa")
    if (hard_zero_age is not None and (isinstance(hard_zero_age, bool)
            or int(hard_zero_age) != hard_zero_age or hard_zero_age <= 0)):
        raise ValueError("The survival hard zero age must be a positive integer or null")
    if "road_class" not in selected or selected.empty:
        raise ValueError("Selected road-class survival schedules are required")
    annual_frames, audits = [], []
    for road_class, group in selected.groupby("road_class", sort=True):
        source = _validate_schedule(group, identity=str(road_class))
        last_age = int(source.age.iloc[-1])
        last_survival = float(source.survival_probability.iloc[-1])
        native_max_age = int(source.get("native_max_age", pd.Series([last_age])).iloc[0])
        previous = float(source.survival_probability.iloc[-2]) if len(source) > 1 else None
        retention = last_survival / previous if previous is not None and previous > 0 else None
        if last_survival == 0:
            natural_terminal = last_age
        elif last_survival <= threshold:
            natural_terminal = last_age + 1
        elif retention is not None and 0 < retention < 1:
            steps = max(1, ceil(log(threshold / last_survival) / log(retention)))
            if last_survival * retention ** steps > threshold:
                steps += 1
            natural_terminal = last_age + steps + 1
        elif hard_zero_age is not None:
            natural_terminal = int(hard_zero_age)
        else:
            raise ValueError(f"Cannot derive a finite survival tail from the last annual retirement rate: {road_class}")
        terminal_age = min(natural_terminal, int(hard_zero_age)) if hard_zero_age is not None else natural_terminal
        result = source.loc[source.age.lt(terminal_age)].copy()
        result["source_survival_probability"] = result.survival_probability
        result["treatment"] = "source_supported"
        if result.survival_probability.lt(TEMOA_MIN_INTERIOR_SURVIVAL).any():
            raise ValueError(f"Native positive survival is below Temoa's supported minimum: {road_class}; "
                             "preserving these observations requires an explicit earlier age cap")
        extension = []
        numerical_zero = False
        for age in range(last_age + 1, terminal_age):
            if retention is None or not 0 < retention <= 1:
                raise ValueError(f"Cannot extend the final annual retirement rate: {road_class}")
            value = last_survival * retention ** (age - last_age)
            if value < TEMOA_MIN_INTERIOR_SURVIVAL:
                terminal_age = age
                numerical_zero = True
                break
            row = source.iloc[-1].to_dict()
            row.update(age=age, survival_probability=value, source_survival_probability=None,
                       treatment="constant_retirement")
            extension.append(row)
        if extension:
            result = pd.concat([result, pd.DataFrame(extension)], ignore_index=True)
        endpoint = source.iloc[-1].to_dict()
        source_endpoint = source.loc[source.age.eq(terminal_age), "survival_probability"]
        endpoint.update(age=terminal_age, survival_probability=0.0,
                        source_survival_probability=float(source_endpoint.iloc[0]) if not source_endpoint.empty else None,
                        treatment=("hard_zero_age" if hard_zero_age == terminal_age
                                   else "temoa_zero_endpoint" if numerical_zero else "threshold_zero_endpoint"))
        result = pd.concat([result, pd.DataFrame([endpoint])], ignore_index=True)
        result = _validate_schedule(result, identity=str(road_class))
        result["terminal_age"] = terminal_age
        result["tail_derivation"] = tail_derivation
        result["threshold"] = threshold
        result["hard_zero_age"] = hard_zero_age
        known = source.set_index("age").survival_probability
        treated = result.set_index("age").survival_probability.reindex(known.index, fill_value=0.0)
        last_positive = result.iloc[-2]
        audits.append({
            "road_class": road_class,
            "source_id": str(source.source_id.iloc[0]) if "source_id" in source else "",
            "source_class": str(source.source_class.iloc[0]) if "source_class" in source else "",
            "native_max_age": native_max_age,
            "known_survival_max_age": last_age,
            "source_terminal_survival": last_survival,
            "tail_derivation": tail_derivation,
            "threshold": threshold,
            "hard_zero_age": hard_zero_age,
            "terminal_age": terminal_age,
            "uncapped_terminal_age": natural_terminal,
            "terminal_retention": retention,
            "terminal_retirement_rate": None if retention is None else 1 - retention,
            "retained_threshold_age": int(last_positive.age) if last_positive.survival_probability <= threshold else None,
            "retained_threshold_fraction": float(last_positive.survival_probability) if last_positive.survival_probability <= threshold else None,
            "removed_source_survival_years": float((known - treated).sum()),
            "added_tail_survival_years": float(result.loc[result.age.gt(last_age), "survival_probability"].sum()),
            "temoa_compatible": True,
        })
        annual_frames.append(result)
    return ReconciledSurvival(pd.concat(annual_frames, ignore_index=True), pd.DataFrame(audits))


def build_composite_cohorts(
    annual_acquisitions: pd.DataFrame,
    annual_survival: pd.DataFrame,
    *,
    reference_year: int,
    first_model_period: int,
) -> CompositeCohorts:
    """Preserve annual survivors using an equivalent historical model cohort.

    For pre-acquisition years only, component survival is held at one. This
    defines an equivalent synthetic vintage; it does not assert historical stock
    before acquisition. Right-labelled bins are normalized by survivors at that
    synthetic vintage. Grouping preserves the harmonized annual survivors until
    their fraction falls below Temoa's supported minimum. That necessary earlier
    final retirement is recorded separately from physical stock.
    """
    keys = ["region", "road_class", "tech", "vintage_year", "vintage"]
    amount = "calibrated_acquisitions_k_vehicles"
    if not set([*keys, amount]) <= set(annual_acquisitions):
        raise ValueError("Annual acquisitions lack the grouped-cohort contract")
    inputs = annual_acquisitions.reset_index(drop=True).copy()
    if (inputs.duplicated(["region", "tech", "vintage_year", "vintage"]).any()
            or inputs[keys].isna().any().any()
            or inputs[amount].map(lambda value: not isfinite(value) or value < 0).any()):
        raise ValueError("Annual acquisitions must have unique keys and finite nonnegative amounts")
    for column in ("vintage_year", "vintage"):
        if inputs[column].mod(1).ne(0).any():
            raise ValueError("Annual and model vintages must be integers")
        inputs[column] = inputs[column].astype(int)
    if reference_year > first_model_period or inputs.vintage_year.gt(reference_year).any():
        raise ValueError("Historical acquisitions must precede the reference/model boundary")
    if inputs.vintage.ge(first_model_period).any():
        raise ValueError("Historical model vintages must precede the first model period")
    schedules = {}
    for road_class in sorted(set(inputs.road_class)):
        schedule = _validate_schedule(annual_survival.loc[annual_survival.road_class.eq(road_class)], identity=road_class)
        if schedule.survival_probability.iloc[-1] != 0:
            raise ValueError(f"Grouped sales survival needs an explicit zero endpoint: {road_class}")
        if schedule.survival_probability.iloc[:-1].le(0).any():
            raise ValueError(f"Grouped sales survival has a zero before its endpoint: {road_class}")
        schedules[road_class] = schedule.set_index("age").survival_probability

    def survival_at(schedule: pd.Series, year: int, acquisition_year: int) -> float:
        age = max(0, int(year) - int(acquisition_year))
        return float(schedule.loc[age]) if age <= int(schedule.index[-1]) else 0.0

    inputs["survival_at_reference"] = [survival_at(schedules[row.road_class], reference_year, row.vintage_year)
        for row in inputs.itertuples(index=False)]
    inputs["physical_stock_k_vehicles"] = inputs[amount] * inputs.survival_at_reference
    inputs["solver_reference_stock_k_vehicles"] = inputs.physical_stock_k_vehicles
    inputs["grouped_tail_reference_loss_k_vehicles"] = 0.0
    inputs["installed_equivalent_k_vehicles"] = [
        row.calibrated_acquisitions_k_vehicles * survival_at(schedules[row.road_class], row.vintage, row.vintage_year)
        for row in inputs.itertuples(index=False)
    ]
    capacities, curves, lifetimes, audits, tail_losses = [], [], [], [], []
    for (region, road_class, tech, vintage), group in inputs.groupby(["region", "road_class", "tech", "vintage"], sort=True):
        schedule = schedules[road_class]
        positive = group.loc[group[amount].gt(0)]
        if positive.empty:
            continue
        cohort_records = tuple(positive.itertuples(index=False))

        def stock_at(year: int) -> float:
            return float(sum(row.calibrated_acquisitions_k_vehicles * survival_at(schedule, year, row.vintage_year)
                             for row in cohort_records))
        original = float(positive[amount].sum())
        source_initial = stock_at(first_model_period)
        basis = stock_at(vintage)
        native_end = int(positive.vintage_year.max()) + int(schedule.index[-1])
        end = native_end
        annual = [(year, stock_at(year) / basis) for year in range(int(vintage), end + 1)] if basis > 0 else []
        crossing = next((year for year, value in annual if 0 < value < TEMOA_MIN_INTERIOR_SURVIVAL), None)
        if crossing is not None:
            end = crossing
        losses = [{"region": region, "road_class": road_class, "tech": tech, "vintage": int(vintage),
                   "period": year, "removed_k_vehicles": value * basis}
                  for year, value in annual if year >= end and value > 0]
        tail_losses.extend(losses)
        reference_loss = float(group.physical_stock_k_vehicles.sum()) if reference_year >= end else 0.0
        first_model_loss = source_initial if first_model_period >= end else 0.0
        if reference_loss:
            inputs.loc[group.index, "solver_reference_stock_k_vehicles"] = 0.0
            inputs.loc[group.index, "grouped_tail_reference_loss_k_vehicles"] = group.physical_stock_k_vehicles
        initial = source_initial - first_model_loss
        record = {"region": region, "road_class": road_class, "tech": tech, "vintage": int(vintage),
                  "acquisitions_k_vehicles": original, "capacity_basis_k_vehicles": basis,
                  "reference_stock_k_vehicles": float(group.physical_stock_k_vehicles.sum()),
                  "first_model_stock_k_vehicles": initial, "terminal_year": end,
                  "native_group_terminal_year": native_end,
                  "required_minimum_survival": TEMOA_MIN_INTERIOR_SURVIVAL,
                  "removed_reference_stock_k_vehicles": reference_loss,
                  "removed_first_model_stock_k_vehicles": first_model_loss,
                  "removed_stock_years_k_vehicles": sum(row["removed_k_vehicles"] for row in losses),
                  "normalization": "original_acquisitions" if positive.vintage_year.ge(vintage).all() else "survivors_at_synthetic_vintage"}
        if initial == 0:
            audits.append({**record, "status": "retired_before_model", "temoa_compatible": True,
                           "minimum_interior_fraction": None})
            continue
        if basis <= 0 or end <= first_model_period:
            raise ValueError(f"Positive surviving cohort has an invalid installation basis/endpoint: {(region, tech, vintage)}")
        annual = [(year, value) for year, value in annual if year < end] + [(end, 0.0)]
        if annual[0][1] != 1 or annual[-1][1] != 0:
            raise ValueError("Grouped survival endpoint arithmetic failed")
        minimum = min(fraction for _, fraction in annual[:-1])
        compatible = minimum >= TEMOA_MIN_INTERIOR_SURVIVAL
        capacities.append({"region": region, "road_class": road_class, "tech": tech,
                           "vintage": int(vintage), "capacity": basis})
        lifetimes.append({"region": region, "tech": tech, "vintage": int(vintage), "lifetime": end - int(vintage)})
        curves.extend({"region": region, "tech": tech, "vintage": int(vintage), "period": year, "fraction": fraction}
                      for year, fraction in annual)
        audits.append({**record, "status": "active", "temoa_compatible": compatible,
                       "minimum_interior_fraction": minimum})
    return CompositeCohorts(
        pd.DataFrame(capacities, columns=["region", "road_class", "tech", "vintage", "capacity"]),
        pd.DataFrame(curves, columns=["region", "tech", "vintage", "period", "fraction"]),
        pd.DataFrame(lifetimes, columns=["region", "tech", "vintage", "lifetime"]),
        inputs,
        pd.DataFrame(audits),
        pd.DataFrame(tail_losses, columns=["region", "road_class", "tech", "vintage", "period", "removed_k_vehicles"]),
    )
