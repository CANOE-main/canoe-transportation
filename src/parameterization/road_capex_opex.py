"""Road purchase and maintenance cost transformations from normalized evidence."""

from __future__ import annotations

from math import exp, isfinite

import pandas as pd

from parameterization.road_efficiencies import interpolate


def select_nlr_purchase_prices(
    vehicles: pd.DataFrame,
    *,
    trajectory: str,
    efficiency_rules: dict,
    rpe_markup: float,
    source_currency: str,
    source_dollar_year: int,
) -> pd.DataFrame:
    """Preserve ATB MSRP and reverse RPE on selected road vehicle prices only."""
    if not isfinite(rpe_markup) or rpe_markup <= 1:
        raise ValueError("NLR road RPE markup must be finite and greater than one")
    required = {
        "year", "vehicle_weight_category", "vehicle_class", "vehicle_powertrain",
        "vehicle_detail", "metric_key", "value", "unit", "trajectory", "source_id",
    }
    if not required <= set(vehicles):
        raise ValueError(f"NLR vehicle-price fields missing: {sorted(required - set(vehicles))}")
    source = vehicles.loc[
        vehicles["trajectory"].eq(trajectory)
        & vehicles["metric_key"].eq("modeled_vehicle_price")
    ].copy()
    expected_unit = f"{source_dollar_year} {source_currency}/vehicle"
    if source.empty or set(source["unit"]) != {expected_unit}:
        raise ValueError(f"Selected ATB vehicle-price evidence lacks {expected_unit}")
    records: list[dict] = []
    for family, archetypes in (
        ("ldv", efficiency_rules["ldv_archetypes"]),
        ("mhdv", efficiency_rules["mhdv_archetypes"]),
    ):
        weight_category = efficiency_rules["atb"]["weight_categories"][family]
        for powertrain, spec in archetypes.items():
            # An efficiency-only fuel proxy is not a reviewed purchase-price basis.
            if "proxy" in spec:
                continue
            selected = source.loc[
                source["vehicle_weight_category"].eq(weight_category)
            ]
            if "range_mi" in spec:
                selected = selected.loc[
                    selected["vehicle_detail"].str.endswith(
                        f"({int(spec['range_mi'])}-mile electric range)"
                    )
                ]
            else:
                selected = selected.loc[
                    selected["vehicle_detail"].eq(spec["detail"])
                ]
            if selected.empty or selected.duplicated(["year", "vehicle_class"]).any():
                raise ValueError(
                    f"Missing or ambiguous ATB purchase-price archetype {family}/{powertrain}"
                )
            numeric_prices = pd.to_numeric(selected["value"], errors="coerce")
            if numeric_prices.isna().any() or (~numeric_prices.map(isfinite)).any() or (
                numeric_prices <= 0
            ).any():
                raise ValueError(f"Invalid ATB purchase prices for {family}/{powertrain}")
            for item in selected.itertuples(index=False):
                records.append(
                    {
                        "family": family,
                        "powertrain": powertrain,
                        "vehicle_class": item.vehicle_class,
                        "vehicle_detail": item.vehicle_detail,
                        "source_year": int(item.year),
                        "trajectory": trajectory,
                        "source_id": item.source_id,
                        "source_currency": source_currency,
                        "source_dollar_year": source_dollar_year,
                        "source_unit": item.unit,
                        "vehicle_price_usd_2022_per_vehicle": float(item.value),
                        "rpe_markup": rpe_markup,
                        "manufacturing_cost_usd_2022_per_vehicle": float(item.value)
                        / rpe_markup,
                    }
                )
    result = pd.DataFrame(records)
    if result.duplicated(["family", "powertrain", "vehicle_class", "source_year"]).any():
        raise ValueError("Duplicate selected ATB purchase-price keys")
    return result.sort_values(
        ["family", "powertrain", "vehicle_class", "source_year"]
    ).reset_index(drop=True)


def aggregate_purchase_price(
    prices: pd.DataFrame, *, powertrain: str, year: int, weights: dict[str, float]
) -> tuple[float, float]:
    """Use the same reviewed class weights for both RPE and manufacturing price."""
    if abs(sum(weights.values()) - 1) > 1e-9 or any(w < 0 for w in weights.values()):
        raise ValueError("Road purchase-cost class weights do not sum to one")
    selected = prices.loc[
        prices.powertrain.eq(powertrain) & prices.vehicle_class.isin(weights)
    ]
    if set(selected.vehicle_class) != set(weights):
        raise ValueError(f"Incomplete road purchase-cost classes for {powertrain}")
    values = []
    for vehicle_class, weight in weights.items():
        series = selected.loc[selected.vehicle_class.eq(vehicle_class)]
        msrp = interpolate(series, year, "vehicle_price_usd_2022_per_vehicle", year_col="source_year")
        manufacturing = interpolate(
            series, year, "manufacturing_cost_usd_2022_per_vehicle", year_col="source_year"
        )
        values.append((weight * msrp, weight * manufacturing))
    return sum(v[0] for v in values), sum(v[1] for v in values)


def ldv_repair_per_mile(
    *, age: int, msrp: float, vehicle_class: str, powertrain: str,
    baseline: pd.DataFrame, class_multipliers: pd.DataFrame,
    powertrain_multipliers: pd.DataFrame, maintenance: pd.DataFrame,
    powertrain_labels: dict[str, str], price_exponent: float, max_age: int,
) -> dict[str, float]:
    """Burnham calibrated repair equation uses retail MSRP, never de-marked CAPEX."""
    if age < 0 or msrp <= 0 or price_exponent <= 0:
        raise ValueError("Invalid LDV repair age, MSRP, or price exponent")
    if powertrain not in powertrain_labels:
        raise ValueError(f"Missing Burnham powertrain mapping: {powertrain}")
    label = powertrain_labels[powertrain]
    def one(frame: pd.DataFrame, mask: pd.Series, column: str) -> float:
        selected = frame.loc[mask]
        if len(selected) != 1:
            raise ValueError(f"Missing or ambiguous Burnham coefficient {column}")
        return float(selected.iloc[0][column])
    baseline_value = one(
        baseline, baseline.year_index.eq(min(age, max_age)), "baseline_repair_cost($/mi)"
    )
    class_factor = one(
        class_multipliers, class_multipliers.vehicle_class.eq(vehicle_class), "class_multiplier"
    )
    powertrain_factor = one(
        powertrain_multipliers,
        powertrain_multipliers.vehicle_powertrain.eq(label), "powertrain_multiplier",
    )
    maintenance_value = one(
        maintenance, maintenance.vehicle_powertrain.eq(label), "maintenance_cost($/mi)"
    )
    repair = class_factor * powertrain_factor * baseline_value * exp(price_exponent * msrp)
    if any(not isfinite(v) or v < 0 for v in (repair, maintenance_value)):
        raise ValueError("Invalid Burnham maintenance and repair result")
    return {"repair": repair, "maintenance": maintenance_value, "total": repair + maintenance_value}


def bean_maintenance_per_mile(
    coefficients: pd.DataFrame, *, vehicle_class: str, powertrain: str, age: int
) -> dict[str, float]:
    """ANL BEAN class/powertrain age model in source currency per mile."""
    if age < 0:
        raise ValueError("Negative BEAN vehicle age")
    selected = coefficients.loc[
        coefficients.vehicle_class.eq(vehicle_class)
        & coefficients.vehicle_powertrain.eq(powertrain)
    ]
    if set(selected.coefficient_key) != {
        "coefficient_a", "coefficient_b", "maintenance_powertrain_multiplier"
    } or selected.coefficient_key.duplicated().any():
        raise ValueError(f"Incomplete BEAN coefficients for {vehicle_class}/{powertrain}")
    values = selected.set_index("coefficient_key").value.astype(float)
    cost = values["maintenance_powertrain_multiplier"] * (
        values["coefficient_a"] * age + values["coefficient_b"]
    )
    if not isfinite(cost) or cost < 0:
        raise ValueError("Invalid BEAN cost")
    return {"source_cost_per_mile": float(cost), "age": age}
