"""Road efficiency calculations on normalized evidence; no SQLite or downloads."""

from __future__ import annotations

import re
from functools import cache
from math import isfinite
from typing import Any

import numpy as np
import pandas as pd


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


def classify_ratings(
    frames: list[pd.DataFrame],
    epa: pd.DataFrame,
    *,
    rules: dict,
    conversions: dict,
    phev_contract: dict | None = None,
) -> pd.DataFrame:
    """Retain native rows; exact EPA hybrid evidence supplements the legacy name rule."""
    spec = rules["ratings"]
    fields = spec["fields"]

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
            frame["powertrain"] = pd.cut(
                frame[fields["bev_range"]],
                spec["bev_range_edges_km"],
                labels=spec["bev_range_labels"],
            ).astype("string")
            frame["native_consumption"] = pd.to_numeric(
                frame[fields["electricity"]], errors="raise"
            )
            frame["native_unit"] = "kWh/100 km"
            frame["consumption_mj_per_vkm"] = (
                frame.native_consumption
                * conversions["energy"]["kwh_to_mj"]
                * conversions["service"]["per_100_km_to_per_km"]
            )
            frame["classification_method"] = "legacy_range_bin"
        elif fields["phev_range"] in frame:
            if (
                phev_contract is None
                or phev_contract["combined_cd_parse"] != "leading_numeric_only"
            ):
                raise ValueError(
                    "NRCan PHEV requires the configured leading-number audit contract"
                )
            frame["powertrain"] = pd.cut(
                frame[fields["phev_range"]],
                spec["phev_range_edges_km"],
                labels=spec["phev_range_labels"],
            ).astype("string")
            frame["native_consumption"] = pd.to_numeric(
                frame[fields["consumption"]], errors="raise"
            )
            frame["native_unit"] = "L/100 km (CS; index only)"
            frame[phev_contract["combined_cs_output_numeric_field"]] = (
                frame.native_consumption
            )
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
            frame["consumption_mj_per_vkm"] = np.nan
            frame["classification_method"] = "legacy_range_bin_cs_index_only"
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
    return pd.concat(results, ignore_index=True).sort_values(
        ["component", "source_row"]
    )


def aggregate_ratings(
    classified: pd.DataFrame, weights: pd.DataFrame, *, rules: dict
) -> pd.DataFrame:
    """Average model ratings within classes, then reweight only observed classes."""
    weights = weights.loc[weights.weight_basis.eq(rules["rating_weight_basis"])].copy()
    if weights.empty or weights.report_year.nunique() != 1:
        raise ValueError("Expected one reviewed NRCan class-weight edition")
    if weights.duplicated(["nrcan_ceud_class", "nrcan_vehicle_class"]).any():
        raise ValueError("Duplicate NRCan aggregation weights")
    suv = rules["historical_suv"]
    annual = (
        classified.loc[classified.included]
        .groupby(["year", "powertrain", "rating_class"], sort=True)
        .agg(
            native_consumption=("native_consumption", "mean"),
            consumption_mj_per_vkm=("consumption_mj_per_vkm", "mean"),
            observations=("source_row", "size"),
            native_unit=("native_unit", "first"),
        )
        .reset_index()
    )
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
                        "original_weight": item.raw_weight,
                        "observed_weight_sum": total,
                        "weight": item.raw_weight / total,
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


def medium_vocation_weights(
    report4: pd.DataFrame, classes: list[str], *, rules: dict
) -> dict[str, float]:
    """MTO GVWR stock weights, equal non-bus/non-refuse vocations within each class."""
    selected = report4.loc[report4.EPA_GVWR.isin(rules["md_gvwr"])]
    if selected.EPA_GVWR.duplicated().any() or set(selected.EPA_GVWR) != set(
        rules["md_gvwr"]
    ):
        raise ValueError("Incomplete MTO medium GVWR counts")
    counts = {
        row.EPA_GVWR: positive(row.NATIVE_COUNT, "MTO GVWR count")
        for row in selected.itertuples()
    }
    result = {}
    for label, gvwr in rules["md_gvwr"].items():
        vocations = [
            c
            for c in classes
            if (m := re.match(rules["atb"]["class_pattern"], c))
            and int(m[1]) == gvwr
            and not any(excluded in c for excluded in rules["md_excluded_vocations"])
        ]
        if not vocations:
            raise ValueError(f"No ATB vocational coverage for {label}")
        result.update(
            {
                c: counts[label] / sum(counts.values()) / len(vocations)
                for c in vocations
            }
        )
    return result


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


def backcast_efficiency(
    anchor_efficiency: float, anchor_consumption: float, historic_consumption: float
) -> float:
    """Higher historical fuel consumption implies lower historical efficiency."""
    return (
        positive(anchor_efficiency, "anchor efficiency")
        * positive(anchor_consumption, "anchor consumption")
        / positive(historic_consumption, "historic consumption")
    )


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
    ):
        self.ceud, self.loads, self.rules = ceud, loads, rules
        self.base_year, self.gcam, self.regen = base_year, gcam, regen
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
        self.derive = cache(self.derive)

    def atb_value(self, region: str, mode: str, powertrain: str, year: int) -> float:
        series = self.atb.loc[
            self.atb.region.eq(region)
            & self.atb["mode"].eq(mode)
            & self.atb.powertrain.eq(powertrain)
        ]
        return interpolate(series, year, "weighted_consumption")

    def rating(
        self, mode: str, powertrain: str, year: int, column: str
    ) -> float | None:
        weights = self.rules["road_classes"][mode]["weights"]
        series = self.ratings.loc[
            self.ratings.ceud_class.eq(weights) & self.ratings.powertrain.eq(powertrain)
        ]
        if series.empty or year < series.year.min() or year > series.year.max():
            return None
        if series[column].isna().any():
            return None
        return interpolate(series, year, column)

    def historical_rating_ratio(
        self, mode: str, powertrain: str, year: int, anchor: int
    ) -> tuple[float, str]:
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

    def derive(self, region: str, mode: str, powertrain: str, year: int) -> dict:
        spec, base = self.rules["road_classes"][mode], self.base_year
        pathway = spec["pathway"]
        load = interpolate(
            self.loads.loc[self.loads.region.eq(region) & self.loads["mode"].eq(mode)],
            min(year, base),
            "load_factor",
        )
        native, native_unit, index, analogue = None, "MJ/vkm", 1.0, powertrain
        if pathway == "ldv":
            if powertrain.startswith("phev"):
                baseline = self.atb_value(region, mode, powertrain, base)
                if year <= base:
                    index, analogue = self.historical_rating_ratio(
                        mode, powertrain, year, base
                    )
                    consumption = baseline * index
                else:
                    consumption = self.atb_value(region, mode, powertrain, year)
                treatment = "ATB_PHEV_level_NRCan_CS_or_analogue_history"
            elif (
                year <= base
                and (observed := self.rating(mode, powertrain, year, "mj")) is not None
            ):
                consumption, treatment = observed, "NRCan_observed_class_weighted"
            else:
                baseline, analogue = self.ldv_baseline(region, mode, powertrain)
                if year > base:
                    index = self.atb_value(
                        region, mode, powertrain, year
                    ) / self.atb_value(region, mode, powertrain, base)
                    treatment = "NRCan_baseline_ATB_consumption_ratio"
                else:
                    index, analogue = self.historical_rating_ratio(
                        mode, powertrain, year, base
                    )
                    treatment = "NRCan_missing_year_approved_analogue_index"
                consumption = baseline * index
            efficiency = load / consumption
            native = consumption
        elif pathway == "mhdv":
            anchor = self.atb_value(
                region, mode, powertrain, base if year <= base else year
            )
            if year <= base:
                fuel = self.rules["historical_index_fuels"][mode][powertrain]
                series = ceud_series(
                    self.ceud.loc[self.ceud.region.eq(region)],
                    spec["stock_table"],
                    spec["cs_prefix"] + "|" + self.rules["fuel_series"][fuel],
                    "L/100 km",
                )
                index = interpolate(series, year, "value") / interpolate(
                    series, base, "value"
                )
                analogue = fuel
            consumption = anchor * index
            efficiency, native = load / consumption, consumption
            treatment = "ATB_direct_or_CEUD_consumption_backcast"
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
                index = interpolate(
                    reference, source_year, "source_value", year_col="source_year"
                ) / interpolate(
                    target, source_year, "source_value", year_col="source_year"
                )
                consumption = (
                    self.atb_value(region, selected_mode, "diesel", source_year) * index
                )
            else:
                consumption = self.atb_value(
                    region, selected_mode, powertrain, source_year
                )
            efficiency = load / consumption
            if year <= base:
                series = ceud_series(
                    self.ceud.loc[self.ceud.region.eq(region)],
                    spec["intensity_table"],
                    spec["intensity"],
                    "MJ/Pkm",
                )
                historic, reference = (
                    interpolate(series, year, "value"),
                    interpolate(series, base, "value"),
                )
                base_load = interpolate(
                    self.loads.loc[
                        self.loads.region.eq(region) & self.loads["mode"].eq(mode)
                    ],
                    base,
                    "load_factor",
                )
                efficiency = backcast_efficiency(
                    base_load / consumption, reference, historic
                )
                index = historic / reference
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
            intensity = interpolate(series, min(year, base), "value")
            if year > base:
                source = self.gcam.loc[self.gcam.source_variable.eq("intensity")]
                if set(source.source_unit) != {"MJ/vkm"}:
                    raise ValueError("Unexpected GCAM motorcycle units")
                incumbent = source.loc[
                    source.source_technology.eq(self.rules["motorcycle"]["gasoline"])
                ]
                target = source.loc[
                    source.source_technology.eq(self.rules["motorcycle"][powertrain])
                ]
                index = interpolate(
                    target, year, "source_value", year_col="source_year"
                ) / interpolate(incumbent, base, "source_value", year_col="source_year")
            efficiency, native, native_unit = (
                1 / (intensity * index),
                intensity,
                "MJ/Pkm",
            )
            treatment = "CEUD_service_intensity_GCAM_vehicle_intensity_index"
        else:
            raise ValueError(f"Unsupported road pathway: {pathway}")
        if analogue != powertrain:
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
            "efficiency": positive(efficiency, "service efficiency"),
            "load_factor": load,
            "native_intensity": native,
            "native_unit": native_unit,
            "consumption_index": index,
            "analogue": analogue,
            "treatment": treatment,
        }
