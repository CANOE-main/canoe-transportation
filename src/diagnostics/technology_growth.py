"""Reproducible, opt-in technology growth evidence and calibration diagnostics.

No production parameter preparation, growth schema rows or SQLite connections live
here. The notebook and CLI use this one preparation path. SciPy is a dev dependency.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from typing import Literal

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from scipy.optimize import least_squares
from scipy.special import expit

from fetching.technology_growth_evidence import (
    acquire_evidence,
    asset_path,
    extract_paper_pages,
    normalize_eafo,
    normalize_hatch,
    registered_assets,
)
from utils import (
    ConfigBundle,
    file_sha256,
    load_config_bundle,
    load_harmonization_rules,
    load_yaml,
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)

LOGGER = logging.getLogger(__name__)


class ResearchModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CurationControls(ResearchModel):
    min_points: int = Field(ge=3)
    legacy_min_points: int = Field(ge=3)
    legacy_max_gap: int = Field(ge=0)
    legacy_countries: list[str]
    legacy_metrics: list[str]
    legacy_removed_ids: list[str]
    legacy_removed_technologies: list[str]
    legacy_clipped_technologies: list[str]
    legacy_clip_value: float = Field(gt=0)


class FitControls(ResearchModel):
    countries: list[str]
    technologies: list[str]
    window_spans: list[int]
    minimum_window_points: int = Field(ge=3)
    max_missing_window_years: int = Field(ge=0)
    max_nfev: int = Field(gt=0)
    k_upper: float = Field(gt=0, allow_inf_nan=False)
    fit_r2_review: float = Field(ge=0, le=1)
    published_r2_review: float = Field(ge=0, le=1)
    published_k_upper: float = Field(gt=0)
    ceiling_multipliers: list[float]
    legacy_ceiling_lower: float = Field(gt=0, le=1)
    broad_ceiling_lower: float = Field(gt=0, le=1)
    inflection_margin_years: int = Field(ge=0)
    start_k: list[float]
    truncation_years: int = Field(ge=1)

    @model_validator(mode="after")
    def bounds(self):
        if any(x <= 1 or not math.isfinite(x) for x in self.ceiling_multipliers):
            raise ValueError("Ceiling multipliers must exceed one")
        if any(x <= 0 for x in self.window_spans):
            raise ValueError("Window spans must be positive year intervals")
        if any(not 0 < x < self.k_upper for x in self.start_k):
            raise ValueError("Starting k must be strictly inside the optimizer bounds")
        return self


class CalibrationControls(ResearchModel):
    maturity_match_factor: float = Field(gt=1)
    candidate_quantiles: list[float]
    seed_batch_vehicles: list[float]
    seed_class_stock_fractions: list[float]
    stress_annual_rates: list[float]
    historical_period_width: int = Field(ge=1)
    horizon_year: int
    illustrative_ceiling_multiples: list[float]

    @model_validator(mode="after")
    def valid_grids(self):
        if any(not 0 <= x <= 1 for x in self.candidate_quantiles):
            raise ValueError("Invalid candidate quantile")
        for values in [
            self.seed_batch_vehicles,
            self.seed_class_stock_fractions,
            self.stress_annual_rates,
        ]:
            if any(x < 0 or not math.isfinite(x) for x in values):
                raise ValueError(
                    "Seed/rate stress grids must be finite and nonnegative"
                )
        if any(x <= 1 for x in self.illustrative_ceiling_multiples):
            raise ValueError("Illustrative ceiling must exceed initial stock")
        return self


class TargetClass(ResearchModel):
    prefix: str
    powertrains: list[Literal["bev", "phev", "hev", "fcev", "cng", "diesel"]]
    norway_class: Literal["M1", "N1", "N2_N3"]
    legacy_bev: str


class GrowthDiagnosticConfig(ResearchModel):
    version: Literal[1]
    diagnostic_only: Literal[True]
    scenario: str
    snapshot_year: int
    legacy_notebook: str
    source_assets: dict[str, str]
    curation: CurationControls
    metric_families: dict[str, str]
    fitting: FitControls
    calibration: CalibrationControls
    fuel_names: dict[str, str]
    norway_powertrains: dict[str, str]
    target_classes: dict[str, TargetClass]
    analogue_rationale: dict[str, str]

    @model_validator(mode="after")
    def contracts(self):
        if set(self.source_assets) != {
            "original",
            "extended",
            "extended_estimates",
            "descriptions",
            "original_estimates",
        }:
            raise ValueError("Incomplete growth source asset contract")
        if set(self.target_classes) != {
            "cars",
            "passenger_light_trucks",
            "freight_light_trucks",
            "medium_trucks",
            "heavy_trucks",
        }:
            raise ValueError(
                "Growth targets must remain within legacy's five road classes"
            )
        if sum(len(x.powertrains) for x in self.target_classes.values()) != 24:
            raise ValueError("Legacy application has 24 class-powertrain targets")
        return self


def load_diagnostic_config(bundle: ConfigBundle) -> GrowthDiagnosticConfig:
    return GrowthDiagnosticConfig.model_validate(
        load_yaml(resolve_parameter_path(bundle, "technology_growth_diagnostic.yaml"))
    )


def annual_to_period(annual_rate: float, years: float) -> float:
    """Discrete fractional annual growth to fractional transition growth."""
    if (
        not math.isfinite(annual_rate)
        or annual_rate <= -1
        or not math.isfinite(years)
        or years <= 0
    ):
        raise ValueError("Require finite annual rate > -1 and positive interval")
    return math.expm1(math.log1p(annual_rate) * years)


def annual_seed_to_period(seed: float, annual_rate: float, years: int) -> float:
    """Equal annual recurrences aggregated over integer years, in capacity units.

    This is an equivalence demonstration, not Temoa's native seed interpretation.
    Temoa applies its supplied seed once per model transition.
    """
    if seed < 0 or not math.isfinite(seed) or not isinstance(years, int):
        raise ValueError("Require a nonnegative finite seed and integer years")
    rate = annual_to_period(annual_rate, years)
    return seed * years if annual_rate == 0 else seed * rate / annual_rate


def s_curve(years, ceiling: float, k: float, midpoint: float, family: str = "logistic"):
    z = k * (np.asarray(years, dtype=float) - midpoint)
    if family == "logistic":
        return ceiling * expit(z)
    if family == "gompertz":
        return ceiling * np.exp(-np.exp(np.clip(-z, -700, 700)))
    raise ValueError(f"Unknown curve family: {family}")


def max_missing_run(values: pd.Series) -> int:
    missing = values.isna()
    groups = missing.ne(missing.shift()).cumsum()
    return int(missing.groupby(groups).sum().max()) if len(values) else 0


def audit_series(
    observations: pd.DataFrame, config: GrowthDiagnosticConfig
) -> pd.DataFrame:
    rows = []
    replacements = {
        "Annual production": "Annual Production",
        "Cumulative total capacity": "Cumulative Total Capacity",
        "Net Total Capacity": "Cumulative Total Capacity",
        "Installed electricity capacity": "Installed Capacity",
        "Total Length": "Cumulative Length",
    }
    for sid, data in observations.groupby("series_id", sort=True):
        data = data.sort_values("year")
        first = data.iloc[0]
        v = data.value
        positive = data.loc[v.gt(0)]
        pos_span = (
            data.loc[data.year.between(positive.year.min(), positive.year.max())]
            if len(positive)
            else data.iloc[:0]
        )
        legacy_gap = max_missing_run(pos_span.value.mask(pos_span.value.eq(0)))
        metric = replacements.get(first.metric, first.metric)
        if metric == "Cumulative Total Capacity" and first.unit == "kilometers":
            metric = "Cumulative Length"
        if metric == "Cumulative Total Capacity" and first.unit == "-":
            metric = "Total Number"
        reasons = []
        if first.country not in config.curation.legacy_countries:
            reasons.append("geography")
        if len(positive) < config.curation.legacy_min_points:
            reasons.append("positive_length")
        if legacy_gap > config.curation.legacy_max_gap:
            reasons.append("gap")
        if metric not in config.curation.legacy_metrics:
            reasons.append("metric")
        if first.native_id in config.curation.legacy_removed_ids:
            reasons.append("explicit_id")
        if first.technology in config.curation.legacy_removed_technologies:
            reasons.append("explicit_technology")
        family = config.metric_families.get(first.metric, "unclassified_review")
        rows.append(
            {
                "series_id": sid,
                "release": first.release,
                "native_id": first.native_id,
                "technology": first.technology,
                "country": first.country,
                "metric": first.metric,
                "unit": first.unit,
                "native_source": first.native_source,
                "variable": first.variable,
                "raw_row": int(first.raw_row),
                "quantity_family": family,
                "first_year": int(data.year.min()),
                "last_year": int(data.year.max()),
                "observed_points": int(v.notna().sum()),
                "positive_points": len(positive),
                "zero_points": int(v.eq(0).sum()),
                "negative_points": int(v.lt(0).sum()),
                "missing_points": int(v.isna().sum()),
                "max_gap": max_missing_run(v),
                "legacy_keep": not reasons,
                "legacy_exclusions": "|".join(reasons),
                "legacy_clipped_points": int(
                    (v.lt(config.curation.legacy_clip_value) & v.gt(0)).sum()
                )
                if first.technology in config.curation.legacy_clipped_technologies
                else 0,
                "research_fit_eligible": int(v.notna().sum())
                >= config.curation.min_points
                and not v.lt(0).any()
                and family
                not in {"performance_not_deployment", "unit_size_not_additions"},
                "review_reason": "No stock/flow mapping solely from metric label; native description and denominator required",
            }
        )
    return pd.DataFrame(rows)


def compare_releases(original: pd.DataFrame, extended: pd.DataFrame) -> pd.DataFrame:
    """Pair unique semantic identities, retaining aliases and transformations."""
    keys = ["country", "technology", "native_source", "unit"]
    old = original.drop_duplicates("series_id")
    new = extended.drop_duplicates("series_id")
    old = old.loc[~old.duplicated(keys, keep=False)]
    new = new.loc[~new.duplicated(keys, keep=False)]
    pairs = old[keys + ["series_id", "metric", "variable"]].merge(
        new[keys + ["series_id", "metric", "variable"]],
        on=keys,
        how="outer",
        suffixes=("_original", "_extended"),
        indicator=True,
    )
    rows = []
    old_groups = dict(tuple(original.groupby("series_id")))
    new_groups = dict(tuple(extended.groupby("series_id")))
    for p in pairs.to_dict("records"):
        p["coverage"] = str(p.pop("_merge"))
        p["comparison"] = (
            "original_only" if p["coverage"] == "left_only" else "extended_only"
        )
        p["common_observations"] = 0
        p["changed_observations"] = 0
        p["new_observations"] = 0
        p["max_absolute_difference"] = np.nan
        if p["coverage"] == "both":
            a = old_groups[p["series_id_original"]][["year", "value"]].copy()
            a["old_cumsum"] = a.value.cumsum()
            b = new_groups[p["series_id_extended"]][["year", "value"]]
            aligned = a.merge(
                b, on="year", how="outer", suffixes=("_original", "_extended")
            )
            common = aligned.dropna(subset=["value_original", "value_extended"])
            close = np.isclose(
                common.value_original, common.value_extended, rtol=1e-10, atol=1e-8
            )
            cumulative = len(common) > 0 and np.allclose(
                common.old_cumsum, common.value_extended, rtol=1e-10, atol=1e-8
            )
            p["common_observations"] = len(common)
            p["changed_observations"] = int((~close).sum())
            p["new_observations"] = int(
                (aligned.value_original.isna() & aligned.value_extended.notna()).sum()
            )
            p["max_absolute_difference"] = (
                float((common.value_extended - common.value_original).abs().max())
                if len(common)
                else np.nan
            )
            p["comparison"] = (
                "equal_overlap"
                if close.all() and len(common)
                else "cumulative_sum_of_original"
                if cumulative
                else "other_changes"
            )
        rows.append(p)
    for release, frame in [("original", original), ("extended", extended)]:
        catalog = frame.drop_duplicates("series_id")
        for ambiguous in catalog.loc[catalog.duplicated(keys, keep=False)].to_dict(
            "records"
        ):
            rows.append(
                {
                    **{k: ambiguous[k] for k in keys},
                    f"series_id_{release}": ambiguous["series_id"],
                    f"metric_{release}": ambiguous["metric"],
                    f"variable_{release}": ambiguous["variable"],
                    "coverage": "ambiguous",
                    "comparison": "ambiguous_identity",
                    "common_observations": 0,
                    "changed_observations": 0,
                    "new_observations": 0,
                    "max_absolute_difference": np.nan,
                }
            )
    return pd.DataFrame(rows)


def compare_release_fits(comparisons: pd.DataFrame, fits: pd.DataFrame) -> pd.DataFrame:
    """Compare like-for-like curve settings on paired national observations."""
    paired = comparisons.loc[comparisons.coverage.eq("both")]
    if fits.empty or paired.empty:
        return pd.DataFrame(
            columns=[
                "series_id_original",
                "series_id_extended",
                "family",
                "sensitivity",
                "ceiling_multiplier",
                "status_original",
                "status_extended",
                "comparison",
                "k_difference_per_year",
            ]
        )
    fields = [
        "series_id",
        "family",
        "sensitivity",
        "ceiling_multiplier",
        "k_per_year",
        "r2",
        "ceiling_native",
        "bound_hit",
        "n_points",
        "status",
    ]
    result = paired.merge(
        fits[fields].rename(columns={"series_id": "series_id_original"}),
        on="series_id_original",
    )
    result = result.merge(
        fits[fields].rename(columns={"series_id": "series_id_extended"}),
        on=["series_id_extended", "family", "sensitivity", "ceiling_multiplier"],
        suffixes=("_original", "_extended"),
    )
    result["k_difference_per_year"] = (
        result.k_per_year_extended - result.k_per_year_original
    )
    return result


def release_inventory(
    bundle: ConfigBundle, config: GrowthDiagnosticConfig
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Preserve all native series, including those with no usable observations."""
    catalogs = []
    for release in ["original", "extended"]:
        native = pd.read_csv(
            asset_path(bundle, config.source_assets[release]), low_memory=False
        )
        years = [c for c in native if c.isdigit() and len(c) == 4]
        catalog = native.loc[
            :, [c for c in native if c not in years and not c.startswith("Unnamed")]
        ].copy()
        catalog["release"] = release
        catalog["series_id"] = release + ":" + native.ID
        catalog["raw_row"] = np.arange(len(native)) + 2
        catalog["nonmissing_observations"] = native[years].notna().sum(axis=1)
        catalog["zero_observations"] = native[years].eq(0).sum(axis=1)
        catalog["negative_observations"] = native[years].lt(0).sum(axis=1)
        catalogs.append(catalog)
    inventory = pd.concat(catalogs, ignore_index=True)
    native_definitions = yaml.safe_load(
        asset_path(bundle, config.source_assets["descriptions"]).read_text(
            encoding="utf-8"
        )
    )
    descriptions = []
    for entry in native_definitions:
        for variable, detail in entry.items():
            descriptions.append({"Variable": variable, **detail})
    metadata = pd.DataFrame(descriptions).merge(
        pd.read_csv(
            asset_path(bundle, "10231865:HATCH_v1.5_DataSources.csv"), encoding="cp1252"
        ),
        on="Variable",
        how="outer",
        validate="one_to_one",
    )
    sheets = pd.read_excel(
        asset_path(bundle, "19579793:HATCH_variable_descriptions.xlsx"), sheet_name=None
    )
    extended_metadata = pd.concat(
        [
            frame.assign(sheet=sheet, raw_row=np.arange(len(frame)) + 2)
            for sheet, frame in sheets.items()
        ],
        ignore_index=True,
    )
    return inventory, metadata, extended_metadata


def published_growth_estimates(
    bundle: ConfigBundle, config: GrowthDiagnosticConfig
) -> tuple[pd.DataFrame, pd.DataFrame]:
    extended = pd.read_csv(
        asset_path(bundle, config.source_assets["extended_estimates"]), low_memory=False
    )
    for field in [
        "Logistic Fit",
        "Logistic Saturation",
        "Logistic Inflection Year",
        "logistic goodness",
        "Gompertz Fit",
        "Exponential Fit",
        "Delta T",
    ]:
        extended[field + "_native"] = extended[field].astype(str)
        extended[field] = pd.to_numeric(extended[field], errors="coerce")
    extended["series_id"] = "extended:" + extended.ID
    extended["paper_filter_pass"] = extended["logistic goodness"].ge(
        config.fitting.published_r2_review
    ) & extended["Logistic Fit"].between(
        0, config.fitting.published_k_upper, inclusive="neither"
    )
    extended["emergence_annual_fraction"] = np.expm1(
        extended["Logistic Fit"].clip(upper=config.fitting.k_upper)
    )
    # Out-of-range fits are kept and flagged, not truncated into usable estimates.
    extended.loc[
        extended["Logistic Fit"].gt(config.fitting.k_upper), "emergence_annual_fraction"
    ] = np.nan
    extended["reported_10_90_average_fraction"] = 2.197 / extended["Delta T"]
    original = pd.read_csv(
        asset_path(bundle, config.source_assets["original_estimates"])
    )
    original = original.loc[:, ~original.columns.str.startswith("Unnamed")]
    for field in [
        c for c in original if c not in {"Technology", "Technology Category"}
    ]:
        original[field + "_native"] = original[field].astype(str)
        original[field] = (
            pd.to_numeric(
                original[field].astype(str).str.replace("%", "", regex=False),
                errors="coerce",
            )
            / 100
        )
    original["reported_inverse_10_90_duration_per_year"] = original["Delta T"]
    original["duration_10_90_years"] = 1 / original["Delta T"].where(
        original["Delta T"].gt(0)
    )
    original["logistic_emergence_annual_fraction"] = np.expm1(original["Logistic"])
    return original, extended


def observation_windows(
    observations: pd.DataFrame, controls: FitControls
) -> pd.DataFrame:
    """OLS on log-levels, endpoint CAGR and native-space exponential fit.

    A seven-year span has endpoints seven years apart (normally eight annual
    observations). Gaps are counted, never interpolated. Missing/zero cases remain
    rows with an exclusion reason. This is not a literal replication of Odenweller.
    """
    rows = []
    for sid, data in observations.groupby("series_id", sort=True):
        data = data.sort_values("year")
        for span in controls.window_spans:
            for end in range(int(data.year.min()) + span, int(data.year.max()) + 1):
                start = end - span
                window = data.loc[data.year.between(start, end)]
                valid = window.dropna(subset=["value"])
                row = {
                    "series_id": sid,
                    "start_year": start,
                    "end_year": end,
                    "span_years": span,
                    "n_observed": len(valid),
                    "missing_years": span + 1 - len(valid),
                    "log_slope_per_year": np.nan,
                    "annual_rate": np.nan,
                    "endpoint_cagr": np.nan,
                    "native_exp_annual_rate": np.nan,
                    "r2_log": np.nan,
                    "r2_native": np.nan,
                    "start_value": np.nan,
                    "end_value": np.nan,
                    "status": "usable",
                }
                if (
                    len(valid) < controls.minimum_window_points
                    or row["missing_years"] > controls.max_missing_window_years
                ):
                    row["status"] = "insufficient_coverage"
                elif not {start, end}.issubset(set(valid.year)):
                    row["status"] = "missing_endpoint"
                elif valid.value.le(0).any():
                    row["status"] = "zero_or_negative_log_undefined"
                else:
                    x = valid.year.to_numpy(dtype=float) - start
                    y = valid.value.to_numpy(dtype=float)
                    logy = np.log(y)
                    slope, intercept = np.polyfit(x, logy, 1)
                    predlog = intercept + slope * x
                    sslog = float(np.square(logy - logy.mean()).sum())
                    row.update(
                        log_slope_per_year=float(slope),
                        annual_rate=float(np.expm1(slope)),
                        endpoint_cagr=float(np.expm1(np.log(y[-1] / y[0]) / span)),
                        r2_log=1 - float(np.square(logy - predlog).sum()) / sslog
                        if sslog
                        else np.nan,
                        start_value=float(y[0]),
                        end_value=float(y[-1]),
                    )
                    # Unbounded in slope; no emergency-deployment cutoff imposed.
                    scale = y.max()
                    fit = least_squares(
                        lambda p: (
                            np.exp(np.clip(p[0] + p[1] * x, -700, 700)) - y / scale
                        ),
                        [intercept - np.log(scale), slope],
                        max_nfev=controls.max_nfev,
                    )
                    ypred = np.exp(fit.x[0] + fit.x[1] * x) * scale
                    ss = float(np.square(y - y.mean()).sum())
                    row["native_exp_annual_rate"] = float(np.expm1(fit.x[1]))
                    row["r2_native"] = (
                        1 - float(np.square(y - ypred).sum()) / ss if ss else np.nan
                    )
                    if not fit.success:
                        row["status"] = "native_fit_failed"
                rows.append(row)
    return pd.DataFrame(rows)


def fit_curve(
    data: pd.DataFrame,
    *,
    family: str,
    ceiling_multiplier: float,
    controls: FitControls,
    legacy_bounds: bool = False,
) -> dict:
    valid = data.dropna(subset=["value"]).sort_values("year")
    row = {
        "family": family,
        "ceiling_multiplier": ceiling_multiplier,
        "legacy_bounds": legacy_bounds,
        "status": "insufficient_data",
        "k_per_year": np.nan,
        "ceiling_native": np.nan,
        "inflection_year": np.nan,
        "r2": np.nan,
        "rmse_native": np.nan,
        "n_points": len(valid),
        "first_observed_year": int(valid.year.min()) if len(valid) else np.nan,
        "last_observed_year": int(valid.year.max()) if len(valid) else np.nan,
        "bound_hit": False,
        "jacobian_condition": np.nan,
        "inflection_observed": False,
    }
    if (
        len(valid) < 3
        or valid.value.lt(0).any()
        or valid.value.max() <= 0
        or valid.value.nunique() < 2
    ):
        return row
    scale = float(valid.value.max())
    origin = float(valid.year.min())
    x = valid.year.to_numpy(dtype=float) - origin
    y = valid.value.to_numpy(dtype=float) / scale
    margin = 0 if legacy_bounds else controls.inflection_margin_years
    lower = [
        controls.legacy_ceiling_lower
        if legacy_bounds
        else controls.broad_ceiling_lower,
        1e-9,
        -margin,
    ]
    upper = [ceiling_multiplier, controls.k_upper, float(x.max()) + margin]
    best = None
    for start_k in controls.start_k:
        result = least_squares(
            lambda p: s_curve(x, p[0], p[1], p[2], family) - y,
            [min(1.05, ceiling_multiplier - 1e-5), start_k, float(np.median(x))],
            bounds=(lower, upper),
            max_nfev=controls.max_nfev,
            x_scale="jac",
        )
        if result.success and (best is None or result.cost < best.cost):
            best = result
    if best is None:
        row["status"] = "optimizer_not_converged"
        return row
    fitted = s_curve(x, *best.x, family)
    residual = float(np.square(y - fitted).sum())
    total = float(np.square(y - y.mean()).sum())
    k = float(best.x[1])
    midpoint = float(best.x[2]) + origin
    row.update(
        status="fitted",
        k_per_year=k,
        ceiling_native=float(best.x[0]) * scale,
        inflection_year=midpoint,
        r2=1 - residual / total,
        rmse_native=math.sqrt(residual / len(y)) * scale,
        bound_hit=bool(np.any(best.active_mask)),
        jacobian_condition=float(np.linalg.cond(best.jac)),
        inflection_observed=origin <= midpoint <= float(valid.year.max()),
        emergence_annual_fraction=math.expm1(k) if family == "logistic" else np.nan,
        duration_10_90_years=math.log(81) / k if family == "logistic" else np.nan,
    )
    return row


def fit_pool(
    observations: pd.DataFrame, audit: pd.DataFrame, config: GrowthDiagnosticConfig
) -> pd.DataFrame:
    eligible = set(audit.loc[audit.research_fit_eligible, "series_id"])
    rows = []
    for sid, data in observations.groupby("series_id", sort=True):
        if sid not in eligible:
            continue
        if data.value.max() <= 0:
            continue
        for family in ["logistic", "gompertz"]:
            for ceiling in config.fitting.ceiling_multipliers:
                result = fit_curve(
                    data,
                    family=family,
                    ceiling_multiplier=ceiling,
                    controls=config.fitting,
                    legacy_bounds=ceiling == config.fitting.ceiling_multipliers[0],
                )
                rows.append({"series_id": sid, "sensitivity": "full", **result})
            trimmed = data.loc[
                data.year.le(data.year.max() - config.fitting.truncation_years)
            ]
            result = fit_curve(
                trimmed,
                family=family,
                ceiling_multiplier=max(config.fitting.ceiling_multipliers),
                controls=config.fitting,
            )
            rows.append(
                {"series_id": sid, "sensitivity": "remove_recent_years", **result}
            )
        LOGGER.info("Fitted %s", sid)
    return pd.DataFrame(rows)


def canadian_registrations(
    bundle: ConfigBundle, config: GrowthDiagnosticConfig
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Read current v2 sources; retain pooled LT allocation and suppression flags."""
    sr = load_harmonization_rules(bundle, "statcan_tables")
    rr = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    directory = resolve_input_path(bundle, "interim", sr["interim_subdir"])
    ldv_path = directory / sr["ldv_history"]["output_file"]
    truck_path = directory / sr["tables"]["23-10-0308-01"]["output_file"]
    frames = []
    for path, classes in [
        (ldv_path, ["cars", "passenger_light_trucks", "freight_light_trucks"]),
        (truck_path, ["medium_trucks", "heavy_trucks"]),
    ]:
        native = pd.read_csv(path)
        expected_unit = "Units" if path == ldv_path else "Number"
        if "units" not in native or set(native.units.dropna()) != {expected_unit}:
            raise ValueError(
                f"Canadian deployment evidence has unexpected native count units: {path.name}"
            )
        if "reference_year" not in native:
            native["reference_year"] = pd.to_numeric(native.reference_period).astype(
                int
            )
        for road_class in classes:
            types = (
                rr["statcan_ldv_source_types"][road_class]
                if road_class in rr["statcan_ldv_source_types"]
                else rr["statcan_medium_source_types"]
                if road_class == "medium_trucks"
                else [rr["statcan_heavy_source_type"]]
            )
            native_class = native.loc[native.vehicle_type.isin(types)]
            output_map = rr["region_output_map"]
            for scenario_region in bundle.scenario.geography.regions:
                output_region = output_map.get(scenario_region, scenario_region)
                proxy = rr["share_region_proxy"].get(scenario_region, scenario_region)
                source_regions = {scenario_region, proxy}
                chosen = native_class.loc[
                    native_class.scenario_region.isin(source_regions)
                ]
                for (year, fuel), g in chosen.groupby(
                    ["reference_year", "fuel_type"], sort=True
                ):
                    if g.duplicated(
                        ["vehicle_type", "reference_year", "fuel_type"]
                    ).any():
                        raise ValueError(
                            "Ambiguous source-region observations for Canadian registrations"
                        )
                    complete = g.scaled_value.notna().all() and set(
                        g.vehicle_type
                    ) == set(types)
                    if path == ldv_path:
                        complete = complete and bool(
                            (
                                (
                                    g.source_table_id.eq("20-10-0021-01")
                                    & g.source_period_count.eq(1)
                                )
                                | (
                                    g.source_table_id.eq("20-10-0025-01")
                                    & g.source_period_count.eq(4)
                                )
                            ).all()
                        )
                    values = g.scaled_value.to_numpy(dtype=float)
                    count = float(values.sum()) if complete else np.nan
                    frames.append(
                        {
                            "region": output_region,
                            "road_class": road_class,
                            "year": int(year),
                            "fuel_type": fuel,
                            "quantity_k_vehicles": count / 1000,
                            "source_file": path.relative_to(
                                bundle.repo_root
                            ).as_posix(),
                            "source_rows": "|".join(str(i + 2) for i in g.index),
                            "coverage_complete": bool(complete),
                            "source_regions": "|".join(
                                sorted(g.scenario_region.unique())
                            ),
                            "proxy_geography": proxy != scenario_region,
                            "association": "pooled_LT_no_passenger_freight_allocation"
                            if "light_trucks" in road_class
                            else "medium_heavy_registered_stock_not_new_sales"
                            if road_class in {"medium_trucks", "heavy_trucks"}
                            else "passenger_car_new_registrations",
                            "quantity": "annual_new_registrations"
                            if road_class in rr["statcan_ldv_source_types"]
                            else "registered_stock",
                            "unit": "k vehicles",
                        }
                    )
    LOGGER.info(
        "Canadian source counts divided by 1000 to k vehicles; stock and gross registrations retain distinct quantities"
    )
    return pd.DataFrame(frames), {
        p.relative_to(bundle.repo_root).as_posix(): file_sha256(p)
        for p in [ldv_path, truck_path]
    }


def existing_capacity_references(
    bundle: ConfigBundle, config: GrowthDiagnosticConfig
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    paths = {
        "capacity": resolve_artifact_path(
            bundle, "existing_capacity_processed", "road_existing_capacity.csv"
        ),
        "shares": resolve_artifact_path(
            bundle,
            "existing_capacity_interim",
            "road_existing_capacity_fuel_shares.csv",
        ),
        "cohorts": resolve_artifact_path(
            bundle,
            "existing_capacity_interim",
            "road_existing_capacity_age_cohorts.csv",
        ),
    }
    capacity = pd.read_csv(paths["capacity"])
    shares = pd.read_csv(paths["shares"])
    cohorts = pd.read_csv(paths["cohorts"])
    if (
        set(capacity.units) != {"k vehicles"}
        or capacity.duplicated(["region", "tech", "vintage"]).any()
    ):
        raise ValueError(
            "Existing road capacity has incompatible units or duplicate keys"
        )
    keys = ["region", "road_class", "tech", "vintage"]
    uncleaned = shares.groupby(keys, as_index=False).capacity.sum()
    expected = uncleaned.loc[
        uncleaned.capacity.ge(bundle.scenario.existing_capacity.cleanup_tolerance)
        & uncleaned.capacity.gt(0)
    ]
    reconciliation = capacity.merge(
        expected,
        on=keys,
        how="outer",
        suffixes=("_processed", "_audit"),
        indicator=True,
    )
    if not reconciliation._merge.eq("both").all() or not np.allclose(
        reconciliation.capacity_processed,
        reconciliation.capacity_audit,
        rtol=0,
        atol=1e-7,
    ):
        raise ValueError(
            "Current processed capacity differs from cleanup of the fuel-share audit"
        )
    audited_totals = (
        uncleaned.groupby(["region", "road_class"]).capacity.sum().sort_index()
    )
    cohort_totals = (
        cohorts.groupby(["region", "road_class"]).stock_k_vehicles.first().sort_index()
    )
    if not audited_totals.index.equals(cohort_totals.index) or not np.allclose(
        audited_totals, cohort_totals, rtol=0, atol=1e-7
    ):
        raise ValueError(
            "Current uncleaned road fuel distribution does not conserve class stock"
        )
    LOGGER.info(
        "Existing capacity reconciled after documented epsilon=%s; removed %s k vehicles",
        bundle.scenario.existing_capacity.cleanup_tolerance,
        uncleaned.capacity.sum() - capacity.capacity.sum(),
    )
    template = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    rules = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]
    range_rules = load_harmonization_rules(bundle, "ldv_ev_ranges")
    rows = []
    member_rows = []
    for region in sorted(capacity.region.unique()):
        for road_class, spec in config.target_classes.items():
            local = capacity.loc[
                capacity.region.eq(region) & capacity.road_class.eq(road_class)
            ]
            total = float(local.capacity.sum())
            source_total = float(cohort_totals.loc[(region, road_class)])
            for powertrain in spec.powertrains:
                fuel = config.fuel_names[powertrain]
                existing_tech = rules["fuel_technology"][road_class].get(fuel)
                selected = (
                    local.loc[local.tech.eq(existing_tech)]
                    if existing_tech
                    else local.iloc[:0]
                )
                stock = float(selected.capacity.sum())
                evidence = shares.loc[
                    shares.region.eq(region)
                    & shares.road_class.eq(road_class)
                    & shares.tech.eq(existing_tech)
                ]
                families = (
                    [powertrain]
                    if powertrain not in {"diesel", "fcev"}
                    else ["diesel"]
                    if powertrain == "diesel"
                    else ["fcev", "fchev"]
                )
                future = template.loc[
                    template.category.eq(road_class)
                    & template.tech.str.endswith("_N")
                    & template.sub_category.str.split("_").str[0].isin(families),
                    "tech",
                ].tolist()
                # Representatives are scenario-owned dynamic structure, not necessarily base-template rows.
                if (
                    road_class in range_rules["representative"]["families"]
                    and powertrain in {"bev", "phev"}
                    and bundle.scenario.BEV_PHEV_range_representation.mode
                    == "representative_archetype"
                ):
                    future = [
                        range_rules["representative"]["families"][road_class][
                            powertrain.upper()
                        ]
                    ]
                target = road_class + ":" + powertrain
                legacy_pt = (
                    spec.legacy_bev
                    if powertrain == "bev"
                    else "GSL_PHEV35"
                    if powertrain == "phev"
                    and road_class in range_rules["representative"]["families"]
                    else "DSL_PHEV"
                    if powertrain == "phev"
                    else "GSL_HEV"
                    if powertrain == "hev"
                    and road_class in range_rules["representative"]["families"]
                    else "DSL_HEV"
                    if powertrain == "hev"
                    else "FCEV400"
                    if powertrain == "fcev"
                    and road_class in range_rules["representative"]["families"]
                    else "FCEV"
                    if powertrain == "fcev"
                    else "DSL"
                    if powertrain == "diesel"
                    else "CNG"
                )
                latest = int(bundle.scenario.periods.existing[-1])
                previous = int(bundle.scenario.periods.existing[-2])
                row = {
                    "region": region,
                    "target_id": target,
                    "road_class": road_class,
                    "powertrain": powertrain,
                    "legacy_selected_tech": spec.prefix + "_" + legacy_pt + "_N",
                    "existing_tech": existing_tech or "",
                    "future_members": "|".join(sorted(future)),
                    "reference_year": bundle.scenario.periods.base_year,
                    "stock_k_vehicles": stock,
                    "class_stock_k_vehicles": total,
                    "stock_share": stock / total if total else np.nan,
                    "source_class_stock_k_vehicles": source_total,
                    "cleanup_removed_class_k": source_total - total,
                    "deployment_status": "represented_stock"
                    if stock > 0
                    else "represented_zero"
                    if existing_tech
                    else "absent_existing_representation",
                    "stock_basis": "CEUD class stock + registered age distribution + inferred cohort fuel mix",
                    "age_sources": "|".join(
                        sorted(
                            cohorts.loc[
                                cohorts.region.eq(region)
                                & cohorts.road_class.eq(road_class),
                                "age_source",
                            ]
                            .dropna()
                            .unique()
                        )
                    ),
                    "source_years": "|".join(
                        str(int(y))
                        for y in sorted(evidence.source_year.dropna().unique())
                    ),
                    "source_regions": "|".join(
                        map(str, sorted(evidence.source_region.dropna().unique()))
                    ),
                    "proxy_geography": bool(evidence.proxy_geography.any()),
                    "dashboard_override": bool(evidence.dashboard_override.any()),
                    "latest_existing_label": latest,
                    "latest_surviving_cohort_k": float(
                        selected.loc[selected.vintage.eq(latest), "capacity"].sum()
                    ),
                    "previous_existing_label": previous,
                    "previous_surviving_cohort_k": float(
                        selected.loc[selected.vintage.eq(previous), "capacity"].sum()
                    ),
                    "units": "k vehicles",
                    "norway_class": spec.norway_class,
                    "scope_review": "Legacy diesel-car target; mature incumbent, emergence analogy unsupported"
                    if powertrain == "diesel"
                    else "FCEV and FCHEV future siblings; missing stock is not measured zero"
                    if powertrain == "fcev"
                    else "legacy emerging road target",
                }
                rows.append(row)
                for tech in [*([existing_tech] if existing_tech else []), *future]:
                    member_rows.append(
                        {
                            "target_id": target,
                            "tech": tech,
                            "role": "existing" if tech == existing_tech else "future",
                            "diagnostic_only": True,
                        }
                    )
    return (
        pd.DataFrame(rows),
        pd.DataFrame(member_rows).drop_duplicates(),
        {
            p.relative_to(bundle.repo_root).as_posix(): file_sha256(p)
            for p in [
                *paths.values(),
                resolve_input_path(bundle, "template", "technology.csv"),
            ]
        },
    )


def period_additions(observations: pd.DataFrame, *, width: int) -> pd.DataFrame:
    """Aggregate complete calendar bins (e.g. 2011-15 -> label 2010); no cumsum stock."""
    if (
        not isinstance(width, int)
        or width < 1
        or observations.duplicated(["series_id", "year"]).any()
        or observations.value.lt(0).any()
    ):
        raise ValueError(
            "Additions need positive integer width, unique annual keys and nonnegative observations"
        )
    rows = []
    for sid, g in observations.groupby("series_id", sort=True):
        g = g.sort_values("year").copy()
        g["period"] = ((g.year - 1) // width) * width
        for period, block in g.groupby("period", sort=True):
            count = int(block.value.notna().sum())
            complete = count == width and len(block) == width
            rows.append(
                {
                    "series_id": sid,
                    "period": int(period),
                    "start_year": int(period) + 1,
                    "end_year": int(period) + width,
                    "observed_years": count,
                    "period_years": width,
                    "complete": bool(complete),
                    "new_capacity_k": float(block.value.sum()) / 1000
                    if complete
                    else np.nan,
                }
            )
    result = (
        pd.DataFrame(rows).sort_values(["series_id", "period"]).reset_index(drop=True)
    )
    result["delta_k"] = result.groupby("series_id").new_capacity_k.diff()
    result.loc[result.groupby("series_id").period.diff().ne(width), "delta_k"] = np.nan
    result["second_difference_k"] = result.groupby("series_id").delta_k.diff()
    return result


def minimum_seed(levels: np.ndarray, rate: float, *, mode: str) -> float:
    levels = np.asarray(levels, dtype=float)
    if not np.isfinite(levels).all() or len(levels) < (
        3 if mode == "growth_new_capacity_delta" else 2
    ):
        raise ValueError("Seed calibration requires finite consecutive observations")
    if rate < -1 or not math.isfinite(rate):
        raise ValueError("Invalid transition rate")
    if mode == "growth_new_capacity_delta":
        deltas = np.diff(levels)
        residual = deltas[1:] - (1 + rate) * deltas[:-1]
    elif mode in {"growth_capacity", "growth_new_capacity"}:
        residual = levels[1:] - (1 + rate) * levels[:-1]
    else:
        raise ValueError("Unknown growth mode")
    return max(0.0, float(residual.max()))


def limit_envelope(
    *,
    mode: str,
    initial: float,
    transition_rate: float,
    seed: float,
    years: list[int],
    previous2: float | None = None,
) -> pd.DataFrame:
    """Bind <= equations to show their envelopes, not optimizer deployment forecasts.

    No retirement, demand or market limit is imposed. A negative upper bound is
    marked infeasible with nonnegative capacity; it is never silently clipped.
    Seeds contribute at every transition. For delta, initial and previous2 are
    consecutive historical additions totals, never the total active vehicle stock.
    """
    if mode not in {
        "growth_capacity",
        "growth_new_capacity",
        "growth_new_capacity_delta",
    }:
        raise ValueError("Unknown growth mode")
    if (
        initial < 0
        or seed < 0
        or transition_rate < -1
        or not all(map(math.isfinite, [initial, seed, transition_rate]))
    ):
        raise ValueError("Invalid growth envelope arguments")
    if len(years) < 2 or any(b <= a for a, b in zip(years, years[1:])):
        raise ValueError("Envelope years must increase")
    if mode == "growth_new_capacity_delta" and (
        previous2 is None or not math.isfinite(previous2) or previous2 < 0
    ):
        raise ValueError("Delta demonstration needs two historical additions levels")
    level = initial
    delta = initial - previous2 if previous2 is not None else np.nan
    rows = [
        {
            "year": years[0],
            "step": 0,
            "capacity_k": level,
            "delta_k": delta,
            "seed_k": seed,
            "transition_rate": transition_rate,
            "feasible_nonnegative": True,
        }
    ]
    for i, year in enumerate(years[1:], 1):
        if mode == "growth_new_capacity_delta":
            delta = seed + (1 + transition_rate) * delta
            level += delta
        else:
            level = seed + (1 + transition_rate) * level
        rows.append(
            {
                "year": year,
                "step": i,
                "capacity_k": level,
                "delta_k": delta,
                "seed_k": seed,
                "transition_rate": transition_rate,
                "feasible_nonnegative": level >= 0,
            }
        )
    return pd.DataFrame(rows)


def calibrate_candidates(
    anchors: pd.DataFrame,
    windows: pd.DataFrame,
    eafo: pd.DataFrame,
    domestic: pd.DataFrame,
    config: GrowthDiagnosticConfig,
) -> pd.DataFrame:
    """Stock maturity participates in selection; measured rates supply the rate.

    Absolute starting deployment is not treated as evidence of growth speed.
    Absent stock cannot establish maturity. HEV has no EAFO direct stock series;
    its domestic sales estimates remain additions evidence, not net stock rates.
    """
    rows = []
    for a in anchors.itertuples(index=False):
        fuel = config.norway_powertrains.get(a.powertrain)
        sid = f"eafo:{a.norway_class}:fleet:{fuel}" if fuel else None
        valid_windows = windows.loc[
            windows.status.eq("usable")
            & windows.r2_log.ge(config.fitting.fit_r2_review)
        ]
        stock_windows = (
            valid_windows.loc[valid_windows.series_id.eq(sid)].copy()
            if sid
            else valid_windows.iloc[:0].copy()
        )
        shares = eafo.loc[
            eafo.vehicle_class.eq(a.norway_class)
            & eafo.quantity.eq("fleet_share")
            & eafo.powertrain.eq(fuel),
            ["year", "value"],
        ]
        stock_windows = stock_windows.merge(
            shares.rename(
                columns={"year": "start_year", "value": "analogue_start_share_percent"}
            ),
            on="start_year",
            how="left",
        )
        stock_windows["distance"] = np.inf
        if a.stock_share > 0:
            pos = stock_windows.analogue_start_share_percent.gt(0)
            stock_windows.loc[pos, "distance"] = np.abs(
                np.log(
                    stock_windows.loc[pos, "analogue_start_share_percent"]
                    / 100
                    / a.stock_share
                )
            )
        selected = stock_windows.loc[
            stock_windows.distance.le(
                math.log(config.calibration.maturity_match_factor)
            )
        ]
        matched = not selected.empty
        if not matched and len(stock_windows):
            # Keep nearest-stage alternatives inspectable but explicitly uncalibrated.
            distance = stock_windows.distance.min()
            selected = (
                stock_windows.loc[stock_windows.distance.eq(distance)]
                if math.isfinite(distance)
                else stock_windows
            )
        for mode in [
            "growth_capacity",
            "growth_new_capacity",
            "growth_new_capacity_delta",
        ]:
            basis = ""
            recommended = False
            chosen = selected
            if mode == "growth_capacity":
                basis = (
                    "Norway stock window, selected using Canadian stock share"
                    if matched
                    else "unanchored or distant-stage Norway stock proxy"
                )
                recommended = (
                    matched
                    and a.stock_k_vehicles > 0
                    and a.powertrain in {"bev", "phev"}
                )
            elif mode == "growth_new_capacity":
                # Distinct quantities: new car registrations directly; MHDV table is STOCK.
                sid_d = f"canada:{a.region}:{a.road_class}:{a.powertrain}"
                chosen = valid_windows.loc[valid_windows.series_id.eq(sid_d)]
                if len(chosen):
                    # Recent sales phase: current inferred stock share is compared with sales
                    # share to expose turnover/maturity, never used as a sales denominator.
                    chosen = chosen.loc[chosen.end_year.eq(chosen.end_year.max())]
                    basis = "Canadian new-registration windows; inferred stock/sales maturity contrast"
                    recommended = a.road_class == "cars" and a.powertrain in {
                        "bev",
                        "phev",
                        "hev",
                    }
                else:
                    sid_n = f"eafo:{a.norway_class}:registrations:{fuel}"
                    chosen = (
                        valid_windows.loc[valid_windows.series_id.eq(sid_n)]
                        if fuel
                        else valid_windows.iloc[:0]
                    )
                    if matched and len(chosen):
                        chosen = chosen.merge(
                            selected[["start_year", "end_year", "span_years"]],
                            on=["start_year", "end_year", "span_years"],
                            how="inner",
                        )
                        basis = "Norway gross-registration windows at Canadian stock-matched maturity; class/reference transfer unresolved"
                    else:
                        basis = "Unanchored Norway registrations proxy; target maturity and additions references unresolved"
                if a.road_class in {"medium_trucks", "heavy_trucks"}:
                    recommended = False
            else:
                chosen = windows.iloc[:0]
                basis = "Recommend transition R=0 for review; calibrate seed to consecutive period-additions second differences"
            quantiles = (
                chosen.annual_rate.quantile(
                    config.calibration.candidate_quantiles
                ).to_dict()
                if len(chosen)
                else {}
            )
            local = domestic.loc[
                domestic.region.eq(a.region)
                & domestic.road_class.eq(a.road_class)
                & domestic.fuel_type.eq(config.fuel_names[a.powertrain])
                & domestic.year.eq(a.reference_year)
                & domestic.quantity.eq("annual_new_registrations")
            ]
            sales = float(local.quantity_k_vehicles.iloc[0]) if len(local) else np.nan
            class_regs = domestic.loc[
                domestic.region.eq(a.region)
                & domestic.road_class.eq(a.road_class)
                & domestic.year.eq(a.reference_year)
                & domestic.quantity.eq("annual_new_registrations")
            ]
            total_regs = (
                float(class_regs.quantity_k_vehicles.sum())
                if len(class_regs) and class_regs.coverage_complete.all()
                else np.nan
            )
            median = float(chosen.annual_rate.median()) if len(chosen) else np.nan
            rows.append(
                {
                    "region": a.region,
                    "target_id": a.target_id,
                    "mode": mode,
                    "reference_year": a.reference_year,
                    "stock_k_vehicles": a.stock_k_vehicles,
                    "stock_share": a.stock_share,
                    "annual_candidate_median": median,
                    "q25": quantiles.get(0.25, np.nan),
                    "q75": quantiles.get(0.75, np.nan),
                    "constant_transition_candidate": 0.0
                    if mode == "growth_new_capacity_delta"
                    else annual_to_period(
                        median, config.calibration.historical_period_width
                    )
                    if math.isfinite(median)
                    else np.nan,
                    "candidate_status": "supported_proxy_for_review"
                    if recommended
                    else "insufficient_target_evidence",
                    "basis": basis,
                    "maturity_match": matched,
                    "matched_windows": len(chosen),
                    "evidence_series": "|".join(sorted(chosen.series_id.unique())),
                    "window_years": "|".join(
                        sorted(
                            {
                                f"{r.start_year}-{r.end_year}"
                                for r in chosen.itertuples()
                            }
                        )
                    ),
                    "median_window_r2_log": float(chosen.r2_log.median())
                    if len(chosen)
                    else np.nan,
                    "minimum_log_maturity_distance": float(selected.distance.min())
                    if len(selected)
                    else np.nan,
                    "base_year_registration_k": sales,
                    "base_year_sales_share": sales / total_regs
                    if total_regs > 0
                    else np.nan,
                    "registration_proxy_geography": bool(local.proxy_geography.any())
                    if len(local)
                    else False,
                    "annual_registration_to_stock_ratio": sales / a.stock_k_vehicles
                    if a.stock_k_vehicles > 0
                    else np.nan,
                    "reference_warning": "Surviving vintage cohorts differ from original gross additions; first-period identity/group and lifetime weighting require review",
                    "diagnostic_only": True,
                }
            )
    return pd.DataFrame(rows)


def seed_frontiers(
    eafo: pd.DataFrame, periods: pd.DataFrame, config: GrowthDiagnosticConfig
) -> pd.DataFrame:
    rows = []
    for sid, g in periods.groupby("series_id", sort=True):
        g = g.sort_values("period")
        for mode in ["growth_new_capacity", "growth_new_capacity_delta"]:
            for annual in config.calibration.stress_annual_rates:
                rate = (
                    0.0
                    if mode == "growth_new_capacity_delta"
                    else annual_to_period(
                        annual, config.calibration.historical_period_width
                    )
                )
                n = g.new_capacity_k.to_numpy(dtype=float)
                residuals = []
                length = 3 if mode == "growth_new_capacity_delta" else 2
                for i in range(length, len(n) + 1):
                    window = n[i - length : i]
                    if np.isfinite(window).all():
                        residuals.append(minimum_seed(window, rate, mode=mode))
                rows.append(
                    {
                        "series_id": sid,
                        "mode": mode,
                        "annual_stress_rate": annual
                        if mode != "growth_new_capacity_delta"
                        else np.nan,
                        "transition_rate": rate,
                        "minimum_constant_seed_k": max(residuals)
                        if residuals
                        else np.nan,
                        "consecutive_blocks": len(residuals),
                        "units": "k vehicles per constraint transition",
                        "interpretation": "Residual bound covering observed blocks, not an initiation seed recommendation",
                    }
                )
    for sid, g in eafo.loc[eafo.quantity.eq("fleet") & eafo.complete_year].groupby(
        "series_id", sort=True
    ):
        g = g.sort_values("year")
        for annual in config.calibration.stress_annual_rates:
            residuals = []
            for i in range(1, len(g)):
                if (
                    np.isfinite(g.value.iloc[i - 1 : i + 1]).all()
                    and g.year.iloc[i] - g.year.iloc[i - 1] == 1
                ):
                    residuals.append(
                        max(
                            0,
                            (g.value.iloc[i] - (1 + annual) * g.value.iloc[i - 1])
                            / 1000,
                        )
                    )
            rows.append(
                {
                    "series_id": sid,
                    "mode": "growth_capacity",
                    "annual_stress_rate": annual,
                    "transition_rate": annual,
                    "minimum_constant_seed_k": max(residuals) if residuals else np.nan,
                    "consecutive_blocks": len(residuals),
                    "units": "k vehicles per annual comparison",
                    "interpretation": "Entire-history residual; contains regime changes and is not a pure zero-start seed",
                }
            )
    return pd.DataFrame(rows).drop_duplicates()


def validate_diagnostic(tables: dict[str, pd.DataFrame], bundle: ConfigBundle) -> dict:
    observations = tables["observations"]
    if observations.duplicated(["series_id", "year"]).any():
        raise ValueError("Diagnostic observations contain duplicate series/year keys")
    anchors = tables["target_references"]
    if anchors.duplicated(["region", "target_id"]).any() or len(anchors) != 24 * len(
        bundle.scenario.geography.regions
    ):
        raise ValueError("Target references do not cover the legacy application")
    if anchors.future_members.eq("").any():
        raise ValueError("Target future technology associations are missing")
    candidates = tables["candidate_calibrations"]
    if len(candidates) != 3 * len(anchors) or not candidates.diagnostic_only.all():
        raise ValueError("Incomplete or non-diagnostic growth candidates")
    return {
        "diagnostic_only": True,
        "observation_rows": len(observations),
        "series_by_release": observations.groupby("release")
        .series_id.nunique()
        .to_dict(),
        "source_assets": len(tables["source_manifest"]),
        "target_reference_rows": len(anchors),
        "target_combinations": anchors.target_id.nunique(),
        "regions": sorted(anchors.region.unique()),
        "release_comparisons": tables["release_comparison"]
        .comparison.value_counts()
        .to_dict(),
        "fit_rows": len(tables["curve_fits"]),
        "window_rows": len(tables["window_rates"]),
        "supported_proxy_candidates": int(
            candidates.candidate_status.eq("supported_proxy_for_review").sum()
        ),
        "insufficient_candidates": int(
            candidates.candidate_status.eq("insufficient_target_evidence").sum()
        ),
        "production_growth_rows_written": 0,
        "sqlite_connections": 0,
    }


def implementation_hashes(bundle: ConfigBundle) -> dict[str, str]:
    files = [
        "src/diagnostics/technology_growth.py",
        "src/fetching/technology_growth_evidence.py",
        "src/validation/config_models.py",
        "config/parameters/rules.yaml",
        "config/sources.yaml",
        "config/paths.yaml",
        "uv.lock",
        bundle.scenario_path.relative_to(bundle.repo_root).as_posix(),
        resolve_parameter_path(bundle, "technology_growth_diagnostic.yaml")
        .relative_to(bundle.repo_root)
        .as_posix(),
    ]
    return {
        name: file_sha256(resolve_repo_path(bundle.repo_root, name)) for name in files
    }


def build_diagnostic(bundle: ConfigBundle) -> dict[str, pd.DataFrame]:
    core_hashes = implementation_hashes(bundle)
    config = load_diagnostic_config(bundle)
    manifest = acquire_evidence(bundle, offline=True)
    original = normalize_hatch(
        asset_path(bundle, config.source_assets["original"]), "original"
    )
    extended = normalize_hatch(
        asset_path(bundle, config.source_assets["extended"]), "extended"
    )
    eafo_frames = []
    for vehicle, suffix in [("M1", "m1"), ("N1", "n1"), ("N2_N3", "n2_n3")]:
        for quantity, stem in [
            ("fleet", f"fleet_{suffix}_total_number"),
            ("fleet_share", f"af_percentage_total_fleet_{suffix}"),
            ("registrations", f"new_registrations_{suffix}"),
            ("registrations_share", f"market_share_new_registrations_{suffix}"),
        ]:
            eafo_frames.append(
                normalize_eafo(
                    asset_path(bundle, "eafo:" + stem),
                    vehicle_class=vehicle,
                    quantity=quantity,
                    snapshot_year=config.snapshot_year,
                )
            )
    eafo = pd.concat(eafo_frames, ignore_index=True)
    domestic, domestic_hashes = canadian_registrations(bundle, config)
    anchors, members, anchor_hashes = existing_capacity_references(bundle, config)
    observations = pd.concat([original, extended, eafo], ignore_index=True)
    audit = audit_series(observations, config)
    selected = observations.loc[
        (
            (
                observations.technology.isin(config.fitting.technologies)
                & observations.country.isin(config.fitting.countries)
            )
            | (
                observations.release.eq("eafo")
                & observations.powertrain.isin(["BEV", "PHEV", "H2", "CNG"])
            )
        )
        & (~observations.release.eq("eafo") | observations.complete_year.fillna(False))
    ].copy()
    # Domestic cars and pooled LT are NEW registrations; truck table 23-10-0308
    missing_selectors = set(config.fitting.technologies) - set(observations.technology)
    if missing_selectors:
        raise ValueError(
            f"Growth diagnostic technology selectors missing from both releases: {sorted(missing_selectors)}"
        )
    audit["in_selected_scope"] = audit.series_id.isin(selected.series_id.unique())
    LOGGER.info(
        "Selected %s observation series for growth-window analysis; %s S-curve eligible",
        selected.series_id.nunique(),
        int((audit.in_selected_scope & audit.research_fit_eligible).sum()),
    )
    # measures registered stock. It is never reclassified as gross additions.
    domestic_pool = []
    for a in anchors.itertuples(index=False):
        subset = domestic.loc[
            domestic.region.eq(a.region)
            & domestic.road_class.eq(a.road_class)
            & domestic.fuel_type.eq(config.fuel_names[a.powertrain])
            & domestic.quantity.eq("annual_new_registrations")
        ]
        for record in subset.itertuples(index=False):
            domestic_pool.append(
                {
                    "series_id": f"canada:{a.region}:{a.road_class}:{a.powertrain}",
                    "year": record.year,
                    "value": record.quantity_k_vehicles * 1000,
                }
            )
    window_input = pd.concat(
        [selected[["series_id", "year", "value"]], pd.DataFrame(domestic_pool)],
        ignore_index=True,
    )
    windows = observation_windows(window_input, config.fitting)
    LOGGER.info(
        "Observed windows: %s; status counts %s",
        len(windows),
        windows.status.value_counts().to_dict(),
    )
    comparisons = compare_releases(original, extended)
    inventory, original_metadata, extended_metadata = release_inventory(bundle, config)
    orig_estimates, ext_estimates = published_growth_estimates(bundle, config)
    fits = fit_pool(selected, audit, config)
    domestic_observations = pd.DataFrame(domestic_pool)
    additions = period_additions(
        pd.concat(
            [
                eafo.loc[
                    eafo.quantity.eq("registrations") & eafo.complete_year,
                    ["series_id", "year", "value"],
                ],
                domestic_observations,
            ],
            ignore_index=True,
        ),
        width=config.calibration.historical_period_width,
    )
    candidates = calibrate_candidates(anchors, windows, eafo, domestic, config)
    tables = {
        "source_manifest": manifest,
        "observations": observations,
        "series_audit": audit,
        "release_comparison": comparisons,
        "original_published_estimates": orig_estimates,
        "extended_published_estimates": ext_estimates,
        "eafo_observations": eafo,
        "canadian_registration_evidence": domestic,
        "target_references": anchors,
        "target_members": members,
        "window_rates": windows,
        "curve_fits": fits,
        "release_fit_comparison": compare_release_fits(comparisons, fits),
        "period_additions": additions,
        "candidate_calibrations": candidates,
        "release_inventory": inventory,
        "original_native_metadata": original_metadata,
        "extended_native_metadata": extended_metadata,
        "seed_frontiers": seed_frontiers(eafo, additions, config),
        "paper_pages": extract_paper_pages(bundle, persist=False),
    }
    raw_names = {
        "source_manifest",
        "observations",
        "eafo_observations",
        "canadian_registration_evidence",
        "original_published_estimates",
        "extended_published_estimates",
    }
    raw_names |= {
        "release_inventory",
        "original_native_metadata",
        "extended_native_metadata",
        "paper_pages",
    }
    input_hashes = {**core_hashes, **anchor_hashes, **domestic_hashes}
    for name, digest in input_hashes.items():
        if file_sha256(resolve_repo_path(bundle.repo_root, name)) != digest:
            raise ValueError(
                f"Growth diagnostic input changed during analysis: {name}; rerun from fixed inputs"
            )
    for _, asset in registered_assets(bundle):
        if file_sha256(asset_path(bundle, asset.key)) != asset.sha256:
            raise ValueError(
                f"Registered evidence changed during analysis: {asset.key}"
            )
    summary = validate_diagnostic(tables, bundle)
    artifact_hashes = {}
    for name, frame in tables.items():
        family = (
            "technology_growth_interim"
            if name in raw_names
            else "technology_growth_validation"
        )
        artifact = resolve_artifact_path(bundle, family, name + ".csv")
        write_dataframe_atomic(frame, artifact)
        artifact_hashes[artifact.relative_to(bundle.repo_root).as_posix()] = (
            file_sha256(artifact)
        )
        LOGGER.info("Wrote diagnostic %s: %s rows", name, len(frame))
    summary["artifact_hashes"] = artifact_hashes
    summary["published_extended_filter_count"] = int(
        ext_estimates.paper_filter_pass.sum()
    )
    summary["published_extended_characteristic_rows"] = len(ext_estimates)
    summary["native_series_by_release"] = (
        inventory.groupby("release").ID.size().to_dict()
    )
    summary["input_hashes"] = input_hashes
    summary["temoa_version"] = bundle.sources.sources["temoa_growth_reference"].version
    summary["eafo_snapshot"] = bundle.sources.sources[
        "eafo_norway_growth_evidence"
    ].version
    summary["unresolved"] = [
        "Candidate values are diagnostic: review analogue transfer, mature HEV/diesel scope and class boundaries.",
        "Absent FCEV/CNG existing stock is missing representation, not a measured zero.",
        "MHDV 23-10-0308 is registered stock, not new sales; dashboard override is a different-year sales share.",
        "Pooled light-truck registrations cannot be independently assigned to passenger and freight model classes.",
        "Temoa rates are direct transitions; unequal periods/first-period reference years need an upstream contract.",
        "Temoa EX/N group identity and lifetime-weighted first capacity reference require review before promotion.",
        "Temoa new/delta fallback uses surviving existing-vintage cohorts, which are not gross additions.",
        "Delta R=0 is a review recommendation; seeds require consecutive, comparable gross-additions totals.",
        "Seed batching/demand/supply allowance and repeated seed effects require explicit scenario policy.",
    ]
    write_text_atomic(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False),
        resolve_artifact_path(bundle, "technology_growth_validation", "integrity.json"),
    )
    output = resolve_artifact_path(
        bundle, "technology_growth_report", "diagnostic_summary.json"
    )
    write_text_atomic(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False), output
    )
    return tables


def load_diagnostic_tables(bundle: ConfigBundle) -> dict[str, pd.DataFrame]:
    """Read the persisted handoff; fail on missing or stale registered inputs."""
    config = load_diagnostic_config(bundle)
    for _, asset in registered_assets(bundle):
        path = asset_path(bundle, asset.key)
        if not path.exists() or file_sha256(path) != asset.sha256:
            raise ValueError(
                f"Missing/changed registered evidence: {asset.key}; acquire and rebuild explicitly"
            )
    summary = json.loads(
        resolve_artifact_path(
            bundle, "technology_growth_validation", "integrity.json"
        ).read_text(encoding="utf-8")
    )
    for name, digest in summary["input_hashes"].items():
        if file_sha256(resolve_repo_path(bundle.repo_root, name)) != digest:
            raise ValueError(
                f"Stale growth diagnostic handoff: {name}; rebuild explicitly"
            )
    tables = {}
    for name, digest in summary["artifact_hashes"].items():
        path = resolve_repo_path(bundle.repo_root, name)
        if file_sha256(path) != digest:
            raise ValueError(
                f"Changed growth diagnostic artifact: {name}; rebuild explicitly"
            )
        tables[path.stem] = pd.read_csv(path, low_memory=False)
    if not config.diagnostic_only:
        raise ValueError("Growth diagnostic cannot be promoted by a notebook setting")
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    args = parser.parse_args()
    bundle = load_config_bundle(args.scenario)
    log = (
        resolve_repo_path(bundle.repo_root, bundle.paths.outputs.logs)
        / "technology_growth_diagnostic.log"
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler(), logging.FileHandler(log, encoding="utf-8")],
    )
    build_diagnostic(bundle)


if __name__ == "__main__":
    main()
