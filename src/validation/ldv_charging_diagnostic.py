"""Read-only comparisons of grid charging, battery mobility and vehicle usage."""

from dataclasses import dataclass

import altair as alt
import numpy as np
import pandas as pd

from fetching.ramp_run_profiles import RunProfileEvidence, validate_bundle
from utils import find_repo_root, load_config_bundle, load_yaml
from validation.notebook_figures import identified_chart

SURVEYS = {"nhts": "NHTS 2022", "tts": "TTS 2016"}
STRATEGIES = {"uncontrolled": "Uncontrolled", "night": "Night", "day": "Day"}
METRICS = {"charging_kw": "Grid charging", "mobility_kw": "Battery mobility", "active_drivers": "Active drivers"}
STRATEGY_COLORS = ["#246c91", "#7e57a3", "#c47b1a"]
METRIC_COLORS = ["#246c91", "#be5a3c", "#32856d"]


@dataclass(frozen=True)
class LdvChargingDiagnostic:
    evidence: RunProfileEvidence
    hourly: pd.DataFrame
    daily: pd.DataFrame
    annual: pd.DataFrame


def build_diagnostic(evidence: RunProfileEvidence) -> LdvChargingDiagnostic:
    hourly = evidence.hourly.copy()
    timezones = {case["calendar_timezone"] for case in evidence.manifest["cases"].values()}
    if len(timezones) != 1:
        raise ValueError("Comparative RAMP diagnostics require a shared local calendar")
    local = pd.to_datetime(hourly.timestamp_utc, utc=True).dt.tz_convert(next(iter(timezones)))
    hourly["local_date"] = local.dt.strftime("%Y-%m-%d")
    hourly["local_hour"] = local.dt.hour
    hourly["month"] = local.dt.month
    hourly["weekday"] = local.dt.dayofweek
    hourly["survey_label"] = hourly.survey.map(SURVEYS)
    hourly["strategy_label"] = hourly.strategy.map(STRATEGIES)
    hourly["case"] = hourly.survey_label + " / " + hourly.strategy_label
    for column in METRICS:
        means = hourly.groupby("component")[column].transform("mean")
        if means.le(0).any():
            raise ValueError("Unit-mean profiles require positive full-year means")
        hourly[f"{column}_unit_mean"] = hourly[column] / means
    groups = ["component", "survey", "strategy", "survey_label", "strategy_label", "case"]
    daily = hourly.groupby(groups + ["local_date"], sort=False, as_index=False).agg(
        grid_kwh=("charging_kw", "sum"), mobility_kwh=("mobility_kw", "sum"),
        driver_hours=("active_drivers", "sum"), physical_hours=("hour_index", "count"),
        month=("month", "first"), weekday=("weekday", "first"),
    )
    annual = hourly.groupby(groups, sort=False, as_index=False).agg(
        hours=("hour_index", "count"), grid_kwh=("charging_kw", "sum"), mobility_kwh=("mobility_kw", "sum"),
        grid_mean_kw=("charging_kw", "mean"), grid_peak_kw=("charging_kw", "max"),
        driving_share=("driving_share", "mean"), active_driver_peak=("active_drivers", "max"),
    )
    annual["grid_to_mobility"] = annual.grid_kwh / annual.mobility_kwh
    annual["grid_load_factor"] = annual.grid_mean_kw / annual.grid_peak_kw
    daily_correlations = daily.groupby("component").apply(
        lambda frame: frame.grid_kwh.corr(frame.mobility_kwh), include_groups=False
    )
    annual["daily_energy_correlation"] = annual.component.map(daily_correlations)
    return LdvChargingDiagnostic(evidence, hourly, daily, annual)


def load_diagnostic() -> LdvChargingDiagnostic:
    root = find_repo_root()
    scenario = load_yaml(root / "config/parameters/ldv_charging_diagnostic.yaml")["scenario"]
    return build_diagnostic(validate_bundle(load_config_bundle(root / scenario)))


def select_subset(frame: pd.DataFrame, label: str, seasons: dict[str, list[int]]) -> pd.DataFrame:
    if label == "All days":
        return frame.copy()
    if label == "Weekdays":
        return frame.loc[frame.weekday < 5].copy()
    if label == "Weekends":
        return frame.loc[frame.weekday >= 5].copy()
    if label not in seasons:
        raise ValueError(f"Unknown diagnostic population: {label}")
    return frame.loc[frame.month.isin(seasons[label])].copy()


def _strategy_color(title: str = "Charging strategy"):
    return alt.Color("strategy_label:N", title=title,
                     scale=alt.Scale(domain=list(STRATEGIES.values()), range=STRATEGY_COLORS),
                     legend=alt.Legend(orient="bottom", direction="horizontal", columns=3))


def diurnal_chart(diagnostic: LdvChargingDiagnostic, subset: str, scale: str, width: int, height: int):
    selected = select_subset(diagnostic.hourly, subset, diagnostic.evidence.settings.presentation.seasons)
    if scale not in {"Own annual mean", "Native kW"}:
        raise ValueError("Unknown charging scale")
    value = "charging_kw_unit_mean" if scale == "Own annual mean" else "charging_kw"
    curve = selected.groupby(["survey_label", "strategy_label", "local_hour"], as_index=False)[value].mean()
    chart = alt.Chart(curve).mark_line(strokeWidth=2.4).encode(
        x=alt.X("local_hour:Q", title="Toronto local hour", scale=alt.Scale(domain=[0, 23]),
                axis=alt.Axis(values=[0, 4, 8, 12, 16, 20, 23])),
        y=alt.Y(f"{value}:Q", title="Charging / own annual mean" if scale == "Own annual mean" else "Grid charging (kW)"),
        color=_strategy_color(), detail="strategy_label:N", order="local_hour:Q",
        tooltip=["survey_label:N", "strategy_label:N", "local_hour:Q", alt.Tooltip(f"{value}:Q", format=".3f")],
    ).properties(width=width, height=height).facet(column=alt.Column("survey_label:N", title=None))
    return identified_chart(chart, "ldv-charge-1", f"When does each strategy charge? · {subset}")


def behavior_chart(diagnostic: LdvChargingDiagnostic, subset: str, width: int, height: int):
    """Use the requested width for the complete two-survey plotting region."""
    selected = select_subset(diagnostic.hourly, subset, diagnostic.evidence.settings.presentation.seasons)
    title = f"Mobility and usage set a different clock from charging · {subset}"
    if selected.empty:
        chart = alt.Chart(alt.Data(values=[{"message": "No hourly observations in the selected calendar population"}])).mark_text(
            color="#64748b", fontSize=13,
        ).encode(text="message:N").properties(width=width, height=height)
        return identified_chart(chart, "ldv-charge-2", title)
    columns = [f"{name}_unit_mean" for name in METRICS]
    curve = selected.groupby(["survey_label", "strategy_label", "local_hour"], as_index=False)[columns].mean()
    curve = curve.melt(id_vars=["survey_label", "strategy_label", "local_hour"], value_vars=columns,
                       var_name="metric", value_name="relative")
    curve["metric"] = curve.metric.str.removesuffix("_unit_mean").map(METRICS)
    legend = alt.Legend(orient="bottom", direction="horizontal", columns=3,
                        symbolType="stroke", symbolSize=400, symbolStrokeWidth=3)
    base = alt.Chart(curve).mark_line(strokeWidth=2.1, point=alt.OverlayMarkDef(size=15, filled=True)).encode(
        x=alt.X("local_hour:Q", title="Toronto local hour", scale=alt.Scale(domain=[0, 23]),
                axis=alt.Axis(values=[0, 6, 12, 18, 23])),
        y=alt.Y("relative:Q", title="Profile / own full-year mean"),
        color=alt.Color("metric:N", title="Profile", scale=alt.Scale(domain=list(METRICS.values()), range=METRIC_COLORS),
                        legend=legend),
        strokeDash=alt.StrokeDash("metric:N", title="Profile", scale=alt.Scale(
            domain=list(METRICS.values()), range=[[1, 0], [6, 3], [2, 3]]), legend=legend),
        detail="metric:N", order="local_hour:Q",
        tooltip=["metric:N", "local_hour:Q", alt.Tooltip("relative:Q", format=".3f")],
    ).properties(width=(width - 20) / len(SURVEYS), height=height)
    chart = base.facet(row=alt.Row("strategy_label:N", title=None, sort=list(STRATEGIES.values())),
                       column=alt.Column("survey_label:N", title=None, sort=list(SURVEYS.values())), spacing=20)
    return identified_chart(chart, "ldv-charge-2", title)


def week_chart(diagnostic: LdvChargingDiagnostic, start: str, width: int, height: int):
    chosen = diagnostic.hourly.loc[diagnostic.hourly.local_date.between(start, (pd.Timestamp(start) + pd.Timedelta(days=6)).strftime("%Y-%m-%d"))].copy()
    if chosen.empty or not chosen.groupby("component").local_date.nunique().eq(7).all():
        raise ValueError("The selected week is outside the complete seven-local-day simulation calendar")
    chosen["elapsed_hour"] = chosen.groupby("component", sort=False).cumcount()
    curve = chosen.melt(id_vars=["survey_label", "strategy_label", "elapsed_hour", "timestamp_local"],
                        value_vars=[f"{name}_unit_mean" for name in METRICS], var_name="metric", value_name="relative")
    curve["metric"] = curve.metric.str.removesuffix("_unit_mean").map(METRICS)
    chart = alt.Chart(curve).mark_line(strokeWidth=1.4).encode(
        x=alt.X("elapsed_hour:Q", title="Physical hour from selected local midnight"),
        y=alt.Y("relative:Q", title="Profile / own full-year mean"),
        color=alt.Color("metric:N", title="Profile", scale=alt.Scale(domain=list(METRICS.values()), range=METRIC_COLORS),
                        legend=alt.Legend(orient="bottom", direction="horizontal", columns=3)),
        detail="metric:N", order="elapsed_hour:Q",
        tooltip=["timestamp_local:N", "metric:N", alt.Tooltip("relative:Q", format=".3f")],
    ).properties(width=width, height=height).facet(
        row=alt.Row("strategy_label:N", title=None, sort=list(STRATEGIES.values())),
        column=alt.Column("survey_label:N", title=None))
    return identified_chart(chart, "ldv-charge-3", f"A week exposes day-to-day timing · {start}")


def heatmap_chart(diagnostic: LdvChargingDiagnostic, subset: str, width: int, height: int):
    selected = select_subset(diagnostic.hourly, subset, diagnostic.evidence.settings.presentation.seasons)
    title = f"Charging timing across all six runs · {subset}"
    if selected.empty:
        chart = alt.Chart(alt.Data(values=[{"message": "No hourly observations in the selected calendar population"}])).mark_text(
            color="#64748b", fontSize=13,
        ).encode(text="message:N").properties(width=width, height=height)
        return identified_chart(chart, "ldv-charge-4", title)
    values = selected.groupby(["case", "local_hour"], sort=False, as_index=False).agg(
        charging_relative=("charging_kw_unit_mean", "mean"), physical_hours=("hour_index", "count"))
    chart = alt.Chart(values).mark_rect().encode(
        x=alt.X("local_hour:O", title="Toronto local hour", scale=alt.Scale(domain=list(range(24))),
                axis=alt.Axis(values=[0, 4, 8, 12, 16, 20, 23], labelAngle=0)),
        y=alt.Y("case:N", title=None, sort=diagnostic.annual.case.tolist()),
        color=alt.Color("charging_relative:Q", title="Charging / annual mean", scale=alt.Scale(scheme="yelloworangered"),
                        legend=alt.Legend(orient="bottom", direction="horizontal")),
        tooltip=["case:N", "local_hour:O", alt.Tooltip("charging_relative:Q", format=".3f"), "physical_hours:Q"],
    ).properties(width=width, height=height)
    return identified_chart(chart, "ldv-charge-4", title)


def monthly_chart(diagnostic: LdvChargingDiagnostic, width: int, height: int):
    data = diagnostic.daily.groupby(["survey_label", "strategy_label", "month"], as_index=False).agg(
        grid_kwh=("grid_kwh", "mean"), mobility_kwh=("mobility_kwh", "mean"))
    data = data.melt(id_vars=["survey_label", "strategy_label", "month"], value_vars=["grid_kwh", "mobility_kwh"],
                     var_name="metric", value_name="daily_kwh")
    data["metric"] = data.metric.map({"grid_kwh": "Grid charging", "mobility_kwh": "Battery mobility"})
    chart = alt.Chart(data).mark_line(strokeWidth=2.2).encode(
        x=alt.X("month:O", title="Calendar month"), y=alt.Y("daily_kwh:Q", title="Mean daily fleet energy (kWh/day)"),
        color=_strategy_color(), detail="strategy_label:N", order="month:Q",
        tooltip=["metric:N", "strategy_label:N", "month:O", alt.Tooltip("daily_kwh:Q", format=".1f")],
    ).properties(width=width, height=height).facet(row=alt.Row("metric:N", title=None),
                                               column=alt.Column("survey_label:N", title=None))
    return identified_chart(chart, "ldv-charge-5", "Seasonal energy demand and charging strategy")


def daily_relationship_chart(diagnostic: LdvChargingDiagnostic, subset: str, width: int, height: int):
    selected = select_subset(diagnostic.daily, subset, diagnostic.evidence.settings.presentation.seasons)
    chart = alt.Chart(selected).mark_point(size=22, opacity=.38, filled=True).encode(
        x=alt.X("mobility_kwh:Q", title="Daily battery mobility energy (kWh)"),
        y=alt.Y("grid_kwh:Q", title="Daily grid charging energy (kWh)"),
        color=_strategy_color("Charging strategy · circle markers"),
        tooltip=["local_date:N", "strategy_label:N", alt.Tooltip("mobility_kwh:Q", format=".1f"),
                 alt.Tooltip("grid_kwh:Q", format=".1f"), alt.Tooltip("driver_hours:Q", format=".1f"), "physical_hours:Q"],
    ).properties(width=width, height=height).facet(column=alt.Column("survey_label:N", title=None))
    return identified_chart(chart, "ldv-charge-6", f"Daily mobility demand and charging move together · {subset}")


def daily_cluster_note(diagnostic: LdvChargingDiagnostic) -> str:
    """Explain the frozen runs' calendar split using full-year daily evidence."""
    daily = diagnostic.daily.loc[diagnostic.daily.strategy.eq("uncontrolled")].copy()
    daily["weekend"] = daily.weekday.ge(5)
    means = daily.groupby(["survey", "weekend"]).mobility_kwh.mean() / 1000
    return (
        "TTS 2016's two main clouds chiefly separate ordinary weekdays from weekend/holiday behavior. "
        f"Across the full year, calendar weekdays average {means.loc['tts', False]:.2f} MWh/day of mobility energy "
        f"and weekends {means.loc['tts', True]:.2f} MWh/day; weekday holidays also join the lower cloud. "
        "Copied weekday survey inputs still pass through RAMP's lower weekend main-activity participation "
        "and different weekend windows; holidays use Sunday behavior. These rules reduce simulated driving "
        "and charging, while weather and stochastic variation broaden each cloud.\n\n"
        f"NHTS has a smaller calendar-day contrast ({means.loc['nhts', False]:.2f} versus "
        f"{means.loc['nhts', True]:.2f} MWh/day), so its clouds overlap more. Its longer weekend distance input "
        "helps offset the same participation reduction; TTS uses the same daily distance for both. "
        "Measuring bias from the TTS weekday-data proxy would require a sensitivity using independent weekend data."
    )


def lag_correlations(diagnostic: LdvChargingDiagnostic) -> pd.DataFrame:
    records = []
    for component, frame in diagnostic.hourly.groupby("component", sort=False):
        for lag in range(diagnostic.evidence.settings.max_lag_hours + 1):
            charging = frame.charging_kw.to_numpy()[lag:]
            mobility = frame.mobility_kw.to_numpy()[:len(frame) - lag]
            correlation = float(np.corrcoef(charging, mobility)[0, 1])
            records.append({"component": component, "survey_label": frame.survey_label.iloc[0],
                            "strategy_label": frame.strategy_label.iloc[0], "lag_hours": lag,
                            "correlation": correlation, "paired_hours": len(charging)})
    return pd.DataFrame(records)


def lag_chart(diagnostic: LdvChargingDiagnostic, width: int, height: int):
    chart = alt.Chart(lag_correlations(diagnostic)).mark_line(strokeWidth=2).encode(
        x=alt.X("lag_hours:Q", title="Charging lag after mobility (physical hours)", axis=alt.Axis(values=[0, 6, 12, 18, 24])),
        y=alt.Y("correlation:Q", title="Hourly Pearson correlation", scale=alt.Scale(domain=[-1, 1])),
        color=_strategy_color(), detail="strategy_label:N", order="lag_hours:Q",
        tooltip=["strategy_label:N", "lag_hours:Q", alt.Tooltip("correlation:Q", format=".3f"), "paired_hours:Q"],
    ).properties(width=width, height=height).facet(column=alt.Column("survey_label:N", title=None))
    return identified_chart(chart, "ldv-charge-7", "Preferred charging windows shift the mobility–charging relationship")


def annual_table(diagnostic: LdvChargingDiagnostic) -> pd.DataFrame:
    frame = diagnostic.annual.copy()
    frame["grid_mwh"] = frame.grid_kwh / 1000
    frame["mobility_mwh"] = frame.mobility_kwh / 1000
    return frame[["survey_label", "strategy_label", "grid_mwh", "mobility_mwh", "grid_mean_kw",
                  "grid_peak_kw", "grid_load_factor"]].rename(columns={
        "survey_label": "Survey", "strategy_label": "Strategy",
        "grid_mwh": "Grid energy (MWh)", "mobility_mwh": "Mobility energy (MWh)",
        "grid_mean_kw": "Mean charging (kW)", "grid_peak_kw": "Hourly peak (kW)",
        "grid_load_factor": "Mean / hourly peak",
    })


def usage_table(diagnostic: LdvChargingDiagnostic) -> pd.DataFrame:
    return diagnostic.annual[["survey_label", "strategy_label", "driving_share", "grid_to_mobility", "daily_energy_correlation"]].rename(columns={
        "survey_label": "Survey", "strategy_label": "Strategy", "driving_share": "Mean driving share (fraction)",
        "grid_to_mobility": "Grid / mobility energy", "daily_energy_correlation": "Daily energy correlation",
    })


def takeaways(diagnostic: LdvChargingDiagnostic) -> str:
    statements = []
    for survey, frame in diagnostic.annual.groupby("survey", sort=False):
        uncontrolled = frame.loc[frame.strategy.eq("uncontrolled")].iloc[0]
        restricted = frame.loc[~frame.strategy.eq("uncontrolled")]
        energy_spread = (frame.grid_kwh.max() - frame.grid_kwh.min()) / frame.grid_kwh.min()
        ratios = restricted.grid_peak_kw / uncontrolled.grid_peak_kw
        statements.append(
            f"**{SURVEYS[survey]}:** annual grid energy spans only {energy_spread:.3%} across strategies, "
            f"while night/day hourly peaks are {ratios.min():.2f}–{ratios.max():.2f} times the uncontrolled peak."
        )
    reference = diagnostic.annual.loc[diagnostic.annual.strategy.eq("uncontrolled")].set_index("survey")
    mobility_difference = reference.loc["nhts", "mobility_kwh"] / reference.loc["tts", "mobility_kwh"] - 1
    usage_difference = reference.loc["nhts", "driving_share"] / reference.loc["tts", "driving_share"] - 1
    statements.append(
        f"NHTS's realized annual mobility energy is {mobility_difference:+.1%} relative to TTS, "
        f"alongside {usage_difference:+.1%} mean simultaneous driving share. "
        "These are different realized travel patterns, so the survey contrast includes demand as well as timing."
    )
    return "\n\n".join(statements)


def lag_peak_table(diagnostic: LdvChargingDiagnostic) -> pd.DataFrame:
    correlations = lag_correlations(diagnostic)
    best = correlations.loc[correlations.groupby("component", sort=False).correlation.idxmax()]
    return best[["survey_label", "strategy_label", "lag_hours", "correlation", "paired_hours"]].reset_index(drop=True).rename(columns={
        "survey_label": "Survey", "strategy_label": "Strategy", "lag_hours": "Lag at highest correlation (h)",
        "correlation": "Hourly correlation", "paired_hours": "Paired physical hours",
    })
