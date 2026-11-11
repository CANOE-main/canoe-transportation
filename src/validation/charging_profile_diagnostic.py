"""Read-only illustration of the production full-year charging-profile rescaling."""

from dataclasses import dataclass
from typing import Literal

import altair as alt
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from fetching.legacy_charging_profiles import build_request, transform_hourly
from parameterization.ev_chargers import prepare_charger_utilization_targets
from parameterization.ldv_charging_profiles import scale_projected_profile
from utils import file_sha256, find_repo_root, load_config_bundle, load_harmonization_rules, load_yaml
from validation.notebook_figures import identified_chart


class ChargingProfilePresentation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scenario: str
    region: str
    profiles: list[Literal["nhts", "tts"]] = Field(min_length=1)
    week_start: str
    figure_id: str


@dataclass(frozen=True)
class ChargingProfileDiagnostic:
    week: pd.DataFrame
    annual: pd.DataFrame
    metadata: dict
    settings: ChargingProfilePresentation


def load_diagnostic() -> ChargingProfileDiagnostic:
    """Validate registered source bytes; use production transformations without writes."""
    root = find_repo_root()
    config = root / "config/parameters/documentation.yaml"
    settings = ChargingProfilePresentation.model_validate(load_yaml(config)["charging_profile"])
    bundle = load_config_bundle(root / settings.scenario)
    rules = load_harmonization_rules(bundle, "legacy_charging_profiles")
    if (rules["hourly_operation"], rules["normalization"], rules["label_basis"]) != (
        "mean", "annual_hourly_peak", "elapsed_hours_from_local_year_start",
    ):
        raise ValueError("Charging illustration requires an implemented, reviewed production normalization rule")
    targets, context = prepare_charger_utilization_targets(bundle)
    technology = load_harmonization_rules(bundle, "ev_chargers")["technology"]["ldv"]["new"]
    selected = targets.loc[targets.region.eq(settings.region) & targets.tech_or_group.eq(technology)]
    if selected.empty or selected.target_utilization.nunique() != 1:
        raise ValueError("Charging illustration requires one production LDV utilization across vintages")
    target = float(selected.target_utilization.iloc[0])
    weeks, annual, sources = [], [], {}
    for profile in settings.profiles:
        request = build_request(bundle, profile)
        path = request.registered_path
        if path.stat().st_size != request.expected_bytes or file_sha256(path) != request.expected_sha256:
            raise ValueError(f"Registered {profile} profile differs from its frozen source identity")
        raw = pd.read_csv(path, index_col=0)
        if len(raw) != request.expected_rows:
            raise ValueError(f"Registered {profile} profile has the wrong row count")
        evidence = transform_hourly(raw, year=request.calendar_year, timezone=request.calendar_timezone,
                                    value_column=rules["value_column"], precision=rules["precision"])
        hourly = evidence.hourly
        mean = float(hourly.factor.mean())
        scaled, audit = scale_projected_profile(hourly, profile_mean=mean, target_utilization=target)
        start = pd.Timestamp(settings.week_start, tz=request.calendar_timezone)
        end = start + pd.Timedelta(days=7)
        local = pd.to_datetime(hourly.timestamp_utc, utc=True).dt.tz_convert(request.calendar_timezone)
        mask = (local >= start) & (local < end)
        if mask.sum() != 168:
            raise ValueError("Illustrative week must contain 168 captured hourly observations")
        week = pd.DataFrame({"hour": range(168), "profile": profile,
                             "native": hourly.loc[mask, "factor"].to_numpy(),
                             "rescaled": scaled.loc[mask, "factor"].to_numpy()})
        weeks.append(week.melt(id_vars=["hour", "profile"], var_name="series", value_name="factor"))
        annual.append({"profile": profile, "native_mean": mean, "target_mean": target,
                       "rescaled_mean": float(scaled.factor.mean()), "multiplier": audit["scale_factor"],
                       "native_peak": float(hourly.factor.max()), "rescaled_peak": float(scaled.factor.max()),
                       "native_week_mean": float(week.native.mean()), "rescaled_week_mean": float(week.rescaled.mean())})
        sources[profile] = {"path": path.relative_to(root).as_posix(), "sha256": request.expected_sha256,
                            "calendar_year": request.calendar_year, "timezone": request.calendar_timezone,
                            "hours": len(hourly)}
    metadata = {"sources": sources, "settings_sha256": file_sha256(config),
                "scenario_sha256": file_sha256(root / settings.scenario), "utilization_data_id": context.data_id,
                "target_technology": technology, "annual_utilization": target,
                "normalization_rules": rules,
                "method": "Production hourly mean/annual-peak normalization, then target/full-year-mean rescaling; no ETL writes"}
    return ChargingProfileDiagnostic(pd.concat(weeks, ignore_index=True), pd.DataFrame(annual), metadata, settings)


def profile_chart(diagnostic: ChargingProfileDiagnostic):
    panels = []
    labels = {"nhts": "US NHTS simulation", "tts": "Ontario TTS simulation"}
    for row in diagnostic.annual.itertuples():
        frame = diagnostic.week.loc[diagnostic.week.profile.eq(row.profile)].copy()
        frame["series"] = frame.series.map({"native": "Native peak-normalized shape", "rescaled": "Rescaled to annual UF"})
        base = alt.Chart(frame).mark_line(strokeWidth=2).encode(
            x=alt.X("hour:Q", title="Hour in illustrative week (Monday 00:00 to Sunday 23:00)",
                    scale=alt.Scale(domain=[0, 167]), axis=alt.Axis(values=[0, 24, 48, 72, 96, 120, 144, 167])),
            y=alt.Y("factor:Q", title="Hourly capacity factor", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format="%")),
            color=alt.Color("series:N", title=None, scale=alt.Scale(
                domain=["Native peak-normalized shape", "Rescaled to annual UF"], range=["#94a3b8", "#087e8b"]),
                legend=alt.Legend(orient="bottom")),
            tooltip=["profile:N", "hour:Q", "series:N", alt.Tooltip("factor:Q", format=".2%")],
        )
        reference = alt.Chart(pd.DataFrame({"annual_target": [row.target_mean]})).mark_rule(
            color="#b45309", strokeDash=[5, 4]).encode(y="annual_target:Q")
        panels.append((base + reference).properties(width=730, height=200, title=alt.Title(
            labels[row.profile], anchor="start", subtitle=[
                f"Full 8,760-hour mean: {row.native_mean:.2%} → {row.rescaled_mean:.2%}; every hour × {row.multiplier:.4f}",
                f"Shown week's mean: {row.native_week_mean:.2%} → {row.rescaled_week_mean:.2%}; annual target is dashed"],
            subtitleFontSize=11)))
    target = diagnostic.metadata["annual_utilization"]
    title = f"Rescaling preserves charging timing and sets the full-year mean to {target:.0%}"
    return identified_chart(alt.vconcat(*panels, spacing=28), diagnostic.settings.figure_id, title).properties(
        title=alt.Title(title, anchor="start", offset=25, subtitle=[
            f"Figure {diagnostic.settings.figure_id} · week starting {diagnostic.settings.week_start} · registered 2018 simulations",
            "Scale using the complete native year; this illustrative week's own mean need not be 20%."], subtitleFontSize=12))
