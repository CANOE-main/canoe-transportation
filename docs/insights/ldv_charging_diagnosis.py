"""Explain production LDV hourly scaling and travel/charging behavior from RAMP runs."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    from validation.charging_profile_diagnostic import load_diagnostic as load_profile_diagnostic, profile_chart
    from validation.ldv_charging_diagnostic import (
        annual_table, behavior_chart, daily_cluster_note, daily_relationship_chart, diurnal_chart,
        heatmap_chart, lag_chart, lag_peak_table, load_diagnostic, monthly_chart,
        takeaways, usage_table, week_chart,
    )
    return (
        annual_table, behavior_chart, daily_cluster_note, daily_relationship_chart, diurnal_chart,
        heatmap_chart, lag_chart, lag_peak_table, load_diagnostic, load_profile_diagnostic, mo, monthly_chart,
        profile_chart, takeaways, usage_table, week_chart,
    )


@app.cell
def _(load_diagnostic, load_profile_diagnostic):
    diagnostic = load_diagnostic()
    profile_diagnostic = load_profile_diagnostic()
    presentation = diagnostic.evidence.settings.presentation
    return diagnostic, presentation, profile_diagnostic


@app.cell
def _(diagnostic, mo, presentation):
    import datetime as dt
    subset = mo.ui.dropdown(["All days", "Weekdays", "Weekends", *presentation.seasons], value=presentation.default_subset, label="Calendar population")
    scale = mo.ui.dropdown(["Own annual mean", "Native kW"], value=presentation.default_scale, label="Charging scale")
    week_start = mo.ui.date(start=diagnostic.hourly.local_date.min(),
                            stop=dt.date.fromisoformat(diagnostic.hourly.local_date.max()) - dt.timedelta(days=6),
                            value=presentation.default_week_start, label="First local date of illustrative week")
    line_width = mo.ui.slider(240, 900, 20, value=presentation.line_width, label="Line-panel width")
    grid_width = mo.ui.slider(240, 600, 20, value=presentation.grid_width, label="Illustrative-week panel width")
    heatmap_width = mo.ui.slider(300, 1200, 20, value=presentation.heatmap_width, label="Hourly comparison width")
    mo.vstack([
        mo.md("# How do travel behavior and annual utilization shape aggregate LDV charging?\n\n"
              "CANOE reuses registered light-duty vehicle (LDV) simulations: travel and charging rules supply the hourly shape, while reviewed annual utilization sets its amplitude. "
              "Six registered RAMP simulations share the 2018 Toronto calendar, random seed 42 and a 2,499-driver fleet. "
              "The US National Household Travel Survey (NHTS 2022) and Ontario's Transportation Tomorrow Survey (TTS 2016) provide alternative travel patterns; "
              "uncontrolled, night and day charging apply different parking-time preferences. "
              "Compare their annual totals, charging times and relationship with driving. These results describe the simulated fleet; they are not population estimates or evidence of causal effects."),
        mo.hstack([subset, scale, week_start], wrap=True),
        mo.accordion({"Figure sizing": mo.hstack([line_width, grid_width, heatmap_width], wrap=True)}),
    ])
    return grid_width, heatmap_width, line_width, scale, subset, week_start


@app.cell
def _(mo, profile_chart, profile_diagnostic):
    mo.vstack([
        mo.md("## How does annual utilization rescale the hourly shape?\n\n"
              "This comparison applies the production method to the registered uncontrolled charging runs for both surveys. Hourly power is first expressed relative to the annual peak; "
              "every hour is then multiplied by **target annual utilization / full-year mean**. This changes amplitude while preserving charging timing. "
              "The multiplier uses all 8,760 hours; the displayed week illustrates the transformation. The dashed line is the annual target, not a requirement for each week or hour."),
        mo.ui.altair_chart(profile_chart(profile_diagnostic), chart_selection=False, legend_selection=False),
        mo.ui.table(profile_diagnostic.annual[["profile", "native_mean", "target_mean", "rescaled_mean", "multiplier", "rescaled_week_mean"]].rename(columns={
            "profile": "Simulation", "native_mean": "Original annual mean", "target_mean": "Annual target",
            "rescaled_mean": "Rescaled annual mean", "multiplier": "Hourly multiplier", "rescaled_week_mean": "Rescaled week mean",
        }), format_mapping={
            "Original annual mean": "{:.2%}", "Annual target": "{:.2%}", "Rescaled annual mean": "{:.2%}",
            "Hourly multiplier": "{:.4f}", "Rescaled week mean": "{:.2%}",
        }, selection=None, show_data_types=False, show_column_summaries=False,
            label="One row per uncontrolled survey simulation; means are dimensionless capacity factors"),
        mo.md("**Model implication:** annual utilization and hourly timing have separate evidence. Matching the annual mean does not validate the simulated travel or charging schedule. "
              "The six-run comparisons below explain that inherited timing without changing production utilization or introducing new behavior."),
        mo.accordion({"Complete annual/weekly means and peaks": mo.ui.table(profile_diagnostic.annual, selection=None),
                      "Registered source identities, production selection and rescaling": mo.json(profile_diagnostic.metadata)}),
    ])
    return


@app.cell
def _(annual_table, diagnostic, mo, takeaways, usage_table):
    mo.vstack([
        mo.md("## How much charging and driving does each run contain?\n\n"
              "One row represents a full survey/strategy year. Annual energy sums 8,760 hourly mean powers; peak and load factor also use the full year. "
              "Driving share is the mean simultaneous active-driver count divided by 2,499. Grid charging is grid-side kW; mobility is temperature-adjusted battery-side power, converted from source W to kW. "
              "Usage counts driving vehicles, **not charging vehicles or charger occupancy**."),
        mo.ui.table(annual_table(diagnostic).round(4), selection=None, label="Complete-year fleet comparison"),
        mo.md(takeaways(diagnostic)),
        mo.ui.table(usage_table(diagnostic).round(4), selection=None, label="Driving share and daily relationships · one row per complete run"),
        mo.md("The three strategies reproduce identical mobility and usage evidence within each survey. Differences in their charging curves therefore reflect charging controls within this fixed realization. "
              "Night prefers 22:00–07:00; day prefers 07:00–18:00. Emergency charging at low battery state of charge (SOC) may occur outside those windows. "
              "The grid/mobility energy ratio is descriptive: conversion losses, buffer treatment and differences in stored energy at the year's start and end prevent a strict annual equality check."),
    ])
    return


@app.cell
def _(diagnostic, diurnal_chart, heatmap_chart, heatmap_width, line_width, mo, presentation, scale, subset):
    mo.vstack([
        mo.md("## When does each strategy charge?\n\n"
              "Each local-hour point averages observations from the selected days. Both occurrences of a repeated autumn clock hour contribute. "
              "The normalized view divides each run by its **own full-year mean**. It compares timing while retaining differences between the selected days and the annual average; "
              "it does not equate fleet energy totals. No smoothing or rescaling after day selection is applied."),
        mo.ui.altair_chart(heatmap_chart(diagnostic, subset.value, heatmap_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
        mo.ui.altair_chart(diurnal_chart(diagnostic, subset.value, scale.value, line_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
    ])
    return


@app.cell
def _(behavior_chart, diagnostic, grid_width, heatmap_width, mo, presentation, subset, week_chart, week_start):
    mo.vstack([
        mo.md("## How do driving and mobility demand relate to charging?\n\n"
              "These three signals have different physical units. Dividing each signal by its own full-year mean puts **timing**, not absolute magnitude, on a common axis. "
              "Mobility and active drivers rise during travel; charging depends on subsequent parking, SOC, access and the selected window. "
              "The week view follows consecutive physical hours and is independent of the day-selection control."),
        mo.ui.altair_chart(behavior_chart(diagnostic, subset.value, heatmap_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
        mo.ui.altair_chart(week_chart(diagnostic, str(week_start.value), grid_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
    ])
    return


@app.cell
def _(daily_cluster_note, daily_relationship_chart, diagnostic, lag_chart, lag_peak_table, line_width, mo, monthly_chart, presentation, subset):
    mo.vstack([
        mo.md("## How do season and day-to-day travel affect charging?\n\n"
              "Monthly points average daily fleet energy, including 23- and 25-hour daylight-saving days. Mobility curves overlap across strategies because their inputs are identical. "
              "Each scatter point is a local calendar day; inspect the tooltip for driving hours and the physical-hour denominator. "
              "A positive lag pairs charging at hour *t + lag* with mobility at *t*, dropping unpaired edges without wrapping the year. "
              "Daily and lag correlations describe associations. Weather, repeated schedules and stored battery energy prevent interpreting them as causal effects."),
        mo.ui.altair_chart(monthly_chart(diagnostic, line_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
        mo.ui.altair_chart(daily_relationship_chart(diagnostic, subset.value, line_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
        mo.md(daily_cluster_note(diagnostic)),
        mo.ui.altair_chart(lag_chart(diagnostic, line_width.value, presentation.panel_height), chart_selection=False, legend_selection=False),
        mo.ui.table(lag_peak_table(diagnostic).round(3), selection=None, label="Maximum within the configured nonnegative lag range · recurring schedules, not causal waiting times"),
    ])
    return


@app.cell
def _(diagnostic, mo):
    mo.vstack([
        mo.md("## What does this establish for CANOE charging profiles?\n\n"
              "Travel patterns and charging rules determine the timing in these registered simulations. CANOE's annual-utilization rescaling changes their amplitude, "
              "so annual utilization and charging timing need separate explanations and checks. These fleet signals do not identify individual parking access, charger occupancy or type-specific charging schedules."),
        mo.accordion({
        "Simulation assumptions and interpretation limits": mo.md(
            "NHTS runs use the unweighted legacy sample of people with trips and place trip occurrence at its midpoint. TTS weekday-derived tables are reused for weekends, while RAMP applies separate weekend scheduling and participation rules. "
            "Both inherit Ontario demographics, legacy fleet/battery composition, charger access/power and weather assumptions. "
            "The travel-survey notebook examines weighting separately without changing these runs. "
            "Source daylight-saving handling is retained in the registered model snapshot; these comparisons cannot recover individual trip sequences or actual charging concurrency."),
        "Registered identities, units and exclusions": mo.json(diagnostic.evidence.manifest),
        "Daily evidence · one row per run and local day": mo.ui.table(diagnostic.daily, selection=None, page_size=10),
        }),
    ])
    return


if __name__ == "__main__":
    app.run()
