"""Walk through maintained NHTS evidence, interpretation and RAMP input derivation."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    from validation.travel_survey_diagnostic import (
        DERIVATION,
        WEIGHTED,
        activity_chart,
        build_diagnostic,
        clock_chart,
        clock_heatmap,
        distance_chart,
        utility_chart,
    )

    return (
        DERIVATION,
        WEIGHTED,
        activity_chart,
        build_diagnostic,
        clock_chart,
        clock_heatmap,
        distance_chart,
        mo,
        utility_chart,
    )


@app.cell
def _(build_diagnostic):
    survey = build_diagnostic()
    return (survey,)


@app.cell
def _(mo):
    mo.md(r"""
    # From NHTS travel records to RAMP mobility inputs

    How do survey trips become the distance, duration and timing inputs used by RAMP?
    Follow **trip screening → daily travel → trip purpose → mobility windows → hourly timing**.
    The evidence is the registered **2022 National Household Travel Survey (NHTS) V2.1** release from
    [FHWA / ORNL](https://nhts.ornl.gov/downloads). The main results use the reviewed
    unweighted method; survey-weighted alternatives are kept in the context panels.

    Current survey calculations are compared with the inputs preserved for the registered
    RAMP runs. Differences help assess those assumptions; they do not change the simulations.
    """)
    return


@app.cell
def _(DERIVATION, mo, survey):
    captured = survey.tables["summary"].set_index("method").loc[DERIVATION]
    screens = survey.tables["exclusions"]
    screens = screens.loc[
        screens.method.eq(DERIVATION),
        ["screen", "entering_rows", "excluded_rows", "remaining_rows"],
    ].rename(
        columns={
            "screen": "Sequential screen",
            "entering_rows": "Entering trips",
            "excluded_rows": "Excluded trips",
            "remaining_rows": "Retained trips",
        }
    )
    mo.vstack(
        [
            mo.md(
                f"## 1. Which trips enter the RAMP calculation?\n\n"
                f"**{int(captured.native_trips):,} native trip records → {int(captured.retained_trips):,} retained trips "
                f"({captured.retained_share:.1%})**, covering **{int(captured.travelling_person_days):,} travelling people** on their recorded survey days. "
                "Each exclusion is counted after the preceding screens. Duplicate records are identified using the reviewed 29 fields."
            ),
            mo.md(
                "The screens retain household cars, vans, SUVs and pickups; respondent driver status `DRIVER=1`; "
                "positive trip distance and reported driver vehicle-miles (`VMT_MILE`) below 1,280 miles; durations below 960 minutes; known endpoint purposes "
                "(codes 1–96); reported start time; and calculated speed from 0.5 to below 120 mph. "
                "`DRIVER` describes the respondent; it does not identify who drove each trip. Requiring positive `VMT_MILE` also follows the reviewed method."
            ),
            mo.ui.table(
                screens,
                selection=None,
                label="Filtering the maintained NHTS V2.1 trip file",
            ),
        ]
    )
    return


@app.cell
def _(DERIVATION, mo, survey):
    daily = survey.tables["daily"]
    daily = daily.loc[
        daily.method.eq(DERIVATION), ["day_type", "person_days", "mean_km"]
    ]
    run_daily = survey.tables["parameters"].loc[
        survey.tables["parameters"].parameter.eq("d_tot.csv"),
        ["day_type", "run_input_value"],
    ]
    daily = daily.merge(run_daily, on="day_type", how="left").rename(
        columns={
            "day_type": "Day type",
            "person_days": "Travelling person-days",
            "mean_km": "Current mean (km/day)",
            "run_input_value": "Registered RAMP (km/day)",
        }
    )
    mo.vstack(
        [
            mo.md(
                "## 2. How far does a travelling person drive in a day?\n\n"
                "For `d_tot.csv`, sum retained trip distance for each household–person, then average those totals within each day type. "
                "One person contributes one recorded travel day. Weekday combines Monday–Friday; weekend combines Saturday and Sunday. "
                "These are **positive-travel person-day means**: people without retained travel are outside the denominator. "
                "RAMP's registered table uses weekday/weekend only; blank comparison cells have no separate Saturday/Sunday input."
            ),
            mo.ui.table(
                daily.round(2),
                selection=None,
                label="Daily total distance: current survey derivation beside the frozen RAMP input",
            ),
        ]
    )
    return


@app.cell
def _(DERIVATION, mo, survey):
    purpose = survey.tables["purpose"]
    purpose = purpose.loc[
        purpose.method.eq(DERIVATION),
        ["day_type", "purpose", "trips", "mean_km", "mean_minutes"],
    ]
    params = survey.tables["parameters"]
    distance_reference = params.loc[
        params.parameter.eq("d_min.csv"), ["day_type", "purpose", "run_input_value"]
    ].rename(columns={"run_input_value": "run_km"})
    duration_reference = params.loc[
        params.parameter.eq("t_func.csv"), ["day_type", "purpose", "run_input_value"]
    ].rename(columns={"run_input_value": "run_minutes"})
    purpose = purpose.merge(distance_reference, on=["day_type", "purpose"]).merge(
        duration_reference, on=["day_type", "purpose"]
    )
    purpose = purpose[
        [
            "day_type",
            "purpose",
            "trips",
            "mean_km",
            "run_km",
            "mean_minutes",
            "run_minutes",
        ]
    ].rename(
        columns={
            "day_type": "Day type",
            "purpose": "Purpose",
            "trips": "Trips",
            "mean_km": "Current km/trip",
            "run_km": "RAMP km/trip",
            "mean_minutes": "Current min/trip",
            "run_minutes": "RAMP min/trip",
        }
    )
    mo.vstack(
        [
            mo.md(
                "## 3. What distance and duration does each trip purpose imply?\n\n"
                "`d_min.csv` receives mean trip distance; `t_func.csv` receives mean trip duration, grouped by purpose and day type. "
                "**Occupation** uses summary-purpose codes 10 and 20: work and school/daycare/religious trips. "
                "**Personal** includes the other reviewed purposes, including trips home. Home returns can originate from work, "
                "so these destination-purpose groups differ from the endpoint-activity proxies below."
            ),
            mo.ui.table(
                purpose.round(2),
                selection=None,
                label="One row per purpose and day type; counts show each mean's denominator",
            ),
        ]
    )
    return


@app.cell
def _(DERIVATION, activity_chart, mo, survey):
    activity_minutes = survey.settings["presentation"]["activity_bins_minutes"]
    activity_bins = 1440 // activity_minutes
    windows = survey.tables["windows"][["activity", "window_number", "label"]].rename(
        columns={
            "activity": "Trip-activity proxy",
            "window_number": "Main window",
            "label": "Nominal local time",
        }
    )
    mo.vstack(
        [
            mo.md(
                "## 4. Which travel patterns support the mobility windows?\n\n"
                f"The three departure histograms use **{activity_bins} bins of {activity_minutes} minutes** and pool all day types. Worker trips have an origin or destination "
                "work code 2–5 (including paid work at home); student trips have school code 6; inactive trips have neither. "
                "Work-to-school trips belong to both worker and student proxies. These labels describe **trip activities**, not occupation shares of people.\n\n"
                "Green bands show the **main mobility windows preserved in the RAMP inputs**. Working/student bands are their weekday "
                "schedule; those model classes use the inactive schedule on weekends. Model variation can move nominal boundaries. "
                "The bands are reviewed assumptions, not fitted thresholds or strict bounds on travel. "
                "No 3% threshold is used to derive them. Mobility windows describe travel schedules; charging windows describe when parked vehicles prefer to charge."
            ),
            mo.ui.altair_chart(
                activity_chart(survey, method=DERIVATION),
                chart_selection=False,
                legend_selection=False,
            ),
            mo.ui.table(
                windows,
                selection=None,
                label="Main mobility windows read from the registered RAMP input database",
            ),
        ]
    )
    return


@app.cell
def _(mo, survey):
    days = mo.ui.multiselect(
        survey.settings["presentation"]["day_types"],
        value=survey.settings["presentation"]["default_day_types"],
        label="Day types",
    )
    clock_options = {
        "Departure": "start",
        "Trip midpoint (RAMP occurrence input)": "midpoint",
        "Derived arrival": "arrival",
    }
    clock_default = next(
        label
        for label, event in clock_options.items()
        if event == survey.settings["presentation"]["default_clock"]
    )
    clock = mo.ui.dropdown(clock_options, value=clock_default, label="Trip event")
    clock_view = mo.ui.dropdown(
        ["Curves", "Heatmap"], value="Curves", label="Timing view"
    )
    mo.hstack([days, clock, clock_view], justify="start", gap=2)
    return clock, clock_view, days


@app.cell
def trip_timing(DERIVATION, clock, clock_chart, clock_heatmap, clock_view, days, mo, survey):
    mo.stop(not days.value, mo.callout("Select at least one day type to compare trip timing.", kind="info"))
    timing = (
        clock_chart(survey, method=DERIVATION, days=days.value, clock=clock.value)
        if clock_view.value == "Curves"
        else clock_heatmap(
            survey, method=DERIVATION, days=days.value, clock=clock.value
        )
    )
    mo.vstack(
        [
            mo.md(
                "## 5. When do the retained trips occur?\n\n"
                "The reviewed `trips_by_time_*.csv` derivation uses the **trip midpoint hour**, normalized separately within weekday, "
                "Saturday and Sunday. Departure is the reported local clock time. Midpoint adds half the trip duration; arrival adds the full duration, "
                "with clock times wrapping after 24 hours. Weekday/Saturday/Sunday groups retain the reported travel day. The activity-window plots above use departure."
            ),
            mo.ui.altair_chart(timing, chart_selection=False, legend_selection=False),
        ]
    )
    return


@app.cell
def _(DERIVATION, WEIGHTED, distance_chart, mo, survey, utility_chart):
    context = survey.tables["summary"][
        [
            "method",
            "retained_trips",
            "trip_mean_km",
            "trip_mean_minutes",
            "travelling_person_days",
            "person_day_mean_km",
        ]
    ].rename(
        columns={
            "method": "Interpretation",
            "retained_trips": "Trips",
            "trip_mean_km": "Mean trip (km)",
            "trip_mean_minutes": "Mean trip (min)",
            "travelling_person_days": "Travelling person-days",
            "person_day_mean_km": "Mean person-day (km)",
        }
    )
    mo.vstack([
        mo.md("## What does this imply for the model?\n\n"
              "Daily distance, trip-purpose averages and hourly occurrence describe different parts of travel behavior and use different denominators. "
              "Keep those definitions explicit when documenting or revising RAMP inputs. A difference from a registered input calls for a reviewed change and a new simulation; "
              "it does not justify relabelling an existing run. US survey weights alone cannot establish Ontario travel patterns."),
        mo.accordion({
            "Additional context: survey weighting": mo.vstack(
                [
                    mo.md(
                        "The main RAMP derivation remains unweighted. This optional comparison uses the trip-specific driver flag and national survey "
                        "weights: seven-day weights for annual trip/activity summaries, five-day weekday and two-day weekend weights for timing/purpose. "
                        "The `WTPERFIN`-weighted distribution describes retained travelling people as an exploratory comparison; it is not FHWA's official miles estimator. "
                        "No uncertainty from replicate survey weights is estimated."
                    ),
                    mo.ui.table(
                        context.round(2),
                        selection=None,
                        label="Context only: weighting does not replace the RAMP derivation",
                    ),
                ]
            ),
            "Additional context: distance tail and idealized electric-range coverage": mo.vstack(
                [
                    mo.md(
                        "The unweighted range illustration uses one observation per **person–vehicle day**, preserving the reviewed range method. "
                        "It therefore differs from the person-day totals used for `d_tot.csv`. The exact distance potential is "
                        "$E[\\min(D,R)]/E[D]$ for one full charge per retained travelling day; it is not measured PHEV utility. "
                        "The weighted comparison uses person-days, so both the observation unit and weighting differ. Neither curve includes zero-travel days or charging constraints."
                    ),
                    mo.ui.altair_chart(
                        distance_chart(survey, method=DERIVATION),
                        chart_selection=False,
                        legend_selection=False,
                    ),
                    mo.ui.altair_chart(
                        utility_chart(survey),
                        chart_selection=False,
                        legend_selection=False,
                    ),
                ]
            ),
            "Full parameter comparison and source audit": mo.vstack(
                [
                    mo.ui.table(
                        survey.tables["parameters"].round(6),
                        selection=None,
                        label="Current derivation compared with registered inputs; no files are overwritten",
                    ),
                    mo.json(survey.metadata),
                ]
            ),
        }),
    ])
    return


if __name__ == "__main__":
    app.run()
