"""Technology growth evidence, deployment calibration and constraint diagnostics.

Prepare the pinned handoff with ``uv run python -m diagnostics.technology_growth``.
Run with ``uv run marimo run docs/insights/growth_constraint_diagnosis.py``.
All parameters and envelopes here are provisional research outputs.
"""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import math

    import altair as alt
    import marimo as mo
    import numpy as np
    import pandas as pd

    from diagnostics.technology_growth import (
        annual_seed_to_period,
        annual_to_period,
        limit_envelope,
        load_diagnostic_config,
        load_diagnostic_tables,
        s_curve,
    )
    from utils import load_config_bundle, resolve_artifact_path

    def chart_ui(chart):
        return mo.ui.altair_chart(
            chart.configure_view(stroke=None),
            chart_selection=False,
            legend_selection=False,
        )

    def inspect_table(frame, limit=250):
        preview = frame.head(limit)
        return mo.vstack(
            [
                mo.md(
                    f"Showing {len(preview):,} of {len(frame):,} rows. Larger tables are bounded previews; use the series selectors or the complete persisted CSVs for further inspection."
                ),
                mo.ui.table(preview, selection=None, page_size=12),
            ]
        )

    return (
        alt,
        annual_seed_to_period,
        annual_to_period,
        chart_ui,
        inspect_table,
        json,
        limit_envelope,
        load_config_bundle,
        load_diagnostic_config,
        load_diagnostic_tables,
        math,
        mo,
        np,
        pd,
        resolve_artifact_path,
        s_curve,
    )


@app.cell
def _(load_config_bundle, load_diagnostic_config, load_diagnostic_tables):
    growth_bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    growth_config = load_diagnostic_config(growth_bundle)
    growth_tables = load_diagnostic_tables(growth_bundle)
    return growth_bundle, growth_config, growth_tables


@app.cell
def _(growth_bundle, growth_tables, json, mo, resolve_artifact_path):
    growth_summary = json.loads(
        resolve_artifact_path(
            growth_bundle, "technology_growth_validation", "integrity.json"
        ).read_text(encoding="utf-8")
    )
    mo.vstack(
        [
            mo.md(
                r"""
                # Technology growth constraints: evidence and calibration

                **Research diagnostic · CANOE transportation v2.0 · snapshot 7 October 2026**

                The purpose is to represent adoption inertia and bound rapid substitution.
                Constant rates are an MVP approximation: they preserve neither eventual
                saturation nor the timing of an S-curve. Every candidate below requires
                review. Existing production growth inputs, scenario wiring, technology
                groups and SQLite insertion are deferred.

                The trace is **pinned raw files → native observations and definitions →
                curation audit → observed/fitted measures → Canadian deployment references
                → provisional rate and seed interpretation**. Widgets consume persisted
                evidence; they do not acquire data or refit on each interaction.

                Complete normalized tables are under
                `inputs/1_interim/technology_growth/`; complete audit, fit, calibration
                and seed tables are under `inputs/validation/technology_growth/`.
                Table captions identify previews so display limits are distinct from
                analytical exclusions.

                Reproduce from the repository root:

                ```powershell
                # Explicit acquisition of missing registered snapshots only:
                uv run python -m fetching.technology_growth_evidence --download-missing --papers-dir '<supplied PDF directory>'
                # Default acquisition verification and all analysis are offline:
                uv run python -m diagnostics.technology_growth
                uv run marimo run docs/insights/growth_constraint_diagnosis.py
                ```

                A changed upstream file is rejected by checksum. A new observation release
                needs a reviewed registry update; cached originals are never repaired in
                place. Rebuild if the diagnostic reports a stale handoff. SciPy belongs to
                the development dependency group; this analysis is not an ETL prerequisite.
                """
            ),
            mo.hstack(
                [
                    mo.stat(
                        str(growth_summary["source_assets"]), label="Pinned assets"
                    ),
                    mo.stat(
                        str(growth_summary["observation_rows"]),
                        label="Observation rows, including gaps",
                    ),
                    mo.stat(
                        str(growth_summary["target_combinations"]),
                        label="Legacy road targets",
                    ),
                    mo.stat(
                        str(growth_summary["supported_proxy_candidates"]),
                        label="Region/mode proxy candidates for review",
                    ),
                ],
                justify="space-around",
            ),
            mo.callout(
                "No accepted growth parameters or database rows are produced. These plots are binding upper-bound envelopes, not deployment forecasts or a solver run.",
                kind="warn",
            ),
        ]
    )
    return (growth_summary,)


@app.cell
def _(growth_tables, inspect_table, mo):
    mo.vstack(
        [
            mo.md(
                """
                ## Source identity and literature definitions

                The source manifest records registered identity, native filename, hash,
                byte size and access route. Supplied PDFs are evidence, not instructions.
                HATCH national and aggregate releases have different grains; the 2023
                aggregate estimates cannot be silently attached to every national series.
                """
            ),
            inspect_table(growth_tables["source_manifest"], 100),
            mo.md(
                r"""
                | Evidence | Meaning of growth and consequence for this diagnostic |
                |---|---|
                | [Odenweller et al. (2022)](https://doi.org/10.1038/s41560-022-01097-4), supplied PDF pp. 9–10 | Emergence is the early, approximately exponential phase, when capacity is small relative to the ceiling. Annual fractional growth is $b=e^k-1$. Conventional wind/solar evidence uses moving seven-year exponential fits; complete emergency analogues use logistic fits. Their 15% floor and policy-conditioned demand pull are research assumptions, not vehicle lower bounds. Gompertz $k$ is not an emergence fraction: they match its initial seven-year growth. |
                | [Nemet et al. (2023)](https://doi.org/10.1038/s43247-023-01056-1), main pp. 7–8; supplied SI pp. 16–21 | Logistic $k$ characterizes early continuous relative growth. Formative growth uses projected data below 2.5% of fitted saturation; inflection growth uses a linear fit over ±5 years normalized by inflection level. Whole-series and fastest ten-year exponential growth are distinct observed-window questions. $T_{10–90}=\ln(81)/k$ is a duration. Their Gompertz maximum slope $Lk/e$ has level/year units before normalization. |
                | [Edwards et al. (2024)](https://doi.org/10.1073/pnas.2215679121), main methods; supplied SI pp. 9–15 and section F | SHARD analogy selection compares manufacturing, infrastructure, institutions and adoption context. The 20-observation/global/capacity screen was specific to DACCS. Logistic projections retain a ceiling and use a continuous coefficient in a discretized growth equation. SI tail and synthetic-early-data tests expose coefficient sensitivity. These choices do not justify copying a single DACCS analogue rate to vehicles. |
                | [Greene & Nemet extended research (2026)](https://doi.org/10.1038/s41467-026-73563-6), methods and SI pp. 4–5, 30 | The release adds country and technology characteristics and national logistic estimates. It standardizes some observations by cumulative calculation. Its 10–90 average continuous growth $\ln(9)/T$ differs from early logistic $k$ and discrete $b$. Associations with technology granularity/lifetime/adopter context help screen analogues; they do not estimate a Canadian vehicle parameter. |

                Here $k$ has units **year⁻¹**, because its exponential argument contains
                years. A percentage reported from $k$ is not automatically a discrete annual
                fraction. At finite logistic deployment,
                $\dot C/C=k(1-C/L)$; at inflection this is $k/2$.
                Gompertz has $\dot C/C=k\ln(L/C)$ and no finite, constant low-stock limit.
                We therefore never map a Gompertz coefficient directly into a growth rate.

                Notation differs: Nemet's exponential $a e^{bt}$ uses $b$ as a continuous
                log-slope; Odenweller's $b=e^k-1$ is a discrete annual fraction. A
                continuous exponential coefficient also needs `expm1` before it is used
                as a discrete annual fractional increase. Native reported columns retain
                their source meaning.

                For a positive observed series, endpoint CAGR is
                $b=(y_1/y_0)^{1/(t_1-t_0)}-1$; the multiplicative growth factor is $1+b$.
                This subtraction is explicit even where a paper calls a reported factor a
                rate. Log-OLS and native-space exponential fits use different error models.
                Formative/inflection measures derived from synthetic fitted observations
                remain model-derived, rather than newly observed early deployment.
                """
            ),
        ]
    )
    return


@app.cell
def _(growth_tables, inspect_table, mo, pd):
    release_overview = (
        growth_tables["release_inventory"]
        .groupby("release")
        .agg(
            native_series=("ID", "size"),
            technologies=("Technology Name", "nunique"),
            observations=("nonmissing_observations", "sum"),
            zeros=("zero_observations", "sum"),
        )
        .reset_index()
    )
    release_overview = release_overview.merge(
        growth_tables["series_audit"]
        .groupby("release")
        .agg(
            countries=("country", "nunique"),
            first_observation_year=("first_year", "min"),
            last_observation_year=("last_year", "max"),
            interior_missing_cells=("missing_points", "sum"),
        )
        .reset_index(),
        on="release",
        how="left",
    )
    release_change_counts = (
        growth_tables["release_comparison"]
        .comparison.value_counts()
        .rename_axis("comparison")
        .reset_index(name="series")
    )
    release_unfitted = growth_tables["release_inventory"].loc[
        growth_tables["release_inventory"].release.eq("extended")
        & ~growth_tables["release_inventory"].ID.isin(
            growth_tables["extended_published_estimates"].ID
        )
    ]
    mo.vstack(
        [
            mo.md(
                """
                ## Original versus extended HATCH

                [Original release 10231865](https://zenodo.org/records/10231865) and
                [extended release 19579793](https://zenodo.org/records/19579793) are both
                retained. Pairing below requires a unique country/technology/native-source/
                unit identity, allowing changed IDs and metric names. Ambiguous identities
                are not forced into pairs; use the full inventory to inspect them.

                The extended release is current evidence for its definitions, characteristics
                and estimates. It is not a universal replacement for the original quantities.
                Its SI reports conversion of 329 number-of-units series to cumulative
                calculations, removal of 95 global series, and battery-capacity substitutions.
                Summing a stock series over years measures stock-years; summing additions
                measures cumulative installations before retirement. Either differs from
                active capacity. The native definition must settle the distinction.
                Added nonmissing cells after standardization are not necessarily new
                measurements. Native zero/missing flags describe cells, rather than
                certifying that zero deployment was observed.

                The main paper, SI and files report different series/filter counts. The
                inventory has four CO₂-pipeline series without published characteristics;
                the notebook uses audited file counts rather than silently reconciling the
                reported 1,555/main and 1,750/SI retained samples.
                """
            ),
            mo.hstack(
                [inspect_table(release_overview), inspect_table(release_change_counts)],
                widths="equal",
            ),
            inspect_table(
                release_unfitted[["ID", "Metric", "Unit", "nonmissing_observations"]]
            ),
            mo.accordion(
                {
                    "Full native inventory, including empty series": inspect_table(
                        growth_tables["release_inventory"], 14000
                    ),
                    "Paired observations and metadata changes": inspect_table(
                        growth_tables["release_comparison"], 14000
                    ),
                    "Original variable descriptions, citations and notes": inspect_table(
                        growth_tables["original_native_metadata"], 400
                    ),
                    "Extended variable and characteristic definitions": inspect_table(
                        growth_tables["extended_native_metadata"], 400
                    ),
                }
            ),
        ]
    )
    return release_change_counts, release_overview


@app.cell
def _(alt, chart_ui, growth_tables, inspect_table, mo):
    release_vehicle_comparison = (
        growth_tables["observations"]
        .loc[
            growth_tables["observations"].technology.eq("Passenger Cars")
            & growth_tables["observations"].country.isin(["CAN", "NOR"])
        ]
        .copy()
    )
    release_vehicle_plot = (
        alt.Chart(release_vehicle_comparison)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "year:Q",
                title="Observation year",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="d", tickCount=7),
            ),
            y=alt.Y(
                "value:Q",
                title="Native quantity (not interchangeable stock)",
                scale=alt.Scale(zero=False),
            ),
            color="release:N",
            column="country:N",
            tooltip=[
                "series_id:N",
                "year:Q",
                "value:Q",
                "metric:N",
                "unit:N",
                "native_source:N",
            ],
        )
        .properties(width=450, height=230)
    )
    mo.vstack(
        [
            chart_ui(release_vehicle_plot),
            mo.md(
                """
                Passenger-car levels demonstrate why a release change matters: the
                extended CHAT series is the cumulative sum of original annual count
                observations, not an update to the same fleet level. Original counts remain
                available as a **stock candidate requiring the CHAT definition**. The
                original variable description alone does not establish retirement treatment.
                No rate from the transformed series is used as a direct vehicle-stock rate.
                Other original car series measure engine-power equivalent MW. Changing
                average power per vehicle would affect their growth; constant-unit scaling
                invariance alone does not make them vehicle-count proxies.
                """
            ),
            inspect_table(
                growth_tables["release_comparison"].loc[
                    growth_tables["release_comparison"].technology.eq("Passenger Cars")
                ],
                200,
            ),
        ]
    )
    return


@app.cell
def _(growth_config, growth_tables, inspect_table, mo):
    curation_summary = (
        growth_tables["series_audit"]
        .groupby("release")
        .agg(
            audited_series=("series_id", "size"),
            legacy_retained=("legacy_keep", "sum"),
            research_fit_eligible=("research_fit_eligible", "sum"),
            zero_observations=("zero_points", "sum"),
            legacy_clipped_observations=("legacy_clipped_points", "sum"),
        )
        .reset_index()
    )
    mo.vstack(
        [
            mo.md(
                """
                ## Curation: inspect exclusions before interpreting rates

                The legacy **Obtain growth rates** workflow restricted geography, required
                20 positive observations, erased zeros, rejected long gaps, merged stock
                and cumulative labels, clipped small observations in native units, and
                selected each technology's fastest country. Those choices can privilege
                mature, fast and well-recorded series. A missing comma also combined
                `Copper|Refining` and `Crude Oil` in its exclusion list. The historical audit
                records that literal behavior, without adopting it.

                This diagnostic retains all observations, zeros and interior missing years.
                Shares and annual output are reconsidered for quantity-specific uses.
                Performance metrics and average size of a new unit are unsuitable as direct
                deployment levels. Negative data, empty series, duplicate keys, suppressed
                observations, rounded zero shares and long gaps are visible. No interpolation,
                positive floor or clipping creates early observations.

                The configurable research fit screen requires ten observations. Windows
                span seven or ten **year intervals** (normally eight or eleven annual
                observations), require endpoints, and allow at most one missing interior
                year. A window containing zero has undefined log growth and remains an
                excluded row. This is a transparent comparison of methods, not a literal
                replication of Odenweller's wind/solar sample. Fit $R^2$ is a screen, not
                evidence of an identifiable ceiling or a transferable rate.
                """
            ),
            inspect_table(curation_summary),
            mo.accordion(
                {
                    "All series and consequential exclusions": inspect_table(
                        growth_tables["series_audit"], 14000
                    ),
                    "Window estimates and exclusions": inspect_table(
                        growth_tables["window_rates"], 100000
                    ),
                    "Diagnostic controls, selections and analogue rationale": mo.json(
                        growth_config.model_dump()
                    ),
                }
            ),
        ]
    )
    return


@app.cell
def _(growth_config, growth_tables, mo):
    fit_series_options = sorted(growth_tables["curve_fits"].series_id.unique())
    fit_series = mo.ui.dropdown(
        fit_series_options,
        value="eafo:M1:fleet:BEV"
        if "eafo:M1:fleet:BEV" in fit_series_options
        else fit_series_options[0],
        label="Series for native observations and S-curve diagnostics",
    )
    fit_ceiling = mo.ui.dropdown(
        growth_config.fitting.ceiling_multipliers,
        value=growth_config.fitting.ceiling_multipliers[2],
        label="Optimizer ceiling / observed maximum",
    )
    mo.hstack([fit_series, fit_ceiling])
    return fit_ceiling, fit_series


@app.cell
def _(
    alt,
    chart_ui,
    fit_ceiling,
    fit_series,
    growth_tables,
    inspect_table,
    mo,
    np,
    pd,
    s_curve,
):
    inspected_observations = (
        growth_tables["observations"]
        .loc[growth_tables["observations"].series_id.eq(fit_series.value)]
        .sort_values("year")
    )
    inspected_fits = growth_tables["curve_fits"].loc[
        growth_tables["curve_fits"].series_id.eq(fit_series.value)
    ]
    inspected_windows = growth_tables["window_rates"].loc[
        growth_tables["window_rates"].series_id.eq(fit_series.value)
    ]
    curve_lines = []
    for fitted_row in inspected_fits.loc[
        inspected_fits.ceiling_multiplier.eq(fit_ceiling.value)
        & inspected_fits.sensitivity.eq("full")
        & inspected_fits.status.eq("fitted")
    ].itertuples():
        for fitted_year, fitted_value in zip(
            np.linspace(
                fitted_row.first_observed_year,
                fitted_row.last_observed_year,
                180,
            ),
            s_curve(
                np.linspace(
                    fitted_row.first_observed_year,
                    fitted_row.last_observed_year,
                    180,
                ),
                fitted_row.ceiling_native,
                fitted_row.k_per_year,
                fitted_row.inflection_year,
                fitted_row.family,
            ),
        ):
            curve_lines.append(
                {
                    "year": fitted_year,
                    "value": fitted_value,
                    "family": fitted_row.family,
                }
            )
    observed_chart = (
        alt.Chart(inspected_observations)
        .mark_point(size=35)
        .encode(
            x=alt.X(
                "year:Q",
                title="Year",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="d", tickCount=7),
            ),
            y=alt.Y(
                "value:Q",
                title=f"Native level · {inspected_observations.unit.iloc[0]}",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                "year:Q",
                "value:Q",
                "observation_status:N",
                "raw_row:Q",
                "metric:N",
                "complete_year:N",
            ],
        )
    )
    if curve_lines:
        observed_chart = observed_chart + alt.Chart(
            pd.DataFrame(curve_lines)
        ).mark_line().encode(x="year:Q", y="value:Q", color="family:N")
    window_chart = (
        alt.Chart(inspected_windows.loc[inspected_windows.status.eq("usable")])
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "end_year:Q",
                title="Window ending year",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="d", tickCount=7),
            ),
            y=alt.Y(
                "annual_rate:Q",
                title="Observed log-OLS annual fraction",
                axis=alt.Axis(format=".0%"),
            ),
            color="span_years:N",
            tooltip=[
                "start_year:Q",
                "end_year:Q",
                "annual_rate:Q",
                "endpoint_cagr:Q",
                "native_exp_annual_rate:Q",
                "r2_log:Q",
                "r2_native:Q",
            ],
        )
    )
    mo.vstack(
        [
            mo.md("## Fit behavior and structural uncertainty"),
            mo.hstack(
                [
                    chart_ui(observed_chart.properties(width=600, height=260)),
                    chart_ui(window_chart.properties(width=600, height=260)),
                ],
                widths="equal",
            ),
            inspect_table(inspected_fits),
            inspect_table(inspected_windows),
            mo.md(
                r"""
                Native-space nonlinear least squares fit logistic and Gompertz curves with
                multiple starts. Scaling by observed maximum is numerical conditioning,
                not unit-dependent clipping or a known saturation level. The 1.1 profile
                recreates legacy ceiling bounds (0.9–1.1 × observed maximum, inflection
                inside the sample); broader profiles allow unobserved inflection years.
                They are **optimizer sensitivity settings**, not assumed future ceilings.
                Curves end at their fitted observation endpoints. The raw EAFO 2026 point
                remains visible but is excluded from annual estimates and fits.

                Bound hits, Jacobian conditioning, unobserved inflections, family choice,
                ceiling sensitivity and removal of the last five years expose uncertainty
                beyond $R^2$. These ranges are structural sensitivity, not confidence
                intervals. A good early-phase fit can leave ceiling and midpoint nearly
                unidentified. Observed-window estimates avoid a future ceiling but depend
                on the selected phase, policy, denominator and time window.
                """
            ),
        ]
    )
    return


@app.cell
def _(alt, chart_ui, growth_tables, inspect_table, mo, np, pd):
    published_ext = growth_tables["extended_published_estimates"]
    published_good = published_ext.loc[published_ext.paper_filter_pass].copy()
    published_technology_column = (
        "technology" if "technology" in published_good else "Technology Name"
    )
    published_balanced = (
        published_good.groupby(published_technology_column)["Logistic Fit"]
        .agg(["count", "median", "max"])
        .reset_index()
    )
    published_distributions = pd.DataFrame(
        [
            {
                "sample": sample_name,
                "q25_k": float(sample_values.quantile(0.25)),
                "median_k": float(sample_values.median()),
                "q75_k": float(sample_values.quantile(0.75)),
                "n": len(sample_values),
            }
            for sample_name, sample_values in [
                (
                    "all accepted country-series (coverage weighted)",
                    published_good["Logistic Fit"],
                ),
                ("one country median per technology", published_balanced["median"]),
                ("fastest accepted country per technology", published_balanced["max"]),
            ]
        ]
    )
    published_distributions["annual_fraction_from_median_k"] = np.expm1(
        published_distributions.median_k
    )
    mo.vstack(
        [
            mo.md(
                rf"""
                **Published estimates and sample-selection sensitivity.** Applying the
                configured SI-style screen $R^2\ge0.95$, $0<k<1$ to the actual CSV retains
                **{len(published_good):,} of {len(published_ext):,}** rows. Failure strings and
                rejected coefficients remain in the table. The three samples below are
                different aggregations, not alternative target calibrations. Fastest-country
                selection is deliberately shown as a source of upward selection.
                """
            ),
            inspect_table(published_distributions),
            chart_ui(
                alt.Chart(published_good)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "Logistic Fit:Q",
                        bin=alt.Bin(maxbins=35),
                        title="Accepted published logistic k (year⁻¹)",
                    ),
                    y=alt.Y("count():Q", title="Country-series count"),
                )
                .properties(width=950, height=220)
            ),
            mo.accordion(
                {
                    "Original aggregate published growth measures": inspect_table(
                        growth_tables["original_published_estimates"], 200
                    ),
                    "Extended country-series estimates and characteristics": inspect_table(
                        published_ext, 6500
                    ),
                    "Country median and maximum by historical technology": inspect_table(
                        published_balanced, 250
                    ),
                }
            ),
            mo.md(
                """
                No broad HATCH quantile or fastest-country coefficient is assigned to a
                target. The original published logistic column is a reported coefficient
                percentage: division by 100 recovers $k$, and `expm1(k)` gives an early
                discrete annual fraction. The original Delta T field reports the **inverse**
                10–90 duration as a percentage (4% means 1/T=0.04 year⁻¹, or T=25 years).
                Extended Delta T reports years; 2.197/T is a different growth measure. Extended fit
                and characteristic coverage changes prevent a paired coefficient update
                from being interpreted as a pure change in underlying observations.
                """
            ),
        ]
    )
    return published_balanced, published_distributions


@app.cell
def _(growth_config, growth_tables, inspect_table, mo):
    paired_curve_estimates = (
        growth_tables["release_fit_comparison"]
        .loc[
            growth_tables["release_fit_comparison"].sensitivity.eq("full")
            & growth_tables["release_fit_comparison"].ceiling_multiplier.eq(
                growth_config.fitting.ceiling_multipliers[2]
            )
            & growth_tables["release_fit_comparison"].status_original.eq("fitted")
            & growth_tables["release_fit_comparison"].status_extended.eq("fitted")
        ]
        .copy()
    )
    paired_curve_estimates["absolute_k_difference"] = (
        paired_curve_estimates.k_difference_per_year.abs()
    )
    paired_curve_summary = (
        paired_curve_estimates.groupby(["comparison", "family"])
        .agg(
            paired_series=("series_id_original", "size"),
            median_absolute_k_difference=("absolute_k_difference", "median"),
            max_absolute_k_difference=("absolute_k_difference", "max"),
        )
        .reset_index()
    )
    mo.vstack(
        [
            mo.md(
                "**Like-for-like estimate changes.** These national-series comparisons use the same curve family, optimizer profile and observation preparation in both releases. Equal-overlap fits should agree unless coverage changed. Cumulative transformations can change coefficients as well as levels. They do not turn an aggregate published rate into a national estimate; all paired sensitivities remain inspectable."
            ),
            inspect_table(paired_curve_summary),
            mo.accordion(
                {
                    "Paired curve estimates and all sensitivities": inspect_table(
                        growth_tables["release_fit_comparison"], 2000
                    )
                }
            ),
        ]
    )
    return


@app.cell
def _(growth_tables, mo):
    raw_release = mo.ui.dropdown(
        ["original", "extended", "eafo"], value="original", label="Raw evidence release"
    )
    raw_release
    return (raw_release,)


@app.cell
def _(growth_tables, mo, raw_release):
    raw_release_frame = growth_tables["observations"].loc[
        growth_tables["observations"].release.eq(raw_release.value)
    ]
    raw_technology_choices = sorted(raw_release_frame.technology.unique())
    raw_technology = mo.ui.dropdown(
        raw_technology_choices,
        value="Passenger Cars"
        if "Passenger Cars" in raw_technology_choices
        else raw_technology_choices[0],
        label="Native technology (evidence, not a model target)",
    )
    raw_technology
    return raw_release_frame, raw_technology


@app.cell
def _(mo, raw_release_frame, raw_technology):
    raw_technology_frame = raw_release_frame.loc[
        raw_release_frame.technology.eq(raw_technology.value)
    ]
    raw_country_choices = sorted(raw_technology_frame.country.dropna().unique())
    raw_country = mo.ui.dropdown(
        raw_country_choices,
        value="CAN" if "CAN" in raw_country_choices else raw_country_choices[0],
        label="Native observation geography",
    )
    raw_country
    return raw_country, raw_technology_frame


@app.cell
def _(mo, raw_country, raw_technology_frame):
    raw_country_frame = raw_technology_frame.loc[
        raw_technology_frame.country.eq(raw_country.value)
    ]
    raw_series_choices = sorted(raw_country_frame.series_id.unique())
    raw_series = mo.ui.dropdown(
        raw_series_choices,
        value=raw_series_choices[0],
        label="Source-native series identity",
    )
    raw_series
    return raw_country_frame, raw_series


@app.cell
def _(alt, chart_ui, growth_tables, inspect_table, mo, raw_country_frame, raw_series):
    raw_selected_rows = raw_country_frame.loc[
        raw_country_frame.series_id.eq(raw_series.value)
    ]
    raw_selected_audit = growth_tables["series_audit"].loc[
        growth_tables["series_audit"].series_id.eq(raw_series.value)
    ]
    mo.vstack(
        [
            mo.md(
                "**Inspect any observation series, including excluded evidence.** Native units, metric labels, source/variable identity, raw row number, zero and missing status stay visible. A plot does not certify that the quantity is stock or gross additions."
            ),
            inspect_table(raw_selected_audit),
            chart_ui(
                alt.Chart(raw_selected_rows)
                .mark_line(point=True)
                .encode(
                    x=alt.X(
                        "year:Q",
                        scale=alt.Scale(zero=False),
                        axis=alt.Axis(format="d", tickCount=7),
                    ),
                    y=alt.Y(
                        "value:Q",
                        scale=alt.Scale(zero=False),
                        title=f"Native level ({raw_selected_rows.unit.iloc[0]})",
                    ),
                    tooltip=[
                        "year:Q",
                        "value:Q",
                        "observation_status:N",
                        "metric:N",
                        "unit:N",
                        "raw_row:Q",
                    ],
                )
                .properties(width=1050, height=200)
            ),
            inspect_table(
                raw_selected_rows[
                    [
                        "series_id",
                        "year",
                        "value",
                        "observation_status",
                        "metric",
                        "unit",
                        "variable",
                        "native_source",
                        "raw_row",
                    ]
                ],
                400,
            ),
        ]
    )
    return


@app.cell
def _(alt, chart_ui, growth_tables, inspect_table, mo):
    norway_latest = growth_tables["eafo_observations"].loc[
        growth_tables["eafo_observations"].year.ge(2024)
    ]
    norway_series = growth_tables["eafo_observations"].loc[
        growth_tables["eafo_observations"].vehicle_class.eq("M1")
        & growth_tables["eafo_observations"].powertrain.isin(["BEV", "PHEV", "H2"])
        & growth_tables["eafo_observations"].quantity.isin(["fleet", "registrations"])
    ]
    mo.vstack(
        [
            mo.md(
                """
                ## Refreshed Norway vehicle evidence

                [EAFO Norway fleet and registrations](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet)
                was acquired on 7 October 2026. The page advertises fleet coverage Q2 2026
                and registrations July 2026; 2026 entries are partial snapshots/YTD, so
                annual fitting and period totals stop at the latest complete year, **2025**.
                Counts, rounded shares, fuels and classes stay separate. Blank fields remain
                missing. Rounded H₂ shares of 0.00% do not establish zero deployment.

                M1 passenger vehicles, N1 vans and N2/N3 trucks are observation classes,
                not exact CANOE road-class matches. M1 includes vehicles outside a Canadian
                car-only class; N2/N3 cannot distinguish medium from heavy trucks. H₂ is the
                EAFO fuel category and needs a drivetrain definition before a strict FCEV
                transfer. Policy, imports/exports, scrappage and replacement affect stock;
                gross registrations are a proxy for model additions, not a stock change.
                """
            ),
            chart_ui(
                alt.Chart(norway_series)
                .mark_line(point=True)
                .encode(
                    x=alt.X(
                        "year:Q",
                        scale=alt.Scale(zero=False),
                        title="Observation year; 2026 partial",
                        axis=alt.Axis(format="d", tickCount=7),
                    ),
                    y=alt.Y("value:Q", scale=alt.Scale(zero=False), title="Vehicles"),
                    color="powertrain:N",
                    strokeDash="complete_year:N",
                    column="quantity:N",
                    tooltip=[
                        "year:Q",
                        "powertrain:N",
                        "value:Q",
                        "complete_year:N",
                        "quantity:N",
                    ],
                )
                .properties(width=470, height=230)
                .resolve_scale(y="independent")
            ),
            inspect_table(norway_latest, 400),
        ]
    )
    return


@app.cell
def _(growth_config, inspect_table, mo, pd):
    analogue_review = pd.DataFrame(
        [
            {"analogue_family": k, "transfer_rationale_and_limits": v}
            for k, v in growth_config.analogue_rationale.items()
        ]
    )
    mo.vstack(
        [
            mo.md(
                """
                ## Which analogues help which targets?

                Direct powertrain adoption is the strongest quantity match, while Norway's
                policy and class boundaries weaken geographic transfer. Historical passenger
                cars inform durable-product diffusion, but market expansion is different from
                substituting one drivetrain for another. Appliances/bicycles inform repeatable
                manufacture and individual purchasing; wind, pipelines and electrolyser
                research illuminate coordinated infrastructure and policy dependence.

                For FCEVs, hydrogen supply, refuelling infrastructure and fleet procurement
                may matter more than a generic manufactured-product rate. Small H₂ series
                can jump by a few vehicles, plateau or decline: a high early percentage
                does not prove sustained commercial-scale growth. HEVs have a different
                infrastructure burden and more mature installed base; CNG is another fuel
                network context. Neither a rate ranking nor equal weighting follows from
                these comparisons. A reviewed analogy selection should compare unit size,
                manufacture, finance, adopter, policy, network needs and replacement cycle.
                """
            ),
            inspect_table(analogue_review),
        ]
    )
    return


@app.cell
def _(growth_tables, mo):
    reference_region = mo.ui.dropdown(
        sorted(growth_tables["target_references"].region.unique()),
        value="ON",
        label="Canadian model region",
    )
    reference_target = mo.ui.dropdown(
        sorted(growth_tables["target_references"].target_id.unique()),
        value="cars:bev",
        label="Legacy target / current physical family",
    )
    mo.hstack([reference_region, reference_target])
    return reference_region, reference_target


@app.cell
def _(growth_tables, inspect_table, mo, reference_region, reference_target):
    region_references = growth_tables["target_references"].loc[
        growth_tables["target_references"].region.eq(reference_region.value)
    ]
    selected_reference = region_references.loc[
        region_references.target_id.eq(reference_target.value)
    ].iloc[0]
    selected_candidates = growth_tables["candidate_calibrations"].loc[
        growth_tables["candidate_calibrations"].region.eq(reference_region.value)
        & growth_tables["candidate_calibrations"].target_id.eq(reference_target.value)
    ]
    selected_domestic = growth_tables["canadian_registration_evidence"].loc[
        growth_tables["canadian_registration_evidence"].region.eq(
            reference_region.value
        )
        & growth_tables["canadian_registration_evidence"].road_class.eq(
            selected_reference.road_class
        )
    ]
    mo.vstack(
        [
            mo.md(
                """
                ## Canadian starting deployment participates in calibration

                The 24 legacy target families cover cars (including the legacy diesel-car
                exception), passenger/freight light trucks, and medium/heavy trucks.
                Current BEV/PHEV range alternatives are grouped only for this diagnostic;
                the table reconciles legacy selected names, current existing names and
                future siblings. Range shares must not multiply one physical fleet several
                times. FCEV/FCHEV grouping remains a representation question for review.

                Reference stock is **2023**, in **thousand vehicles**, from current processed
                road existing capacity. It combines CEUD class totals, MTO age evidence,
                cohort eligibility/redistribution and fuel composition inferred from source
                registration evidence. It is not a direct census of each powertrain. The
                uncleaned cohort audit conserves class stock; processed stock omits rows
                below the configured 0.001 k-vehicle threshold, explicitly reconciled here.

                Fuel shares vary by source year/cohort. AB and NL use Canada proxies;
                BC/BCT and PE/PEI aliases are explicit. Passenger and freight light trucks
                share an unallocated registration pool. The medium-truck latest-vintage
                BEV dashboard override comes from 2026 YTD sales-share evidence, despite
                the 2023 stock reference. StatCan 23-10-0308 measures **registered stock**,
                not new sales; it must not be reused as a gross-additions history.

                Missing FCEV/CNG representation is not measured zero. The legacy artificial
                initial floor based on an assumed ceiling is not retained as observed deployment.
                Surviving 2015/2020 cohorts are also not original sales in those periods.
                """
            ),
            inspect_table(region_references),
            mo.callout(
                f"Selected reference: {selected_reference.target_id}, {selected_reference.region}; "
                f"{selected_reference.stock_k_vehicles:.6g} k vehicles in {selected_reference.reference_year}; "
                f"{selected_reference.stock_share:.3%} of represented class stock. Status: {selected_reference.deployment_status}.",
                kind="info" if selected_reference.stock_k_vehicles > 0 else "warn",
            ),
            inspect_table(selected_candidates),
            mo.accordion(
                {
                    "Underlying Canadian observations, units, source rows and proxy flags": inspect_table(
                        selected_domestic, 1000
                    ),
                    "All target-to-v2 technology associations (diagnostic only)": inspect_table(
                        growth_tables["target_members"], 350
                    ),
                }
            ),
        ]
    )
    return selected_candidates, selected_domestic, selected_reference


@app.cell
def _(alt, chart_ui, inspect_table, mo, pd, selected_domestic, selected_reference):
    domestic_level_chart = (
        alt.Chart(selected_domestic)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "year:Q",
                title="Canadian source reference year",
                axis=alt.Axis(format="d", tickCount=7),
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "quantity_k_vehicles:Q",
                title="Native count (k vehicles)",
                scale=alt.Scale(zero=False),
            ),
            color="fuel_type:N",
            tooltip=[
                "year:Q",
                "fuel_type:N",
                "quantity:N",
                "quantity_k_vehicles:Q",
                "source_regions:N",
                "proxy_geography:N",
                "coverage_complete:N",
            ],
        )
        .properties(width=1050, height=230)
    )
    domestic_recent_changes = []
    for recent_fuel, recent_data in selected_domestic.loc[
        selected_domestic.coverage_complete
    ].groupby("fuel_type"):
        recent_valid = (
            recent_data.dropna(subset=["quantity_k_vehicles"])
            .sort_values("year")
            .tail(2)
        )
        if (
            len(recent_valid) == 2
            and recent_valid.year.iloc[-1] - recent_valid.year.iloc[0] == 1
        ):
            domestic_recent_changes.append(
                {
                    "fuel": recent_fuel,
                    "quantity": recent_valid.quantity.iloc[-1],
                    "year": int(recent_valid.year.iloc[-1]),
                    "latest_level_k": recent_valid.quantity_k_vehicles.iloc[-1],
                    "latest_annual_fraction": recent_valid.quantity_k_vehicles.iloc[-1]
                    / recent_valid.quantity_k_vehicles.iloc[0]
                    - 1
                    if recent_valid.quantity_k_vehicles.iloc[0] > 0
                    else float("nan"),
                }
            )
    mo.vstack(
        [
            chart_ui(domestic_level_chart),
            inspect_table(pd.DataFrame(domestic_recent_changes))
            if domestic_recent_changes
            else mo.md("Comparable complete recent observations are unavailable."),
            mo.md(
                "Recent changes above are observed one-year fractions. They can differ sharply from a seven/ten-year emergence proxy, especially with replacement, policy changes or declining sales. A positive longer-window candidate does not establish positive current growth. The quantity label remains registered stock for medium/heavy data and gross new registrations for LDV data."
            ),
        ]
    )
    return


@app.cell
def _(
    alt, chart_ui, growth_config, growth_tables, inspect_table, mo, selected_reference
):
    maturity_fuel = growth_config.norway_powertrains.get(selected_reference.powertrain)
    maturity_share = growth_tables["eafo_observations"].loc[
        growth_tables["eafo_observations"].vehicle_class.eq(
            selected_reference.norway_class
        )
        & growth_tables["eafo_observations"].quantity.eq("fleet_share")
        & growth_tables["eafo_observations"].powertrain.eq(maturity_fuel)
        & growth_tables["eafo_observations"].complete_year
    ]
    if len(maturity_share):
        maturity_plot = (
            alt.Chart(maturity_share)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "year:Q",
                    title="Norway observation year",
                    axis=alt.Axis(format="d", tickCount=7),
                    scale=alt.Scale(zero=False),
                ),
                y=alt.Y(
                    "value:Q",
                    title="Norway fleet share (%)",
                    scale=alt.Scale(zero=False),
                ),
                tooltip=["year:Q", "value:Q", "powertrain:N"],
            )
        )
        maturity_plot = maturity_plot + alt.Chart(
            maturity_share.head(1).assign(
                reference=selected_reference.stock_share * 100
            )
        ).mark_rule(color="#c65f32").encode(y="reference:Q")
        maturity_display = chart_ui(maturity_plot.properties(width=1050, height=200))
    else:
        maturity_display = mo.callout(
            "No direct EAFO fleet-share series for this target; a maturity match cannot be asserted.",
            kind="warn",
        )
    mo.vstack(
        [
            mo.md(
                r"""
                **Joint stock/rate calibration, without a future saturation point.**
                The stock candidate selects Norway stock-growth windows whose starting
                fleet share lies within a configurable factor of two of the Canadian 2023
                inferred share. Only positive observations and log-fit $R^2\ge0.8$ pass.
                The median and quartiles of those overlapping windows summarize a
                **conditional analogue range**, not statistical confidence bounds. The
                starting level scales the bound; observed growth in the matched windows
                supplies the rate. Absolute $C_0$ alone never establishes growth speed.

                Fleet share is an observable maturity proxy, not $C/L$ for a known future
                market. Its cross-country denominator, class mix and policies require
                review. A nearest-stage or unanchored estimate remains visible but is
                flagged insufficient. H₂/CNG and commercial classes do not receive an
                automatic BEV/PHEV recommendation. Missing stock prevents a credible match.

                For new capacity, recent Canadian car-registration windows supply separate
                flow evidence; the current inferred stock share, sales share and annual
                sales/stock ratio expose replacement/maturity differences before review.
                Norway flow windows at stock-matched maturity provide an alternative when
                domestic flow evidence is absent. Unallocated light-truck pools and missing
                commercial sales histories remain insufficient. Recent windows may include
                post-2023 observations; these are explicitly dated updated diagnostics.

                Delta growth uses comparable period additions and their preceding change.
                We recommend testing **native $R=0$** first; no annual CAGR is mapped to a
                delta rate. Existing stock helps check the scale and market context but
                supplies neither of its two gross-additions reference levels.
                """
            ),
            maturity_display,
            mo.md(
                "Orange line: current Canadian inferred stock share, a reference rather than a future saturation assumption."
            ),
        ]
    )
    return


@app.cell
def _(growth_tables, inspect_table, mo, reference_region):
    candidate_region = growth_tables["candidate_calibrations"].loc[
        growth_tables["candidate_calibrations"].region.eq(reference_region.value)
    ]
    mo.vstack(
        [
            mo.md(
                "**Technology-specific candidate ledger.** Values remain provisional. `insufficient_target_evidence` does not become an accepted parameter because a numerical estimate is present."
            ),
            inspect_table(
                candidate_region[
                    [
                        "target_id",
                        "mode",
                        "annual_candidate_median",
                        "q25",
                        "q75",
                        "constant_transition_candidate",
                        "candidate_status",
                        "matched_windows",
                        "window_years",
                        "basis",
                    ]
                ],
                100,
            ),
            mo.accordion(
                {
                    "All regions, candidate metadata and reference warnings": inspect_table(
                        growth_tables["candidate_calibrations"], 1000
                    )
                }
            ),
        ]
    )
    return


@app.cell
def _(
    annual_seed_to_period,
    annual_to_period,
    growth_bundle,
    growth_config,
    inspect_table,
    mo,
    pd,
):
    conversion_table = pd.DataFrame(
        [
            {
                "annual_fraction": b,
                "interval_years": dt,
                "native_transition_R": annual_to_period(b, dt),
                "native_seed_k_equivalent_to_one_vehicle_each_year": annual_seed_to_period(
                    0.001, b, dt
                ),
            }
            for b in growth_config.calibration.stress_annual_rates
            for dt in [2, growth_config.calibration.historical_period_width]
        ]
    )
    mo.vstack(
        [
            mo.md(
                r"""
                ## Temoa equations: quantities, units and time interpretation

                For upper bounds and a physical technology family:

                $$C_i\le S+(1+R)C_{i-1}$$
                $$N_i\le S+(1+R)N_{i-1}$$
                $$N_i-N_{i-1}\le S+(1+R)(N_{i-1}-N_{i-2})$$

                | Mode | Appropriate evidence and transformations | Additional references |
                |---|---|---|
                | `growth_capacity` | Active stock or available capacity. A cumulative installation total needs retirement accounting; annual production needs utilization/capacity interpretation. Count/share labels alone do not settle meaning. Convert fleet share with its changing fleet denominator. | Available group stock at the model reference, including surviving vintages and lifetime weighting. |
                | `growth_new_capacity` | Gross installations/new registrations; aggregate annual counts into comparable model-period additions. Production of vehicles can be a sales/supply proxy after trade and allocation review. Installed-stock differences are net change and omit replacements. Market share needs the year's total sales denominator. | One previous gross-additions period, period width, region/class allocation, replacement/trade treatment. |
                | `growth_new_capacity_delta` | Change between comparable period additions, not between stock observations. Test the recurrence jointly for $R,S$. For $R=0$, $S$ bounds the second difference of period additions. Negative prior changes matter. | Two previous additions periods and their difference, consistent widths/units; evidence for acceleration allowance. |

                $R$ is a **fraction per constraint transition** and $S$ has capacity units
                (here k vehicles). $C_i$ is available stock; $N_i$ is the model's new-vintage
                capacity variable. The seed recurs on every transition.
                [Temoa's documentation](https://docs.temoaproject.org/en/latest/extensions/growth_rates.html)
                calls rates annual; the
                [inspected implementation](https://github.com/TemoaProject/temoa/tree/a2b0bf24cc0dd2a99570ae48772316341e79c0b5/temoa/extensions/growth_rates)
                applies `1 + rate` directly, without elapsed-year exponentiation. This
                snapshot is pinned; a later engine revision must be rechecked.

                If a discrete annual fraction $b$ is to retain its interpretation over equal
                $\Delta$ years, supply $R=(1+b)^\Delta-1$; for logistic emergence this is
                $R=e^{k\Delta}-1$. A constant native $R$ can represent a constant annual
                fraction only with equal transition widths. A two-year 2023→2025 stock
                demonstration differs from a five-year 2020→2025 label transition.
                This diagnostic does not resolve that first-period production contract.

                An annual recurrence seed $s$ aggregates to
                $S=s[(1+b)^\Delta-1]/b$ (or $s\Delta$ at $b=0$), not just the same seed.
                This equivalence is for stock/level recurrences, not an automatic rule for
                period-additions deltas. Annual sales rates compared between unequal
                period totals also require period-width normalization.

                In this code snapshot the first stock reference uses adjusted existing
                capacity, life fractions and the last existing period. The first new-capacity
                reference uses only the last existing vintage; delta uses the last two.
                Existing `_EX` and future `_N` identifiers must be associated explicitly
                before those fallbacks can see one physical family. Their surviving cohorts
                are not observed gross sales. Diagnostic membership does not create
                `tech_groups`, and 2023 stock is not claimed to equal native `CAPAVL`.
                """
            ),
            inspect_table(conversion_table),
            mo.md(
                f"Current scenario labels: existing {growth_bundle.scenario.periods.existing}; model {growth_bundle.scenario.periods.model}; base year {growth_bundle.scenario.periods.base_year}."
            ),
        ]
    )
    return


@app.cell
def _(growth_config, growth_tables, inspect_table, mo, selected_reference):
    additions_domestic_id = f"canada:{selected_reference.region}:{selected_reference.road_class}:{selected_reference.powertrain}"
    additions_norway_id = f"eafo:{selected_reference.norway_class}:registrations:{growth_config.norway_powertrains.get(selected_reference.powertrain, 'no_direct_series')}"
    target_period_additions = growth_tables["period_additions"].loc[
        growth_tables["period_additions"].series_id.isin(
            [additions_domestic_id, additions_norway_id]
        )
    ]
    target_seed_frontiers = growth_tables["seed_frontiers"].loc[
        growth_tables["seed_frontiers"].series_id.isin(
            [
                additions_domestic_id,
                additions_norway_id,
                f"eafo:{selected_reference.norway_class}:fleet:{growth_config.norway_powertrains.get(selected_reference.powertrain, 'no_direct_series')}",
            ]
        )
    ]
    mo.vstack(
        [
            mo.md(
                r"""
                ## Gross-additions references and empirical seed residuals

                Annual registrations are summed only for complete, fixed-width calendar
                blocks: 2011–2015 is labelled 2010, 2016–2020 is labelled 2015, and
                2021–2025 is labelled 2020. These are **historical analytical bins**, not
                accepted CANOE vintage mappings. Incomplete blocks remain missing; no
                interpolation or cumulative sum fills them. Counts are divided by 1,000.

                For fixed $R$, the smallest nonnegative constant seed covering observed
                level transitions is $\max(0,\max_i[y_i-(1+R)y_{i-1}])$.
                For delta it is $\max(0,\max_i[D_i-(1+R)D_{i-1}])$.
                These full-history residual bounds absorb regime changes and shocks as well
                as initiation; they are **not** recommendations to use a large recurring seed.
                Stock frontiers use annual comparisons; new/delta frontiers use complete
                five-year additions blocks, so their seed magnitudes are not interchangeable.

                Recommend zero seed where a reviewed positive reference and chosen rate
                already cover relevant transitions. At a measured zero, use a separately
                justified pilot/procurement allowance: one vehicle is 0.001 k vehicles, ten
                is 0.01, and 100 is 0.1. Those batches and fractions of class stock below are
                scale tests, not universal numerical recommendations. FCEV zero-start
                allowances require actual baseline and infrastructure/project evidence first.
                """
            ),
            inspect_table(target_period_additions),
            inspect_table(target_seed_frontiers),
        ]
    )
    return target_period_additions, target_seed_frontiers


@app.cell
def _(growth_config, mo):
    envelope_mode = mo.ui.dropdown(
        ["growth_capacity", "growth_new_capacity", "growth_new_capacity_delta"],
        value="growth_capacity",
        label="Envelope formulation",
    )
    envelope_seed = mo.ui.dropdown(
        growth_config.calibration.seed_batch_vehicles,
        value=growth_config.calibration.seed_batch_vehicles[0],
        label="Recurring seed batch (vehicles / transition)",
    )
    envelope_reference = mo.ui.dropdown(
        ["gross_registration_bins", "native_surviving_cohort_fallback"],
        value="gross_registration_bins",
        label="New/delta starting-reference demonstration",
    )
    mo.hstack([envelope_mode, envelope_seed, envelope_reference], wrap=True)
    return envelope_mode, envelope_reference, envelope_seed


@app.cell
def _(envelope_mode, growth_config, mo, selected_candidates):
    mode_candidate = selected_candidates.loc[
        selected_candidates["mode"].eq(envelope_mode.value)
    ].iloc[0]
    default_annual_rate = (
        float(mode_candidate.annual_candidate_median)
        if mode_candidate.candidate_status == "supported_proxy_for_review"
        and mode_candidate.annual_candidate_median
        == mode_candidate.annual_candidate_median
        else growth_config.calibration.stress_annual_rates[1]
    )
    envelope_annual = mo.ui.number(
        start=-0.9,
        stop=5.0,
        step=0.01,
        value=min(max(default_annual_rate, -0.9), 5.0),
        label="Annual level growth fraction (ignored for delta; native R=0)",
    )
    mo.vstack(
        [
            envelope_annual,
            mo.md(
                f"Default basis: **{mode_candidate.candidate_status}**. Insufficient estimates use the configured 10% annual scale stress; available unaccepted estimates remain in the ledger. A numerical value in an insufficient row remains a sensitivity experiment."
            ),
        ]
    )
    return (envelope_annual,)


@app.cell
def _(
    alt,
    annual_to_period,
    chart_ui,
    envelope_annual,
    envelope_mode,
    envelope_reference,
    envelope_seed,
    growth_config,
    inspect_table,
    limit_envelope,
    mo,
    pd,
    selected_reference,
    target_period_additions,
):
    envelope_warning = ""
    envelope_previous2 = None
    envelope_start_year = int(selected_reference.reference_year)
    envelope_initial = float(selected_reference.stock_k_vehicles)
    envelope_reference_basis = "2023 inferred existing stock; this is not the engine's lifetime-weighted CAPAVL fallback"
    if envelope_mode.value != "growth_capacity":
        if envelope_reference.value == "native_surviving_cohort_fallback":
            envelope_initial = float(selected_reference.latest_surviving_cohort_k)
            envelope_previous2 = float(selected_reference.previous_surviving_cohort_k)
            envelope_start_year = int(selected_reference.latest_existing_label)
            envelope_reference_basis = "Surviving existing-vintage cohorts: engine fallback illustration, not gross additions evidence"
        else:
            envelope_history = target_period_additions.loc[
                target_period_additions.series_id.str.startswith("canada:")
                & target_period_additions.complete
            ].sort_values("end_year")
            if len(envelope_history) >= 2:
                envelope_initial = float(envelope_history.new_capacity_k.iloc[-1])
                envelope_previous2 = float(envelope_history.new_capacity_k.iloc[-2])
                envelope_start_year = int(envelope_history.end_year.iloc[-1])
                envelope_reference_basis = "Complete domestic gross-registration bins; last endpoint 2025, not the 2023 stock reference"
                if "light_trucks" in selected_reference.road_class:
                    envelope_warning = "This registration pool has no passenger/freight allocation. Its scale is unsuitable as a target-specific capacity reference."
            else:
                envelope_initial = 0.0
                envelope_previous2 = 0.0
                envelope_reference_basis = "Zero-start scale stress only: required domestic gross-additions references are absent"
                envelope_warning = "No calibrated new/delta target envelope can be claimed without its historical additions references."
    if selected_reference.deployment_status == "absent_existing_representation":
        envelope_warning += " Missing existing representation is not observed zero; the zero-start plot is a sensitivity experiment."
    envelope_years = list(
        range(
            envelope_start_year,
            growth_config.calibration.horizon_year + 1,
            growth_config.calibration.historical_period_width,
        )
    )
    envelope_rate = (
        0.0
        if envelope_mode.value == "growth_new_capacity_delta"
        else annual_to_period(
            envelope_annual.value, growth_config.calibration.historical_period_width
        )
    )
    envelope_frames = []
    for envelope_batch in sorted(
        {*growth_config.calibration.seed_batch_vehicles, envelope_seed.value}
    ):
        envelope_frame = limit_envelope(
            mode=envelope_mode.value,
            initial=envelope_initial,
            previous2=envelope_previous2,
            transition_rate=envelope_rate,
            seed=envelope_batch / 1000,
            years=envelope_years,
        )
        envelope_frame["seed_batch_vehicles"] = str(envelope_batch)
        envelope_frames.append(envelope_frame)
    envelope_data = pd.concat(envelope_frames, ignore_index=True)
    envelope_chart = (
        alt.Chart(envelope_data)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "year:Q",
                scale=alt.Scale(zero=False),
                title="Equal-width diagnostic transition years",
                axis=alt.Axis(format="d", tickCount=7),
            ),
            y=alt.Y(
                "capacity_k:Q",
                scale=alt.Scale(zero=False),
                title="Available stock (k vehicles)"
                if envelope_mode.value == "growth_capacity"
                else "New capacity per period (k vehicles)",
            ),
            color=alt.Color("seed_batch_vehicles:N", title="Recurring seed, vehicles"),
            tooltip=[
                "year:Q",
                "capacity_k:Q",
                "delta_k:Q",
                "transition_rate:Q",
                "seed_k:Q",
                "feasible_nonnegative:N",
            ],
        )
        .properties(width=1050, height=280)
    )
    mo.vstack(
        [
            mo.md(
                f"## Constant-rate envelopes and seed sensitivity\n\n**{envelope_reference_basis}.** Native transition R = {envelope_rate:.5g}; recurring seed options are in k vehicles after dividing the displayed batches by 1,000."
            ),
            mo.callout(
                envelope_warning
                or "An upper bound permits slower deployment; it does not require the plotted maximum to occur.",
                kind="warn" if envelope_warning else "info",
            ),
            chart_ui(envelope_chart),
            inspect_table(
                envelope_data.loc[
                    envelope_data.seed_batch_vehicles.eq(str(envelope_seed.value))
                ]
            ),
            mo.md(
                r"""
                All lines use constant $R,S$. For level modes, from zero with $R=0$,
                $C_n=nS$; with $R>0$, $C_n=S[(1+R)^n-1]/R$. A recurring seed therefore
                builds capacity even without a positive starting fleet.
                Delta with native $R=0$ and binding bounds gives
                $N_n=N_0+nD_0+S n(n+1)/2$: additions can grow quadratically, and their
                cumulative sum faster still. Native delta $R=0$ is **not** zero level growth.

                A positive prior $D$ can decelerate under an upper bound; a negative prior
                $D$ may force further decline. A negative upper bound conflicts with
                nonnegative new capacity and is flagged, never clipped. Literal empty/null
                rate handling is not assumed; the demonstration supplies explicit zero.
                This supports testing an acceleration allowance separately from level
                growth, rather than importing a sales CAGR into the delta equation.

                Retirement, available-capacity averaging, demand, competing technologies,
                fleet totals and infrastructure can bind before these envelopes. None is
                silently imposed as a future saturation point here.
                """
            ),
        ]
    )
    return envelope_data


@app.cell
def _(
    alt,
    chart_ui,
    envelope_annual,
    growth_config,
    inspect_table,
    mo,
    np,
    pd,
    selected_reference,
    s_curve,
):
    constant_curve_rows = []
    if selected_reference.stock_k_vehicles > 0:
        curve_base = float(selected_reference.stock_k_vehicles)
        curve_k = float(np.log1p(envelope_annual.value))
        curve_years = np.arange(
            int(selected_reference.reference_year),
            growth_config.calibration.horizon_year + 1,
        )
        for curve_year in curve_years:
            constant_curve_rows.append(
                {
                    "year": int(curve_year),
                    "capacity_k": float(
                        curve_base
                        * np.exp(
                            curve_k * (curve_year - selected_reference.reference_year)
                        )
                    ),
                    "interpretation": "constant annual rate, seed=0",
                }
            )
        if curve_k > 0:
            for (
                ceiling_multiple
            ) in growth_config.calibration.illustrative_ceiling_multiples:
                illustrative_ceiling = curve_base * ceiling_multiple
                illustrative_midpoint = (
                    selected_reference.reference_year
                    + np.log(ceiling_multiple - 1) / curve_k
                )
                for curve_year, curve_value in zip(
                    curve_years,
                    s_curve(
                        curve_years,
                        illustrative_ceiling,
                        curve_k,
                        illustrative_midpoint,
                    ),
                ):
                    constant_curve_rows.append(
                        {
                            "year": int(curve_year),
                            "capacity_k": float(curve_value),
                            "interpretation": f"illustrative logistic ceiling {ceiling_multiple:g} × C0",
                        }
                    )
        constant_curve_table = pd.DataFrame(constant_curve_rows)
        constant_curve_chart = (
            alt.Chart(constant_curve_table)
            .mark_line()
            .encode(
                x=alt.X(
                    "year:Q",
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(format="d", tickCount=7),
                ),
                y=alt.Y(
                    "capacity_k:Q",
                    title="k vehicles (log scale)",
                    scale=alt.Scale(type="log", zero=False),
                ),
                color="interpretation:N",
                tooltip=["year:Q", "capacity_k:Q", "interpretation:N"],
            )
        )
        constant_curve_chart = constant_curve_chart + alt.Chart(
            pd.DataFrame([{"class_stock": selected_reference.class_stock_k_vehicles}])
        ).mark_rule(color="#555", strokeDash=[5, 5]).encode(y="class_stock:Q")
        constant_curve_display = chart_ui(
            constant_curve_chart.properties(width=1050, height=280)
        )
    else:
        constant_curve_display = mo.callout(
            "No represented positive stock reference: an anchored S-curve comparison cannot be calibrated for this target.",
            kind="warn",
        )
    mo.vstack(
        [
            mo.md(
                r"""
                ## Constant growth versus S-curve stages

                All curves below start at the selected 2023 deployment and share the same
                **early continuous coefficient** $k=\ln(1+b)$. Three arbitrary ceiling
                multiples expose how local growth at the same $C_0$ changes with unknown
                maturity. They are not target saturation estimates or recommendations.
                The dashed line is the 2023 class-stock reference, not a future fleet cap.

                The constant-rate line approximates the emergence limit; a logistic local
                rate is already smaller when $C_0/L$ is appreciable and slows thereafter.
                Growth inertia alone can thus permit implausible mature-market expansion
                if it is mistaken for a complete diffusion model. Calibration can use
                observed growth windows and current shares without presuming future $L$;
                the resulting bound must be reviewed with demand, turnover and competing
                technologies. An early high percentage should not be extrapolated as a
                prediction of market saturation or its date.
                """
            ),
            constant_curve_display,
        ]
    )
    return


@app.cell
def _(growth_config, inspect_table, mo, pd, selected_reference):
    seed_scale_table = pd.DataFrame(
        [
            {
                "class_stock_fraction_per_transition": f,
                "seed_k_vehicles": f * selected_reference.class_stock_k_vehicles,
                "seed_vehicles": f * selected_reference.class_stock_k_vehicles * 1000,
                "seed_relative_to_target_stock": f
                * selected_reference.class_stock_k_vehicles
                / selected_reference.stock_k_vehicles
                if selected_reference.stock_k_vehicles > 0
                else float("nan"),
            }
            for f in growth_config.calibration.seed_class_stock_fractions
        ]
    )
    mo.vstack(
        [
            inspect_table(seed_scale_table),
            mo.md(
                """
                A small fraction of total class stock can exceed a nascent target's whole
                deployment. Review seed scale against actual projects, delivery capacity,
                procurement cycles and the target region/class. Fixed batches and stock
                fractions above demonstrate this tradeoff; neither should become a default
                without evidence. Initiation from zero and accommodation of observed
                historical acceleration are different purposes for the same recurring term.
                """
            ),
        ]
    )
    return


@app.cell
def _(growth_summary, inspect_table, mo, pd):
    review_recommendations = pd.DataFrame(
        [
            {
                "mode": "growth_capacity",
                "recommendation_for_review": "Use quantity-verified stock windows, conditioned on inferred starting share; BEV/PHEV proxy ranges where supported. Preserve a separate rate for each physical target family.",
                "still_needed": "Direct FCEV/CNG stock; truck fuel coverage; retirement/available-capacity mapping; cross-country maturity transfer; EX/N identity.",
            },
            {
                "mode": "growth_new_capacity",
                "recommendation_for_review": "Use dated Canadian car registrations as flow evidence; test complete-period additions and stock/sales maturity contrast. Keep pooled LT and Norway commercial proxies explicitly provisional.",
                "still_needed": "Class-specific gross additions and trade/replacement allocation, previous-period reference and width, lifetime/cohort fallback resolution.",
            },
            {
                "mode": "growth_new_capacity_delta",
                "recommendation_for_review": "Test explicit native R=0 and empirical acceleration-seed frontiers from consecutive complete additions periods. Assess negative-change feasibility before calibration.",
                "still_needed": "Two comparable historical additions periods for each target, acceleration/pilot evidence, intended recurring allowance and engine null-rate contract.",
            },
        ]
    )
    mo.vstack(
        [
            mo.md(
                """
                ## Review outcome and remaining decisions

                Evidence supports **conditional, technology-specific constant-rate
                candidates**, rather than one pooled emergence rate. Starting deployment
                helps select a relevant observed phase and scale the bound; it does not
                identify a rate by itself. FCEV, CNG, heavy-truck and missing HEV stock
                evidence remain explicit gaps. A numerical analogue estimate in an
                insufficient row is not a recommendation to use it.

                Prefer observed positive windows when saturation is unknown. Use fitted
                logistic coefficients as a distinct early-limit sensitivity after checking
                identifiability; retain Gompertz and ceiling/tail comparisons as diagnostics.
                Current Canadian stocks are inferred and have heterogeneous source years,
                which limits how finely rates can be differentiated across regions/classes.

                Review the analogy choices, maturity match, candidate range, first-period
                time interpretation, physical membership and seed purpose before promotion.
                Existing capacity remains distinct from artificial allowances. All three
                formulation demonstrations are reproducible diagnostics, not accepted
                growth inputs; no eventual fleet saturation or timing is prescribed.
                """
            ),
            inspect_table(review_recommendations),
            mo.accordion(
                {"Unresolved items and reproducibility hashes": mo.json(growth_summary)}
            ),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
