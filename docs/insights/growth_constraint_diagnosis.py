"""Technology growth evidence, deployment calibration and constraint diagnostics.

Prepare with ``uv run python -m parameterization.road_adoption_constraints``.
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

    from parameterization.road_adoption_constraints import (
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
def growth_introduction(mo):
    mo.md(r"""
    # What evidence can bound vehicle deployment growth?

    **Research diagnostic · provisional rates and seeds**

    Compare observed growth, fitted curves and Canadian stock/registration references
    before choosing a deployment bound. Constant rates describe a growth allowance;
    they do not predict saturation or its timing. No candidate here is an accepted
    production parameter.

    Follow **registered observations → screening and fits → Canadian references →
    candidate rates and seeds → constraint behavior**. Controls read prepared tables;
    they do not fetch data or refit curves. Complete observations are in
    `inputs/1_interim/technology_growth/`; audits and candidate tables are in
    `inputs/validation/technology_growth/`. Table captions identify previews.

    Prepare explicitly from the repository root:

    ```powershell
    # Acquire missing registered snapshots only when needed:
    uv run python -m fetching.technology_growth_evidence --download-missing --papers-dir '<supplied PDF directory>'
    # Verify and analyze registered local evidence:
    uv run --offline python -m parameterization.road_adoption_constraints
    uv run --offline marimo run docs/insights/growth_constraint_diagnosis.py
    ```

    Before preparing this analysis, run the selected scenario's build or prerequisite
    and existing-capacity preparation. Shared stock, fuel-share and cohort tables must
    match that scenario. This optional analysis uses development dependencies and remains
    separate from scenario preparation. Changed source hashes require registry review;
    rebuild stale evidence explicitly rather than changing its recorded identity.
    """)
    return


@app.cell
def growth_evidence(load_config_bundle, load_diagnostic_config, load_diagnostic_tables, mo):
    growth_bundle = load_config_bundle("config/scenarios/default.yaml")
    growth_config = load_diagnostic_config(growth_bundle)
    preparation_error = None
    try:
        growth_tables = load_diagnostic_tables(growth_bundle)
    except FileNotFoundError as error:
        preparation_error = str(error)
    except ValueError as error:
        if not str(error).startswith((
            "Stale growth diagnostic handoff:",
            "Changed growth diagnostic artifact:",
            "Missing/changed registered evidence:",
        )):
            raise
        preparation_error = str(error)
    mo.stop(
        preparation_error is not None,
        mo.callout(mo.md(f"""
        ## Evidence preparation required

        The prepared growth evidence is unavailable or does not match the current
        registered inputs and configuration. No diagnostic results are shown.

        ```text
        {preparation_error}
        ```

        After preparing matching source and existing-capacity inputs for
        `config/scenarios/default.yaml`, run the owning offline analysis:

        ```powershell
        uv run --offline python -m parameterization.road_adoption_constraints
        ```

        Then reopen this notebook. Incomplete or incompatible prerequisites must be
        resolved by their owning preparation; changing recorded hashes or using
        stale tables would not reproduce the analysis.
        """), kind="warn"),
    )
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
                "These are upper-bound demonstrations. They do not publish growth parameters, run a solver or forecast deployment.",
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
                ## 1. What quantity and growth definition does each source measure?

                The manifest records files, hashes and access routes. HATCH country series and
                aggregate estimates describe different populations; an aggregate estimate cannot
                represent every country. The definitions below establish which rates can be compared.
                """
            ),
            inspect_table(growth_tables["source_manifest"], 100),
            mo.md(
                r"""
                | Evidence | Meaning of growth and consequence for this diagnostic |
                |---|---|
                | [Odenweller et al. (2022)](https://doi.org/10.1038/s41560-022-01097-4), supplied PDF pp. 9–10 | Emergence is the early, approximately exponential phase, when capacity is small relative to the ceiling. Annual fractional growth is $b=e^k-1$. Conventional wind/solar evidence uses moving seven-year exponential fits; complete emergency analogues use logistic fits. Their 15% floor and policy-conditioned demand pull are research assumptions, not vehicle lower bounds. Gompertz $\boldsymbol{k}$ is not an emergence fraction: they match its initial seven-year growth. |
                | [Nemet et al. (2023)](https://doi.org/10.1038/s43247-023-01056-1), main pp. 7–8; supplied SI pp. 16–21 | Logistic $\boldsymbol{k}$ characterizes early continuous relative growth. Formative growth uses projected data below 2.5% of fitted saturation; inflection growth uses a linear fit over ±5 years normalized by inflection level. Whole-series and fastest ten-year exponential growth are distinct observed-window questions. $T_{10–90}=\ln(81)/k$ is a duration. Their Gompertz maximum slope $Lk/e$ has level/year units before normalization. |
                | [Edwards et al. (2024)](https://doi.org/10.1073/pnas.2215679121), main methods; supplied SI pp. 9–15 and section F | SHARD analogy selection compares manufacturing, infrastructure, institutions and adoption context. The 20-observation/global/capacity screen was specific to DACCS. Logistic projections retain a ceiling and use a continuous coefficient in a discretized growth equation. SI tail and synthetic-early-data tests expose coefficient sensitivity. These choices do not justify copying a single DACCS analogue rate to vehicles. |
                | [Greene & Nemet extended research (2026)](https://doi.org/10.1038/s41467-026-73563-6), methods and SI pp. 4–5, 30 | The release adds country and technology characteristics and national logistic estimates. It standardizes some observations by cumulative calculation. Its 10–90 average continuous growth $\ln(9)/T$ differs from early logistic $\boldsymbol{k}$ and discrete $\boldsymbol{b}$. Associations with technology granularity/lifetime/adopter context help screen analogues; they do not estimate a Canadian vehicle parameter. |

                Here $\boldsymbol{k}$ has units **year⁻¹**, because its exponential argument contains
                years. A percentage reported from $\boldsymbol{k}$ is not automatically a discrete annual
                fraction. At finite logistic deployment,
                $\dot C/C=k(1-C/L)$; at inflection this is $k/2$.
                Gompertz has $\dot C/C=k\ln(L/C)$ and no finite, constant low-stock limit.
                We therefore never map a Gompertz coefficient directly into a growth rate.

                Notation differs: Nemet's exponential $a e^{bt}$ uses $\boldsymbol{b}$ as a continuous
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
                ## 2. Do the original and extended HATCH releases measure the same thing?

                Both the [original release](https://zenodo.org/records/10231865) and
                [extended release](https://zenodo.org/records/19579793) are retained. Pairing requires
                one country/technology/source/unit match. Changed IDs are allowed; ambiguous matches
                remain unpaired in the inventory.

                The extension adds definitions, characteristics and estimates, but also transforms
                quantities: its SI reports cumulative conversion of 329 unit-count series, removal
                of 95 global series and battery-capacity substitutions. Summed annual stock measures
                **stock-years**; summed additions measure installations before retirement. Neither
                automatically measures active stock. Extra nonmissing cells after standardization
                are not necessarily new observations, and zero/missing flags alone do not prove
                observed zero deployment.

                File and paper sample counts differ. Four CO₂-pipeline series lack published
                characteristics; this notebook reports audited file counts rather than forcing
                agreement with the main-paper 1,555 or SI 1,750 retained samples.
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
                The passenger-car example shows the consequence: extended CHAT levels sum the
                original annual counts rather than update the same fleet measure. The original
                counts remain a stock candidate pending the CHAT retirement definition. No rate
                from the cumulative transformation becomes a direct vehicle-stock rate.

                Other car series measure engine-power equivalent MW. Their growth can change with
                average power per vehicle; invariance to constant unit conversion does not make
                them vehicle-count proxies.
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
                ## 3. Which observations support a growth estimate?

                Preserve zeros, missing years and exclusion reasons. Active stock, annual output
                and market share have different uses; performance and average unit size do not
                measure deployment. The historical screen is compared with the configured research
                screen below.

                Windows count **year intervals**, require observed endpoints and enforce the gap
                limit. Windows containing zero have no log-growth estimate. No interpolation,
                positive floor or clipping creates early evidence. A high $\boldsymbol{R^2}$ does
                not establish a reliable ceiling or a rate transferable to Canadian vehicles.
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
            mo.md("""
                  ## 4. How sensitive are the fitted rates?
                  """),
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
                Logistic and Gompertz curves are fitted to observed levels from multiple starting
                points. Scaling by the observed maximum improves numerical stability; it does not
                supply a known saturation level. The 1.1 profile reproduces legacy ceiling bounds
                (0.9–1.1 × observed maximum, with inflection inside the sample). Broader profiles
                permit later inflections. These are fitting sensitivities, not future ceilings.
                Curves stop at their observation endpoints. The partial EAFO Norway 2026 observation
                is visible but excluded from annual estimates and fits.

                Check bound hits, unstable parameter combinations, unobserved inflections, curve
                family and sensitivity to ceiling bounds or removing the last five years. These
                ranges are **structural sensitivities, not confidence intervals**. Early observations
                can fit well while leaving ceiling and midpoint poorly determined. Observed-window
                rates avoid a future ceiling but still depend on phase, policy, denominator and window.
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
                No broad [*HATCH*](https://zenodo.org/records/10231865) quantile or fastest-country coefficient is assigned to a
                target. The original published logistic column is a reported coefficient
                percentage: division by 100 recovers $\\boldsymbol{k}$, and `expm1(k)` gives an early
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
                """
                **Compare matching estimates.** Both releases use the same curve family, fitting
                settings and observation treatment. Fits over identical overlapping data should
                agree; coverage changes and cumulative transformations can alter the estimates.
                These country-level sensitivities do not turn a published aggregate rate into a
                country estimate. Full paired comparisons remain in the audit.
                """
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
                ## 5. What can Norway's vehicle experience tell us?

                [EAFO Norway](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet)
                was captured on 7 October 2026. Fleet coverage reaches Q2 2026 and registrations
                July 2026; these partial observations are displayed, while annual fits and totals
                stop at **2025**. Counts, rounded shares, fuels and vehicle classes remain separate.
                Blank fields are missing; a rounded H₂ share of 0.00% does not prove zero vehicles.

                Norwegian M1, N1 and N2/N3 classes do not exactly match CANOE road classes. M1 is
                broader than Canadian cars; N2/N3 does not separate medium and heavy trucks. H₂
                needs a drivetrain definition before FCEV transfer. Stock also reflects trade and
                retirement, while registrations approximate gross additions. These boundaries
                limit direct transfer of a Norwegian rate to a Canadian class.
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
                ## 6. Which analogues are relevant to each target?

                Powertrain adoption has the closest quantity match, but Norwegian policy and class
                boundaries limit transfer. Historical cars describe durable-product diffusion,
                not necessarily replacement of an incumbent drivetrain. Appliances and bicycles
                inform manufacturing and household adoption; wind, pipelines and electrolysers
                illustrate infrastructure coordination and policy dependence.

                FCEV adoption also needs hydrogen supply, refuelling and fleet procurement. A few
                vehicles can produce a large percentage increase in a small H₂ fleet without
                sustained growth. HEV maturity and CNG fuel networks pose different conditions.
                Compare unit size, manufacturing, finance, adopter, policy, infrastructure and
                replacement cycle before selecting analogues; this evidence supplies no automatic
                ranking or equal weighting.
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
                ## 7. What is the Canadian starting fleet?

                Targets cover cars, passenger/freight light trucks and medium/heavy trucks. BEV/PHEV
                range alternatives are grouped for this diagnostic so they cannot multiply the
                same physical fleet. FCEV/FCHEV grouping still requires review.

                Reference stock is in **thousand vehicles**, with year shown below. It combines
                processed NRCan CEUD totals, registered age/cohort evidence and inferred fuel
                shares. The cohort audit conserves class stock and reconciles the production
                small-row threshold against solver capacity. The deployment reference then uses
                physical survivors on those published cohort keys. These are inferred powertrain
                totals; survival-curve mode can have higher installed-equivalent capacity.

                Fuel inputs vary by cohort/year. AB/NL Canada proxies and BC/BCT, PE/PEI aliases
                remain explicit. Passenger and freight light trucks share an unallocated
                new-registration pool. StatCan 23-10-0308-01 is registered stock: its 4,536–11,793 kg
                fuel mix proxies the full CEUD medium-truck class, whose total remains authoritative.
                Heavy-truck stock remains diesel-only. These stock distributions do not supply
                historical gross additions; surviving cohorts are not original sales.

                Missing FCEV/CNG representation does not establish zero deployment.
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
                    "Target-to-technology associations (diagnostic only)": inspect_table(
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
                supplies the rate. Absolute $\boldsymbol{C_0}$ alone never establishes growth speed.

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
                """
                **Candidates by technology.** Read each estimate with its evidence window and status.
                An `insufficient_target_evidence` value remains a sensitivity input, not a production
                parameter, even when a numerical estimate is available.
                """
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
                ## 8. What do Temoa's three growth bounds constrain?

                For one physical technology family, the upper-bound equations are:

                $$C_i\le S+(1+R)C_{i-1}$$
                $$N_i\le S+(1+R)N_{i-1}$$
                $$N_i-N_{i-1}\le S+(1+R)(N_{i-1}-N_{i-2})$$

                | Mode | Required quantity | Starting reference |
                |---|---|---|
                | `growth_capacity` | Active stock/available capacity. Cumulative installations need retirement accounting; shares need their changing fleet denominator. | Available group stock, including vintage survival and lifetime weights. |
                | `growth_new_capacity` | Gross additions over comparable model periods. Stock differences omit replacements; sales shares need total sales. Production needs trade/allocation review. | One previous gross-additions period with compatible width, class and region. |
                | `growth_new_capacity_delta` | Change between comparable periods' additions. At $R=0$, $S$ bounds their second difference; negative preceding changes matter. | Two previous additions periods and their difference, plus evidence for an acceleration allowance. |

                $R$ is a **fraction per transition**; $S$ is capacity, here thousand vehicles.
                $C_i$ is available stock and $N_i$ new-vintage capacity. The seed $S$ recurs at
                every transition. [Temoa documentation](https://docs.temoaproject.org/en/latest/extensions/growth_rates.html)
                calls rates annual, but the
                [pinned implementation](https://github.com/TemoaProject/temoa/tree/a2b0bf24cc0dd2a99570ae48772316341e79c0b5/temoa/extensions/growth_rates)
                applies `1 + rate` without elapsed-year exponentiation. Recheck later engine revisions.

                To preserve an annual fraction $b$ across equal $\Delta$ years, use
                $R=(1+b)^\Delta-1$, or $R=e^{k\Delta}-1$ for logistic early growth. Unequal
                transition widths need separate treatment. A two-year 2023→2025 reference differs
                from a five-year 2020→2025 label transition; this diagnostic does not settle that
                first-period production contract.

                An annual level-recurrence seed $s$ becomes $S=s[(1+b)^\Delta-1]/b$, or $s\Delta$
                at $b=0$. This is not an automatic conversion for additions deltas. Unequal-period
                sales totals also need width normalization.

                The pinned engine's initial stock reference uses adjusted existing capacity,
                lifetime fractions and the last existing period. New-capacity bounds reference
                the last existing vintage; delta bounds use two. Existing `_EX` and future `_N`
                technologies must be associated before these fallbacks represent one fleet.
                The production mapping from cohorts to `CAPAVL` still requires an engine check.
                Diagnostic membership does not create `tech_groups`, and surviving cohorts do not
                measure gross sales.
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
                ## 9. How large a recurring seed would past additions require?

                Annual registrations are summed only within complete five-year blocks: 2011–2015
                is labelled 2010, 2016–2020 labelled 2015 and 2021–2025 labelled 2020. These are
                **historical analysis bins**, not accepted CANOE vintage mappings. Incomplete blocks
                stay missing; no interpolation or cumulative sum fills them. Counts are divided
                by 1,000.

                For a fixed $R$, the smallest nonnegative constant seed covering observed levels is
                $\max(0,\max_i[y_i-(1+R)y_{i-1}])$; for additions changes it is
                $\max(0,\max_i[D_i-(1+R)D_{i-1}])$. These historical bounds absorb shocks and
                policy changes as well as initiation. They do not justify a large recurring seed.
                Stock comparisons are annual; new/delta comparisons use complete five-year additions,
                so seed magnitudes are not interchangeable.

                A zero seed merits testing when a reviewed positive reference and rate already
                cover the transitions. Starting from measured zero needs separate pilot/procurement
                evidence. One, ten and 100 vehicles equal 0.001, 0.01 and 0.1 thousand vehicles;
                these batches and stock fractions are scale tests, not general recommendations.
                FCEV allowances require actual baseline and infrastructure/project evidence.
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

                A positive prior $\boldsymbol{D}$ can decelerate under an upper bound; a negative prior
                $\boldsymbol{D}$ may force further decline. A negative upper bound conflicts with
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
                ## 10. When does a constant rate depart from an S-curve?

                Curves start at the selected deployment and share the **early continuous coefficient**
                $k=\ln(1+b)$. Ceiling multiples test how assumed maturity changes local growth at
                the same $C_0$. The dashed class-stock reference is not a future cap.

                A constant rate approximates early emergence. Logistic growth is lower once
                $C_0/L$ matters and slows with deployment. Demand, retirement and competing
                technologies still need review: an early growth fraction predicts neither saturation
                nor its date.
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
                ## What can inform a production choice, and what remains unresolved?

                Read each technology's candidate alongside its status, quantity and observation
                window. Starting stock helps match deployment stage and scale a bound; it does
                not determine growth speed. FCEV, CNG, heavy-truck and missing HEV stock evidence
                remain gaps. Numbers in insufficient rows are sensitivity inputs.

                Observed positive windows avoid assuming saturation. Fitted logistic coefficients
                provide a separate early-growth sensitivity after checking whether their parameters
                are identifiable. Gompertz, ceiling and observation-tail comparisons remain diagnostics.
                Inferred Canadian stock and mixed source years limit regional/class precision.

                **Before promotion:** review analogy transfer, deployment stage, first-period timing,
                physical technology membership and the purpose of the recurring seed. Document these
                choices separately from existing capacity and artificial allowances. The three
                formulations show their consequences without choosing a saturation level or date.
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
