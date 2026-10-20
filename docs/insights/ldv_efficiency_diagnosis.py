"""Recorded road-efficiency source distributions and existing-to-future bridge."""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd

    from validation.ldv_efficiency_diagnostic import COHORT, VIEW_MODES, load_diagnostic

    def chart_ui(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def table_ui(frame, label):
        return mo.ui.table(frame, selection=None, pagination=True, page_size=10, label=label)

    return COHORT, VIEW_MODES, alt, chart_ui, load_diagnostic, mo, pd, table_ui


@app.cell
def _(alt, diagnostic, pd):
    def range_label(value):
        return str(value).replace("_miles", "").replace("_existing_representative", " existing").replace("_", " ").upper()

    range_colors = {
        "BEV 150": "#4477aa", "BEV 200": "#ddaa33", "BEV 300": "#228833", "BEV 400": "#aa3377",
        "PHEV 35": "#009988", "PHEV 50": "#ee7733", "BEV EXISTING": "#444444", "PHEV EXISTING": "#444444",
        "BEV": "#4477aa", "PHEV": "#009988", "GASOLINE": "#bb5566", "DIESEL": "#4477aa",
        "HEV": "#228833", "CNG": "#aa3377", "FCEV": "#66aabb",
    }

    def range_color(field, values):
        domain = [value for value in range_colors if value in set(values)]
        return alt.Color(field + ":N", title="Powertrain / range", sort=domain,
                         scale=alt.Scale(domain=domain, range=[range_colors[value] for value in domain]),
                         legend=alt.Legend(orient="right", labelLimit=250))

    def trajectory_chart(frame, title, *, native=False, facet="units", width=820):
        plot = frame.copy()
        plot["range_label"] = plot.powertrain.map(range_label)
        plot["style_category"] = plot.range_label if native else plot.series
        plot["marker_category"] = plot.style_category
        plot["curve"] = (plot.series + " / " + plot.powertrain + " / "
                         + plot.get("source_class", pd.Series("", index=plot.index)).fillna("") + " / "
                         + plot.get("index_segment", pd.Series("", index=plot.index)).fillna("").astype(str))
        styles = sorted(plot.style_category.unique())
        dashes = [[1, 0], [8, 4], [2, 3], [10, 3, 2, 3], [12, 5], [6, 3, 2, 3, 2, 3]]
        shapes = ["square", "cross", "triangle-up", "diamond", "circle", "triangle-down"]
        if len(styles) > len(dashes):
            raise ValueError("Facet trajectories before adding more method styles")
        if native:
            classes = sorted(diagnostic.frames["raw_sources"].loc[
                diagnostic.frames["raw_sources"].vehicle_group.isin(plot.vehicle_group.unique())
                & diagnostic.frames["raw_sources"].series.str.startswith("NRCan"), "source_class"].unique())
            color = alt.Color("source_class:N", title="NRCan size class", sort=classes,
                              scale=alt.Scale(domain=classes, scheme="tableau20"),
                              legend=alt.Legend(orient="right", labelLimit=270))
        else:
            color = range_color("range_label", plot.range_label)
        style_title = "Range variant" if native else "Evidence / method"
        faceted = plot[facet].nunique() > 1
        y_title = None if faceted and facet in {"units", "axis_basis"} else str(plot.units.iloc[0])
        data = alt.Data(values=plot.astype(object).where(plot.notna(), None).to_dict("records"))
        base = alt.Chart(data).encode(
            x=alt.X("year:Q", title="Actual source / calculation year", axis=alt.Axis(format="d", tickMinStep=1)),
            y=alt.Y("value:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=color, detail="curve:N", order="year:Q",
            tooltip=["series:N", "source_class:N", "atb_class:N", "powertrain:N", "year:O", "vintage:O",
                     alt.Tooltip("value:Q", format=".5g"), "units:N", "time_basis:N", "treatment:N", "analogue:N", "native_basis:N",
                     "source_fuel:N", "consumption_index:Q", "index_basis:N", "index_support:N", "calibration_observation:N"],
        )
        lines = base.mark_line(strokeWidth=2.4).encode(
            strokeDash=alt.StrokeDash("style_category:N", title=style_title, sort=styles,
                scale=alt.Scale(domain=styles, range=dashes[:len(styles)]),
                legend=alt.Legend(orient="right", symbolType="stroke", symbolStrokeWidth=3,
                                  symbolSize=500, labelLimit=310)),
        )
        # Separate fields plus explicit legend resolution keep both keys visible.
        points = base.mark_point(size=65, filled=True, opacity=.95).encode(
            shape=alt.Shape("marker_category:N", title=style_title + " · markers", sort=styles,
                            scale=alt.Scale(domain=styles, range=shapes[:len(styles)]),
                            legend=alt.Legend(orient="right", labelLimit=310)),
        )
        if not native:
            points = points.transform_filter(
                (alt.datum.year % 5 == 0) | (alt.datum.year == diagnostic.summary["base_year"])
                | (alt.datum.year == int(plot.year.min())) | (alt.datum.year == int(plot.year.max()))
                | (alt.datum.series == "Final prepared period CSV"))
        boundary = alt.Chart(data).transform_aggregate(rows="count()").mark_rule(
            color="#9299a1", strokeWidth=1, strokeDash=[4, 4]).encode(x=alt.datum(diagnostic.summary["base_year"]))
        chart = alt.layer(lines, points, boundary).resolve_legend(
            strokeDash="independent", shape="independent", color="shared",
        ).properties(width=width, height=265, title=title)
        if faceted:
            return chart.facet(row=alt.Row(facet + ":N", title=None,
                header=alt.Header(labelAngle=0, labelLimit=180))).resolve_scale(y="independent")
        return chart

    return range_color, range_label, trajectory_chart


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Historical and future road efficiency

    Compare the source values, their energy harmonisation and the indexed bridge at
    NLR ATB vehicle-class resolution. The notebook reads the **latest successful
    diagnostic evidence build**. It opens recorded data and configuration together;
    changing a scenario file cannot reinterpret an earlier result.

    Run the prerequisite command below when inputs or the backend change, then reload.
    Opening this notebook performs no ETL. Exploratory anchors and MTO benchmarks
    remain diagnostic evidence.
    """)
    return


@app.cell
def _(mo):
    from validation.notebook_evidence import prepare_command

    refresh = mo.ui.run_button(label="Reload latest successful build")
    mo.vstack([mo.md(f"```powershell\n{prepare_command()}\n```"), refresh])
    return (refresh,)


@app.cell
def _(load_diagnostic, refresh):
    refresh.value
    diagnostic = load_diagnostic()
    return (diagnostic,)


@app.cell(hide_code=True)
def _(diagnostic, mo):
    build = diagnostic.summary["build"]
    mo.vstack([
        mo.md(f"""
        Evidence build: **{build['build_id']}** · prepared **{build['built_at']}** ·
        base year **{diagnostic.summary['base_year']}** · region **{diagnostic.summary['region']}** ·
        MTO edition **{diagnostic.summary['population_year']}** · ATB **{diagnostic.summary['atb_trajectory']}**.
        The final prepared CSVs and database identity belong to this same build.
        """),
        mo.accordion({"Recorded build and input identity": mo.json({"build": build, "inputs": diagnostic.summary["inputs"]})}),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Existing-to-future bridge

    Each dedicated view follows three steps over the recorded bridge window:
    **NRCan native units → harmonised NRCan and ATB energy → indexed consumption or service efficiency**.
    Native NRCan size classes use color; range variants use dashes and marker shapes.
    ATB enters the comparison on the harmonised energy axis. Lower consumption means
    greater efficiency. Separate line and marker keys show the same method categories.

    “Production class aggregate” follows the promoted calculation and includes final
    prepared period values where the selected metric permits comparison. Detailed
    ATB classes are **diagnostic recalculations**, using the same formulas and
    renormalising the existing NRCan size-class weights within that ATB class.
    They are not independently observed fleet cohorts. Historical source-year gaps
    remain visible. Light-truck service efficiency uses the passenger service view.
    Detailed classes inherit their parent-mode CEUD load factors and broad
    car/light-truck OMEGA range shares; these are not class-specific observations.

    The vertical line marks the base year. Future model vintages use interval-end
    source years; historical final CSV points are period means plotted at their
    last source year. Their tooltips retain the vintage and timing basis.
    """)
    return


@app.cell
def _(COHORT, VIEW_MODES, diagnostic, mo):
    class_controls = mo.ui.dictionary({
        group: mo.ui.dropdown(
            options=[COHORT, *sorted(diagnostic.frames["bridge"].loc[
                diagnostic.frames["bridge"].vehicle_group.eq(group)
                & diagnostic.frames["bridge"].atb_class.ne(COHORT), "atb_class"].unique())],
            value=COHORT, label=f"{group}: ATB class / vocation", full_width=True,
        ) for group in VIEW_MODES
    })
    family_controls = mo.ui.dictionary({
        group: mo.ui.dropdown(
            options=sorted(diagnostic.frames["bridge"].loc[diagnostic.frames["bridge"].vehicle_group.eq(group), "powertrain_family"].dropna().unique()),
            value="bev" if group in {"Cars", "Light trucks"} else "diesel",
            label=f"{group}: powertrain family", full_width=True,
        ) for group in VIEW_MODES
    })
    bridge_metric = mo.ui.dropdown(options={"Energy consumption (MJ/vkm)": "consumption", "Service efficiency": "service_efficiency"},
                                  value="Energy consumption (MJ/vkm)", label="Indexed bridge metric")
    bridge_metric
    return bridge_metric, class_controls, family_controls


@app.cell
def _(COHORT, VIEW_MODES, bridge_metric, chart_ui, class_controls, diagnostic, family_controls, mo, table_ui, trajectory_chart):
    def bridge_view(group):
        chosen_class, family = class_controls.value[group], family_controls.value[group]
        start, end = diagnostic.summary["bridge_years"]
        rows = diagnostic.frames["bridge"]
        rows = rows.loc[rows.vehicle_group.eq(group) & rows.atb_class.eq(chosen_class)
                        & rows.powertrain_family.eq(family) & rows.year.between(start, end)].copy()
        raw = diagnostic.frames["raw_sources"]
        index_fuel = diagnostic.summary["historical_index_fuels"].get(VIEW_MODES[group], {}).get(family)
        index_context = raw.series.eq("NRCan CEUD observed on-road consumption") & raw.powertrain.eq(index_fuel)
        raw = raw.loc[raw.vehicle_group.eq(group) & (raw.powertrain_family.eq(family) | index_context)
                      & raw.year.between(start, end) & raw.series.str.startswith("NRCan")].copy()
        if chosen_class != COHORT:
            raw = raw.loc[raw.atb_class.eq(chosen_class) | raw.series.eq("NRCan CEUD observed on-road consumption")]

        def curves(frame, title, *, native=False):
            if frame.empty:
                return mo.md(f"**{title}:** no prepared evidence for this selection.")
            frame = frame.copy()
            frame["axis_basis"] = frame.units
            if family == "phev" and "stage" in frame and frame.stage.eq("Harmonised before indexing").all():
                frame["axis_basis"] = "MJ per vehicle-km · operating bases differ"
            return chart_ui(trajectory_chart(frame, title, native=native, facet="axis_basis"))

        note = ("Historical medium/heavy curves apply coarse CEUD fuel indices to each ATB class/vocation where a historical bridge is configured. The pre-index CEUD line shows the parent fleet's fuel-energy level, used only through its base-year-normalised ratio. It is not an observed consumption level for the selected powertrain or vocation. All supported future powertrains retain their direct ATB projections."
                if group in {"Medium trucks", "Heavy trucks"} else
                "PHEV: NRCan reports CS fuel and CD equivalent consumption per kilometre in each operating mode. Chart 2 shows those unweighted component rates beside ATB's utility-weighted total; they are not three estimates of the same total. "
                f"This recorded build backcasts with the {diagnostic.summary['phev_historical_index']} ratio against the ATB base-year total. "
                "The other operating-mode index is shown only for observed cohorts as a diagnostic comparison; its gaps are not filled."
                if family == "phev" else
                "Native NRCan curves are equal catalogue-row means by size class. The harmonised aggregate applies the recorded MTO size-class weights; the indexed panel then applies the production baseline and ATB improvement ratios.")
        return mo.vstack([
            mo.hstack([class_controls[group], family_controls[group]], widths="equal"), mo.md(note),
            curves(raw, "1. Native NRCan trajectories", native=True),
            curves(rows.loc[rows.stage.eq("Harmonised before indexing")], "2. Harmonised energy before indexing"),
            curves(rows.loc[rows.stage.eq("Indexed bridge") & rows.metric.eq(bridge_metric.value)], "3. Indexed bridge and exploratory anchor"),
            mo.accordion({"Inspect bridge values and timing": table_ui(rows, f"{group}: bridge evidence")}),
        ])

    mo.ui.tabs({group: bridge_view(group) for group in VIEW_MODES}, lazy=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## PEV sole-baseline alternative

    The exploratory curves use **BEV300** for BEV and **PHEV35** for PHEV. Their
    prominence can be checked against the recorded OMEGA shares below. BEV uses the
    NRCan baseline (including any recorded approved analogue); PHEV retains the
    accepted ATB utility-weighted total-energy level.

    Because current PHEV projections already use ATB total-energy levels, the
    PHEV35 shared anchor reproduces their current future levels. The meaningful
    comparison there is with the existing representative and its historical index.

    For target range *r*, the candidate is
    **anchor baseline × ATB consumption(r, year) / ATB consumption(anchor, base year)**.
    This applies one Canadian level adjustment to all range variants and preserves
    ATB's relative range penalties. Reusing only the anchor's improvement rate would
    not establish those relative levels. Existing OMEGA representatives retain their
    sales-weighted consumption. The table quantifies the candidate's base-year gap
    against that existing representative and the promoted range-specific baseline.
    """)
    return


@app.cell
def _(alt, chart_ui, diagnostic, mo, range_color, range_label, table_ui):
    shares = diagnostic.frames["omega_shares"].copy()
    shares["range_label"] = shares.bucket.map(range_label)
    shares["vehicle_group"] = shares.market_class.map({"car": "Cars", "light_truck": "Light trucks"})
    shares_chart = alt.Chart(shares).mark_bar(size=75).encode(
        x=alt.X("powertrain:N", title="Powertrain family", sort=["BEV", "PHEV"], axis=alt.Axis(labelAngle=0)),
        y=alt.Y("share_of_all_sales:Q", stack="zero", title="Share within family", scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(format="%")),
        color=range_color("range_label", shares.range_label), order=alt.Order("bucket:N", sort="ascending"),
        tooltip=["vehicle_group:N", "powertrain:N", "range_label:N", "sales:Q", alt.Tooltip("share_of_all_sales:Q", format=".1%")],
    ).properties(width=320, height=240).facet(column=alt.Column("vehicle_group:N", title=None))
    anchor_columns = ["vehicle_group", "atb_class", "powertrain", "anchor", "anchor_baseline_analogue", "shared_scale",
                      "production_base_mj_per_vkm", "candidate_base_mj_per_vkm", "existing_representative_base_mj_per_vkm",
                      "candidate_discontinuity_vs_existing_representative", "candidate_change_vs_production", "omega_share"]
    mo.vstack([chart_ui(shares_chart), table_ui(diagnostic.frames["anchor_comparison"][anchor_columns], "Exploratory anchor levels and existing-to-future gaps")])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## NRCan make-model distributions behind the means

    Production takes an arithmetic mean of **catalogue rows** within each
    size-class / powertrain / model-year cohort, then applies reviewed MTO **size-class**
    weights. A make-model with more rated variants therefore contributes more rows.
    These are rating observations, not vehicle stocks. The boxplot and points expose
    this spread. Black diamonds show catalogue-row means; black crosses show
    equal-distinct-model means. The alternative is not promoted. Each family has
    its own panel, with representative five-year observations arranged horizontally.
    These are individual model years, not five-year pooled averages; blank cohorts
    have no ratings. The tables retain all available years.
    Distinct models use exact NRCan Make/Model labels; engine, transmission and
    motor details in the source-row table expose variants sharing the same label.

    PEV buckets group heterogeneous rated vehicles; a longer-range group can have
    lower consumption because its make-model composition differs. Averaging those
    populations does not identify a controlled battery-weight penalty. Post-base-year
    observations are displayed as **holdouts**, excluded from baseline calibration.
    """)
    return


@app.cell
def _(mo):
    distribution_group = mo.ui.dropdown(options=["Cars", "Light trucks"], value="Cars", label="Distribution vehicle group")
    distribution_group
    return (distribution_group,)


@app.cell
def _(alt, chart_ui, diagnostic, distribution_group, mo, pd, range_color, range_label, table_ui):
    distribution = diagnostic.frames["rating_distribution"]
    distribution = distribution.loc[distribution.vehicle_group.eq(distribution_group.value)].copy()
    representative_years = [year for year in diagnostic.summary["available_rating_years"] if year % 5 == 0]
    family_panels = []

    def distribution_chart(plot, title, unit):
        plot = plot.copy()
        plot["range_label"] = plot.powertrain.map(range_label)
        plot["cohort"] = plot.rating_class + (" · " + plot.range_label if plot.powertrain_family.iloc[0] in {"bev", "phev"} else "")
        plot["period"] = plot.year.map(lambda year: f"{year} · holdout" if year > diagnostic.summary["base_year"] else str(year))
        groups = ["cohort", "range_label", "period"]
        # Inline only plotted fields: marimo's Arrow transport loses fields for
        # these wide source tables in the browser despite valid standalone SVGs.
        fields = [*groups, "year", "rating_class", "atb_class", "Make", "Model", "make_model", "powertrain",
                  "native_consumption", "native_unit", "range_km", "equal_row_contribution",
                  "model_rows_in_cohort", "make_model_contribution", "calibration_observation"]
        records = plot[fields].astype(object).where(plot[fields].notna(), None).to_dict("records")
        data = alt.Data(values=records)
        y_axis = alt.Y("cohort:N", title=None, sort=sorted(plot.cohort.unique()), axis=alt.Axis(labelLimit=235))
        color = range_color("range_label", plot.range_label)
        # Min/max whiskers, quartiles and medians share one cohort grain.
        summary = alt.Chart(data).transform_aggregate(
            minimum="min(native_consumption)", lower="q1(native_consumption)", median="median(native_consumption)",
            upper="q3(native_consumption)", maximum="max(native_consumption)", row_mean="mean(native_consumption)",
            count="count()", groupby=groups,
        ).encode(y=y_axis, color=color)
        whiskers = summary.mark_rule().encode(x=alt.X("minimum:Q", title=unit, scale=alt.Scale(zero=False)), x2="maximum:Q")
        boxes = summary.mark_bar(size=12, opacity=.45).encode(x="lower:Q", x2="upper:Q")
        medians = summary.mark_tick(color="#555", thickness=1.5, size=13).encode(x="median:Q")
        dots = alt.Chart(data).mark_point(size=17, opacity=.32, filled=True).encode(
            y=y_axis, x="native_consumption:Q", color=color,
            tooltip=["year:O", "rating_class:N", "atb_class:N", "Make:N", "Model:N", "powertrain:N", "native_consumption:Q",
                     "native_unit:N", "range_km:Q", "equal_row_contribution:Q", "model_rows_in_cohort:Q",
                     "make_model_contribution:Q", "calibration_observation:N"],
        )
        mean_shape = alt.Shape("mean_method:N", title="Mean", scale=alt.Scale(
            domain=["Catalogue rows", "Distinct make-models"], range=["diamond", "cross"]), legend=alt.Legend(orient="right"))
        row_mean = summary.transform_calculate(mean_method="'Catalogue rows'").mark_point(size=55, filled=True).encode(
            x="row_mean:Q", color=alt.value("#222"), shape=mean_shape,
            tooltip=["cohort:N", "period:N", "row_mean:Q", "count:Q"],
        )
        model_mean = alt.Chart(data).transform_aggregate(
            model_value="mean(native_consumption)", groupby=[*groups, "make_model"],
        ).transform_aggregate(model_mean="mean(model_value)", groupby=groups).transform_calculate(
            mean_method="'Distinct make-models'",
        ).mark_point(size=65, filled=True).encode(y=y_axis, x="model_mean:Q", color=alt.value("#222"), shape=mean_shape,
            tooltip=["cohort:N", "period:N", "model_mean:Q"])
        return alt.layer(whiskers, boxes, medians, dots, row_mean, model_mean).properties(
            width=150, height=max(100, 22 * plot.cohort.nunique()),
        ).facet(column=alt.Column("period:N", title="NRCan model year", sort=sorted(plot.period.unique()))).properties(title=title)

    for family in ["gasoline", "diesel", "hev", "bev", "phev", "cng"]:
        family_rows = distribution.loc[distribution.powertrain_family.eq(family) & distribution.year.isin(representative_years)].copy()
        if family_rows.empty:
            family_panels.append(mo.md(f"**{family.upper()}:** no observations at the five-year display intervals; see all-year tables below."))
            continue
        if family == "phev":
            # Preserve both reported operating modes, independently of the selected index.
            components = []
            for field, unit in [("combined_cs_consumption_L_100_km", "L/100 km · CS"),
                                ("combined_cd_consumption_Le_100_km", "Le/100 km · CD")]:
                component_rows = family_rows.assign(native_consumption=family_rows[field], native_unit=unit)
                components.append(component_rows)
            family_rows = pd.concat(components, ignore_index=True)
        for unit, panel in family_rows.groupby("native_unit", sort=False):
            family_panels.append(chart_ui(distribution_chart(panel, f"{family.upper()} · {unit}", unit)))
    cohort_summary = diagnostic.frames["rating_summary"]
    cohort_summary = cohort_summary.loc[cohort_summary.vehicle_group.eq(distribution_group.value)]
    observation_columns = [column for column in ["year", "rating_class", "powertrain", "Make", "Model", "Engine size (L)", "Cylinders", "Transmission", "Motor (kW)", "native_consumption", "native_unit", "range_km",
                          "combined_cs_consumption_L_100_km", "combined_cd_consumption_Le_100_km",
                          "source_row", "component", "equal_row_contribution", "model_rows_in_cohort", "make_model_contribution", "calibration_observation"] if column in distribution]
    distribution_output = mo.vstack([
        *family_panels,
        mo.accordion({
            "All years: equal-row versus equal-distinct-model means": table_ui(cohort_summary, "All rating cohorts"),
            "All years: rated rows and make-model contributions": table_ui(distribution[observation_columns], "NRCan source observations"),
        }),
    ])
    distribution_output
    return


@app.cell(hide_code=True)
def _(chart_ui, diagnostic, mo, table_ui, trajectory_chart):
    benchmark = diagnostic.frames["mto_benchmark"].dropna(subset=["value"])
    benchmark = benchmark.loc[benchmark.powertrain_family.eq("bev") & benchmark.year.between(*diagnostic.summary["bridge_years"])]
    benchmark_chart = trajectory_chart(benchmark, "Catalogue / MTO benchmark · MJ/vkm", facet="ceud_class")
    mo.accordion({
        "MTO cohort-weighting benchmark and statistical limits": mo.vstack([
            mo.md("""
            MTO keys retain their promoted **vehicle-class / vintage** role. They do
            not observe fuel, trim or PEV range. Exact-key powertrain weighting is
            retained below as a diagnostic comparison; unresolved survivors and
            uneven matching make these trajectories noisy and potentially biased.
            Wholly uncovered cohorts reuse the catalogue baseline; the table's
            method and matched-stock fields identify those fallbacks.

            Bootstrapping those matched records would reproduce the selection bias
            and missing labels. It cannot establish population-equivalent powertrain
            efficiency or repair unobserved range allocations. A defensible future
            estimator first needs representative sampling or independently validated
            propulsion labels and inclusion probabilities. No bootstrap is fitted.
            """), chart_ui(benchmark_chart),
            table_ui(benchmark, "Diagnostic MTO weighting versus promoted catalogue aggregation"),
            mo.json(diagnostic.summary["benchmark_stock_dispositions"]),
        ]),
        "Interpretation and source limitations": mo.md("\n\n".join(diagnostic.summary["limitations"])),
    })
    return


if __name__ == "__main__":
    app.run()
