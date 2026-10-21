"""Recorded road-efficiency source distributions and existing-to-future bridge."""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd

    from validation.ldv_efficiency_diagnostic import COHORT, VIEW_MODES, load_diagnostic, select_rating_statistic

    def chart_ui(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def table_ui(frame, label):
        return mo.ui.table(frame, selection=None, pagination=True, page_size=10, label=label)

    return COHORT, VIEW_MODES, alt, chart_ui, load_diagnostic, mo, pd, select_rating_statistic, table_ui


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

    def trajectory_chart(frame, title, *, native=False, facet="units", width=820, interpolation=False):
        plot = frame.reset_index(drop=True).copy()
        plot["range_label"] = plot.powertrain.map(range_label)
        plot["style_category"] = plot.range_label if native else plot.series
        plot["marker_category"] = plot.style_category
        indexed = "sample_kind" in plot and plot.sample_kind.isin(["curve", "period", "base_year_anchor"]).any()
        if indexed:
            plot["marker_category"] = plot.marker_source.fillna("")
        plot["curve"] = (plot.series + " / " + plot.powertrain + " / "
                         + plot.get("source_class", pd.Series("", index=plot.index)).fillna("") + " / "
                         + plot.get("index_segment", pd.Series("", index=plot.index)).fillna("").astype(str))
        styles = sorted(plot.style_category.unique())
        dashes = [[], [8, 4], [2, 3], [10, 3, 2, 3], [12, 5], [6, 3, 2, 3, 2, 3]]
        shapes = ["square", "cross", "triangle-up", "diamond", "circle", "triangle-down"]
        if len(styles) > len(dashes):
            raise ValueError("Facet trajectories before adding more method styles")
        if native:
            classes = sorted(diagnostic.frames["raw_sources"].loc[
                diagnostic.frames["raw_sources"].vehicle_group.isin(plot.vehicle_group.unique())
                & diagnostic.frames["raw_sources"].series.str.startswith("NRCan"), "source_class"].unique())
            color = alt.Color("source_class:N", title="NRCan size class", sort=classes,
                              scale=alt.Scale(domain=classes, scheme="tableau20"),
                              legend=alt.Legend(orient="right", labelLimit=270, values=sorted(plot.source_class.unique())))
        else:
            color = range_color("range_label", plot.range_label)
        style_title = "Range variant" if native else "Evidence / method"
        faceted = plot[facet].nunique() > 1
        y_title = None if faceted and facet in {"units", "axis_basis"} else str(plot.units.iloc[0])
        # Keep the observed coordinate fixed; move only label positions, with leaders.
        plot["label_value"] = float("nan")
        plot["base_label"] = ""
        base_year = diagnostic.summary["base_year"]
        anchors = plot.year.eq(base_year) & plot.powertrain.str.startswith(("bev", "phev"))
        if indexed:
            anchors &= plot.sample_kind.eq("base_year_anchor")
        else:
            anchors &= plot.series.str.contains("ATB", regex=False) & (not native)
        for _, panel in plot.groupby(facet, dropna=False):
            selected = panel.loc[anchors].drop_duplicates("range_label").sort_values("value")
            if selected.empty:
                continue
            span = max(float(panel.value.max() - panel.value.min()), abs(float(panel.value.mean())) * .25, .01)
            gap = span * 18 / 265
            positions = []
            for value in selected.value:
                positions.append(max(float(value), positions[-1] + gap) if positions else float(value))
            shift = (sum(positions) - selected.value.sum()) / len(positions)
            service = "metric" in selected and selected.metric.eq("service_efficiency").all()
            ranked = selected.loc[~selected.powertrain.str.endswith("_existing_representative")]
            ranks = {index: rank for rank, index in enumerate(ranked.sort_values("value", ascending=not service).index, 1)}
            for rank, (index, row) in enumerate(selected.iterrows(), 1):
                plot.loc[index, "label_value"] = positions[rank - 1] - shift
                rank_label = f"{ranks[index]}. " if index in ranks else ""
                plot.loc[index, "base_label"] = f"{rank_label}{row.range_label} · {row.value:.3g}"
        tooltip_fields = ["series:N", "source_class:N", "atb_class:N", "powertrain:N", "year:Q", "vintage:O",
                          "value:Q", "units:N", "time_basis:N", "treatment:N", "analogue:N", "native_basis:N",
                          "source_fuel:N", "consumption_index:Q", "historical_index_anchor:N", "calibration_observation:N",
                          "observation_basis:N", "source_treatments:N", "cohort_stock_coverage:Q", "candidate_year_count:Q", "included_year_count:Q",
                          "sample_kind:N", "marker_source:N", "source_year_start:O", "source_year_end:O", "load_factor:Q"]
        tooltip_options = {"value:Q": {"format": ".5g"},
                           "cohort_stock_coverage:Q": {"title": "Included cohort stock", "format": ".1%"},
                           "candidate_year_count:Q": {"title": "Candidate years", "format": "d"},
                           "included_year_count:Q": {"title": "Included years", "format": "d"}}
        tooltips = [alt.Tooltip(field, **tooltip_options.get(field, {}))
                    for field in tooltip_fields if field.split(":")[0] in plot and plot[field.split(":")[0]].notna().any()]
        data = alt.Data(values=plot.astype(object).where(plot.notna(), None).to_dict("records"))
        base = alt.Chart(data).encode(
            x=alt.X("year:Q", title="Source year / period position", axis=alt.Axis(format="d", tickMinStep=1)),
            y=alt.Y("value:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=color, detail="curve:N", order="year:Q",
            tooltip=tooltips,
        )
        lines = base.mark_line(strokeWidth=2.4)
        if indexed:
            lines = lines.transform_filter(
                alt.datum.sample_kind == "curve" if interpolation else
                alt.FieldOneOfPredicate(field="sample_kind", oneOf=["period", "base_year_anchor"])
            )
        else:
            lines = lines.encode(
                strokeDash=alt.StrokeDash("style_category:N", title=style_title, sort=styles,
                    scale=alt.Scale(domain=styles, range=dashes[:len(styles)]),
                    legend=alt.Legend(orient="right", symbolType="stroke", symbolStrokeWidth=3,
                                      symbolSize=500, labelLimit=310)),
            )
        marker_order = {"Final prepared period CSV": "diamond", "Recalculated period": "circle", "Production range component": "square",
                        "ATB class recalculation": "cross", "Base-year anchor": "triangle-up"}
        marker_styles = ([name for name in marker_order if name in set(plot.marker_category)] if indexed else styles)
        marker_shapes = [marker_order[name] for name in marker_styles] if indexed else shapes[:len(styles)]
        # Separate fields plus explicit legend resolution keep both keys visible.
        points = base.mark_point(size=65, filled=True, opacity=.95).encode(
            shape=alt.Shape("marker_category:N", title="Period / anchor evidence" if indexed else style_title + " · markers", sort=marker_styles,
                            scale=alt.Scale(domain=marker_styles, range=marker_shapes),
                            legend=alt.Legend(orient="right", labelLimit=310)),
        )
        if indexed:
            points = points.transform_filter(alt.FieldOneOfPredicate(field="sample_kind", oneOf=["period", "base_year_anchor"]))
        elif not native:
            points = points.transform_filter(
                (alt.datum.year % 5 == 0) | (alt.datum.year == diagnostic.summary["base_year"])
                | (alt.datum.year == int(plot.year.min())) | (alt.datum.year == int(plot.year.max()))
            )
        boundary = alt.Chart(data).transform_aggregate(rows="count()").mark_rule(
            color="#9299a1", strokeWidth=1, strokeDash=[4, 4]).encode(x=alt.datum(base_year))
        labels = alt.Chart(data).transform_filter(alt.datum.base_label != "")
        leaders = labels.mark_rule(strokeWidth=.8, opacity=.8).encode(
            x=alt.X("year:Q"), x2=alt.datum(base_year + 1), y=alt.Y("value:Q"), y2="label_value:Q", color=color,
        )
        text = labels.mark_text(align="left", baseline="middle", dx=4, fontSize=11, fontWeight=600).encode(
            x=alt.datum(base_year + 1), y=alt.Y("label_value:Q"), text="base_label:N", color=color,
            tooltip=["powertrain:N", alt.Tooltip("value:Q", format=".5g"), "units:N", "series:N"],
        )
        halo = labels.mark_text(align="left", baseline="middle", dx=4, fontSize=11, fontWeight=600,
                                color="white", stroke="white", strokeWidth=3).encode(
            x=alt.datum(base_year + 1), y=alt.Y("label_value:Q"), text="base_label:N",
        )
        chart = alt.layer(lines, points, boundary, leaders, halo, text).resolve_legend(
            strokeDash="independent", shape="independent", color="shared",
        ).properties(width=width, height=265)
        if faceted:
            return chart.facet(row=alt.Row(facet + ":N", title=None,
                header=alt.Header(labelAngle=270, labelOrient="left", labelLimit=250, labelPadding=10))).resolve_scale(y="independent").properties(title=title)
        return chart.properties(title=title)

    return range_color, range_label, trajectory_chart


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Historical and future road efficiency

    Compare the source values, their energy harmonisation and the indexed bridge at
    NLR ATB vehicle-class resolution. The notebook reads the **latest successful
    diagnostic evidence build**. It opens recorded data and configuration together;
    the bridge can explicitly recalculate mean or median alternatives from those
    same recorded inputs. Changing a scenario file alone does not change the build.

    Run the prerequisite command below when inputs or the backend change, then reload.
    Opening this notebook performs no ETL. Start with the rated make-model
    distributions, then follow the production range anchors into the period bridge.
    The optional MTO powertrain benchmark remains diagnostic evidence.
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
        Production NRCan statistic: **distinct make-model {diagnostic.summary['rating_statistic']}**.
        Historical period efficiencies, marker years and load factors use the
        raw cohort-stock weights from the fleet age distribution, before lifetime
        eligibility or redistribution. The final prepared CSVs
        and database identity belong to this same build.
        """),
        mo.accordion({"Recorded build and input identity": mo.json({"build": build, "inputs": diagnostic.summary["inputs"]})}),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## NRCan make-model distributions behind the means

    A **catalogue row** is one rated variant, such as an engine/transmission option.
    A **distinct make-model** is one exact NRCan Make/Model label within a model year,
    size class and powertrain/range bucket. We first average a model's rated variants;
    each resulting dot contributes equally to the **mean and median**, shown together.
    These are rating observations, not vehicle stocks. Boxes show the middle half of
    distinct-model values, with a median tick and min–max whiskers. Black diamonds
    show means and black crosses show medians, slightly separated vertically for
    visibility while retaining their exact consumption coordinates. The audit tables retain catalogue counts and
    source variants, so labels with many ratings remain identifiable.

    Columns select **individual model years at five-year intervals**, beginning in
    2005; they do not pool years into five-year aggregates. Every family uses the same
    columns, with blank panels for missing cohorts. Liquid-fuel panels share the
    displayed limits. Edge triangles identify values outside that window; tooltips
    retain the full values, and every observation still contributes to the summaries.
    PHEV CD uses its own equivalent-consumption limits,
    and BEV appears last with a separate kWh/100 km axis.

    PEV buckets group heterogeneous rated vehicles; a longer-range group can have
    lower consumption because its make-model composition differs. Averaging those
    populations does not identify a controlled battery-weight penalty. **After baseline**
    means the observation year is later than the build's recorded base year;
    these observations are displayed but excluded from calibration.
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
    representative_years = [year for year in diagnostic.summary["available_rating_years"]
                            if year >= diagnostic.summary["minimum_model_year"]
                            and year % diagnostic.summary["distribution_year_step"] == 0]
    year_labels = {year: f"{year} · after baseline" if year > diagnostic.summary["base_year"] else str(year)
                   for year in representative_years}
    family_panels = []
    distribution_panel_count = 0

    def distribution_chart(plot, title, unit, *, show_year_title):
        plot = plot.copy()
        plot["range_label"] = plot.powertrain.map(range_label)
        plot["cohort"] = plot.rating_class + (" · " + plot.range_label if plot.powertrain_family.iloc[0] in {"bev", "phev"} else "")
        plot["period"] = plot.year.map(year_labels)
        groups = ["cohort", "range_label", "period"]
        identities = [*groups, "year", "rating_class", "atb_class", "Make", "Model", "make_model", "powertrain", "native_unit"]
        models = plot.groupby(identities, dropna=False, as_index=False).agg(
            native_consumption=("native_consumption", "mean"), range_km=("range_km", "mean"),
            catalogue_rows=("native_consumption", "count"),
        )
        cohorts = sorted(plot.cohort.unique())
        # Explicit empty groups preserve the same year columns across all families.
        placeholders = pd.DataFrame([{"period": year_labels[year], "year": year, "cohort": cohort}
                                     for year in representative_years for cohort in cohorts])
        display = pd.concat([models, placeholders], ignore_index=True)
        # Bounded inline fields keep marimo's chart transport independent of wide source tables.
        records = display.astype(object).where(display.notna(), None).to_dict("records")
        data = alt.Data(values=records)
        y_axis = alt.Y("cohort:N", title=None, sort=cohorts, scale=alt.Scale(domain=cohorts),
                       axis=alt.Axis(labelLimit=235, minExtent=240, maxExtent=240))
        color = range_color("range_label", plot.range_label)
        domain = None if plot.powertrain_family.iloc[0] == "bev" else diagnostic.summary[
            "distribution_cd_consumption_domains" if unit.endswith("· CD") else "distribution_consumption_domains"
        ][distribution_group.value]
        scale = alt.Scale(zero=True) if domain is None else alt.Scale(domain=domain, nice=False)
        observed = alt.Chart(data).transform_filter("isValid(datum.native_consumption)")
        summary = observed.transform_aggregate(
            minimum="min(native_consumption)", lower="q1(native_consumption)", median="median(native_consumption)",
            upper="q3(native_consumption)", maximum="max(native_consumption)", model_mean="mean(native_consumption)",
            count="count()", catalogue_rows="sum(catalogue_rows)", groupby=groups,
        ).encode(y=y_axis, color=color)
        whiskers = summary.mark_rule(clip=True).encode(x=alt.X("minimum:Q", title=unit, scale=scale), x2="maximum:Q")
        boxes = summary.mark_bar(size=12, opacity=.45, clip=True).encode(x="lower:Q", x2="upper:Q")
        medians = summary.mark_tick(color="#555", thickness=1.5, size=13, clip=True).encode(x="median:Q")
        dots = observed.mark_point(size=21, opacity=.4, filled=True, clip=True).encode(
            y=y_axis, x="native_consumption:Q", color=color,
            tooltip=["year:O", "rating_class:N", "atb_class:N", "Make:N", "Model:N", "powertrain:N", "native_consumption:Q",
                     "native_unit:N", "range_km:Q", "catalogue_rows:Q"],
        )
        statistics = summary.transform_fold(["model_mean", "median"], as_=["statistic", "statistic_value"]).transform_calculate(
            statistic_label="datum.statistic === 'model_mean' ? 'Mean' : 'Median'",
        ).mark_point(size=75, filled=True, clip=True).encode(
            x=alt.X("statistic_value:Q"), color=alt.value("#151515"),
            yOffset=alt.YOffset("statistic_label:N", scale=alt.Scale(domain=["Mean", "Median"], range=[-4, 4])),
            shape=alt.Shape("statistic_label:N", title="Distinct make-models", sort=["Mean", "Median"],
                            scale=alt.Scale(domain=["Mean", "Median"], range=["diamond", "cross"]),
                            legend=alt.Legend(orient="right", symbolSize=90)),
            tooltip=["cohort:N", "period:N", "statistic_label:N", alt.Tooltip("statistic_value:Q", format=".3f"),
                     alt.Tooltip("count:Q", title="Distinct make-models"), "catalogue_rows:Q"],
        )
        layers = [whiskers, boxes, medians, dots, statistics]
        if domain is not None:
            outside = models.native_consumption.notna() & ~models.native_consumption.between(*domain)
            outside_count = int(outside.sum())
            edge = observed.transform_filter(
                (alt.datum.native_consumption < domain[0]) | (alt.datum.native_consumption > domain[1])
            ).transform_calculate(
                edge_value=f"clamp(datum.native_consumption, {domain[0]}, {domain[1]})",
            ).encode(
                y=y_axis, x="edge_value:Q", color=color,
                tooltip=["year:O", "rating_class:N", "Make:N", "Model:N", "native_consumption:Q", "native_unit:N"],
            )
            layers.extend([
                edge.transform_filter(alt.datum.native_consumption < domain[0]).mark_point(shape="triangle-left", size=55, filled=True, opacity=.85),
                edge.transform_filter(alt.datum.native_consumption > domain[1]).mark_point(shape="triangle-right", size=55, filled=True, opacity=.85),
            ])
            if outside_count:
                title += f" · {outside_count} model values beyond axis limits (triangles)"
        return alt.layer(*layers).properties(
            width=140, height=max(100, 22 * len(cohorts)),
        ).facet(column=alt.Column("period:N", title="Selected NRCan model year" if show_year_title else None, sort=list(year_labels.values()),
                                  header=alt.Header(labelLimit=180, labelFontSize=11)), spacing=5).properties(title=title)

    for family in ["gasoline", "diesel", "hev", "phev", "cng", "bev"]:
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
            family_panels.append(chart_ui(distribution_chart(panel, f"{family.upper()} · {unit}", unit,
                                                              show_year_title=distribution_panel_count == 0)))
            distribution_panel_count += 1
    cohort_summary = diagnostic.frames["rating_summary"]
    cohort_summary = cohort_summary.loc[cohort_summary.vehicle_group.eq(distribution_group.value)]
    observation_columns = [column for column in ["year", "rating_class", "powertrain", "Make", "Model", "Engine size (L)", "Cylinders", "Transmission", "Motor (kW)", "native_consumption", "native_unit", "range_km",
                          "combined_cs_consumption_L_100_km", "combined_cd_consumption_Le_100_km",
                          "source_row", "component", "equal_row_contribution", "model_rows_in_cohort", "make_model_contribution", "calibration_observation"] if column in distribution]
    distribution_output = mo.vstack([
        *family_panels,
        mo.accordion({
            "All years: distinct-model mean and median, with catalogue audit": table_ui(cohort_summary, "All rating cohorts"),
            "All years: rated rows and make-model contributions": table_ui(distribution[observation_columns], "NRCan source observations"),
        }),
    ])
    distribution_output
    return


@app.cell(hide_code=True)
def _(diagnostic, mo, range_label):
    bev_anchor = range_label(diagnostic.summary["anchor_powertrains"]["bev"])
    phev_anchor = range_label(diagnostic.summary["anchor_powertrains"]["phev"])
    mo.md(f"""
    ## OMEGA range composition and production anchors

    **{bev_anchor}** and **{phev_anchor}** supply each
    family's historical NRCan change rates. Each range retains its own ATB baseline
    level and future trajectory. Existing fleet consumption combines the range
    components using the recorded OMEGA shares. This avoids inferring a battery-weight
    penalty from differently composed NRCan range buckets.

    The bars compare cars and light trucks within each powertrain family; colors
    identify range classes. The table connects each production baseline to its
    family anchor and existing OMEGA representative.
    """)
    return


@app.cell
def _(alt, chart_ui, diagnostic, mo, range_color, range_label, table_ui):
    shares = diagnostic.frames["omega_shares"].copy()
    shares["range_label"] = shares.bucket.map(range_label)
    shares["vehicle_group"] = shares.market_class.map({"car": "Cars", "light_truck": "Light trucks"})
    shares_chart = alt.Chart(shares).mark_bar(size=75).encode(
        x=alt.X("vehicle_group:N", title=None, sort=["Cars", "Light trucks"], axis=alt.Axis(labelAngle=0)),
        y=alt.Y("share_of_all_sales:Q", stack="zero", title="Share within family", scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(format="%")),
        color=range_color("range_label", shares.range_label), order=alt.Order("bucket:N", sort="ascending"),
        tooltip=["vehicle_group:N", "powertrain:N", "range_label:N", "sales:Q", alt.Tooltip("share_of_all_sales:Q", format=".1%")],
    ).properties(width=320, height=240).facet(column=alt.Column("powertrain:N", title="Powertrain family", sort=["BEV", "PHEV"]))
    anchor_columns = ["vehicle_group", "atb_class", "powertrain", "anchor", "base_year", "atb_anchor_base_mj_per_vkm",
                      "production_base_mj_per_vkm", "existing_representative_base_mj_per_vkm", "omega_share"]
    mo.vstack([chart_ui(shares_chart), table_ui(diagnostic.frames["anchor_comparison"][anchor_columns], "Production range baselines and family anchors")])
    return


@app.cell(hide_code=True)
def _(bridge_diagnostic, mo):
    mo.md(f"""
    ## Existing-to-future bridge

    Each dedicated view follows three steps over the recorded bridge window:
    **NRCan native units → harmonised NRCan and ATB energy → indexed consumption or service efficiency**.
    Native NRCan size classes use color; range variants use dashes and marker shapes.
    ATB enters the comparison on the harmonised energy axis. Lower consumption means
    greater efficiency. Separate line and marker keys show the same method categories.
    Recorded production statistic: **distinct make-model {bridge_diagnostic.summary['recorded_rating_statistic']}**.
    Selected statistic: **{bridge_diagnostic.summary['rating_statistic']}**. The control
    recalculates the complete source-to-period comparison from recorded inputs using
    the production formulas. It does not run ETL or replace the prepared build.

    **Graph 3** shows the period values and exact ATB base-year anchor, connected
    by straight segments. These segments do not fit or smooth the production values.
    Optional **graph 4** separately shows shape-preserving cubic interpolation
    (PCHIP) through those consumption markers. It is diagnostic only and does not
    create model periods or change their parameters. Both statistics use the same
    displayed future horizon. Annual fluctuations are not fitted.
    Annual indexed calculations are retained in the detail table. Detailed ATB classes
    are **diagnostic recalculations**, using the same formulas and renormalising the
    NRCan size-class weights within that ATB class. They are not independently observed
    fleet cohorts. Light-truck service efficiency uses the passenger service view.
    Detailed classes inherit their parent-mode CEUD load factors and broad
    car/light-truck OMEGA range shares; these are not class-specific observations.
    Partially observed periods use their available annual consumption values;
    tooltips and the detail table report how much cohort stock those years cover.
    No missing NRCan years are interpolated. A wholly empty period can use only
    the configured, explicitly recorded analogue policy.

    **Final prepared period CSV** markers identify the recorded production result.
    Selecting a different statistic labels the alternative **Recalculated period**;
    those values have not been exported by the build. Both metric views describe the
    same periods using their native consumption, corresponding service efficiency
    and load factor. Historical aggregation uses raw cohort-stock counts from the
    fleet age distribution, before lifetime eligibility or redistribution. Consumption
    and load factors are averaged first; service-output conversion is the final step.
    Range-component markers show the inputs to the existing
    OMEGA representative; detailed-class markers identify diagnostic recalculations.

    Historical markers are positioned at their included-cohort-stock-weighted mean year;
    future markers use interval-end source years. Tooltips retain the model vintage and source-year
    bounds. The grey line marks the base year. Range labels rank variants at their
    actual coordinate (1 = most efficient); the OMEGA representative is labelled separately.
    Leaders separate nearby labels without
    moving observations. The BEV300 / PHEV35 historical indices and ATB baseline
    levels are the production method, with OMEGA aggregation for the existing fleet.
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
    bridge_statistic = mo.ui.dropdown(options={"Mean": "mean", "Median": "median"},
                                     value=diagnostic.summary["rating_statistic"].title(), label="Bridge distinct make-model statistic")
    show_interpolation = mo.ui.checkbox(value=False, label="Show diagnostic PCHIP interpolation (graph 4)")
    mo.vstack([mo.hstack([bridge_statistic, bridge_metric], justify="start"), show_interpolation])
    return bridge_metric, bridge_statistic, class_controls, family_controls, show_interpolation


@app.cell
def _(bridge_statistic, diagnostic, select_rating_statistic):
    bridge_diagnostic = select_rating_statistic(diagnostic, bridge_statistic.value)
    return (bridge_diagnostic,)


@app.cell
def _(COHORT, VIEW_MODES, bridge_diagnostic, bridge_metric, chart_ui, class_controls, family_controls, mo, show_interpolation, table_ui, trajectory_chart):
    def bridge_view(group):
        chosen_class, family = class_controls.value[group], family_controls.value[group]
        start, end = bridge_diagnostic.summary["bridge_years"]
        rows = bridge_diagnostic.frames["bridge"]
        rows = rows.loc[rows.vehicle_group.eq(group) & rows.atb_class.eq(chosen_class)
                        & rows.powertrain_family.eq(family) & rows.year.between(start, end)].copy()
        raw = bridge_diagnostic.frames["raw_sources"]
        index_fuel = bridge_diagnostic.summary["historical_index_fuels"].get(VIEW_MODES[group], {}).get(family)
        index_context = raw.series.eq("NRCan CEUD observed on-road consumption") & raw.powertrain.eq(index_fuel)
        raw = raw.loc[raw.vehicle_group.eq(group) & (raw.powertrain_family.eq(family) | index_context)
                      & raw.year.between(start, end) & raw.series.str.startswith("NRCan")].copy()
        if chosen_class != COHORT:
            raw = raw.loc[raw.atb_class.eq(chosen_class) | raw.series.eq("NRCan CEUD observed on-road consumption")]

        def curves(frame, title, *, native=False, interpolation=False):
            if frame.empty:
                return mo.md(f"**{title}:** no prepared evidence for this selection.")
            frame = frame.copy()
            frame["axis_basis"] = frame.units
            if family == "phev" and "stage" in frame and frame.stage.eq("Harmonised before indexing").all():
                frame["axis_basis"] = "MJ per vehicle-km · operating bases differ"
            return chart_ui(trajectory_chart(frame, title, native=native, facet="axis_basis", interpolation=interpolation))

        note = ("Historical medium/heavy curves apply coarse CEUD fuel indices to each ATB class/vocation where a historical bridge is configured. The pre-index CEUD line shows the parent fleet's fuel-energy level, used only through its base-year-normalised ratio. It is not an observed consumption level for the selected powertrain or vocation. All supported future powertrains retain their direct ATB projections."
                if group in {"Medium trucks", "Heavy trucks"} else
                "PHEV: NRCan reports CS fuel and CD equivalent consumption per kilometre in each operating mode. Chart 2 shows those unweighted component rates beside ATB's utility-weighted total; they are not three estimates of the same total. "
                f"The selected calculation backcasts with the {bridge_diagnostic.summary['phev_historical_index']} ratio against the ATB base-year total. "
                "PHEV35 supplies the historical CD change rates for both PHEV ranges; CS remains visible only as a source component in charts 1 and 2."
                if family == "phev" else
                f"Native NRCan curves show the distinct make-model {bridge_diagnostic.summary['rating_statistic']} within each size class. The harmonised aggregate applies the recorded MTO size-class weights. "
                + ("BEV300 supplies the historical BEV change rates against each range's ATB baseline; OMEGA combines existing-fleet ranges."
                   if family == "bev" else "The indexed panel follows the production baseline and ATB improvement ratios."))
        recorded = bridge_diagnostic.summary["rating_statistic"] == bridge_diagnostic.summary["recorded_rating_statistic"]
        period_title = "3. Production method: prepared period values" if recorded else "3. Recalculated period values: selected statistic"
        if chosen_class != COHORT:
            period_title = "3. Diagnostic class recalculation: " + ("recorded statistic" if recorded else "selected statistic")
        interpolation_rows = rows.loc[rows.stage.isin(["Indexed bridge", "Diagnostic interpolation"]) & rows.metric.eq("consumption")]
        return mo.vstack([
            mo.hstack([class_controls[group], family_controls[group]], widths="equal"), mo.md(note),
            curves(raw, "1. Native NRCan trajectories", native=True),
            curves(rows.loc[rows.stage.eq("Harmonised before indexing")], "2. Harmonised energy before indexing"),
            curves(rows.loc[rows.stage.eq("Indexed bridge") & rows.metric.eq(bridge_metric.value)], period_title),
            *([curves(interpolation_rows, "4. Diagnostic interpolation through consumption periods", interpolation=True)]
              if show_interpolation.value else []),
            mo.accordion({"Inspect bridge values and timing": table_ui(rows, f"{group}: bridge evidence")}),
        ])

    mo.ui.tabs({group: bridge_view(group) for group in VIEW_MODES}, lazy=False)
    return


@app.cell(hide_code=True)
def _(chart_ui, diagnostic, mo, table_ui, trajectory_chart):
    benchmark = diagnostic.frames["mto_benchmark"].dropna(subset=["value"])
    benchmark = benchmark.loc[benchmark.powertrain_family.eq("bev") & benchmark.year.between(*diagnostic.summary["bridge_years"])]
    benchmark_chart = trajectory_chart(benchmark, "NRCan / MTO benchmark · MJ/vkm", facet="ceud_class")
    mo.accordion({
        "MTO cohort-weighting benchmark and statistical limits": mo.vstack([
            mo.md("""
            MTO keys retain their promoted **vehicle-class / vintage** role. They do
            not observe fuel, trim or PEV range. Exact-key powertrain weighting is
            retained below as a diagnostic comparison; unresolved survivors and
            uneven matching make these trajectories noisy and potentially biased.
            The table's method and matched-stock fields identify the baseline
            fallback used for wholly uncovered cohorts.

            Bootstrapping those matched records would reproduce the selection bias
            and missing labels. It cannot establish population-equivalent powertrain
            efficiency or repair unobserved range allocations. A defensible future
            estimator first needs representative sampling or independently validated
            propulsion labels and inclusion probabilities. No bootstrap is fitted.
            """), chart_ui(benchmark_chart),
            table_ui(benchmark, "Diagnostic MTO weighting and recorded rating aggregation"),
            mo.json(diagnostic.summary["benchmark_stock_dispositions"]),
        ]),
        "Interpretation and source limitations": mo.md("\n\n".join(diagnostic.summary["limitations"])),
    })
    return


if __name__ == "__main__":
    app.run()
