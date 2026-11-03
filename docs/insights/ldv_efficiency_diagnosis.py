"""Recorded road-efficiency source composition and existing-to-future bridge."""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd

    from validation.ldv_efficiency_diagnostic import COHORT, VIEW_MODES, composition_class_columns, composition_detail_frames, composition_heatmap_data, composition_review_cases, composition_review_summary, composition_view_settings, distribution_metric_rows, load_diagnostic, nrcan_anchor_endpoints, practical_axis, supported_display_years

    def chart_ui(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def table_ui(frame, label):
        if not frame.empty:
            frame = frame.dropna(axis=1, how="all")
        formats = {
            column: "{:,.1f}" if "(%)" in column else "{:,.0f}" if pd.api.types.is_integer_dtype(frame[column].dtype)
                or any(word in column.lower() for word in ("count", "rows", "models", "families", "variants", "year"))
                else "{:,.3f}"
            for column in frame.select_dtypes(include="number")
        }
        return mo.ui.table(frame, selection=None, pagination=True, page_size=15, label=label,
                           format_mapping=formats, show_data_types=False, show_column_summaries=False,
                           freeze_columns_left=["Target model year"] if "Target model year" in frame else [], max_columns=None)

    def gasoline_hev_classification_view(ratings, classes):
        rows = ratings.loc[ratings.rating_class.isin(classes) & ratings.native_unit.eq("L/100 km")
                           & ratings["Fuel type"].isin(["X", "Z"])].copy()
        rows["propulsion_label"] = rows.powertrain.map({"gasoline": "Gasoline", "hev": "HEV"}).fillna("Unresolved gasoline/HEV")
        unresolved = rows.get("propulsion_resolved", pd.Series(False, index=rows.index)).ne(True)
        rows.loc[unresolved, "propulsion_label"] = "Unresolved gasoline/HEV"
        methods = {
            "hybrid_name": "NRCan hybrid wording",
            "exact_epa_hybrid": "Same-year named EPA hybrid configuration",
            "exact_epa_conventional": "Same-year named EPA gasoline configuration",
            "configuration_epa_hybrid": "Compatible EPA hybrid configuration",
            "configuration_epa_conventional": "Compatible EPA gasoline configuration",
            "exact_epa_unresolved": "EPA configuration evidence unavailable or conflicting",
            "configuration_epa_unresolved": "EPA configuration evidence unavailable or conflicting",
            "unverified_source_fuel": "No supported EPA configuration correspondence",
            "source_fuel": "NRCan fuel field",
        }
        rows["evidence_label"] = rows.classification_method.map(methods).fillna("Classification evidence unavailable")
        columns = {"year": "Model year", "Make": "Make", "Model": "Model", "rating_class": "Size class",
            "Engine size (L)": "Engine (L)", "Cylinders": "Cylinders", "Transmission": "Transmission",
            "Fuel type": "Fuel code", "propulsion_label": "Propulsion", "evidence_label": "Classification evidence",
            "included": "Eligible?", "component": "Source component", "source_row": "Source row"}
        fields = [field for field in columns if field in rows and (rows.empty or rows[field].notna().any())]
        return rows[fields].sort_values([field for field in ("year", "Make", "Model", "source_row") if field in fields],
                                        kind="stable").rename(columns=columns)

    return COHORT, VIEW_MODES, alt, chart_ui, composition_class_columns, composition_detail_frames, composition_heatmap_data, composition_review_cases, composition_review_summary, composition_view_settings, distribution_metric_rows, gasoline_hev_classification_view, load_diagnostic, mo, nrcan_anchor_endpoints, pd, practical_axis, supported_display_years, table_ui


@app.cell
def trajectory_helpers(alt, composition_view, diagnostic, nrcan_anchor_endpoints, pd):
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

    def trajectory_chart(frame, title, *, width, native=False, facet="units", y_domain=None, year_domain=None):
        plot = frame.reset_index(drop=True).copy()
        if year_domain is not None:
            plot = plot.loc[plot.year.between(*year_domain)].copy()
        if not native and "stage" in plot and plot.stage.eq("Harmonised before indexing").all():
            plot = nrcan_anchor_endpoints(diagnostic, plot)
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
            available_classes = set(diagnostic.frames["raw_sources"].loc[
                diagnostic.frames["raw_sources"].vehicle_group.isin(plot.vehicle_group.unique())
                & diagnostic.frames["raw_sources"].series.str.startswith("NRCan"), "source_class"].unique())
            ordered = [size for group in plot.vehicle_group.unique() for size in composition_view["class_order"].get(group, [])]
            classes = list(dict.fromkeys([size for size in ordered if size in available_classes]))
            classes += sorted(available_classes - set(classes))
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
        plot["label_year"] = float("nan")
        plot["base_label"] = ""
        base_year = diagnostic.summary["base_year"]
        anchors = plot.powertrain.str.startswith(("bev", "phev"))
        nrcan_anchors = plot.get("nrcan_anchor_window", pd.Series("", index=plot.index)).ne("")
        if indexed:
            anchors &= plot.sample_kind.eq("base_year_anchor") & plot.series.eq("Production method")
        else:
            anchors &= (plot.year.eq(diagnostic.summary["atb_reference_year"])
                        & plot.series.str.contains("ATB", regex=False) & (not native))
            anchors |= nrcan_anchors
        for _, panel in plot.groupby(facet, dropna=False):
            selected = panel.loc[anchors].drop_duplicates(["range_label", "series"]).sort_values("value")
            if selected.empty:
                continue
            span = max(float(panel.value.max() - panel.value.min()), abs(float(panel.value.mean())) * .25, .01)
            gap = span * (30 if nrcan_anchors.any() else 18) / 290
            positions = []
            for value in selected.value:
                positions.append(max(float(value), positions[-1] + gap) if positions else float(value))
            shift = (sum(positions) - selected.value.sum()) / len(positions)
            service = "metric" in selected and selected.metric.eq("service_efficiency").all()
            ranked = selected.loc[~selected.powertrain.str.endswith("_existing_representative") & ~nrcan_anchors]
            ranks = {index: rank for rank, index in enumerate(ranked.sort_values("value", ascending=not service).index, 1)}
            for rank, (index, row) in enumerate(selected.iterrows(), 1):
                plot.loc[index, "label_value"] = positions[rank - 1] - shift
                plot.loc[index, "label_year"] = row.year - 7 if nrcan_anchors.loc[index] else row.year + 1
                rank_label = f"{ranks[index]}. " if index in ranks else ""
                plot.loc[index, "base_label"] = (f"{row.range_label} · {row.value:.3g}\n"
                    f"Cohort anchor: {row.nrcan_anchor_consumption:.3g}"
                    if nrcan_anchors.loc[index] else f"{rank_label}{row.range_label} · {row.value:.3g}")
        tooltip_fields = ["series:N", "source_class:N", "atb_class:N", "powertrain:N", "year:Q", "vintage:O",
                          "value:Q", "units:N", "time_basis:N", "treatment:N", "analogue:N", "native_basis:N",
                          "source_fuel:N", "consumption_index:Q", "historical_index_anchor:N", "projection_anchor_powertrain:N", "calibration_observation:N",
                          "observation_basis:N", "source_treatments:N", "cohort_stock_coverage:Q", "candidate_year_count:Q", "included_year_count:Q",
                          "sample_kind:N", "marker_source:N", "source_year_start:O", "source_year_end:O", "source_years:N",
                          "weighted_source_year:Q", "model_vintage:O", "load_factor:Q"]
        tooltip_options = {"value:Q": {"format": ".5g"},
                           "historical_index_anchor:N": {"title": "Reference powertrain"},
                           "cohort_stock_coverage:Q": {"title": "Included cohort stock", "format": ".1%"},
                           "candidate_year_count:Q": {"title": "Candidate years", "format": "d"},
                           "included_year_count:Q": {"title": "Included years", "format": "d"}}
        tooltips = [alt.Tooltip(field, **tooltip_options.get(field, {}))
                    for field in tooltip_fields if field.split(":")[0] in plot and plot[field.split(":")[0]].notna().any()]
        first_year, last_year = year_domain if year_domain is not None else composition_view["bridge_years"]
        tick_years = set(range((first_year // 5 + 1) * 5, last_year + 1, 5)) | {first_year, last_year}
        tick_years = {year for year in tick_years if year in {first_year, last_year}
            or min(year - first_year, last_year - year) >= 2}
        if first_year <= base_year <= last_year:
            tick_years.add(base_year)
        if indexed:
            tick_years.update(plot.loc[plot.sample_kind.isin(["period", "base_year_anchor"]), "year"])
        data = alt.Data(values=plot.astype(object).where(plot.notna(), None).to_dict("records"))
        base = alt.Chart(data).encode(
            x=alt.X("year:Q", title="Source year", scale=alt.Scale(domain=[first_year, last_year], nice=False),
                    axis=alt.Axis(format="d", values=sorted(tick_years), labelOverlap=False)),
            y=alt.Y("value:Q", title=y_title, scale=alt.Scale(zero=False, domain=y_domain if y_domain is not None else alt.Undefined)),
            color=color, detail="curve:N", order="year:Q",
            tooltip=tooltips,
        )
        lines = base.mark_line(strokeWidth=2.4)
        if indexed:
            lines = lines.transform_filter(
                alt.FieldOneOfPredicate(field="sample_kind", oneOf=["period", "base_year_anchor"])
            )
            if plot.series.nunique() > 1:
                methods = sorted(plot.series.unique())
                lines = lines.encode(strokeDash=alt.StrokeDash("series:N", title="Aggregation method",
                    scale=alt.Scale(domain=methods, range=[[8, 4] if "Catalogue" in name or "catalogue" in name else [] for name in methods]),
                    legend=alt.Legend(orient="right", symbolType="stroke", symbolSize=420, labelLimit=310)))
        else:
            lines = lines.encode(
                strokeDash=alt.StrokeDash("style_category:N", title=style_title, sort=styles,
                    scale=alt.Scale(domain=styles, range=dashes[:len(styles)]),
                    legend=alt.Legend(orient="right", symbolType="stroke", symbolStrokeWidth=3,
                                      symbolSize=500, labelLimit=310)),
            )
        marker_order = {"Final prepared period CSV": "diamond", "Recalculated period": "circle", "Production range component": "square",
                        "Cohort fallback median": "cross",
                        "ATB class recalculation": "cross", "NRCan-cohort projection anchor": "triangle-up",
                        "CEUD-cohort projection anchor": "triangle-up",
                        "Base-year anchor": "triangle-up", "ATB source projection": "triangle-down"}
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
                | (alt.datum.base_label != "")
            )
        boundary = alt.Chart(data).transform_aggregate(rows="count()").mark_rule(
            color="#9299a1", strokeWidth=1, strokeDash=[4, 4]).encode(x=alt.datum(base_year))
        labels = alt.Chart(data).transform_filter(alt.datum.base_label != "")
        leaders = labels.mark_rule(strokeWidth=.8, opacity=.8).encode(
            x=alt.X("year:Q"), x2="label_year:Q", y=alt.Y("value:Q"), y2="label_value:Q", color=color,
        )
        text = labels.mark_text(align="left", baseline="middle", dx=4, fontSize=11, fontWeight=600, lineBreak="\n").encode(
            x=alt.X("label_year:Q"), y=alt.Y("label_value:Q"), text="base_label:N", color=color,
            tooltip=["powertrain:N", alt.Tooltip("value:Q", format=".5g"), "units:N", "series:N"],
        )
        halo = labels.mark_text(align="left", baseline="middle", dx=4, fontSize=11, fontWeight=600, lineBreak="\n",
                                color="white", stroke="white", strokeWidth=3).encode(
            x=alt.X("label_year:Q"), y=alt.Y("label_value:Q"), text="base_label:N",
        )
        chart = alt.layer(lines, points, boundary, leaders, halo, text).resolve_legend(
            strokeDash="independent", shape="independent", color="shared",
        ).properties(width=width, height=290)
        if faceted:
            return chart.facet(row=alt.Row(facet + ":N", title=None,
                header=alt.Header(labelAngle=270, labelOrient="left", labelLimit=250, labelPadding=10))).resolve_scale(y="independent").properties(title=title)
        return chart.properties(title=title)

    return range_color, range_label, trajectory_chart


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Historical and future road efficiency

    Follow NRCan ratings through observed vehicle composition, cohort aggregation and ATB
    projections. This notebook and the vehicle-stock notebook read the **same
    latest successful `notebook_diagnostics.yaml` build**: captured ETL tables and
    their configuration. The accompanying SQLite is recorded by path and hash;
    these views read the captured tables, not SQLite.
    Display years, class ordering and chart dimensions read current diagnostic
    settings; source data and modelling rules remain tied to the recorded build.

    Run the prerequisite command below when inputs or the backend change, then reload.
    Opening this notebook performs no ETL. Source years remain visible before
    prospective model-period mapping. Comparisons with the catalogue-median
    estimator and the independent composition benchmark are labelled diagnostic-only.
    """)
    return


@app.cell
def _(mo):
    from validation.notebook_evidence import prepare_command

    refresh = mo.ui.run_button(label="Reload latest successful build")
    mo.vstack([mo.md(f"```powershell\n{prepare_command()}\n```"), refresh])
    return (refresh,)


@app.cell
def _(composition_view_settings, load_diagnostic, refresh):
    refresh.value
    diagnostic = load_diagnostic()
    composition_view = composition_view_settings(diagnostic.bundle)
    return composition_view, diagnostic


@app.cell(hide_code=True)
def _(diagnostic, mo):
    build = diagnostic.summary["build"]
    mo.vstack([
        mo.md(f"""
        Evidence build: **{build['build_id']}** ·
        base year **{diagnostic.summary['base_year']}** · region **{diagnostic.summary['region']}** ·
        MTO edition **{diagnostic.summary['population_year']}** · ATB **{diagnostic.summary['atb_trajectory']}**.
        Production NRCan basis: **documented MTO gasoline composition, iZEV PEV acquisitions,
        and distinct-trim family medians followed by cohort medians wherever empirical weights are unavailable**; gaps remain explicit.
        Historical period efficiencies and load factors use the
        raw cohort-stock weights from the fleet age distribution, before lifetime
        eligibility or redistribution. All displayed evidence belongs to this build.
        """),
        mo.accordion({"Recorded build and input identity": mo.json({"build": build, "inputs": diagnostic.summary["inputs"]})}),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## NRCan catalogue distributions

    These are eligible Ratings observations. Distinct trims first form a median
    within each canonical make/model family, model year, size class and powertrain/range.
    Boxes show the middle half of family medians, with min–max whiskers;
    black diamonds mark their descriptive mean and red crosses the cohort median. These markers
    do not establish empirical vehicle weights. The production comparison follows.

    The measurement selector keeps native consumption, engine displacement and
    electric motor power separate. Motor kW is not combustion-engine power.
    PHEV native consumption is CD; no CS curve is plotted. Missing engine/motor
    measurements remain missing. Columns include all observed size classes in the same
    smaller-to-larger order within each body family as the composition plots. Body families
    do not share one physical-volume scale. Rows show 2005–2025 in two-year
    steps, trimmed to each powertrain's observed span; internal gaps stay empty.
    Each powertrain uses one practical axis across its size classes, with four readable
    ticks and all selected model values retained. Axis limits vary between powertrains.
    `*` marks model years after the recorded baseline.
    """)
    return


@app.cell
def _(mo):
    distribution_group = mo.ui.dropdown(options=["Cars", "Light trucks"], value="Cars", label="Distribution vehicle group")
    distribution_metric = mo.ui.dropdown(options=["Consumption", "Engine size", "Motor power"], value="Consumption", label="NRCan measurement")
    mo.hstack([distribution_group, distribution_metric], justify="start")
    return distribution_group, distribution_metric


@app.cell
def distribution_plot(alt, chart_ui, composition_class_columns, composition_view, diagnostic, distribution_group, distribution_metric, distribution_metric_rows, gasoline_hev_classification_view, mo, pd, practical_axis, range_color, range_label, supported_display_years, table_ui):
    distribution = diagnostic.frames["rating_distribution"]
    distribution = distribution.loc[distribution.vehicle_group.eq(distribution_group.value)].copy()
    distribution, missing_measurements = distribution_metric_rows(distribution, distribution_metric.value)
    representative_years = composition_view["distribution_years"]
    ranked = composition_class_columns(diagnostic, distribution_group.value, composition_view)
    classes = ranked.nrcan_vehicle_class.tolist()
    year_labels = {year: str(year) + ("*" if year > diagnostic.summary["base_year"] else "")
                   for year in representative_years}
    family_panels = []

    def distribution_chart(plot, title, unit):
        plot = plot.copy()
        plot["range_label"] = plot.powertrain.map(range_label)
        plot["period"] = plot.year.map(year_labels)
        groups = ["rating_class", "range_label", "period"]
        if "rating_variant_id" in plot:
            plot = plot.drop_duplicates("rating_variant_id")
        identities = [*groups, "year", "atb_class", "family_make", "family_model", "make_model", "powertrain", "native_unit"]
        models = plot.groupby(identities, dropna=False, as_index=False).agg(
            native_consumption=("native_consumption", "median"), range_km=("range_km", "median"),
            distinct_trims=("native_consumption", "count"),
        )
        models["Make"], models["Model"] = models.family_make, models.family_model
        panel_years = supported_display_years(plot, representative_years)
        periods = [year_labels[year] for year in panel_years]
        # Preserve internal gaps; remove years beyond this powertrain's evidence.
        placeholders = pd.DataFrame([{"period": year_labels[year], "year": year, "rating_class": size}
                                     for year in panel_years for size in classes])
        display = pd.concat([models, placeholders], ignore_index=True)
        # Bounded inline fields keep marimo's chart transport independent of wide source tables.
        records = display.astype(object).where(display.notna(), None).to_dict("records")
        data = alt.Data(values=records)
        y_axis = alt.Y("period:N", title="Vehicle model year", sort=periods, scale=alt.Scale(domain=periods),
                       axis=alt.Axis(labelOverlap=False, labelPadding=6, grid=True))
        color = range_color("range_label", plot.range_label)
        color.legend = alt.Legend(orient="bottom", direction="horizontal", titleOrient="left", columns=4)
        domain, ticks = practical_axis(models.native_consumption)
        scale = alt.Scale(domain=domain, zero=False, nice=False)
        axis = alt.Axis(values=ticks, format=".3~f", labelOverlap=False, labelFontSize=10, grid=True, gridColor="#eeeeee")
        observed = alt.Chart(data).transform_filter("isValid(datum.native_consumption)")
        summary = observed.transform_aggregate(
            minimum="min(native_consumption)", lower="q1(native_consumption)", median="median(native_consumption)",
            upper="q3(native_consumption)", maximum="max(native_consumption)", model_mean="mean(native_consumption)",
            count="count()", distinct_trims="sum(distinct_trims)", groupby=groups,
        ).encode(y=y_axis, color=color)
        whiskers = summary.mark_rule(clip=True).encode(x=alt.X("minimum:Q", title=None, scale=scale, axis=axis), x2="maximum:Q")
        boxes = summary.mark_bar(size=12, opacity=.45, clip=True).encode(x=alt.X("lower:Q", scale=scale, axis=axis), x2="upper:Q")
        medians = summary.mark_tick(thickness=1.5, size=13, clip=True).encode(
            x=alt.X("median:Q", scale=scale, axis=axis), color=alt.value("#c62828"))
        dots = observed.mark_point(size=21, opacity=.4, filled=True, clip=True).encode(
            y=y_axis, x=alt.X("native_consumption:Q", scale=scale, axis=axis), color=color,
            tooltip=["year:O", "rating_class:N", "atb_class:N", "Make:N", "Model:N", "powertrain:N", "native_consumption:Q",
                     "native_unit:N", "range_km:Q", alt.Tooltip("distinct_trims:Q", title="Distinct trims")],
        )
        statistics = summary.transform_fold(["model_mean", "median"], as_=["statistic", "statistic_value"]).transform_calculate(
            statistic_label="datum.statistic === 'model_mean' ? 'Mean' : 'Median'",
        ).mark_point(size=75, filled=True, clip=True).encode(
            x=alt.X("statistic_value:Q", scale=scale, axis=axis),
            color=alt.value("#151515"),
            fill=alt.Fill("statistic_label:N", title="Family medians", sort=["Mean", "Median"],
                          scale=alt.Scale(domain=["Mean", "Median"], range=["#151515", "#c62828"])),
            shape=alt.Shape("statistic_label:N", title="Family medians", sort=["Mean", "Median"],
                            scale=alt.Scale(domain=["Mean", "Median"], range=["diamond", "cross"]),
                            legend=alt.Legend(orient="bottom", direction="horizontal", columns=2, titleOrient="left", symbolSize=90)),
            tooltip=["rating_class:N", "period:N", "statistic_label:N", alt.Tooltip("statistic_value:Q", format=".3f"),
                     alt.Tooltip("count:Q", title="Distinct families"), alt.Tooltip("distinct_trims:Q", title="Distinct trims")],
        )
        layers = [whiskers, boxes, medians, dots, statistics]
        return alt.layer(*layers).properties(
            width=(composition_view["grid_plot_width"] - 20 * (len(classes) - 1)) / len(classes), height=max(70, 24 * len(panel_years)),
        ).facet(column=alt.Column("rating_class:N", title=title + " · NRCan size class", sort=classes,
                                  header=alt.Header(labelExpr="split(datum.label, ': ')", labelLimit=130, labelFontSize=11,
                                                    titleFontSize=12, titlePadding=8, labelPadding=5)),
                spacing=20).configure_legend(offset=8, padding=0, titlePadding=8)

    for powertrain in sorted(distribution.powertrain.unique(), key=lambda value: (
            {"gasoline": 0, "diesel": 1, "hev": 2, "phev": 3, "cng": 4, "bev": 5}.get(value.split("_", 1)[0], 9), value)):
        family = powertrain.split("_", 1)[0]
        family_rows = distribution.loc[distribution.powertrain.eq(powertrain) & distribution.year.isin(representative_years)
            & distribution.rating_class.isin(classes)].copy()
        if family_rows.empty:
            family_panels.append(mo.md(f"**{range_label(powertrain)}:** no observations in these class/year selections; see all-year tables below."))
            continue
        if family == "phev" and distribution_metric.value == "Consumption":
            # CD stays independent of utility weighting; CS is retained in source tables.
            components = []
            for field, unit in [("combined_cd_consumption_Le_100_km", "Le/100 km · CD")]:
                component_rows = family_rows.assign(native_consumption=family_rows[field], native_unit=unit)
                components.append(component_rows)
            family_rows = pd.concat(components, ignore_index=True)
        for unit, panel in family_rows.groupby("native_unit", sort=False):
            family_panels.append(chart_ui(distribution_chart(panel, f"{range_label(powertrain)} · {unit}", unit)))
    cohort_summary = diagnostic.frames["rating_summary"]
    cohort_summary = cohort_summary.loc[cohort_summary.vehicle_group.eq(distribution_group.value)]
    consumption_columns = {"year": "Model year", "rating_class": "Size class", "powertrain": "Powertrain / range",
        "equal_distinct_model_mean": "Mean of family medians", "equal_distinct_model_median": "Median of family medians",
        "native_unit": "Native unit", "distinct_models": "Distinct families", "catalogue_rows": "Catalogue rows",
        "minimum": "Minimum rated value", "maximum": "Maximum rated value"}
    measurement_columns = {"Make": "Make", "Model": "Model", "native_consumption": distribution_metric.value,
        "native_unit": "Unit", "year": "Model year", "rating_class": "Size class", "powertrain": "Powertrain / range",
        "model_rows_in_cohort": "Rows for this make-model"}
    raw_catalogue = diagnostic.frames["rating_distribution"].loc[
        diagnostic.frames["rating_distribution"].vehicle_group.eq(distribution_group.value)]
    classification_view = gasoline_hev_classification_view(diagnostic.inputs["ratings"], classes)
    distribution_output = mo.vstack([
        mo.md(f"**{distribution_metric.value}:** {len(distribution):,} eligible catalogue rows with a reported positive value; {missing_measurements:,} unavailable measurements omitted."),
        *family_panels,
        mo.accordion({
            "All-year consumption summary": table_ui(cohort_summary[list(consumption_columns)].rename(columns=consumption_columns),
                "Consumption statistics · independent of the measurement dropdown"),
            "Rows behind the selected measurement": table_ui(distribution[list(measurement_columns)].rename(columns=measurement_columns),
                f"{distribution_metric.value} observations · units shown per row"),
            "Full cohort statistics audit": table_ui(cohort_summary, "All descriptive statistics · original field names"),
            "Full catalogue provenance and technical fields": table_ui(raw_catalogue, "Original consumption and engine fields · eligible catalogue rows"),
            "Gasoline / HEV classification evidence": mo.vstack([
                mo.md("These are captured **X/Z, L/100 km source rows** for the selected group's size classes, including uncertain rows excluded from the eligible catalogue graphs. **Unresolved gasoline/HEV** means propulsion is unverified; its provisional internal gasoline label is not an accepted classification. X/Z identify gasoline fuel modes, not hybrid status. Eligibility here does not establish empirical composition weight."),
                table_ui(classification_view, "Gasoline and HEV configuration evidence · included and excluded rows"),
            ]),
        }),
    ])
    distribution_output
    return


@app.cell(hide_code=True)
def _(composition_view, diagnostic, mo):
    supported = diagnostic.summary["composition_rules"]
    displayed_supported_years = [year for year in composition_view["years"]
        if supported["izev_model_year_min"] <= year <= supported["izev_model_year_max"]]
    temporal_composition = (f"Pre-{supported['izev_model_year_min']} PEV points use the "
        f"{supported['izev_model_year_min']}–{supported['izev_model_year_max']} composition proxy where target-year NRCan models match. "
        "The target consumption year and source composition window stay separate in the tables."
        if "izev_historical_composition_proxy" in diagnostic.summary["production_methods"] else
        "Historical cohorts without recorded empirical support retain catalogue medians.")
    mo.md(f"""
    ## Observed composition behind production cohorts

    Each row is a powertrain/range. Columns include **all observed size classes** within the selected vehicle
    group, arranged from smaller to larger within each body family. This is a display sequence,
    not a common measured volume scale across unlike bodies. The y-axis shows each vehicle model year
    from **{min(composition_view['years'])} to {max(composition_view['years'])}**. Grey dots are distinct catalogue
    family medians of distinct trims; grey ranges span those medians. Amber dots show the actual
    contributing empirical source descriptions, with size proportional to their contribution.
    Large hollow blue diamonds show the median of catalogue family medians; smaller filled red crosses show the
    **recorded production estimate**: count-weighted composition where supported,
    and the same hierarchical median in every other rated subclass. Both symbols remain visible
    when estimates coincide. Tooltips identify contributors,
    counts, geography, matching basis and remaining fallback. Consumption uses total
    MJ per vehicle-km; source-native values remain in tooltips and tables.

    Rows before or after a powertrain's actual catalogue support are omitted;
    missing cohorts inside that span remain empty. Classes without production support remain empty.
    The iZEV window is **{supported['izev_model_year_min']}–{supported['izev_model_year_max']}**;
    displayed supported years are **{', '.join(map(str, displayed_supported_years))}**.
    {temporal_composition} Post-window cohorts retain catalogue medians. Any class/range
    without compatible empirical support also retains its median.
    The y-axis years are individual target cohorts, not multi-year bins. `*` marks target years
    after the recorded baseline; these are outside historical calibration. Ranking uses the
    captured MTO stock snapshot pooled across vintages, with the same all-fuel outer-class
    weights as production; it is not a powertrain-specific or new-sales distribution.
    Display years, class order, optional column limit and chart widths read current `rules.yaml`; all consumption,
    contributors and MTO ranking weights remain from the recorded build.
    A common axis compares size classes within each powertrain; limits vary between
    powertrains to retain detail without unused space or clipping catalogue tails.
    """)
    return


@app.cell
def _(mo):
    vehicle_group = mo.ui.dropdown(options=["Cars", "Light trucks"], value="Cars", label="Vehicle group")
    vehicle_group
    return (vehicle_group,)


@app.cell(hide_code=True)
def _(diagnostic, mo):
    def recorded_policy():
        policy = diagnostic.summary["composition_rules"]
        minimum = policy["izev_pool_selection"]["minimum_observations"]
        source = diagnostic.frames["composition_observations"]
        acquisitions = source.loc[source.source_kind.eq("izev") & source.transaction_eligible.eq(True)
            & source.year.between(policy["izev_model_year_min"], policy["izev_model_year_max"])]
        coverage = []
        for family, group in acquisitions.groupby("observed_powertrain", sort=True):
            if family not in {"bev", "phev"}:
                continue
            total = group.source_count.sum()
            matched = group.loc[group.selected.eq(True), "source_count"].sum()
            coverage.append(f"{family.upper()} **{matched:,.0f}/{total:,.0f} ({matched / total:.2%})**")
        gasoline = diagnostic.frames["composition_coverage"]
        gasoline = gasoline.loc[gasoline.powertrain.eq("gasoline")]
        if "used_in_aggregation" in gasoline:
            gasoline = gasoline.loc[gasoline.used_in_aggregation.eq(True)]
        gasoline = gasoline.drop_duplicates(["year", "rating_class", "powertrain"])
        empirical_classes = int(gasoline.aggregation_method.eq("mto_identity_supported_weighted").sum())
        return mo.md(f"""
        **Current rules.** iZEV includes eligible new **purchases and leases**, model years
        **{policy['izev_model_year_min']}–{policy['izev_model_year_max']}**. The **{minimum:,}-record** threshold
        applies to the whole provincial BEV/PHEV pool **before size-class/range matching**;
        smaller pools use the whole national pool. Own-year cohorts use one source year;
        historical proxies use the full supported window. Clear PEV families use their median
        eligible-trim range for allocation; contradictory identities or classes remain unallocated.
        Documented MTO identities must cover **{policy['mto_selection']['minimum_resolved_count_share']:.0%}** of
        gasoline-compatible family counts before weighting that class; otherwise it uses its median.
        MTO does not observe fuel; compatible gasoline trims exclude hybrids and alternative fuels.
        Gasoline-capable FFVs may contribute their X/Z ratings; E85 and diesel modes do not.
        HEVs use NRCan model signals plus compatible FuelEconomy.gov configuration evidence,
        and always retain catalogue medians without MTO composition weights.
        This is a coverage policy, not a confidence score. Every rated subclass
        without empirical weights uses **distinct trim → family median → cohort median**, retaining original outer
        class weights. Only genuinely absent rated classes require renormalization.
        **{empirical_classes}/{len(gasoline)} gasoline class-year cohorts** receive MTO weights;
        the class tables show eligible/resolved and whole-mapped coverage separately.

        Supported families retrieve **all eligible target-year configurations** before medians.
        A class may predominate only within its configured body group, at
        **{policy['family_matching']['class_resolution']['minimum_predominant_share']:.0%}** of distinct configurations;
        catalogue frequency is not sales share and cannot overrule a reviewed class or body change.
        Residual consumption variation is screened by **(P90 − P10) / median**, with a limit of
        **{policy['family_matching']['consumption_dispersion']['maximum_relative_p90_p10_span']:.0%}**.
        This central-spread rule measures representative adequacy, not statistical confidence.

        Counts proxy activity weights under **equal annual distance within each CEUD class**:
        the common VKT factor cancels. Equal use is an assumption, not measured behaviour.
        **Allocated own-year incentive records in the supported window:** {'; '.join(coverage)}.
        These are eligible-record coverage shares, not market coverage or confidence estimates. Public records lack VINs;
        repeat requests and program demonstrator exceptions mean they are not unique-vehicle counts.
        """)
    policy_summary = recorded_policy()
    policy_summary
    return (policy_summary,)


@app.cell
def composition_plot(alt, chart_ui, composition_class_columns, composition_view, diagnostic, mo, pd, practical_axis, range_label, supported_display_years, vehicle_group):
    def composition_overview():
        years = composition_view["years"]
        ranked = composition_class_columns(diagnostic, vehicle_group.value, composition_view)
        report_year = int(ranked.report_year.max())
        classes = ranked.nrcan_vehicle_class.tolist()
        models = diagnostic.frames["composition_models"]
        models = models.loc[models.vehicle_group.eq(vehicle_group.value) & models.year.isin(years)
            & models.rating_class.isin(classes)].copy()
        summaries = diagnostic.frames["composition_comparison"]
        summaries = summaries.loc[summaries.vehicle_group.eq(vehicle_group.value) & summaries.year.isin(years)
            & summaries.rating_class.isin(classes)].copy()
        families = diagnostic.frames["composition_families"]
        families = families.loc[families.rating_class.isin(classes) & families.year.isin(years)].copy()
        contributions = {
            key: " | ".join(f"{row.family_label}: {row.family_weight:.1%} ({row.family_count:,.0f})"
                            for row in group.sort_values("family_weight", ascending=False).itertuples(index=False))
            for key, group in families.groupby(["year", "rating_class", "powertrain"])
        }
        kinds = ["Catalogue family median", "Selected empirical description", "Cohort fallback median", "Production estimate"]
        colors = ["#a1a7b0", "#d28d23", "#2475a8", "#b7303e"]
        powertrains = sorted(set(models.powertrain) | set(summaries.powertrain),
            key=lambda value: (["gasoline", "diesel", "hev", "cng", "phev", "bev"].index(value.split("_", 1)[0])
                               if value.split("_", 1)[0] in {"gasoline", "diesel", "hev", "cng", "phev", "bev"} else 9, value))
        ranking_labels = "; ".join(f"{row.nrcan_vehicle_class} **{row.aggregation_weight:.1%}**" if pd.notna(row.aggregation_weight)
            else f"{row.nrcan_vehicle_class} (historical catalogue; no current stock weight)"
            for row in ranked.itertuples(index=False))
        outputs = [mo.md(f"**{vehicle_group.value} · {report_year} MTO stock, all vintages:** {ranking_labels}. "
            f"Shown classes cover **{ranked.aggregation_weight.sum():.1%}** of mapped {vehicle_group.value.lower()} stock. "
            "Columns run from smaller to larger within each body family; unlike body types have no common volume scale.")]
        for powertrain in powertrains:
            catalogue = models.loc[models.powertrain.eq(powertrain)]
            selected = families.loc[families.powertrain.eq(powertrain)]
            estimates = summaries.loc[summaries.powertrain.eq(powertrain)]
            panel_years = supported_display_years(pd.concat([catalogue, selected, estimates], ignore_index=True), years)
            year_labels = [str(year) + ("*" if year > diagnostic.summary["base_year"] else "") for year in panel_years]
            records = []
            for item in catalogue.to_dict("records"):
                records.append({**item, "evidence_kind": kinds[0], "value": item["family_median_mj_per_vkm"],
                    "label": item["make_model"], "native_value": item["model_native_consumption"]})
            for item in selected.to_dict("records"):
                records.append({**item, "evidence_kind": kinds[1], "value": item["family_median_mj_per_vkm"],
                    "label": item["family_label"], "native_value": item["family_native_consumption"]})
            for item in estimates.to_dict("records"):
                common = {**item, "contributors": contributions.get((item["year"], item["rating_class"], powertrain), "")}
                if "median" in str(item.get("aggregation_method", "")):
                    cohort_models = catalogue.loc[catalogue.year.eq(item["year"]) & catalogue.rating_class.eq(item["rating_class"])].sort_values("family_median_mj_per_vkm")
                    count = len(cohort_models)
                    central = cohort_models.iloc[[(count - 1) // 2, count // 2]].make_model.unique() if count else []
                    common["contributors"] = f"Median over {count} distinct families; central: " + " | ".join(central)
                records.append({**common, "evidence_kind": "Catalogue range", "value": item.get("catalogue_model_min"),
                    "range_max": item.get("catalogue_model_max")})
                records.append({**common, "evidence_kind": kinds[2], "value": item["catalogue_mj_per_vkm"],
                    "label": "Median of family medians", "native_value": item["catalogue_native_consumption"]})
                records.append({**common, "evidence_kind": kinds[3], "value": item["production_mj_per_vkm"],
                    "label": item.get("aggregation_method", "Unsupported production class"),
                    "native_value": item["production_native_consumption"]})
            records.extend({"year": year, "rating_class": size, "evidence_kind": "Missing cohort"}
                           for year in panel_years for size in classes)
            plot = pd.DataFrame(records)
            values = pd.to_numeric(plot.value, errors="coerce").dropna()
            if values.empty:
                outputs.append(mo.md(f"**{range_label(powertrain)}:** no configured total-energy observations; native records remain in the catalogue tables."))
                continue
            domain, ticks = practical_axis(pd.concat([values, pd.to_numeric(plot.get("range_max", pd.Series(dtype=float)), errors="coerce")]))
            plot["model_year"] = plot.year.map(dict(zip(panel_years, year_labels, strict=True)))
            tooltips = ["evidence_kind:N", "label:N", "year:O", "rating_class:N", "powertrain:N",
                alt.Tooltip("value:Q", title="Total MJ/vkm", format=".4g"), "native_value:Q", "native_unit:N",
                alt.Tooltip("family_count:Q", title="Source record count", format=",.0f"),
                alt.Tooltip("family_weight:Q", title="Selected-count weight", format=".1%"), "geography:N", "pool_geography:N",
                "range_assignment:N", "catalogue_powertrain:N", "aggregation_method:N", "rating_statistic:N",
                alt.Tooltip("composition_basis:N", title="Temporal composition basis"),
                alt.Tooltip("source_model_year_min:O", title="Composition source window starts"),
                alt.Tooltip("source_model_year_max:O", title="Composition source window ends"),
                alt.Tooltip("source_model_years:N", title="Actual composition source model years"),
                alt.Tooltip("empirical_families:Q", title="Contributing source descriptions", format="d"),
                alt.Tooltip("selected_source_count:Q", title="Contributing source count", format=",.0f"),
                "coverage_status:N", "contributors:N"]
            tooltip_fields = [(field if isinstance(field, str) else field.shorthand).split(":")[0] for field in tooltips]
            available = [field for field, name in zip(tooltips, tooltip_fields, strict=True) if name in plot]
            # Audit identities stay in the detail tables. Only marks and tooltips
            # belong in the chart payload; absent values need no serialized key.
            chart_fields = list(dict.fromkeys(["model_year", "range_max", "family_weight", *tooltip_fields]))
            chart_data = plot[[field for field in chart_fields if field in plot]]
            data = alt.Data(values=[
                {field: value for field, value in record.items() if value is not None}
                for record in chart_data.astype(object).where(chart_data.notna(), None).to_dict("records")
            ])
            evidence_legend = alt.Legend(orient="bottom", direction="horizontal", columns=4, titleOrient="left", labelLimit=230,
                symbolOpacity=1, symbolStrokeWidth=1.7,
                symbolStrokeColor=alt.ExprRef(expr="datum.label === 'Catalogue family median' ? '#a1a7b0' : datum.label === 'Selected empirical description' ? '#d28d23' : datum.label === 'Cohort fallback median' ? '#2475a8' : '#b7303e'"),
                symbolFillColor=alt.ExprRef(expr="datum.label === 'Production estimate' ? '#b7303e' : 'transparent'"))
            color = alt.Color("evidence_kind:N", title="Evidence", sort=kinds,
                scale=alt.Scale(domain=kinds, range=colors),
                legend=evidence_legend)
            shape = alt.Shape("evidence_kind:N", title="Evidence", sort=kinds,
                scale=alt.Scale(domain=kinds, range=["circle", "circle", "diamond", "cross"]), legend=evidence_legend)
            base = alt.Chart(data).encode(
                x=alt.X("value:Q", title=None, scale=alt.Scale(domain=domain, nice=False),
                    axis=alt.Axis(values=ticks, format=".3~f", labelOverlap=False, labelFontSize=10, grid=True, gridColor="#eeeeee")),
                y=alt.Y("model_year:N", title="Vehicle model year", sort=year_labels, scale=alt.Scale(domain=year_labels),
                        axis=alt.Axis(labelOverlap=False, labelPadding=6, grid=True)))
            ranges = base.transform_filter(alt.datum.evidence_kind == "Catalogue range").mark_rule(color="#c3c7cd", strokeWidth=1.2).encode(x2="range_max:Q")
            dots = base.transform_filter(alt.datum.evidence_kind == kinds[0]).mark_point(size=19, filled=False, opacity=.45).encode(color=color, shape=shape, tooltip=available)
            family_dots = base.transform_filter(alt.datum.evidence_kind == kinds[1]).mark_point(filled=False, strokeWidth=1.7).encode(
                color=color, shape=shape, size=alt.Size("family_weight:Q", title="Count weight", scale=alt.Scale(domain=[0, 1], range=[25, 200]),
                                          legend=alt.Legend(orient="bottom", direction="horizontal", columns=3, titleOrient="left", format="%", values=[.1, .5, 1],
                                              symbolStrokeColor=colors[1], symbolFillColor="transparent")), tooltip=available)
            means = base.transform_filter(alt.FieldOneOfPredicate(field="evidence_kind", oneOf=kinds[2:])).mark_point(filled=False, strokeWidth=1.8).encode(
                color=color, shape=shape,
                fill=alt.condition(alt.datum.evidence_kind == kinds[3], alt.value(colors[3]), alt.value("transparent")),
                size=alt.condition(alt.datum.evidence_kind == kinds[2], alt.value(180), alt.value(60)), tooltip=available)
            chart = alt.layer(ranges, dots, family_dots, means).resolve_legend(color="shared", shape="shared", size="shared").properties(
                width=(composition_view["grid_plot_width"] - 20 * (len(classes) - 1)) / len(classes), height=max(70, 24 * len(panel_years))).facet(
                column=alt.Column("rating_class:N", title=range_label(powertrain) + " · total MJ/vkm · NRCan size class", sort=classes,
                    header=alt.Header(labelFontSize=11, labelExpr="split(datum.label, ': ')", labelLimit=130,
                                      titleFontSize=12, titlePadding=8, labelPadding=5)),
                spacing=20).configure_legend(offset=8, padding=0, titlePadding=8)
            outputs.append(chart_ui(chart))
        return mo.vstack(outputs)

    composition_overview()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Inspect the observations and matched Ratings rows

    PEV and gasoline MTO evidence have independent filters. The source tables retain
    native labels, vehicle and request years, recipient geography, counts, selection
    decisions and unresolved matches. The compatible-match tables retain NRCan component
    and row identities, original range buckets, assigned production buckets and row
    contributions. Family tables show individual and aggregate consumption together.
    Selecting **All size classes** also retains observations whose class is unresolved.
    """)
    return


@app.cell
def _(diagnostic, mo, range_label):
    def detail_filter(kind):
        coverage = diagnostic.frames["composition_coverage"]
        coverage = coverage.loc[coverage.powertrain.str.startswith(("bev", "phev")) if kind == "izev" else coverage.powertrain.eq("gasoline")]
        years = sorted(set(coverage.year))
        return mo.ui.dictionary({
            "year": mo.ui.dropdown(options={"All vintages": None, **{str(int(year)): int(year) for year in years}},
                value=str(diagnostic.summary["base_year"]), label="Vehicle model year"),
            "rating_class": mo.ui.dropdown(options={"All size classes": None, "Unresolved class": "", **{value: value for value in sorted(set(coverage.rating_class))}},
                value="All size classes", label="Size class"),
            "powertrain": mo.ui.dropdown(options={"All powertrains": None, **{range_label(value): value for value in sorted(set(coverage.powertrain))}},
                value="All powertrains", label="Powertrain / range"),
        })
    pev_filters = detail_filter("izev")
    mto_filters = detail_filter("mto")
    return mto_filters, pev_filters


@app.cell
def _(composition_detail_frames, diagnostic, mo, mto_filters, pd, pev_filters, table_ui):
    def detailed_evidence(kind, controls):
        frames = composition_detail_frames(diagnostic, kind, **controls.value)
        def view(frame, columns):
            return frame[[field for field in columns if field in frame]].rename(columns=columns)

        classes = frames["coverage"].sort_values(["year", "rating_class", "powertrain"], kind="stable")
        identities = {"year": "Target model year", "rating_class": "Size class", "powertrain": "Powertrain / range"}
        classes = classes.assign(catalogue_minus_production_mj_per_vkm=classes.catalogue_mj_per_vkm - classes.production_mj_per_vkm)
        methods = {"izev_incentive_weighted": "iZEV weighted mean", "izev_historical_composition_proxy": "iZEV proxy mean",
            "mto_identity_supported_weighted": "MTO weighted mean", "catalogue_distinct_model_median": "Median",
            "catalogue_fallback_median_insufficient_identity_support": "Median",
            "catalogue_fallback_median_no_empirical_support": "Median"}
        classes["method_label"] = classes.aggregation_method.map(methods).fillna(classes.aggregation_method)
        classes["fallback_reason"] = classes.aggregation_method.map({
            "catalogue_distinct_model_median": "Configured catalogue-median basis",
            "catalogue_fallback_median_insufficient_identity_support": "Documented identity coverage is below the gate",
            "catalogue_fallback_median_no_empirical_support": "No compatible empirical count weights"}).fillna("")
        if {"source_model_year_min", "source_model_year_max"}.issubset(classes):
            first = classes.source_model_year_min.map(lambda year: "" if pd.isna(year) else str(int(year))).astype("string")
            last = classes.source_model_year_max.map(lambda year: "" if pd.isna(year) else str(int(year))).astype("string")
            classes["source_window"] = (first + "–" + last).where(first.ne(last), first)
            classes["source_window"] = classes.source_window.replace("–", "")
        estimates = view(classes, {**identities, "catalogue_native_consumption": "Cohort fallback median",
            "production_native_consumption": "Production", "native_unit": "Native unit",
            "catalogue_mj_per_vkm": "Cohort fallback median (total MJ/vkm)", "production_mj_per_vkm": "Production (total MJ/vkm)",
            "catalogue_minus_production_mj_per_vkm": "Catalogue − production (MJ/vkm)",
            "catalogue_rows": "Catalogue rows", "catalogue_models": "Distinct families",
            "weighted_catalogue_models": "Weighted catalogue families", "empirical_families": "Source descriptions", "selected_source_count": "Contributing source count",
            "method_label": "Method"})
        if "composition_basis" in classes:
            classes["basis_label"] = classes.composition_basis.map({
                "historical_pooled_model_year_window": "Pooled source-model-year window", "observed_model_year": "Own model year",
                "observed_model_year_stock": "Model-year stock observations", "catalogue_only": "Catalogue only"}).fillna(classes.composition_basis)
        context_columns = {**identities, "method_label": "Method", "fallback_reason": "Fallback reason",
            "source_window": "Composition source MYs", "basis_label": "Temporal composition basis"}
        context_columns.update({"pool_geography": "Geographic pool", "pool_provinces": "Pool provinces"} if kind == "izev" else
                               {"composition_geography": "MTO evidence geography"})
        source_context = view(classes, context_columns)
        families = frames["families"].copy()
        if "family_weight" in families:
            families["weight_percent"] = families.family_weight * 100
        family_view = view(families, {"family_label": "Source description", "family_count": "Source count",
            "weight_percent": "Selected-count weight (%)", "family_native_consumption": "Native rate", "native_unit": "Native unit",
            "family_median_mj_per_vkm": "Total MJ/vkm", "variant_count": "Compatible variants",
            **identities, "source_model_years": "Composition source MYs", "geography": "Recipient provinces"})

        def observation_view(frame):
            rows = frame.copy()
            rows["source_label"] = rows.raw_make.fillna("") + " / " + rows.raw_model.fillna("")
            rows["family_label"] = rows.family_make.fillna("") + " / " + rows.family_model.fillna("")
            rows["outcome"] = rows.get("selection_status", rows.match_status).fillna("").str.replace("_", " ").str.capitalize()
            if "relative_contribution" in rows:
                rows["weight_percent"] = rows.relative_contribution * 100
            return view(rows, {"source_label": "Observed make / model", "source_count": "Source count", "outcome": "Selection outcome",
                "match_reason": "Matching reason",
                "family_label": "Canonical family", "weight_percent": "Class weight (%)", "year": "Target model year", "source_model_years": "Composition source MYs", "province": "Recipient province",
                "rating_class": "Assigned class", "resolved_powertrain": "Assigned powertrain / range", "duplicate_identity_merged": "Naming duplicate merged"})

        matches = frames["matches"].copy()
        matches["rated_model"] = matches.Make.fillna("") + " / " + matches.Model.fillna("")
        matches["weight_percent"] = matches.variant_weight * 100
        match_view = view(matches, {"rated_model": "NRCan make / model", "native_consumption": "Native rate", "native_unit": "Native unit",
            "consumption_mj_per_vkm": "Total MJ/vkm", "weight_percent": "Median contribution (%)", "allocated_count": "Count × median coefficient",
            "selected": "Contributes?", "year": "Target NRCan model year", "rating_class": "NRCan class", "powertrain": "Rated powertrain / range",
            "assigned_powertrain": "Assigned powertrain / range", "component": "NRCan component", "source_row": "Source row"})
        for ratio in ("identity_supported_resolved_share", "minimum_resolved_count_share", "identity_supported_all_mapped_share"):
            if ratio in classes:
                classes[ratio + "_percent"] = classes[ratio] * 100
        coverage_columns = identities.copy()
        coverage_columns.update({"identity_supported_count": "Identity-supported count",
            "resolved_gasoline_count": "Gasoline-compatible family count", "identity_supported_resolved_share_percent": "Supported / compatible (%)",
            "minimum_resolved_count_share_percent": "Required coverage (%)", "identity_coverage_met": "Coverage gate met?",
            "identity_supported_all_mapped_share_percent": "Supported / all-mapped (%)"} if kind == "mto" else
            {"provincial_observation_count": "Provincial pool count",
            "pool_observation_count": "Chosen pool count", "minimum_observations": "Minimum pool count",
            "pool_geography": "Chosen pool", "pool_provinces": "Pool provinces"})
        coverage_columns["coverage_status"] = "Coverage status"
        coverage = view(classes, coverage_columns)
        return mo.vstack([
            mo.hstack([controls["year"], controls["rating_class"], controls["powertrain"]], widths="equal"),
            mo.md("Native PHEV values are independent **CD Le/100 km**; **total MJ/vkm includes utility weighting**. The numeric **catalogue − production** difference is positive when production uses less energy. Sort it to inspect the most affected cohorts; target year, size and powertrain sort independently. Historical iZEV proxies use the supported source window with target-year NRCan rates; their temporal window is separate from the geographic pool. Method identifies the weights actually used; a median can remain despite an evaluated source pool."),
            table_ui(estimates, "Recorded class estimates: consumption, catalogue size and contributing evidence"),
            (mo.md("No empirical source description contributes under these filters; the aggregate and its catalogue-median basis remain in the class table above.")
             if families.empty else mo.vstack([table_ui(family_view, "Contributing source descriptions and their actual counts"),
                 mo.md("A description with **100%** weight supplies all selected empirical evidence for that class, not the whole market. Several descriptions may match one catalogue family; both counts are separate above. Historical proxies reuse window records for different target vintages; do not sum those counts across target years.")])),
            mo.accordion({
                "Source window, geography and fallback context": table_ui(source_context, "Target year versus composition window and geographic pool"),
                "Coverage: identity evidence and provincial / national pools": table_ui(coverage, "Coverage denominators and policy gates"),
                "Observed names, counts and selection outcomes": table_ui(observation_view(frames["observations"]), "Source observations"),
                "Compatible NRCan rows and their contributions": table_ui(match_view, "Compatible ratings · zero median coefficient means no direct contribution"),
                "Unresolved observations, including unknown class / powertrain": table_ui(observation_view(frames["unresolved"]), "Unresolved source context; class/powertrain filters do not assign these observations"),
                "Full provenance and technical audit": mo.ui.tabs({
                    "Class audit": table_ui(frames["coverage"], "All class fields · original names"),
                    "Source descriptions": table_ui(frames["families"], "Description allocations and observation identities"),
                    "Observations": table_ui(frames["observations"], "Raw labels, identity routes, source-row IDs and decisions"),
                    "NRCan matches": table_ui(frames["matches"], "All source-row fields and median coefficients"),
                    "Unresolved": table_ui(frames["unresolved"], "Full unresolved observations"),
                }, lazy=False),
            }),
        ])
    mo.ui.tabs({"PEV acquisitions": detailed_evidence("izev", pev_filters),
                "Gasoline MTO observations": detailed_evidence("mto", mto_filters)}, lazy=False)
    return


@app.cell(hide_code=True)
def ambiguity_review(composition_review_cases, composition_review_summary, diagnostic, mo, table_ui):
    review = composition_review_cases(diagnostic)
    summary = composition_review_summary(review)
    labels = {"source_kind": "Source", "raw_make": "Observed make", "raw_model": "Observed description",
        "observed_powertrain": "Reported powertrain",
        "source_count_exposure": "Source-count exposure", "province": "Source province",
        "target_model_years": "Affected target MYs", "reviewed_identity": "Supported reviewed identity", "review_reason": "Reason",
        "recommended_handling": "Handling"}
    def reviewed_table(category, title):
        rows = summary.loc[summary.review_kind.eq(category)] if not summary.empty else summary
        columns = [field for field in labels if field in rows
                   and (rows[field].notna() & rows[field].astype(str).str.strip().ne("")).any()]
        return table_ui(rows[columns].rename(columns=labels), title)
    mo.vstack([
        mo.md("""
        ### Matching review · ranked by source-count exposure

        Each row groups one source description and source province. **Source-count exposure**
        counts each own-year source observation once, including all legitimate native records;
        repeated target-year and geographic applications do not increase it. Source counts are
        acquisitions for iZEV and fit-active stock for MTO, so their totals describe different populations.
        Every positive unmapped MTO key remains visible with an unknown assigned class,
        including wholly unresolved keys and missing intervals for mixed-scope keys.
        Reviewed non-light-duty vehicles and configured year-scope exclusions appear separately.

        Historical proxies are separate: their pooled-window count appears once per description,
        across all affected target years. **Do not add proxy exposure to own-year exposure.**
        Missing proxy-era ratings and gasoline-only exclusions do not imply that supported-year
        iZEV observations fail. All applications and candidate details remain in the audit below.
        """),
        reviewed_table("Actionable matching issue", "Matching issues requiring identity, year or configuration review"),
        mo.accordion({
            "Expected estimator and program exclusions": reviewed_table("Expected exclusion", "Expected exclusions"),
            "Historical proxy limitations": reviewed_table("Historical proxy limitation", "Reused source-window evidence; separate from own-year exposure"),
            "Years before the captured NRCan catalogue": reviewed_table("Historical catalogue limitation", "Historical catalogue coverage"),
            "Supported families using representative medians": reviewed_table("Retained family-median representative", "Supported distinct-trim family medians"),
            "Source exposure, target applications and alternatives": table_ui(summary, "Exposure reconciliation and candidate context"),
            "All target-year applications and provenance": table_ui(review, "Captured applications; source counts are not additive across proxy years or recipient regions"),
        }),
    ])
    return


@app.cell(hide_code=True)
def _(diagnostic, mo, range_label):
    bev_anchor = range_label(diagnostic.summary["anchor_powertrains"]["bev"])
    phev_anchor = range_label(diagnostic.summary["anchor_powertrains"]["phev"])
    mo.md(f"""
    ## OMEGA range composition and projection anchors

    **{bev_anchor}** and **{phev_anchor}** anchor each family's future trajectory to
    the cohort-weighted NRCan consumption of its latest historical source period.
    ATB supplies subsequent relative improvements and consumption ratios between
    range variants. Historical cohorts retain recorded NRCan consumption; range groups
    use matched family representatives or a hierarchical catalogue median; contradictory identities remain unallocated.
    Existing fleet consumption combines independently aggregated range-period values
    using the recorded OMEGA shares; ranges need not have the same observed years.

    In consumption units, the projection is
    `C_range(p) = C_NRCan-anchor × C_ATB-range(p) / C_ATB-anchor({diagnostic.summary['atb_reference_year']})`.
    Source year {diagnostic.summary['atb_reference_year']} holds the NRCan cohort anchor; subsequent points apply ATB
    changes from that year. Thus ATB's overlapping absolute levels do not replace
    observed historical NRCan consumption.
    A range variant can change level at this transition: its historical NRCan
    population and ATB's relative range comparison describe different vehicle mixes.

    The bars compare cars and light trucks within each powertrain family; colors
    identify range classes. The table records each projection anchor's source years,
    cohort coverage and ATB reference year.
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
    anchor_columns = {"vehicle_group": "Vehicle group", "atb_class": "ATB class", "powertrain": "Target powertrain / range",
        "anchor_powertrain": "NRCan anchor", "anchor_consumption_mj_per_vkm": "Cohort anchor (MJ/vkm)",
        "parameter_reference_consumption_mj_per_vkm": "Projection reference (MJ/vkm)", "atb_reference_year": "ATB reference year",
        "source_years": "Historical source years", "cohort_stock_coverage": "Included cohort share"}
    mo.vstack([chart_ui(shares_chart),
        table_ui(diagnostic.frames["projection_anchors"][list(anchor_columns)].rename(columns=anchor_columns), "Projection levels and source cohorts"),
        mo.accordion({"Projection anchor provenance and technical fields": table_ui(diagnostic.frames["projection_anchors"], "Full recorded anchor audit")})])
    return


@app.cell(hide_code=True)
def _(diagnostic, mo):
    bridge_periods = diagnostic.bundle.scenario.periods
    anchor_vintage = bridge_periods.latest_observed_vintage
    anchor_window = bridge_periods.historical_years(anchor_vintage)
    mo.md(f"""
    ## Existing-to-future bridge

    **Distinct trims → family medians → empirical count weighting / cohort median fallback → energy → production periods**.
    Lower consumption means greater efficiency. Graph 1 shows native catalogue trajectories using the median of distinct-trim family medians
    over 2000 through the latest captured model year. Graph 2 is a heatmap of
    **catalogue median − production (total MJ/vkm)** by size class and model year,
    with separate panels for each range. Blue/positive means production consumes less;
    red/negative means more. Grey cells lack a comparison; neutral cells have zero change.
    This isolates within-class composition weighting or median fallback, before outer-class
    and period aggregation. For PHEVs, both sides use the same utility-weighted total energy;
    native CD Le/100 km is not subtracted from total MJ/vkm.
    Graphs 3 and 4 span 2010–2040. Graph 4 shows the current production bridge.
    **Final prepared period CSV** markers identify the published production values.

    Historical PEV cohorts retain recorded source years; contradictory records
    remain unresolved and unsupported subclasses use their hierarchical catalogue median. Existing PEV
    consumption combines supported range periods using OMEGA shares.
    Partial periods expose coverage; wholly empty periods require the configured
    recorded analogue. Missing years are not interpolated. The forward anchor is
    source year {diagnostic.summary['atb_reference_year']}, followed by ATB changes.

    Detailed ATB classes remain diagnostic recalculations: they inherit parent
    loads, cohort counts and OMEGA shares. Light trucks use passenger service units.
    Graph 3 labels the NRCan trajectory endpoint contributing to each observed anchor,
    alongside the recorded multi-year, stock-weighted cohort anchor value. These are
    different quantities; graph 4 uses the cohort anchor and ATB range ratios.
    The grey line marks the same base year in graphs 1, 3 and 4; their year domains differ.
    Tooltips retain source years and model vintages.

    **Source year and model vintage differ.** In this {bridge_periods.period_mode} build, historical
    vintage **{anchor_vintage}** combines the **{anchor_window[0]}–{anchor_window[-1]}** observed cohorts; graph 4 places that
    point at its last included source year, **{anchor_window[-1]}**. The **{bridge_periods.model[0]}** point is the ATB
    projection reference using the historical cohort anchor, not a new observation
    or an additional historical vintage. For BEVs other than BEV300, that reference
    also applies the ATB range ratio to the shared BEV300 anchor. It does not apply
    a {anchor_window[-1]}→{bridge_periods.model[0]} ATB time improvement. The first future parameter has vintage **{bridge_periods.model[0]}**
    and uses the period source year **{bridge_periods.projection_year(bridge_periods.model[0], legacy_at_end=False)}**.
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
                                  value="Energy consumption (MJ/vkm)", label="Source-period metric")
    bridge_metric
    return bridge_metric, class_controls, family_controls


@app.cell
def _(diagnostic):
    bridge_diagnostic = diagnostic
    return (bridge_diagnostic,)


@app.cell
def bridge_plot(COHORT, VIEW_MODES, alt, bridge_diagnostic, bridge_metric, chart_ui, class_controls, composition_heatmap_data, composition_view, family_controls, mo, pd, range_label, table_ui, trajectory_chart):
    def bridge_view(group):
        chosen_class, family = class_controls.value[group], family_controls.value[group]
        start, end = composition_view["bridge_years"]
        native_years = [composition_view["native_year_start"], max(bridge_diagnostic.summary["available_rating_years"])]
        rows = bridge_diagnostic.frames["bridge"]
        rows = rows.loc[rows.vehicle_group.eq(group) & rows.atb_class.eq(chosen_class)
                        & rows.powertrain_family.eq(family) & rows.year.between(start, end)].copy()
        def native_rows(key):
            source = bridge_diagnostic.frames[key]
            selected = source.loc[source.vehicle_group.eq(group) & source.powertrain_family.eq(family)
                                  & source.year.between(*native_years) & source.series.str.startswith("NRCan")].copy()
            selected = selected.dropna(subset=["value"])
            if chosen_class != COHORT:
                selected = selected.loc[selected.atb_class.eq(chosen_class)
                                        | selected.series.eq("NRCan CEUD observed on-road consumption")]
            return selected

        raw = native_rows("raw_sources")
        native_domains = {unit: [float(frame.value.min()), float(frame.value.max())]
                          for unit, frame in raw.groupby("units")}

        def curves(frame, title, *, native=False):
            if frame.empty:
                return []
            frame = frame.copy()
            frame["axis_basis"] = frame.units
            if family == "phev" and "stage" in frame and frame.stage.eq("Harmonised before indexing").all():
                frame["axis_basis"] = "MJ per vehicle-km · operating bases differ"
            return [chart_ui(trajectory_chart(panel, title + (f" · {unit}" if frame.axis_basis.nunique() > 1 else ""),
                                              native=native, facet="axis_basis", width=composition_view["line_plot_width"],
                                              year_domain=native_years if native else [start, end],
                                              y_domain=native_domains.get(unit) if native else None))
                    for unit, panel in frame.groupby("axis_basis", sort=False)]

        comparison = composition_heatmap_data(bridge_diagnostic, group, family,
            atb_class=chosen_class, years=tuple(native_years))
        heatmaps = []
        if comparison.empty or comparison.delta_mj_per_vkm.notna().sum() == 0:
            heatmaps.append(mo.md("**2. Class consumption changes:** no comparable NRCan total-energy class estimates under this selection. "
                "Medium/heavy CEUD histories have no separate NRCan catalogue composition estimator; missing MJ conversions remain unavailable."))
        else:
            available = set(comparison.rating_class)
            ordered = [size for size in composition_view["class_order"].get(group, []) if size in available]
            ordered += sorted(available - set(ordered))
            maximum = float(comparison.delta_mj_per_vkm.abs().max())
            heatmaps.append(mo.md("**2. Class consumption changes · catalogue median − production.** "
                "Positive/blue means lower production energy use; negative/red means higher. Grey means missing; neutral means zero. "
                + ("All comparable cells are zero under the selected aggregation policy." if maximum == 0 else
                   "The same symmetric MJ/vkm colour scale applies to every range panel.")))
            years = list(range(native_years[0], native_years[1] + 1))
            fields = ["year", "rating_class", "powertrain", "catalogue_mj_per_vkm", "production_mj_per_vkm",
                "delta_mj_per_vkm", "delta_fraction", "aggregation_method", "empirical_families", "selected_source_count"]
            for powertrain, panel in comparison.groupby("powertrain", sort=True):
                grid = pd.DataFrame([{"year": year, "rating_class": size, "powertrain": powertrain}
                    for year in years for size in ordered])
                plot = grid.merge(panel[[field for field in fields if field in panel]], on=["year", "rating_class", "powertrain"],
                    how="left", validate="one_to_one")
                data = alt.Data(values=plot.astype(object).where(plot.notna(), None).to_dict("records"))
                tooltips = ["year:O", "rating_class:N", "powertrain:N",
                    alt.Tooltip("catalogue_mj_per_vkm:Q", title="Cohort fallback median MJ/vkm", format=".4f"),
                    alt.Tooltip("production_mj_per_vkm:Q", title="Production MJ/vkm", format=".4f"),
                    alt.Tooltip("delta_mj_per_vkm:Q", title="Catalogue − production MJ/vkm", format="+.4f"),
                    alt.Tooltip("delta_fraction:Q", title="Relative reduction", format="+.1%"), "aggregation_method:N",
                    alt.Tooltip("empirical_families:Q", title="Source descriptions"),
                    alt.Tooltip("selected_source_count:Q", title="Contributing source count")]
                base = alt.Chart(data).encode(
                    x=alt.X("year:O", title="Vehicle model year", sort=years, scale=alt.Scale(domain=years),
                        axis=alt.Axis(labelAngle=0, labelOverlap=False, labelFontSize=10)),
                    y=alt.Y("rating_class:N", title=None, sort=ordered, scale=alt.Scale(domain=ordered),
                        axis=alt.Axis(labelLimit=235)), tooltip=tooltips)
                background = base.mark_rect(color="#dfe3e8", stroke="white", strokeWidth=.5)
                effects = base.transform_filter("isValid(datum.delta_mj_per_vkm)").mark_rect(stroke="white", strokeWidth=.5)
                if maximum:
                    effects = effects.encode(color=alt.Color("delta_mj_per_vkm:Q", title="Catalogue − production (MJ/vkm)",
                        scale=alt.Scale(domain=[-maximum, 0, maximum], range=["#b7303e", "#ffffff", "#2475a8"], interpolate="rgb"),
                        legend=alt.Legend(orient="right", direction="vertical", gradientLength=150,
                                          title=["Composition adjusted", "change [MJ/vkm]"], titleLimit=200)))
                else:
                    effects = effects.encode(color=alt.value("#ffffff"))
                heatmaps.append(chart_ui((background + effects).properties(width=composition_view["heatmap_plot_width"],
                    height=max(55, 25 * len(ordered)), title="2. Class consumption changes · " + range_label(powertrain))))
        composition_columns = {"year": "Model year", "rating_class": "Size class", "powertrain": "Powertrain / range",
            "catalogue_native_consumption": "Cohort fallback median", "production_native_consumption": "Production", "native_unit": "Native unit",
            "catalogue_mj_per_vkm": "Cohort fallback median (total MJ/vkm)", "production_mj_per_vkm": "Production (total MJ/vkm)",
            "delta_mj_per_vkm": "Catalogue − production (MJ/vkm)",
            "empirical_families": "Source descriptions", "aggregation_method": "Method"}
        composition_details = ([mo.accordion({"Production and hierarchical fallback values by class": mo.vstack([
            table_ui(comparison[[field for field in composition_columns if field in comparison]].rename(columns=composition_columns), "Current cohort fallback and production estimate"),
            mo.accordion({"Full class-composition audit": table_ui(comparison, "All captured fields · original names")})])})]
                               if not comparison.empty else [])

        note = ("Medium/heavy gasoline and diesel use their own observed CEUD fleet consumption. Emerging powertrains use diesel-relative ATB ratios; these are proxies, not native CEUD observations. "
                f"All future trajectories anchor to the latest existing cohort at source year {bridge_diagnostic.summary['atb_reference_year']}, then apply ATB relative improvements. "
                "Gasoline uses diesel improvement rates. Absolute ATB levels do not replace the fleet anchor. CEUD observations describe the parent fleet, not individual vocations."
                if group in {"Medium trucks", "Heavy trucks"} else
                "Each NRCan PHEV row uses total MJ/vkm = [UF × (3.6 × CD kWh + fuel HHV × CD litres) + (1 − UF) × fuel HHV × CS litres] / 100, "
                "using NRCan rates per 100 km and the ATB range-bucket UF. CD remains an independent curve; CS remains in tables. "
                "Description/cohort aggregation follows. This is a cross-source travel-share assumption. PHEV35 anchors future projections only."
                if family == "phev" else
                ("Diesel/HEV class estimates use medians of distinct-trim family medians. " if family in {"diesel", "hev"} else
                 "Production class estimates use the selected observed family counts within supported classes. ")
                + "Unsupported rated subclasses use the same hierarchical family/cohort median; the harmonised aggregate follows recorded class weights. "
                + ("Clear BEV families use median eligible-trim range; contradictory matches remain unresolved. Hierarchical medians fill missing empirical support. BEV300 anchors future projections; ATB supplies later relative changes and range ratios."
                   if family == "bev" else f"The latest observed NRCan cohort anchors the {bridge_diagnostic.summary['atb_reference_year']} reference point; future periods apply ATB changes from that point."))
        period_title = "4. Production periods"
        if chosen_class != COHORT:
            period_title = "4. Diagnostic class recalculation"
        bridge_columns = {"powertrain": "Powertrain / range", "year": "Source year", "value": "Value", "units": "Unit",
            "series": "Evidence / method", "vintage": "Model vintage", "stage": "Stage", "sample_kind": "Point type",
            "source_years": "Contributing source years", "cohort_stock_coverage": "Included cohort share"}
        return mo.vstack([
            mo.hstack([class_controls[group], family_controls[group]], widths="equal"), mo.md(note),
            *curves(raw, "1. Native catalogue trajectories", native=True),
            *heatmaps, *composition_details,
            *curves(rows.loc[rows.stage.eq("Harmonised before indexing")], "3. Harmonised source energy"),
            *curves(rows.loc[rows.stage.eq("Indexed bridge") & rows.metric.eq(bridge_metric.value)], period_title),
            mo.accordion({"Bridge values and source timing": table_ui(rows[[field for field in bridge_columns if field in rows]].rename(columns=bridge_columns),
                              f"{group}: displayed values and their units"),
                          "Full bridge provenance and technical fields": table_ui(rows, f"{group}: complete recorded bridge evidence")}),
        ])

    mo.ui.tabs({group: bridge_view(group) for group in VIEW_MODES}, lazy=False)
    return


@app.cell(hide_code=True)
def _(diagnostic, mo):
    mo.accordion({"Recorded assumptions and limitations": mo.md("\n\n".join(diagnostic.summary["limitations"]))})
    return


@app.cell(hide_code=True)
def _(diagnostic, mo):
    mo.md(f"""
    ## Independent composition benchmarks

    These comparisons ask **how closely our available composition evidence resembles
    independent published counts**. They can reveal acquisition selection and stock-coverage
    differences; they do not validate fuel consumption, fine NRCan classes or range allocation.
    "Independent" refers to the StatCan reference counts; our broad-class mappings still
    reuse the catalogue/crosswalk evidence.

    **Validation only · {diagnostic.summary['region']}.** These counts do not set production weights.
    Compare Cars/light trucks in annual **registered stock** with available mapped MTO
    FIT_ACTIVE survivors. MTO coverage refers to its available cohort table, not all MTO records.
    Compare **BEV/PHEV acquisition shares** over **{min(diagnostic.summary['benchmark_calendar_years'])}–{max(diagnostic.summary['benchmark_calendar_years'])}** with iZEV purchases plus leases,
    using request calendar year alongside StatCan registration calendar year. Incentive eligibility,
    incomplete class matching and timing differences can explain disagreement. iZEV acquisition
    records are not identifiable unique vehicles; repeat incentives and demonstrator exceptions are possible.
    The partial 2019 launch and 2025 closure years stay in the audits, outside the acquisition comparison.

    MTO does not record fuel: its catalogue-compatible subset is shown only in the coverage tables,
    with its own denominator and report year. It cannot validate a full-fleet fuel distribution.
    StatCan's full stock powertrain mix is a separate reference. Missing or suppressed cells remain
    missing; request year, vehicle model year, native classes and source populations remain in the tables.
    """)
    return


@app.cell
def _(mo):
    benchmark_scope = mo.ui.dropdown({
        "Stock · Cars / light trucks": "stock_vehicle_group_share",
        "New PEVs · BEV / PHEV": "new_pev_powertrain_share",
        "New PEVs · Cars / light trucks": "new_pev_vehicle_group_share",
        "StatCan stock · powertrain reference": "statcan_stock_powertrain_reference",
    }, value="Stock · Cars / light trucks", label="Comparison and denominator")
    benchmark_scope
    return (benchmark_scope,)


@app.cell
def composition_benchmark_plot(alt, benchmark_scope, chart_ui, diagnostic, mo, table_ui):
    def benchmark_panels():
        scope = benchmark_scope.value
        region = diagnostic.summary["region"]
        frame = diagnostic.frames["composition_benchmark"]
        frame = frame.loc[frame.region.eq(region) & frame.comparison_scope.eq(scope)].copy()
        if frame.empty:
            return mo.md("No captured observations support this comparison in the recorded region.")
        stock_reference = scope == "statcan_stock_powertrain_reference"
        category = "vehicle_group" if scope in {"stock_vehicle_group_share", "new_pev_vehicle_group_share"} else "powertrain"
        condition = "powertrain" if scope == "new_pev_vehicle_group_share" else "vehicle_group"
        paired = scope != "statcan_stock_powertrain_reference"
        sources = {"statcan_stock", "mto_stock"} if scope == "stock_vehicle_group_share" else {"statcan_new", "izev"}
        keys = ["year"] if scope == "stock_vehicle_group_share" else ["year", condition]
        valid_sources = frame.source.where(frame.share.notna(), "")
        frame["comparison_overlap"] = valid_sources.groupby([frame[key] for key in keys], dropna=False).transform(
            lambda values: "Both sources present" if sources.issubset(set(values)) else "Source only or incomplete") if paired else "StatCan reference only"
        source_labels = {"statcan_stock": "StatCan registered stock", "mto_stock": "Mapped MTO FIT_ACTIVE stock",
                         "statcan_new": "StatCan new registrations", "izev": "iZEV acquisition requests"}
        frame["source_label"] = frame.source.map(source_labels)
        frame["powertrain_label"] = frame.powertrain.str.upper()
        frame["panel"] = frame[category] if category == "vehicle_group" else frame.powertrain_label
        frame["condition"] = frame[condition] if condition == "vehicle_group" else frame.powertrain_label
        year_min, year_max = int(frame.year.min()), int(frame.year.max())
        plot_fields = ["year", "share", "count", "denominator", "panel", "condition", "source_label", "powertrain_label",
                       "vehicle_group", "denominator_basis", "population", "time_basis", "status", "source_geography", "comparison_overlap"]
        data = alt.Data(values=frame[plot_fields].astype(object).where(frame[plot_fields].notna(), None).to_dict("records"))
        color = (alt.Color("powertrain_label:N", title="StatCan powertrain", scale=alt.Scale(scheme="tableau10")) if stock_reference else
                 alt.Color("source_label:N", title="Counted population", scale=alt.Scale(
                     domain=list(source_labels.values()), range=["#256c91", "#c65b35", "#256c91", "#c65b35"]),
                     legend=alt.Legend(labelLimit=300, values=[label for label in source_labels.values() if label in set(frame.source_label)])))
        base = alt.Chart(data).encode(
            x=alt.X("year:Q", title="Source calendar / report year", scale=alt.Scale(domain=[year_min - .3, year_max + .3], nice=False),
                    axis=alt.Axis(format="d", tickMinStep=1)),
            y=alt.Y("share:Q", title="Share of stated denominator", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format="%")),
            color=color, order="year:Q",
            detail="powertrain_label:N" if stock_reference else "source_label:N",
            tooltip=["source_label:N", "vehicle_group:N", "powertrain_label:N", "year:O", alt.Tooltip("count:Q", format=",.0f"),
                     alt.Tooltip("denominator:Q", format=",.0f"), alt.Tooltip("share:Q", format=".2%"), "denominator_basis:N",
                     "population:N", "time_basis:N", "source_geography:N", "status:N", "comparison_overlap:N"],
        )
        points = base.mark_point(filled=stock_reference, size=70, strokeWidth=1.7)
        if paired:
            point_sources = [label for label in source_labels.values() if label in set(frame.source_label)]
            points = points.encode(shape=alt.Shape("source_label:N", scale=alt.Scale(domain=point_sources, range=["square", "cross"]), legend=None))
        chart = (base.mark_line(strokeWidth=2) + points).properties(width=390, height=210)
        if scope in {"new_pev_powertrain_share", "new_pev_vehicle_group_share"}:
            chart = chart.facet(column=alt.Column("panel:N", title=None), row=alt.Row("condition:N", title=None))
        else:
            chart = chart.facet(column=alt.Column("vehicle_group:N", title=None))
        overlap = frame.loc[frame.comparison_overlap.eq("Both sources present"), "year"].nunique()
        context = (f"**{overlap} calendar/report years have both counted populations** within at least one displayed conditional group. "
                   "Unpaired years stay visible and labelled in the table. " if paired else "**StatCan reference only.** ")
        context += " ".join(frame.comparability_note.dropna().unique())
        frame["share_percent"] = frame.share * 100
        comparison_columns = {"year": "Calendar / report year", "vehicle_group": "Vehicle group", "powertrain_label": "Powertrain",
            "source_label": "Counted population", "count": "Count", "share_percent": "Share (%)",
            "denominator": "Denominator count", "comparison_overlap": "Counterpart availability"}
        return mo.vstack([mo.md(context), chart_ui(chart),
            table_ui(frame[list(comparison_columns)].rename(columns=comparison_columns), "Counts, shares and their exact denominators"),
            mo.accordion({"Comparison definitions, source labels and technical audit": table_ui(frame, "Full comparison cells · original field names")})])
    benchmark_output = benchmark_panels()
    benchmark_output
    return (benchmark_output,)


@app.cell
def _(diagnostic, mo, table_ui):
    def benchmark_audits():
        region = diagnostic.summary["region"]
        frames = {key: diagnostic.frames[key].loc[diagnostic.frames[key].region.eq(region)].copy()
                  for key in ("composition_benchmark_coverage", "composition_benchmark_timing", "composition_benchmark_observations")}
        subset = diagnostic.frames["composition_benchmark"]
        subset = subset.loc[subset.region.eq(region) & subset.comparison_scope.eq("mto_catalogue_subset_powertrain")]
        source_labels = {"izev": "iZEV requests", "mto_stock": "Mapped MTO cohorts", "mto_catalogue_subset": "MTO compatible subset",
            "statcan_new": "StatCan new registrations", "statcan_stock": "StatCan registered stock"}

        def view(frame, columns):
            return frame[[field for field in columns if field in frame]].rename(columns=columns)

        coverage = frames["composition_benchmark_coverage"].copy()
        coverage["population"] = coverage.source.map(source_labels).fillna(coverage.source)
        coverage["mapped_percent"] = coverage.coverage_share * 100
        coverage["outcome"] = coverage.status.str.replace("_", " ").str.capitalize()
        coverage_view = view(coverage, {"population": "Counted population", "year": "Calendar / report year",
            "vehicle_group": "Vehicle group", "powertrain": "Powertrain", "total_count": "Available source count",
            "mapped_count": "Mapped count", "mapped_percent": "Mapped / available (%)", "outcome": "Coverage result"})
        subset = subset.assign(share_percent=subset.share * 100)
        subset_view = view(subset, {"year": "MTO report year", "vehicle_group": "Vehicle group", "powertrain": "Compatible powertrain",
            "count": "Subset count", "share_percent": "Share of compatible subset (%)", "denominator": "Compatible subset total"})
        timing_view = view(frames["composition_benchmark_timing"], {"request_year": "Request calendar year", "model_year": "Vehicle model year",
            "year_lag": "Request year minus model year", "powertrain": "Powertrain", "count": "Request records",
            "in_comparison_calendar_years": "Inside acquisition comparison window?"})
        native = frames["composition_benchmark_observations"].copy()
        native["population_label"] = native.source.map(source_labels).fillna(native.source)
        native["source_label"] = native.source_make.fillna("") + " / " + native.source_model.fillna("")
        native["outcome"] = native.status.str.replace("_", " ").str.capitalize()
        native_view = view(native, {"population_label": "Counted population", "year": "Calendar / report year", "count": "Count",
            "vehicle_group": "Vehicle group", "powertrain": "Powertrain", "source_label": "Source make / model",
            "model_year": "Vehicle model year", "source_geography": "Native geography", "outcome": "Mapping / coverage result"})
        return mo.ui.tabs({
            "Coverage and gaps": mo.vstack([mo.md("Mapped percentages use each source's **available-count denominator**. MTO's available cohort table is not all MTO stock; exact population definitions remain in the technical audit."),
                table_ui(coverage_view, "Available counts, mapped counts and coverage gaps"),
                mo.md("**MTO fuel is unobserved.** The following shares condition on catalogue-compatible families only; their year may not overlap StatCan stock."),
                table_ui(subset_view, "MTO catalogue-compatible subset · coverage only")]),
            "Request year / model year": table_ui(timing_view, "Two separate clocks · request records, not unique vehicles"),
            "Native observations": table_ui(native_view, "Native source populations and mapped broad classes"),
            "Full technical audit": mo.ui.tabs({
                "Coverage definitions": table_ui(frames["composition_benchmark_coverage"], "All denominators, notes and statuses"),
                "MTO subset": table_ui(subset, "Full conditional subset definition"),
                "Timing": table_ui(frames["composition_benchmark_timing"], "Original request/model-year fields"),
                "Native source fields": table_ui(frames["composition_benchmark_observations"], "All native classes, fuels and source labels"),
            }, lazy=False),
        })
    benchmark_audit_output = benchmark_audits()
    benchmark_audit_output
    return (benchmark_audit_output,)


if __name__ == "__main__":
    app.run()
