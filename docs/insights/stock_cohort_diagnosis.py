"""Read-only comparison of sales cohorts, fixed allocation and curve-mode inputs."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from math import ceil, floor
    import altair as alt
    from validation.notebook_figures import identified_chart
    import marimo as mo
    import numpy as np
    import pandas as pd
    from validation.stock_cohort_diagnostic import comparison_lineage, load_stock_cohort_diagnosis

    CLASS_LABELS = {
        "cars": "Cars", "passenger_light_trucks": "Passenger light trucks",
        "freight_light_trucks": "Freight light trucks", "medium_trucks": "Medium trucks",
        "heavy_trucks": "Heavy trucks",
    }
    STOCK_LABELS = {**CLASS_LABELS, "passenger_light_trucks": "Light trucks", "freight_light_trucks": "Light trucks"}
    STOCK_ORDER = ["Cars", "Light trucks", "Medium trucks", "Heavy trucks"]
    PROFILE_LABELS = {"moves": "MOVES", "nhtsa_nems": "NHTSA + NEMS"}

    def stock_grid(chart):
        return chart.properties(width=370, height=190).facet(
            facet=alt.Facet("Class:N", sort=STOCK_ORDER, title=None), columns=2,
        ).resolve_scale(x="shared", y="independent")

    def clipped_diagonals(x_left, x_right, y_bottom, y_top, spacing=7.):
        """Return 45-degree pixel-space segments clipped to an axis-aligned box."""
        if x_right <= x_left or y_top <= y_bottom:
            return []
        lower = floor((y_bottom - x_right) / spacing)
        upper = ceil((y_top - x_left) / spacing)
        result = []
        for offset_index in range(lower, upper + 1):
            intercept = offset_index * spacing
            start = max(x_left, y_bottom - intercept)
            stop = min(x_right, y_top - intercept)
            if stop > start + 1e-10:
                result.append((start, stop, start + intercept, stop + intercept))
        return result


    def build_cohort_hatching(contributions, builds, panel_height=190, panel_width=370,
                             value_column="surviving_k_vehicles", period_step=5):
        """Stack unchanged capacities and clip diagonal rules to legacy bar bounds.

        Quantitative rectangle coordinates use years and thousand vehicles. Invisible
        extents fix the per-class scale; with nice=False at 370x190, hatch spacing is
        seven pixels and slopes are 45 degrees. This is display geometry only.
        """
        if len(builds) != 2 or len(set(builds)) != 2 or panel_height <= 0 or panel_width <= 0 or period_step <= 0:
            raise ValueError("Two distinct builds and positive panel dimensions are required")
        required = ["Class", "evidence", "vintage", "annual_position", "cohort_position",
                    "vintage_year", "interval", value_column]
        rectangles = contributions[required].copy()
        if rectangles.empty or rectangles[required].isna().any().any():
            raise ValueError("Cohort contributions must be nonempty with complete labels")
        numeric = rectangles[["vintage", "annual_position", "vintage_year", value_column]]
        if not np.isfinite(numeric).all().all() or rectangles[value_column].lt(0).any():
            raise ValueError("Cohort coordinates/capacities must be finite and nonnegative")
        if not set(rectangles.evidence) <= set(builds):
            raise ValueError("Unknown build in cohort contributions")
        rectangles["build_order"] = rectangles.evidence.map({value: index for index, value in enumerate(builds)})
        rectangles = rectangles.sort_values(["Class", "vintage", "build_order", "annual_position", "vintage_year"]).reset_index(drop=True)
        keys = ["Class", "evidence", "vintage"]
        rectangles["y1"] = rectangles.groupby(keys, sort=False)[value_column].cumsum()
        rectangles["y0"] = rectangles.y1 - rectangles[value_column]
        rectangles["bar_total"] = rectangles.groupby(keys, sort=False)[value_column].transform("sum")
        # The grouped pair occupies 60% of the configured vintage spacing.
        centers = rectangles.vintage + rectangles.build_order.map({0: -.16 * period_step, 1: .16 * period_step})
        rectangles["x0"], rectangles["x1"] = centers - .14 * period_step, centers + .14 * period_step
        minimum = float(rectangles.vintage.min()) - .5 * period_step
        maximum = float(rectangles.vintage.max()) + .5 * period_step
        span = maximum - minimum
        totals = rectangles.groupby(keys, as_index=False).bar_total.first()
        maxima = totals.groupby("Class").bar_total.max()
        extents = pd.DataFrame({"Class": maxima.index, "y1": np.where(maxima.gt(0), maxima * 1.05, 1.)})
        extents["y0"], extents["x0"], extents["x1"] = 0., minimum, maximum
        ymax_by_class = extents.set_index("Class").y1.to_dict()
        stripes = []
        legacy = rectangles.loc[rectangles.evidence.eq(builds[1])].drop_duplicates(keys)
        for bar in legacy.itertuples(index=False):
            ymax = ymax_by_class[bar.Class]
            left = (bar.x0 - minimum) * panel_width / span
            right = (bar.x1 - minimum) * panel_width / span
            top = bar.bar_total * panel_height / ymax
            for stripe_id, (x0, x1, y0, y1) in enumerate(clipped_diagonals(left, right, 0., top)):
                stripes.append({
                    "Class": bar.Class, "evidence": bar.evidence, "vintage": bar.vintage,
                    "stripe_id": stripe_id,
                    "x0": minimum + x0 * span / panel_width,
                    "x1": minimum + x1 * span / panel_width,
                    "y0": y0 * ymax / panel_height, "y1": y1 * ymax / panel_height,
                    "bar_x0": bar.x0, "bar_x1": bar.x1, "bar_y1": bar.bar_total,
                })
        stripe_columns = ["Class", "evidence", "vintage", "stripe_id", "x0", "x1", "y0", "y1", "bar_x0", "bar_x1", "bar_y1"]
        stripes = pd.DataFrame(stripes, columns=stripe_columns)
        rectangles.attrs.update({"vintage_ticks": sorted(rectangles.vintage.unique().tolist()),
                                 "x_domain": [minimum, maximum], "y_domains": ymax_by_class,
                                 "stripe_spacing_px": 7., "stripe_slope_pixels": 1.})
        return rectangles, stripes, extents

    def cohort_style_legend(positions, builds, bases):
        """A vertical key using the same colours and real solid/striped swatches."""
        year_rows = pd.DataFrame({"cohort_position": positions, "y0": [24 + 21 * i for i in range(len(positions))]})
        year_rows["y1"] = year_rows.y0 + 13
        year_rows["y"] = year_rows.y0 + 6.5
        build_title_y = 50 + 21 * len(positions)
        build_rows = pd.DataFrame({"label": [builds[0] + " (solid)\n" + bases[0], builds[1] + " (striped)\n" + bases[1]],
                                   "y0": [build_title_y + 16., build_title_y + 60.],
                                   "y1": [build_title_y + 32., build_title_y + 76.],
                                   "y": [build_title_y + 24., build_title_y + 68.]})
        stripe_rows = pd.DataFrame(clipped_diagonals(0., 16., build_title_y + 60., build_title_y + 76., spacing=5.),
                                   columns=["x0", "x1", "y0", "y1"])
        stripe_rows[["y0", "y1"]] = (2 * build_title_y + 136.) - stripe_rows[["y0", "y1"]]
        x_scale = alt.Scale(domain=[0, 240], nice=False, zero=False)
        y_scale = alt.Scale(domain=[build_title_y + 95, 0], nice=False, zero=False)
        years = alt.Chart(year_rows).mark_rect().encode(
            x=alt.X(datum=0, scale=x_scale, axis=None), x2=alt.X2(datum=16),
            y=alt.Y("y0:Q", scale=y_scale, axis=None), y2="y1:Q",
            color=alt.Color("cohort_position:N", scale=alt.Scale(domain=positions, scheme="blues"), legend=None))
        year_text = alt.Chart(year_rows).mark_text(align="left", baseline="middle", fontSize=11).encode(
            x=alt.X(datum=24, scale=x_scale, axis=None), y=alt.Y("y:Q", scale=y_scale, axis=None), text="cohort_position:N")
        swatches = alt.Chart(build_rows).mark_rect(fill="#bdbdbd", stroke="#777", strokeWidth=.4).encode(
            x=alt.X(datum=0, scale=x_scale, axis=None), x2=alt.X2(datum=16),
            y=alt.Y("y0:Q", scale=y_scale, axis=None), y2="y1:Q")
        stripes = alt.Chart(stripe_rows).mark_rule(stroke="#222", strokeWidth=.7).encode(
            x=alt.X("x0:Q", scale=x_scale, axis=None), x2="x1:Q",
            y=alt.Y("y0:Q", scale=y_scale, axis=None), y2="y1:Q")
        build_text = alt.Chart(build_rows).mark_text(align="left", baseline="middle", fontSize=11, lineBreak="\n", lineHeight=14).encode(
            x=alt.X(datum=24, scale=x_scale, axis=None), y=alt.Y("y:Q", scale=y_scale, axis=None), text="label:N")
        titles = alt.Chart(pd.DataFrame({"label": [f"{len(positions)}-year cohorts", "Method"], "y": [10, build_title_y]})).mark_text(
            align="left", baseline="middle", fontWeight="bold", fontSize=12).encode(
            x=alt.X(datum=0, scale=x_scale, axis=None), y=alt.Y("y:Q", scale=y_scale, axis=None), text="label:N")
        return alt.layer(years, year_text, swatches, stripes, build_text, titles).properties(width=240, height=build_title_y + 95)

    def display_table(frame, **options):
        formats = {column: "{:,.4f}" for column in frame.select_dtypes(include="floating").columns}
        formats.update(options.pop("format_mapping", {}))
        return mo.ui.table(frame, format_mapping=formats, **options)

    def display_chart(chart, figure_id, title):
        return mo.ui.altair_chart(identified_chart(chart, figure_id, title), chart_selection=False, legend_selection=False)

    return (CLASS_LABELS, PROFILE_LABELS, STOCK_LABELS, alt, build_cohort_hatching,
            cohort_style_legend, comparison_lineage, display_chart, display_table,
            load_stock_cohort_diagnosis, mo, pd, stock_grid)


@app.cell
def _(load_stock_cohort_diagnosis):
    diagnosis = load_stock_cohort_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    reference_year = diagnosis["metadata"]["reference_year"]
    mo.md(f"""
    # Stock cohorts and existing capacity

    **Reference year: {reference_year}**, from `periods.base_year` in
    `{diagnosis['metadata']['scenario_path']}`. This notebook recomputes the production owners'
    calculations from registered inputs. It does not build a database, publish parameters or download sources.
    CEUD **sales, stock and energy** beyond the latest observed year are held constant under the selected
    policy; held values are estimates, not additional observations. There is no extrapolated diagnostic scenario.

    With curves **off**, existing capacity is surviving stock assigned to fixed-lifetime vintages.
    With curves **on**, it is original-installation-equivalent acquisitions; Temoa applies absolute annual
    survival and averages effective capacity over each model period. Supplying already retired stock to that
    curve would retire it twice. Physical survivors remain the basis for utilization and efficiency weights.
    The approved **constant-retirement** treatment continues each source's last observed retirement rate
    to the configured **{100 * diagnosis['metadata']['tail_survival_threshold']:g}% survival threshold**, followed by zero. The same treatment supports stock calibration
    and future fleet retirement for both survival sources.
    Stock closure here precedes the configured
    `{diagnosis['metadata']['scenario']['existing_capacity']['cleanup_tolerance']:g}` thousand-vehicle cleanup
    threshold; the inserted database can differ by removed tiny rows.
    [Inspected Temoa capacity rule](https://github.com/TemoaProject/temoa/blob/cb2f51503ddb4dc6607c73635098d34c5e2d1283/temoa/components/capacity.py#L471)
    and [annual survival/period averaging](https://github.com/TemoaProject/temoa/blob/cb2f51503ddb4dc6607c73635098d34c5e2d1283/temoa/components/technology.py#L116).

    The deprecated MTO method is retained here as an explicit comparison. Its Ontario age distribution is a
    proxy outside Ontario. Cars and trucks migrate to sales cohorts; motorcycles and buses keep their separate
    stock methods. Every chart states whether its numbers are acquisitions or surviving stock.
    """)
    return (reference_year,)


@app.cell
def _(diagnosis, mo):
    region = mo.ui.dropdown(diagnosis["metadata"]["regions"], value=diagnosis["metadata"]["regions"][0], label="Region")
    profile = mo.ui.dropdown(list(diagnosis["annual"].profile.unique()),
        value=diagnosis["metadata"]["selected_profile"], label="Survival source")
    mo.hstack([region, profile], justify="start")
    return profile, region


@app.cell(hide_code=True)
def _(CLASS_LABELS, STOCK_LABELS, alt, diagnosis, display_chart, display_table, mo, pd,
      profile, reference_year, region, stock_grid):
    off_annual = diagnosis["annual"].loc[
        diagnosis["annual"].region.eq(region.value) & diagnosis["annual"].profile.eq(profile.value)
    ].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    off_values = off_annual.groupby(["Class", "vintage_year"], as_index=False)[[
        "sales_k_vehicles", "surviving_k_vehicles", "calibrated_surviving_k_vehicles",
        "fixed_allocated_k_vehicles", "deprecated_mto_k_vehicles",
    ]].sum()
    off_values["retired_k_vehicles"] = off_values.sales_k_vehicles - off_values.surviving_k_vehicles
    off_portions = off_values.drop(columns="sales_k_vehicles").melt(
        id_vars=["Class", "vintage_year"], var_name="Component", value_name="Vehicles")
    off_portions["Kind"] = off_portions.Component.isin(["surviving_k_vehicles", "retired_k_vehicles"]).map({True: "Sales", False: "Model"})
    off_portions["Component"] = off_portions.Component.map({
        "surviving_k_vehicles": "Raw surviving sales", "retired_k_vehicles": "Cumulative retirements",
        "calibrated_surviving_k_vehicles": "CEUD-stock scaling", "fixed_allocated_k_vehicles": "Fixed-lifetime allocation",
        "deprecated_mto_k_vehicles": "Deprecated MTO age allocation",
    })
    off_legend = ["Raw surviving sales", "Cumulative retirements", "CEUD-stock scaling",
                  "Fixed-lifetime allocation", "Deprecated MTO age allocation"]
    off_colors = ["#4477aa", "#d9d9d9", "#cc8844", "#222222", "#b2182b"]
    off_bars = alt.Chart().transform_filter(alt.datum.Kind == "Sales").mark_bar().encode(
        x=alt.X("vintage_year:O", title="Annual cohort year", axis=alt.Axis(labelAngle=0, labelExpr="toNumber(datum.label) % 5 == 0 ? datum.label : ''")),
        y=alt.Y("Vehicles:Q", title="Thousand vehicles", stack="zero"),
        color=alt.Color("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=off_legend, range=off_colors),
                        legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        order=alt.Order("Component:N", sort="descending"),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    off_lines = alt.Chart().transform_filter(alt.datum.Kind == "Model").mark_line(strokeWidth=2.34).encode(
        x="vintage_year:O", y=alt.Y("Vehicles:Q", stack=None), order="vintage_year:O",
        color=alt.Color("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=off_legend, range=off_colors),
                        legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        strokeDash=alt.StrokeDash("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=off_legend, range=[[1, 0], [1, 0], [1, 0], [5, 3], [2, 2]]),
                                  legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    retirement_chart = stock_grid(alt.layer(off_bars, off_lines, data=off_portions))
    redistribution = diagnosis["vintage"].loc[
        diagnosis["vintage"].region.eq(region.value) & diagnosis["vintage"].profile.eq(profile.value)
    ].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    mo.vstack([mo.md(f"""
    ## 1. Curves off: survivors and fixed-lifetime redistribution

    Blue + grey reconstruct raw sales. Blue is sales × cumulative annual survival at {reference_year};
    orange applies one CEUD stock multiplier per region/class. The black dashed line redistributes
    ineligible surviving stock within each technology, preserving its total. Red shows the deprecated MTO
    age allocation after its fixed-lifetime redistribution, with the same fuel-share rules. **Bars are unchanged; line widths
    are 30% larger.** Light-truck plots combine passenger and freight; tables retain those services separately.
    Cohort year is the sales year for CEUD and the age-implied year for MTO.

    Eligibility requires both the annual cohort and its representative model vintage to remain alive at
    the first model period. Passing that test does not guarantee a full period of effective capacity:
    Temoa prorates fixed lifetimes that end within a period. The table shows signed redistribution by vintage;
    cumulative redistribution must return to zero within each class.
    """), display_chart(retirement_chart, 'cohort-1', 'Fixed lifetimes redistribute calibrated surviving cohorts'), display_table(redistribution[[
        "Class", "vintage", "calibrated_surviving_k_vehicles", "fixed_allocated_k_vehicles",
        "redistributed_k_vehicles", "cumulative_redistribution_k_vehicles",
    ]].rename(columns={"vintage": "Model vintage", "calibrated_surviving_k_vehicles": "Before allocation (k)",
        "fixed_allocated_k_vehicles": "After allocation (k)", "redistributed_k_vehicles": "Redistributed (k)",
        "cumulative_redistribution_k_vehicles": "Cumulative redistribution (k)"}), selection=None, page_size=15)])
    return (retirement_chart,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, STOCK_LABELS, alt, diagnosis, display_chart, display_table, mo,
      profile, reference_year, region, stock_grid):
    on_annual = diagnosis["annual"].loc[
        diagnosis["annual"].region.eq(region.value) & diagnosis["annual"].profile.eq(profile.value)
    ].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    on_values = on_annual.groupby(["Class", "vintage_year"], as_index=False)[[
        "sales_k_vehicles", "calibrated_acquisitions_k_vehicles", "deprecated_mto_physical_k_vehicles",
    ]].sum().melt(id_vars=["Class", "vintage_year"], var_name="Stage", value_name="Vehicles")
    on_values["Stage"] = on_values.Stage.replace({
        "sales_k_vehicles": "Raw acquisitions", "calibrated_acquisitions_k_vehicles": "CEUD-stock scaling",
        "deprecated_mto_physical_k_vehicles": "Deprecated MTO surviving stock"})
    on_legend = ["Raw acquisitions", "CEUD-stock scaling", "Deprecated MTO surviving stock"]
    on_encoding = dict(
        x=alt.X("vintage_year:O", title="Annual cohort year", axis=alt.Axis(labelAngle=0,
            labelExpr="toNumber(datum.label) % 5 == 0 ? datum.label : ''")),
        y=alt.Y("Vehicles:Q", title="Acquisitions / stock (thousand)", scale=alt.Scale(zero=True)),
        color=alt.Color("Stage:N", title="Acquisitions and stock", scale=alt.Scale(domain=on_legend, range=["#4477aa", "#cc8844", "#b2182b"]),
            legend=alt.Legend(orient="right", labelLimit=280, symbolType="stroke", symbolSize=350)),
        tooltip=["Class:N", "vintage_year:O", "Stage:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    on_bars = alt.Chart().transform_filter(alt.datum.Stage == "Raw acquisitions").mark_bar().encode(**on_encoding)
    on_line = alt.Chart().transform_filter(alt.datum.Stage != "Raw acquisitions").mark_line(strokeWidth=2.34).encode(
        **on_encoding, order="vintage_year:O",
        strokeDash=alt.StrokeDash("Stage:N", title="Acquisitions and stock",
            scale=alt.Scale(domain=on_legend, range=[[1, 0], [1, 0], [2, 2]]),
            legend=alt.Legend(orient="right", labelLimit=280, symbolType="stroke", symbolSize=350)))
    acquisitions_chart = stock_grid(alt.layer(on_bars, on_line, data=on_values))
    acquisitions = diagnosis["vintage"].loc[
        diagnosis["vintage"].region.eq(region.value) & diagnosis["vintage"].profile.eq(profile.value)
    ].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    mo.vstack([mo.md(f"""
    ## 2. Curves on: acquisitions before and after stock calibration

    Blue and orange are **original-installation equivalents**, not survivors. One region/class multiplier preserves
    annual sales shape while its surviving stock closes to CEUD at the reference year {reference_year}.
    No fixed-lifetime redistribution is applied to acquisitions. Aggregated model vintages use
    acquisition-weighted combined vintage curves, preserving the contributions of their actual sales years.

    The red comparison is **MTO surviving stock before fixed-lifetime redistribution**, using its historical
    coverage and renormalization. It has a different quantity basis from acquisitions.
    Under the approved curve-mode capacity contract, physical
    survivors are used separately for utilization and efficiency weights. The table keeps raw and calibrated
    acquisitions adjacent, including cumulative totals; future survival is not a new CEUD calibration.
    Matching point-year physical stock does not force Temoa's first model-period average to equal CEUD stock.
    """), display_chart(acquisitions_chart, 'cohort-2', 'Curve mode separates acquisitions from physical survivors'), display_table(acquisitions[[
        "Class", "vintage", "sales_k_vehicles", "calibrated_acquisitions_k_vehicles",
        "cumulative_sales_k_vehicles", "cumulative_calibrated_acquisitions_k_vehicles", "deprecated_mto_physical_k_vehicles",
    ]].rename(columns={"vintage": "Model vintage", "sales_k_vehicles": "Raw acquisitions (k)",
        "calibrated_acquisitions_k_vehicles": "Calibrated acquisitions (k)",
        "deprecated_mto_physical_k_vehicles": "MTO surviving stock (k)",
        "cumulative_sales_k_vehicles": "Cumulative raw (k)",
        "cumulative_calibrated_acquisitions_k_vehicles": "Cumulative calibrated (k)"}), selection=None, page_size=15)])
    return (acquisitions_chart,)


@app.cell(hide_code=True)
def _(mo):
    lineage_basis = mo.ui.dropdown(
        ["Acquisitions versus stock", "Physical surviving stock"],
        value="Acquisitions versus stock", label="Paired cohort comparison",
    )
    mo.vstack([mo.md("""
    ## 3. Sales cohorts versus the deprecated MTO method

    Solid sales bars and striped MTO bars retain the annual-position colours from the transportation
    comparison. **Default quantities differ:** curve-mode sales parameters are calibrated acquisitions;
    deprecated MTO quantities are age-allocated survivors **before fixed-lifetime redistribution**. Select physical surviving stock for a like-quantity
    comparison. Each annual segment sums exactly to its model-vintage total; partial bins remain visible.
    The acquisition view uses the production installation basis. In the configured prospective bins
    this equals calibrated acquisitions; right-labelled synthetic bins would use their own normalized basis.
    Both methods are shown before fixed-lifetime redistribution here. The deprecated MTO distribution still
    uses its original historical-window restriction and renormalization; it is not unfiltered source stock.
    """), lineage_basis])
    return (lineage_basis,)


@app.cell(hide_code=True)
def _(STOCK_LABELS, alt, build_cohort_hatching, cohort_style_legend, comparison_lineage,
      diagnosis, display_chart, display_table, lineage_basis, mo, pd, profile, region, stock_grid):
    paired_annual = diagnosis["annual"].loc[
        diagnosis["annual"].region.eq(region.value) & diagnosis["annual"].profile.eq(profile.value)]
    paired_lineage = comparison_lineage(paired_annual,
        physical_survivors=lineage_basis.value == "Physical surviving stock")
    paired_lineage["Class"] = paired_lineage.road_class.map(STOCK_LABELS)
    paired_lineage["evidence"] = paired_lineage.method.replace({"Deprecated MTO age allocation": "Deprecated MTO"})
    paired_step = diagnosis["metadata"]["scenario"]["periods"]["step"]
    paired_lineage["annual_position"] = ((paired_lineage.vintage_year - paired_lineage.vintage - 1) % paired_step + 1).astype(int)
    paired_lineage["cohort_position"] = "Year " + paired_lineage.annual_position.astype(str)
    paired_lineage["interval"] = paired_lineage.vintage.map(lambda year: f"({year}, {year + paired_step}]")
    paired_lineage.loc[paired_lineage.vintage_year.le(paired_lineage.vintage), "interval"] += "; oldest proxy includes lower endpoint"
    paired_contributions = paired_lineage.groupby(["Class", "evidence", "vintage", "vintage_year",
        "annual_position", "cohort_position", "interval"], as_index=False).vehicles_k.sum()
    paired_builds = ["Sales cohorts", "Deprecated MTO"]
    paired_bases = [("Physical surviving stock" if lineage_basis.value == "Physical surviving stock"
                     else "Installed-equivalent acquisitions"), "Physical surviving stock"]
    paired_positions = [f"Year {position}" for position in range(1, paired_step + 1)]
    paired_rectangles, paired_hatches, paired_extents = build_cohort_hatching(
        paired_contributions, paired_builds, value_column="vehicles_k", period_step=paired_step)
    paired_geometry = pd.concat([paired_rectangles.assign(Kind="Stock"), paired_hatches.assign(Kind="Hatch"),
                                paired_extents.assign(Kind="Extent")], ignore_index=True)
    paired_x = alt.X("x0:Q", title="Model vintage", scale=alt.Scale(domain=paired_rectangles.attrs["x_domain"], nice=False, zero=False),
        axis=alt.Axis(values=paired_rectangles.attrs["vintage_ticks"], format="d", labelAngle=0))
    paired_y = alt.Y("y0:Q", title=("Surviving stock (thousand)" if lineage_basis.value == "Physical surviving stock"
        else "Acquisitions / stock (thousand)"), scale=alt.Scale(nice=False, zero=False))
    paired_bars = alt.Chart().transform_filter(alt.datum.Kind == "Stock").mark_rect().encode(
        x=paired_x, x2="x1:Q", y=paired_y, y2="y1:Q",
        color=alt.Color("cohort_position:N", scale=alt.Scale(domain=paired_positions, scheme="blues"), legend=None),
        tooltip=["Class:N", "evidence:N", "vintage:O", "vintage_year:O", "interval:N", alt.Tooltip("vehicles_k:Q", format=",.4f")])
    paired_stripes = alt.Chart().transform_filter(alt.datum.Kind == "Hatch").mark_rule(stroke="#111", strokeWidth=.7, opacity=.42).encode(
        x=paired_x, x2="x1:Q", y=paired_y, y2="y1:Q")
    paired_bounds = alt.Chart().transform_filter(alt.datum.Kind == "Extent").mark_rect(opacity=0).encode(
        x=paired_x, x2="x1:Q", y=paired_y, y2="y1:Q")
    paired_chart = alt.hconcat(stock_grid(alt.layer(paired_bounds, paired_bars, paired_stripes, data=paired_geometry)),
        cohort_style_legend(paired_positions, paired_builds, paired_bases), spacing=18).resolve_scale(color="independent", x="independent", y="independent")
    mo.vstack([display_chart(paired_chart, 'cohort-3', 'Annual contributions within each representative vintage'), mo.accordion({"Annual contributions and explicit quantity basis":
        display_table(paired_lineage[["road_class", "evidence", "basis", "vintage", "vintage_year", "vehicles_k"]],
                      selection=None, page_size=10)})])
    return (paired_chart,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, PROFILE_LABELS, alt, diagnosis, display_chart, display_table, mo, profile, region):
    calibration = diagnosis["calibration"].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS),
                                                Source=lambda frame: frame.profile.map(PROFILE_LABELS))
    calibration_tiles = alt.Chart(calibration).mark_rect().encode(
        x=alt.X("Class:N", sort=list(CLASS_LABELS.values()), title=None, axis=alt.Axis(labelAngle=-20, labelLimit=180)),
        y=alt.Y("region:N", title="Region"),
        color=alt.Color("multiplier:Q", title="Stock multiplier", scale=alt.Scale(scheme="redblue", domainMid=1, reverse=True)),
        tooltip=["Source:N", "region:N", "Class:N", alt.Tooltip("multiplier:Q", format=".4f"),
            alt.Tooltip("raw_survivors_k_vehicles:Q", format=",.3f"), alt.Tooltip("ceud_stock_k_vehicles:Q", format=",.3f")],
    )
    calibration_labels = alt.Chart(calibration).mark_text(fontSize=11).encode(
        x=alt.X("Class:N", sort=list(CLASS_LABELS.values())), y="region:N", text=alt.Text("multiplier:Q", format=".2f"),
        color=alt.condition((alt.datum.multiplier > 1.8) | (alt.datum.multiplier < .75), alt.value("white"), alt.value("black")))
    calibration_chart = alt.layer(calibration_tiles, calibration_labels).properties(width=470, height=270).facet(
        column=alt.Column("Source:N", title=None))
    selected_composition = diagnosis["composition"].loc[
        diagnosis["composition"].profile.eq(profile.value) & diagnosis["composition"].region.eq(region.value)].copy()
    selected_composition["Class"] = selected_composition.road_class.map(CLASS_LABELS)
    selected_composition["share_percent"] = 100 * selected_composition.physical_stock_k_vehicles / selected_composition.groupby(
        ["road_class", "method"]).physical_stock_k_vehicles.transform("sum")
    largest_factor = calibration.loc[calibration.multiplier.idxmax()]
    mo.vstack([mo.md(f"""
    ## 4. CEUD-stock scaling across regions and classes

    Each factor is `CEUD reference stock / reconstructed raw survivors`, separately by region and class.
    Values above one increase original-installation equivalents; values below one reduce them. This is a level
    calibration, not evidence that every historical sales count was wrong. Large factors can reflect incomplete
    history before {diagnosis['metadata']['scenario']['periods']['existing'][0]}, regional fleet boundaries
    and transfers, survival proxies, or the held observation tail.
    Aggregate stock alone cannot identify a separate correction for every vintage.

    The largest factor shown is **{largest_factor.multiplier:.3f}** for {largest_factor.region}
    {largest_factor.Class.lower()} using {largest_factor.Source}: {largest_factor.raw_survivors_k_vehicles:,.2f}
    thousand reconstructed survivors must match {largest_factor.ceud_stock_k_vehicles:,.2f} thousand CEUD stock.
    This identifies the size of the mismatch; the aggregate inputs cannot isolate which explanation causes it.

    The same class factor is used for acquisitions and physical survivors. Utilization uses calibrated physical
    stock, while combined vintage curves use acquisition weights. Existing fuel, powertrain and geography proxy rules
    are preserved; changing annual age weights can still change aggregate technology composition. The composition
    audit makes that consequence visible. LDV shares represent new registrations; truck shares include registered
    stock proxies and the existing medium-truck EV override.
    """), display_chart(calibration_chart, 'cohort-4', 'CEUD stock calibration changes the level of inferred cohorts'), display_table(calibration[[
        "Source", "region", "Class", "raw_sales_k_vehicles", "raw_survivors_k_vehicles", "ceud_stock_k_vehicles",
        "multiplier", "adjustment_percent", "stock_residual_k_vehicles",
    ]].rename(columns={"region": "Region", "raw_sales_k_vehicles": "Raw sales (k)", "raw_survivors_k_vehicles": "Raw survivors (k)",
        "ceud_stock_k_vehicles": "CEUD stock (k)", "multiplier": "Stock multiplier", "adjustment_percent": "Adjustment (%)",
        "stock_residual_k_vehicles": "Closure residual (k)"}), selection=None, page_size=10),
        mo.accordion({"Technology composition: selected source and region": display_table(selected_composition[[
            "Class", "tech", "method", "physical_stock_k_vehicles", "share_percent"]].rename(columns={
                "tech": "Technology", "method": "Method", "physical_stock_k_vehicles": "Physical stock (k)",
                "share_percent": "Within-class share (%)"}), selection=None, page_size=10),
            "Registered inputs and freshness": mo.vstack([mo.md("Inputs are read afresh; no captured charging or comparison report is required."),
                mo.json(diagnosis["metadata"])])})])
    return (calibration_chart,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, PROFILE_LABELS, alt, diagnosis, display_chart, display_table, mo, pd, region):
    survival_methods = ["Native survival", "Constant retirement"]
    native_curves = diagnosis["source_curves"].assign(Method=survival_methods[0])
    production_curves = diagnosis["production_curves"].assign(Method=survival_methods[1])
    survival_curves = pd.concat([native_curves, production_curves], ignore_index=True).assign(
        Class=lambda frame: frame.road_class.map(CLASS_LABELS), Source=lambda frame: frame.profile.map(PROFILE_LABELS))
    survival_chart = alt.Chart(survival_curves).mark_line(strokeWidth=2.0).encode(
        x=alt.X("age:Q", title="Vehicle age (years)", axis=alt.Axis(tickCount=4)),
        y=alt.Y("survival_probability:Q", title="Cumulative survival", scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("Method:N", scale=alt.Scale(domain=survival_methods, range=["#777777", "#438564"]),
            legend=alt.Legend(orient="bottom", symbolType="stroke", symbolSize=350)),
        strokeDash=alt.StrokeDash("Method:N", scale=alt.Scale(domain=survival_methods, range=[[1, 0], [6, 3]])),
        detail=["road_class:N", "profile:N", "Method:N"], order="age:Q",
        tooltip=["Source:N", "Class:N", "Method:N", "age:Q", alt.Tooltip("survival_probability:Q", format=".6f")],
    ).properties(width=180, height=170).facet(
        row=alt.Row("Source:N", title=None), column=alt.Column("Class:N", sort=list(CLASS_LABELS.values()), title=None,
            header=alt.Header(labelFontSize=10))).resolve_scale(x="independent")
    historical_projection = diagnosis["historical_projection"].loc[
        diagnosis["historical_projection"].region.eq(region.value)].copy()
    differing_groups = historical_projection.groupby(["profile", "road_class"]).native_difference_k_vehicles.agg(
        lambda values: values.abs().gt(1e-9).any()).rename("differs_from_native").reset_index()
    projection_values = historical_projection[["profile", "road_class", "calendar_year", "retained_k_vehicles"]].assign(
        Method=survival_methods[1])
    native_projection = historical_projection.merge(differing_groups, on=["profile", "road_class"], validate="many_to_one")
    native_projection = native_projection.loc[native_projection.differs_from_native & native_projection.native_complete,
        ["profile", "road_class", "calendar_year", "native_survivors_k_vehicles"]].rename(
            columns={"native_survivors_k_vehicles": "retained_k_vehicles"}).assign(Method=survival_methods[0])
    projection_methods = survival_methods if not native_projection.empty else [survival_methods[1]]
    projection_colors = [{"Native survival": "#777777", "Constant retirement": "#438564"}[name] for name in projection_methods]
    projection_dashes = [{"Native survival": [1, 0], "Constant retirement": [6, 3]}[name] for name in projection_methods]
    projection_values = pd.concat([native_projection, projection_values], ignore_index=True).assign(
        Class=lambda frame: frame.road_class.map(CLASS_LABELS), Source=lambda frame: frame.profile.map(PROFILE_LABELS))
    projection_start = int(historical_projection.calendar_year.min())
    projection_end = int(historical_projection.calendar_year.max())
    projection_ticks = sorted({projection_start, (projection_start + projection_end) // 2, projection_end})
    projection_chart = alt.Chart(projection_values).mark_line(strokeWidth=2.0).encode(
        x=alt.X("calendar_year:Q", title="Calendar year", axis=alt.Axis(format="d", values=projection_ticks)),
        y=alt.Y("retained_k_vehicles:Q", title="Historical survivors (k)", scale=alt.Scale(zero=True)),
        color=alt.Color("Method:N", scale=alt.Scale(domain=projection_methods, range=projection_colors),
            legend=alt.Legend(orient="bottom", symbolType="stroke", symbolSize=350)),
        strokeDash=alt.StrokeDash("Method:N", scale=alt.Scale(domain=projection_methods, range=projection_dashes)),
        detail=["road_class:N", "profile:N", "Method:N"], order="calendar_year:Q",
        tooltip=["Source:N", "Class:N", "Method:N", "calendar_year:O", alt.Tooltip("retained_k_vehicles:Q", format=",.6f")],
    ).properties(width=180, height=170).facet(row=alt.Row("Source:N", title=None),
        column=alt.Column("Class:N", sort=list(CLASS_LABELS.values()), title=None,
            header=alt.Header(labelFontSize=10))).resolve_scale(y="independent")
    survival_audit = diagnosis["production_survival_audit"].assign(
        Source=lambda frame: frame.profile.map(PROFILE_LABELS), Class=lambda frame: frame.road_class.map(CLASS_LABELS),
        retirement_percent=lambda frame: 100 * frame.terminal_retirement_rate)
    projection_audit = historical_projection.assign(
        comparable_year=lambda frame: frame.calendar_year.where(frame.native_complete),
    ).groupby(["profile", "road_class"], as_index=False).agg(
        last_comparable_year=("comparable_year", "max"),
        max_difference_k_vehicles=("native_difference_k_vehicles", lambda values: values.abs().max()),
        max_removed_k_vehicles=("removed_k_vehicles", "max"),
    ).assign(Source=lambda frame: frame.profile.map(PROFILE_LABELS), Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    reference_comparison = diagnosis["reference_survival_comparison"].loc[
        diagnosis["reference_survival_comparison"].region.eq(region.value)].assign(
            Source=lambda frame: frame.profile.map(PROFILE_LABELS), Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    model_losses = diagnosis["vintage_survival_audit"].groupby(["profile", "road_class"], as_index=False)[[
        "removed_reference_stock_k_vehicles", "removed_first_model_stock_k_vehicles", "removed_stock_years_k_vehicles"]].sum().assign(
            Source=lambda frame: frame.profile.map(PROFILE_LABELS), Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    threshold = diagnosis["metadata"]["tail_survival_threshold"]
    hard_zero_age = diagnosis["metadata"]["survival_treatment"]["hard_zero_age"]
    age_cap_note = "No maximum-age cap is selected." if hard_zero_age is None else f"The configured age cap forces zero at age {hard_zero_age}."
    changed_pairs = int(differing_groups.differs_from_native.sum())
    reference_change = reference_comparison.max_cohort_stock_difference_k_vehicles.max()
    reference_change_label = (f"{reference_change:,.6f} thousand vehicles" if pd.notna(reference_change)
                              else "unavailable because native support is incomplete")
    mo.vstack([mo.md(f"""
    ## 5. Native survival versus constant retirement

    The approved method preserves native survival observations, then continues the **last observed annual
    retirement rate** after source support ends. When the extended survival reaches **{100 * threshold:g}% or below**,
    the next year is zero. {age_cap_note} These are the production calculations for both MOVES and NHTSA + NEMS.
    The upper panels use independent age axes so each source's complete tail remains readable.

    The lower panels follow the same calibrated historical acquisitions, with **no future sales or future
    CEUD calibration**. Values are point-year survivors, not Temoa period averages. Native evidence stops when
    any positive cohort exceeds its known support; missing native tails are never treated as zero.
    A native comparison line appears only where known-support values differ from production: **{changed_pairs}
    source/class pairs** in {region.value}. Otherwise, one production line represents the overlapping values.
    The table quantifies differences that may be too small to see.

    The selected {100 * threshold:g}% threshold governs source extension. Combining annual cohorts into model
    vintages preserves those stock totals. Only final survival values below Temoa's supported numerical
    limit end earlier, and that stock loss is measured directly by the production calculation.
    Physical stock for utilization remains based on the source annual schedule, with the same stock multiplier. The maximum
    reference-year cohort-stock change from source harmonization in this region is
    **{reference_change_label}** among cohorts with complete native support; the audit also separates any final tail removal.
    """), display_chart(survival_chart, 'cohort-5', 'Native survival and the shared constant-retirement tail'), display_table(survival_audit[[
        "Source", "Class", "known_survival_max_age", "retirement_percent", "terminal_age"]].rename(columns={
            "known_survival_max_age": "Last native age", "retirement_percent": "Last annual retirement (%)",
            "terminal_age": "Zero-survival age"}), selection=None, pagination=False,
        label="One row per source and class; the configured threshold applies after native support"),
        display_chart(projection_chart, 'cohort-6', 'Historical cohorts retire under alternative survival schedules'), display_table(projection_audit[[
            "Source", "Class", "last_comparable_year", "max_difference_k_vehicles", "max_removed_k_vehicles"]].rename(columns={
                "last_comparable_year": "Last year with complete native support",
                "max_difference_k_vehicles": "Largest native/production difference (k)",
                "max_removed_k_vehicles": "Largest annual small-tail removal (k)"}), selection=None, pagination=False,
            label=f"Selected region {region.value}; maxima are across the displayed calendar years"),
        mo.accordion({"Reference-year stock scaling: native evidence and production": display_table(reference_comparison[[
            "Source", "Class", "native_complete", "native_raw_survivors_k_vehicles", "production_raw_survivors_k_vehicles",
            "native_multiplier", "production_multiplier", "max_cohort_stock_difference_k_vehicles"]].rename(columns={
                "native_complete": "Complete native support", "native_raw_survivors_k_vehicles": "Native raw survivors (k)",
                "production_raw_survivors_k_vehicles": "Production raw survivors (k)", "native_multiplier": "Native multiplier",
                "production_multiplier": "Production multiplier", "max_cohort_stock_difference_k_vehicles": "Largest cohort-stock change (k)"}),
            selection=None, pagination=False),
            "Small-tail retirement across all configured regions": display_table(model_losses[[
                "Source", "Class", "removed_reference_stock_k_vehicles", "removed_first_model_stock_k_vehicles", "removed_stock_years_k_vehicles"]].rename(columns={
                    "removed_reference_stock_k_vehicles": "Reference-year stock removed (k)",
                    "removed_first_model_stock_k_vehicles": "First model-year stock removed (k)",
                    "removed_stock_years_k_vehicles": "Lifetime stock removed (thousand vehicle-years)"}),
                selection=None, pagination=False)})])
    return projection_chart, survival_chart


if __name__ == "__main__":
    app.run()
