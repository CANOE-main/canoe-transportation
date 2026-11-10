"""Independent 2025 stock, activity–energy and demand comparison."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd
    import numpy as np
    from math import ceil, floor
    from validation.transportation_research import load_validation_diagnosis
    from validation.notebook_figures import identified_chart, source_legend_style

    CLASS_LABELS = {
        "cars": "Cars", "passenger_light_trucks": "Passenger light trucks",
        "freight_light_trucks": "Freight light trucks", "medium_trucks": "Medium trucks",
        "heavy_trucks": "Heavy trucks",
    }
    STOCK_LABELS = {**CLASS_LABELS, "passenger_light_trucks": "Light trucks", "freight_light_trucks": "Light trucks"}
    STOCK_ORDER = ["Cars", "Light trucks", "Medium trucks", "Heavy trucks"]
    BUILD_LABELS = {"Current backend": "v2.0 backend", "Legacy recipe": "Legacy variant"}
    CASE_LABELS = {
        "Backend before calibration": "5-year avg.",
        **BUILD_LABELS,
        "Backend energy sensitivity": "v2.0 energy sensitivity",
    }
    BUILDS = list(BUILD_LABELS.values())
    CASES = list(CASE_LABELS.values())
    CASE_COLORS = ["#999999", "#2166ac", "#7b3294", "#b35806"]
    CASE_SHAPES = ["circle", "square", "triangle-up", "diamond"]

    def stock_grid(chart, paired_scales=False):
        if paired_scales:
            rows = [chart.transform_filter(alt.FieldOneOfPredicate(field="Class", oneOf=names)).properties(
                width=370, height=215).facet(column=alt.Column("Class:N", sort=names, title=None)).resolve_scale(y="shared")
                for names in [STOCK_ORDER[:2], STOCK_ORDER[2:]]]
            return alt.vconcat(*rows, spacing=20).resolve_scale(y="independent")
        return chart.properties(width=370, height=190).facet(
            facet=alt.Facet("Class:N", sort=STOCK_ORDER, title=None), columns=2,
        ).resolve_scale(x="shared", y="independent")

    def class_grid(chart, relative=False):
        prefix = "" if relative else "billion "
        labels = [label + (f" ({prefix}passenger-km)" if name in {"cars", "passenger_light_trucks"}
                           else f" ({prefix}tonne-km)") for name, label in CLASS_LABELS.items()]
        grid = chart.properties(width=280, height=210).facet(
            facet=alt.Facet("Panel:N", sort=labels, title=None, header=alt.Header(labelFontSize=11)), columns=3,
        ).resolve_scale(x="independent", y="independent")
        return grid.transform_calculate(Panel=(
            "datum.Class + (indexof(['Cars', 'Passenger light trucks'], datum.Class) >= 0"
            f" ? ' ({prefix}passenger-km)' : ' ({prefix}tonne-km)')"))

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


    def build_cohort_hatching(contributions, builds, panel_height=190, panel_width=370, value_column="surviving_k_vehicles"):
        """Stack unchanged capacities and clip diagonal rules to legacy bar bounds.

        Quantitative rectangle coordinates use years and thousand vehicles. Invisible
        extents fix the per-class scale; with nice=False at 370x190, hatch spacing is
        seven pixels and slopes are 45 degrees. This is display geometry only.
        """
        if len(builds) != 2 or len(set(builds)) != 2 or panel_height <= 0 or panel_width <= 0:
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
        # Five-year vintage spacing; grouped pair occupies 3.0 of those 5.0 years.
        centers = rectangles.vintage + rectangles.build_order.map({0: -.8, 1: .8})
        rectangles["x0"], rectangles["x1"] = centers - .7, centers + .7
        minimum = float(rectangles.vintage.min()) - 2.5
        maximum = float(rectangles.vintage.max()) + 2.5
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

    def cohort_style_legend(positions, builds):
        """A vertical key using the same colours and real solid/striped swatches."""
        year_rows = pd.DataFrame({"cohort_position": positions, "y0": [24 + 21 * i for i in range(5)]})
        year_rows["y1"] = year_rows.y0 + 13
        year_rows["y"] = year_rows.y0 + 6.5
        build_rows = pd.DataFrame({"label": [builds[0] + " (solid)", builds[1] + " (striped)"],
                                   "y0": [171., 197.], "y1": [187., 213.], "y": [179., 205.]})
        stripe_rows = pd.DataFrame(clipped_diagonals(0., 16., 197., 213., spacing=5.),
                                   columns=["x0", "x1", "y0", "y1"])
        stripe_rows[["y0", "y1"]] = 410. - stripe_rows[["y0", "y1"]]
        x_scale = alt.Scale(domain=[0, 195], nice=False, zero=False)
        y_scale = alt.Scale(domain=[224, 0], nice=False, zero=False)
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
        build_text = alt.Chart(build_rows).mark_text(align="left", baseline="middle", fontSize=11).encode(
            x=alt.X(datum=24, scale=x_scale, axis=None), y=alt.Y("y:Q", scale=y_scale, axis=None), text="label:N")
        titles = alt.Chart(pd.DataFrame({"label": ["5-year cohorts", "Build"], "y": [10, 155]})).mark_text(
            align="left", baseline="middle", fontWeight="bold", fontSize=12).encode(
            x=alt.X(datum=0, scale=x_scale, axis=None), y=alt.Y("y:Q", scale=y_scale, axis=None), text="label:N")
        return alt.layer(years, year_text, swatches, stripes, build_text, titles).properties(width=195, height=224)

    def display_table(frame, **options):
        # Format display only; scientific inputs and sortable numeric values stay intact.
        formats = {column: "{:,.4f}" for column in frame.select_dtypes(include="floating").columns}
        formats.update(options.pop("format_mapping", {}))
        return mo.ui.table(frame, format_mapping=formats, **options)

    def display_chart(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    return BUILDS, BUILD_LABELS, CASES, CASE_COLORS, CASE_LABELS, CASE_SHAPES, CLASS_LABELS, STOCK_LABELS, STOCK_ORDER, alt, class_grid, cohort_style_legend, build_cohort_hatching, display_chart, display_table, load_validation_diagnosis, mo, pd, stock_grid, identified_chart, source_legend_style


@app.cell
def _(load_validation_diagnosis):
    diagnosis = load_validation_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    scenario = diagnosis["fleet_metadata"]["scenario"]
    mo.vstack([mo.md(f"""
    # Transportation validation: independent methods

    **2025 evaluation**, shared **[*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) 2023 edition**; observed stock, activity and energy are held
    constant through 2025. Backend stock is normalized to [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm); legacy stock is independently reconstructed.
    This comparison uses production's **fixed-lifetime mode** (`survival_curves: false`).
    Road stock comes from annual sales and configured survival schedules, including their harmonized tails,
    followed by [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) calibration and fixed-lifetime eligibility allocation. Curve-mode installed capacity
    and the separate physical-stock handoff are examined in [stock_cohort_diagnosis.py](stock_cohort_diagnosis.py).
    [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) observation year, UF calibration year, evaluation year and model labels are different concepts.
    Prospective **model period 2025 represents demand calendar year 2030**; baseline accounting here is calendar 2025.
    Scenario: [{scenario['scenario']['name']}](../../{diagnosis['scenario_path']}).

    Primary legacy reference: the supplied notebook's **active `fixed_lifetime=False` recipe** and workbooks.
    The supplied `canoe_ON_16d.sqlite` does not reproduce its capacity output; it remains discrepancy evidence.
    It is also the explicitly selected **inherited-parameter snapshot**, substituting for missing
    `Input data/canoe_ALL_cm.sqlite`. Legacy efficiency-dependent energy is conditional on that substitution;
    it does not establish the original input's historical identity. Reference files remain immutable.
    The comparison preserves each method's vehicle efficiencies, costs and calibration.
    Production charger transfers are now **1.0 PJ/PJ** because vehicle electricity consumption
    already includes charging losses; Section 6 explains the accounting correction.
    """), mo.accordion({"Reproduce offline, in order": mo.md(f"""
    ```powershell
    uv run --offline python src/build_transport.py --scenario {diagnosis['scenario_path']} --overwrite
    uv run --offline python -m validation.transportation_research --scenario {diagnosis['scenario_path']} --write-report
    uv run --offline python -m validation.charging_cost_comparison --scenario {diagnosis['scenario_path']} --write-report
    uv run --offline marimo edit docs/insights/transportation_validation_diagnosis.py
    ```
    Both reports must identify the same configured build. All numerical comparison tables are exported
    under its configured research report route, with input and CSV hashes. A fresh notebook replays the
    inspected legacy calculation functions and rejects changed recipe/input identities.
    """)})])
    return


@app.cell
def _(diagnosis, mo):
    regions = sorted(diagnosis["fleet"]["stock_activity_energy"].region.unique())
    region = mo.ui.dropdown(regions, value=regions[0], label="Comparison region")
    region
    return (region,)


@app.cell(hide_code=True)
def _(mo):
    stock_quantity = mo.ui.dropdown(
        ["Native ExistingCapacity parameters", "Surviving stock at calendar 2025"],
        value="Native ExistingCapacity parameters", label="Quantity for both cohort charts",
    )
    mo.vstack([mo.md("""
    ## 1. Existing capacity and cohort lineage

    The default shows each method's **native ExistingCapacity parameter**. Legacy exports the
    **sum of original acquisitions** in each vintage; the backend exports **already surviving,
    [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)-scaled stock after fixed-lifetime allocation in this scenario**. These are deliberately different meanings,
    even though both use thousand vehicles. The legacy calculation has not been normalized to [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
    or forced through the backend's allocation.

    Switch to **surviving stock at calendar 2025** to compare the backend stock with legacy
    acquisitions × its own rounded survival at `2025 − representative vintage`. This second view
    is a point-year diagnostic, **not Temoa's period-average adjusted capacity**. It is the quantity
    the previous two charts displayed; it was not the legacy ExistingCapacity export.
    The backend sales → survivors → allocation plot and Temoa retirement assessment are now in
    [stock_cohort_diagnosis.py](stock_cohort_diagnosis.py).
    """), stock_quantity])
    return (stock_quantity,)


@app.cell
def _(BUILD_LABELS, STOCK_LABELS, diagnosis, region, stock_quantity):
    lineage_keys = ["region", "road_class", "evidence", "vintage", "vintage_year",
                    "annual_position", "cohort_position", "interval"]
    comparison_lineage = diagnosis["fleet"]["comparison_annual_stock"]
    selected_lineage = comparison_lineage.loc[comparison_lineage.region.eq(region.value)].groupby(
        lineage_keys, as_index=False).surviving_k_vehicles.sum()
    legacy_acquisitions = diagnosis["fleet"]["legacy_reconstructed_annual"].groupby(
        ["region", "road_class", "vintage", "vintage_year"], as_index=False).raw_capacity_k_vehicles.sum()
    legacy_acquisitions["evidence"] = "Legacy recipe"
    selected_lineage = selected_lineage.merge(
        legacy_acquisitions, on=["region", "road_class", "evidence", "vintage", "vintage_year"],
        how="left", validate="one_to_one")
    if selected_lineage.loc[selected_lineage.evidence.eq("Legacy recipe"), "raw_capacity_k_vehicles"].isna().any():
        raise ValueError("Native legacy acquisition lineage is incomplete")
    selected_lineage["parameter_k_vehicles"] = selected_lineage.raw_capacity_k_vehicles.fillna(
        selected_lineage.surviving_k_vehicles)
    show_survivors = stock_quantity.value == "Surviving stock at calendar 2025"
    selected_lineage["Vehicles"] = selected_lineage[
        "surviving_k_vehicles" if show_survivors else "parameter_k_vehicles"]
    selected_lineage["Class"] = selected_lineage.road_class.map(STOCK_LABELS)
    selected_lineage["evidence"] = selected_lineage.evidence.replace(BUILD_LABELS)
    stock_axis_title = ("Surviving stock at calendar 2025 (thousand)" if show_survivors
                        else "Native ExistingCapacity (thousand vehicles)")
    return selected_lineage, show_survivors, stock_axis_title


@app.cell(hide_code=True)
def _(BUILDS, STOCK_LABELS, alt, diagnosis, display_chart, display_table, mo, pd, region,
      selected_lineage, show_survivors, stock_axis_title, stock_grid):
    vintage_stocks = selected_lineage.groupby(["Class", "evidence", "vintage"], as_index=False).Vehicles.sum()
    vintage_stocks["Kind"] = "Stock"
    stock_totals = selected_lineage.groupby(["Class", "evidence"], as_index=False)[[
        "parameter_k_vehicles", "surviving_k_vehicles"]].sum()
    ceud_totals = diagnosis["fleet"]["stock_activity_energy"]
    ceud_totals = ceud_totals.loc[ceud_totals.region.eq(region.value)].assign(
        Class=lambda frame: frame.road_class.map(STOCK_LABELS)).groupby("Class", as_index=False).ceud_stock_k_vehicles.sum()
    stock_totals = stock_totals.merge(ceud_totals, on="Class", validate="many_to_one")
    stock_totals["Difference (%)"] = 100 * (stock_totals.surviving_k_vehicles / stock_totals.ceud_stock_k_vehicles - 1)
    targets = stock_totals.rename(columns={"ceud_stock_k_vehicles": "Target"}).assign(Kind="Target")
    stock_data = pd.concat([vintage_stocks, targets if show_survivors else targets.iloc[:0]], ignore_index=True)
    stock_bars = alt.Chart().transform_filter(alt.datum.Kind == "Stock").mark_bar(size=58).encode(
        x=alt.X("evidence:N", sort=BUILDS, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("Vehicles:Q", title=stock_axis_title, stack="zero"),
        color=alt.Color("vintage:O", title="Model vintage", scale=alt.Scale(scheme="blues"), legend=alt.Legend(orient="right")),
        order="vintage:O", tooltip=["Class:N", "evidence:N", "vintage:O", alt.Tooltip("Vehicles:Q", title=stock_axis_title, format=",.3f")],
    )
    stock_target = alt.Chart().transform_filter(alt.datum.Kind == "Target").mark_point(
        shape="cross", color="#111111", filled=False, size=180, strokeWidth=2.5).encode(
        x=alt.X("evidence:N", sort=BUILDS), y=alt.Y("Target:Q", title=stock_axis_title),
        tooltip=["Class:N", alt.Tooltip("Target:Q", title="CEUD 2023 stock held to 2025 (k)", format=",.3f")])
    stock_comparison_chart = stock_grid(alt.layer(stock_bars, stock_target, data=stock_data), paired_scales=True)
    stock_table_columns = ["Class", "evidence", "parameter_k_vehicles", "surviving_k_vehicles"]
    if show_survivors:
        stock_table_columns += ["ceud_stock_k_vehicles", "Difference (%)"]
    quantity_note = ("Black crosses are CEUD 2023 stock held through 2025; the backend matches it by calibration."
                     if show_survivors else "No CEUD target is drawn for raw historical acquisitions; CEUD measures surviving stock.")
    mo.vstack([mo.md(f"""
    ### Native parameters and the separate stock snapshot

    One bar per build, stacked by model vintage. Cars/light trucks share one vertical scale;
    medium/heavy share another. **{quantity_note}**

    **Why the earlier car bar was smaller:** it showed surviving stock after native representative-vintage
    retirement. The legacy history starts in 2005, and its car powertrain shares are missing in **2005 and
    2006**, so those products become zero in the native recipe. Its older representative vintage also
    gives every annual segment in a bin the same lower survival fraction. The backend instead retains
    annual-age survival over 2000–2025 and scales the resulting stock to [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm). None of those differences
    is corrected away here. The adjacent columns show the actual export and the separate point-year stock.
    """), display_chart(stock_comparison_chart), display_table(stock_totals[stock_table_columns].rename(columns={
        "evidence": "Build", "parameter_k_vehicles": "Native ExistingCapacity (k)",
        "surviving_k_vehicles": "Point-2025 survivors (k)", "ceud_stock_k_vehicles": "CEUD stock target (k)",
    }), selection=None, pagination=False, format_mapping={"Difference (%)": "{:+.4f}%"}),
        mo.accordion({"Disputed supplied SQLite versus raw notebook export": mo.vstack([
            mo.md("Raw export is compared with raw stored capacity here. No matching branch is inferred from filenames or saved messages."),
            display_table(diagnosis["fleet"]["reference_recipe_totals"], selection=None, pagination=False),
            display_table(diagnosis["fleet"]["legacy_reconstruction_context"], selection=None, pagination=False),
        ])})])
    return (stock_comparison_chart,)


@app.cell(hide_code=True)
def _(BUILDS, alt, display_chart, mo, pd, stock_grid, display_table, build_cohort_hatching,
      cohort_style_legend, selected_lineage, show_survivors, stock_axis_title):
    contributions = selected_lineage.groupby(["Class", "evidence", "vintage", "vintage_year", "annual_position", "cohort_position", "interval"], as_index=False).Vehicles.sum()
    positions = ["Year 1", "Year 2", "Year 3", "Year 4", "Year 5"]
    rectangles, hatch_lines, plot_extents = build_cohort_hatching(contributions, BUILDS, value_column="Vehicles")
    vintage_ticks = rectangles.attrs["vintage_ticks"]
    vintage_domain = rectangles.attrs["x_domain"]
    geometry = pd.concat([rectangles.assign(Kind="Stock"), hatch_lines.assign(Kind="Hatch"),
                          plot_extents.assign(Kind="Extent")], ignore_index=True)
    cohort_x = alt.X("x0:Q", title="Model vintage", scale=alt.Scale(domain=vintage_domain, nice=False, zero=False),
                     axis=alt.Axis(values=vintage_ticks, format="d", labelAngle=0))
    cohort_y = alt.Y("y0:Q", title=stock_axis_title, scale=alt.Scale(nice=False, zero=False))
    capacity_bars = alt.Chart().transform_filter(alt.datum.Kind == "Stock").mark_rect().encode(
        x=cohort_x, x2="x1:Q", y=cohort_y, y2="y1:Q",
        color=alt.Color("cohort_position:N", scale=alt.Scale(domain=positions, scheme="blues"), legend=None),
        tooltip=["Class:N", "evidence:N", "vintage:O", "vintage_year:O", "cohort_position:N", "interval:N",
                 alt.Tooltip("Vehicles:Q", title=stock_axis_title, format=",.4f")])
    legacy_hatching = alt.Chart().transform_filter(alt.datum.Kind == "Hatch").mark_rule(
        stroke="#111", strokeWidth=.7, opacity=.42).encode(x=cohort_x, x2="x1:Q", y=cohort_y, y2="y1:Q")
    domain_extents = alt.Chart().transform_filter(alt.datum.Kind == "Extent").mark_rect(opacity=0).encode(
        x=cohort_x, x2="x1:Q", y=cohort_y, y2="y1:Q")
    vintage_chart = alt.hconcat(
        stock_grid(alt.layer(domain_extents, capacity_bars, legacy_hatching, data=geometry)),
        cohort_style_legend(positions, BUILDS), spacing=18,
    ).resolve_scale(color="independent", x="independent", y="independent")
    lineage_note = ("Legacy annual colours carry one representative-vintage survival fraction shared by the bin; they do not imply annual-age survival."
                    if show_survivors else "Legacy annual colours are unretired acquisition contributions, exactly as exported by its native recipe.")
    mo.vstack([mo.md(f"""
    ### Annual contributions inside each model vintage

    **Solid bars = v2.0 backend; striped bars = legacy variant**, as shown in the key.
    Both builds retain the same annual-position colours. Sum of annual segments equals the selected vintage
    and class quantities above. Positions mean `vintage + 1 … vintage + 5`; exact years are in tooltips.
    **Partial bins are summed, never discarded**: available 2013–2015 all contribute to vintage 2010.
    Aggregation precedes the backend's separate eligibility screen. The legacy's oldest bin contains only
    **2005 → 2000**; the backend oldest proxy includes **2000–2005**, an extra lower endpoint
    (zero allocated stock here). Hover identifies that boundary exception.

    **{lineage_note}** Backend lineage retains its annual survival, [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) multiplier and technology-specific
    redistribution in either view. The audit table preserves both quantities separately.
    """), display_chart(vintage_chart), mo.accordion({"Full annual lineage": display_table(selected_lineage, selection=None, page_size=10)})])
    return (vintage_chart,)


@app.cell(hide_code=True)
def _(CASES, CASE_COLORS, CASE_LABELS, CASE_SHAPES, CLASS_LABELS, alt, diagnosis, display_chart, mo, region, display_table):
    cases = diagnosis["fleet"]["baseline_activity_energy_cases"]
    cases = cases.loc[cases.region.eq(region.value)].assign(
        Class=lambda frame: frame.road_class.map(CLASS_LABELS), case=lambda frame: frame.case.replace(CASE_LABELS))
    legacy_bounds = diagnosis["fleet"]["legacy_reconstructed_utilization"]
    legacy_bounds = legacy_bounds.loc[legacy_bounds.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    utilization_points = alt.Chart(cases).mark_point(filled=True, size=110).encode(
        y=alt.Y("Class:N", sort=list(CLASS_LABELS.values()), title=None),
        x=alt.X("utilization:Q", title="Annual utilization factor (dimensionless)", scale=alt.Scale(zero=True)),
        color=alt.Color("case:N", scale=alt.Scale(domain=CASES, range=CASE_COLORS), title=None, legend=alt.Legend(orient="bottom", columns=2, labelLimit=260)),
        shape=alt.Shape("case:N", scale=alt.Scale(domain=CASES, range=CASE_SHAPES), title=None, legend=alt.Legend(orient="bottom", columns=2, labelLimit=260)),
        tooltip=["Class:N", "case:N", alt.Tooltip("utilization:Q", format=".6f")],
    )
    utilization_bounds = alt.Chart(legacy_bounds).mark_rule(color="#7b3294", strokeWidth=2).encode(
        y=alt.Y("Class:N", sort=list(CLASS_LABELS.values())), x="lower_bound:Q", x2="upper_bound:Q",
        tooltip=["Class:N", alt.Tooltip("lower_bound:Q", format=".6f"), alt.Tooltip("upper_bound:Q", format=".6f")])
    utilization_chart = alt.layer(utilization_bounds, utilization_points).properties(width=650, height=200)
    calibration = diagnosis["fleet"]["backend_utilization_bridge"]
    calibration = calibration.loc[calibration.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    mo.vstack([mo.md("""
    ## 2. Utilization: calibration values, bounds and sensitivity

    Grey = arithmetic mean of `activity_y / (stock_y × c2a)` for observed **2017, 2018, 2019, 2022, 2023**;
    2020–2021 are excluded. Blue = **2023 [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) class activity held at 2025 / Σ(inserted stock × c2a)**.
    The table exposes target, denominator and resulting change. Reviewed manual c2a and efficiency stay fixed.
    This is historical activity calibration, separate from GDP-indexed service demand.

    Purple triangles = **native legacy central calibration**, with a line showing its **±2.5% parameter bounds**.
    Its denominator is raw model-vintage capacity × workbook survival at **2023** × own c2a.
    It already includes the 2024–2025 extended sales in vintage 2020; it is a recipe calibration identity,
    not an observation of calendar-2023 acquisitions. Bounds are constraints, not realized solver operation.

    Orange = **unimplemented backend energy sensitivity**: blue UF × [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) energy / backend energy.
    Only UF changes; stock, composition, efficiencies and c2a are held fixed. The native legacy point uses
    its own complete parameters and is independent of that sensitivity.
    """), display_chart(utilization_chart), display_table(calibration[[
        "Class", "source_years", "ceud_activity_bn", "stock_denominator_k_vehicles", "c2a",
        "capacity_activity_denominator_bn", "prior_mean_utilization", "implemented_utilization", "calibration_percent",
    ]].rename(columns={"ceud_activity_bn": "2023 activity held at 2025 (bn)", "stock_denominator_k_vehicles": "Backend stock (k)",
        "capacity_activity_denominator_bn": "Σ stock × c2a (bn)", "prior_mean_utilization": "Grey UF", "implemented_utilization": "Blue UF",
        "calibration_percent": "UF change (%)", "source_years": "Grey source years"}), selection=None, pagination=False,
        label="c2a: billion passenger-km or tonne-km per thousand vehicles; UF = target / capacity-activity denominator"),
        mo.accordion({"Annual grey inputs and native legacy denominator": mo.vstack([
            display_table(diagnosis["fleet"]["backend_utilization_source_years"], selection=None, page_size=10),
            display_table(legacy_bounds, selection=None, pagination=False),
        ])})])
    return calibration, cases, utilization_chart


@app.cell(hide_code=True)
def _(CASES, CASE_COLORS, CASE_SHAPES, alt, calibration, cases, class_grid, display_chart, mo, pd, display_table, identified_chart, source_legend_style):
    target_rows = cases.loc[cases.case.eq(CASES[1])].copy()
    references = []
    for label, column in [("NRCan CEUD 2023 held at 2025", "ceud_activity_bn"),
                           ("Backend real-GDP 2025 target", "current_base_year_demand_bn"),
                           ("Legacy workbook 2025 target", "legacy_calendar_base_year_demand_bn")]:
        references.append(target_rows.assign(Reference=label, utilization=float("nan"), Service=lambda frame: frame[column],
                                            Energy=lambda frame: frame.ceud_secondary_energy_pj))
    reference_points = pd.concat(references, ignore_index=True)
    reconciliation_data = pd.concat([
        cases.assign(Kind="Case", Service=lambda frame: frame.implied_activity_bn, Energy=lambda frame: frame.implied_vehicle_input_pj),
        reference_points.assign(Kind="Reference"),
    ], ignore_index=True)
    reconciliation_data["Point"] = reconciliation_data["case"].where(reconciliation_data.Kind.eq("Case"), reconciliation_data.Reference)
    point_order = CASES + [item["Reference"].iloc[0] for item in references]
    point_colors = CASE_COLORS + ["#111111"] * 3
    point_shapes = CASE_SHAPES + ["cross", "triangle-up", "triangle-down"]
    reconciliation_data["Energy difference (%)"] = 100 * (reconciliation_data.Energy / reconciliation_data.ceud_secondary_energy_pj - 1)
    reconciliation_data["Service difference (%)"] = 100 * (reconciliation_data.Service / reconciliation_data.ceud_activity_bn - 1)
    # Symmetric per-class extents keep all four quadrants visible, even for near closure.
    residual_extents = []
    for class_name, group in reconciliation_data.groupby("Class"):
        x_limit = max(1., float(group["Energy difference (%)"].abs().max()) * 1.12)
        y_limit = max(1., float(group["Service difference (%)"].abs().max()) * 1.12)
        for sign in [-1, 1]:
            residual_extents.append({"Class": class_name, "Kind": "Extent",
                                     "Energy difference (%)": sign * x_limit, "Service difference (%)": sign * y_limit})
    residual_plot_data = pd.concat([reconciliation_data, pd.DataFrame(residual_extents)], ignore_index=True)
    residual_x = alt.X("Energy difference (%):Q", title="Energy difference from NRCan CEUD (%)", scale=alt.Scale(zero=False, nice=False))
    residual_y = alt.Y("Service difference (%):Q", title="Service difference from NRCan CEUD (%)", scale=alt.Scale(zero=False, nice=False))
    residual_points = alt.Chart().transform_filter(alt.datum.Kind != "Extent").mark_point(filled=True, size=100, strokeWidth=1.5).encode(
        x=residual_x, y=residual_y,
        color=alt.Color("Point:N", scale=alt.Scale(domain=point_order, range=point_colors), title=None,
                        legend=alt.Legend(orient="bottom", columns=2, labelLimit=360, labelFontStyle=source_legend_style())),
        shape=alt.Shape("Point:N", scale=alt.Scale(domain=point_order, range=point_shapes), title=None,
                        legend=alt.Legend(orient="bottom", columns=2, labelLimit=360, labelFontStyle=source_legend_style())),
        stroke=alt.Stroke("Point:N", scale=alt.Scale(domain=point_order, range=point_colors), legend=None),
        tooltip=["Class:N", "Point:N", alt.Tooltip("utilization:Q", format=".6f"),
                 alt.Tooltip("Energy difference (%):Q", format="+.4f"), alt.Tooltip("Service difference (%):Q", format="+.4f"),
                 alt.Tooltip("Energy:Q", title="Total energy (PJ)", format=",.4f"),
                 alt.Tooltip("Service:Q", title="Total service (billion)", format=",.4f")])
    residual_bounds = alt.Chart().transform_filter(alt.datum.Kind == "Extent").mark_point(opacity=0).encode(x=residual_x, y=residual_y)
    zero_energy = alt.Chart().transform_aggregate(groupby=["Class"]).mark_rule(color="#777", strokeDash=[5, 4]).encode(x=alt.datum(0))
    zero_service = alt.Chart().transform_aggregate(groupby=["Class"]).mark_rule(color="#777", strokeDash=[5, 4]).encode(y=alt.datum(0))
    activity_energy_chart = class_grid(alt.layer(residual_bounds, zero_energy, zero_service, residual_points,
                                                data=residual_plot_data), relative=True)
    activity_energy_chart = identified_chart(activity_energy_chart, "comparison-activity-energy",
                                            "Activity and energy agreement are separate comparisons")
    mo.vstack([mo.md("""
    ## 3. Baseline activity–energy reconciliation

    **x = energy difference from [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) (%); y = service difference from [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) (%)**.
    Each difference is `100 × (model / CEUD − 1)`. Dashed axes meet at **(0, 0)**: simultaneous
    activity and energy agreement. Right means more energy; above means more service. Each class has
    symmetric axes so all four quadrants remain visible. Tooltips retain total PJ and billion passenger-km
    or tonne-km. `Service = Σ(stock × c2a × UF)`; `energy = Σ(service / cohort efficiency)`.
    Energy retains each vehicle source's consumption boundary and counts PHEV blend input once.
    Embedded charging losses remain in vehicle electricity consumption; no extra charger-loss multiplier is applied.
    [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) secondary energy has its reported fleet/fuel boundary; unsupported fuels and composition can differ.
    Black cross = [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) at zero difference. Black triangles have zero energy difference and use the distinct
    calendar-2025 demand targets (backend real GDP; legacy workbook price index) for their service difference. They combine
    observed energy with projected service for context; they are not independent observations.

    A common UF change with fixed composition/efficiency scales **both** totals. Collinearity of backend cases
    is an accounting identity, not evidence of fit. If the energy-per-service ratio differs from [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm),
    one common UF cannot meet both targets: energy closure necessarily moves activity off its target.
    The table quantifies the tradeoff. The native legacy point also loses stock between its 2023 calibration
    and 2025 evaluation, so its service need not close the original activity target.

    Gaps suggest a mismatch in aggregate energy per unit service, but do not by themselves identify
    efficiency error. Fleet composition, service units/load, cohort efficiency, supported fuels, energy
    boundaries and the missing inherited input identity need reconciliation before recommending a new
    efficiency or utilization calibration. **No further scientific recalibration is implemented.**
    """), display_chart(activity_energy_chart), display_table(cases[[
        "Class", "case", "implied_vehicle_input_pj", "implied_activity_bn", "energy_difference_percent", "activity_difference_percent",
        "energy_per_service_pj_per_bn", "ceud_energy_per_service_pj_per_bn",
    ]].rename(columns={"case": "UF case", "implied_vehicle_input_pj": "Energy (PJ)", "implied_activity_bn": "Service (bn)",
        "energy_difference_percent": "Energy gap (%)", "activity_difference_percent": "Activity gap (%)",
        "energy_per_service_pj_per_bn": "PJ / bn service", "ceud_energy_per_service_pj_per_bn": "CEUD PJ / bn service"}), selection=None,
        pagination=False, label="Passenger and freight units stay separate; energy sensitivity is conditional, legacy energy uses the declared inherited snapshot"),
        display_table(calibration[["Class", "implemented_utilization", "energy_closing_utilization", "energy_closure_activity_difference_percent"]],
                    selection=None, pagination=False, label="Backend sensitivity: UF needed for energy closure and resulting historical activity error")])
    return (activity_energy_chart,)


@app.cell(hide_code=True)
def _(BUILDS, BUILD_LABELS, CLASS_LABELS, alt, class_grid, diagnosis, display_chart, mo, pd, region, display_table):
    methods = diagnosis["fleet"]["demand_method_comparison"]
    matched = methods.loc[methods.region.eq(region.value) & methods.calendar_year.notna()].copy()
    matched["Build"] = matched.evidence.replace({**BUILD_LABELS, "Notebook recipe replay": BUILD_LABELS["Legacy recipe"]})
    matched["Status"] = "Emitted model demand"
    baseline_context = diagnosis["fleet"]["demand_baseline_comparison"]
    baseline_context = baseline_context.loc[baseline_context.region.eq(region.value)]
    context_rows = []
    for target in baseline_context.itertuples():
        unit = "bn passenger-km" if target.road_class in {"cars", "passenger_light_trucks"} else "bn tonne-km"
        for build, anchor_year, anchor_value, base_value in [
            (BUILDS[0], target.current_anchor_year, target.current_activity_bn, target.current_base_year_demand_bn),
            (BUILDS[1], target.notebook_baseline_year, target.notebook_activity_bn, target.legacy_calendar_base_year_demand_bn),
        ]:
            for year, value, status in [(anchor_year, anchor_value, "Observed CEUD anchor"),
                                         (target.current_baseline_year, base_value, "2025 growth context; not an emitted period")]:
                context_rows.append({"region": target.region, "road_class": target.road_class, "Build": build,
                                     "calendar_year": year, "period": None, "demand_bn": value, "unit": unit, "Status": status})
    matched = pd.concat([matched, pd.DataFrame(context_rows)], ignore_index=True)
    matched["Class"] = matched.road_class.map(CLASS_LABELS)
    common_years = sorted(set.intersection(*[set(group.calendar_year) for _, group in matched.groupby("Build")]))
    matched = matched.loc[matched.calendar_year.isin(common_years)].sort_values(["road_class", "Build", "calendar_year"]).copy()
    matched["calendar_year"] = matched.calendar_year.astype(int)
    matched["period"] = matched.period.astype("Int64")
    matched["Growth index"] = matched.demand_bn / matched.groupby(["road_class", "Build"]).demand_bn.transform("first")
    demand_years = sorted(matched.calendar_year.unique().tolist())
    demand_base = alt.Chart().encode(
        x=alt.X("calendar_year:Q", title="Demand calendar year", axis=alt.Axis(format="d", values=demand_years)),
        color=alt.Color("Build:N", scale=alt.Scale(domain=BUILDS, range=["#2166ac", "#7b3294"]), title=None,
                        legend=alt.Legend(orient="bottom", symbolType="stroke", symbolSize=300)),
        y=alt.Y("demand_bn:Q", title="Billion service-km (units in panel)", scale=alt.Scale(zero=True)),
        detail="Build:N", order="calendar_year:Q",
        tooltip=["Class:N", "Build:N", "Status:N", "calendar_year:O", "period:O", "unit:N",
                 alt.Tooltip("demand_bn:Q", format=",.4f"), alt.Tooltip("Growth index:Q", format=".4f")])
    demand_lines = demand_base.mark_line().encode(
        strokeDash=alt.StrokeDash("Build:N", scale=alt.Scale(domain=BUILDS, range=[[1, 0], [7, 3]]), title=None,
                                  legend=alt.Legend(orient="bottom", symbolType="stroke", symbolSize=300)))
    emitted_points = demand_base.transform_filter(alt.datum.Status == "Emitted model demand").mark_point(filled=True, size=35)
    context_points = demand_base.transform_filter(alt.datum.Status == "2025 growth context; not an emitted period").mark_point(shape="diamond", filled=False, size=70)
    anchor_points = demand_base.transform_filter(alt.datum.Status == "Observed CEUD anchor").mark_point(shape="cross", size=80).encode(color=alt.value("#111"))
    demand_chart = class_grid(alt.layer(demand_lines, emitted_points, context_points, anchor_points, data=matched))
    growth_spread = matched.groupby(["Build", "calendar_year"])["Growth index"].agg(["min", "max"])
    if (growth_spread["max"] - growth_spread["min"]).abs().gt(1e-12).any():
        raise ValueError("Growth differs by class; retain class facets instead of collapsing trajectories")
    growth_data = matched.groupby(["Build", "calendar_year"], as_index=False).agg({"Growth index": "first", "Status": "first"})
    demand_growth_chart = alt.Chart(growth_data).mark_line(point=True).encode(
        x=alt.X("calendar_year:Q", title="Demand calendar year", axis=alt.Axis(format="d", values=demand_years)),
        y=alt.Y("Growth index:Q", title="Observed CEUD 2023 anchor = 1", scale=alt.Scale(zero=False)),
        color=alt.Color("Build:N", scale=alt.Scale(domain=BUILDS, range=["#2166ac", "#7b3294"]), title=None, legend=alt.Legend(orient="bottom")),
        tooltip=["Build:N", "Status:N", "calendar_year:O", alt.Tooltip("Growth index:Q", format=".4f")],
    ).properties(width=790, height=190)
    attribution = diagnosis["fleet"]["demand_attribution"]
    attribution = attribution.loc[attribution.region.eq(region.value)]
    car_context = matched.loc[matched.road_class.eq("cars")].set_index(["Build", "calendar_year"]).demand_bn
    mo.vstack([mo.md(f"""
    ## 4. Demand: workbook meaning and calendar alignment

    Both methods anchor to the same **2023 [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) observed activity**. Backend demand multiplies it by
    **[*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) real GDP(calendar year) / real GDP(2023)**. The active legacy `growth_rate_type='gdp'` selects
    workbook `gdp`, whose formulas are **column C / C$2**: GDP-deflator levels normalized to 2023.
    This interpretation is checked against actual formulas and the registered [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) macro source values,
    rather than inferred from a column name or previous report.

    Specific contradiction: the compiler describes national GDP activity growth, yet calendar 2030 uses
    **144.1 / 123 = 1.171545**, the GDP deflator. Real GDP is **2,652,642 / 2,385,443 = 1.112012**.
    The earlier deflator characterization is confirmed; the economic description in the legacy notes is wrong.
    The comparison preserves that executable price-index recipe without replacing its assumption.

    Legacy `project_demand_v` generates calendar 2025–2050, then `shift_time_period` subtracts five and
    retains model labels 2025–2045. Thus **calendar 2025 → label 2020 is dropped**; first emitted
    **label 2025 → calendar 2030**. Backend prospective demand uses the same interval-end calendar axis.
    The earlier axis starting at 2030 hid the common origin; the first visible points were already
    **seven years past the shared 2023 observation**. Their difference was real growth divergence,
    not different observed starting demand. Both curves now show **black crosses at the 2023 anchor**,
    **open diamonds at the contextual 2025 growth targets**, and circles at emitted demands from 2030 onward.
    The 2025 diamonds are explanatory reconstructions, not additional emitted model periods.
    The growth chart likewise starts at **2023 = 1**, rather than resetting each method to 1 in 2030.

    **Cars example (billion passenger-km):** both start at **{car_context.loc[(BUILDS[0], 2023)]:.4f} in 2023**. In 2025, backend real GDP
    gives **{car_context.loc[(BUILDS[0], 2025)]:.4f}** and the legacy deflator recipe gives **{car_context.loc[(BUILDS[1], 2025)]:.4f}**. At the first emitted calendar year,
    2030, they are **{car_context.loc[(BUILDS[0], 2030)]:.4f}** and **{car_context.loc[(BUILDS[1], 2030)]:.4f}**, respectively. No growth assumption has been changed.
    """), display_chart(demand_chart), display_chart(demand_growth_chart),
        display_table(matched.loc[matched.road_class.eq("cars") & matched.calendar_year.isin([2023, 2025, 2030]),
                                  ["Build", "calendar_year", "demand_bn", "Status"]].rename(columns={
            "calendar_year": "Calendar year", "demand_bn": "Car demand (billion passenger-km)", "Status": "Evidence"}),
            selection=None, pagination=False, label="Shared car-demand origin and the first emitted year; 2025 remains context only"),
        display_table(attribution[["road_class", "period", "current_source_year", "notebook_recipe_demand_bn", "current", "replay_relative_percent"]].rename(columns={
            "road_class": "Class", "period": "Model label", "current_source_year": "Calendar year", "notebook_recipe_demand_bn": "Legacy variant (bn)",
            "current": "v2.0 backend (bn)", "replay_relative_percent": "Backend / legacy − 1 (%)",
        }), selection=None, page_size=10),
        mo.accordion({"Formula, source-value and year audit": display_table(diagnosis["fleet"]["demand_growth_series_audit"], selection=None, page_size=10),
            "Observation, calibration and 2025 demand targets": display_table(diagnosis["fleet"]["demand_baseline_service_targets"], selection=None, pagination=False),
            "Full trajectories, stored SQLite discrepancy and ordered attribution": mo.vstack([
                display_table(methods, selection=None, page_size=10), display_table(attribution, selection=None, page_size=10),
                display_table(diagnosis["fleet"]["demand_method_context"], selection=None, pagination=False),
            ])})])
    return demand_chart, demand_growth_chart


@app.cell(hide_code=True)
def _(diagnosis, display_table, mo, region):
    backend_costs = diagnosis["charging"]["backend_cost_basis"]
    backend_costs = backend_costs.loc[backend_costs.region.eq(region.value)].copy()
    backend_costs["Class"] = backend_costs.charger_class.map({"ldv": "LDV", "mhdv": "MHDV"})
    backend_costs["Rate basis"] = backend_costs.status.map({
        "existing_implied_capex_from_fixed_cost": "Existing: fixed cost / ratio",
        "new_investment_rate": "New investment",
        "existing_capex_unavailable_zero_fixed_ratio": "Existing: capex unavailable",
    })
    cost_components = diagnosis["charging"]["source_components"].copy()
    cost_components["Class"] = cost_components.charger_class.map({"ldv": "LDV", "mhdv": "MHDV"})
    cost_components["Currency/year"] = cost_components.source_currency + cost_components.source_dollar_year.astype(str)
    mo.vstack([
        mo.md("""
        ## 5. Charger costs: power, throughput and source year

        Costs now start from **native capex per port and rated kW for the same source year**.
        Convert each component to CAD2020, then calculate
        **`M CAD2020/GW = Σ(count share × CAD2020/port) / Σ(count share × kW/port)`**.
        CAD/kW and million CAD/GW are numerically equal. This conserves total expenditure and
        total rated power; it replaces the former count-weighted average of normalized rates.

        The first new vintage, **model label 2025 → cost source year 2030**, uses that year's
        matched costs and powers. Existing implied capex uses the first-model-year cost proxy;
        it explains fixed costs and is not charged again as investment in existing infrastructure.
        Fixed O&M is **1% of the corrected capital rate per year**, within the infrastructure lifetime.

        Both existing/new LDV and MHDV annual utilization targets are **20%**. Thus the
        throughput denominator is **`31.536 × 0.2 = 6.3072 PJ/year per GW`** for both classes.
        The table separates capital cost per rated GW from cost per annual energy throughput.
        Profiled LDV keeps its hourly shape and the configured annual-floor slack.

        Higher-power equipment can cost more per port and less per kW. In the
        [*Dunsky*/*NRCan Ratings* source](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada),
        Table 21's calendar-2025 MHDV ports cost **78,637 / 234,129 / 645,224 CAD2022**
        at **50 / 350 / 2,000 kW**, or **1,572.74 / 668.94 / 322.61 CAD2022/kW**.
        LDV L2 is **15,000 CAD2022 / 7.2 kW ≈ 2,083.33 CAD2022/kW** (Table 12).

        Power shares are derived as **`count share × rated kW / Σ(count share × rated kW)`**.
        Weighting each component's CAD/kW rate by these shares gives the same aggregate.
        The component table retains native amounts, rating trajectories and both kinds of share;
        the report independently reconstructs the rates and checks them against SQLite.

        **Framework comparison:** [HDVCHARGE v1.2](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/#calculate-number-of-required-chargers)
        explicitly excludes costs from its calculations and outputs. Separately,
        [ICCT's 2025 facility-cost study, Table 5](https://theicct.org/wp-content/uploads/2025/10/ID-408-%E2%80%93-Energizing-trucks_technical-brief_final.pdf)
        finds lower behind-the-meter cost per MW at its largest prototype despite higher cost per port.
        This supports the direction of the scale effect; its US sites, 2025 prices and grid boundary
        do not independently validate Canadian model rates.
        """),
        display_table(backend_costs[[
            "Class", "vintage", "source_year", "Rate basis", "cost_mcad_per_gw", "utilization",
            "throughput_pj_year_per_gw", "cost_mcad_per_pj_year",
        ]].rename(columns={
            "vintage": "Model vintage", "source_year": "Cost source year",
            "cost_mcad_per_gw": "M CAD2020/GW", "utilization": "Utilization",
            "throughput_pj_year_per_gw": "PJ/year per GW",
            "cost_mcad_per_pj_year": "M CAD2020 per PJ/year",
        }), selection=None, pagination=False, label="One row per charger class and vintage; existing rates are implied capex proxies"),
        display_table(cost_components[[
            "Class", "source_year", "charger_type", "source_cost_per_port", "Currency/year",
            "rated_power_kw", "count_share", "power_share", "weighted_cost_per_gw",
        ]].rename(columns={
            "source_year": "Source year", "charger_type": "Charger type",
            "source_cost_per_port": "Native capex/port",
            "rated_power_kw": "Rated kW/port", "count_share": "Port-count share",
            "power_share": "Rated-power share", "weighted_cost_per_gw": "Contribution (M CAD2020/GW)",
        }), selection=None, page_size=12, format_mapping={
            "Port-count share": "{:.2%}", "Rated-power share": "{:.2%}",
        }, label="One row per class, cost source year and charger type; contributions sum to the capital rate above"),
    ])
    return


@app.cell(hide_code=True)
def _(alt, diagnosis, display_chart, mo, pd, display_table, identified_chart):
    fixed_comparison = diagnosis["charging"]["harmonized_costs"]
    fixed_comparison = fixed_comparison.loc[fixed_comparison.table.eq("cost_fixed")].copy()
    fixed_comparison["Class"] = fixed_comparison.charger_class.map({"ldv": "LDV", "mhdv": "MHDV"})
    fixed_comparison[["reference_vintage", "reference_period", "backend_vintage"]] = fixed_comparison[[
        "reference_vintage", "reference_period", "backend_vintage",
    ]].astype("Int64")
    harmonized = diagnosis["charging"]["harmonized_costs"]
    harmonized = harmonized.loc[harmonized.table.eq("cost_invest")].copy()
    harmonized["Class"] = harmonized.charger_class.map({"ldv": "Light-duty charging", "mhdv": "Medium/heavy-duty charging"})
    rates = harmonized.melt(
        id_vars=["Class", "reference_vintage", "backend_vintage", "scope_status"],
        value_vars=["backend_million_cad_per_gw", "reference_million_cad_per_gw"], var_name="Evidence", value_name="Cost",
    )
    rates["Evidence"] = rates.Evidence.map({
        "backend_million_cad_per_gw": "v2.0 backend", "reference_million_cad_per_gw": "Legacy charger variant (backend UF)",
    })
    charging_chart = alt.Chart(rates).mark_line(point=True).encode(
        x=alt.X("reference_vintage:Q", title="Cost source calendar year", axis=alt.Axis(format="d", tickCount=6)),
        y=alt.Y("Cost:Q", title="Million CAD2020/GW", scale=alt.Scale(zero=True)),
        color=alt.Color("Evidence:N", scale=alt.Scale(domain=["v2.0 backend", "Legacy charger variant (backend UF)"], range=["#2166ac", "#b35806"]),
                        legend=alt.Legend(orient="bottom", title=None, labelLimit=280, symbolType="stroke", symbolSize=300)),
        strokeDash=alt.StrokeDash("Evidence:N", scale=alt.Scale(domain=["v2.0 backend", "Legacy charger variant (backend UF)"], range=[[1, 0], [7, 3]]),
                                  legend=alt.Legend(orient="bottom", title=None, labelLimit=280, symbolType="stroke", symbolSize=300)),
        tooltip=["Class:N", "Evidence:N", "reference_vintage:O", "backend_vintage:O", "scope_status:N", alt.Tooltip("Cost:Q", format=",.2f")],
    ).properties(width=430, height=210).facet(column=alt.Column("Class:N", title=None))
    charging_residual_chart = alt.Chart(harmonized).mark_bar(size=24).encode(
        x=alt.X("reference_vintage:O", title="Cost source calendar year"),
        y=alt.Y("harmonized_relative_percent:Q", title="Backend / reference − 1 (%)"), color=alt.value("#2166ac"),
        tooltip=["reference_vintage:O", alt.Tooltip("harmonized_relative_percent:Q", format="+.1f")],
    ).properties(width=430, height=110).facet(column=alt.Column("Class:N", title=None))
    charging_chart = identified_chart(charging_chart, "comparison-charging",
                                      "Charger costs on a common throughput and monetary basis")
    charging_residual_chart = identified_chart(charging_residual_chart, "comparison-charging-residual",
                                               "Charger cost differences after explicit normalization")
    mo.vstack([
        mo.md("""
        ### The separate legacy charger-cost variant

        **Yes—the legacy line uses the supplied charger-cost variant inputs:** `CostInvest.csv`,
        `CostFixed.csv`, and `CostVariable_ALL.csv` in `legacy_backend/legacy_vatiant_charger_costs/`
        (the directory's spelling is retained). This is a **different reference from the sales-cohort
        variant in Sections 1–4**. Its investment and fixed-cost CSV rows govern this comparison;
        the variable-cost CSV contains no mapped charger entries, which does not imply zero operating cost.
        `canoe_ON_16d.sqlite` is not this section's cost source. The original `canoe_on_12d_baseline.sqlite`
        supplies non-cost context only; it does not establish the charger CSV variant's embedded utilization.

        **Blue = current backend; dashed orange = legacy charger-cost CSV rates converted at backend UF.**
        The table exposes the original CSV rate, the throughput conversion factor and the resulting per-GW rate.
        Both rates use **million CAD2020/GW**. Convert reference million CAD2020/(PJ/year)
        using **reference rate × c2a × backend utilization**. Here c2a = 31.536 PJ/year
        per GW; existing/new charger utilization is 20% for both LDV and MHDV. This is equivalent to
        dividing backend per-GW cost by utilized throughput before comparison.

        The common-utilization interpretation remains conditional: reference embedded
        utilization and losses are undocumented. No extra loss multiplier is imposed.
        Source years match explicitly; reference 2025 has no backend cost-year bridge.

        **LDV:** reference costs mix US home L2, public L2 and DCFC equally; backend
        L1/L2/DCFC count shares are 12/87/1%, with mostly Canadian inputs. **MHDV:**
        reference 50/150/350 kW includes transmission upgrades; backend 50/350/2000 kW
        uses shares 82/16/2%. Equipment mixture and infrastructure boundaries differ;
        their individual effects are not isolated. Fixed O&M is 1% of investment
        annually on both sides, repeating its relative gap. Equal BEV/PHEV or MDV/HDV
        reference rates appear once after verification, without summing duplicate rates.
        """), display_chart(charging_chart), display_chart(charging_residual_chart),
        display_table(harmonized[[
            "Class", "reference_vintage", "reference_million_target_cad", "backend_denominator_per_gw", "reference_million_cad_per_gw",
            "backend_million_cad_per_gw", "harmonized_relative_percent", "scope_status",
        ]].rename(columns={
            "reference_vintage": "Source year", "reference_million_target_cad": "Legacy CSV (M CAD2020 per PJ/year)",
            "backend_denominator_per_gw": "c2a × backend UF (PJ/year per GW)",
            "reference_million_cad_per_gw": "Legacy converted (M CAD2020/GW)", "backend_million_cad_per_gw": "Backend (M CAD2020/GW)",
            "harmonized_relative_percent": "Difference (%)", "scope_status": "Coverage",
        }), selection=None, pagination=False, format_mapping={"Difference (%)": "{:+.4f}%"}, label="All years, including missing backend coverage"),
        mo.accordion({
            "Annual fixed O&M comparison by cohort and active period": display_table(fixed_comparison[[
                "Class", "reference_vintage", "reference_period", "backend_vintage",
                "reference_million_cad_per_gw", "backend_million_cad_per_gw", "harmonized_relative_percent", "scope_status",
            ]].rename(columns={
                "reference_vintage": "Legacy vintage", "reference_period": "Active model period",
                "backend_vintage": "Backend vintage", "reference_million_cad_per_gw": "Legacy (M CAD2020/GW/year)",
                "backend_million_cad_per_gw": "Backend (M CAD2020/GW/year)",
                "harmonized_relative_percent": "Difference (%)", "scope_status": "Coverage",
            }), selection=None, page_size=10, format_mapping={"Difference (%)": "{:+.4f}%"},
                label="One row per class, reference cohort and active period; existing cohorts use the historical rate bridge"),
            "Separate legacy charger-cost CSV identities": display_table(pd.DataFrame([
                {"Input file": record["path"].replace("\\", "/").rsplit("/", 1)[-1],
                 "Charger rows": record["selected_rows"], "Total CSV rows": record["total_rows"],
                 "SHA-256": record["sha256"]}
                for record in diagnosis["charging_metadata"]["reference_files"].values()
            ]), selection=None, pagination=False, label="Captured identities of the separate supplied charger-cost variant"),
            "Equipment components, currency conversion and weights": display_table(diagnosis["charging"]["source_components"], selection=None, page_size=10),
            "Fixed ratios and variable-cost coverage": mo.vstack([
                display_table(diagnosis["charging"]["fixed_ratios"], selection=None, page_size=8),
                display_table(diagnosis["charging"]["variable_coverage"], selection=None, pagination=False),
            ]),
            "Inherited context and unit evidence": mo.vstack([
                display_table(diagnosis["charging"]["reference_inherited_context"], selection=None, page_size=8),
                display_table(diagnosis["charging"]["reference_rows"], selection=None, page_size=8),
            ]),
        }),
    ])
    return charging_chart, charging_residual_chart


@app.cell(hide_code=True)
def _(diagnosis, display_table, mo, pd, region):
    charger_transfers = diagnosis["charging"]["backend_efficiency"]
    charger_transfers = charger_transfers.loc[charger_transfers.region.eq(region.value)].copy()
    if charger_transfers.empty or not charger_transfers.efficiency.eq(1.0).all():
        raise ValueError("Charging capture lacks the current unity electricity transfers; rebuild and recapture")
    transfer_summary = charger_transfers.groupby(
        ["tech", "input_comm", "output_comm", "efficiency"], as_index=False,
    ).agg(first_vintage=("vintage", "min"), last_vintage=("vintage", "max"), rows=("vintage", "count"))
    charging_accounting = diagnosis["charging_metadata"]["electricity_accounting"]
    mo.vstack([
        mo.md("""
        ## 6. Charging losses: vehicle accounting and proxy limits

        **The former 0.95 LDV and 0.80 MHDV factors applied charging losses twice.**
        [*NLR ATB* BEV assumptions](https://atb.nlr.gov/transportation/2024/battery_electric_vehicle_assumptions)
        include onboard-charger and EVSE losses in fuel economy. The
        [LDV PHEV](https://atb.nlr.gov/transportation/2024/plug-in_hybrid) and
        [MHDV PHEV](https://atb.nlr.gov/transportation/2024/plug-in_hybrid_mdhd)
        assumptions likewise adjust electric consumption for charging losses. The production
        consumption/ratio calculations preserve that source boundary.

        Vehicle efficiencies are retained. Existing/new charger technologies now transfer electricity
        at **1.0 PJ/PJ**, preserving the capacity, utilization and cost representation while applying
        no additional loss. Explicit transfer rows are required for the electricity route.
        This is an accounting factor, not a claim of physically lossless charging. Relative to the
        former chain, electricity required for the same downstream activity falls by **5% for LDV**
        and **20% for MHDV**, before other system effects; these are arithmetic changes, not solver results.

        HDVCHARGE's global 0.80 includes equipment, onboard charging where applicable, and battery losses;
        it cannot be stacked on consumption that already includes those losses. Its power-delivery ratio
        describes session power, a separate concept. The reviewed ICCT factors remain unapplied audit
        evidence. Unity rows use an internal accounting dataset, without an external DQ claim.
        **Motorcycles:** their consumption does not include charging losses. With the shared unity
        LDV charger transfer, motorcycle charging losses are currently unrepresented.
        **Intercity buses:** EPRI does not disclose whether its consumption includes charging losses;
        that boundary remains unknown. Neither exception establishes physically lossless charging.
        """),
        display_table(transfer_summary.rename(columns={
            "tech": "Charger technology", "input_comm": "Input electricity", "output_comm": "Output electricity",
            "efficiency": "Transfer (PJ/PJ)", "first_vintage": "First vintage", "last_vintage": "Last vintage",
            "rows": "Vintage rows",
        }), selection=None, pagination=False, label="Current SQLite electricity transfers by technology in the selected region"),
        mo.accordion({
            "Reviewed factors retained but not applied": display_table(
                pd.DataFrame(charging_accounting["reviewed_efficiencies_not_applied"]),
                selection=None, pagination=False),
            "Accounting boundary and complete SQLite row evidence": mo.vstack([
                mo.json(charging_accounting), display_table(charger_transfers, selection=None, page_size=10),
            ]),
        }),
    ])
    return


@app.cell(hide_code=True)
def _(diagnosis, mo):
    mo.accordion({"Captured identities and reproducibility": mo.vstack([
        mo.md("CSV hashes, the live validation YAML and both reports' database/build fingerprints are checked before display. Reproduce with the commands at the top. Workbook reconstruction does not establish historical notebook execution or optimization."),
        mo.json({
            "fleet_scenario": diagnosis["fleet_metadata"]["scenario"], "fleet_revision": diagnosis["fleet_metadata"]["revision"],
            "charging_scenario": diagnosis["charging_metadata"]["scenario"], "charging_build_utc": diagnosis["charging_metadata"]["build_timestamp_utc"],
            "limitations": diagnosis["fleet_metadata"]["limitations"], "commands": diagnosis["fleet_metadata"]["commands"],
            "legacy_reconstruction": diagnosis["fleet_metadata"]["legacy_reconstruction"],
        }),
    ])})
    return


if __name__ == "__main__":
    app.run()
