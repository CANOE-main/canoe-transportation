"""Independent 2025 stock, activity–energy and demand comparison."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd
    from validation.transportation_research import load_validation_diagnosis

    CLASS_LABELS = {
        "cars": "Cars", "passenger_light_trucks": "Passenger light trucks",
        "freight_light_trucks": "Freight light trucks", "medium_trucks": "Medium trucks",
        "heavy_trucks": "Heavy trucks",
    }
    STOCK_LABELS = {**CLASS_LABELS, "passenger_light_trucks": "Light trucks", "freight_light_trucks": "Light trucks"}
    STOCK_ORDER = ["Cars", "Light trucks", "Medium trucks", "Heavy trucks"]
    BUILDS = ["Current backend", "Legacy recipe"]
    CASES = ["Backend before calibration", "Current backend", "Legacy recipe", "Backend energy sensitivity"]
    CASE_COLORS = ["#999999", "#2166ac", "#7b3294", "#b35806"]
    CASE_SHAPES = ["circle", "square", "triangle-up", "diamond"]

    def stock_grid(chart, paired_scales=False):
        if paired_scales:
            rows = [chart.transform_filter(alt.FieldOneOfPredicate(field="Class", oneOf=names)).properties(
                width=370, height=215).facet(column=alt.Column("Class:N", sort=names, title=None)).resolve_scale(y="shared")
                for names in [STOCK_ORDER[:2], STOCK_ORDER[2:]]]
            return alt.vconcat(*rows, spacing=25).resolve_scale(y="independent")
        return chart.properties(width=370, height=190).facet(
            facet=alt.Facet("Class:N", sort=STOCK_ORDER, title=None), columns=2,
        ).resolve_scale(x="shared", y="independent")

    def class_grid(chart):
        labels = [label + (" (billion passenger-km)" if name in {"cars", "passenger_light_trucks"}
                           else " (billion tonne-km)") for name, label in CLASS_LABELS.items()]
        grid = chart.properties(width=290, height=210).facet(
            facet=alt.Facet("Panel:N", sort=labels, title=None, header=alt.Header(labelFontSize=11)), columns=3,
        ).resolve_scale(x="independent", y="independent")
        return grid.transform_calculate(Panel=(
            "datum.Class + (indexof(['Cars', 'Passenger light trucks'], datum.Class) >= 0"
            " ? ' (billion passenger-km)' : ' (billion tonne-km)')"))

    def display_chart(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    return BUILDS, CASES, CASE_COLORS, CASE_SHAPES, CLASS_LABELS, STOCK_LABELS, STOCK_ORDER, alt, class_grid, display_chart, load_validation_diagnosis, mo, pd, stock_grid


@app.cell
def _(load_validation_diagnosis):
    diagnosis = load_validation_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    scenario = diagnosis["fleet_metadata"]["scenario"]
    mo.vstack([mo.md(f"""
    # Transportation validation: independent methods

    **2025 evaluation**, shared **CEUD 2023 edition**; observed stock, activity and energy are held
    constant through 2025. Backend stock is normalized to CEUD; legacy stock is independently reconstructed.
    CEUD observation year, UF calibration year, evaluation year and model labels are different concepts.
    Prospective **model period 2025 represents demand calendar year 2030**; baseline accounting here is calendar 2025.
    Scenario: [{scenario['scenario']['name']}](../../{diagnosis['scenario_path']}).

    Primary legacy reference: the supplied notebook's **active `fixed_lifetime=False` recipe** and workbooks.
    The supplied `canoe_ON_16d.sqlite` does not reproduce its capacity output; it remains discrepancy evidence.
    It is also the explicitly selected **inherited-parameter snapshot**, substituting for missing
    `Input data/canoe_ALL_cm.sqlite`. Legacy efficiency-dependent energy is conditional on that substitution;
    it does not establish the original input's historical identity. Reference files remain immutable.
    No efficiencies, costs, charging assumptions or scientific calibration are changed by this comparison.
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
def _(STOCK_LABELS, alt, diagnosis, display_chart, mo, region, stock_grid):
    annual = diagnosis["fleet"]["sales_cohort_reconciliation"]
    annual = annual.loc[annual.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    annual_stock = annual.groupby(["Class", "vintage_year"], as_index=False)[[
        "surviving_k_vehicles", "retired_k_vehicles", "calibrated_surviving_k_vehicles", "inserted_cohort_k_vehicles",
    ]].sum()
    portions = annual_stock.melt(id_vars=["Class", "vintage_year"], var_name="Component", value_name="Vehicles")
    portions["Kind"] = portions.Component.isin(["surviving_k_vehicles", "retired_k_vehicles"]).map({True: "Sales", False: "Model"})
    portions["Component"] = portions.Component.map({
        "surviving_k_vehicles": "Raw surviving sales", "retired_k_vehicles": "Cumulative retirements",
        "calibrated_surviving_k_vehicles": "After stock scaling", "inserted_cohort_k_vehicles": "After fixed-lifetime allocation",
    })
    cohort_legend = ["Raw surviving sales", "Cumulative retirements", "After stock scaling", "After fixed-lifetime allocation"]
    cohort_colors = ["#4477aa", "#d9d9d9", "#cc8844", "#222222"]
    retirement_bars = alt.Chart().transform_filter(alt.datum.Kind == "Sales").mark_bar().encode(
        x=alt.X("vintage_year:O", title="Sales calendar year", axis=alt.Axis(labelAngle=0, labelExpr="toNumber(datum.label) % 5 == 0 ? datum.label : ''")),
        y=alt.Y("Vehicles:Q", title="Thousand vehicles", stack="zero"),
        color=alt.Color("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=cohort_colors),
                        legend=alt.Legend(orient="bottom", labelLimit=280, columns=2, symbolType="stroke", symbolSize=350)),
        order=alt.Order("Component:N", sort="descending"), tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    allocated_line = alt.Chart().transform_filter(alt.datum.Kind == "Model").mark_line(strokeWidth=1.8).encode(
        x="vintage_year:O", y=alt.Y("Vehicles:Q", stack=None), order="vintage_year:O",
        color=alt.Color("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=cohort_colors)),
        strokeDash=alt.StrokeDash("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=[[1, 0], [1, 0], [1, 0], [5, 3]]),
                                  legend=alt.Legend(orient="bottom", labelLimit=280, columns=2, symbolType="stroke", symbolSize=350)),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    retirement_chart = stock_grid(alt.layer(retirement_bars, allocated_line, data=portions))
    cutoffs = diagnosis["fleet"]["backend_lifetime_exclusions"]
    cutoffs = cutoffs.loc[cutoffs.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    mo.vstack([mo.md("""
    ## 1. Stock and cohort lineage

    **Backend only:** blue + grey reconstruct raw sales; blue is sales × cumulative annual survival at 2025.
    Orange scales those survivors by **CEUD stock / raw surviving stock**. Dashed allocation then redistributes
    excluded stock proportionally **within each technology**, preserving its total. CEUD equality is a calibration identity.

    Eligibility requires both `annual year + lifetime > 2025` and `model vintage + lifetime > 2025`.
    Lifetimes are **16 years (cars), 17 (light trucks), 20 (medium/heavy)**. Vintage 2005 represents 2006–2010:
    even `2005 + 20 = 2025` fails the strict endpoint. Vintage 2010 represents 2011–2015 and passes all three.
    That explains the common **2011 cutoff**. Annual-only cutoffs would be 2010 / 2009 / 2006 respectively.
    This is an effect of the current fixed-lifetime representation, not missing aggregation bins.
    """), display_chart(retirement_chart), mo.ui.table(cutoffs[[
        "Class", "fixed_lifetime_years", "all_excluded_k_vehicles", "total_redistributed_percent",
        "coarse_vintage_exclusion_k_vehicles", "coarse_exclusion_percent",
    ]].rename(columns={"fixed_lifetime_years": "Lifetime (years)", "all_excluded_k_vehicles": "All redistributed (k)",
        "total_redistributed_percent": "All / CEUD (%)", "coarse_vintage_exclusion_k_vehicles": "Extra from model-vintage test (k)",
        "coarse_exclusion_percent": "Extra / CEUD (%)"}), selection=None, pagination=False,
        label="Separate service classes retained for the exclusion audit; light-truck stock plots combine them"),
        mo.accordion({"Raw → scaled → allocated class totals and explanations": mo.ui.table(
            diagnosis["fleet"]["fleet_reconciliation"], selection=None, page_size=5)})])
    return (retirement_chart,)


@app.cell(hide_code=True)
def _(BUILDS, STOCK_LABELS, alt, diagnosis, display_chart, mo, pd, region, stock_grid):
    vintages = diagnosis["fleet"]["comparison_vintage_stock"]
    vintages = vintages.loc[vintages.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    vintage_stocks = vintages.groupby(["Class", "evidence", "vintage"], as_index=False).surviving_k_vehicles.sum()
    vintage_stocks["Kind"] = "Stock"
    totals = diagnosis["fleet"]["comparison_stock_totals"]
    totals = totals.loc[totals.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    stock_totals = totals.groupby(["Class", "evidence"], as_index=False)[["surviving_k_vehicles", "ceud_stock_k_vehicles"]].sum()
    stock_totals["Difference (%)"] = 100 * (stock_totals.surviving_k_vehicles / stock_totals.ceud_stock_k_vehicles - 1)
    targets = stock_totals.rename(columns={"ceud_stock_k_vehicles": "Target"}).assign(Kind="Target")
    stock_data = pd.concat([vintage_stocks, targets], ignore_index=True)
    stock_bars = alt.Chart().transform_filter(alt.datum.Kind == "Stock").mark_bar(size=58).encode(
        x=alt.X("evidence:N", sort=BUILDS, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("surviving_k_vehicles:Q", title="Surviving stock at 2025 (thousand vehicles)", stack="zero"),
        color=alt.Color("vintage:O", title="Model vintage", scale=alt.Scale(scheme="blues"), legend=alt.Legend(orient="bottom")),
        order="vintage:O", tooltip=["Class:N", "evidence:N", "vintage:O", alt.Tooltip("surviving_k_vehicles:Q", format=",.3f")],
    )
    stock_target = alt.Chart().transform_filter(alt.datum.Kind == "Target").mark_point(
        shape="cross", color="#111111", filled=False, size=180, strokeWidth=2.5).encode(
        x=alt.X("evidence:N", sort=BUILDS), y=alt.Y("Target:Q", title="Surviving stock at 2025 (thousand vehicles)"),
        tooltip=["Class:N", alt.Tooltip("Target:Q", title="CEUD 2023 stock held to 2025 (k)", format=",.3f")])
    stock_comparison_chart = stock_grid(alt.layer(stock_bars, stock_target, data=stock_data), paired_scales=True)
    mo.vstack([mo.md("""
    ### Surviving stock: independent 2025 parameterizations

    One bar per build, stacked by model vintage. **Black crosses = CEUD 2023 stock held through 2025**.
    Cars/light trucks share one vertical scale; medium/heavy share another. Backend capacity already contains
    annual survival, stock normalization and eligibility allocation: no second retirement is applied.
    Legacy exported ExistingCapacity is **raw acquisitions**, not surviving stock. Its own rounded workbook
    survival is applied once **after aggregation**, at `2025 − representative vintage`; no CEUD scaling is imposed.
    """), display_chart(stock_comparison_chart), mo.ui.table(stock_totals.rename(columns={
        "evidence": "Build", "surviving_k_vehicles": "Surviving 2025 stock (k)", "ceud_stock_k_vehicles": "CEUD target (k)",
    }), selection=None, pagination=False, format_mapping={"Difference (%)": "{:+.2f}%"}),
        mo.accordion({"Disputed supplied SQLite versus raw notebook export": mo.vstack([
            mo.md("Raw export is compared with raw stored capacity here, separately from the surviving-stock comparison. No matching branch is inferred from filenames or saved messages."),
            mo.ui.table(diagnosis["fleet"]["reference_recipe_totals"], selection=None, pagination=False),
            mo.ui.table(diagnosis["fleet"]["legacy_reconstruction_context"], selection=None, pagination=False),
        ])})])
    return (stock_comparison_chart,)


@app.cell(hide_code=True)
def _(BUILDS, STOCK_LABELS, alt, diagnosis, display_chart, mo, region, stock_grid):
    lineage = diagnosis["fleet"]["comparison_annual_stock"]
    lineage = lineage.loc[lineage.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(STOCK_LABELS))
    contributions = lineage.groupby(["Class", "evidence", "vintage", "vintage_year", "annual_position", "cohort_position", "interval"], as_index=False).surviving_k_vehicles.sum()
    positions = ["Year 1", "Year 2", "Year 3", "Year 4", "Year 5"]
    vintage_chart = stock_grid(alt.Chart(contributions).mark_bar(size=20).encode(
        x=alt.X("vintage:O", title="Model vintage · left backend / right legacy", axis=alt.Axis(labelAngle=0)),
        xOffset=alt.XOffset("evidence:N", sort=BUILDS, scale=alt.Scale(domain=BUILDS)),
        y=alt.Y("surviving_k_vehicles:Q", title="Surviving stock at 2025 (thousand)", stack="zero"),
        color=alt.Color("cohort_position:N", title="5-year cohorts", scale=alt.Scale(domain=positions,
            range=["#4477aa", "#66ccee", "#228833", "#ddcc77", "#cc6677"]), legend=alt.Legend(orient="bottom", columns=5)),
        order=alt.Order("annual_position:Q"), tooltip=["Class:N", "evidence:N", "vintage:O", "vintage_year:O", "cohort_position:N", "interval:N",
            alt.Tooltip("surviving_k_vehicles:Q", format=",.3f")],
    ))
    mo.vstack([mo.md("""
    ### Annual contributions inside each model vintage

    Left **current backend**, right **legacy recipe**; sum of annual segments equals the same vintage and class
    capacities above. Positions mean `vintage + 1 … vintage + 5`. Exact years are in tooltips.
    **Partial bins are summed, never discarded**: available 2013–2015 all contribute to vintage 2010.
    Aggregation precedes the separate eligibility screen. The legacy's oldest bin contains only **2005 → 2000**;
    the backend oldest proxy includes **2000–2005**, an extra lower endpoint (zero surviving allocated stock here).
    Hover identifies any such boundary exception rather than treating it as a regular five-year interval.

    Legacy annual colours carry **one representative-vintage survival fraction shared by the bin**.
    They do not imply annual-age survival. Backend lineage retains its own annual survival, CEUD multiplier
    and technology-specific redistribution. Missing annual SQLite detail does not prevent workbook reconstruction.
    """), display_chart(vintage_chart), mo.accordion({"Full annual lineage": mo.ui.table(lineage, selection=None, page_size=10)})])
    return (vintage_chart,)


@app.cell(hide_code=True)
def _(CASES, CASE_COLORS, CASE_SHAPES, CLASS_LABELS, alt, diagnosis, display_chart, mo, region):
    cases = diagnosis["fleet"]["baseline_activity_energy_cases"]
    cases = cases.loc[cases.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
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
    utilization_chart = alt.layer(utilization_bounds, utilization_points).properties(width=800, height=220)
    calibration = diagnosis["fleet"]["backend_utilization_bridge"]
    calibration = calibration.loc[calibration.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map(CLASS_LABELS))
    mo.vstack([mo.md("""
    ## 2. Utilization: calibration values, bounds and sensitivity

    Grey = arithmetic mean of `activity_y / (stock_y × c2a)` for observed **2017, 2018, 2019, 2022, 2023**;
    2020–2021 are excluded. Blue = **2023 CEUD class activity held at 2025 / Σ(inserted stock × c2a)**.
    The table exposes target, denominator and resulting change. Reviewed manual c2a and efficiency stay fixed.
    This is historical activity calibration, separate from GDP-indexed service demand.

    Purple triangles = **native legacy central calibration**, with a line showing its **±2.5% parameter bounds**.
    Its denominator is raw model-vintage capacity × workbook survival at **2023** × own c2a.
    It already includes the 2024–2025 extended sales in vintage 2020; it is a recipe calibration identity,
    not an observation of calendar-2023 acquisitions. Bounds are constraints, not realized solver operation.

    Orange = **unimplemented backend energy sensitivity**: blue UF × CEUD energy / backend energy.
    Only UF changes; stock, composition, efficiencies and c2a are held fixed. The native legacy point uses
    its own complete parameters and is independent of that sensitivity.
    """), display_chart(utilization_chart), mo.ui.table(calibration[[
        "Class", "source_years", "ceud_activity_bn", "stock_denominator_k_vehicles", "c2a",
        "capacity_activity_denominator_bn", "prior_mean_utilization", "implemented_utilization", "calibration_percent",
    ]].rename(columns={"ceud_activity_bn": "2023 activity held at 2025 (bn)", "stock_denominator_k_vehicles": "Backend stock (k)",
        "capacity_activity_denominator_bn": "Σ stock × c2a (bn)", "prior_mean_utilization": "Grey UF", "implemented_utilization": "Blue UF",
        "calibration_percent": "UF change (%)", "source_years": "Grey source years"}), selection=None, pagination=False,
        label="c2a: billion passenger-km or tonne-km per thousand vehicles; UF = target / capacity-activity denominator"),
        mo.accordion({"Annual grey inputs and native legacy denominator": mo.vstack([
            mo.ui.table(diagnosis["fleet"]["backend_utilization_source_years"], selection=None, page_size=10),
            mo.ui.table(legacy_bounds, selection=None, pagination=False),
        ])})])
    return calibration, cases, utilization_chart


@app.cell(hide_code=True)
def _(CASES, CASE_COLORS, CASE_SHAPES, alt, calibration, cases, class_grid, display_chart, mo, pd):
    target_rows = cases.loc[cases.case.eq("Current backend")].copy()
    references = []
    for label, column in [("CEUD 2023 held at 2025", "ceud_activity_bn"),
                           ("Backend real-GDP 2025 target", "current_base_year_demand_bn"),
                           ("Legacy workbook 2025 target", "legacy_calendar_base_year_demand_bn")]:
        references.append(target_rows.assign(Reference=label, Service=lambda frame: frame[column],
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
    activity_energy_chart = class_grid(alt.Chart(reconciliation_data).mark_point(filled=True, size=120, strokeWidth=1.8).encode(
        x=alt.X("Energy:Q", title="2025 vehicle-input energy (PJ)", scale=alt.Scale(zero=True)),
        y=alt.Y("Service:Q", title="2025 service (billion)", scale=alt.Scale(zero=True)),
        color=alt.Color("Point:N", scale=alt.Scale(domain=point_order, range=point_colors), title=None,
                        legend=alt.Legend(orient="bottom", columns=2, labelLimit=320)),
        shape=alt.Shape("Point:N", scale=alt.Scale(domain=point_order, range=point_shapes), title=None,
                        legend=alt.Legend(orient="bottom", columns=2, labelLimit=320)),
        stroke=alt.Stroke("Point:N", scale=alt.Scale(domain=point_order, range=point_colors), legend=None),
        tooltip=["Class:N", "Point:N", alt.Tooltip("utilization:Q", format=".6f"),
                 alt.Tooltip("Energy:Q", format=",.3f"), alt.Tooltip("Service:Q", format=",.3f")],
    ))
    mo.vstack([mo.md("""
    ## 3. Baseline activity–energy reconciliation

    **x = total energy (PJ); y = total service**, separately in billion passenger-km and billion tonne-km.
    `Service = Σ(stock × c2a × UF)`; `energy = Σ(service / cohort efficiency)`.
    Vehicle-input energy counts PHEV blend input once and excludes upstream charger losses.
    CEUD secondary energy has its reported fleet/fuel boundary; unsupported fuels and composition can differ.
    Black cross = CEUD activity/energy. Black triangles retain CEUD energy but use the distinct
    calendar-2025 demand targets (backend real GDP; legacy workbook price index). Those triangles combine
    observed energy with projected service for context; they are not independent observations.

    A common UF change with fixed composition/efficiency scales **both** totals. Collinearity of backend cases
    is an accounting identity, not evidence of fit. If the energy-per-service ratio differs from CEUD,
    one common UF cannot meet both targets: energy closure necessarily moves activity off its target.
    The table quantifies the tradeoff. The native legacy point also loses stock between its 2023 calibration
    and 2025 evaluation, so its service need not close the original activity target.

    Gaps suggest a mismatch in aggregate energy per unit service, but do not by themselves identify
    efficiency error. Fleet composition, service units/load, cohort efficiency, supported fuels, energy
    boundaries and the missing inherited input identity need reconciliation before recommending a new
    efficiency or utilization calibration. **No further scientific recalibration is implemented.**
    """), display_chart(activity_energy_chart), mo.ui.table(cases[[
        "Class", "case", "implied_vehicle_input_pj", "implied_activity_bn", "energy_difference_percent", "activity_difference_percent",
        "energy_per_service_pj_per_bn", "ceud_energy_per_service_pj_per_bn",
    ]].rename(columns={"case": "UF case", "implied_vehicle_input_pj": "Energy (PJ)", "implied_activity_bn": "Service (bn)",
        "energy_difference_percent": "Energy gap (%)", "activity_difference_percent": "Activity gap (%)",
        "energy_per_service_pj_per_bn": "PJ / bn service", "ceud_energy_per_service_pj_per_bn": "CEUD PJ / bn service"}), selection=None,
        pagination=False, label="Passenger and freight units stay separate; energy sensitivity is conditional, legacy energy uses the declared inherited snapshot"),
        mo.ui.table(calibration[["Class", "implemented_utilization", "energy_closing_utilization", "energy_closure_activity_difference_percent"]],
                    selection=None, pagination=False, label="Backend sensitivity: UF needed for energy closure and resulting historical activity error")])
    return (activity_energy_chart,)


@app.cell(hide_code=True)
def _(BUILDS, CLASS_LABELS, alt, class_grid, diagnosis, display_chart, mo, region):
    methods = diagnosis["fleet"]["demand_method_comparison"]
    matched = methods.loc[methods.region.eq(region.value) & methods.calendar_year.notna()].copy()
    matched["Build"] = matched.evidence.replace({"Notebook recipe replay": "Legacy recipe"})
    matched["Class"] = matched.road_class.map(CLASS_LABELS)
    common_years = sorted(set.intersection(*[set(group.calendar_year) for _, group in matched.groupby("Build")]))
    matched = matched.loc[matched.calendar_year.isin(common_years)].sort_values(["road_class", "Build", "calendar_year"]).copy()
    matched["Growth index"] = matched.demand_bn / matched.groupby(["road_class", "Build"]).demand_bn.transform("first")
    demand_base = alt.Chart(matched).encode(
        x=alt.X("calendar_year:Q", title="Demand calendar year", axis=alt.Axis(format="d", tickCount=5)),
        color=alt.Color("Build:N", scale=alt.Scale(domain=BUILDS, range=["#2166ac", "#7b3294"]), title=None, legend=alt.Legend(orient="bottom")),
        strokeDash=alt.StrokeDash("Build:N", scale=alt.Scale(domain=BUILDS, range=[[1, 0], [7, 3]]), title=None, legend=alt.Legend(orient="bottom")),
        tooltip=["Class:N", "Build:N", "calendar_year:O", "period:O", "unit:N", alt.Tooltip("demand_bn:Q", format=",.3f"), alt.Tooltip("Growth index:Q", format=".4f")],
    )
    demand_chart = class_grid(demand_base.mark_line(point=True).encode(y=alt.Y("demand_bn:Q", title="Billion service-km (units in panel)", scale=alt.Scale(zero=True))))
    growth_spread = matched.groupby(["Build", "calendar_year"])["Growth index"].agg(["min", "max"])
    if (growth_spread["max"] - growth_spread["min"]).abs().gt(1e-12).any():
        raise ValueError("Growth differs by class; retain class facets instead of collapsing trajectories")
    growth_data = matched.groupby(["Build", "calendar_year"], as_index=False)["Growth index"].first()
    demand_growth_chart = alt.Chart(growth_data).mark_line(point=True).encode(
        x=alt.X("calendar_year:Q", title="Demand calendar year", axis=alt.Axis(format="d")),
        y=alt.Y("Growth index:Q", title="Each build's first common calendar year = 1", scale=alt.Scale(zero=False)),
        color=alt.Color("Build:N", scale=alt.Scale(domain=BUILDS, range=["#2166ac", "#7b3294"]), title=None, legend=alt.Legend(orient="bottom")),
        tooltip=["Build:N", "calendar_year:O", alt.Tooltip("Growth index:Q", format=".4f")],
    ).properties(width=790, height=190)
    attribution = diagnosis["fleet"]["demand_attribution"]
    attribution = attribution.loc[attribution.region.eq(region.value)]
    mo.vstack([mo.md("""
    ## 4. Demand: workbook meaning and calendar alignment

    Both methods anchor to the same **2023 CEUD observed activity**. Backend demand multiplies it by
    **CER real GDP(calendar year) / real GDP(2023)**. The active legacy `growth_rate_type='gdp'` selects
    workbook `gdp`, whose formulas are **column C / C$2**: GDP-deflator levels normalized to 2023.
    This interpretation is checked against actual formulas and the registered CER macro source values,
    rather than inferred from a column name or previous report.

    Specific contradiction: the compiler describes national GDP activity growth, yet calendar 2030 uses
    **144.1 / 123 = 1.171545**, the GDP deflator. Real GDP is **2,652,642 / 2,385,443 = 1.112012**.
    The earlier deflator characterization is confirmed; the economic description in the legacy notes is wrong.
    The comparison preserves that executable price-index recipe without replacing its assumption.

    Legacy `project_demand_v` generates calendar 2025–2050, then `shift_time_period` subtracts five and
    retains model labels 2025–2045. Thus **calendar 2025 → label 2020 is dropped**; first emitted
    **label 2025 → calendar 2030**. Backend prospective demand uses the same interval-end calendar axis.
    Calendar-2025 contextual service targets in Section 3 are not the first emitted model-period demands.
    """), display_chart(demand_chart), display_chart(demand_growth_chart),
        mo.ui.table(attribution[["road_class", "period", "current_source_year", "notebook_recipe_demand_bn", "current", "replay_relative_percent"]].rename(columns={
            "road_class": "Class", "period": "Model label", "current_source_year": "Calendar year", "notebook_recipe_demand_bn": "Legacy recipe (bn)",
            "current": "Current backend (bn)", "replay_relative_percent": "Backend / legacy − 1 (%)",
        }), selection=None, page_size=10),
        mo.accordion({"Formula, source-value and year audit": mo.ui.table(diagnosis["fleet"]["demand_growth_series_audit"], selection=None, page_size=10),
            "Observation, calibration and 2025 demand targets": mo.ui.table(diagnosis["fleet"]["demand_baseline_service_targets"], selection=None, pagination=False),
            "Full trajectories, stored SQLite discrepancy and ordered attribution": mo.vstack([
                mo.ui.table(methods, selection=None, page_size=10), mo.ui.table(attribution, selection=None, page_size=10),
                mo.ui.table(diagnosis["fleet"]["demand_method_context"], selection=None, pagination=False),
            ])})])
    return demand_chart, demand_growth_chart


@app.cell(hide_code=True)
def _(alt, diagnosis, display_chart, mo):
    harmonized = diagnosis["charging"]["harmonized_costs"]
    harmonized = harmonized.loc[harmonized.table.eq("cost_invest")].copy()
    harmonized["Class"] = harmonized.charger_class.map({"ldv": "Light-duty charging", "mhdv": "Medium/heavy-duty charging"})
    rates = harmonized.melt(
        id_vars=["Class", "reference_vintage", "backend_vintage", "scope_status"],
        value_vars=["backend_million_cad_per_gw", "reference_million_cad_per_gw"], var_name="Evidence", value_name="Cost",
    )
    rates["Evidence"] = rates.Evidence.map({
        "backend_million_cad_per_gw": "Current backend", "reference_million_cad_per_gw": "Reference at backend utilization",
    })
    charging_chart = alt.Chart(rates).mark_line(point=True).encode(
        x=alt.X("reference_vintage:Q", title="Cost source calendar year", axis=alt.Axis(format="d", tickCount=6)),
        y=alt.Y("Cost:Q", title="Million CAD2020/GW", scale=alt.Scale(zero=True)),
        color=alt.Color("Evidence:N", scale=alt.Scale(domain=["Current backend", "Reference at backend utilization"], range=["#2166ac", "#b35806"]),
                        legend=alt.Legend(orient="bottom", title=None, labelLimit=280, symbolType="stroke", symbolSize=300)),
        strokeDash=alt.StrokeDash("Evidence:N", scale=alt.Scale(domain=["Current backend", "Reference at backend utilization"], range=[[1, 0], [7, 3]]),
                                  legend=alt.Legend(orient="bottom", title=None, labelLimit=280, symbolType="stroke", symbolSize=300)),
        tooltip=["Class:N", "Evidence:N", "reference_vintage:O", "backend_vintage:O", "scope_status:N", alt.Tooltip("Cost:Q", format=",.2f")],
    ).properties(width=430, height=210).facet(column=alt.Column("Class:N", title=None))
    charging_residual_chart = alt.Chart(harmonized).mark_bar(size=24).encode(
        x=alt.X("reference_vintage:O", title="Cost source calendar year"),
        y=alt.Y("harmonized_relative_percent:Q", title="Backend / reference − 1 (%)"), color=alt.value("#2166ac"),
        tooltip=["reference_vintage:O", alt.Tooltip("harmonized_relative_percent:Q", format="+.1f")],
    ).properties(width=430, height=110).facet(column=alt.Column("Class:N", title=None))
    mo.vstack([
        mo.md("""
        ## 5. Charging: harmonized throughput basis, all years

        Both rates use **million CAD2020/GW**. Convert reference million CAD2020/(PJ/year)
        using **reference rate × c2a × backend utilization**. Here c2a = 31.536 PJ/year
        per GW; new-charger utilization is 20% LDV and 30% MHDV. This is equivalent to
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
        mo.ui.table(harmonized[[
            "Class", "reference_vintage", "backend_vintage", "reference_million_cad_per_gw",
            "backend_million_cad_per_gw", "harmonized_relative_percent", "scope_status",
        ]].rename(columns={
            "reference_vintage": "Source year", "backend_vintage": "Backend vintage",
            "reference_million_cad_per_gw": "Reference (M CAD2020/GW)", "backend_million_cad_per_gw": "Backend (M CAD2020/GW)",
            "harmonized_relative_percent": "Difference (%)", "scope_status": "Coverage",
        }), selection=None, pagination=False, format_mapping={"Difference (%)": "{:+.1f}%"}, label="All years, including missing backend coverage"),
        mo.accordion({
            "Equipment components, currency conversion and weights": mo.ui.table(diagnosis["charging"]["source_components"], selection=None, page_size=10),
            "Fixed ratios and variable-cost coverage": mo.vstack([
                mo.ui.table(diagnosis["charging"]["fixed_ratios"], selection=None, page_size=8),
                mo.ui.table(diagnosis["charging"]["variable_coverage"], selection=None, pagination=False),
            ]),
            "Inherited context and unit evidence": mo.vstack([
                mo.ui.table(diagnosis["charging"]["reference_inherited_context"], selection=None, page_size=8),
                mo.ui.table(diagnosis["charging"]["reference_rows"], selection=None, page_size=8),
            ]),
        }),
    ])
    return charging_chart, charging_residual_chart


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
