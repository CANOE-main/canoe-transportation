"""Equivalent fleet, utilization, demand and charging-cost diagnostics."""

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
    STAGES = ["Before calibration", "Implemented activity calibration", "Energy closure sensitivity"]
    STAGE_COLORS = ["#999999", "#2166ac", "#b35806"]
    STOCK_LABELS = {**CLASS_LABELS, "passenger_light_trucks": "Light trucks", "freight_light_trucks": "Light trucks"}
    STOCK_ORDER = ["Cars", "Light trucks", "Medium trucks", "Heavy trucks"]

    def stock_grid(chart):
        return chart.properties(width=370, height=190).facet(
            facet=alt.Facet("Class:N", sort=STOCK_ORDER, title=None, header=alt.Header(labelFontSize=12)),
            columns=2,
        ).resolve_scale(x="shared", y="independent")

    def class_grid(chart, shared_y=False, service_units=False):
        field = "Class:N"
        labels = list(CLASS_LABELS.values())
        if service_units:
            field = "Panel:N"
            labels = [label + (" (passenger-km)" if name in {"cars", "passenger_light_trucks"}
                               else " (tonne-km)") for name, label in CLASS_LABELS.items()]
        grid = chart.properties(width=280, height=170).facet(
            facet=alt.Facet(field, sort=labels, title=None, header=alt.Header(labelFontSize=12)), columns=3,
        ).resolve_scale(x="independent", y="shared" if shared_y else "independent")
        if service_units:
            grid = grid.transform_calculate(Panel=(
                "datum.Class + (indexof(['Cars', 'Passenger light trucks'], datum.Class) >= 0"
                " ? ' (passenger-km)' : ' (tonne-km)')"))
        return grid

    def display_chart(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    return CLASS_LABELS, STAGES, STAGE_COLORS, STOCK_LABELS, STOCK_ORDER, alt, class_grid, display_chart, load_validation_diagnosis, mo, pd, stock_grid


@app.cell
def _(load_validation_diagnosis):
    diagnosis = load_validation_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    scenario = diagnosis["fleet_metadata"]["scenario"]
    mo.vstack([mo.md(f"""
    # Transportation validation: methods and closure

    Build: **[{scenario['scenario']['name']}](../../{diagnosis['scenario_path']})**, base year **{scenario['periods']['base_year']}**.
    References: **legacy_variant_sales_cohorts** (workbooks and `canoe_ON_16d.sqlite`)
    and **legacy_variant_charger_costs** (cost tables).
    Stock panels combine passenger and freight light trucks; service panels keep their different units separate.
    Stock panels use separate vertical scales.
    **{diagnosis['significance_percent']:g}%** flags discrepancies for review, not an acceptance tolerance.
    """), mo.accordion({"Generate this comparison (run in order)": mo.md(f"""
    ```powershell
    uv run python src/build_transport.py --scenario {diagnosis['scenario_path']} --overwrite
    uv run python -m validation.transportation_research --scenario {diagnosis['scenario_path']} --write-report
    uv run python -m validation.charging_cost_comparison --scenario {diagnosis['scenario_path']} --write-report
    uv run marimo edit docs/insights/transportation_validation_diagnosis.py
    ```
    Both reports must match this live YAML and the same database. Reference inputs remain read-only.
    """)})])
    return


@app.cell
def _(diagnosis, mo):
    regions = sorted(diagnosis["fleet"]["stock_activity_energy"].region.unique())
    region = mo.ui.dropdown(regions, value=regions[0], label="Comparison region")
    region
    return (region,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, STOCK_LABELS, STOCK_ORDER, alt, diagnosis, display_chart, mo, pd, region, stock_grid):
    annual = diagnosis["fleet"]["sales_cohort_reconciliation"]
    annual = annual.loc[annual.region.eq(region.value)].copy()
    annual["Class"] = annual.road_class.map(STOCK_LABELS)
    annual_stock = annual.groupby(["Class", "vintage_year"], as_index=False)[[
        "sales_k_vehicles", "surviving_k_vehicles", "retired_k_vehicles",
        "calibrated_surviving_k_vehicles", "inserted_cohort_k_vehicles",
    ]].sum()
    portions = annual_stock.melt(id_vars=["Class", "vintage_year"], value_vars=[
        "surviving_k_vehicles", "retired_k_vehicles", "calibrated_surviving_k_vehicles", "inserted_cohort_k_vehicles",
    ], var_name="Component", value_name="Vehicles")
    portions["Kind"] = portions.Component.isin(["surviving_k_vehicles", "retired_k_vehicles"]).map({True: "Sales", False: "Model"})
    portions["Component"] = portions.Component.map({
        "surviving_k_vehicles": "Raw surviving sales", "retired_k_vehicles": "Cumulative retirements",
        "calibrated_surviving_k_vehicles": "After stock scaling", "inserted_cohort_k_vehicles": "After fixed-lifetime allocation",
    })
    cohort_legend = ["Raw surviving sales", "Cumulative retirements", "After stock scaling", "After fixed-lifetime allocation"]
    cohort_colors = ["#4477aa", "#d9d9d9", "#cc8844", "#222222"]
    retirement_bars = alt.Chart().transform_filter(alt.datum.Kind == "Sales").mark_bar().encode(
        x=alt.X("vintage_year:O", title="Sales year", axis=alt.Axis(labelAngle=0, labelExpr="toNumber(datum.label) % 5 == 0 ? datum.label : ''")),
        y=alt.Y("Vehicles:Q", title="Thousand vehicles", stack="zero"),
        color=alt.Color("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=cohort_colors),
            legend=alt.Legend(orient="right", direction="vertical", labelLimit=260, symbolType="stroke", symbolSize=350)),
        order=alt.Order("Component:N", sort="descending"),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.2f")],
    )
    allocated_line = alt.Chart().transform_filter(alt.datum.Kind == "Model").mark_line(strokeWidth=1.8).encode(
        x="vintage_year:O", y=alt.Y("Vehicles:Q", stack=None), order="vintage_year:O",
        color=alt.Color("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=cohort_colors),
                        legend=alt.Legend(orient="right", direction="vertical", labelLimit=260, symbolType="stroke", symbolSize=350)),
        strokeDash=alt.StrokeDash("Component:N", title=None, scale=alt.Scale(domain=cohort_legend, range=[[1, 0], [1, 0], [1, 0], [5, 3]]),
                                  legend=alt.Legend(orient="right", direction="vertical", labelLimit=260, symbolType="stroke", symbolSize=350)),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.2f")],
    )
    retirement_chart = stock_grid(alt.layer(retirement_bars, allocated_line, data=portions)).properties(
        title="Backend only · raw sales, stock scaling and cohort redistribution",
    )
    fleet_totals = diagnosis["fleet"]["fleet_reconciliation"]
    fleet_totals = fleet_totals.loc[fleet_totals.region.eq(region.value)].copy()
    fleet_totals["Class"] = fleet_totals.road_class.map(CLASS_LABELS)
    backend_stages = fleet_totals.assign(Class=fleet_totals.road_class.map(STOCK_LABELS)).groupby("Class", as_index=False)[[
        "surviving_k_vehicles", "calibrated_surviving_k_vehicles", "allocated_stock_k_vehicles", "inserted_stock_k_vehicles",
    ]].sum()
    backend_stages["Stock multiplier"] = backend_stages.calibrated_surviving_k_vehicles / backend_stages.surviving_k_vehicles
    backend_stages = backend_stages.set_index("Class").reindex(STOCK_ORDER).reset_index()
    heavy = fleet_totals.loc[fleet_totals.road_class.eq("heavy_trucks")].iloc[0]
    built_totals = diagnosis["fleet"]["built_stock_totals"]
    built_totals = built_totals.loc[built_totals.region.eq(region.value)].copy()
    built_totals["Class"] = built_totals.road_class.map(STOCK_LABELS)
    stock_comparison = built_totals.groupby(["Class", "evidence"], as_index=False)[[
        "capacity_k_vehicles", "ceud_stock_k_vehicles",
    ]].sum()
    stock_comparison["Difference from CEUD (%)"] = 100 * (stock_comparison.capacity_k_vehicles / stock_comparison.ceud_stock_k_vehicles - 1)
    stock_comparison["Share of CEUD target (%)"] = 100 + stock_comparison["Difference from CEUD (%)"]
    stock_methods = ["Backend SQLite", "Sales variant SQLite"]
    stock_comparison["Kind"] = "Database"
    targets = stock_comparison.loc[stock_comparison.evidence.eq("Backend SQLite"), ["Class", "ceud_stock_k_vehicles"]].rename(
        columns={"ceud_stock_k_vehicles": "capacity_k_vehicles"},
    )
    targets["Kind"] = "Target"
    targets["Share of CEUD target (%)"] = 100.
    stock_data = pd.concat([stock_comparison, targets], ignore_index=True)
    stock_bars = alt.Chart().transform_filter(alt.datum.Kind == "Database").mark_bar(size=13).encode(
        y=alt.Y("Class:N", sort=STOCK_ORDER, title=None),
        yOffset=alt.YOffset("evidence:N", sort=stock_methods, scale=alt.Scale(domain=stock_methods)),
        x=alt.X("Share of CEUD target (%):Q", title="Stored capacity / backend CEUD 2025 target (%)", scale=alt.Scale(zero=True)),
        color=alt.Color("evidence:N", title=None, scale=alt.Scale(domain=stock_methods, range=["#2166ac", "#cc8844"]),
                        legend=alt.Legend(orient="right", direction="vertical", labelLimit=240)),
        tooltip=["Class:N", "evidence:N", alt.Tooltip("capacity_k_vehicles:Q", title="Stored capacity (thousand)", format=",.3f"),
                 alt.Tooltip("Difference from CEUD (%):Q", format="+.2f")],
    )
    stock_target = alt.Chart().transform_filter(alt.datum.Kind == "Target").mark_tick(
        orient="vertical", color="#222222", thickness=2, size=38,
    ).encode(y=alt.Y("Class:N", sort=STOCK_ORDER), x="Share of CEUD target (%):Q",
             tooltip=["Class:N", alt.Tooltip("capacity_k_vehicles:Q", title="Backend CEUD 2025 target (thousand)", format=",.3f")])
    stock_residual_chart = alt.layer(stock_bars, stock_target, data=stock_data).properties(width=640, height=220)
    mo.vstack([
        mo.md("""
        ## 1. Backend cohorts: raw survival, stock scaling and allocation

        **Blue + gray = original sales; blue = sales × cumulative survival at 2025 age.**
        The backend then explicitly scales survivors by **CEUD stock / raw survivors** (orange).
        Fixed-lifetime allocation redistributes that scaled stock among eligible cohorts (dashed).
        Its total equals CEUD by construction. This calibration identity does not validate the raw reconstruction.

        Fixed-lifetime eligibility applies with **`survival_curves: false`**. Cars/light trucks/medium
        and heavy trucks have 16/17/20-year medians; the shared 2011 sales-year cutoff results from
        also testing the five-year vintage's eligibility. Allocation changes the cohort distribution.
        """), display_chart(retirement_chart),
        mo.md(f"""
        **Heavy trucks:** {heavy.surviving_k_vehicles:,.3f}k raw survivors × {heavy.stock_calibration_multiplier:.4f}
        = **{heavy.calibrated_surviving_k_vehicles:,.3f}k** stock-calibrated vehicles;
        fixed-lifetime allocation preserves **{heavy.allocated_stock_k_vehicles:,.3f}k**.
        Scaling removes the raw surplus; it is a separate operation from cumulative retirement.
        Consequently, allocated cohorts can lie below raw survivors while their total equals CEUD.
        """),
        mo.ui.table(backend_stages[["Class", "surviving_k_vehicles", "Stock multiplier", "calibrated_surviving_k_vehicles", "allocated_stock_k_vehicles"]].rename(columns={
            "surviving_k_vehicles": "Raw survivors (k)", "calibrated_surviving_k_vehicles": "After stock scaling (k)",
            "allocated_stock_k_vehicles": "After allocation (k)",
        }), selection=None, pagination=False, label="Backend transformations only; light-truck quantities are summed before calculating the multiplier",
            format_mapping={"Stock multiplier": "{:.4f}", "Raw survivors (k)": "{:,.3f}", "After stock scaling (k)": "{:,.3f}", "After allocation (k)": "{:,.3f}"}),
        mo.md("""
        ### What the two databases actually contain

        Both bars below sum **stored vehicle-capacity rows**, including every BEV/PHEV range.
        The sales-variant values are read directly from its SQLite; no CEUD scaling or backend
        retirement method is applied to them. Black ticks mark the backend's CEUD 2025 target
        at 100% (2023 stock held constant); the percentage axis makes classes comparable and
        absolute counts remain in the table. This is a reference point, not a verified variant base year.
        Stored capacity can include cohorts that expire before 2025; active fleets require a separate retirement calculation.
        """), display_chart(stock_residual_chart),
        mo.ui.table(stock_comparison[["Class", "evidence", "capacity_k_vehicles", "ceud_stock_k_vehicles", "Difference from CEUD (%)"]].rename(columns={
            "evidence": "Database", "capacity_k_vehicles": "Stored capacity (k)", "ceud_stock_k_vehicles": "Backend CEUD 2025 target (k)",
        }), selection=None, pagination=False, label="Actual database totals; CEUD equality on the backend side is the implemented stock calibration",
            format_mapping={"Stored capacity (k)": "{:,.3f}", "Backend CEUD 2025 target (k)": "{:,.3f}", "Difference from CEUD (%)": "{:+.2f}%"}),
        mo.accordion({
            "Backend class-specific stock and cutoff audit": mo.ui.table(fleet_totals[[
                "Class", "stock_explanation", "fixed_lifetime_years", "first_eligible_annual_cohort", "first_eligible_model_vintage",
                "calibrated_to_inserted_residual_k_vehicles",
            ]], selection=None, pagination=False),
        }),
    ])
    return retirement_chart, stock_residual_chart


@app.cell(hide_code=True)
def _(CLASS_LABELS, STOCK_LABELS, alt, diagnosis, display_chart, mo, region, stock_grid):
    composition = diagnosis["fleet"]["built_stock_parameters"]
    composition = composition.loc[composition.region.eq(region.value)].copy()
    composition["Class"] = composition.road_class.map(STOCK_LABELS)
    stacks = composition.groupby(["Class", "vintage", "evidence", "powertrain"], as_index=False).agg(
        Stock=("capacity_k_vehicles", "sum"), Technology_count=("tech", "nunique"),
    )
    comparison_methods = ["Backend SQLite", "Sales variant SQLite"]
    powertrain_order = ["Gasoline", "Diesel", "HEV", "PHEV", "BEV", "CNG", "LPG", "Other"]
    powertrain_order = [powertrain for powertrain in powertrain_order if powertrain in set(stacks.powertrain)]
    powertrain_colors = {"Gasoline": "#ddcc77", "Diesel": "#4477aa", "HEV": "#228833", "PHEV": "#aa3377",
                         "BEV": "#66ccee", "CNG": "#ee7733", "LPG": "#cc6677", "Other": "#999999"}
    stacks["Powertrain order"] = stacks.powertrain.map({powertrain: index for index, powertrain in enumerate(powertrain_order)})
    vintage_chart = stock_grid(alt.Chart(stacks).mark_bar(size=22).encode(
        x=alt.X("vintage:O", title="Native vintage · left backend / right sales variant", axis=alt.Axis(labelAngle=0)),
        xOffset=alt.XOffset("evidence:N", sort=comparison_methods, scale=alt.Scale(domain=comparison_methods)),
        y=alt.Y("Stock:Q", title="Stored capacity (thousand vehicles)", stack="zero"),
        color=alt.Color("powertrain:N", title="Stored powertrain", scale=alt.Scale(domain=powertrain_order,
            range=[powertrain_colors[powertrain] for powertrain in powertrain_order]),
            legend=alt.Legend(orient="right", direction="vertical", labelLimit=240)),
        order=alt.Order("Powertrain order:Q", sort="ascending"),
        tooltip=["Class:N", "evidence:N", "vintage:O", "powertrain:N", "Technology_count:Q", alt.Tooltip("Stock:Q", format=",.3f")],
    ))
    recipe_totals = diagnosis["fleet"]["reference_recipe_totals"]
    recipe_totals = recipe_totals.loc[recipe_totals.region.eq(region.value)].copy()
    recipe_totals["Class"] = recipe_totals.road_class.map(CLASS_LABELS)
    mo.vstack([
        mo.md("""
        ### Actual vintage composition: backend versus sales variant

        Each pair now contains **only the two databases' stored parameters**; no markers or substituted
        workbook reconstruction. Segments are powertrains, which both SQLite files preserve.
        The variant does not retain the annual split inside its five-year vintages, so yearly colors
        would imply unsupported data. Backend vintage 2020 contains 2021–2025 cohorts;
        the exact annual composition of the variant's stored vintage 2020 is unverified.
        """), display_chart(vintage_chart),
        mo.md("""
        ### Do the annual Excel files reproduce the sales-variant SQLite?

        The supplied notebook multiplies annual class sales by powertrain shares, extends the
        **2023 products** through 2024–2025, and groups 2005–2025 into five-year bins.
        Its `ExistingCapacity` calculation applies **no survival or CEUD stock normalization**;
        survival enters later utilization calculations. Replaying those steps uses their own inputs and bins.
        The table compares that intended capacity output with the supplied SQLite after explicit technology-suffix alignment.

        The replay does not reproduce the stored rows. This discrepancy remains **after annual-to-vintage
        aggregation**. The newer workbooks, notebook and older SQLite therefore describe different
        evidence states; the inspected notebook supplies no CSV-to-SQLite write path establishing this build.
        Its stored road capacities also equal the original legacy database's values.
        """),
        mo.ui.table(recipe_totals[["Class", "stored_capacity_k_vehicles", "recipe_capacity_k_vehicles",
                                     "recipe_minus_stored_k_vehicles", "recipe_difference_percent", "common_keys", "matching_keys"]].rename(columns={
            "stored_capacity_k_vehicles": "Stored variant (k)", "recipe_capacity_k_vehicles": "Notebook Excel recipe (k)",
            "recipe_minus_stored_k_vehicles": "Recipe − stored (k)", "recipe_difference_percent": "Difference (%)",
            "common_keys": "Shared technology/vintage keys", "matching_keys": "Matching values",
        }), selection=None, pagination=False, label="One row per original CEUD class; light-truck service classes remain separate for this source reconciliation",
            format_mapping={"Stored variant (k)": "{:,.3f}", "Notebook Excel recipe (k)": "{:,.3f}", "Recipe − stored (k)": "{:+,.3f}", "Difference (%)": "{:+.2f}%"}),
        mo.accordion({
            "Recipe steps, years and evidence limits": mo.vstack([
                mo.ui.table(diagnosis["fleet"]["reference_recipe_steps"], selection=None, pagination=False),
                mo.ui.table(diagnosis["fleet"]["reference_recipe_context"], selection=None, pagination=False),
                mo.ui.table(diagnosis["fleet"]["reference_replay_context"], selection=None, pagination=False),
            ]),
            "Stored variant lifetimes and timing evidence": mo.ui.table(diagnosis["fleet"]["reference_timing_context"], selection=None, pagination=False),
            "Recipe versus SQLite: every native technology/vintage key": mo.ui.table(diagnosis["fleet"]["reference_recipe_comparison"], selection=None, page_size=10),
            "Actual database rows used in both stacks": mo.ui.table(composition, selection=None, page_size=10),
            "Literal annual workbook recipe": mo.ui.table(diagnosis["fleet"]["reference_recipe_annual"], selection=None, page_size=10),
            "Backend annual eligibility and fuel allocation": mo.vstack([
                mo.json(diagnosis["fleet_metadata"]["cohort_audit"]["period_mapping"]),
                mo.ui.table(diagnosis["fleet"]["road_age_cohorts"], selection=None, page_size=10),
                mo.ui.table(diagnosis["fleet"]["road_fuel_shares"], selection=None, page_size=10),
            ]),
        }),
    ])
    return (vintage_chart,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, STAGES, STAGE_COLORS, alt, diagnosis, display_chart, mo, region):
    calibration = diagnosis["fleet"]["utilization_calibration"]
    calibration = calibration.loc[calibration.region.eq(region.value)].copy()
    calibration["Class"] = calibration.road_class.map(CLASS_LABELS)
    utilization_long = calibration.melt(id_vars="Class", value_vars=[
        "prior_mean_utilization", "implemented_utilization", "energy_closing_utilization",
    ], var_name="Stage", value_name="Annual factor")
    utilization_long["Stage"] = utilization_long.Stage.map(dict(zip([
        "prior_mean_utilization", "implemented_utilization", "energy_closing_utilization",
    ], STAGES)))
    utilization_chart = alt.Chart(utilization_long).mark_point(filled=True, size=100).encode(
        y=alt.Y("Class:N", sort=list(CLASS_LABELS.values()), title=None),
        x=alt.X("Annual factor:Q", title="Annual utilization factor (dimensionless)", axis=alt.Axis(tickCount=6), scale=alt.Scale(zero=True)),
        color=alt.Color("Stage:N", sort=STAGES, scale=alt.Scale(domain=STAGES, range=STAGE_COLORS),
                        legend=alt.Legend(orient="bottom", title=None, labelLimit=280)),
        shape=alt.Shape("Stage:N", sort=STAGES, scale=alt.Scale(domain=STAGES, range=["circle", "square", "diamond"]),
                        legend=alt.Legend(orient="bottom", title=None, labelLimit=280)),
        tooltip=["Class:N", "Stage:N", alt.Tooltip("Annual factor:Q", format=".5f")],
    ).properties(width=760, height=185)
    mo.vstack([
        mo.md("""
        ## 2. Annual utilization and its implemented calibration

        Gray is the captured CEUD-derived factor before sales-stock calibration;
        blue is inserted utilization. Calibration preserves class **annual activity**,
        applying the same flat factor to existing and new technologies. It does not
        calibrate energy consumption.

        Orange is an **unimplemented energy-closure sensitivity**: implemented factor ×
        NRCan energy / reconstructed energy, holding stock, efficiencies and c2a fixed.
        One factor generally cannot close activity and energy simultaneously when
        implied fleet efficiency differs. The table quantifies that tradeoff.

        `capacity_to_activity` uses the reviewed legacy-aligned manual factors in regular
        production. Raw factors are comparable only when those scaling factors agree.
        The next grid multiplies them into physical annual service per vehicle.
        """), display_chart(utilization_chart),
        mo.ui.table(calibration[[
            "Class", "prior_mean_utilization", "implemented_utilization", "calibration_percent",
            "energy_closing_utilization", "energy_closure_activity_difference_percent", "energy_closure_feasible",
        ]].rename(columns={
            "prior_mean_utilization": "Before", "implemented_utilization": "Implemented",
            "calibration_percent": "Implemented change (%)", "energy_closing_utilization": "Energy sensitivity",
            "energy_closure_activity_difference_percent": "Activity gap at energy closure (%)",
            "energy_closure_feasible": "Sensitivity factor ≤ 1",
        }), selection=None, pagination=False, format_mapping={
            "Before": "{:.5f}", "Implemented": "{:.5f}", "Energy sensitivity": "{:.5f}",
            "Implemented change (%)": "{:+.2f}%", "Activity gap at energy closure (%)": "{:+.2f}%",
        }, label="Actual calibration and diagnostic energy closure; one row per class"),
        mo.accordion({"Reference utilization bounds": mo.vstack([
            mo.md("Legacy bounds are period-indexed; backend factors are vintage-indexed. Raw ranges are not matched-year physical utilization."),
            mo.ui.table(diagnosis["fleet"]["utilization_comparison"], selection=None, page_size=10),
        ])}),
    ])
    return calibration, utilization_chart


@app.cell(hide_code=True)
def _(CLASS_LABELS, STAGES, STAGE_COLORS, alt, calibration, class_grid, diagnosis, display_chart, mo, region):
    cohort_energy = diagnosis["fleet"]["cohort_utilization_energy"]
    cohort_energy = cohort_energy.loc[cohort_energy.region.eq(region.value)].copy()
    cohort_energy["Class"] = cohort_energy.road_class.map(CLASS_LABELS)
    cohort_energy = cohort_energy.merge(calibration[["road_class", "ceud_secondary_energy_pj"]], on="road_class", validate="many_to_one")
    cohort_energy["NRCan energy (%)"] = 100 * cohort_energy.vehicle_input_pj / cohort_energy.ceud_secondary_energy_pj
    contributions = cohort_energy.groupby(["Class", "stage", "vintage"], as_index=False).agg(
        contribution=("NRCan energy (%)", "sum"), energy_pj=("vehicle_input_pj", "sum"),
    )
    bars = alt.Chart().mark_bar().encode(
        x=alt.X("stage:N", sort=STAGES, title=None, axis=alt.Axis(labelAngle=0,
            labelExpr="datum.label === 'Before calibration' ? 'Before' : datum.label === 'Implemented activity calibration' ? 'Implemented' : 'Energy sensitivity'")),
        y=alt.Y("contribution:Q", title="% of NRCan energy", stack="zero"),
        color=alt.Color("vintage:O", title="Cohort vintage", scale=alt.Scale(scheme="blues"), legend=alt.Legend(orient="bottom")),
        order="vintage:O",
        tooltip=["Class:N", "stage:N", "vintage:O", alt.Tooltip("energy_pj:Q", title="Vehicle input (PJ)", format=",.3f"),
                 alt.Tooltip("contribution:Q", title="% of NRCan energy", format=".2f")],
    )
    target = alt.Chart().mark_rule(strokeDash=[4, 3], color="#b35806").encode(y=alt.datum(100))
    energy_chart = class_grid(alt.layer(bars, target, data=contributions), shared_y=True)
    cohort_energy["Service (thousand/vehicle/year)"] = cohort_energy.annual_service_per_vehicle / 1000
    annual_service_chart = class_grid(alt.Chart(cohort_energy.loc[cohort_energy.stage.ne(STAGES[2])]).mark_point(filled=True, size=45).encode(
        x=alt.X("vintage:O", title="Inserted cohort vintage"), xOffset=alt.XOffset("stage:N", sort=STAGES[:2]),
        y=alt.Y("Service (thousand/vehicle/year):Q", title="Thousand service-km/vehicle/year", scale=alt.Scale(zero=True)),
        color=alt.Color("stage:N", scale=alt.Scale(domain=STAGES[:2], range=STAGE_COLORS[:2]), legend=alt.Legend(orient="bottom", title=None, labelLimit=260)),
        tooltip=["Class:N", "stage:N", "tech:N", "vintage:O", "service_unit:N",
                 alt.Tooltip("c2a:Q", title="Reviewed c2a", format=".3f"),
                 alt.Tooltip("annual_service_per_vehicle:Q", title="Annual service per vehicle", format=",.0f")],
    ), service_units=True)
    mo.vstack([
        mo.md("""
        ### Physical annual service per vehicle

        Annual service per vehicle = `c2a × factor × 10⁶`, converting billion service-km
        per thousand vehicles. Service means passenger-km for passenger classes and
        tonne-km for freight, not vehicle-km: that would require occupancy or payload.
        All inserted vintages are shown; equal-service technologies have coincident points.
        """), display_chart(annual_service_chart),
        mo.md("""
        ## 3. How cohorts add up to baseline energy

        **Activityᵢ = stockᵢ × c2aᵢ × factorᵢ; energyᵢ = activityᵢ / efficiencyᵢ.**
        Segments are additive vintage contributions; the dashed line is NRCan class
        energy (100%). Common percentage axes compare closure across classes; actual PJ
        remain in the table. The sensitivity closes energy by construction and changes
        activity as shown above; it is not independent validation.

        This is vehicle-input energy, counting PHEV blend input once and excluding
        upstream charging losses. NRCan secondary energy covers its reported fuel/fleet
        population. Composition, supported fuels and cohort efficiency can affect gaps;
        this aggregate check does not isolate their individual effects.
        """), display_chart(energy_chart),
        mo.ui.table(calibration[[
            "Class", "ceud_activity_bn", "implied_activity_bn", "prior_energy_pj",
            "ceud_secondary_energy_pj", "implied_vehicle_input_pj", "energy_relative_difference", "activity_at_energy_closure_bn",
        ]].rename(columns={
            "ceud_activity_bn": "NRCan activity (bn)", "implied_activity_bn": "Implemented activity (bn)",
            "prior_energy_pj": "Before calibration (PJ)", "ceud_secondary_energy_pj": "NRCan energy (PJ)",
            "implied_vehicle_input_pj": "Implemented energy (PJ)", "energy_relative_difference": "Energy gap",
            "activity_at_energy_closure_bn": "Activity at energy closure (bn)",
        }), selection=None, pagination=False, format_mapping={"Energy gap": "{:+.2%}"},
            label="Passenger activity: billion passenger-km; freight: billion tonne-km"),
        mo.ui.table(calibration[["Class", "energy_explanation"]].rename(columns={"energy_explanation": "Energy discrepancy and calibration tradeoff"}),
                    selection=None, pagination=False, label="Quantitative explanations for further review"),
        mo.accordion({
            "Every cohort, scaling factor, efficiency and contribution": mo.ui.table(cohort_energy, selection=None, page_size=10),
            "NRCan endpoints and extension evidence": mo.vstack([
                mo.ui.table(diagnosis["fleet"]["stock_activity_energy"], selection=None, page_size=5),
                mo.ui.table(diagnosis["fleet"]["ceud_extension_audit"], selection=None, page_size=10),
            ]),
        }),
    ])
    return annual_service_chart, energy_chart


@app.cell(hide_code=True)
def _(CLASS_LABELS, alt, class_grid, diagnosis, display_chart, mo, region):
    demand_methods = diagnosis["fleet"]["demand_method_comparison"]
    demand_methods = demand_methods.loc[demand_methods.region.eq(region.value)].copy()
    matched = demand_methods.loc[demand_methods.calendar_year.notna()].copy()
    common_years = set.intersection(*[set(group.calendar_year) for _, group in matched.groupby("evidence")])
    for (road_class, evidence), trajectory in matched.groupby(["road_class", "evidence"]):
        first_value = trajectory.loc[trajectory.calendar_year.eq(min(common_years)), "demand_bn"].iloc[0]
        matched.loc[trajectory.index, "growth_index"] = trajectory.demand_bn / first_value
    matched["Class"] = matched.road_class.map(CLASS_LABELS)
    matched["Demand units"] = matched.unit
    method_base = alt.Chart(matched).encode(
        x=alt.X("calendar_year:Q", title="Projection calendar year", axis=alt.Axis(format="d", tickCount=4)),
        color=alt.Color("evidence:N", scale=alt.Scale(domain=["Current backend", "Notebook recipe replay"], range=["#2166ac", "#b35806"]),
                        legend=alt.Legend(orient="bottom", title=None, symbolType="stroke", symbolSize=300)),
        strokeDash=alt.StrokeDash("evidence:N", scale=alt.Scale(domain=["Current backend", "Notebook recipe replay"], range=[[1, 0], [7, 3]]),
                                  legend=alt.Legend(orient="bottom", title=None, symbolType="stroke", symbolSize=300)),
        detail="evidence:N", order="calendar_year:Q",
        tooltip=["Class:N", "evidence:N", "period:O", "calendar_year:O", "Demand units:N",
                 alt.Tooltip("demand_bn:Q", title="Demand (billion)", format=",.3f"), alt.Tooltip("growth_index:Q", format=".3f")],
    )
    demand_chart = class_grid(method_base.mark_line(point=True).encode(y=alt.Y("demand_bn:Q", title="Billion service-km", scale=alt.Scale(zero=True))), service_units=True)
    growth_spread = matched.groupby(["evidence", "calendar_year"]).growth_index.agg(["min", "max"])
    common_growth = (growth_spread["max"] - growth_spread["min"]).abs().le(1e-12).all()
    if common_growth:
        growth_values = matched.groupby(["evidence", "calendar_year"], as_index=False).growth_index.first()
        demand_growth_chart = method_base.mark_line(point=True).encode(
            y=alt.Y("growth_index:Q", title="First common year = 1", scale=alt.Scale(zero=False)),
            tooltip=["evidence:N", "calendar_year:O", alt.Tooltip("growth_index:Q", format=".3f")],
        ).properties(data=growth_values, width=760, height=180,
                     title="All five classes have the same normalized growth trajectory")
    else:
        demand_growth_chart = class_grid(method_base.mark_line(point=True).encode(
            y=alt.Y("growth_index:Q", title="First common year = 1", scale=alt.Scale(zero=False))), shared_y=True)
    attribution = diagnosis["fleet"]["demand_attribution"]
    attribution = attribution.loc[attribution.region.eq(region.value)]
    first_projection = attribution.loc[attribution.period.eq(attribution.current_baseline_year)]
    mo.vstack([
        mo.md("""
        ## 4. Demand: calendar alignment, baseline and growth method

        Both methods anchor demand to actual **2023 NRCan observations**, including
        real-GDP growth to the backend's 2025 base year. The revised notebook uses a
        workbook column named `gdp` that equals **GDP-deflator growth**; the backend
        selects **real GDP**. Both map period 2025 to calendar
        year 2030. These two known calendar interpretations share the primary axis.

        The supplied SQLite's demand equals the original legacy baseline; notes cite
        CEUD 2021 / CER 2023. Its exact calendar interpretation remains unresolved, so
        native stored labels stay in the audit rather than being forced onto this axis.
        It is not evidence that the revised recipe was executed. Service-km means
        passenger-km for passenger classes and tonne-km for freight.
        """), display_chart(demand_chart),
        mo.md("""
        ### Growth shape without the initial level difference

        Index each method to its own value at the first common calendar year. The
        Class growth paths that coincide are shown once; physical plots above retain
        their different levels. The
        additive table below changes baseline, anchor and method in that order;
        the method remainder is not an isolated causal estimate.
        """), display_chart(demand_growth_chart),
        mo.ui.table(first_projection[[
            "road_class", "reference", "notebook_recipe_demand_bn", "current", "replay_relative_percent",
            "stored_to_recipe_change_bn", "baseline_change_bn", "anchor_change_bn", "growth_driver_change_bn",
        ]].rename(columns={
            "road_class": "Class", "reference": "Stored (native label)", "notebook_recipe_demand_bn": "Recipe replay (bn)",
            "current": "Backend (bn)", "replay_relative_percent": "Backend / replay − 1 (%)",
            "stored_to_recipe_change_bn": "Stored → recipe (bn)", "baseline_change_bn": "Baseline effect (bn)",
            "anchor_change_bn": "Anchor effect (bn)", "growth_driver_change_bn": "Method remainder (bn)",
        }), selection=None, pagination=False, format_mapping={"Backend / replay − 1 (%)": "{:+.2f}%"},
            label="First projection: ordered bridge by native label; stored-year equivalence remains unresolved"),
        mo.accordion({
            "Methods and calendar interpretation": mo.ui.table(diagnosis["fleet"]["demand_method_context"], selection=None, pagination=False),
            "Baseline equivalence": mo.ui.table(diagnosis["fleet"]["demand_baseline_comparison"], selection=None, pagination=False),
            "Observed anchor to model base year": mo.ui.table(diagnosis["fleet"]["demand_base_year_bridge"], selection=None, pagination=False),
            "Full attribution and residual": mo.ui.table(attribution, selection=None, page_size=10),
            "All trajectories and unmatched stored years": mo.ui.table(demand_methods, selection=None, page_size=10),
            "Original period-label comparisons": mo.ui.table(diagnosis["fleet"]["demand_comparison"], selection=None, page_size=10),
        }),
    ])
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
        }),
    ])})
    return


if __name__ == "__main__":
    app.run()
