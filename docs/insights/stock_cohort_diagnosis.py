"""Read-only stock/cohort diagnosis using the configured transportation research report.

Owner: validation.transportation_research; source layer: output_validation.
This documentation notebook does not publish parameters or acquire upstream code.
"""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import numpy as np
    import pandas as pd
    from validation.transportation_research import load_validation_diagnosis

    STOCK_LABELS = {
        "cars": "Cars", "passenger_light_trucks": "Light trucks",
        "freight_light_trucks": "Light trucks", "medium_trucks": "Medium trucks",
        "heavy_trucks": "Heavy trucks",
    }
    STOCK_ORDER = ["Cars", "Light trucks", "Medium trucks", "Heavy trucks"]
    TEMOA_COMMIT = "cb2f51503ddb4dc6607c73635098d34c5e2d1283"
    TEMOA_SOURCE = f"https://github.com/TemoaProject/temoa/blob/{TEMOA_COMMIT}/temoa"

    def stock_grid(chart, paired_scales=False):
        if paired_scales:
            rows = [chart.transform_filter(alt.FieldOneOfPredicate(field="Class", oneOf=names)).properties(
                width=370, height=215).facet(column=alt.Column("Class:N", sort=names, title=None)).resolve_scale(y="shared")
                for names in [STOCK_ORDER[:2], STOCK_ORDER[2:]]]
            return alt.vconcat(*rows, spacing=20).resolve_scale(y="independent")
        return chart.properties(width=370, height=190).facet(
            facet=alt.Facet("Class:N", sort=STOCK_ORDER, title=None), columns=2,
        ).resolve_scale(x="shared", y="independent")

    def display_table(frame, **options):
        formats = {column: "{:,.4f}" for column in frame.select_dtypes(include="floating").columns}
        formats.update(options.pop("format_mapping", {}))
        return mo.ui.table(frame, format_mapping=formats, **options)

    def display_chart(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def period_survival(parameters, curves, start, length):
        """Evaluate native annual fractions; require all years, never fill missing support."""
        keys = ["region", "tech", "vintage"]
        if length <= 0 or parameters.duplicated(keys).any():
            raise ValueError("Positive period length and unique capacity keys required")
        years = pd.DataFrame({"period": range(start, start + length)})
        requested = parameters[keys].merge(years, how="cross")
        annual = requested.merge(curves[keys + ["period", "fraction"]],
                                 on=keys + ["period"], how="left", validate="one_to_one")
        if (annual.fraction.isna().any() or not np.isfinite(annual.fraction).all()
                or not annual.fraction.between(0, 1).all()):
            raise ValueError("Native curves lack complete physical annual period support")
        return annual.groupby(keys, as_index=False).fraction.mean().rename(
            columns={"fraction": "period_survival_fraction"})

    return (STOCK_LABELS, STOCK_ORDER, TEMOA_COMMIT, TEMOA_SOURCE, alt, display_chart,
            display_table, load_validation_diagnosis, mo, np, pd, period_survival, stock_grid)


@app.cell
def _(load_validation_diagnosis):
    diagnosis = load_validation_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(TEMOA_COMMIT, TEMOA_SOURCE, mo):
    mo.md(rf"""
    # Stock cohorts: what does Temoa retire?

    **Confirmed in current Temoa: with survival curves, `existing_capacity` is the original
    installed cohort at its vintage — sales/additions equivalent, before cumulative retirement.**
    Supplying stock already reduced to the base year applies historical survival twice.
    With no curve, Temoa applies fixed end-of-life retirement, including a partial final period;
    it does not apply historical gradual survival. Base-year surviving stock is therefore the
    appropriate existing-fleet input for that fixed representation.

    This is a diagnostic assessment. Production methods, scenarios and captured inputs are unchanged.
    The current sales-based validation scenario already has curves **off** and rejects combining
    that method with curves. The separate stock-age curve path needs further compatibility work.

    Inspected upstream `main` on **6 November 2026**, pinned to [`{TEMOA_COMMIT[:12]}`](https://github.com/TemoaProject/temoa/commit/{TEMOA_COMMIT}).
    The notebook runs offline against the configured, freshness-checked comparison capture.
    [Native backend/legacy comparison](transportation_validation_diagnosis.py) retains both
    the actual parameter values and the separately derived survivors.

    For a vintage \(v\), original installed capacity \(E_v\), model period \(p\) of length \(L_p\),
    and absolute annual survival \(S_{{y,v}}\), without optional early retirement:

    \[
    C_{{p,v}}=E_v F_{{p,v}},\qquad
    F^{{curve}}_{{p,v}}=\frac{{1}}{{L_p}}\sum_{{y=p}}^{{p+L_p-1}} S_{{y,v}}.
    \]

    There is **no division by base-year survival**. The fixed-lifetime branch uses
    \(F^{{fixed}}_{{p,v}}=\min(1,(v+\ell_v-p)/L_p)\) **only for active processes**;
    vintages with \(p\ge v+\ell_v\) are excluded first.
    See [adjusted capacity]({TEMOA_SOURCE}/components/capacity.py#L471),
    [life fraction]({TEMOA_SOURCE}/components/technology.py#L116), and
    [active-process filtering]({TEMOA_SOURCE}/components/technology.py#L154).
    """)
    return


@app.cell
def _(diagnosis, mo):
    regions = sorted(diagnosis["fleet"]["stock_activity_energy"].region.unique())
    region = mo.ui.dropdown(regions, value=regions[0], label="Comparison region")
    region
    return (region,)


@app.cell(hide_code=True)
def _(STOCK_LABELS, alt, diagnosis, display_chart, mo, region, stock_grid, display_table):
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
        color=alt.Color("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=cohort_legend, range=cohort_colors),
                        legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        order=alt.Order("Component:N", sort="descending"), tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    allocated_line = alt.Chart().transform_filter(alt.datum.Kind == "Model").mark_line(strokeWidth=1.8).encode(
        x="vintage_year:O", y=alt.Y("Vehicles:Q", stack=None), order="vintage_year:O",
        color=alt.Color("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=cohort_legend, range=cohort_colors),
                        legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        strokeDash=alt.StrokeDash("Component:N", title="Stock flow / allocation", scale=alt.Scale(domain=cohort_legend, range=[[1, 0], [1, 0], [1, 0], [5, 3]]),
                                  legend=alt.Legend(orient="right", direction="vertical", columns=1, labelLimit=280, symbolType="stroke", symbolSize=350)),
        tooltip=["Class:N", "vintage_year:O", "Component:N", alt.Tooltip("Vehicles:Q", format=",.3f")],
    )
    retirement_chart = stock_grid(alt.layer(retirement_bars, allocated_line, data=portions))
    cutoffs = diagnosis["fleet"]["backend_lifetime_exclusions"]
    cutoffs = cutoffs.loc[cutoffs.region.eq(region.value)].assign(Class=lambda frame: frame.road_class.map({
        **STOCK_LABELS, "passenger_light_trucks": "Passenger light trucks", "freight_light_trucks": "Freight light trucks",
    }))
    mo.vstack([mo.md("""
    ## 1. Current backend: annual stock and cohort lineage

    **Backend only:** blue + grey reconstruct raw sales; blue is sales × cumulative annual survival at 2025.
    Orange scales those survivors by **CEUD stock / raw surviving stock**. Dashed allocation then redistributes
    excluded stock proportionally **within each technology**, preserving its total. CEUD equality is a calibration identity.

    **Only when `survival_curves: false` (this scenario)**, eligibility requires both
    `annual year + lifetime > 2025` and `model vintage + lifetime > 2025`.
    **Annual year** is the actual sales calendar year. **Model vintage** is the five-year model label:
    a vehicle sold in **2013** belongs to model vintage **2010**, representing 2011–2015.
    The first test checks the real cohort's age; the second checks the model's coarser representation.
    Lifetimes are **16 years (cars), 17 (light trucks), 20 (medium/heavy)**. Vintage 2005 represents 2006–2010:
    even `2005 + 20 = 2025` fails the strict endpoint. Vintage 2010 represents 2011–2015 and passes all three.
    That explains the common **2011 cutoff**. Annual-only cutoffs would be 2010 / 2009 / 2006 respectively.
    This is an effect of the current fixed-lifetime representation, not missing aggregation bins.
    With curves enabled on supported stock paths, eligibility checks accepted curve-age coverage and the
    full model-vintage age block; it does **not** apply these median cutoffs. The selected sales-survival
    reconstruction already applies retirement and its configuration guard rejects combining it with curves.
    That guard protects this fixed-lifetime path; it does not establish compatibility of other stock paths with Temoa curves.
    """), display_chart(retirement_chart), display_table(cutoffs[[
        "Class", "fixed_lifetime_years", "all_excluded_k_vehicles", "total_redistributed_percent",
        "coarse_vintage_exclusion_k_vehicles", "coarse_exclusion_percent",
    ]].rename(columns={"fixed_lifetime_years": "Lifetime (years)", "all_excluded_k_vehicles": "All redistributed (k)",
        "total_redistributed_percent": "All / CEUD (%)", "coarse_vintage_exclusion_k_vehicles": "Extra from model-vintage test (k)",
        "coarse_exclusion_percent": "Extra / CEUD (%)"}), selection=None, pagination=False,
        label="Separate service classes retained for the exclusion audit; light-truck stock plots combine them"),
        mo.accordion({"Raw → scaled → allocated class totals and explanations": display_table(
            diagnosis["fleet"]["fleet_reconciliation"], selection=None, page_size=5)})])
    return (retirement_chart,)


@app.cell(hide_code=True)
def _(alt, display_chart, display_table, mo, pd):
    demonstration = pd.DataFrame({
        "Case": ["Original cohort + curve", "Surviving stock + curve",
                 "Stock + fixed full period", "Stock + fixed partial period"],
        "Input (vehicles)": [100., 60., 60., 60.],
        "Period fraction": [.5, .5, 1., .6],
        "Effective capacity (vehicles)": [50., 30., 60., 36.],
        "Meaning": ["Correct curve input", "Double retirement", "No smooth survival", "Three years of five remain"],
    })
    demonstration_chart = alt.Chart(demonstration).mark_bar().encode(
        y=alt.Y("Case:N", title=None, sort=demonstration.Case.tolist()),
        x=alt.X("Effective capacity (vehicles):Q", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("Meaning:N", scale=alt.Scale(
            domain=demonstration.Meaning.tolist(), range=["#2166ac", "#b2182b", "#438564", "#a6761d"]),
            legend=None),
        tooltip=["Case:N", "Input (vehicles):Q", "Period fraction:Q", "Effective capacity (vehicles):Q"],
    ).properties(width=680, height=180)
    mo.vstack([mo.md("""
    ## 2. Direct Temoa function check: double retirement and the fixed case

    **Illustrative inputs, not fleet assumptions:** 100 vehicles installed at vintage 2010;
    annual survival during 2025–2029 is 60%, 55%, 50%, 45%, 40%. Beginning-2025 stock is 60;
    mean surviving capacity is 50. If 60 is supplied instead of 100, Temoa returns **30**.

    For the fixed examples, the supplied stock is 60. Lifetime 30 covers the entire period;
    lifetime 18 ends in 2028, so only 2025–2027 contribute: **60 × 3/5 = 36**.
    Stock at a single date and period-average capacity are distinct even with fixed lifetimes.

    All four values were asserted by calling the **actual upstream-equivalent**
    `param_process_life_fraction_rule` and `adjusted_capacity_constraint`, with Pyomo expressions.
    This verifies constraint arithmetic, not optimization or a complete fleet solve.
    """), display_chart(demonstration_chart), display_table(demonstration, selection=None, pagination=False)])
    return demonstration, demonstration_chart


@app.cell(hide_code=True)
def _(STOCK_LABELS, alt, diagnosis, display_chart, display_table, mo, pd, period_survival, region, stock_grid):
    legacy_parameters = diagnosis["fleet"]["legacy_reconstructed_parameters"]
    legacy_parameters = legacy_parameters.loc[legacy_parameters.region.eq(region.value)].copy()
    first_period = min(diagnosis["fleet_metadata"]["scenario"]["periods"]["model"])
    period_length = diagnosis["fleet_metadata"]["scenario"]["periods"]["step"]
    legacy_periods = period_survival(legacy_parameters,
        diagnosis["fleet"]["legacy_reconstructed_survival"], first_period, period_length)
    legacy_capacity = legacy_parameters.merge(legacy_periods, on=["region", "tech", "vintage"], validate="one_to_one")
    legacy_capacity["period_capacity"] = legacy_capacity.raw_capacity_k_vehicles * legacy_capacity.period_survival_fraction
    legacy_capacity["double_retired_capacity"] = legacy_capacity.surviving_k_vehicles * legacy_capacity.period_survival_fraction
    legacy_capacity["Class"] = legacy_capacity.road_class.map(STOCK_LABELS)
    capacity_labels = {
        "raw_capacity_k_vehicles": "Native parameter: acquisitions",
        "surviving_k_vehicles": f"Point stock: {first_period}",
        "period_capacity": f"Temoa formula mean: {first_period}–{first_period + period_length - 1}",
        "double_retired_capacity": "If survivors were supplied",
    }
    basis_totals = legacy_capacity.groupby("Class", as_index=False)[list(capacity_labels)].sum()
    basis_values = basis_totals.melt(id_vars="Class", var_name="Basis", value_name="Thousand vehicles")
    basis_values["Basis"] = basis_values.Basis.map(capacity_labels)
    basis_chart = stock_grid(alt.Chart(basis_values).mark_bar().encode(
        x=alt.X("Basis:N", title=None, sort=list(capacity_labels.values()), axis=alt.Axis(labels=False, ticks=False)),
        y=alt.Y("Thousand vehicles:Q", scale=alt.Scale(zero=True)),
        color=alt.Color("Basis:N", title=None, scale=alt.Scale(domain=list(capacity_labels.values()),
            range=["#7b3294", "#2166ac", "#438564", "#b2182b"]),
            legend=alt.Legend(orient="right", direction="vertical", labelLimit=260)),
        tooltip=["Class:N", "Basis:N", alt.Tooltip("Thousand vehicles:Q", format=",.4f")],
    ))
    mo.vstack([mo.md(f"""
    ## 3. Native legacy acquisitions, survivors and Temoa period capacity

    **Purple is the actual native `ExistingCapacity` export.** Blue is the point-{first_period}
    stock used in the earlier comparison. Green applies current Temoa's period-mean formula to
    those native acquisitions and their own rounded annual workbook curve. Red is the deliberately
    incorrect counterfactual of supplying already-surviving stock to the same curve.
    No backend survival curve, CEUD multiplier or fixed-lifetime redistribution is imposed on legacy.

    One bar represents a class total; light-truck passenger/freight services are combined.
    Axes are independent across classes; all values are thousand vehicles. This is a capacity-only
    calculation with complete {first_period}–{first_period + period_length - 1} curve support checked;
    it excludes early decision retirement and is **not a solver run**. No modified parameter is emitted.
    Full-horizon compatibility is not established: the native recipe assigns lifetime 42, but its
    rounded car curve reaches zero at **age 32**. Current Temoa rejects nonfinal zero fractions.
    A direct call to `create_survival_curve` rejected `ON / T_LDV_C_GSL / vintage 2005`
    at calendar 2037 (age 32), before its declared end in 2047. All 17 positive existing car
    capacity keys share this incompatibility, covering 4,356.854 thousand acquisitions.
    These native rows are preserved as evidence; the green bar evaluates only the requested
    period's arithmetic and does not certify a loadable legacy model.

    Legacy UF is independently calibrated against **2023** activity using
    `Σ(raw capacity × survival at 2023 × own capacity-to-activity)`.
    The earlier point-2025 stock/energy calculations remain legitimate descriptive accounting,
    but they are not Temoa's period-average capacity or a simulated dispatch result.
    """), display_chart(basis_chart), display_table(basis_totals.rename(columns=capacity_labels),
        selection=None, pagination=False, label="Native legacy only; every numerical column is thousand vehicles"),
        mo.accordion({"Per-technology and vintage audit": display_table(legacy_capacity[[
            "road_class", "tech", "vintage", "raw_capacity_k_vehicles", "baseline_survival_fraction",
            "surviving_k_vehicles", "period_survival_fraction", "period_capacity", "double_retired_capacity",
        ]], selection=None, page_size=10)})])
    return basis_chart, basis_totals, legacy_capacity


@app.cell(hide_code=True)
def _(STOCK_LABELS, alt, diagnosis, display_chart, display_table, mo, region, stock_grid):
    fixed_annual = diagnosis["fleet"]["sales_cohort_reconciliation"].copy()
    fixed_annual = fixed_annual.loc[fixed_annual.region.eq(region.value)]
    fixed_step = diagnosis["fleet_metadata"]["scenario"]["periods"]["step"]
    if fixed_annual.fixed_lifetime_years.isna().any():
        raise ValueError("This fixed-mode comparison requires explicit fixed lifetimes")
    fixed_annual["period_fraction"] = (
        (fixed_annual.vintage + fixed_annual.fixed_lifetime_years - fixed_annual.first_model_period)
        / fixed_step).clip(lower=0, upper=1)
    fixed_annual["effective_capacity"] = fixed_annual.inserted_cohort_k_vehicles * fixed_annual.period_fraction
    fixed_annual["Class"] = fixed_annual.road_class.map(STOCK_LABELS)
    fixed_vintages = fixed_annual.groupby(["Class", "vintage"], as_index=False).agg(
        inserted_stock=("inserted_cohort_k_vehicles", "sum"),
        effective_capacity=("effective_capacity", "sum"),
    )
    fixed_vintages = fixed_vintages.loc[fixed_vintages.inserted_stock.gt(0)]
    fixed_labels = {"inserted_stock": "Inserted surviving stock", "effective_capacity": "Mean capacity: 2025–2029"}
    fixed_values = fixed_vintages.melt(id_vars=["Class", "vintage"], var_name="Basis", value_name="Thousand vehicles")
    fixed_values["Basis"] = fixed_values.Basis.map(fixed_labels)
    fixed_chart = stock_grid(alt.Chart(fixed_values).mark_bar().encode(
        x=alt.X("vintage:O", title="Model vintage", axis=alt.Axis(labelAngle=0)),
        xOffset=alt.XOffset("Basis:N", sort=list(fixed_labels.values())),
        y=alt.Y("Thousand vehicles:Q", scale=alt.Scale(zero=True)),
        color=alt.Color("Basis:N", title=None, scale=alt.Scale(domain=list(fixed_labels.values()),
            range=["#2166ac", "#438564"]), legend=alt.Legend(orient="right", direction="vertical", labelLimit=260)),
        tooltip=["Class:N", "vintage:O", "Basis:N", alt.Tooltip("Thousand vehicles:Q", format=",.4f")],
    ))
    mo.vstack([mo.md("""
    ## 4. Curves off: current backend stock still faces fixed retirement

    No historical smooth-survival multiplier is applied here. Blue is the current inserted
    surviving stock; green applies Temoa's fixed remaining-life fraction over 2025–2029.
    The vintage-2010 cars have lifetime 16, hence only **2025** remains: their mean capacity
    is **one fifth** of inserted stock. Light trucks have two years left; medium/heavy trucks have all five.
    Passing the backend's eligibility test means alive at the start, not alive for the whole period.

    Each pair is one class/vintage, in thousand vehicles; only positive inserted cohorts appear.
    This calculation uses the captured fixed lifetime and vintage for every annual contribution,
    with zero outside viable years. It excludes early decision retirement and is not a model solve.
    The existing UF/activity calibration is based on inserted stock, so those earlier descriptive
    comparisons do not by themselves establish first-period model service closure. Production
    parameters are unchanged; period averaging is a separate issue from double smooth retirement.
    """), display_chart(fixed_chart), display_table(fixed_vintages.rename(columns=fixed_labels),
        selection=None, pagination=False, label="Fixed-mode stock and effective first-period capacity (thousand vehicles)")])
    return fixed_chart, fixed_vintages


@app.cell(hide_code=True)
def _(TEMOA_SOURCE, mo):
    mo.md(rf"""
    ## 5. What this means for the backend

    | Configuration | Appropriate existing-fleet basis | What Temoa does |
    |---|---|---|
    | Absolute survival curve present for this process | Original installations or a reviewed sales-equivalent cohort | Applies absolute annual survival, then averages across the period |
    | No survival curve for this process | Surviving base-year stock assigned to viable historical vintages | Retains full capacity until fixed EOL; prorates a final partial period |

    **Raw sales are not the only possible curve-compatible calibration.** To reproduce a reviewed
    beginning-of-horizon cohort stock \(B_v\), the original-installation equivalent would be
    \(E_v=B_v/S_{{p_0,v}}\), for positive survival. For a period-mean target, the divisor would
    instead be \(F_{{p_0,v}}\). These are different modeling choices. Temoa does neither conversion
    for the modeler; conditional survival \(S_y/S_{{p_0}}\) is not its current existing-capacity rule.
    A future change must decide which quantity CEUD calibration represents before changing capacity,
    UF or cohort weights. Five-year representative vintages also alter age relative to annual sales.

    **Current production exposure:** [the scenario validator](../../src/validation/config_models.py)
    already forbids `sales_survival` together with `survival_curves: true`. Both the current default
    and validation scenario use fixed lifetimes. This assessment therefore does not establish
    double retirement in the current sales-based validation build. The separate `stock_age` path
    distributes reported surviving stock and permits curves; a future curve-compatible design
    must review that path as well.

    **A second, independent interface issue:**
    [the backend curve-row producer](../../src/parameterization/road_lifetimes_survival.py)
    currently emits survival averaged within model-period age blocks, at model-period labels.
    Current Temoa instead expects **annual calendar-year survival**, including `S(v)=1` and
    `S(v+lifetime)=0`, then interpolates any intermediate gaps and computes period averages itself.
    A model-period-only table lacks required historical endpoints and cannot be treated as a verified
    input to current Temoa. Fixing capacity alone would not establish compatibility.
    See [curve validation/interpolation]({TEMOA_SOURCE}/components/technology.py#L244).

    The legacy notebook's own `fixed_lifetime=True` option creates a step-shaped survival table;
    that option is not the same as omitting curves from Temoa. Its active call uses `False`.

    Curve activation is **per region, technology and vintage**, determined by presence of curve rows;
    it is not a global Temoa switch. The loader passes
    [existing capacity]({TEMOA_SOURCE}/data_io/hybrid_loader.py#L616) and
    [survival values]({TEMOA_SOURCE}/data_io/hybrid_loader.py#L738) through without rebasing them.
    Optional early retirement is handled separately: the
    [capacity rule]({TEMOA_SOURCE}/components/capacity.py#L471) divides those retired amounts by
    survival at retirement to subtract original-installation equivalents. Myopic carryover similarly
    [adjusts original capacity]({TEMOA_SOURCE}/components/utils.py#L141); neither mechanism
    converts a residual input stock into its original cohort.

    **Decision boundary:** preserve the current fixed-lifetime workflow while reviewing a coherent
    curve-mode contract for capacity, annual curve rows, representative vintages and utilization.
    This notebook establishes the direction of that review; it does not promote a replacement method.
    """)
    return


@app.cell(hide_code=True)
def _(TEMOA_SOURCE, mo):
    mo.accordion({"Reproduce the actual-function check against a verified Temoa checkout": mo.md(rf"""
    The inspected local checkout was at `7f45f47494194fa310035c1e94d4eedb76db73b6`.
    `technology.py`, `utils.py` and process-viability source matched pinned upstream main;
    the complete `adjusted_capacity_constraint` function also matched. Other capacity-file
    content on that local feature branch differs, so this claim is scoped to the functions tested.
    Run the following in a Temoa environment with Pyomo after checking those source identities.
    It creates no database, does not optimize, and writes no parameters.

    ```python
    from collections import defaultdict
    from types import SimpleNamespace
    from pyomo.environ import ConcreteModel, Var, value
    from temoa.components.capacity import adjusted_capacity_constraint
    from temoa.components.technology import param_process_life_fraction_rule

    r, t, p, v = "R", "vehicle", 2025, 2010
    for amount, curve, life, expected in [
        (100, True, 30, 50), (60, True, 30, 30),
        (60, False, 30, 60), (60, False, 18, 36),
    ]:
        pm = ConcreteModel()
        pm.cap = Var()
        model = SimpleNamespace(
            time_exist=[v], time_optimize=[p], tech_retirement=set(),
            existing_capacity={{(r, t, v): amount}},
            retired_existing_capacity=defaultdict(float),
            lifetime_survival_curve=defaultdict(
                lambda: 0 if curve else 1,
                {{(r, y, t, v): s for y, s in
                 zip(range(p, p + 5), [.6, .55, .5, .45, .4])}} if curve else {{}}),
            period_length={{p: 5}}, lifetime_process={{(r, t, v): life}},
            is_survival_curve_process={{(r, t, v): curve}},
            v_capacity={{(r, p, t, v): pm.cap}},
        )
        fraction = param_process_life_fraction_rule(model, r, p, t, v)
        model.process_life_frac = {{(r, p, t, v): fraction}}
        rhs = value(adjusted_capacity_constraint(model, r, p, t, v).args[1])
        assert abs(rhs - expected) < 1e-12
        print(amount, curve, fraction, rhs)
    ```

    These stand-ins test the numerical constraints after model construction. Curve endpoint
    validation and process filtering were inspected separately in
    [technology.py]({TEMOA_SOURCE}/components/technology.py#L244); the five illustrative
    annual fractions alone are not a complete loadable lifetime curve.
    """)})
    return


if __name__ == "__main__":
    app.run()
