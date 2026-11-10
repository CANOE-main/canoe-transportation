"""Vehicle investment, maintenance and repair from a recorded scenario build."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from validation.notebook_figures import source_markdown
    import marimo as mo
    from validation.vehicle_cost_diagnostic import (
        ROAD_MODES, investment_trajectory_chart, load_diagnostic,
        maintenance_age_chart, range_cost_chart, service_maintenance_chart,
        source_price_chart,
    )

    def show_chart(chart):
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def show_table(frame, label):
        return mo.ui.table(frame, label=label, selection=None, page_size=12,
                           show_data_types=False, show_column_summaries=False)

    return (ROAD_MODES, investment_trajectory_chart, load_diagnostic, maintenance_age_chart,
            mo, range_cost_chart, service_maintenance_chart, show_chart, show_table, source_price_chart, source_markdown)


@app.cell
def _(load_diagnostic, mo):
    try:
        diagnostic = load_diagnostic()
        missing_evidence = None
    except FileNotFoundError as error:
        diagnostic = None
        missing_evidence = str(error)
    mo.stop(missing_evidence is not None, mo.callout(missing_evidence or "", kind="warn"))
    return (diagnostic,)


@app.cell
def _(diagnostic, mo):
    scenario = diagnostic.metadata["scenario"]
    mo.md(f"""
    # What determines vehicle investment, maintenance and repair costs?

    Compare source-class prices, emitted investment costs and age-dependent maintenance and repair (M&R).
    These are **parameter diagnostics**, not optimized vehicle choices or independent validation of empirical assumptions.

    **Recorded scenario:** {scenario['scenario']['name']} · [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) {diagnostic.metadata['trajectory']} ·
    {scenario['economics']['cost_reference_year']} CAD · range representation
    `{scenario['BEV_PHEV_range_representation']['mode']}` · build {diagnostic.metadata['build_timestamp_utc']}.
    **Evidence capture:** `{diagnostic.metadata['capture_id']}`.

    Reading this notebook never runs ETL or changes a scenario. To replace the capture, first build the intended scenario,
    then explicitly run:

    ```sh
    uv run python -m validation.vehicle_cost_diagnostic --scenario {diagnostic.metadata['scenario_path']} --write-report
    ```

    Every plotted source year is distinct from its dollar year. A model vintage can use interval-end price evidence;
    the tables retain both. Fuel purchases are outside these M&R costs.
    """)
    return (scenario,)


@app.cell
def _(diagnostic, mo):
    source_classes = sorted(diagnostic.frames["source_investment"].vehicle_class.unique())
    source_class = mo.ui.dropdown(source_classes, value="Compact" if "Compact" in source_classes else source_classes[0], label="Source vehicle class")
    price_basis = mo.ui.dropdown({"Manufacturing estimate": "manufacturing", "Retail MSRP": "retail"}, value="Manufacturing estimate", label="Price basis")
    currency = mo.ui.dropdown({"Common CAD dollar year": "CAD", "Source-native currency and year": "native"}, value="Common CAD dollar year", label="Money basis")
    mo.hstack([source_class, price_basis, currency], justify="start")
    return currency, price_basis, source_class


@app.cell
def _(currency, diagnostic, mo, price_basis, show_chart, source_class, source_price_chart):
    source_figure = source_price_chart(diagnostic, vehicle_class=source_class.value, basis=price_basis.value, currency=currency.value)
    mo.vstack([mo.md("""
    ## 1. What price is being represented?

    [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) supplies retail-price-equivalent projections. Production estimates manufacturing cost by dividing by
    the configured family markup, **1.5 for LDVs and 1.2 for MHDVs**. This does not change the retail MSRP used
    in the repair equation. The Conservative case is a manufacturing-cost proxy obtained from AEO retail prices.

    Differences between powertrains can reflect source architecture and component assumptions. A lower FCEV
    price than a particular BEV range is not automatically an error, nor a ranking that holds in every year.
    Lines join registered source projection points; they do not add unsupported endpoint extrapolation.
    """), show_chart(source_figure)])
    return (source_figure,)


@app.cell
def _(ROAD_MODES, diagnostic, mo):
    regions = sorted(diagnostic.frames["investment"].region.unique())
    region = mo.ui.dropdown(regions, value="ON" if "ON" in regions else regions[0], label="Region")
    modes = {label: key for key, label in ROAD_MODES.items() if key in set(diagnostic.frames["investment"]["mode"])}
    road_class = mo.ui.dropdown(modes, value="Cars" if "Cars" in modes else next(iter(modes)), label="Model vehicle class")
    vintages = sorted(map(int, diagnostic.frames["investment"].vintage.unique()))
    vintage = mo.ui.dropdown(vintages, value=vintages[0], label="New cohort vintage for M&R")
    mo.hstack([region, road_class, vintage], justify="start")
    return region, road_class, vintage


@app.cell
def _(diagnostic, investment_trajectory_chart, mo, region, road_class, show_chart, show_table):
    investment_figure = investment_trajectory_chart(diagnostic, region=region.value, mode=road_class.value)
    investment_rows = diagnostic.frames["investment"].loc[lambda frame: frame.region.eq(region.value) & frame["mode"].eq(road_class.value)]
    mo.vstack([mo.md("""
    ## 2. How do composition and calendar choices change investment costs?

    The emitted values combine the selected source trajectory with reviewed vehicle-class weights and, when
    selected, range representation. For road investment, million CAD per thousand vehicles equals thousand CAD
    per vehicle. It is a capacity cost; it is not a cost per passenger-km or tonne-km.

    The source and final views answer different questions. Use the audit to trace a changed level to class weights,
    source years, retail-price treatment or money conversion before treating a discontinuity as a defect.
    """), show_chart(investment_figure), show_table(investment_rows[["tech", "vintage", "purchase_evidence_year", "cost", "units"]].rename(columns={
        "tech": "Technology", "vintage": "Model vintage", "purchase_evidence_year": "Price source year", "cost": "Investment cost", "units": "Units"}), "Emitted investment rows: one region, technology and vintage")])
    return (investment_figure,)


@app.cell
def _(diagnostic, mo):
    maintenance_family = mo.ui.dropdown({"LDV (Burnham)": "ldv", "MHDV (BEAN)": "mhdv"}, value="LDV (Burnham)", label="Empirical model")
    maintenance_years = sorted(map(int, diagnostic.frames["source_investment"].source_year.unique()))
    maintenance_year = mo.ui.dropdown(maintenance_years, value=2025 if 2025 in maintenance_years else maintenance_years[0], label="LDV MSRP source year")
    mo.hstack([maintenance_family, maintenance_year], justify="start")
    return maintenance_family, maintenance_year


@app.cell
def _(diagnostic, maintenance_family, mo):
    maintenance_classes = sorted(diagnostic.frames["source_maintenance"].loc[lambda frame: frame.family.eq(maintenance_family.value), "vehicle_class"].unique())
    maintenance_class = mo.ui.dropdown(maintenance_classes, value="Compact" if "Compact" in maintenance_classes else maintenance_classes[0], label="Empirical source class")
    maintenance_class
    return (maintenance_class,)


@app.cell
def _(currency, diagnostic, maintenance_age_chart, maintenance_class, maintenance_family, maintenance_year, mo, show_chart):
    age_figure = maintenance_age_chart(diagnostic, family=maintenance_family.value, vehicle_class=maintenance_class.value,
                                      source_year=maintenance_year.value, currency=currency.value)
    mo.vstack([mo.md(r"""
    ## 3. Why do maintenance and repairs vary with age?

    LDV repair cost combines a vehicle-class multiplier, a powertrain multiplier, an age coefficient and retail MSRP.
    Maintenance adds a separate powertrain-specific rate. In source dollars per vehicle-mile:

    \[R_{a,c,p}=s_c m_p b_{\min(a,a_*)}\exp(\beta P_{c,p,y}),\qquad V_{a,c,p}=R_{a,c,p}+M_p.\]

    Here \(a\) is age in years, \(c\) class, \(p\) powertrain and \(y\) the MSRP source year;
    \(s_c,m_p\) are dimensionless, \(b_a,M_p\) are USD/mile, \(P\) is retail USD/vehicle and
    \(\beta\) is the reciprocal-price coefficient. Production holds \(b_a\) after \(a_*=14\).
    MSRP is **2022 USD**, while the empirical coefficients/output basis are **2020 USD**: this existing mismatch
    is exposed rather than silently rebased inside the exponential.

    [Burnham et al. (2021), §3.5.4, Eq. 3.13 and Table 3.17](https://publications.anl.gov/anlpubs/2021/05/167399.pdf#page=116)
    describes the repair model. Its Edmunds inputs estimate repairs from extended-warranty costs;
    they are not direct repair bills. The ten-year evidence combines two model years, with sparse
    alternative-powertrain coverage. Scheduled maintenance is described separately in §3.5.3/Table 3.16.
    The backend age-14 hold is distinct from the empirical source's numbered years.

    MHDV total M&R uses \(V_{a,c,p}=m_p(A_{c,p}a+B_{c,p})\), with \(A\) in USD/(mile·year),
    \(B\) in USD/mile and a dimensionless multiplier. The registered evidence does not establish a validated
    age domain. Displaying a long-age curve therefore does not establish empirical support at that age.
    [Islam et al. (2022), §3.3](https://publications.anl.gov/anlpubs/2023/11/179337.pdf#page=88)
    describes [*ANL BEAN*](https://publications.anl.gov/anlpubs/2023/11/179337.pdf)'s use of Burnham's methods; registered workbook cells supply the coefficients here.
    """), show_chart(age_figure)])
    return (age_figure,)


@app.cell
def _(diagnostic, mo, region, road_class, service_maintenance_chart, show_chart, show_table, vintage):
    service_figure = service_maintenance_chart(diagnostic, region=region.value, mode=road_class.value, vintage=vintage.value)
    service_rows = diagnostic.frames["maintenance"].loc[lambda frame: frame.region.eq(region.value) & frame["mode"].eq(road_class.value) & frame.vintage.eq(vintage.value) & frame.tech.str.endswith("_N")]
    mo.vstack([mo.md("""
    ## 4. How does a per-mile rate become a service cost?

    Convert money to the common CAD dollar year, convert miles to kilometres, then divide by the captured occupancy
    or payload. Passenger-km and tonne-km denominators remain separate. Bus price-ratio and motorcycle annual-cost
    treatments appear in the source audit; they are not silently replaced with an LDV age equation.

    Period labels, evidence years and active lifetimes determine the ages emitted. Missing later rows mean that a
    process is outside the supported lifetime/coverage; they do not imply zero maintenance cost.
    """), show_chart(service_figure), show_table(service_rows[["tech", "period", "age", "cost", "units", "load_factor", "treatment"]], "Emitted new-cohort M&R with captured age, service denominator and source treatment")])
    return (service_figure,)


@app.cell
def _(diagnostic, mo):
    range_modes = sorted(diagnostic.frames["range_comparison"]["mode"].unique())
    range_mode = mo.ui.dropdown(range_modes, value="cars" if "cars" in range_modes else range_modes[0], label="LDV range class")
    range_family = mo.ui.dropdown(["BEV", "PHEV"], value="BEV", label="Range family")
    mo.hstack([range_mode, range_family], justify="start")
    return range_family, range_mode


@app.cell
def _(diagnostic, mo, range_cost_chart, range_family, range_mode, region, show_chart, show_table):
    range_figure = range_cost_chart(diagnostic, region=region.value, mode=range_mode.value, family=range_family.value)
    mo.vstack([mo.md("""
    ## 5. What is preserved by a representative range cost?

    With common capacity units and the same class composition, the representative purchase cost is the sum of
    each range cost multiplied by its [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) within-family sales share. It sits within the range-cost envelope.
    This illustration compares the arithmetic without changing the selected production representation.

    The US modeled range mix is transferred across regions and vintages. It is neither current Canadian sales
    nor an adoption constraint between BEVs and PHEVs. Equal aggregate investment cost does not establish equal
    operating energy, dispatch or total system cost across complete scenarios.
    """), show_chart(range_figure), show_table(diagnostic.frames["range_shares"], "Captured OMEGA bucket weights and denominator coverage")])
    return (range_figure,)


@app.cell
def _(diagnostic, mo, show_table, source_markdown):
    mo.vstack([mo.md("""
    ## Evidence and remaining limitations

    The source-step table ranks all adjacent [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) price changes by magnitude. This is an investigation aid;
    it applies no arbitrary defect threshold or smoothing. Source changes, architecture differences and
    aggregation can all be consequential without being implementation errors.
    """), show_table(diagnostic.frames["source_steps"][["vehicle_class", "powertrain", "previous_source_year", "source_year", "previous_native_price", "retail_native", "relative_change", "source_unit"]], "Source price steps, largest absolute proportional change first"),
        mo.md(source_markdown("\n".join(f"- {item}" for item in diagnostic.metadata["limitations"]))),
        mo.accordion({"Complete cost and monetary audit": mo.vstack([
            show_table(diagnostic.frames["investment_audit"], "Production investment derivation before optional new-range aggregation"),
            show_table(diagnostic.frames["maintenance_audit"], "Production M&R derivation before optional new-range aggregation")]),
            "Captured sources, build identity and figure generator": mo.json(diagnostic.metadata)})])
    return


if __name__ == "__main__":
    app.run()
