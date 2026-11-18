import marimo

__generated_with = "0.23.0"
app = marimo.App(width="full")


@app.cell
def _():
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    import marimo as mo
    from validation.ceud_calibration_diagnostic import (
        calibration_charts,
        load_regional_calibration_diagnostic,
    )

    return calibration_charts, load_regional_calibration_diagnostic, mo


@app.cell
def _(load_regional_calibration_diagnostic, mo):
    try:
        diagnosis = load_regional_calibration_diagnostic()
    except FileNotFoundError as error:
        mo.stop(True, mo.callout(str(error), kind="warn"))
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    mo.md(rf"""
    # Can energy match CEUD while travel activity stays fixed?

    This optional calibration adjusts vehicle consumption to match reported
    [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
    gasoline/diesel energy for five road classes: cars, passenger and freight light trucks,
    medium trucks and heavy trucks. The reviewed heavy-truck total is entirely diesel;
    it is used once, without adding a separate fuel component.

    Calibration is **off in ordinary scenarios**. This option requires fixed lifetimes
    and flat annual utilization. Survival-curve mode is rejected because some physically
    surviving cohorts lack the compiled activity/efficiency rows required for this fit.

    Each region retains its captured fleet, activity level and representation.
    “Unadjusted” uses that build's source-derived consumption; “Calibrated” applies
    one consumption multiplier per region/class. Fleet, travel and representation stay fixed.

    Calibration is available for every configured province/model region. This notebook
    reads the dedicated **all-region capture**; an Ontario-only paired comparison has
    its own capture and cannot replace the regional heatmap evidence.

    **Recorded build:** `{diagnosis.metadata['build_id']}`  
    **Database identity:** `{diagnosis.metadata['database']['sha256']}`  
    **Scenario:** `{diagnosis.metadata['scenario']['name']}`

    To prepare a new capture, run the recorded command from the repository root:
    `{diagnosis.metadata['prepare_command']}`. Opening this notebook never runs ETL.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What is fitted, and what stays fixed?

    The existing activity calibration assigns annual utilization to physical surviving
    stock so that class activity matches backend projected demand at `periods.base_year`.
    That demand starts from the latest observed [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) activity
    and follows real GDP growth to the base year. Survival-curve mode uses
    installation-equivalent capacity: a cohort quantity to which survival is applied,
    rather than the physical stock remaining at the base year. For region $\boldsymbol{r}$, vehicle
    class $\boldsymbol{c}$, technology $\boldsymbol{i}$ and vintage $\boldsymbol{v}$:

    \[
    A_{riv}=B_{riv}\,q_i\,u_{rc},\qquad
    E^0_{rc,\mathrm{liq}}=\sum_{i,v}\frac{A_{riv}}{\eta^0_{riv}}\,f_{ri,\mathrm{liq}},
    \qquad k_{rc}=\frac{E^{CEUD}_{rc,\mathrm{gasoline+diesel}}}{E^0_{rc,\mathrm{liq}}}.
    \]

    $\boldsymbol{B}$ is surviving stock in thousands of vehicles; $\boldsymbol{q}$ is capacity-to-activity in
    billion service-km per thousand vehicles per year at full utilization; $\boldsymbol{u}$ is
    dimensionless annual utilization; $\boldsymbol{A}$ is annual billion passenger-km or tonne-km;
    $\boldsymbol{\eta}$ is billion service-km/PJ.
    $\boldsymbol{f}$ is the emitted liquid-energy fraction: one for gasoline/diesel/HEV, zero for
    BEV, and the retained input-blend fraction for PHEV. Energy $\boldsymbol{E}$ is in PJ and $\boldsymbol{k}$
    is dimensionless. Annual vehicle utilization $\boldsymbol{u}$ is distinct from the PHEV
    distance utility factor used earlier to derive charge-depleting/charge-sustaining energy.

    At every historical/future year and range, native consumption becomes $C^1=kC^0$.
    Service efficiency becomes $\eta^1=L/C^1=\eta^0/k$, where $\boldsymbol{C}$ is MJ/vehicle-km
    and $\boldsymbol{L}$ is occupancy (passengers/vehicle) or payload (tonnes/vehicle).
    Applying the same class multiplier before or after time/range averaging gives the same result.
    Loads, stock, activity, relative powertrain/range differences,
    projection indices and PHEV energy fractions remain fixed. Both PHEV components
    scale equally; blending energy is counted once, with no extra charger-loss multiplier.

    **Interpretation limits.** One aggregate target identifies one class scalar, not
    separate technology, vintage, range or fuel corrections. [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) has no electricity
    target in these selected class tables; transfer to electricity/hydrogen consumption
    is an explicit assumption, not electric-efficiency validation. Gasoline/diesel use
    the backend's [*ANL GREET*](https://greet.anl.gov/greet/versions.html) higher heating values; [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)'s aggregate energy need not reproduce
    those conversion constants exactly. The multiplier can absorb these boundary and
    composition differences, so it does not identify the cause of the original residual.
    Fitted energy closure and activity closure are calibration identities, not independent validation.
    """)
    return


@app.cell
def _(diagnosis, mo):
    regions = sorted(diagnosis.frames["summary"].region.unique())
    region = mo.ui.dropdown(regions, value="ON" if "ON" in regions else regions[0],
                            label="Region for energy and consumption trajectories")
    region
    return (region,)


@app.cell
def _(calibration_charts, diagnosis, region):
    charts = calibration_charts(diagnosis, region=region.value)
    return (charts,)


@app.cell(hide_code=True)
def _(charts, diagnosis, mo):
    mo.vstack([
        mo.md("## Comparable fuel energy and the fitted adjustment"),
        mo.ui.altair_chart(charts["ceud-cal-1"], chart_selection=False, legend_selection=False),
        mo.md("**Figure ceud-cal-1.** Bars compare source-derived energy, the CEUD gasoline/diesel target and calibrated energy. "
              "The striped extension above the orange target is reported propane/natural-gas energy excluded from the fit. "
              "Black diamonds use the right axis for the consumption multiplier: above one means higher consumption and lower service efficiency. "
              "The physical fleet and annual activity are unchanged. Heavy trucks use their confirmed diesel-only total. "
              "The complete audit retains CEUD component rounding and residuals; electricity is not an independent fitted target."),
        mo.ui.altair_chart(charts["ceud-cal-4"], chart_selection=False, legend_selection=False),
        mo.md(f"**Figure ceud-cal-4.** Colors distinguish road classes; solid lines are unadjusted and dashed lines calibrated. "
              f"The source-year axis spans {diagnosis.metadata['trajectory_years'][0]}–{diagnosis.metadata['trajectory_years'][1]}. "
              "LDV cars/light trucks use gasoline; medium/heavy trucks use diesel, all in MJ/vehicle-km. "
              f"Historical source-based values through {diagnosis.metadata['projection_reference_year']} include older cohorts absent from current stock; "
              "later points retain the captured annual projection values. Points identify available source years, not an existing/new-technology distinction. "
              "These source-year comparisons do not add compiled model rows."),
        mo.md("## Regional base-year anchors\nThe two heatmaps retain all ten province/model regions regardless of the selected region above. "
              "Light-duty cars and passenger/freight light trucks share one powertrain axis; medium/heavy trucks share the other. "
              "BCT combines British Columbia with the territories. Each cell shows the prepared base-year range-anchor service-efficiency change "
              "before representative aggregation; the common class multiplier gives final representatives the same percentage adjustment, checked separately at the SQL boundary. "
              "Grey cells with a dash have no comparable range; they are not zero adjustments."),
        *[mo.ui.altair_chart(charts[identity], chart_selection=False, legend_selection=False)
          for identity in ("ceud-cal-5-ldv", "ceud-cal-5-mhdv")],
    ])
    return


@app.cell(hide_code=True)
def _(diagnosis, mo, region):
    summary = diagnosis.frames["summary"].loc[lambda frame: frame.region.eq(region.value)]
    activity_view = summary[["road_class", "base_year", "observed_year", "observation_status",
                             "physical_stock_k_vehicles", "ceud_activity_bn", "target_activity_bn",
                             "activity_bn", "activity_residual_bn"]]
    energy_view = summary[["road_class", "status", "liquid_target_basis", "ceud_liquid_target_pj", "unadjusted_liquid_energy_pj",
                           "consumption_multiplier", "calibrated_liquid_energy_pj", "liquid_residual_pj",
                           "calibrated_electric_energy_pj", "electricity_target_status"]]
    mo.vstack([
        mo.md("## Targets, residuals and source support\nOne row is one region–[*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) class at the base year. Stock is thousands of vehicles; activity is billion passenger-km for passenger classes or billion tonne-km for freight. Energy is PJ. Held/extrapolated targets are labelled by their actual observation year."),
        mo.ui.table(activity_view.drop(columns=["base_year", "ceud_activity_bn"]).rename(columns={
            "road_class": "Vehicle class", "base_year": "Base year", "observed_year": "Observation year",
            "observation_status": "Observation / hold", "physical_stock_k_vehicles": "Stock (thousand vehicles)",
            "ceud_activity_bn": "CEUD activity (bn)", "target_activity_bn": "Target activity (bn)",
            "activity_bn": "Compiled activity (bn)", "activity_residual_bn": "Activity minus target (bn)",
        }), format_mapping={"Observation year": "{:.0f}", "Stock (thousand vehicles)": "{:,.3f}",
            "Target activity (bn)": "{:,.3f}", "Compiled activity (bn)": "{:,.3f}",
            "Activity minus target (bn)": "{:.3g}"}, show_data_types=False, show_column_summaries=False,
            selection=None, pagination=False, label="Physical stock and fixed activity · passenger-km or tonne-km as defined above"),
        mo.ui.table(energy_view.drop(columns=["calibrated_electric_energy_pj", "electricity_target_status"]).rename(columns={
            "road_class": "Vehicle class", "status": "Calibration status", "liquid_target_basis": "Target fuel boundary",
            "ceud_liquid_target_pj": "CEUD target (PJ)", "unadjusted_liquid_energy_pj": "Unadjusted (PJ)",
            "consumption_multiplier": "Consumption multiplier", "calibrated_liquid_energy_pj": "Calibrated (PJ)",
            "liquid_residual_pj": "Calibrated minus target (PJ)", "calibrated_electric_energy_pj": "Resulting electricity (PJ)",
            "electricity_target_status": "Electricity target status",
        }), format_mapping={"CEUD target (PJ)": "{:,.3f}", "Unadjusted (PJ)": "{:,.3f}",
            "Consumption multiplier": "{:.4f}", "Calibrated (PJ)": "{:,.3f}",
            "Calibrated minus target (PJ)": "{:.3g}"}, show_data_types=False, show_column_summaries=False,
            selection=None, pagination=False, label="Liquid-energy fit; the electricity transfer remains an assumption"),
        mo.md("Source activity, base-year fields and the assumed electricity transfer remain in the complete audit. "
              "Rounding here affects display only; residuals retain their scientific notation."),
        mo.accordion({"Complete captured audit tables": mo.ui.tabs({
            "Class boundary": mo.ui.table(summary, selection=None, pagination=True),
            "Cohort accounting": mo.ui.table(diagnosis.frames["cohorts"], selection=None, pagination=True),
            "Parameter trajectories": mo.ui.table(diagnosis.frames["trajectories"], selection=None, pagination=True),
            "Source-year consumption": mo.ui.table(diagnosis.frames["source_trajectories"], selection=None, pagination=True),
            "Activity calibration": mo.ui.table(diagnosis.frames["activity_calibration"], selection=None, pagination=True),
        })}),
        mo.md("""### Verify changes in the final database

After preparing the Ontario OFF/ON scenarios, the explicit read-only comparison
checks the supplied OFF database against its successful-build report and the ON
database against the current immutable capture:

```sh
uv run python -m validation.ceud_calibration_diagnostic --baseline outputs/sqlite/validation_baseline.sqlite --calibrated outputs/sqlite/validation_energy_calibration.sqlite --baseline-report outputs/validation/validation_baseline.json
```

The configured report records source fingerprints, final cohort/representative/future
efficiencies, unchanged parameter families, PHEV splits, provenance and DQ. The OFF
annual/period exports were not separately captured: their byte equality is not claimed.
The common-factor invariant and final SQL comparison establish the checked propagation
scope. The regional calibration profile independently retains all-region evidence;
it cannot stand in for an Ontario paired-build comparison, and a later Ontario run
cannot replace its heatmap coverage. Calibration closure remains distinct from independent
empirical validation.
"""),
    ])
    return


if __name__ == "__main__":
    app.run()
