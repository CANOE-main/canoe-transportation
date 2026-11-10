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
        CLASS_LABELS,
        calibration_charts,
        load_calibration_diagnostic,
    )

    return CLASS_LABELS, calibration_charts, load_calibration_diagnostic, mo


@app.cell
def _(load_calibration_diagnostic):
    diagnosis = load_calibration_diagnostic()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    mo.md(rf"""
    # CEUD calibration: keep activity fixed, examine energy

    This optional evaluation fits reported gasoline and diesel energy for cars,
    passenger/freight light trucks and medium trucks. Heavy-truck energy lacks a
    comparable fuel breakdown in the selected evidence, so its parameters stay unchanged.
    The ordinary scenarios keep energy calibration **off**.
    This evaluated option requires fixed lifetimes and flat emitted utilization.
    Curve-mode reference-only cohorts can lack emitted activity/efficiency rows;
    the backend rejects that combination rather than discarding physical survivors.

    The comparison uses one captured fleet, activity level and representation.
    “Unadjusted” is the retained source-derived parameter estimate from that build;
    “Calibrated” adds only the explicitly selected consumption multiplier.
    It is not a comparison of different fleet/scenario runs.

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
    stock so that class activity matches CEUD. Solver installation-equivalent capacity
    in survival-curve mode is not the physical stock used here. For region $r$, vehicle
    class $c$, technology $i$ and vintage $v$:

    \[
    A_{riv}=B_{riv}\,q_i\,u_{rc},\qquad
    E^0_{rc,\mathrm{liq}}=\sum_{i,v}\frac{A_{riv}}{\eta^0_{riv}}\,f_{ri,\mathrm{liq}},
    \qquad k_{rc}=\frac{E^{CEUD}_{rc,\mathrm{gasoline+diesel}}}{E^0_{rc,\mathrm{liq}}}.
    \]

    $B$ is surviving stock in thousands of vehicles; $q$ is capacity-to-activity in
    billion service-km per thousand vehicles per year at full utilization; $u$ is
    dimensionless annual utilization; $A$ is annual billion passenger-km or tonne-km;
    $\eta$ is billion service-km/PJ.
    $f$ is the emitted liquid-energy fraction: one for gasoline/diesel/HEV, zero for
    BEV, and the retained input-blend fraction for PHEV. Energy $E$ is in PJ and $k$
    is dimensionless. Annual vehicle utilization $u$ is distinct from the PHEV
    distance utility factor used earlier to derive charge-depleting/charge-sustaining energy.

    At every historical/future year and range, native consumption becomes $C^1=kC^0$.
    Service efficiency becomes $\eta^1=L/C^1=\eta^0/k$, where $C$ is MJ/vehicle-km
    and $L$ is occupancy (passengers/vehicle) or payload (tonnes/vehicle). The same class scalar commutes with weighted
    temporal/range means. Loads, stock, activity, relative powertrain/range differences,
    projection indices and PHEV energy fractions remain fixed. Both PHEV components
    scale equally; blending energy is counted once, with no extra charger-loss multiplier.

    **Interpretation limits.** One aggregate target identifies one class scalar, not
    separate technology, vintage, range or fuel corrections. CEUD has no electricity
    target in these selected class tables; transfer to electricity/hydrogen consumption
    is an explicit assumption, not electric-efficiency validation. Gasoline/diesel use
    the backend's GREET higher heating values; CEUD's aggregate energy need not reproduce
    those conversion constants exactly. The multiplier can absorb these boundary and
    composition differences, so it does not identify the cause of the original residual.
    Fitted energy closure and activity closure are calibration identities, not independent validation.
    """)
    return


@app.cell
def _(CLASS_LABELS, diagnosis, mo):
    regions = sorted(diagnosis.frames["summary"].region.unique())
    region = mo.ui.dropdown(regions, value=regions[0], label="Region")
    vehicle_class = mo.ui.dropdown({label: key for key, label in CLASS_LABELS.items()}, value="Cars", label="Vehicle class")
    mo.hstack([region, vehicle_class])
    return region, vehicle_class


@app.cell
def _(diagnosis, mo, region, vehicle_class):
    powertrains = sorted(diagnosis.frames["trajectories"].loc[
        lambda frame: frame.region.eq(region.value) & frame["mode"].eq(vehicle_class.value), "powertrain"
    ].unique())
    powertrain = mo.ui.dropdown(powertrains, value="gasoline" if "gasoline" in powertrains else powertrains[0], label="Powertrain / range")
    powertrain
    return (powertrain,)


@app.cell
def _(calibration_charts, diagnosis, powertrain, region, vehicle_class):
    charts = calibration_charts(diagnosis, region=region.value, road_class=vehicle_class.value,
                                powertrain=powertrain.value)
    return (charts,)


@app.cell(hide_code=True)
def _(charts, mo):
    mo.vstack([
        mo.md("## Comparable fuel energy and the fitted adjustment"),
        mo.ui.altair_chart(charts["ceud-cal-1"], chart_selection=False, legend_selection=False),
        mo.md("**Figure ceud-cal-1.** Gasoline/diesel closure uses the unchanged physical fleet and emitted annual utilization. Excluded fuel energy and electricity are not fitted targets."),
        mo.ui.altair_chart(charts["ceud-cal-2"], chart_selection=False, legend_selection=False),
        mo.md("**Figure ceud-cal-2.** A multiplier above one increases consumption and reduces service efficiency. Heavy trucks retain one because their fuel boundary is unsupported."),
        mo.ui.altair_chart(charts["ceud-cal-3"], chart_selection=False, legend_selection=False),
        mo.md("**Figure ceud-cal-3.** These additive parts reconstruct the reported total within source rounding. CEUD reports energy to 0.000001 PJ; the complete audit retains the signed component-total residual and its rounding allowance. Fitted closure uses a separate tighter tolerance. Missing electricity evidence is not a zero electricity measurement; full class-total closure is not claimed."),
        mo.ui.altair_chart(charts["ceud-cal-4"], chart_selection=False, legend_selection=False),
        mo.md("**Figure ceud-cal-4.** Historical cohort means and future model vintages retain their relative consumption changes. Model vintage labels are not observation years; source-year selections remain in captured annual/period evidence."),
    ])
    return


@app.cell(hide_code=True)
def _(diagnosis, mo, region):
    summary = diagnosis.frames["summary"].loc[lambda frame: frame.region.eq(region.value)]
    activity_view = summary[["road_class", "base_year", "observed_year", "observation_status",
                             "physical_stock_k_vehicles", "ceud_activity_bn", "activity_bn", "activity_residual_bn"]]
    energy_view = summary[["road_class", "status", "ceud_liquid_target_pj", "unadjusted_liquid_energy_pj",
                           "consumption_multiplier", "calibrated_liquid_energy_pj", "liquid_residual_pj",
                           "calibrated_electric_energy_pj", "electricity_target_status"]]
    mo.vstack([
        mo.md("## Targets, residuals and source support\nOne row is one region–CEUD class at the base year. Stock is thousands of vehicles; activity is billion passenger-km for passenger classes or billion tonne-km for freight. Energy is PJ. Held/extrapolated targets are labelled by their actual observation year."),
        mo.ui.table(activity_view, selection=None, pagination=False, label="Physical stock and fixed activity"),
        mo.ui.table(energy_view, selection=None, pagination=False, label="Comparable liquid energy and unvalidated electric transfer"),
        mo.accordion({"Complete captured audit tables": mo.ui.tabs({
            "Class boundary": mo.ui.table(summary, selection=None, pagination=True),
            "Cohort accounting": mo.ui.table(diagnosis.frames["cohorts"], selection=None, pagination=True),
            "Parameter trajectories": mo.ui.table(diagnosis.frames["trajectories"], selection=None, pagination=True),
            "Activity calibration": mo.ui.table(diagnosis.frames["activity_calibration"], selection=None, pagination=True),
        })}),
        mo.md("""### Check the final database boundary

The explicit read-only comparison checks the supplied OFF database against its
successful-build report and the ON database against this immutable capture:

```sh
uv run python -m validation.ceud_calibration_diagnostic --baseline outputs/sqlite/validation_baseline.sqlite --calibrated outputs/sqlite/validation_energy_calibration.sqlite --baseline-report outputs/validation/validation_baseline.json
```

The configured report records source fingerprints, final cohort/representative/future
efficiencies, unchanged parameter families, PHEV splits, provenance and DQ. The OFF
annual/period exports were not separately captured: their byte equality is not claimed.
The common-factor invariant and final SQL comparison establish the checked propagation
scope. Calibration closure remains distinct from independent empirical validation.
"""),
    ])
    return


if __name__ == "__main__":
    app.run()
