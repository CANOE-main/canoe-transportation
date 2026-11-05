"""Focused comparison of current transport parameters and legacy researcher evidence."""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    from validation.transportation_research import load_validation_diagnosis

    CLASS_LABELS = {
        "cars": "Cars",
        "passenger_light_trucks": "Passenger light trucks",
        "freight_light_trucks": "Freight light trucks",
        "medium_trucks": "Medium trucks",
        "heavy_trucks": "Heavy trucks",
    }
    return CLASS_LABELS, alt, load_validation_diagnosis, mo


@app.cell
def _(load_validation_diagnosis):
    diagnosis = load_validation_diagnosis()
    return (diagnosis,)


@app.cell(hide_code=True)
def _(diagnosis, mo):
    _scenario = diagnosis["fleet_metadata"]["scenario"]
    _identity = _scenario["scenario"]["name"]
    _base = _scenario["periods"]["base_year"]
    _state = diagnosis["fleet"]["reference_state"].set_index("table")
    _stock = _state.loc["ExistingCapacity"]
    _demand = _state.loc["Demand"]
    _factor = _state.loc["LimitAnnualCapacityFactor"]
    mo.md(f"""
    # Transportation validation comparisons

    Current fleet scenario **{_identity}**, base year **{_base}**.
    The original legacy baseline and notebook-derived Ontario extract provide
    comparison values. Their workbooks and cost CSVs remain legacy evidence.

    The plots emphasize cohort stock, demand trajectories and charging investment
    assumptions. Cohort allocations, utilization bounds and supporting inputs are
    available below each comparison. Source years and physical boundaries explain
    differences alongside the parameter values.

    On shared transport keys, the supplied Ontario extract matches the original
    legacy baseline on all **{int(_stock.common_keys)} non-charger stock rows** and
    **{int(_demand.common_keys)} demand rows**. Annual utilization differs on
    **{int(_factor.different_values)} of {int(_factor.common_keys)} shared rows**.
    Notebook modifications describe the intended calculations. Their effect on
    supplied parameter rows is established by these database comparisons.
    """)
    return


@app.cell
def _(diagnosis, mo):
    _capacity = diagnosis["fleet"]["capacity_comparison"]
    regions = sorted(_capacity.loc[_capacity.reference.notna(), "region"].unique())
    region = mo.ui.dropdown(regions, value=regions[0], label="Comparison region")
    region
    return (region,)


@app.cell
def _(CLASS_LABELS, diagnosis, mo):
    classes = sorted(diagnosis["fleet"]["demand_comparison"].road_class.unique())
    demand_class = mo.ui.dropdown(
        {CLASS_LABELS[road_class]: road_class for road_class in classes},
        value="Cars" if "cars" in classes else CLASS_LABELS[classes[0]],
        label="Road class",
    )
    demand_class
    return (demand_class,)


@app.cell(hide_code=True)
def _(CLASS_LABELS, alt, demand_class, diagnosis, mo, region):
    _capacity = diagnosis["fleet"]["capacity_comparison"]
    _capacity = _capacity.loc[_capacity.region.eq(region.value)]
    _selected = _capacity.loc[_capacity.road_class.eq(demand_class.value)].copy()
    _selected["Difference (%)"] = _selected.relative_percent
    _labels = {
        "baseline": "Original legacy",
        "reference": "validation_1",
        "current": "Current backend",
    }
    _long = _selected.melt(
        id_vars="vintage",
        value_vars=list(_labels),
        var_name="Evidence",
        value_name="Thousand vehicles",
    )
    _long["Evidence"] = _long.Evidence.map(_labels)
    _plot = (
        alt.Chart(_long)
        .mark_bar()
        .encode(
            x=alt.X("vintage:O", title="Existing-capacity vintage label"),
            xOffset="Evidence:N",
            y="Thousand vehicles:Q",
            color="Evidence:N",
            tooltip=[
                "vintage:O",
                "Evidence:N",
                alt.Tooltip("Thousand vehicles:Q", format=",.1f"),
            ],
        )
        .properties(
            width=850,
            height=300,
            title=f"{CLASS_LABELS[demand_class.value]} existing stock by vintage",
        )
    )
    _largest = _selected.loc[
        _selected["Difference (%)"].abs().ge(diagnosis["significance_percent"])
        | _selected.current.isna()
        | _selected.reference.isna()
    ].sort_values("vintage")
    _audit = diagnosis["fleet_metadata"]["cohort_audit"]
    _redistributed = _audit["road_ineligible_raw_stock_k_vehicles"]
    _raw_stock = _audit["road_stock_k_vehicles"]
    mo.vstack(
        [
            mo.md(f"""
        ## Fleet stock and utilization

        The backend reconstructs provincial cohorts from CEUD sales and selected
        survival, then calibrates provincial annual activity on retained capacity.
        Legacy stock anchors, sales bins, fuel allocations and retirement treatment
        can each change the represented total and its allocation among vintages.
        The bars retain each scenario's reported existing stock and calibration year.
        Empty slots identify vintages with no corresponding capacity row.

        Across this run's road classes, **{_redistributed:,.1f} thousand vehicles
        ({100 * _redistributed / _raw_stock:.1f}% of raw stock)** fall outside
        fixed-lifetime eligibility and are redistributed among surviving vintages.
        This retirement constraint contributes to the cohort shifts shown here.
        """),
            mo.ui.altair_chart(_plot, chart_selection=False, legend_selection=False),
            mo.ui.table(
                _largest[
                    ["vintage", "baseline", "reference", "current", "Difference (%)"]
                ].rename(
                    columns={
                        "vintage": "Vintage",
                        "baseline": "Original legacy",
                        "reference": "validation_1",
                        "current": "Current backend",
                    }
                ),
                selection=None,
                pagination=False,
                label="Significant cohort differences and unmatched vintage keys",
                format_mapping={"Difference (%)": "{:+.1f}%"},
            ),
            mo.accordion(
                {
                    "Supplied SQLite versus original legacy on exact keys": mo.ui.table(
                        diagnosis["fleet"]["reference_state"], selection=None,
                        pagination=False,
                    ),
                    "Cohort allocations in thousand vehicles": mo.ui.table(
                        _capacity, selection=None, pagination=True, page_size=10
                    ),
                    "Utilization bounds by class and existing/new vehicles": mo.vstack(
                        [
                            mo.md(
                                "Bounds span the reported rows. The legacy extract uses operating-period keys, while the backend uses fixed vintage factors. Existing and new vehicles remain separate."
                            ),
                            mo.ui.table(
                                diagnosis["fleet"]["utilization_comparison"].loc[
                                    diagnosis["fleet"][
                                        "utilization_comparison"
                                    ].region.eq(region.value)
                                ],
                                selection=None,
                                pagination=True,
                                page_size=10,
                            ),
                        ]
                    ),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(alt, demand_class, diagnosis, mo, region):
    _demand = diagnosis["fleet"]["demand_comparison"]
    _selected = _demand.loc[
        _demand.region.eq(region.value) & _demand.road_class.eq(demand_class.value)
    ]
    _long = _selected.melt(
        id_vars="period",
        value_vars=["baseline", "reference", "current"],
        var_name="Evidence",
        value_name="Demand",
    ).dropna()
    _unit = (
        "Billion passenger-km"
        if demand_class.value in {"cars", "passenger_light_trucks"}
        else "Billion tonne-km"
    )
    _long["Evidence"] = _long.Evidence.map(
        {
            "baseline": "Original legacy",
            "reference": "validation_1",
            "current": "Current backend",
        }
    )
    _plot = (
        alt.Chart(_long)
        .mark_line(point=True)
        .encode(
            x=alt.X("period:O", title="Model period label"),
            y=alt.Y("Demand:Q", title=_unit),
            color="Evidence:N",
            tooltip=[
                "period:O",
                "Evidence:N",
                alt.Tooltip("Demand:Q", title=_unit, format=",.2f"),
            ],
        )
        .properties(width=850, height=280, title="Demand under each parameter set")
    )
    mo.vstack(
        [
            mo.md("""
        ## Demand trajectory

        Current demand uses CEUD base activity and real-GDP indexing, including the
        configured car/light-truck split. CEUD endpoint holding affects the baseline
        level. The model's period labels and interval-end projection years are
        recorded separately. Legacy growth assumptions and timing remain visible
        through their supplied parameter values.
        """),
            mo.ui.altair_chart(_plot, chart_selection=False, legend_selection=False),
            mo.accordion(
                {
                    "Demand values by model label": mo.ui.table(
                        _selected, selection=None, pagination=True, page_size=10
                    )
                }
            ),
        ]
    )
    return


@app.cell
def _(diagnosis, mo):
    _costs = diagnosis["charging"]["comparisons"]
    _invest = _costs.loc[
        _costs.table.eq("cost_invest") & _costs.scope_status.eq("source_year_matched")
    ]
    _confirmed = _invest.loc[_invest.basis_status.eq("confirmed"), "basis_name"]
    _default_basis = (
        _confirmed.iloc[0]
        if not _confirmed.empty
        else sorted(_invest.basis_name.unique())[0]
    )
    cost_basis = mo.ui.dropdown(
        sorted(_invest.basis_name.unique()),
        value=_default_basis,
        label="Charging denominator",
    )
    _cost_years = [int(year) for year in sorted(_invest.reference_vintage.unique())]
    cost_year = mo.ui.dropdown(
        _cost_years, value=_cost_years[0], label="Charging source year",
    )
    mo.hstack([cost_basis, cost_year], justify="start")
    return cost_basis, cost_year


@app.cell(hide_code=True)
def _(alt, cost_basis, cost_year, diagnosis, mo):
    _comparison = diagnosis["charging"]["comparisons"]
    _matched = _comparison.loc[
        _comparison.table.eq("cost_invest")
        & _comparison.scope_status.eq("source_year_matched")
        & _comparison.basis_name.eq(cost_basis.value)
        & _comparison.reference_vintage.eq(cost_year.value)
    ]
    _rates = _matched[
        [
            "charger_class",
            "reference_million_target_cad",
            "backend_million_target_cad",
            "relative_percent",
            "denominator",
        ]
    ].drop_duplicates()
    _chart_data = _rates.melt(
        id_vars=["charger_class"],
        value_vars=["reference_million_target_cad", "backend_million_target_cad"],
        var_name="Evidence",
        value_name="Million CAD2020 per PJ/year",
    )
    _chart_data["Evidence"] = _chart_data.Evidence.map(
        {
            "reference_million_target_cad": "validation_2",
            "backend_million_target_cad": "Current backend",
        }
    )
    _plot = (
        alt.Chart(_chart_data)
        .mark_bar()
        .encode(
            x=alt.X("charger_class:N", title="Charger class"),
            xOffset="Evidence:N",
            y="Million CAD2020 per PJ/year:Q",
            color="Evidence:N",
            tooltip=[
                "charger_class:N",
                "Evidence:N",
                alt.Tooltip("Million CAD2020 per PJ/year:Q", format=",.2f"),
            ],
        )
        .properties(
            width=850,
            height=300,
            title="Charging investment assumptions at matched source year",
        )
    )
    _fixed = diagnosis["charging"]["fixed_ratios"]
    _matched_ratios = _fixed.loc[_fixed.status.eq("matched_raw_ratio"), "ratio"]
    _fixed_text = f"Matched reference fixed costs span {_matched_ratios.min():.1%} to {_matched_ratios.max():.1%} of investment annually."
    mo.vstack(
        [
            mo.md(f"""
        ## Charging infrastructure assumptions

        Supplied costs use million CAD2020 per PJ/year. Backend per-GW rates are
        harmonized with the configured physical conversion and the selected
        utilization interpretation. Source calendar years are matched to the
        backend's cost inputs. Its prospective vintage label can be five years
        earlier than that source year.

        The legacy context supplies missing utilization, efficiency and lifetime
        assumptions. Canadian versus US cost components, charger-type shares and
        infrastructure boundaries contribute to the differences. Backend averaging
        uses port-count shares for per-GW rates, which differs from aggregating
        expenditure over total installed power. {_fixed_text}
        """),
            mo.ui.altair_chart(_plot, chart_selection=False, legend_selection=False),
            mo.accordion(
                {
                    "Matched investment values": mo.ui.table(
                        _rates, selection=None, pagination=False
                    ),
                    "Component costs and monetary conversion": mo.ui.table(
                        diagnosis["charging"]["source_components"],
                        selection=None,
                        pagination=True,
                        page_size=8,
                    ),
                    "Inherited original-legacy context": mo.ui.table(
                        diagnosis["charging"]["reference_inherited_context"],
                        selection=None,
                        pagination=True,
                        page_size=8,
                    ),
                    "Variable-cost row coverage": mo.ui.table(
                        diagnosis["charging"]["variable_coverage"],
                        selection=None,
                        pagination=False,
                    ),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(diagnosis, mo):
    _fleet = diagnosis["fleet_metadata"]
    _charging = diagnosis["charging_metadata"]
    mo.accordion(
        {
            "Captured settings and interpretation": mo.vstack(
                [
                    mo.md(
                        "All displayed tables are verified against the report's exported hashes. Parameter reports are generated explicitly after a scenario build. The references retain their original files and row identities."
                    ),
                    mo.json(
                        {
                            "fleet_scenario": _fleet["scenario"],
                            "fleet_revision": _fleet["revision"],
                            "charging_scenario": _charging["scenario"],
                            "charging_build_utc": _charging["build_timestamp_utc"],
                            "limitations": _fleet["limitations"],
                        }
                    ),
                ]
            ),
        }
    )
    return


if __name__ == "__main__":
    app.run()
