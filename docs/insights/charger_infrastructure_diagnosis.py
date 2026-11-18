"""Production charger inputs and explicit research alternatives for utilization and stock."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd
    from validation.charger_infrastructure_diagnostic import load_diagnostic
    from validation.charger_representation_diagnostic import build_diagnostic
    from validation.notebook_figures import identified_chart

    def table_view(frame, columns, label, formats=None):
        return mo.ui.table(
            frame[list(columns)].rename(columns=columns).reset_index(drop=True),
            selection=None, page_size=24, show_data_types=False,
            show_column_summaries=False, label=label, format_mapping=formats or {},
        )

    def inline(frame):
        return alt.Data(values=frame.astype(object).where(frame.notna(), None).to_dict("records"))

    def figure(chart, identity, title):
        return mo.ui.altair_chart(
            identified_chart(chart, identity, title),
            chart_selection=False, legend_selection=False,
        )

    return alt, build_diagnostic, figure, inline, load_diagnostic, mo, pd, table_view


@app.cell
def _(mo, representation):
    mo.md(f"""
    # From charging needs to production charger parameters

    **Adopted source-to-parameter method.** The shared production calculation derives
    three light-duty (LDV) and two medium/heavy-duty (MHDV) physical charger archetypes.
    The configured representation is **{representation.metadata['configured_representation']}**:
    aggregated mode pools their investment-cost basis into LDV/MHDV technologies; explicit mode
    retains the five archetypes and their available-capacity share constraints.
    Aggregate annual utilization and existing-stock rating conventions remain separate.

    Follow the calculation from **Dunsky's port composition → charging utilization
    → parameter trajectories → capacity shares → existing stock**.
    The tables and figures call the production core. Home-utilization and truck
    recharge sensitivities, and the point-year registration stock illustration,
    remain labelled research alternatives. Type-specific hourly profiles are pending.

    **Utilization boundary:** {representation.metadata['utilization_policy']}.
    """)
    return


@app.cell
def _(build_diagnostic, load_diagnostic):
    diagnostic = load_diagnostic()
    representation = build_diagnostic(diagnostic)
    return diagnostic, representation


@app.cell
def _(mo):
    vehicle_class = mo.ui.dropdown(["LDV", "MHDV"], value="LDV", label="Vehicle chargers")
    vehicle_class
    return (vehicle_class,)


@app.cell
def _(alt, diagnostic, figure, inline, mo, representation, table_view, vehicle_class):
    selected = representation.parameters.loc[representation.parameters.vehicle_class.eq(vehicle_class.value)].copy()
    source_rows = representation.composition.loc[
        representation.composition.vehicle_class.eq(vehicle_class.value)
        & representation.composition.year.le(representation.metadata["settings"]["count_hold_after_year"])
    ]
    source_table = source_rows.pivot(index=["archetype", "component"], columns="source_year", values="ports").reset_index()
    source_columns = {"archetype": "Archetype", "component": "Dunsky component"}
    source_columns.update({column: f"{column} ports" for column in source_table if isinstance(column, int)})
    share_rows = selected.loc[selected.year.le(representation.metadata["settings"]["count_hold_after_year"])].copy()
    share_rows["population_ports"] = share_rows.groupby("year").port_count.transform("sum")
    composition_note = ("Shares use the component sum; differences from the report's rounded totals remain in the audit."
                        if vehicle_class.value == "MHDV" else "")
    composition_plot = alt.Chart(inline(selected)).mark_bar().encode(
        x=alt.X("year:O", title="Calendar year"),
        y=alt.Y("port_share:Q", stack="zero", title="Share of all selected-class ports", axis=alt.Axis(format="%")),
        color=alt.Color("archetype:N", title="Archetype", legend=alt.Legend(orient="bottom", labelLimit=340)),
        order="archetype:N",
        tooltip=["archetype:N", "year:O", "count_source_year:O", alt.Tooltip("port_share:Q", format=".2%")],
    ).properties(width=850, height=235)
    mo.vstack([
        mo.md(f"""## 1. How do the report's ports become our charger groups?

        The [Dunsky/ICCT assessment]({diagnostic.metadata['url']}) supplies LDV Table 6
        (high-home-access base case) and MHDV Table 38 (primary Policy Reference case)
        for Canada-wide port requirements.
        Each row below is one source component assigned to an archetype.
        Sum its components, then divide by **all LDV ports** or **all MHDV ports**
        in that year to obtain the archetype's port share.

        Public and private workplace entries are separate halves of workplace charging;
        both are retained. Overnight includes private depot and public long-haul charging,
        so the MHDV group is named `T_MHDV_CHRG_NIGHT`. Fast and ultrafast ports
        are combined in `T_MHDV_CHRG_FC`.
        """),
        table_view(source_table, source_columns, "Input evidence: separate report components, in ports",
                   {label: "{:,.0f}" for label in source_columns.values() if label.endswith(" ports")}),
        mo.md(composition_note),
        table_view(share_rows, {"year": "Year", "archetype": "Archetype", "port_count": "Summed group ports",
                   "population_ports": "All selected-class ports", "port_share": "Group / all ports"},
                   "Aggregation check: component counts determine each archetype's share",
                   {"Year": "{:.0f}", "Summed group ports": "{:,.0f}", "All selected-class ports": "{:,.0f}",
                    "Group / all ports": "{:.2%}"}),
        figure(composition_plot, "charger-infrastructure-composition", "Projected port shares after grouping the report components"),
        mo.md(f"""These are installed-port requirements, not annual additions or energy shares.
        Composition is held after **{representation.metadata['settings']['count_hold_after_year']}**;
        repeated counts only preserve those shares and are not forecasts of absolute future stock."""),
    ])
    return (selected,)


@app.cell
def _(alt, figure, inline, mo, pd, representation, table_view, vehicle_class):
    settings = representation.metadata["settings"]
    if vehicle_class.value == "LDV":
        public_inputs = representation.ldv_utilization.loc[~representation.ldv_utilization.component.str.startswith("Home")].pivot(
            index="year", columns="component", values="annual_utilization").reset_index()
        public_inputs["Home assumption"] = settings["private_home_utilization_reference"]
        first_year = min(settings["years"])
        private_inputs = representation.ldv_utilization.loc[
            representation.ldv_utilization.year.eq(first_year)
            & representation.ldv_utilization.archetype.eq(settings["archetypes"]["ldv_private"])
        ].copy()
        private_inputs["capacity_share"] = private_inputs.ports * private_inputs.power_kw
        private_inputs["capacity_share"] /= private_inputs.capacity_share.sum()
        private_inputs["contribution"] = private_inputs.capacity_share * private_inputs.annual_utilization
        utilization_content = [
            mo.md(f"""## 2. What determines annual charging utilization?

            Annual utilization is the fraction of the year's rated charging capacity
            used by the assumed charging schedule. **LDV public and workplace factors
            come directly from Table 31.** Changing the model's power rating does not
            establish a change in charging time, so these factors are not rescaled
            by a nameplate/model-power ratio.

            Home utilization is the scenario's **empirical proxy**, currently
            **{settings['private_home_utilization_reference']:.0%}**. Residential L2 evidence
            is transferred to home L1; this is not a measured national annual factor.
            For the combined private group,
            weight home L1, home L2 and private workplace utilization by their shares of
            installed power: **Port count × Charger power / Total private power**.
            This preserves the components' annual charging output.
            """),
            table_view(public_inputs, {"year": "Year", "Public L2": "Public L2", "Public fast": "Public fast",
                       "Private workplace L2": "Workplace", "Home assumption": "Home assumption"},
                       "Reported annual utilization, used unchanged; home values are assumed",
                       {"Year": "{:.0f}", "Public L2": "{:.1%}", "Public fast": "{:.1%}",
                        "Workplace": "{:.1%}", "Home assumption": "{:.1%}"}),
            table_view(private_inputs, {"component": "Private component", "ports": "Ports", "power_kw": "kW/port",
                       "annual_utilization": "Utilization", "capacity_share": "Share of private power",
                       "contribution": "Contribution to group utilization"},
                       f"Worked private-group calculation for {first_year}: sum the final column",
                       {"Ports": "{:,.0f}", "kW/port": "{:.2f}", "Utilization": "{:.1%}",
                        "Share of private power": "{:.2%}", "Contribution to group utilization": "{:.2%}"}),
        ]
    else:
        battery_table = representation.battery.class_batteries.pivot(index="year", columns="road_class", values="battery_kwh").reset_index()
        heavy_activity = representation.battery.activity_weights.loc[
            representation.battery.activity_weights.road_class.eq("heavy_trucks"), ["year", "activity_share"]]
        battery_table = battery_table.merge(heavy_activity, on="year").merge(representation.battery.pooled_battery[["year", "battery_kwh"]], on="year")
        first_mhd_year = min(settings["years"])
        durations = representation.utilization_components.loc[representation.utilization_components.year.eq(first_mhd_year)]
        duration_table = durations.pivot(index=["location", "charger", "power_kw", "vehicles_per_day"],
                                         columns="case_label", values="charging_hours").reset_index()
        duration_columns = {"location": "Access", "charger": "Type", "power_kw": "Rated kW",
                            "vehicles_per_day": "Vehicles/port/day", "Table 19 duration": "Reference hours/vehicle"}
        duration_columns.update({f"{fraction:.0%} battery recharge": f"{fraction:.0%} hours/vehicle" for fraction in settings["mhdv_recharge_fractions"]})
        utilization_plot = alt.Chart(inline(representation.utilization_cases)).mark_line(point=True).encode(
            x=alt.X("year:O", title="Calendar year"),
            y=alt.Y("annual_utilization:Q", title="Annual utilization", axis=alt.Axis(format="%")),
            color=alt.Color("case_label:N", title="Charging-duration basis",
                            legend=alt.Legend(orient="bottom", labelLimit=250, symbolType="stroke")),
            strokeDash=alt.StrokeDash("case_label:N", legend=None),
            tooltip=["archetype:N", "case_label:N", "year:O", alt.Tooltip("annual_utilization:Q", format=".2%")],
        ).properties(width=455, height=245).facet(column=alt.Column("archetype:N", title=None))
        utilization_content = [
            mo.md(f"""## 2. How do truck batteries and charging time determine utilization?

            Start with **NLR Annual Technology Baseline (ATB) usable battery capacity**, using the backend's medium-truck
            weight-class weights and heavy-truck vocation weights. Combine the two class
            estimates using projected freight activity in billion tonne-km.
            This battery/activity calculation covers **{', '.join(representation.battery.metadata['regions'])}**,
            the configured analysis region(s).

            **Pooled battery = (Medium battery × Medium activity share) + (Heavy battery × Heavy activity share).**

            This is an activity-weighted planning proxy. It is not a measured average
            battery per truck or charging session: payloads, travel distances and
            charger use differ between classes.
            """),
            table_view(battery_table, {"year": "Year", "medium_trucks": "Medium battery (kWh)",
                       "heavy_trucks": "Heavy battery (kWh)", "activity_share": "Heavy share of tonne-km",
                       "battery_kwh": "Pooled usable battery (kWh)"},
                       "Battery evidence: configured ATB trajectory and projected freight activity",
                       {"Year": "{:.0f}", "Medium battery (kWh)": "{:.1f}", "Heavy battery (kWh)": "{:.1f}",
                        "Heavy share of tonne-km": "{:.2%}", "Pooled usable battery (kWh)": "{:.1f}"}),
            mo.accordion({"Weight-class and vocation inputs": mo.ui.table(representation.battery.component_audit, selection=None, page_size=20)}),
            mo.md(f"""For each public/private charger type, calculate:

            **Charging hours = Recharge fraction × Usable battery (kWh) / Rated power (kW)**

            **Annual utilization = Charging hours × Vehicles per port per day × {settings['mhdv_operating_days']} operating days / 8,760 hours**

            Table 19's charging durations provide a **schedule reference**. The
            **60% and 75% usable-battery recharge cases** replace those durations
            with the battery/power calculation; they retain Table 19 throughput.
            """),
            table_view(duration_table, duration_columns, f"Worked charging durations for {first_mhd_year}; all source access/type combinations",
                       {"Rated kW": "{:,.0f}", "Vehicles/port/day": "{:.1f}", **{label: "{:.2f}" for label in duration_columns.values() if "hours/" in label}}),
            figure(utilization_plot, "charger-explicit-utilization", "Charging-duration assumptions lead to different annual utilization"),
            mo.md("""Pool component utilizations by their **shares of rated charging power**,
            retaining each type's throughput. High-power fast/ultrafast ports can have
            low utilization because each recharge takes little time and daily throughput
            is limited. The initial drop in the fast group follows the introduction of
            very high-power ultrafast ports. Conversely, the pooled battery can require more overnight hours
            than Table 19 allows. This exposes a need for class-specific charging profiles.

            These are ideal constant-power durations: charging taper, vehicle power limits,
            losses and downtime are not estimated here. The parameter table retains
            the schedule reference and both recharge cases; neither recharge fraction
            has been selected for production.
            """),
        ]
    if representation.metadata["configured_aggregate_utilization_targets"]:
        utilization_content.append(mo.accordion({
            "Current aggregate annual-utilization targets (production)": table_view(
                pd.DataFrame(representation.metadata["configured_aggregate_utilization_targets"]),
                {"region": "Region", "tech_or_group": "Aggregate technology", "vintage": "Vintage",
                 "target_utilization": "Annual utilization target"},
                "Configured aggregate targets; distinct from the physical-archetype worked calculation",
                {"Annual utilization target": "{:.2%}", "Vintage": "{:.0f}"},
            ),
        }))
    mo.vstack(utilization_content)
    return


@app.cell
def _(alt, figure, inline, mo, representation, selected, table_view, vehicle_class):
    cost_label = f"M CAD{representation.metadata['cost_dollar_year']}/GW"
    om_label = f"Fixed O&M (M CAD{representation.metadata['cost_dollar_year']}/GW/year)"
    parameter_columns = {"year": "Year", "archetype": "Archetype", "rated_power_kw": "kW/port",
                         "capex_mcad_per_gw": cost_label, "fixed_om_mcad_per_gw_year": om_label,
                         "annual_utilization": "Annual utilization" if vehicle_class.value == "LDV" else "Table 19 utilization"}
    if vehicle_class.value == "MHDV":
        parameter_columns.update({f"recharge_{fraction:g}_utilization": f"{fraction:.0%} recharge utilization"
                                  for fraction in representation.metadata["settings"]["mhdv_recharge_fractions"]})
    parameter_formats = {"Year": "{:.0f}", "kW/port": "{:.2f}", cost_label: "{:,.2f}", om_label: "{:,.2f}"}
    parameter_formats.update({label: "{:.2%}" for label in parameter_columns.values() if "utilization" in label.lower()})
    cost_plot = alt.Chart(inline(selected)).mark_line(point=True).encode(
        x=alt.X("year:O", title="Calendar year"),
        y=alt.Y("capex_mcad_per_gw:Q", title=cost_label),
        color=alt.Color("archetype:N", title="Archetype", legend=alt.Legend(orient="bottom", labelLimit=340)),
        tooltip=["archetype:N", "year:O", alt.Tooltip("capex_mcad_per_gw:Q", format=",.2f"), "cost_status:N"],
    ).properties(width=850, height=240)
    mo.vstack([
        mo.md("""## 3. What parameter trajectories follow?

        One row is one archetype and calendar year. All years are shown together.
        Mean power is total rated power divided by port count; capex per GW is total
        port expenditure divided by total rated power. One CAD/kW equals one million
        CAD/GW. Fixed O&M uses the configured annual fraction of capex.

        Public fast uses the Table 12 cost/power pair. Home L2 uses a public-L2 cost
        proxy; public 100-kW overnight costs scale the 50-kW source cost per kW.
        L1 retains its separate reviewed NREL USD2021 price and currency conversion. These cost
        choices are separate from the utilization derivations above.
        Explicit mode emits the archetype parameters shown here. Aggregated mode
        pools their investment-cost basis and retains its configured annual-utilization
        and existing-stock rating conventions; these are not inferred from this table.
        """),
        table_view(selected, parameter_columns, "Production-core parameters across all calendar years", parameter_formats),
        figure(cost_plot, "charger-explicit-cost", "Cost trajectories for the production physical charger archetypes"),
    ])
    return


@app.cell
def _(alt, figure, inline, mo, representation, selected, table_view, vehicle_class):
    names = representation.metadata["settings"]["archetypes"]
    groups = representation.metadata["settings"]["groups"]
    short_names = {
        names["ldv_public_l2"]: "Public L2", names["ldv_public_fast"]: "Public fast",
        names["ldv_private"]: "Private", names["mhdv_overnight"]: "Overnight",
        names["mhdv_fast"]: "Fast + ultrafast", groups["ldv_public"]: "All public LDV",
        groups["ldv_all"]: "All LDV chargers", groups["mhdv_all"]: "All MHDV chargers",
    }
    class_names = set(selected.archetype)
    group_members = representation.metadata["group_members"]
    short_names.update({group: short_names[members[0]] for group, members in group_members.items()
                        if len(members) == 1 and members[0] in short_names})
    class_groups = {group for group, members in group_members.items() if class_names.intersection(members)}
    share_constraints = representation.constraints.loc[representation.constraints.sub_group.isin(class_groups | class_names)].copy()
    share_constraints["child"] = share_constraints.sub_group.map(short_names)
    share_constraints["parent"] = share_constraints.super_group.map(short_names)
    capacity_comparison = selected.melt(id_vars=["year", "archetype"], value_vars=["port_share", "capacity_share"],
                                       var_name="basis", value_name="share")
    capacity_comparison["basis"] = capacity_comparison.basis.map({"port_share": "Port share", "capacity_share": "Rated-GW share"})
    capacity_plot = alt.Chart(inline(capacity_comparison)).mark_bar().encode(
        x=alt.X("year:O", title="Calendar year"),
        y=alt.Y("share:Q", stack="zero", title="Share", axis=alt.Axis(format="%")),
        color=alt.Color("archetype:N", title="Archetype", legend=alt.Legend(orient="bottom", labelLimit=340)),
        order="archetype:N",
    ).properties(width=455, height=210).facet(column=alt.Column("basis:N", title=None, sort=["Port share", "Rated-GW share"]))
    mo.vstack([
        mo.md("""## 4. Which equipment shares does the explicit representation constrain?

        `limit_capacity_share` constrains available **GW**, including active existing
        equipment. Convert the port mix to **Group ports × Group power / Total power**.
        Applying port fractions directly to GW would impose a different equipment mix.

        For LDVs, constrain L2 versus fast within public chargers, then public versus
        private within all LDV chargers. For MHDVs, constrain overnight versus
        fast/ultrafast. The same national MHDV mix is assumed for every province.
        """),
        figure(capacity_plot, f"charger-capacity-composition-{vehicle_class.value.lower()}", f"{vehicle_class.value}: port shares and GW shares use different denominators"),
        table_view(share_constraints, {"period": "Model period", "source_year": "Calendar year", "child": "Group",
                   "parent": "Within", "share": "Required GW share"}, "Explicit-mode available-capacity equalities",
                   {"Model period": "{:.0f}", "Calendar year": "{:.0f}", "Required GW share": "{:.2%}"}),
        mo.md("""Model periods map to calendar years through the scenario's period convention.
        These period-rating targets approximate a stock containing different equipment
        vintages. These targets are emitted by explicit mode; aggregated mode pools
        the physical archetypes instead. Joint solver feasibility with existing
        equipment and charging requirements is a separate validation question.

        Exact shares apply to all available GW, including existing equipment that
        must remain in service. Combined with annual-utilization equalities, they
        can require additional charger capacity and annual energy throughput, and
        therefore additional vehicle uptake through the model's energy balances.
        They do more than redistribute future investment. Full joint Temoa
        feasibility has not yet been established."""),
    ])
    return


@app.cell
def _(mo, pd, representation, table_view, vehicle_class):
    def display_count(value):
        return f"{value:,.0f}" if pd.notna(value) else "Unavailable"

    def display_ports(value):
        return f"{value:,.1f}" if pd.notna(value) else "Unavailable"

    def display_capacity(value):
        return f"{value:.6f}" if pd.notna(value) else "Unavailable"

    fleet = representation.stock_evidence.fleet_counts.loc[representation.stock_evidence.fleet_counts.vehicle_class.eq(vehicle_class.value)].copy()
    fleet["road_class"] = fleet.road_class.map({"cars": "Cars", "light_trucks": "Light trucks",
                                             "medium_trucks": "GVWR classes 3–6", "heavy_trucks": "GVWR classes 7–8"})
    fleet["observation_status"] = fleet.observation_status.map({"observed": "Published", "partly_observed": "Incomplete", "unavailable": "Unavailable"})
    source_notice = ("This illustration uses the 2025 StatCan source-update preview; the production source selection is unchanged."
                     if representation.stock_evidence.metadata["source_is_candidate"] else "This illustration uses the registered StatCan source.")
    stock_math = pd.DataFrame(representation.metadata["stock_calculations"])
    stock_math = stock_math.loc[stock_math.vehicle_class.eq(vehicle_class.value)]
    stock_ports = representation.existing_capacity.loc[representation.existing_capacity.vehicle_class.eq(vehicle_class.value)]
    mo.vstack([
        mo.md(f"""## 5. What does a point-year existing stock alternative imply?

        The configured baseline is **{representation.metadata['baseline_year']}**. {source_notice}
        **Production uses prepared physical BEV stock.** This separate research
        illustration uses point-year **battery-electric registrations** instead;
        it is not the production `existing_capacity` calculation.
        Plug-in hybrids are excluded under the retained vehicle/port
        assumption. The source count is a vehicle population, not a charger count.

        **Total ports = Battery-electric vehicles / Vehicles per port.**

        For LDVs, keep AFDC public L2 and fast ports, subtract all public ports from
        the estimated total, and assign the remainder to private charging. Public L1
        is negligible and omitted from the modeled groups; its count remains in the audit.
        For MHDVs, apply the national overnight/fast port mix to the estimated total
        in each province. No AFDC MHDV inventory is used.
        """),
        table_view(fleet, {"region": "Region", "source_year": "Registration year", "road_class": "Registration group",
                   "known_bev_vehicles": "Published BEVs", "observation_status": "Coverage"},
                   "Vehicle evidence before applying a vehicle/port ratio",
                   {"Registration year": "{:.0f}", "Published BEVs": display_count}),
        table_view(stock_math, {"region": "Region", "known_bev_vehicles": "Known BEVs",
                   "vehicles_per_port": "Vehicles per port", "total_inferred_ports": "Inferred total ports", "status": "Interpretation"},
                   "Count-to-port calculation; incomplete registration coverage gives a lower bound",
                   {"Known BEVs": display_count, "Vehicles per port": "{:.2f}", "Inferred total ports": display_ports}),
        table_view(stock_ports, {"region": "Region", "archetype": "Archetype", "ports": "Allocated ports",
                   "rated_power_kw": "kW/port", "capacity_gw": "Existing GW"},
                   "Allocate estimated ports, then multiply by rated power",
                   {"Allocated ports": display_ports, "kW/port": "{:.2f}", "Existing GW": display_capacity}),
        mo.md("""Unpublished heavy-vehicle counts are unknown, not zero. The totals are lower
        bounds for the selected registration categories, not a freight-only fleet.
        GVWR classes 3–6 also contain some SUVs and pickups. This stock
        boundary differs from the ATB fleet weights used for future battery estimates.
        Source release, public-count dates and full class coverage are retained below."""),
    ])
    return


@app.cell
def _(diagnostic, mo, representation):
    mo.vstack([
        mo.md("""## What is adopted, and what remains a research alternative?

        The five groups make the equipment mix and its utilization assumptions
        inspectable. Battery-based durations show where a common truck battery or
        overnight window may be too coarse. Costs, annual charging capacity and
        hourly availability need compatible definitions for model interpretation.

        The production core supplies the displayed physical parameter trajectories
        and explicit-mode GW share targets. Battery recharge sensitivities and
        point-year stock estimates remain alternatives. Type-specific charging
        profiles and joint solver feasibility require separate checks; an aggregate
        hourly profile cannot establish them.
        """),
        mo.accordion({
            "Production-core parameters and explicit-mode capacity targets": mo.ui.tabs({
                "Parameters": mo.ui.table(representation.parameters, selection=None, page_size=20),
                "Constraints": mo.ui.table(representation.constraints, selection=None, page_size=20),
                "Group memberships": mo.json(representation.metadata["group_members"]),
            }),
            "Charging-duration calculations": mo.ui.table(representation.utilization_components, selection=None, page_size=20),
            "Battery and activity evidence identities": mo.json(representation.battery.metadata),
            "Registration source, release and completeness": mo.vstack([
                mo.json(representation.stock_evidence.metadata),
                mo.ui.table(representation.stock_evidence.components, selection=None, page_size=20),
            ]),
            "Assumptions, stock arithmetic and excluded public L1": mo.json(representation.metadata),
            "Source totals, cost bases and home-utilization sensitivities": mo.ui.table(representation.audit, selection=None, page_size=20),
            "Original report tables and extraction identity": mo.json({
                "tables": diagnostic.source_tables, "source_sha256": diagnostic.metadata["source_sha256"],
                "url": diagnostic.metadata["url"],
            }),
        }),
    ])
    return


if __name__ == "__main__":
    app.run()
