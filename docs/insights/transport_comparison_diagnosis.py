"""Refreshable transport parameter comparison against the transport-only legacy SQLite.

Prepare: uv run python src/build_transport.py --scenario config/scenarios/legacy_comparison.yaml --overwrite
Open: uv run marimo edit docs/insights/transport_comparison_diagnosis.py
"""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from html import escape
    import json

    import altair as alt
    import marimo as mo
    import pandas as pd

    from validation.transport_comparison import (
        explanation_rows, load_diagnostic, ranked_observations, ranked_rows,
    )

    def wrapped_text(value):
        return mo.Html(
            '<div style="white-space:pre-wrap;overflow-wrap:anywhere;min-width:0;">'
            + escape(str(value)) + '</div>'
        )

    def display_ranking(frame):
        columns = [
            "rank", "identifier", "candidate", "legacy", "delta", "relative_percent",
            "observation_count", "different_observations", "comparable_observations",
            "units", "ranking_basis",
        ]
        return mo.ui.table(
            frame[[column for column in columns if column in frame]].dropna(axis=1, how="all").reset_index(drop=True),
            selection=None,
            page_size=10,
            show_data_types=False,
            show_column_summaries=False,
            freeze_columns_left=["rank", "identifier"],
            header_tooltip={
                "observation_count": "Exact-key observations with the displayed refactored and legacy values (and bounds, when applicable).",
                "different_observations": "All numerically differing observations for this identifier in this time range and unit group.",
                "comparable_observations": "All comparable observations for this identifier, including values within tolerance.",
            },
            format_mapping={
                name: "{:.6g}" for name in (
                    "candidate", "legacy", "delta", "absolute_difference",
                    "relative_percent", "legacy_lower", "legacy_upper",
                )
            },
        )

    def explanation_table(diagnostic, table_name):
        records = explanation_rows(diagnostic.config, table_name).drop(columns="table")
        return mo.ui.table(
            records.rename(columns={
                "scope": "Technology / scope", "legacy_basis": "Legacy source / assumption",
                "refactored_basis": "Refactored source / assumption",
                "explanation": "How this drives the observed difference",
                "assumption_sections": "Assumptions to review", "evidence_paths": "Evidence paths",
            }),
            selection=None, pagination=False, show_search=False,
            show_data_types=False, show_column_summaries=False,
            wrapped_columns=[
                "Technology / scope", "Legacy source / assumption", "Refactored source / assumption",
                "How this drives the observed difference", "Assumptions to review", "Evidence paths",
            ],
        )

    def table_panel(table_name, diagnostic, ranking):
        table_summary = diagnostic.summary.loc[
            diagnostic.summary["table"].eq(table_name)
        ]
        table_rows = diagnostic.rows.loc[diagnostic.rows["table"].eq(table_name)]
        table_ranked = ranking.loc[ranking["table"].eq(table_name)]
        parts = []
        for band_name in table_summary["band"].drop_duplicates():
            band_summary = table_summary.loc[table_summary["band"].eq(band_name)].iloc[0]
            band_ranked = table_ranked.loc[table_ranked["band"].eq(band_name)]
            parts.append(mo.md(
                f"#### {band_name}\n"
                f"**{int(band_summary['different']):,} differences** among "
                f"{int(band_summary['compared']):,} comparable rows; "
                f"{int(band_summary['equal']):,} within tolerance. "
                f"{int(band_summary['different_identifiers']):,} identifiers have differences. "
                f"{int(band_summary['candidate_only']):,} refactored-only and "
                f"{int(band_summary['legacy_only']):,} legacy-only keys."
            ))
            if band_ranked.empty:
                parts.append(mo.md(
                    "No numeric differences to rank in this slice. Check coverage "
                    "and representation evidence below where rows are not comparable."
                ))
            else:
                parts.append(display_ranking(band_ranked))
        parts.extend([mo.md("#### Traced sources and assumptions"), explanation_table(diagnostic, table_name)])
        grains = table_rows["comparison_grain"].dropna().drop_duplicates().tolist()
        if grains:
            parts.append(mo.md("**Comparison basis:** " + "; ".join(grains)))
        return mo.vstack(parts)

    return (
        alt, explanation_table, json, load_diagnostic, mo, pd,
        ranked_observations, ranked_rows, table_panel, wrapped_text,
    )


@app.cell
def _(mo):
    mo.md("""
    # Transport baseline: differences to review

    Compare the supplied **transport-only legacy database** with a fresh refactored
    build using **legacy period handling**. Rankings cover parameters generated by
    this backend in regions represented by the reference. Embodied emissions,
    growth constraints and PEV market/range-share constraints are excluded.

    **Read the rankings as diagnostic evidence, not accepted parity.** Signed change
    is refactored minus legacy. Relative change uses the absolute legacy value as
    denominator; zero baselines have no finite percentage. Absolute rankings are
    separated by compatible units. Missing rows are never treated as zeros.

    Each ranked identifier appears once per time range and compatible unit group.
    Its displayed values are the observation with the largest selected difference;
    unequal values are never averaged. **observation_count** counts entries with
    those values across vintages, periods or hours. The other counts show all
    differing and comparable observations for that technology/region. Demand uses
    commodity/region identifiers. Zero-baseline observations remain in the counts
    and detailed evidence; identifiers with only zero baselines rank separately.

    Existing (2000–2023) and future (2025–2050) rankings are separate only when a
    table contains both ranges. Vintage takes precedence over operating period in
    tables containing both. The 2023 capacity cohort is retained as unmatched historical
    coverage; it is not added to, or renamed as, 2020. The current model labels end
    in 2045, with 2050 as the horizon; unmatched legacy endpoint rows remain visible.
    """)
    return


@app.cell
def _(mo):
    refresh = mo.ui.button(
        value=0, on_click=lambda count: count + 1,
        label="Reload databases and review notes", kind="success",
    )
    mo.vstack([
        refresh,
        mo.md("""
        After reviewing `docs/assumptions.md`, change the owning configuration or
        reviewed input when a numerical change is intended, then rebuild offline:

        ```powershell
        uv run python src/build_transport.py --scenario config/scenarios/legacy_comparison.yaml --overwrite
        ```

        Click **Reload** after the build completes. It rereads both databases,
        checks build fingerprints, reruns the development comparison functions,
        and rereads the relevant assumptions. Editing prose alone changes the
        displayed review evidence, not compiled parameter values. Run builds
        serially because intermediate parameter files are shared across scenarios.
        Rebuild after implementation or registered source changes as well;
        configuration fingerprints do not certify unbuilt Python or cache edits.
        Comparison settings and explanation cues are in
        `config/parameters/transport_comparison.yaml`.
        Keep those curated explanation cues current when source or assumption
        choices change; Reload updates the numerical evidence and assumption
        excerpts, but does not automatically rewrite causal explanations.

        To save a complete CSV/JSON evidence snapshot:

        ```powershell
        uv run python -m validation.transport_comparison --write-report
        ```
        """),
    ])
    return (refresh,)


@app.cell
def _(load_diagnostic, refresh):
    refresh.value
    diagnostic = load_diagnostic()
    return (diagnostic,)


@app.cell
def _(diagnostic, mo, pd):
    mo.vstack([
        mo.hstack([
            mo.stat(str(diagnostic.summary["table"].nunique()), label="Parameter tables"),
            mo.stat(f"{diagnostic.summary['compared'].sum():,}", label="Comparable rows"),
            mo.stat(f"{diagnostic.summary['different'].sum():,}", label="Numeric differences"),
        ], justify="space-around"),
        mo.md("**Shared comparison regions:** " + ", ".join(diagnostic.metadata["shared_regions"])),
        mo.md("""
        ## Comparison coverage

        These counts retain one-sided keys and unsupported representations.
        Only comparable rows enter numeric rankings. Other candidate regions and
        legacy technologies outside the new backend are counted in the scope audit.
        """),
        mo.ui.table(
            diagnostic.summary.drop(columns=["explanation"], errors="ignore"),
            selection=None, page_size=25, show_data_types=False,
            show_column_summaries=False, max_height=520,
            format_mapping={
                name: lambda value: "—" if pd.isna(value) else f"{value:,.3g}"
                for name in ("max_relative_percent", "max_absolute_difference")
            },
        ),
    ])
    return


@app.cell
def _(diagnostic, mo):
    top_n = mo.ui.slider(
        start=1, stop=max(int(diagnostic.summary.different_identifiers.max()), diagnostic.config.top_n),
        step=1, value=diagnostic.config.top_n,
        label="Top identifiers / chart bars per time range", show_value=True,
    )
    show_all = mo.ui.checkbox(value=True, label="Show all technology/region identifiers in tables")
    metric = mo.ui.dropdown(
        options={
            "Relative change (%)": "relative_percent",
            "Absolute change (within each unit)": "absolute_difference",
        },
        value="Relative change (%)", label="Rank by",
    )
    mo.hstack([metric, top_n, show_all], wrap=True)
    return metric, show_all, top_n


@app.cell
def _(diagnostic, metric, ranked_rows, show_all, top_n):
    ranking = ranked_rows(
        diagnostic.rows, len(diagnostic.rows) if show_all.value else top_n.value,
        metric=metric.value,
    )
    return (ranking,)


@app.cell
def _(diagnostic, mo, ranking, table_panel):
    mo.vstack([
        mo.md("""
        ## Ranked differences in every parameter table

        Open a table for its rankings and documented drivers. Explanations are
        review cues supported by source notes and implementation evidence;
        they do not quantify how much of a difference each assumption causes.
        The detailed inspector below shows individual row evidence and the latest
        assumption text. Counts retain the vintage/period coverage behind each
        identifier. All identifiers are shown by default; the Top control limits
        tables when Show all is switched off. Download buttons retain full precision.
        """),
        mo.accordion({
            table_name: table_panel(table_name, diagnostic, ranking)
            for table_name in diagnostic.summary["table"].drop_duplicates()
        }),
        mo.download(
            ranking.to_csv(index=False).encode("utf-8"),
            filename="transport_ranked_differences.csv", mimetype="text/csv",
            label="Download all current rankings",
        ),
    ])
    return


@app.cell
def _(diagnostic, mo):
    table_picker = mo.ui.dropdown(
        options=diagnostic.summary["table"].drop_duplicates().tolist(),
        value="cost_invest", label="Inspect parameter", searchable=True,
    )
    mo.vstack([mo.md("## Inspect rows and assumptions"), table_picker])
    return (table_picker,)


@app.cell
def _(diagnostic, ranking, table_picker):
    selected_rows = diagnostic.rows.loc[diagnostic.rows["table"].eq(table_picker.value)]
    selected_ranking = ranking.loc[ranking["table"].eq(table_picker.value)].copy()
    return selected_ranking, selected_rows


@app.cell
def _(alt, json, metric, mo, selected_ranking, top_n):
    chart_rows = selected_ranking.dropna(subset=[metric.value]).groupby(
        ["band", "units"], sort=False, dropna=False,
    ).head(top_n.value).copy()
    chart_rows["direction"] = chart_rows["delta"].ge(0).map({True: "Higher", False: "Lower"})
    chart_rows["bar_label"] = chart_rows["identifier"].astype(str)
    chart_rows["comparison_group"] = chart_rows["band"] + " · " + chart_rows["units"]
    chart_metric = "delta" if metric.value == "absolute_difference" else metric.value
    # Inline the small plotted subset: transient Arrow URLs can expire on reload
    # and previously left the chart empty even though table data were available.
    chart_columns = [
        "identifier", "candidate", "legacy", "delta", "relative_percent", "units",
        "observation_count", "direction", "bar_label", "comparison_group",
    ]
    chart_values = json.loads(chart_rows.reindex(columns=chart_columns).to_json(orient="records", double_precision=15))
    ranking_chart = alt.Chart(alt.Data(values=chart_values)).mark_bar().encode(
        x=alt.X(f"{chart_metric}:Q", title=metric.selected_key),
        y=alt.Y("bar_label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=400)),
        color=alt.Color("direction:N", scale=alt.Scale(
            domain=["Higher", "Lower"], range=["#c26b36", "#287c8e"],
        )),
        tooltip=["identifier:N", "candidate:Q", "legacy:Q", "units:N", "relative_percent:Q", "observation_count:Q"],
    ).properties(width=680, height=alt.Step(23)).facet(
        row=alt.Row("comparison_group:N", title=None),
    ).resolve_scale(x="independent", y="independent")
    chart_display = (
        mo.md("This table has no comparable numeric differences to plot. Its coverage and representation evidence remains available below.")
        if chart_rows.empty
        else mo.ui.altair_chart(ranking_chart, chart_selection=False, legend_selection=False)
    )
    mo.vstack([
        mo.md(f"**Ranked signed changes — up to {top_n.value} identifiers per time range and unit.** Unit groups have independent axes. Rows with a zero legacy value have no percentage bar; inspect their absolute changes in the tables."),
        chart_display,
    ])
    return


@app.cell
def _(mo, selected_ranking):
    row_options = {
        f"{index + 1}. {row.band}: {row.identifier}": index
        for index, row in enumerate(selected_ranking.itertuples(index=False))
    }
    row_picker = mo.ui.dropdown(
        options=row_options, value=next(iter(row_options), None),
        label="Ranked identifier evidence", searchable=True,
    )
    row_picker
    return (row_picker,)


@app.cell
def _(diagnostic, mo, pd, ranked_observations, row_picker, selected_ranking, selected_rows, wrapped_text):
    if row_picker.value is None:
        row_evidence = mo.md("No comparable numeric differences in this table.")
    else:
        selected_record = selected_ranking.iloc[row_picker.value]
        numeric_fields = {
            "candidate": "Refactored", "legacy": "Legacy", "delta": "Signed change",
            "relative_percent": "Change (%)", "units": "Unit",
            "legacy_lower": "Legacy lower bound", "legacy_upper": "Legacy upper bound",
            "comparison_period": "Compared operating period",
        }
        numeric_values = {
            label: selected_record[name] for name, label in numeric_fields.items()
            if name in selected_record and pd.notna(selected_record[name])
        }
        selected_observations = ranked_observations(selected_rows, selected_record)
        observation_columns = [
            "identifier", "status", "candidate", "legacy", "delta", "relative_percent", "units",
        ]
        source_id = selected_record.get("data_source", "")
        source_names = [name for name, identifier in diagnostic.metadata["source_id_mapping"].items()
                        if identifier == source_id]
        source_label = ", ".join(source_names) or str(source_id) or "Internal structural parameter"
        row_evidence = mo.vstack([
            mo.md(f"### {selected_record['identifier']}\n\n"
                  f"**Observations at the displayed values: {int(selected_record['observation_count']):,}**; "
                  f"{int(selected_record['different_observations']):,} differing / "
                  f"{int(selected_record['comparable_observations']):,} comparable / "
                  f"{int(selected_record['total_observations']):,} total observations. "
                  f"{int(selected_record['zero_baseline_observations']):,} zero-baseline differences.\n\n"
                  f"**Representative keys:** `{selected_record['representative_identifier']}`\n\n"
                  f"**Comparison basis:** {selected_record['comparison_grain']}"),
            mo.ui.table(
                pd.DataFrame([numeric_values]), selection=None, pagination=False,
                show_search=False, show_data_types=False, show_column_summaries=False,
                format_mapping={label: "{:.6g}" for name, label in numeric_fields.items() if name != "units"},
            ),
            mo.hstack([
                mo.vstack([
                    mo.md(f"**Refactored source:** `{source_label}`"),
                    wrapped_text(selected_record.get("notes", "") or "No row annotation."),
                ]),
                mo.vstack([
                    mo.md("**Legacy annotation and reference**"),
                    wrapped_text(selected_record.get("legacy_notes", "") or "No row annotation."),
                    wrapped_text(selected_record.get("legacy_reference", "") or "No row reference."),
                ]),
            ], widths="equal", align="start", wrap=True),
            mo.md(
                f"**Vintages at the displayed values:** {selected_record['value_vintages'] or 'Not vintage-indexed'}. "
                f"**Periods at the displayed values:** {selected_record['value_periods'] or 'Not period-indexed'}."
            ),
            mo.accordion({"Exact-key observations for this identifier": mo.ui.table(
                selected_observations[observation_columns].reset_index(drop=True),
                selection=None, page_size=10, show_data_types=False, show_column_summaries=False,
            )}),
        ])
    mo.vstack([
        mo.md("**Compiled row evidence.** Database notes are historical annotations and may be stale; numerical values and current configuration take precedence."),
        row_evidence,
    ])
    return


@app.cell
def _(diagnostic, explanation_table, mo, table_picker):
    assumption_sections = diagnostic.metadata["assumption_excerpts"][table_picker.value]
    mo.vstack([
        mo.md("### Traced sources and assumptions"),
        explanation_table(diagnostic, table_picker.value),
        mo.accordion({
            section["heading"]: mo.md(section["text"])
            for section in assumption_sections
        }),
        mo.md(
            "**Current scenario selections:** "
            f"CEUD {diagnostic.bundle.scenario.periods.base_year}; "
            f"demand {diagnostic.bundle.scenario.demand.future_car_demand}; "
            f"efficiency ATB {diagnostic.bundle.scenario.efficiencies.atb_trajectory}; "
            f"cost ATB {diagnostic.bundle.scenario.costs.atb_trajectory}; "
            f"charging {diagnostic.bundle.scenario.charging_profiles.travel_behavior_source}."
        ),
    ])
    return


@app.cell
def _(mo, selected_rows):
    coverage_rows = selected_rows.loc[~selected_rows["status"].isin(["equal", "different"])]
    coverage_columns = [
        "band", "status", "identifier", "candidate", "legacy", "units",
        "comparison_grain", "notes", "legacy_notes",
    ]
    mo.vstack([
        mo.md(f"### Coverage and representation evidence\n\n{len(coverage_rows):,} rows excluded from numeric ranking for this table. The preview is limited to 250 rows; download the complete evidence below."),
        mo.ui.table(
            coverage_rows[[name for name in coverage_columns if name in coverage_rows]].head(250).reset_index(drop=True),
            selection=None, page_size=10, show_data_types=False,
            show_column_summaries=False,
        ),
        mo.download(
            selected_rows.to_csv(index=False).encode("utf-8"),
            filename="transport_selected_table_evidence.csv", mimetype="text/csv",
            label="Download complete selected table evidence",
        ),
    ])
    return


@app.cell
def _(diagnostic, json, mo):
    mo.accordion({
        "Snapshot identity, freshness and scope audit": mo.json(diagnostic.metadata),
        "Reused parameter-development comparisons": mo.vstack([
            mo.md("These established comparisons were rerun against this exact pair of databases. Their original scopes and representation limitations are retained; the rankings above add unit, coverage and temporal handling."),
            mo.json(diagnostic.development_comparisons),
        ]),
        "Download full diagnostic rows": mo.download(
            diagnostic.rows.to_csv(index=False).encode("utf-8"),
            filename="transport_comparison_all_rows.csv", mimetype="text/csv",
            label="Download all rows and coverage",
        ),
        "Download snapshot metadata": mo.download(
            json.dumps(diagnostic.metadata, indent=2).encode("utf-8"),
            filename="transport_comparison_snapshot.json", mimetype="application/json",
            label="Download snapshot identity",
        ),
    })
    return


if __name__ == "__main__":
    app.run()
