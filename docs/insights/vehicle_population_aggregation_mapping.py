"""Ontario Report A vehicle mapping and aggregation diagnostic."""

import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell
def _():
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import pandas as pd

    from utils import (
        load_config_bundle,
        load_harmonization_rules,
        resolve_input_path,
        resolve_parameter_path,
    )

    NLR_ORDER = ["Compact", "Midsize", "Small SUV", "Midsize SUV", "Pickup"]
    NRCAN_CLASS_ORDER = {
        "Car": [
            "Two-seater",
            "Minicompact",
            "Subcompact",
            "Compact",
            "Mid-size",
            "Full-size",
            "Station wagon: Small",
            "Station wagon: Mid-size",
        ],
        "Light Truck": [
            "Sport utility vehicle: Small",
            "Sport utility vehicle: Standard",
            "Pickup truck: Small",
            "Pickup truck: Standard",
            "Minivan",
            "Van: Passenger",
            "Van: Cargo",
        ],
    }
    STATUS_ORDER = [
        "FIT_ACTIVE",
        "FIT_INACTIVE",
        "UNFIT",
        "WRECKED",
        "OUT_OF_PROV",
        "SOLD",
        "SUSPENDED",
        "TEMPORARY",
    ]
    STATUS_COLORS = [
        "#2a9d8f",
        "#e9c46a",
        "#e76f51",
        "#8d99ae",
        "#457b9d",
        "#f4a261",
        "#9b5de5",
        "#90be6d",
    ]


    def chart_ui(chart: alt.TopLevelMixin) -> mo.Html:
        """Render an Altair chart without marimo selection callbacks."""
        return mo.ui.altair_chart(
            chart,
            chart_selection=False,
            legend_selection=False,
        )

    return (
        NLR_ORDER,
        NRCAN_CLASS_ORDER,
        Path,
        STATUS_COLORS,
        STATUS_ORDER,
        alt,
        chart_ui,
        load_config_bundle,
        load_harmonization_rules,
        mo,
        pd,
        resolve_input_path,
        resolve_parameter_path,
    )


@app.cell
def introduction(mo):
    mo.md("""
    # Ontario Report A vehicle mapping and survival diagnostic

    This notebook asks four questions: how much latest fit-active stock is mapped;
    why the remainder cannot be mapped reliably; what class and vintage weights the
    accepted subset implies; and what the available annual cohorts can actually say
    about retention and survival.

    It is a minimal exploratory UI over version-controlled configuration and
    backend artifacts. It does not fetch sources, duplicate ETL transformations,
    modify configuration, or render notebook exports. The noisy 2015 edition is
    excluded. `PASSENGER` and `COMMERCIAL` are treated equally by the mapping and
    cohort logic, while remaining visible as source categories in the diagnostics.

    **Mapping bootstrap and promotion policy.** The development-only bootstrap first
    pools positive fit-active stock by MTO make/model/model-year across Report A
    editions. It normalizes punctuation and case, resolves configured make aliases,
    and compares each MTO model label with canonical model families assembled from
    NRCan Fuel Consumption Ratings and FuelEconomy.gov. Candidates are ranked within
    the normalized make; ties at the best score remain ambiguous. The best unambiguous
    family is then resolved to a single NRCan → NLR ATB → CEUD hierarchy, checked
    against exact-year public evidence and the dedicated vPIC temporal audit where
    applicable, and collapsed into non-overlapping vintage ranges. Ordinary backend
    runs only read the resulting reviewed CSV and never repeat this inference.

    The model similarity is deterministic. After removing punctuation, spacing, and
    case, identical labels score **1.00**, a prefix relationship scores **0.95**, and
    an MTO label contained within the canonical family scores **0.90**. All other
    pairs use Python's sequence-matching ratio: twice the total length of matching
    blocks divided by the combined label lengths, on a 0–1 scale. As an explicit
    review decision, every unambiguous rank-one candidate scoring **at least 0.70** is
    now promoted; this includes all pending exact and prefix strong-label candidates
    and supersedes conflicting lower-impact manual-pass arbitration for that unique
    approved family.
    Explicit configured make/model corrections remain stronger than the generic
    similarity rule.
    The original score and match method remain in generated evidence. Manual candidates
    superseded by the high-stock re-audit are not restored by this rule.
    """)
    return


@app.cell
def load_evidence(
    Path,
    load_config_bundle,
    load_harmonization_rules,
    pd,
    resolve_input_path,
    resolve_parameter_path,
):
    scenario_path = Path("config/scenarios/legacy_reproduction.yaml")


    def load_evidence(selected_scenario: str | Path) -> dict[str, pd.DataFrame]:
        bundle = load_config_bundle(selected_scenario)
        ontario_rules = load_harmonization_rules(bundle, "ontario_vehicle_population")
        road_rules = load_harmonization_rules(bundle, "road_aggregation")
        stock_rules = load_harmonization_rules(bundle, "stocks_and_demands")["ontario_report_a"]
        lifetime_rules = load_harmonization_rules(bundle, "lifetimes_survival")
        output_dir = resolve_input_path(bundle, "interim", ontario_rules["interim_subdir"])
        artifact_names = {
            "coverage": road_rules["coverage_file"],
            "unresolved_reason_detail": road_rules["unresolved_reason_detail_file"],
            "unresolved_reason_summary": road_rules["unresolved_reason_summary_file"],
            "latest_unresolved_worklist": road_rules["latest_unresolved_worklist_file"],
            "mapped_fleet": road_rules["mapped_current_stock_file"],
            "age_distribution": stock_rules["age_distribution_file"],
            "nlr_weights": road_rules["nlr_weights_file"],
            "fleet_composition_weights": road_rules["fleet_composition_weights_file"],
            "vehicle_class_evidence": road_rules["vehicle_class_evidence_file"],
            "mapping_bootstrap": road_rules["bootstrap_evidence_file"],
            "wards_comparison": road_rules["wards_comparison_file"],
            "reconciliation": ontario_rules["reconciliation_file"],
            "status_long": ontario_rules["long_status_file"],
            "top_observations": road_rules["top_observations_file"],
            "passenger_cohorts": lifetime_rules["cohort_snapshot_file"],
            "commercial_cohorts": lifetime_rules["commercial_snapshot_file"],
            "cohort_transitions": lifetime_rules["transition_observations_file"],
            "raw_key_snapshots": lifetime_rules["raw_key_snapshot_file"],
            "raw_key_transitions": lifetime_rules["raw_key_transition_file"],
            "mapped_key_transitions": lifetime_rules["mapped_key_transition_file"],
            "nlr_vintage_retention": lifetime_rules["nlr_class_vintage_retention_file"],
            "nlr_class_retention": lifetime_rules["nlr_class_retention_file"],
            "ceud_vintage_retention": lifetime_rules["ceud_class_vintage_retention_file"],
            "ceud_class_retention": lifetime_rules["ceud_class_retention_file"],
            "ceud_scope_comparison": lifetime_rules["ceud_scope_comparison_file"],
            "transition_mapping_coverage": lifetime_rules["transition_mapping_coverage_file"],
            "mto_survival_decision": lifetime_rules["mto_survival_decision_file"],
            "pooled_retention": lifetime_rules["pooled_estimates_file"],
            "retention_comparison": lifetime_rules["retention_comparison_file"],
            "legacy_survival": lifetime_rules["legacy_survival_curves_file"],
            "nlr_survival": lifetime_rules["nlr_survival_curves_file"],
            "source_survival": lifetime_rules["source_curves_file"],
            "transformed_survival": lifetime_rules["transformed_curves_file"],
            "median_lifetimes": lifetime_rules["median_lifetimes_file"],
        }
        frames: dict[str, pd.DataFrame] = {}
        for key, filename in artifact_names.items():
            artifact_path = output_dir / str(filename)
            if not artifact_path.is_file():
                raise FileNotFoundError(
                    "Generate Ontario backend artifacts before opening the diagnostic: "
                    f"{artifact_path}"
                )
            frames[key] = pd.read_csv(artifact_path, low_memory=False)
        mapping_path = resolve_parameter_path(bundle, road_rules["vehicle_size_class_map_file"])
        frames["mapping_config"] = pd.read_csv(mapping_path, low_memory=False)
        return frames

    return load_evidence, scenario_path


@app.cell
def _(load_evidence, scenario_path):
    evidence = load_evidence(scenario_path)
    return (evidence,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Source integrity
    """)
    return


@app.cell(hide_code=True)
def source_reconciliation_heading(mo):
    mo.md("""
    ### Source reconciliation
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        reconciliation = evidence["reconciliation"].copy()
        for _column in ["source_count", "long_count", "difference"]:
            reconciliation[_column] = pd.to_numeric(reconciliation[_column], errors="coerce")
        reconciliation_summary = (
            reconciliation.groupby("report_year", as_index=False)
            .agg(
                source_count=("source_count", "sum"),
                normalized_count=("long_count", "sum"),
                status_groups=("stock_status", "size"),
                unreconciled_groups=("reconciled", lambda values: int((~values.astype(str).str.lower().eq("true")).sum())),
                absolute_difference=("difference", lambda values: values.abs().sum()),
            )
            .sort_values("report_year")
        )
        reconciliation_long = reconciliation_summary.melt(
            id_vars=["report_year"],
            value_vars=["source_count", "normalized_count"],
            var_name="measure",
            value_name="vehicles",
        )
        reconciliation_chart = (
            alt.Chart(reconciliation_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("report_year:O", title="Report year"),
                y=alt.Y("vehicles:Q", title="Vehicles across statuses", scale=alt.Scale(domain=[50_000_000, 80_000_000])),
                color=alt.Color("measure:N", title="Count"),
                strokeDash=alt.StrokeDash("measure:N", title="Count"),
                tooltip=["report_year:O", "measure:N", alt.Tooltip("vehicles:Q", format=",")],
            )
            .properties(width=700, height=300)
        )
        reconciliation_difference = reconciliation_summary["absolute_difference"].sum()
        source_reconciliation_output = mo.vstack([
            mo.md("""
            This tests conservation during normalization, not mapping
            correctness. `source_count` is the annual Report A total before reshaping;
            `normalized_count` is the long-status total afterward. A zero absolute
            difference means registrations were neither created nor lost.
            """),
            mo.stat(f"{int(reconciliation_difference):,}", "Absolute reconciliation difference", "Zero means every annual status total is conserved", bordered=True, target_direction="decrease"),
            chart_ui(reconciliation_chart),
        ])
        return source_reconciliation_output

    _build_view()
    return


@app.cell(hide_code=True)
def mapping_quality_section(mo):
    mo.md("""
    ## Mapping quality and unresolved evidence
    """)
    return


@app.cell(hide_code=True)
def registration_status_heading(mo):
    mo.md("""
    ### Registration-status evidence
    """)
    return


@app.cell
def _(STATUS_COLORS, STATUS_ORDER, alt, chart_ui, evidence, mo, pd):
    def _build_view():
        registration_status_rows = evidence["status_long"].copy()
        for _column in ["report_year", "MODEL_YEAR", "native_count"]:
            registration_status_rows[_column] = pd.to_numeric(
                registration_status_rows[_column], errors="coerce"
            )
        registration_status_rows = registration_status_rows.loc[
            registration_status_rows["VEHICLE_CLASS"].isin(["PASSENGER", "COMMERCIAL"])
            & registration_status_rows["stock_status"].isin(STATUS_ORDER)
        ].copy()
        registration_status_rows["status_order"] = registration_status_rows[
            "stock_status"
        ].map({status: index for index, status in enumerate(STATUS_ORDER)})
        registration_latest_year = int(registration_status_rows["report_year"].max())

        registration_edition_status = (
            registration_status_rows.groupby(
                ["VEHICLE_CLASS", "report_year", "stock_status", "status_order"],
                as_index=False,
            )["native_count"]
            .sum()
            .rename(columns={"native_count": "vehicle_records"})
        )
        registration_edition_status["status_share"] = registration_edition_status[
            "vehicle_records"
        ] / registration_edition_status.groupby(
            ["VEHICLE_CLASS", "report_year"]
        )["vehicle_records"].transform("sum")

        registration_latest_model_year = (
            registration_status_rows.loc[
                registration_status_rows["report_year"].eq(registration_latest_year)
                & registration_status_rows["MODEL_YEAR"].between(
                    2000, registration_latest_year
                )
            ]
            .groupby(
                ["VEHICLE_CLASS", "MODEL_YEAR", "stock_status", "status_order"],
                as_index=False,
            )["native_count"]
            .sum()
            .rename(
                columns={"MODEL_YEAR": "model_year", "native_count": "vehicle_records"}
            )
        )

        registration_latest_counts = (
            registration_status_rows.loc[
                registration_status_rows["report_year"].eq(registration_latest_year)
            ]
            .groupby("stock_status", as_index=False)["native_count"]
            .sum()
            .rename(columns={"native_count": "latest_vehicle_records"})
        )
        registration_status_definitions = pd.DataFrame(
            [
                {
                    "stock_status": "FIT_ACTIVE",
                    "source meaning": "Vehicle status FIT in Report A's ACTIVE subcolumn.",
                    "inferred working interpretation": "Registered, insured, and currently allowed on public roads with valid plates.",
                    "diagnostic caution": "The mapping stock basis; it does not prove that the vehicle was driven during the year.",
                },
                {
                    "stock_status": "FIT_INACTIVE",
                    "source meaning": "Vehicle status FIT in Report A's INACTIVE subcolumn.",
                    "inferred working interpretation": "Meets fit requirements, but is not currently plated or insured for active road use.",
                    "diagnostic caution": "MTO does not state the cause or duration of inactivity in the published dictionary.",
                },
                {
                    "stock_status": "UNFIT",
                    "source meaning": "MTO code UNF: permit/record has unfit status.",
                    "inferred working interpretation": "Fails fit or safety requirements and cannot currently have plates attached.",
                    "diagnostic caution": "Ontario permit rules prevent plate approval or validation until fit requirements are met; this is not necessarily permanent retirement.",
                },
                {
                    "stock_status": "WRECKED",
                    "source meaning": "MTO code WRK: Wrecked.",
                    "inferred working interpretation": "Declared heavily damaged, dismantled, or written off.",
                    "diagnostic caution": "The published dictionary supplies the label but no event date or permanence definition.",
                },
                {
                    "stock_status": "OUT_OF_PROV",
                    "source meaning": "MTO code OOP: Out of Province.",
                    "inferred working interpretation": "An out-of-province record pending or associated with Ontario registration.",
                    "diagnostic caution": "A jurisdictional status, not physical scrappage.",
                },
                {
                    "stock_status": "SOLD",
                    "source meaning": "MTO code SLD: Sold.",
                    "inferred working interpretation": "An ownership transfer has been recorded.",
                    "diagnostic caution": "Ownership/status changed; the vehicle may remain in service.",
                },
                {
                    "stock_status": "SUSPENDED",
                    "source meaning": "MTO code SUS: Suspended.",
                    "inferred working interpretation": "Registration or plate privileges are currently suspended.",
                    "diagnostic caution": "A registration/permit status that can be reversible.",
                },
                {
                    "stock_status": "TEMPORARY",
                    "source meaning": "MTO code TMP: Temporary.",
                    "inferred working interpretation": "A short-term permit or other temporary registration condition.",
                    "diagnostic caution": "The published dictionary does not define the temporary condition further.",
                },
            ]
        )
        registration_status_summary = registration_status_definitions.merge(
            registration_latest_counts, on="stock_status", how="left"
        )
        registration_status_summary["share_of_latest_all_statuses"] = (
            registration_status_summary["latest_vehicle_records"]
            / registration_status_summary["latest_vehicle_records"].sum()
        )

        def registration_share_chart(vehicle_class: str) -> alt.Chart:
            rows = registration_edition_status.loc[
                registration_edition_status["VEHICLE_CLASS"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "status_share:Q",
                        title="Share of observed registration statuses",
                        axis=alt.Axis(format="%"),
                        scale=alt.Scale(domain=[0, 1]),
                    ),
                    y=alt.Y(
                        "report_year:O",
                        title="Report edition",
                        sort="descending",
                        axis=alt.Axis(labelAngle=0),
                    ),
                    color=alt.Color(
                        "stock_status:N",
                        title="Registration status",
                        scale=alt.Scale(domain=STATUS_ORDER, range=STATUS_COLORS),
                    ),
                    order=alt.Order("status_order:Q"),
                    tooltip=[
                        "VEHICLE_CLASS:N",
                        "report_year:O",
                        "stock_status:N",
                        alt.Tooltip("vehicle_records:Q", format=",", title="Vehicles"),
                        alt.Tooltip("status_share:Q", format=".2%", title="Status share"),
                    ],
                )
                .properties(
                    width=760,
                    height=260,
                    title=(
                        f"{vehicle_class.title()} registration-status shares "
                        "across Report A editions"
                    ),
                )
            )

        def registration_heatmap(vehicle_class: str) -> alt.Chart:
            rows = registration_latest_model_year.loc[
                registration_latest_model_year["VEHICLE_CLASS"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_rect(stroke="white", strokeWidth=0.4)
                .encode(
                    x=alt.X(
                        "model_year:O",
                        title="Model year",
                        sort="ascending",
                        axis=alt.Axis(labelAngle=0),
                    ),
                    y=alt.Y(
                        "stock_status:N",
                        title=None,
                        sort=STATUS_ORDER,
                        axis=alt.Axis(labelLimit=150),
                    ),
                    color=alt.Color(
                        "vehicle_records:Q",
                        title="Vehicles",
                        scale=alt.Scale(type="sqrt", scheme="viridis"),
                    ),
                    tooltip=[
                        "VEHICLE_CLASS:N",
                        "stock_status:N",
                        "model_year:O",
                        alt.Tooltip("vehicle_records:Q", format=",", title="Vehicles"),
                    ],
                )
                .properties(
                    width=800,
                    height=230,
                    title=f"{vehicle_class.title()} status stock by model year, {registration_latest_year}",
                )
            )

        registration_status_output = mo.vstack(
            [
                mo.md("""
                The percentage bars use every fetched Report A edition and their
                eight registration statuses. Passenger and Commercial use the same
                status order and colors. 
                The MTO vehicle population dictionary provides codes and short labels, not complete
                lifecycle definitions. The interpretations below are working inferences:

                - **FIT_ACTIVE:** registered, insured, and currently allowed on public roads with valid plates.
                - **FIT_INACTIVE:** meets fit requirements, but is not currently plated or insured for active road use.
                - **UNFIT:** fails fit or safety requirements and cannot currently have plates attached.
                - **WRECKED:** declared heavily damaged, dismantled, or written off.
                - **OUT_OF_PROV:** an out-of-province record pending or associated with Ontario registration.
                - **SOLD:** an ownership transfer has been recorded.
                - **SUSPENDED:** registration or plate privileges are currently suspended.
                - **TEMPORARY:** a short-term permit or other temporary registration condition.
                """),
                mo.ui.tabs(
                    {
                        "Passenger": chart_ui(registration_share_chart("PASSENGER")),
                        "Commercial": chart_ui(
                            registration_share_chart("COMMERCIAL")
                        ),
                    }
                ),
                mo.md("""
                The heat maps then show the latest edition's
                absolute stock by status and model year from 2000 onward; their square-root
                color scale keeps small administrative buckets visible beside fit-active
                stock.
                """),
                chart_ui(registration_heatmap("PASSENGER")),
                chart_ui(registration_heatmap("COMMERCIAL")),
                mo.ui.tabs(
                    {
                        "Registration-status definitions": mo.ui.table(
                            registration_status_summary,
                            selection=None,
                            pagination=False,
                            page_size=10,
                            wrapped_columns=[
                                "source meaning",
                                "inferred working interpretation",
                                "diagnostic caution",
                            ],
                            format_mapping={
                                "share_of_latest_all_statuses": "{:.2%}"
                            },
                        ),
                        "All-edition status shares": mo.ui.table(
                            registration_edition_status.sort_values(
                                ["VEHICLE_CLASS", "report_year", "status_order"]
                            ),
                            selection=None,
                            pagination=True,
                            page_size=10,
                            format_mapping={"status_share": "{:.2%}"},
                        ),
                    }
                ),
            ]
        )
        return registration_status_output

    _build_view()
    return


@app.cell(hide_code=True)
def mapping_progress_heading(mo):
    mo.md("""
    ### Mapping progress
    """)
    return


@app.cell
def _(NLR_ORDER, alt, chart_ui, evidence, mo, pd):
    def _build_view():
        coverage = evidence["coverage"].copy()
        for _column in ["mapped_ldv", "mapped_non_ldv", "unmapped", "out_of_scope", "total", "mapped_ldv_share", "mapped_non_ldv_share", "unmapped_share", "out_of_scope_share"]:
            coverage[_column] = pd.to_numeric(coverage[_column], errors="coerce")
        mapping_cohorts = pd.concat(
            [evidence["passenger_cohorts"], evidence["commercial_cohorts"]],
            ignore_index=True,
        )
        for _column in ["report_year", "model_year", "cohort_count"]:
            mapping_cohorts[_column] = pd.to_numeric(
                mapping_cohorts[_column], errors="coerce"
            )
        mapping_cohorts["vehicle_class"] = mapping_cohorts["population_group"].map(
            {
                "mapped_passenger": "PASSENGER",
                "mapped_commercial": "COMMERCIAL",
            }
        )
        mapping_model_year_mapped = (
            mapping_cohorts.loc[
                mapping_cohorts["stock_status"].eq("FIT_ACTIVE")
                & mapping_cohorts["model_year"].between(
                    2000, mapping_cohorts["report_year"]
                )
            ]
            .groupby(
                ["vehicle_class", "report_year", "model_year"], as_index=False
            )["cohort_count"]
            .sum()
            .rename(columns={"cohort_count": "mapped_fit_active"})
        )
        mapping_status_totals = evidence["status_long"].copy()
        for _column in ["report_year", "MODEL_YEAR", "native_count"]:
            mapping_status_totals[_column] = pd.to_numeric(
                mapping_status_totals[_column], errors="coerce"
            )
        mapping_config = evidence["mapping_config"].copy()
        accepted_mapping_ranges = mapping_config.loc[
            mapping_config["entry_type"].eq("mto_crosswalk"),
            [
                "mto_make_code",
                "mto_model_code",
                "model_year_from",
                "model_year_to",
                "vehicle_scope",
            ],
        ].copy()
        for _column in ["model_year_from", "model_year_to"]:
            accepted_mapping_ranges[_column] = pd.to_numeric(
                accepted_mapping_ranges[_column], errors="coerce"
            ).astype(int)
        accepted_mapping_years = accepted_mapping_ranges.assign(
            MODEL_YEAR=accepted_mapping_ranges.apply(
                lambda row: list(
                    range(int(row["model_year_from"]), int(row["model_year_to"]) + 1)
                ),
                axis=1,
            )
        ).explode("MODEL_YEAR")
        accepted_mapping_years["MODEL_YEAR"] = accepted_mapping_years[
            "MODEL_YEAR"
        ].astype(int)
        accepted_mapping_years["mapping_outcome"] = accepted_mapping_years[
            "vehicle_scope"
        ].map({"ldv": "mapped_ldv", "mhdv": "mapped_non_ldv", "non_ldv_unclassified": "mapped_non_ldv"})
        accepted_mapping_years = accepted_mapping_years[
            ["mto_make_code", "mto_model_code", "MODEL_YEAR", "mapping_outcome"]
        ].drop_duplicates()
        mapping_model_year_floor = int(
            accepted_mapping_ranges["model_year_from"].min()
        )
        mapping_row_source = mapping_status_totals.loc[
            mapping_status_totals["stock_status"].eq("FIT_ACTIVE")
            & mapping_status_totals["VEHICLE_CLASS"].isin(
                ["PASSENGER", "COMMERCIAL"]
            )
        ].merge(
            accepted_mapping_years,
            left_on=["MAKE", "MODEL", "MODEL_YEAR"],
            right_on=["mto_make_code", "mto_model_code", "MODEL_YEAR"],
            how="left",
            validate="many_to_one",
        )
        mapping_row_source["mapping_outcome"] = mapping_row_source[
            "mapping_outcome"
        ].fillna("unmapped")
        mapping_row_source.loc[
            mapping_row_source["MODEL_YEAR"].lt(mapping_model_year_floor),
            "mapping_outcome",
        ] = "out_of_scope"
        mapping_reason_labels = {
            "weak_model_label_agreement": "Weak model-label agreement",
            "ambiguous_top_candidate": "Ambiguous best candidate",
            "no_normalized_make_agreement": "No normalized make agreement",
            "suppressed_or_unknown_code": "Suppressed or unknown source code",
            "no_model_label_candidate": "No model-family candidate",
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Strong label candidate",
        }
        latest_reason_keys = (
            evidence["unresolved_reason_detail"][["VEHICLE_CLASS", "MAKE", "MODEL", "unresolved_reason"]]
            .drop_duplicates()
            .groupby(["VEHICLE_CLASS", "MAKE", "MODEL"], as_index=False)["unresolved_reason"]
            .first()
        )
        latest_report_year = int(mapping_row_source["report_year"].max())
        latest_keys = mapping_row_source.loc[
            mapping_row_source["report_year"].eq(latest_report_year),
            ["VEHICLE_CLASS", "MAKE", "MODEL"],
        ].drop_duplicates().assign(present_in_latest=True)
        mapping_row_source = mapping_row_source.merge(
            latest_reason_keys,
            on=["VEHICLE_CLASS", "MAKE", "MODEL"],
            how="left",
            validate="many_to_one",
        ).merge(
            latest_keys,
            on=["VEHICLE_CLASS", "MAKE", "MODEL"],
            how="left",
            validate="many_to_one",
        )
        mapping_row_source["row_reason"] = mapping_row_source[
            "unresolved_reason"
        ].fillna("no_model_label_candidate")
        mapping_row_source["present_in_latest"] = (
            mapping_row_source["present_in_latest"].fillna(False).astype(bool)
        )
        mapping_row_source.loc[
            ~mapping_row_source["present_in_latest"], "row_reason"
        ] = "not_present_in_latest_snapshot"
        mapping_row_source.loc[mapping_row_source["mapping_outcome"].eq("mapped_ldv"), "row_reason"] = "mapped_ldv"
        mapping_row_source.loc[mapping_row_source["mapping_outcome"].eq("mapped_non_ldv"), "row_reason"] = "mapped_non_ldv"
        mapping_row_source.loc[mapping_row_source["mapping_outcome"].eq("out_of_scope"), "row_reason"] = "out_of_scope"
        top_row_reasons = (
            mapping_row_source.loc[mapping_row_source["mapping_outcome"].eq("unmapped")]
            .groupby("row_reason", as_index=False)
            .size()
            .nlargest(4, "size")["row_reason"]
            .tolist()
        )
        mapping_row_source["row_reason_group"] = mapping_row_source["row_reason"].where(
            mapping_row_source["row_reason"].isin(["mapped_ldv", "mapped_non_ldv", "out_of_scope"])
            | mapping_row_source["row_reason"].isin(top_row_reasons),
            "other",
        )
        mapping_row_labels = {
            "mapped_ldv": "Mapped to Car or Light Truck",
            "mapped_non_ldv": "Mapped, non-LDV",
            "out_of_scope": "Outside 1981+ scope",
            "other": "Other",
            **mapping_reason_labels,
        }
        mapping_edition_row_parts = (
            mapping_row_source.groupby(
                ["VEHICLE_CLASS", "report_year", "row_reason_group"], as_index=False
            )
            .size()
            .rename(columns={"size": "rows"})
        )
        mapping_edition_row_parts["mapping_status"] = mapping_edition_row_parts[
            "row_reason_group"
        ].map(mapping_row_labels)
        mapping_row_order = [
            "Mapped to Car or Light Truck",
            "Mapped, non-LDV",
            "Outside 1981+ scope",
            *[mapping_row_labels[reason] for reason in top_row_reasons],
            "Other",
        ]
        mapping_edition_row_parts["status_order"] = mapping_edition_row_parts[
            "mapping_status"
        ].map({label: index for index, label in enumerate(mapping_row_order)})
        mapping_edition_row_parts["total_rows"] = mapping_edition_row_parts.groupby(
            ["VEHICLE_CLASS", "report_year"]
        )["rows"].transform("sum")
        mapping_edition_row_parts["share"] = (
            mapping_edition_row_parts["rows"]
            / mapping_edition_row_parts["total_rows"]
        )
        mapping_model_year_total = (
            mapping_status_totals.loc[
                mapping_status_totals["stock_status"].eq("FIT_ACTIVE")
                & mapping_status_totals["VEHICLE_CLASS"].isin(
                    ["PASSENGER", "COMMERCIAL"]
                )
                & mapping_status_totals["MODEL_YEAR"].between(
                    2000, mapping_status_totals["report_year"]
                )
            ]
            .groupby(
                ["VEHICLE_CLASS", "report_year", "MODEL_YEAR"], as_index=False
            )["native_count"]
            .sum()
            .rename(
                columns={
                    "VEHICLE_CLASS": "vehicle_class",
                    "MODEL_YEAR": "model_year",
                    "native_count": "total_fit_active",
                }
            )
        )
        mapping_model_year_coverage = mapping_model_year_total.merge(
            mapping_model_year_mapped,
            on=["vehicle_class", "report_year", "model_year"],
            how="left",
            validate="one_to_one",
        )
        mapping_model_year_coverage["mapped_fit_active"] = (
            mapping_model_year_coverage["mapped_fit_active"].fillna(0)
        )
        mapping_model_year_coverage["mapping_coverage"] = (
            mapping_model_year_coverage["mapped_fit_active"]
            / mapping_model_year_coverage["total_fit_active"]
        )
        mapping_edition_order = sorted(
            mapping_model_year_coverage["report_year"].dropna().astype(int).unique()
        )


        def edition_row_coverage_chart(vehicle_class: str) -> alt.Chart:
            rows = mapping_edition_row_parts.loc[
                mapping_edition_row_parts["VEHICLE_CLASS"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_bar()
                .encode(
                    x=alt.X("share:Q", title="Share of Report A rows", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    y=alt.Y("report_year:O", title="Report edition", sort="descending", axis=alt.Axis(labelAngle=0)),
                    color=alt.Color("mapping_status:N", title="Mapping result", scale=alt.Scale(domain=mapping_row_order, range=["#2a9d8f", "#457b9d", "#8d99ae", "#e76f51", "#f4a261", "#c77dff", "#d9d9d9", "#b8b8b8"][:len(mapping_row_order)])),
                    order=alt.Order("status_order:Q"),
                    tooltip=["VEHICLE_CLASS:N", "report_year:O", "mapping_status:N", alt.Tooltip("rows:Q", format=",", title="Rows"), alt.Tooltip("share:Q", format=".2%", title="Row share")],
                )
                .properties(width=700, height=260, title="Report A row mapping coverage by edition")
            )


        def mapping_model_year_chart(vehicle_class: str) -> alt.Chart:
            rows = mapping_model_year_coverage.loc[
                mapping_model_year_coverage["vehicle_class"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "model_year:O",
                        title="Model year",
                        sort="ascending",
                        axis=alt.Axis(labelAngle=0),
                    ),
                    xOffset=alt.XOffset("report_year:O", sort=mapping_edition_order),
                    y=alt.Y(
                        "mapping_coverage:Q",
                        title="Fit-active stock mapped",
                        axis=alt.Axis(format="%"),
                        scale=alt.Scale(domain=[0, 1]),
                    ),
                    color=alt.Color(
                        "report_year:O",
                        title="Report edition",
                        sort=mapping_edition_order,
                        scale=alt.Scale(scheme="viridis"),
                    ),
                    tooltip=[
                        "vehicle_class:N",
                        "report_year:O",
                        "model_year:O",
                        alt.Tooltip(
                            "mapping_coverage:Q", format=".2%", title="Mapped"
                        ),
                        alt.Tooltip(
                            "mapped_fit_active:Q", format=",", title="Mapped stock"
                        ),
                        alt.Tooltip(
                            "total_fit_active:Q", format=",", title="Total stock"
                        ),
                    ],
                )
                .properties(width=850, height=280, title=vehicle_class.title())
            )


        catalog_families = evidence["vehicle_class_evidence"][
            ["canonical_make", "canonical_model", "nlr_atb_class"]
        ].drop_duplicates()
        crosswalk_families = (
            mapping_config.loc[mapping_config["entry_type"].eq("mto_crosswalk"), ["canonical_make", "canonical_model", "nlr_atb_class"]]
            .drop_duplicates()
            .assign(used_by_crosswalk=True)
        )
        family_use = catalog_families.merge(
            crosswalk_families,
            on=["canonical_make", "canonical_model", "nlr_atb_class"],
            how="left",
        )
        family_use["family_status"] = family_use["used_by_crosswalk"].fillna(False).map({True: "Matched to an MTO make-model key", False: "Unused normalized evidence"})
        family_use_summary = (
            family_use.groupby(["nlr_atb_class", "family_status"], as_index=False)
            .size()
            .rename(columns={"size": "normalized_family_count"})
        )
        family_use_summary["share"] = family_use_summary["normalized_family_count"] / family_use_summary.groupby("nlr_atb_class")["normalized_family_count"].transform("sum")
        family_use_chart = (
            alt.Chart(family_use_summary)
            .mark_bar()
            .encode(
                x=alt.X("share:Q", title="Share of canonical make-model families", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                y=alt.Y("nlr_atb_class:N", title=None, sort=NLR_ORDER, axis=alt.Axis(labelLimit=150)),
                color=alt.Color("family_status:N", title="Family use", scale=alt.Scale(domain=["Matched to an MTO make-model key", "Unused normalized evidence"], range=["#457b9d", "#d9d9d9"])),
                order=alt.Order("family_status:N", sort="descending"),
                tooltip=["nlr_atb_class:N", "family_status:N", alt.Tooltip("normalized_family_count:Q", format=","), alt.Tooltip("share:Q", format=".2%")],
            )
            .properties(width=700, height=190, title="Canonical make-model evidence families by NLR ATB class")
        )

        passenger_stock = coverage.loc[coverage["vehicle_class"].eq("PASSENGER") & coverage["measure"].eq("fit_active_stock")].iloc[0]
        commercial_stock = coverage.loc[coverage["vehicle_class"].eq("COMMERCIAL") & coverage["measure"].eq("fit_active_stock")].iloc[0]
        crosswalk_count = int(mapping_config["entry_type"].eq("mto_crosswalk").sum())
        used_family_count = int(family_use["used_by_crosswalk"].fillna(False).sum())
        mapping_progress_output = mo.vstack([
            mo.md("""
            This measures the usable scale of the reviewed map and the
            evidence breadth behind it. A Report A *row* is one make/model/model-year
            cohort; fit-active stock is the vehicle count carried by those rows.
            """),
            mo.hstack([
                mo.stat(f"{passenger_stock['mapped_ldv_share']:.2%}", "Passenger LDV fit-active coverage", f"{int(passenger_stock['mapped_ldv']):,} LDV; {int(passenger_stock['mapped_non_ldv']):,} non-LDV", bordered=True),
                mo.stat(f"{commercial_stock['mapped_ldv_share']:.2%}", "Commercial LDV fit-active coverage", f"{int(commercial_stock['mapped_ldv']):,} LDV; {int(commercial_stock['mapped_non_ldv']):,} non-LDV", bordered=True),
                mo.stat(f"{used_family_count:,}", "Canonical families matched", f"Across {crosswalk_count:,} accepted MTO ranges", bordered=True),
            ], widths="equal"),
            mo.ui.tabs({
                "Passenger row coverage": chart_ui(edition_row_coverage_chart("PASSENGER")),
                "Commercial row coverage": chart_ui(edition_row_coverage_chart("COMMERCIAL")),
            }),

            mo.md("""
            The row charts partition every edition into LDV-mapped, mapped non-LDV,
            the four largest
            unresolved reasons by row count, and an `Other` remainder. These are row
            shares rather than stock shares; the reasons and their evidence are explained
            in detail immediately below. Non-LDV stock is identified but excluded from
            LDV weights and survival pooling.
            """),

            mo.md("""
            A reviewed MTO crosswalk is an accepted MTO make/model code, not a source-row
            count: several MTO keys can resolve to one normalized family. This chart uses
            the unified NRCan and FuelEconomy.gov family catalogue, deduplicates by
            canonical make, canonical model, and NLR ATB class, and then asks whether any
            accepted MTO key points to each family.
            """),
            chart_ui(family_use_chart),

            mo.md("""
            #### Fit-active mapping coverage by model year and edition
            The model-year bars reconcile mapped `FIT_ACTIVE` cohort snapshots against
            raw `FIT_ACTIVE` totals for every fetched edition. Grouped bars retain the
            edition dimension instead of averaging coverage across repeated snapshots;
            only model years from 2000 through each report edition are included. The row
            charts apply the reviewed make/model keys directly to every Report A edition.
            """),
            mo.vstack([
                chart_ui(mapping_model_year_chart("PASSENGER")),
                chart_ui(mapping_model_year_chart("COMMERCIAL")),
            ]),

            mo.md("""
            **Unused normalized evidence** means the canonical family exists in the
            combined evidence catalogue but no reviewed MTO make/model code currently
            points to it; it does not mean its source rows are unused elsewhere.
            Model variants such as RAV4 AWD, Hybrid, and trim labels are collapsed into
            their canonical Ratings family.
            """),
            mo.ui.tabs({
                "Coverage": mo.ui.table(coverage, selection=None, pagination=False, page_size=10, format_mapping={"mapped_ldv_share": "{:.2%}", "mapped_non_ldv_share": "{:.2%}", "unmapped_share": "{:.2%}", "out_of_scope_share": "{:.2%}"}),
                "Ratings-family use": mo.ui.table(family_use_summary.sort_values(["nlr_atb_class", "share"], ascending=[True, False]), selection=None, pagination=False, page_size=10, format_mapping={"share": "{:.2%}"}),
                "Edition row coverage": mo.ui.table(mapping_edition_row_parts.sort_values(["VEHICLE_CLASS", "report_year", "status_order"]), selection=None, pagination=True, page_size=10, format_mapping={"share": "{:.2%}"}),
                "Model-year coverage": mo.ui.table(mapping_model_year_coverage.sort_values(["vehicle_class", "report_year", "model_year"]), selection=None, pagination=True, page_size=10, format_mapping={"mapping_coverage": "{:.2%}"}),
            }),
        ])
        return mapping_progress_output

    _build_view()
    return


@app.cell(hide_code=True)
def unmapped_reasons_heading(mo):
    mo.md("""
    ### Why stock remains unmapped
    """)
    return


@app.cell
def unmapped_reasons(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        unmapped_reason_summary = evidence["unresolved_reason_summary"].copy()
        for _column in ["unresolved_rows", "unresolved_fit_active_stock", "share_of_unresolved_rows", "share_of_unresolved_stock", "share_of_total_rows", "share_of_total_stock"]:
            unmapped_reason_summary[_column] = pd.to_numeric(unmapped_reason_summary[_column], errors="coerce")
        reason_labels = {
            "weak_model_label_agreement": "Weak model-label agreement",
            "ambiguous_top_candidate": "Ambiguous best candidate",
            "no_normalized_make_agreement": "No normalized make agreement",
            "suppressed_or_unknown_code": "Suppressed or unknown source code",
            "no_model_label_candidate": "No model-family candidate",
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Strong label candidate, crosswalk not accepted",
        }
        unmapped_reason_summary["reason_label"] = unmapped_reason_summary["unresolved_reason"].map(reason_labels).fillna(unmapped_reason_summary["unresolved_reason"])
        reason_order = list(reason_labels.values())
        reason_colors = ["#457b9d", "#e76f51", "#f4a261", "#8d99ae", "#90be6d", "#577590", "#9b5de5"]
        unmapped_reason_summary["reason_order"] = unmapped_reason_summary[
            "reason_label"
        ].map({reason: index for index, reason in enumerate(reason_order)})
        unmapped_reason_keys = (
            evidence["unresolved_reason_detail"][
                ["VEHICLE_CLASS", "MAKE", "MODEL", "unresolved_reason"]
            ]
            .drop_duplicates()
            .groupby(["VEHICLE_CLASS", "MAKE", "MODEL"], as_index=False)[
                "unresolved_reason"
            ]
            .first()
        )
        unmapped_mapping_keys = (
            evidence["mapping_config"].loc[
                evidence["mapping_config"]["entry_type"].eq("mto_crosswalk"),
                ["mto_make_code", "mto_model_code"],
            ]
            .drop_duplicates()
            .assign(mapping_accepted=True)
        )
        unmapped_edition_source = evidence["status_long"].copy()
        for _column in ["report_year", "native_count"]:
            unmapped_edition_source[_column] = pd.to_numeric(
                unmapped_edition_source[_column], errors="coerce"
            )
        unmapped_edition_source = unmapped_edition_source.loc[
            unmapped_edition_source["stock_status"].eq("FIT_ACTIVE")
            & unmapped_edition_source["VEHICLE_CLASS"].isin(
                ["PASSENGER", "COMMERCIAL"]
            )
        ].merge(
            unmapped_mapping_keys,
            left_on=["MAKE", "MODEL"],
            right_on=["mto_make_code", "mto_model_code"],
            how="left",
            validate="many_to_one",
        )
        unmapped_edition_source = unmapped_edition_source.loc[
            ~unmapped_edition_source["mapping_accepted"].fillna(False).astype(bool)
        ].merge(
            unmapped_reason_keys,
            on=["VEHICLE_CLASS", "MAKE", "MODEL"],
            how="left",
            validate="many_to_one",
        )
        unmapped_edition_source["unresolved_reason"] = unmapped_edition_source[
            "unresolved_reason"
        ].fillna("not_present_in_latest_snapshot")
        unmapped_edition_reason_summary = (
            unmapped_edition_source.groupby(
                ["VEHICLE_CLASS", "report_year", "unresolved_reason"], as_index=False
            )["native_count"]
            .sum()
            .rename(columns={"native_count": "unresolved_fit_active_stock"})
        )
        unmapped_edition_reason_summary["reason_label"] = (
            unmapped_edition_reason_summary["unresolved_reason"]
            .map(reason_labels)
            .fillna(unmapped_edition_reason_summary["unresolved_reason"])
        )
        unmapped_edition_reason_summary["reason_order"] = (
            unmapped_edition_reason_summary["reason_label"].map(
                {reason: index for index, reason in enumerate(reason_order)}
            )
        )
        unmapped_edition_reason_summary["share_of_unresolved_stock"] = (
            unmapped_edition_reason_summary["unresolved_fit_active_stock"]
            / unmapped_edition_reason_summary.groupby(
                ["VEHICLE_CLASS", "report_year"]
            )["unresolved_fit_active_stock"].transform("sum")
        )
        strong_candidate_stock = int(
            unmapped_reason_summary.loc[
                unmapped_reason_summary["unresolved_reason"].eq(
                    "unreviewed_high_confidence_candidate"
                ),
                "unresolved_fit_active_stock",
            ].sum()
        )


        def unmapped_share_chart(vehicle_class: str) -> alt.Chart:
            rows = unmapped_edition_reason_summary.loc[
                unmapped_edition_reason_summary["VEHICLE_CLASS"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_bar()
                .encode(
                    x=alt.X("share_of_unresolved_stock:Q", title="Share of category's unmapped fit-active stock", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    y=alt.Y("report_year:O", title="Report edition", sort="descending", axis=alt.Axis(labelAngle=0)),
                    color=alt.Color(
                        "reason_label:N",
                        title="Reason",
                        scale=alt.Scale(domain=reason_order, range=reason_colors),
                    ),
                    order=alt.Order("reason_order:Q"),
                    tooltip=["VEHICLE_CLASS:N", "report_year:O", alt.Tooltip("reason_label:N", title="Reason"), alt.Tooltip("unresolved_fit_active_stock:Q", format=",", title="Unmapped vehicles"), alt.Tooltip("share_of_unresolved_stock:Q", format=".2%", title="Share of edition's unmapped stock")],
                )
                .properties(width=700, height=260, title=f"{vehicle_class.title()} unresolved stock by reason and edition")
            )


        unmapped_reasons_output = mo.vstack([
            mo.md(f"""
            Each horizontal bar partitions one category and edition's
            unmapped fit-active stock, so every bar sums to 100%. Reason labels are
            assigned from the latest candidate pass by MTO make/model key and projected
            back to earlier editions. A historical key absent from the 2025 snapshot is
            kept visible as **Not present in latest snapshot** rather than assigned a
            speculative current-fleet reason. Absolute stock remains in the tooltip and
            latest table.

            - **Weak model-label agreement:** make evidence exists, but only substring
              or general similarity supports the model.
            - **Ambiguous best candidate:** equally strong families or class
              interpretations remain.
            - **No normalized make agreement:** the MTO make code has no configured
              alias or normalized make-prefix match, so model scoring cannot begin.
            - **Suppressed or unknown source code:** make or model is blank, masked, or
              explicitly unknown.
            - **No model-family candidate:** the make normalized successfully, but no
              canonical model family was generated for the MTO label.
            - **Not present in latest snapshot:** the historical key does not occur in
              the latest fleet snapshot. This lowers its priority for current fleet
              composition and age weights, but it does **not** exclude an otherwise
              eligible historical key from the cohort-transition survival estimator.
            - **Strong label candidate, crosswalk not accepted:** this is not a runtime
              error or a failed model-year test. These {strong_candidate_stock:,} vehicles have an exact or
              normalized-prefix Ratings label candidate, but mapping acceptance is
              now carry zero FIT_ACTIVE stock after the approved strong candidates were
              promoted. Only nonzero unresolved evidence affects the charts below.
            """),
            mo.vstack([
                chart_ui(unmapped_share_chart("PASSENGER")),
                chart_ui(unmapped_share_chart("COMMERCIAL")),
            ]),
            mo.ui.table(unmapped_reason_summary[["VEHICLE_CLASS", "reason_label", "unresolved_fit_active_stock", "share_of_unresolved_stock", "share_of_total_stock", "unresolved_rows", "share_of_total_rows"]], selection=None, pagination=False, page_size=10, format_mapping={"share_of_unresolved_stock": "{:.2%}", "share_of_total_stock": "{:.2%}", "share_of_total_rows": "{:.2%}"}, label="Relative and absolute unresolved-stock attribution"),
        ])
        return unmapped_reasons_output, reason_labels

    _unmapped_reasons_output, reason_labels = _build_view()
    _unmapped_reasons_output
    return (reason_labels,)


@app.cell(hide_code=True)
def highest_unresolved_heading(mo):
    mo.md("""
    ### Highest-stock unresolved keys
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd, reason_labels):
    def _build_view():
        unresolved_detail = evidence["unresolved_reason_detail"].copy()
        unresolved_detail["FIT_ACTIVE"] = pd.to_numeric(unresolved_detail["FIT_ACTIVE"], errors="coerce")
        unresolved_detail["MODEL_YEAR"] = pd.to_numeric(unresolved_detail["MODEL_YEAR"], errors="coerce").astype("Int64")
        unresolved_detail["candidate_model_similarity"] = pd.to_numeric(unresolved_detail["candidate_model_similarity"], errors="coerce")
        historical_status = evidence["status_long"].copy()
        for column in ["report_year", "MODEL_YEAR", "native_count"]:
            historical_status[column] = pd.to_numeric(
                historical_status[column], errors="coerce"
            )
        historical_status = historical_status.loc[
            historical_status["stock_status"].eq("FIT_ACTIVE")
            & historical_status["VEHICLE_CLASS"].isin(["PASSENGER", "COMMERCIAL"])
            & historical_status["native_count"].gt(0)
        ].copy()
        latest_snapshot_year = int(historical_status["report_year"].max())
        current_key_set = set(
            historical_status.loc[
                historical_status["report_year"].eq(latest_snapshot_year),
                ["VEHICLE_CLASS", "MAKE", "MODEL"],
            ].apply(tuple, axis=1)
        )
        accepted_key_set = set(
            evidence["mapping_config"].loc[
                evidence["mapping_config"]["entry_type"].eq("mto_crosswalk"),
                ["mto_make_code", "mto_model_code"],
            ].apply(tuple, axis=1)
        )
        expired_rows = historical_status.loc[
            ~historical_status[["VEHICLE_CLASS", "MAKE", "MODEL"]]
            .apply(tuple, axis=1)
            .isin(current_key_set)
            & ~historical_status[["MAKE", "MODEL"]]
            .apply(tuple, axis=1)
            .isin(accepted_key_set)
        ].rename(columns={"native_count": "FIT_ACTIVE"})
        expired_rows["unresolved_reason"] = "not_present_in_latest_snapshot"
        expired_rows["unresolved_reason_detail"] = (
            "Historical MTO key is absent from the latest snapshot and has no accepted crosswalk."
        )
        for column in [
            "candidate_status",
            "candidate_canonical_make",
            "candidate_canonical_model",
            "candidate_nrcan_vehicle_class",
            "candidate_nlr_atb_class",
            "candidate_match_method",
            "candidate_model_similarity",
            "rating_model_year_from",
            "rating_model_year_to",
            "overlap_years",
            "rating_model_labels",
            "manual_canonical_make",
            "candidate_pass_agreement",
            "agreed_model_candidate",
            "highest_confidence_candidate",
            "vpic_second_pass_candidate",
        ]:
            expired_rows[column] = pd.NA
        unresolved_detail = pd.concat(
            [unresolved_detail, expired_rows[unresolved_detail.columns]],
            ignore_index=True,
        )
        unresolved_detail["mapping_key"] = unresolved_detail["VEHICLE_CLASS"].astype(str) + " / " + unresolved_detail["MAKE"].astype(str) + "/" + unresolved_detail["MODEL"].astype(str)
        top_reason_codes = (
            unresolved_detail.groupby("unresolved_reason", as_index=False)["FIT_ACTIVE"]
            .sum()
            .nlargest(3, "FIT_ACTIVE")["unresolved_reason"]
            .tolist()
        )
        no_make_agreement_code = "no_normalized_make_agreement"
        if no_make_agreement_code not in top_reason_codes:
            top_reason_codes.append(no_make_agreement_code)
        strong_candidate_code = "unreviewed_high_confidence_candidate"
        if (
            unresolved_detail.loc[
                unresolved_detail["unresolved_reason"].eq(strong_candidate_code),
                "FIT_ACTIVE",
            ].sum() > 0
            and strong_candidate_code not in top_reason_codes
        ):
            top_reason_codes.append(strong_candidate_code)
        expired_reason_code = "not_present_in_latest_snapshot"
        if expired_reason_code not in top_reason_codes:
            top_reason_codes.append(expired_reason_code)
        unresolved_model_year_domain = sorted(
            unresolved_detail["MODEL_YEAR"].dropna().astype(int).unique()
        )
        unresolved_model_year_legend = [
            year for year in unresolved_model_year_domain if year >= 1980 and year % 5 == 0
        ]


        def first_present(values: pd.Series):
            present = values.dropna()
            present = present.loc[present.astype(str).str.strip().ne("")]
            return present.iloc[0] if len(present) else pd.NA


        unresolved_tabs = {}
        for reason_code in top_reason_codes:
            reason_rows = unresolved_detail.loc[unresolved_detail["unresolved_reason"].eq(reason_code)].copy()
            key_columns = ["VEHICLE_CLASS", "MAKE", "MODEL", "mapping_key", "unresolved_reason", "unresolved_reason_detail"]
            key_totals = (
                reason_rows.groupby(key_columns, as_index=False, dropna=False)
                .agg(
                    fit_active_stock=("FIT_ACTIVE", "sum"),
                    model_year_from=("MODEL_YEAR", "min"),
                    model_year_to=("MODEL_YEAR", "max"),
                    observed_model_years=("MODEL_YEAR", "nunique"),
                    candidate_canonical_make=("candidate_canonical_make", first_present),
                    candidate_canonical_model=("candidate_canonical_model", first_present),
                    candidate_nrcan_vehicle_class=("candidate_nrcan_vehicle_class", first_present),
                    candidate_match_method=("candidate_match_method", first_present),
                    candidate_model_similarity=("candidate_model_similarity", "max"),
                    rating_model_year_from=("rating_model_year_from", first_present),
                    rating_model_year_to=("rating_model_year_to", first_present),
                    overlap_years=("overlap_years", first_present),
                    rating_model_labels=("rating_model_labels", first_present),
                )
                .nlargest(20, "fit_active_stock")
            )
            year_rows = (
                reason_rows.groupby(["mapping_key", "MODEL_YEAR"], as_index=False)["FIT_ACTIVE"]
                .sum()
                .merge(key_totals[["mapping_key", "fit_active_stock"]], on="mapping_key", how="inner")
            )
            family_order = key_totals.sort_values("fit_active_stock", ascending=False)["mapping_key"].tolist()
            stock_axis_title = (
                "Historical fit-active exposure across editions"
                if reason_code == expired_reason_code
                else "Unresolved fit-active stock"
            )
            reason_chart = (
                alt.Chart(year_rows)
                .mark_bar()
                .encode(
                    x=alt.X("FIT_ACTIVE:Q", title=stock_axis_title, axis=alt.Axis(format="~s")),
                    y=alt.Y("mapping_key:N", title=None, sort=family_order, axis=alt.Axis(labelLimit=320)),
                    color=alt.Color(
                        "MODEL_YEAR:O",
                        title="Model year",
                        scale=alt.Scale(
                            domain=unresolved_model_year_domain, scheme="sinebow"
                        ),
                        legend=alt.Legend(values=unresolved_model_year_legend),
                    ),
                    order=alt.Order("MODEL_YEAR:O"),
                    tooltip=[alt.Tooltip("mapping_key:N", title="Unresolved key"), alt.Tooltip("MODEL_YEAR:O", title="Model year"), alt.Tooltip("FIT_ACTIVE:Q", format=",", title="Fit-active in year"), alt.Tooltip("fit_active_stock:Q", format=",", title="Key total")],
                )
                .properties(width=650, height=460, title=reason_labels.get(reason_code, reason_code))
            )
            if reason_code in {
                "no_normalized_make_agreement",
                "not_present_in_latest_snapshot",
            }:
                visible_columns = ["VEHICLE_CLASS", "MAKE", "MODEL", "fit_active_stock", "model_year_from", "model_year_to", "observed_model_years", "unresolved_reason_detail"]
            else:
                visible_columns = ["VEHICLE_CLASS", "MAKE", "MODEL", "fit_active_stock", "model_year_from", "model_year_to", "observed_model_years", "candidate_canonical_make", "candidate_canonical_model", "candidate_nrcan_vehicle_class", "candidate_match_method", "candidate_model_similarity", "rating_model_year_from", "rating_model_year_to", "overlap_years", "rating_model_labels"]
            unresolved_tabs[reason_labels.get(reason_code, reason_code)] = mo.vstack([
                chart_ui(reason_chart),
                mo.ui.table(key_totals, selection=None, pagination=True, page_size=10, visible_columns=visible_columns, freeze_columns_left=["VEHICLE_CLASS", "MAKE", "MODEL"], wrapped_columns=[column for column in ["unresolved_reason_detail", "rating_model_labels"] if column in visible_columns], label=f"Top 20 unresolved make-model keys: {reason_labels.get(reason_code, reason_code)}"),
            ])

        highest_unresolved_output = mo.vstack([
            mo.md("""
            **Insight:** one bar now represents one Passenger/Commercial MTO make-model
            key, with model-year stock shown as colored segments. This exposes twenty
            high-impact keys per reason without repeating one table row for every year.
            For `no normalized make agreement`, candidate columns are omitted because
            no Ratings candidate can exist until make agreement succeeds; showing those
            structurally missing values as NaNs was misleading.

            `candidate_canonical_make` and `candidate_canonical_model` identify the
            highest-ranked normalized Ratings family; `candidate_nrcan_vehicle_class`
            is that family's source body-size class. `candidate_match_method` records
            the implemented label test: `exact_normalized` = identical normalized
            labels, `normalized_prefix` = either label starts with the other,
            `normalized_substring` = the MTO label occurs within the family label, and
            `string_similarity` = the fallback sequence comparison.
            `candidate_model_similarity` is the resulting 0-1 score (1.00 exact, 0.95
            prefix, 0.90 substring, otherwise the sequence-similarity ratio). The
            approved bootstrap promotes an unambiguous rank-one candidate at 0.70 or
            above. Ratings year bounds and `overlap_years` remain provenance and
            diagnostics; model year does not change the candidate similarity rank.

            The strong-candidate review gate contains no FIT_ACTIVE stock after all
            unambiguous exact/prefix candidates and the broader 0.70+ candidate set are
            promoted. The **Not present in latest snapshot** tab
            instead shows historical exposure summed across observed editions; those
            keys are lower priority for latest-fleet composition, but latest presence is
            never used to exclude a mapped historical key from survival estimation.
            Chart data retain every observed model year, while the color legend labels
            only five-year intervals from 1980 onward to keep it compact.
            """),
            mo.ui.tabs(unresolved_tabs),
        ])
        return highest_unresolved_output

    _build_view()
    return


@app.cell(hide_code=True)
def fleet_representation_section(mo):
    mo.md("""
    ## Mapped fleet representation
    """)
    return


@app.cell(hide_code=True)
def mapped_composition_heading(mo):
    mo.md("""
    ### Mapped fleet composition
    """)
    return


@app.cell
def _(NRCAN_CLASS_ORDER, alt, chart_ui, evidence, mo, pd):
    def _build_view():
        composition_status_rows = evidence["status_long"].copy()
        for _column in ["report_year", "native_count"]:
            composition_status_rows[_column] = pd.to_numeric(
                composition_status_rows[_column], errors="coerce"
            )
        composition_crosswalk = (
            evidence["mapping_config"].loc[
                evidence["mapping_config"]["entry_type"].eq("mto_crosswalk"),
                [
                    "mto_make_code",
                    "mto_model_code",
                    "model_year_from",
                    "model_year_to",
                    "nrcan_vehicle_class",
                    "nlr_atb_class",
                    "nrcan_ceud_class",
                ],
            ]
        )
        for column in ["model_year_from", "model_year_to"]:
            composition_crosswalk[column] = pd.to_numeric(
                composition_crosswalk[column], errors="coerce"
            ).astype(int)
        composition_crosswalk = composition_crosswalk.assign(
            MODEL_YEAR=composition_crosswalk.apply(
                lambda row: list(
                    range(int(row["model_year_from"]), int(row["model_year_to"]) + 1)
                ),
                axis=1,
            )
        ).explode("MODEL_YEAR")
        composition_crosswalk["MODEL_YEAR"] = composition_crosswalk[
            "MODEL_YEAR"
        ].astype(int)
        composition_crosswalk = composition_crosswalk.drop_duplicates(
            ["mto_make_code", "mto_model_code", "MODEL_YEAR"]
        )
        edition_ratings_composition = (
            composition_status_rows.loc[
                composition_status_rows["stock_status"].eq("FIT_ACTIVE")
                & composition_status_rows["VEHICLE_CLASS"].isin(
                    ["PASSENGER", "COMMERCIAL"]
                )
            ]
            .merge(
                composition_crosswalk,
                left_on=["MAKE", "MODEL", "MODEL_YEAR"],
                right_on=["mto_make_code", "mto_model_code", "MODEL_YEAR"],
                how="inner",
                validate="many_to_one",
            )
            .groupby(
                ["report_year", "nrcan_ceud_class", "nrcan_vehicle_class"],
                as_index=False,
                dropna=False,
            )["native_count"]
            .sum()
            .rename(columns={"native_count": "fit_active_stock"})
        )
        edition_ratings_composition["within_edition_ceud_share"] = (
            edition_ratings_composition["fit_active_stock"]
            / edition_ratings_composition.groupby(
                ["report_year", "nrcan_ceud_class"]
            )["fit_active_stock"].transform("sum")
        )

        def edition_composition_chart(ceud_class: str) -> alt.Chart:
            rows = edition_ratings_composition.loc[
                edition_ratings_composition["nrcan_ceud_class"].eq(ceud_class)
            ].copy()
            observed_classes = set(rows["nrcan_vehicle_class"].dropna().astype(str))
            class_order = [
                vehicle_class
                for vehicle_class in NRCAN_CLASS_ORDER[ceud_class]
                if vehicle_class in observed_classes
            ]
            class_order.extend(sorted(observed_classes - set(class_order)))
            rows["stack_order"] = rows["nrcan_vehicle_class"].map(
                {vehicle_class: order for order, vehicle_class in enumerate(class_order)}
            )
            return (
                alt.Chart(rows)
                .mark_bar(cornerRadiusEnd=3)
                .encode(
                    x=alt.X("within_edition_ceud_share:Q", title="Share within mapped fit-active stock", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    y=alt.Y("report_year:O", title="Report edition", sort="descending", axis=alt.Axis(labelAngle=0)),
                    color=alt.Color(
                        "nrcan_vehicle_class:N",
                        title="NRCan Ratings class",
                        sort=class_order,
                        scale=alt.Scale(domain=class_order, scheme="tableau20"),
                    ),
                    order=alt.Order("stack_order:Q", sort="ascending"),
                    tooltip=["report_year:O", alt.Tooltip("nrcan_vehicle_class:N", title="NRCan Ratings class"), alt.Tooltip("fit_active_stock:Q", format=",", title="Mapped fit-active stock"), alt.Tooltip("within_edition_ceud_share:Q", format=".2%", title="Within-edition share")],
                )
                .properties(width=760, height=280, title=f"{ceud_class} composition across Report A editions")
            )

        latest_mapped = evidence["mapped_fleet"].loc[
            evidence["mapped_fleet"]["mapping_accepted"]
            .astype(str)
            .str.lower()
            .eq("true")
        ].copy()
        for column in ["report_year", "MODEL_YEAR", "FIT_ACTIVE"]:
            latest_mapped[column] = pd.to_numeric(
                latest_mapped[column], errors="coerce"
            )
        latest_mapped_year = int(latest_mapped["report_year"].max())
        latest_eligible_vintage = latest_mapped_year - 1
        latest_vintage_nlr = (
            latest_mapped.loc[
                latest_mapped["report_year"].eq(latest_mapped_year)
                & latest_mapped["MODEL_YEAR"].between(
                    2000, latest_eligible_vintage
                )
                & latest_mapped["FIT_ACTIVE"].gt(0)
            ]
            .groupby(["MODEL_YEAR", "nlr_atb_class"], as_index=False)["FIT_ACTIVE"]
            .sum()
            .rename(
                columns={
                    "MODEL_YEAR": "model_year",
                    "FIT_ACTIVE": "fit_active_stock",
                }
            )
        )
        latest_vintage_total = float(latest_vintage_nlr["fit_active_stock"].sum())
        latest_vintage_nlr["share_of_2000_plus_mapped_stock"] = (
            latest_vintage_nlr["fit_active_stock"] / latest_vintage_total
        )
        latest_nlr_order = (
            latest_vintage_nlr.groupby("nlr_atb_class", as_index=False)[
                "fit_active_stock"
            ]
            .sum()
            .sort_values("fit_active_stock", ascending=False, kind="stable")[
                "nlr_atb_class"
            ]
            .tolist()
        )
        latest_vintage_area = (
            alt.Chart(latest_vintage_nlr)
            .mark_area(interpolate="monotone", opacity=0.85)
            .encode(
                x=alt.X("model_year:Q", title="Model-year vintage", axis=alt.Axis(format="d", tickMinStep=1)),
                y=alt.Y("share_of_2000_plus_mapped_stock:Q", title="Share of mapped 2000+ FIT_ACTIVE stock", stack="zero", axis=alt.Axis(format="%")),
                color=alt.Color("nlr_atb_class:N", title="NLR ATB class", sort=latest_nlr_order),
                tooltip=["model_year:O", "nlr_atb_class:N", alt.Tooltip("fit_active_stock:Q", format=","), alt.Tooltip("share_of_2000_plus_mapped_stock:Q", format=".2%")],
            )
            .properties(width=760, height=300, title=f"{latest_mapped_year} mapped fleet: 2000-{latest_eligible_vintage} vintage composition")
        )


        mapped_composition_output = mo.vstack([
            mo.md("""
            **Insight:** each horizontal bar is one Report A edition and partitions the
            accepted fit-active fleet into source-native NRCan Ratings classes. Car and
            Light Truck are separated because their class vocabularies differ. These are
            relative shares of mapped stock, so a changing segment can reflect both fleet
            evolution and changing mapping coverage. NRCan class order is fixed from
            smaller to larger size classes within each source body-type family, so both
            stack position and color remain stable across editions. Two-seaters and vans
            follow the size-ordered classes because they are separate source categories.
            """),
            chart_ui(edition_composition_chart("Car")),
            chart_ui(edition_composition_chart("Light Truck")),
            mo.md("""
            The area chart below isolates the latest snapshot and model-year vintages
            from 2000 onward. Each colored area is an NLR ATB class contribution divided
            by all mapped 2000+ FIT_ACTIVE stock; the class-vintage contributions sum to
            100% across the displayed data.
            """),
            chart_ui(latest_vintage_area),
            mo.ui.table(edition_ratings_composition.sort_values(["nrcan_ceud_class", "report_year", "within_edition_ceud_share"], ascending=[True, True, False]), selection=None, pagination=True, page_size=10, format_mapping={"within_edition_ceud_share": "{:.2%}"}),
        ])
        return mapped_composition_output

    _build_view()
    return


@app.cell(hide_code=True)
def top_mapped_heading(mo):
    mo.md("""
    ### Top mapped observations
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        top_source = evidence["mapped_fleet"].loc[evidence["mapped_fleet"]["mapping_accepted"].astype(str).str.lower().eq("true")].copy()
        top_source["FIT_ACTIVE"] = pd.to_numeric(top_source["FIT_ACTIVE"], errors="coerce")
        top_source["MODEL_YEAR"] = pd.to_numeric(top_source["MODEL_YEAR"], errors="coerce").astype("Int64")
        top_source["report_year"] = pd.to_numeric(top_source["report_year"], errors="coerce").astype("Int64")
        top_source = top_source.loc[
            top_source["MODEL_YEAR"].between(1990, top_source["report_year"])
            & top_source["FIT_ACTIVE"].gt(0)
        ].copy()
        top_source["normalized_make_model"] = top_source["canonical_make"].astype(str) + " " + top_source["canonical_model"].astype(str)
        top_source["mto_key"] = (
            top_source["MAKE"].astype(str) + "/" + top_source["MODEL"].astype(str)
        )
        top_by_year = (
            top_source.groupby(["nrcan_ceud_class", "normalized_make_model", "MODEL_YEAR", "nrcan_vehicle_class", "nlr_atb_class"], as_index=False, dropna=False)["FIT_ACTIVE"]
            .sum()
            .rename(columns={"MODEL_YEAR": "model_year", "FIT_ACTIVE": "fit_active_stock"})
        )
        top_family_totals = (
            top_by_year.groupby(["nrcan_ceud_class", "normalized_make_model"], as_index=False)["fit_active_stock"]
            .sum()
            .rename(columns={"fit_active_stock": "family_fit_active_stock"})
        )
        top_family_keys = (
            top_source.groupby(
                ["nrcan_ceud_class", "normalized_make_model"], as_index=False
            )["mto_key"]
            .agg(
                lambda values: "["
                + ", ".join(
                    sorted(set(map(str, values)), key=lambda value: value.casefold())
                )
                + "]"
            )
            .rename(columns={"mto_key": "matched_mto_keys"})
        )
        top_family_totals = top_family_totals.merge(
            top_family_keys,
            on=["nrcan_ceud_class", "normalized_make_model"],
            how="left",
            validate="one_to_one",
        )
        top_family_totals["family_rank"] = top_family_totals.groupby("nrcan_ceud_class")["family_fit_active_stock"].rank(method="first", ascending=False)
        top_20_families = top_family_totals.loc[top_family_totals["family_rank"].le(30)].copy()
        top_heatmap_data = top_by_year.merge(
            top_20_families[
                [
                    "nrcan_ceud_class",
                    "normalized_make_model",
                    "family_fit_active_stock",
                    "family_rank",
                    "matched_mto_keys",
                ]
            ],
            on=["nrcan_ceud_class", "normalized_make_model"],
            how="inner",
        )
        top_crosswalk_intervals = evidence["mapping_config"].loc[
            evidence["mapping_config"]["entry_type"].eq("mto_crosswalk")
        ].copy()
        top_crosswalk_intervals["normalized_make_model"] = (
            top_crosswalk_intervals["canonical_make"].astype(str)
            + " "
            + top_crosswalk_intervals["canonical_model"].astype(str)
        )
        top_crosswalk_intervals = top_crosswalk_intervals.merge(
            top_20_families[
                ["nrcan_ceud_class", "normalized_make_model", "family_rank"]
            ],
            on=["nrcan_ceud_class", "normalized_make_model"],
            how="inner",
        )
        top_crosswalk_interval_table = top_crosswalk_intervals.loc[
            :,
            [
                "nrcan_ceud_class",
                "normalized_make_model",
                "mto_make_code",
                "mto_model_code",
                "model_year_from",
                "model_year_to",
                "nrcan_vehicle_class",
                "nlr_atb_class",
                "match_method",
            ],
        ].sort_values(
            [
                "nrcan_ceud_class",
                "normalized_make_model",
                "mto_make_code",
                "mto_model_code",
                "model_year_from",
            ],
            kind="stable",
        ).reset_index(drop=True)


        def top_family_heatmap(ceud_class: str) -> alt.Chart:
            rows = top_heatmap_data.loc[top_heatmap_data["nrcan_ceud_class"].eq(ceud_class)]
            return (
                alt.Chart(rows)
                .mark_rect(stroke="white", strokeWidth=0.5)
                .encode(
                    x=alt.X("model_year:O", 
                            title="Model year", 
                            sort="ascending", 
                            axis=alt.Axis(labelAngle=0, labelExpr="\"'\"+substring(datum.label, 2)"),
                    ),
                    y=alt.Y(
                        "normalized_make_model:N",
                        title=None,
                        sort=alt.EncodingSortField(
                            field="family_fit_active_stock",
                            op="max",
                            order="descending",
                        ),
                        axis=alt.Axis(labelLimit=260),
                    ),
                    color=alt.Color("fit_active_stock:Q", title="Fit-active stock", scale=alt.Scale(scheme="blues")),
                    tooltip=["normalized_make_model:N", alt.Tooltip("matched_mto_keys:N", title="Matched MTO keys"), "model_year:O", "nrcan_vehicle_class:N", "nlr_atb_class:N", alt.Tooltip("fit_active_stock:Q", format=",")],
                )
                .properties(width=900, height=550, title=ceud_class)
            )


        top_mapped_output = mo.vstack([
            mo.md("""
            **Insight:** each heat-map cell is the accepted fit-active stock for one
            normalized make-model and exact model year. Only model years from 1990
            through the latest report year are shown. Families are ranked once by their
            total observation count across those years, and the categorical y-axis is
            explicitly sorted in descending order by that total. Totals appear only in
            the family table and are not repeated as if they were annual observations.
            Matched abbreviated MTO make/model keys are retained as list-form tooltips and
            table columns; keeping them out of the categorical axis preserves heat-map
            alignment and readability when many keys resolve to one normalized family.
            """),
            mo.vstack([
                chart_ui(top_family_heatmap("Car")),
                chart_ui(top_family_heatmap("Light Truck")),
            ]),
            mo.md("""
            A blank model-year cell means no accepted FIT_ACTIVE observation exists for
            that normalized family in the latest Report A; it does not by itself mean a
            vintage was rejected. The cited examples are source-data absences: `TOYT/COR`
            is observed in 2003, 2008, and 2020-2026, while Elantra observations are split
            between `HYUN/ELA` and `HYUN/ESM`. The rebuilt crosswalk now covers Corolla
            continuously from 2003-2026 and Elantra continuously within its reviewed
            Compact/Midsize class intervals, so future observations inside those ranges
            will map without requiring an exact Ratings model year.
            """),
            mo.ui.tabs({
                "Family totals": mo.ui.table(top_20_families.sort_values(["nrcan_ceud_class", "family_rank"]), selection=None, pagination=True, page_size=10, freeze_columns_left=["nrcan_ceud_class", "normalized_make_model"]),
                "Exact model-year stock": mo.ui.table(top_heatmap_data.sort_values(["nrcan_ceud_class", "family_rank", "model_year"]), selection=None, pagination=True, page_size=10, visible_columns=["nrcan_ceud_class", "normalized_make_model", "matched_mto_keys", "model_year", "fit_active_stock", "nrcan_vehicle_class", "nlr_atb_class"], freeze_columns_left=["nrcan_ceud_class", "normalized_make_model"]),
                "Crosswalk intervals": mo.ui.table(top_crosswalk_interval_table, selection=None, pagination=True, page_size=10, label="Accepted crosswalk intervals for top mapped families"),
            }),
        ])
        return top_mapped_output

    _build_view()
    return


@app.cell(hide_code=True)
def least_mapped_heading(mo):
    mo.md("""
    ### Mapped observations by Q1-Q3 buckets
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        percentile_source = evidence["mapped_fleet"].loc[
            evidence["mapped_fleet"]["mapping_accepted"]
            .astype(str)
            .str.lower()
            .eq("true")
        ].copy()
        for _column in ["FIT_ACTIVE", "MODEL_YEAR", "report_year"]:
            percentile_source[_column] = pd.to_numeric(
                percentile_source[_column], errors="coerce"
            )
        percentile_source = percentile_source.loc[
            percentile_source["MODEL_YEAR"].between(
                1990, percentile_source["report_year"]
            )
            & percentile_source["FIT_ACTIVE"].gt(0)
        ].copy()
        percentile_source["normalized_make_model"] = (
            percentile_source["canonical_make"].astype(str)
            + " "
            + percentile_source["canonical_model"].astype(str)
        )
        percentile_source["mto_key"] = (
            percentile_source["MAKE"].astype(str)
            + "/"
            + percentile_source["MODEL"].astype(str)
        )
        percentile_by_year = (
            percentile_source.groupby(
                [
                    "nrcan_ceud_class",
                    "normalized_make_model",
                    "MODEL_YEAR",
                    "nrcan_vehicle_class",
                    "nlr_atb_class",
                ],
                as_index=False,
                dropna=False,
            )["FIT_ACTIVE"]
            .sum()
            .rename(columns={"MODEL_YEAR": "model_year", "FIT_ACTIVE": "fit_active_stock"})
        )
        percentile_family_totals = (
            percentile_by_year.groupby(
                ["nrcan_ceud_class", "normalized_make_model"], as_index=False
            )["fit_active_stock"]
            .sum()
            .rename(columns={"fit_active_stock": "family_fit_active_stock"})
        )
        percentile_family_keys = (
            percentile_source.groupby(
                ["nrcan_ceud_class", "normalized_make_model"], as_index=False
            )["mto_key"]
            .agg(
                lambda values: "["
                + ", ".join(
                    sorted(set(map(str, values)), key=lambda value: value.casefold())
                )
                + "]"
            )
            .rename(columns={"mto_key": "matched_mto_keys"})
        )
        percentile_family_totals = percentile_family_totals.merge(
            percentile_family_keys,
            on=["nrcan_ceud_class", "normalized_make_model"],
            how="left",
            validate="one_to_one",
        )
        percentile_family_totals["stock_percentile"] = percentile_family_totals.groupby(
            "nrcan_ceud_class"
        )["family_fit_active_stock"].rank(method="average", pct=True)
        percentile_bucket_order = [
            "Bottom 25%",
            "25th-50th percentile",
            "50th-75th percentile",
        ]
        percentile_family_totals["sample_bucket"] = pd.cut(
            percentile_family_totals["stock_percentile"],
            bins=[0, 0.25, 0.50, 0.75],
            labels=percentile_bucket_order,
            include_lowest=True,
        ).astype("string")
        percentile_samples = []
        for sample_bucket in percentile_bucket_order:
            for ceud_class in ["Car", "Light Truck"]:
                bucket_candidates = percentile_family_totals.loc[
                    percentile_family_totals["sample_bucket"].eq(sample_bucket)
                    & percentile_family_totals["nrcan_ceud_class"].eq(ceud_class)
                ]
                percentile_samples.append(
                    bucket_candidates.sample(n=min(10, len(bucket_candidates)))
                )
        percentile_sampled_families = pd.concat(
            percentile_samples, ignore_index=True
        )
        ceud_display_labels = {"Light Truck": "LD Truck"}
        percentile_sampled_families["display_family"] = (
            percentile_sampled_families["nrcan_ceud_class"]
            .replace(ceud_display_labels)
            + " | "
            + percentile_sampled_families["normalized_make_model"]
        )
        percentile_heatmap_data = percentile_by_year.merge(
            percentile_sampled_families[
                [
                    "nrcan_ceud_class",
                    "normalized_make_model",
                    "family_fit_active_stock",
                    "stock_percentile",
                    "sample_bucket",
                    "matched_mto_keys",
                    "display_family",
                ]
            ],
            on=["nrcan_ceud_class", "normalized_make_model"],
            how="inner",
        )

        def percentile_family_heatmap(sample_bucket: str) -> alt.Chart:
            rows = percentile_heatmap_data.loc[
                percentile_heatmap_data["sample_bucket"].eq(sample_bucket)
            ]
            return (
                alt.Chart(rows)
                .mark_rect(stroke="white", strokeWidth=0.5)
                .encode(
                    x=alt.X(
                        "model_year:O",
                        title="Model year",
                        sort="ascending",
                        axis=alt.Axis(labelAngle=0, labelExpr="\"'\"+substring(datum.label, 2)"),
                    ),
                    y=alt.Y(
                        "display_family:N",
                        title=None,
                        sort=alt.EncodingSortField(
                            field="family_fit_active_stock",
                            op="max",
                            order="descending",
                        ),
                        axis=alt.Axis(labelLimit=330),
                    ),
                    color=alt.Color(
                        "fit_active_stock:Q",
                        title="Fit-active stock",
                        scale=alt.Scale(scheme="oranges"),
                    ),
                    tooltip=[
                        "normalized_make_model:N",
                        alt.Tooltip("matched_mto_keys:N", title="Matched MTO keys"),
                        "nrcan_ceud_class:N",
                        "model_year:O",
                        "nrcan_vehicle_class:N",
                        "nlr_atb_class:N",
                        alt.Tooltip("fit_active_stock:Q", format=","),
                        alt.Tooltip(
                            "family_fit_active_stock:Q", format=",", title="Family total"
                        ),
                        alt.Tooltip(
                            "stock_percentile:Q", format=".1%", title="Within-class percentile"
                        ),
                    ],
                )
                .properties(width=900, height=450, title=sample_bucket)
            )

        least_mapped_output = mo.vstack(
            [
                mo.md("""
                **Insight:** each heat map draws ten random Car families and ten random
                Light Truck families from one within-class fit-active-stock percentile
                bucket. Rows are then ordered by descending family stock so the scale
                remains interpretable even though the sampled families change. Re-run
                this cell to reshuffle all three samples.

                Matched abbreviated MTO keys are included in list-form tooltips and tables.
                A second categorical axis would compress the model-year cells, so the
                validation labels remain available without changing the heat-map geometry.
                """),
                mo.vstack(
                    [
                        chart_ui(percentile_family_heatmap(bucket))
                        for bucket in percentile_bucket_order
                    ]
                ),
                mo.ui.tabs(
                    {
                        "Sampled family totals": mo.ui.table(
                            percentile_sampled_families.sort_values(
                                [
                                    "sample_bucket",
                                    "nrcan_ceud_class",
                                    "family_fit_active_stock",
                                ],
                                ascending=[True, True, False],
                            ),
                            selection=None,
                            pagination=True,
                            page_size=10,
                            freeze_columns_left=[
                                "nrcan_ceud_class",
                                "normalized_make_model",
                            ],
                        ),
                        "Exact model-year stock": mo.ui.table(
                            percentile_heatmap_data.sort_values(
                                [
                                    "sample_bucket",
                                    "nrcan_ceud_class",
                                    "family_fit_active_stock",
                                    "model_year",
                                ],
                                ascending=[True, True, False, True],
                            ),
                            selection=None,
                            pagination=True,
                            page_size=10,
                            visible_columns=[
                                "nrcan_ceud_class",
                                "normalized_make_model",
                                "matched_mto_keys",
                                "sample_bucket",
                                "stock_percentile",
                                "model_year",
                                "fit_active_stock",
                                "nrcan_vehicle_class",
                                "nlr_atb_class",
                            ],
                            freeze_columns_left=[
                                "nrcan_ceud_class",
                                "normalized_make_model",
                            ],
                        ),
                    }
                ),
            ]
        )
        return least_mapped_output

    _build_view()
    return


@app.cell(hide_code=True)
def existing_stock_heading(mo):
    mo.md("""
    ### Existing-stock model-year composition
    """)
    return


@app.cell
def _(NLR_ORDER, NRCAN_CLASS_ORDER, alt, chart_ui, evidence, mo, pd):
    def _build_view():
        age_source = evidence["mapped_fleet"].loc[evidence["mapped_fleet"]["mapping_accepted"].astype(str).str.lower().eq("true")].copy()
        for _column in ["FIT_ACTIVE", "MODEL_YEAR", "report_year"]:
            age_source[_column] = pd.to_numeric(age_source[_column], errors="coerce")
        latest_report_year = int(age_source["report_year"].max())
        age_source = age_source.loc[
            age_source["report_year"].eq(latest_report_year)
            & age_source["MODEL_YEAR"].between(2000, latest_report_year)
            & age_source["FIT_ACTIVE"].gt(0)
        ].copy()
        age_source["age"] = latest_report_year - age_source["MODEL_YEAR"]


        def empirical_composition(class_column: str) -> pd.DataFrame:
            result = (
                age_source.groupby(["nrcan_ceud_class", class_column, "MODEL_YEAR", "age"], as_index=False, dropna=False)["FIT_ACTIVE"]
                .sum()
                .rename(columns={"MODEL_YEAR": "model_year", "FIT_ACTIVE": "fit_active_stock"})
            )
            result["within_model_year_share"] = result["fit_active_stock"] / result.groupby(["nrcan_ceud_class", "model_year"])["fit_active_stock"].transform("sum")
            return result


        nlr_model_year_composition = empirical_composition("nlr_atb_class")
        nrcan_model_year_composition = empirical_composition("nrcan_vehicle_class")
        wards = evidence["wards_comparison"].copy()
        wards["year"] = pd.to_numeric(wards["year"], errors="coerce").astype("Int64")
        wards["market_share"] = pd.to_numeric(wards["market_share"], errors="coerce")
        wards = wards.loc[wards["year"].isin([2018, 2021])].copy()
        wards_nlr = wards.groupby(["nrcan_ceud_class", "nlr_atb_class", "year"], as_index=False)["market_share"].sum()
        wards_nrcan = wards.groupby(["nrcan_ceud_class", "nrcan_vehicle_class", "year"], as_index=False)["market_share"].sum()


        def model_year_comparison_chart(empirical: pd.DataFrame, wards_frame: pd.DataFrame, ceud_class: str, class_column: str, legend_title: str) -> alt.Chart:
            empirical_rows = empirical.loc[
                empirical["nrcan_ceud_class"].eq(ceud_class)
            ].copy()
            wards_rows = wards_frame.loc[
                wards_frame["nrcan_ceud_class"].eq(ceud_class)
            ].copy()
            observed_classes = set(
                pd.concat(
                    [empirical_rows[class_column], wards_rows[class_column]],
                    ignore_index=True,
                )
                .dropna()
                .astype(str)
            )
            preferred_order = (
                NLR_ORDER
                if class_column == "nlr_atb_class"
                else NRCAN_CLASS_ORDER[ceud_class]
            )
            class_domain = [
                vehicle_class
                for vehicle_class in preferred_order
                if vehicle_class in observed_classes
            ]
            class_domain.extend(sorted(observed_classes - set(class_domain)))
            stack_order = {
                vehicle_class: order
                for order, vehicle_class in enumerate(class_domain)
            }
            empirical_rows["stack_order"] = empirical_rows[class_column].map(
                stack_order
            )
            wards_rows["stack_order"] = wards_rows[class_column].map(stack_order)
            areas = (
                alt.Chart(empirical_rows)
                .mark_area()
                .encode(
                    x=alt.X("model_year:Q", title="Model year", scale=alt.Scale(domain=[2000, latest_report_year]), axis=alt.Axis(format="d")),
                    y=alt.Y(
                        "fit_active_stock:Q",
                        title="Mapped FIT_ACTIVE stock",
                        stack="zero",
                        axis=alt.Axis(format="~s"),
                        scale=alt.Scale(zero=True, nice=True),
                    ),
                    color=alt.Color(f"{class_column}:N", title=legend_title, scale=alt.Scale(domain=class_domain, scheme="tableau10"), legend=alt.Legend(orient="right")),
                    order=alt.Order("stack_order:Q", sort="ascending"),
                    tooltip=[alt.Tooltip(f"{class_column}:N", title=legend_title), "model_year:Q", "age:Q", alt.Tooltip("fit_active_stock:Q", format=","), alt.Tooltip("within_model_year_share:Q", format=".2%")],
                )
                .properties(width=650, height=280, title="Ontario mapped stock")
            )
            wards_bars = (
                alt.Chart(wards_rows)
                .mark_bar()
                .encode(
                    x=alt.X("year:O", title="Wards year", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("market_share:Q", title="Wards new-sales share", stack="zero", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color(f"{class_column}:N", title=legend_title, scale=alt.Scale(domain=class_domain, scheme="tableau10"), legend=alt.Legend(orient="right")),
                    order=alt.Order("stack_order:Q", sort="ascending"),
                    tooltip=[alt.Tooltip(f"{class_column}:N", title=legend_title), alt.Tooltip("year:Q", title="Wards sales year"), alt.Tooltip("market_share:Q", format=".2%", title="Wards market share")],
                )
                .properties(width=150, height=280, title="Wards sales")
            )
            return (
                alt.hconcat(areas, wards_bars, spacing=18)
                .resolve_scale(color="shared", y="independent")
                .configure_legend(orient="right")
                .properties(title=ceud_class)
            )


        def composition_tab(empirical: pd.DataFrame, wards_frame: pd.DataFrame, class_column: str, legend_title: str) -> mo.Html:
            return mo.vstack([
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Car", class_column, legend_title)),
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Light Truck", class_column, legend_title)),
                mo.ui.tabs({
                    "MTO latest-stock table": mo.ui.table(empirical.sort_values(["nrcan_ceud_class", class_column, "model_year"]), selection=None, pagination=True, page_size=10, format_mapping={"within_model_year_share": "{:.2%}"}),
                    "Wards 2018/2021 table": mo.ui.table(wards_frame.sort_values(["nrcan_ceud_class", "year", class_column]), selection=None, pagination=False, page_size=10, format_mapping={"market_share": "{:.2%}"}),
                }),
            ])


        age_distribution_output = mo.vstack([
            mo.md("""
            **Insight:** this uses only accepted `FIT_ACTIVE` stock from the latest
            Report A and retains model years 2000 through the report year. It does not
            use the scenario median-lifetime or maximum-age cutoff.

            The stacked areas show the absolute mapped `FIT_ACTIVE` stock observed at each
            model year, with an automatically scaled stock axis so the fleet-size trend is
            visible without a fixed percentage ceiling. The compact stacked bars at right
            remain Wards 2018 and 2021 new-sales percentages and therefore use their own
            labelled 0-100% axis. Wards describes Canadian new sales, while the areas
            describe Ontario surviving registered stock, so differences reflect geography,
            retirement, migration, mapping coverage, and sales history; the bars are
            benchmarks, not inputs to the empirical composition.
            """),
            mo.ui.tabs({
                "NLR ATB aggregation": composition_tab(nlr_model_year_composition, wards_nlr, "nlr_atb_class", "NLR ATB class"),
                "NRCan Ratings aggregation": composition_tab(nrcan_model_year_composition, wards_nrcan, "nrcan_vehicle_class", "NRCan Ratings class"),
            }),
        ])
        return age_distribution_output

    _build_view()
    return


@app.cell(hide_code=True)
def retention_survival_section(mo):
    mo.md("""
    ## Retention and survival evidence
    """)
    return


@app.cell(hide_code=True)
def retention_method_heading(mo):
    mo.md("""
    ### From Report A transitions to partial MTO retention
    """)
    return


@app.cell
def _(evidence, mo, pd):
    def _build_view():
        transition_rows = evidence["raw_key_transitions"].copy()
        for _column in ["age", "cohort_count_t", "cohort_count_t1", "apparent_retirements", "annual_survival_factor", "annual_retirement_rate"]:
            transition_rows[_column] = pd.to_numeric(transition_rows[_column], errors="coerce")
        usable_transition_rows = transition_rows.loc[
            transition_rows["stock_status"].eq("FIT_ACTIVE")
            & transition_rows["age"].ge(0)
            & transition_rows["cohort_count_t"].gt(0)
        ].copy()
        transition_support = (
            usable_transition_rows.groupby(["population_group", "vehicle_class"], as_index=False)
            .agg(
                minimum_observed_age=("age", "min"),
                maximum_observed_age=("age", "max"),
                transition_observations=("age", "size"),
                distinct_mto_model_codes=("mto_model_code", "nunique"),
                exposed_fit_active_stock=("cohort_count_t", "sum"),
                survival_factor_above_one=("annual_survival_factor", lambda values: int(values.gt(1).sum())),
            )
        )
        transition_support["share_survival_factor_above_one"] = transition_support["survival_factor_above_one"] / transition_support["transition_observations"]
        final_mto_curves = evidence["ceud_class_retention"].copy()
        for column in [
            "age",
            "annual_retirement_rate",
            "annual_survival_factor",
            "cumulative_survival",
        ]:
            final_mto_curves[column] = pd.to_numeric(
                final_mto_curves[column], errors="coerce"
            )
        recurrence_errors = []
        for _, class_rows in final_mto_curves.groupby("vehicle_class"):
            class_rows = class_rows.sort_values("age").reset_index(drop=True)
            prior = class_rows.iloc[:-1]
            following = class_rows.iloc[1:]
            comparable = prior["annual_survival_factor"].notna().to_numpy() & following[
                "cumulative_survival"
            ].notna().to_numpy()
            expected = (
                prior["cumulative_survival"].to_numpy()
                * prior["annual_survival_factor"].to_numpy()
            )
            observed = following["cumulative_survival"].to_numpy()
            recurrence_errors.extend(abs(observed[comparable] - expected[comparable]))
        maximum_recurrence_error = max(recurrence_errors, default=0.0)
        mto_retention_method_output = mo.vstack([
            mo.md(r"""
            The raw transition file is the audit-grain estimator input. Class mapping is
            deliberately absent from it: Passenger/Commercial, MTO make-model key,
            vintage, report-year pair, and both FIT_ACTIVE counts are preserved before
            any class claim is attached. A row exists only when both consecutive editions
            contain the same strictly FIT_ACTIVE key, starting exposure is positive, and
            starting age is between 0 and the configured maximum of 35. No model-year
            floor is applied at this survival interface; the separate 2000 floor remains
            limited to existing-fleet weights and age-distribution aggregation. Older
            source vintages are retained whenever they contribute a transition in the
            supported age window.

            Variable meanings:

            - $p$: Report A source category, Passenger or Commercial.
            - $k$: stable abbreviated MTO make-model key.
            - $v$: model-year vintage of that key.
            - $t$: report year at the beginning of a one-year transition.
            - $a=t-v$: vehicle age at the beginning of the transition.
            - $N_{p,k,v,t}$: observed FIT_ACTIVE stock at the start of the transition.
            - $D$: apparent retirements, equal to starting stock minus next-year stock;
              it can be negative when registrations grow.
            - $q$: empirical one-year apparent retirement rate; $r=1-q$ is the
              corresponding survival factor.
            - $c$: reviewed target vehicle class, and $m(k,v)$ is the class assigned
              to a particular MTO key and vintage.
            - $E$: pooled starting exposure—the sum of beginning-of-transition stock
              contributing to a class-vintage-age estimate.
            - $S$ and $F$: cumulative survival and cumulative scrappage implied by the
              sequence of pooled annual rates.

            For each eligible transition the backend executes:

            \[
            a=t-v,
            \qquad
            D_{p,k,v,t}=N_{p,k,v,t}-N_{p,k,v,t+1},
            \qquad
            r_{p,k,v,t}=\frac{N_{p,k,v,t+1}}{N_{p,k,v,t}},
            \qquad
            q_{p,k,v,t}=\frac{D_{p,k,v,t}}{N_{p,k,v,t}}=1-r_{p,k,v,t}.
            \]

            Raw growth cases retain \(q<0\) and \(r>1\); they are never clipped. Only
            after these quantities exist does the reviewed crosswalk attach class labels.
            For class \(c\), vintage \(v\), and age \(a\), the backend pools counts—not
            percentages—and then aggregates vintages with starting exposure:

            \[
            E_{c,v,a}=\sum_{p,k:m(k,v)=c}N_{p,k,v,v+a},
            \qquad
            D_{c,v,a}=\sum_{p,k:m(k,v)=c}
            \left(N_{p,k,v,v+a}-N_{p,k,v,v+a+1}\right),
            \]

            \[
            q_{c,a}=\frac{\sum_v D_{c,v,a}}{\sum_v E_{c,v,a}},
            \qquad r_{c,a}=1-q_{c,a}.
            \]

            Cumulative MTO survival is an actual product of those empirical annual
            factors with an explicit age-zero baseline:

            \[
            S_c(0)=1,
            \qquad S_c(a+1)=S_c(a)r_{c,a},
            \qquad F_c(a)=1-S_c(a).
            \]

            This follows the NHTSA indexing convention that a vehicle is age zero in its
            production year and survival is 100% at that baseline. Age zero is also the
            first empirical MTO starting age:
            the age-0 rate updates survival at age 1, the age-1 rate updates survival at
            age 2, and so on. The terminal row after the oldest observed rate shows the
            effect of that final annual factor. Thus, the configured age-35 transition
            ceiling produces a terminal cumulative point at age 36. Unlike NHTSA's richer
            dynamic method, the MTO calculation uses every available early transition,
            including age 0 to 1; it does not discard ages before 2. Source-reported
            pre-2000 vintages remain eligible, but observations whose starting age exceeds
            35 are excluded before mapping and pooling because that sparse tail is outside
            the selected forward-model evidence horizon.

            The recurrence is checked directly against the generated class-age output.
            NHTSA values are loaded only after the MTO transition, mapping, pooling, and
            cumulative-product outputs have been constructed. They are comparison evidence
            and are not parameters, targets, calibration points, or force-fitting inputs
            for the MTO curves.

            #### Implemented source-to-diagnostic stages

            | Stage | Grain | Purpose |
            |---|---|---|
            | Raw annual snapshot | source category x MTO make-model x vintage x report year | Preserve each observed fit-active Report A cohort count before mapping. |
            | Raw key transition | same make-model-vintage in consecutive report years, starting age 0-35 | Calculate un-clipped apparent retirements, annual survival factor, and retirement rate. |
            | Mapped key transition | eligible raw transition plus reviewed vintage-range crosswalk | Attach class evidence without recomputing or filtering the raw rate by latest-snapshot presence. |
            | NLR and CEUD class-vintage | class x vintage x age | Sum starting exposure and apparent retirements. |
            | Final CEUD class-age | Car/Light Truck x age | Sum across vintages, calculate annual rates, support counts, and the cumulative product. |
            | Decision gate | class-level diagnostic | Retain NHTSA schedules unless coverage, bounds, continuity, monotonicity, support, and comparison gates all pass. |
            """),
            mo.stat(f"{maximum_recurrence_error:.3g}", "Maximum cumulative-survival recurrence error", "Zero within numerical precision confirms the displayed curve is the product of the empirical annual factors", bordered=True, target_direction="decrease"),
            mo.ui.table(transition_support, selection=None, pagination=False, page_size=10, format_mapping={"share_survival_factor_above_one": "{:.2%}"}, label="Passenger and Commercial audit support"),
        ])
        return mto_retention_method_output

    _build_view()
    return


@app.cell(hide_code=True)
def transition_rate_distribution_heading(mo):
    mo.md("""
    ### Make-model-vintage retirement-rate distributions by age
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        rate_rows = evidence["raw_key_transitions"].copy()
        for column in [
            "age",
            "annual_retirement_rate",
            "cohort_count_t",
        ]:
            rate_rows[column] = pd.to_numeric(rate_rows[column], errors="coerce")
        rate_rows = rate_rows.loc[
            rate_rows["annual_retirement_rate"].notna()
        ].copy()
        display_minimum = -0.35 # covers rates from 5th percentile to 95th percentile for all classes and ages below 30
        display_maximum = 0.40
        bin_width = 0.02
        central_rates = rate_rows.loc[
            rate_rows["annual_retirement_rate"].between(
                display_minimum, display_maximum
            )
        ].copy()
        central_rates["rate_bin_number"] = (
            (
                central_rates["annual_retirement_rate"] - display_minimum
            )
            / bin_width
        ).astype(int).clip(
            lower=0,
            upper=int((display_maximum - display_minimum) / bin_width) - 1,
        )
        central_rates["rate_bin_start"] = (
            display_minimum + central_rates["rate_bin_number"] * bin_width
        )
        central_rates["rate_bin_end"] = (
            central_rates["rate_bin_start"] + bin_width
        )
        histogram_rows = (
            central_rates.groupby(
                [
                    "vehicle_class",
                    "age",
                    "rate_bin_start",
                    "rate_bin_end",
                ],
                as_index=False,
            )
            .agg(
                transition_count=("annual_retirement_rate", "size"),
                starting_exposure=("cohort_count_t", "sum"),
            )
        )
        distribution_summary = (
            rate_rows.groupby(["vehicle_class", "age"], as_index=False)
            .agg(
                transition_count=("annual_retirement_rate", "size"),
                starting_exposure=("cohort_count_t", "sum"),
                minimum_rate=("annual_retirement_rate", "min"),
                fifth_percentile=(
                    "annual_retirement_rate",
                    lambda values: values.quantile(0.05),
                ),
                median_rate=("annual_retirement_rate", "median"),
                ninety_fifth_percentile=(
                    "annual_retirement_rate",
                    lambda values: values.quantile(0.95),
                ),
                maximum_rate=("annual_retirement_rate", "max"),
                negative_rate_count=(
                    "annual_retirement_rate",
                    lambda values: int(values.lt(0).sum()),
                ),
                below_display_window=(
                    "annual_retirement_rate",
                    lambda values: int(values.lt(display_minimum).sum()),
                ),
                above_display_window=(
                    "annual_retirement_rate",
                    lambda values: int(values.gt(display_maximum).sum()),
                ),
            )
        )
        distribution_summary["negative_rate_share"] = (
            distribution_summary["negative_rate_count"]
            / distribution_summary["transition_count"]
        )
        def histogram_matrix(source_category: str, color: str) -> alt.VConcatChart:
            source_rows = histogram_rows.loc[
                histogram_rows["vehicle_class"].eq(source_category)
            ].copy()
            source_rows["age_label"] = (
                "age=" + source_rows["age"].astype(int).astype(str)
            )
            ages = sorted(source_rows["age"].astype(int).unique())
            matrix_rows = []
            for row_start in range(0, len(ages), 6):
                row_ages = ages[row_start : row_start + 6]
                row_source = source_rows.loc[source_rows["age"].isin(row_ages)]
                row_transition_max = max(
                    1, int(row_source["transition_count"].max())
                )
                panels = []
                for column_index, age in enumerate(row_ages):
                    panel_rows = row_source.loc[row_source["age"].eq(age)]
                    bars = (
                        alt.Chart(panel_rows)
                        .mark_bar(color=color)
                        .encode(
                            x=alt.X(
                                "rate_bin_start:Q",
                                bin=alt.Bin(binned=True),
                                title=None,
                                axis=alt.Axis(format="%", tickCount=5),
                                scale=alt.Scale(
                                    domain=[display_minimum, display_maximum]
                                ),
                            ),
                            x2=alt.X2("rate_bin_end:Q"),
                            y=alt.Y(
                                "transition_count:Q",
                                title=None,
                                axis=alt.Axis(
                                    tickCount=4,
                                    labels=column_index == 0,
                                    ticks=column_index == 0,
                                    domain=column_index == 0,
                                ),
                                scale=alt.Scale(
                                    domain=[0, row_transition_max], nice=True
                                ),
                            ),
                            tooltip=[
                                alt.Tooltip("age:O", title="Starting age"),
                                alt.Tooltip("rate_bin_start:Q", format=".1%"),
                                alt.Tooltip("rate_bin_end:Q", format=".1%"),
                                alt.Tooltip("transition_count:Q", format=","),
                                alt.Tooltip("starting_exposure:Q", format=","),
                            ],
                        )
                    )
                    age_labels = (
                        alt.Chart(pd.DataFrame({"age_label": [f"age={age}"]}))
                        .mark_text(
                            align="right",
                            baseline="top",
                            color="#949393",
                            fontSize=11,
                        )
                        .encode(
                            x=alt.value(141),
                            y=alt.value(4),
                            text=alt.Text("age_label:N"),
                        )
                    )
                    median_rate = distribution_summary.loc[
                        distribution_summary["vehicle_class"].eq(source_category)
                        & distribution_summary["age"].eq(age),
                        "median_rate",
                    ].iloc[0]
                    median_labels = (
                        alt.Chart(
                            pd.DataFrame(
                                {
                                    "median_label": [
                                        f"med={median_rate:.1%}"
                                    ]
                                }
                            )
                        )
                        .mark_text(
                            align="right",
                            baseline="top",
                            color="#949393",
                            fontSize=10,
                        )
                        .encode(
                            x=alt.value(145),
                            y=alt.value(17),
                            text=alt.Text("median_label:N"),
                        )
                    )
                    panels.append(
                        alt.layer(bars, age_labels, median_labels).properties(
                            width=145, height=85
                        )
                    )
                matrix_rows.append(
                    alt.hconcat(*panels, spacing=4).resolve_scale(
                        x="shared", y="shared"
                    )
                )
            return (
                alt.vconcat(*matrix_rows, spacing=4)
                .resolve_scale(y="independent")
                .configure_view(stroke="#e2e2e2")
            )

        rate_distribution_output = mo.vstack([
            mo.md("""
            Each small histogram describes the cross-sectional distribution of raw
            one-year rates across eligible MTO make-model-vintage series at one starting
            age. Every series receives one observation per eligible report-year pair;
            these histograms are counts, not stock-weighted class estimates. Passenger and
            Commercial remain source categories because this view precedes class mapping.
            Age labels sit inside each panel; all panels and both tabs share the same
            -35% to +40% x-domain. Each six-panel row shares its own automatically scaled
            transition-count y-domain, so later-age distributions remain visible as
            observations diminish. Axis titles and subplot headers are omitted to keep
            the grid compact.

            The histogram window is -35% to +40% since it covers 90% of the data. 
            This is display-only: no raw value is clipped or discarded from the
            estimator. The table reports full-range minima, maxima, quantiles, negative
            growth-case counts, and observations outside the displayed window. Very large
            negative rates occur when a small starting cohort grows sharply in the next
            edition; their low exposure is visible separately from their observation count.
            """),
            mo.ui.tabs({
                "Passenger distributions": chart_ui(
                    histogram_matrix("PASSENGER", "#457b9d")
                ),
                "Commercial distributions": chart_ui(
                    histogram_matrix("COMMERCIAL", "#e76f51")
                ),
            }),
            mo.ui.table(
                distribution_summary,
                selection=None,
                pagination=True,
                page_size=10,
                format_mapping={
                    "minimum_rate": "{:.2%}",
                    "fifth_percentile": "{:.2%}",
                    "median_rate": "{:.2%}",
                    "ninety_fifth_percentile": "{:.2%}",
                    "maximum_rate": "{:.2%}",
                    "negative_rate_share": "{:.2%}",
                },
                label="Full un-clipped rate distribution and tail audit by age",
            ),
        ])
        return rate_distribution_output

    _build_view()
    return


@app.cell(hide_code=True)
def status_proxy_heading(mo):
    mo.md("""
    ### Fit-active versus fit-inactive
    """)
    return


@app.cell(hide_code=True)
def passenger_status_proxy(
    STATUS_COLORS,
    STATUS_ORDER,
    alt,
    chart_ui,
    evidence,
    mo,
    pd,
):
    def _build_view():
        raw_status_cohorts = evidence["status_long"].copy()
        for _column in ["report_year", "MODEL_YEAR", "native_count"]:
            raw_status_cohorts[_column] = pd.to_numeric(
                raw_status_cohorts[_column], errors="coerce"
            )
        fit_status_order = ["FIT_ACTIVE", "FIT_INACTIVE"]
        fit_status_colors = [
            STATUS_COLORS[STATUS_ORDER.index(status)] for status in fit_status_order
        ]
        fit_status_latest_year = int(raw_status_cohorts["report_year"].max())
        fit_status_latest = raw_status_cohorts.loc[
            raw_status_cohorts["report_year"].eq(fit_status_latest_year)
            & raw_status_cohorts["stock_status"].isin(fit_status_order)
        ].copy()
        fit_status_latest["age"] = (
            fit_status_latest_year - fit_status_latest["MODEL_YEAR"]
        )
        fit_status_latest = fit_status_latest.loc[
            fit_status_latest["age"].between(0, 40)
        ].copy()
        fit_status_by_age = (
            fit_status_latest.groupby(
                ["VEHICLE_CLASS", "age", "stock_status"], as_index=False
            )["native_count"]
            .sum()
            .rename(columns={"native_count": "cohort_count"})
        )
        fit_status_age_totals = fit_status_by_age.groupby(
            ["VEHICLE_CLASS", "age"]
        )["cohort_count"].transform("sum")
        fit_status_by_age["status_share"] = (
            fit_status_by_age["cohort_count"] / fit_status_age_totals
        )
        fit_status_bucket_summary = (
            fit_status_latest.groupby(
                ["VEHICLE_CLASS", "stock_status"], as_index=False
            )["native_count"]
            .sum()
            .rename(columns={"native_count": "cohort_count"})
        )
        fit_status_bucket_summary["share_of_two_status_stock"] = (
            fit_status_bucket_summary["cohort_count"]
            / fit_status_bucket_summary.groupby("VEHICLE_CLASS")["cohort_count"].transform(
                "sum"
            )
        )

        def status_view(vehicle_class: str) -> mo.Html:
            rows = fit_status_by_age.loc[
                fit_status_by_age["VEHICLE_CLASS"].eq(vehicle_class)
            ]
            counts = (
                alt.Chart(rows)
                .mark_area(opacity=0.8)
                .encode(
                    x=alt.X("age:Q", title=f"Vehicle age in {fit_status_latest_year}", scale=alt.Scale(domain=[0, 40])),
                    y=alt.Y("cohort_count:Q", title="Raw registrations", stack="zero"),
                    color=alt.Color("stock_status:N", title="Status bucket", scale=alt.Scale(domain=fit_status_order, range=fit_status_colors)),
                    tooltip=["stock_status:N", "age:Q", alt.Tooltip("cohort_count:Q", format=",")],
                )
                .properties(width=600, height=260, title="Registration counts by age")
            )
            share = (
                alt.Chart(rows.loc[rows["stock_status"].eq("FIT_ACTIVE")])
                .mark_line(point=True, color="#2a9d8f")
                .encode(
                    x=alt.X("age:Q", title=f"Vehicle age in {fit_status_latest_year}", scale=alt.Scale(domain=[0, 40])),
                    y=alt.Y("status_share:Q", title="Fit-active share of two buckets", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    tooltip=["age:Q", alt.Tooltip("status_share:Q", format=".2%")],
                )
                .properties(width=700, height=300, title="Fit-active share by age")
            )
            return mo.vstack([chart_ui(counts), chart_ui(share)])


        status_proxy_output = mo.vstack([
            mo.md("""
            **Insight:** Passenger and Commercial remain separate, and this comparison
            now uses only the raw `FIT_ACTIVE` and `FIT_INACTIVE` Report A columns. No
            unfit, wrecked, sold, suspended, temporary, or out-of-province records are
            folded into the second bucket. It remains an administrative-status view,
            not physical retirement or scrappage.
            """),
            mo.ui.tabs({"Passenger": status_view("PASSENGER"), "Commercial": status_view("COMMERCIAL")}),
            mo.ui.table(fit_status_bucket_summary, selection=None, pagination=False, page_size=10, format_mapping={"share_of_two_status_stock": "{:.2%}"}, label=f"Fit-active and fit-inactive totals in {fit_status_latest_year}"),
        ])
        return status_proxy_output

    _build_view()
    return


@app.cell(hide_code=True)
def survival_comparison_heading(mo):
    mo.md("""
    ### Legacy survival curves versus MTO-only evidence
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        coverage = evidence["transition_mapping_coverage"].copy()
        for _column in ["fit_active_exposure", "mapped_fit_active_exposure", "mapped_non_ldv_fit_active_exposure", "unmapped_fit_active_exposure", "out_of_scope_fit_active_exposure", "mapped_exposure_share", "mapped_non_ldv_exposure_share", "unmapped_exposure_share", "out_of_scope_exposure_share"]:
            coverage[_column] = pd.to_numeric(coverage[_column], errors="coerce")
        coverage_parts = pd.concat([
            coverage[["source_category", "fit_active_exposure", "mapped_fit_active_exposure"]]
            .rename(columns={"mapped_fit_active_exposure": "exposure"})
            .assign(mapping_status="Mapped to Car or Light Truck", status_order=0),
            coverage[["source_category", "fit_active_exposure", "mapped_non_ldv_fit_active_exposure"]]
            .rename(columns={"mapped_non_ldv_fit_active_exposure": "exposure"})
            .assign(mapping_status="Mapped, non-LDV", status_order=1),
            coverage[["source_category", "fit_active_exposure", "unmapped_fit_active_exposure"]]
            .rename(columns={"unmapped_fit_active_exposure": "exposure"})
            .assign(mapping_status="Unmapped", status_order=2),
            coverage[["source_category", "fit_active_exposure", "out_of_scope_fit_active_exposure"]]
            .rename(columns={"out_of_scope_fit_active_exposure": "exposure"})
            .assign(mapping_status="Outside 1981+ scope", status_order=3),
        ], ignore_index=True)
        coverage_parts["exposure_share"] = coverage_parts["exposure"] / coverage_parts["fit_active_exposure"]
        coverage_chart = (
            alt.Chart(coverage_parts)
            .mark_bar()
            .encode(
                x=alt.X("exposure_share:Q", title="Share of starting FIT_ACTIVE exposure", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                y=alt.Y("source_category:N", title="Report A source category", sort=["PASSENGER", "COMMERCIAL"]),
                color=alt.Color("mapping_status:N", title="Transition mapping", scale=alt.Scale(domain=["Mapped to Car or Light Truck", "Mapped, non-LDV", "Unmapped", "Outside 1981+ scope"], range=["#2a9d8f", "#457b9d", "#d9d9d9", "#8d99ae"])),
                order=alt.Order("status_order:Q"),
                tooltip=["source_category:N", "mapping_status:N", alt.Tooltip("exposure:Q", format=","), alt.Tooltip("exposure_share:Q", format=".2%")],
            )
            .properties(width=700, height=150, title="Historical transition-exposure mapping coverage")
        )

        mto_curves = evidence["ceud_class_retention"].copy()
        numeric_columns = [
            "age", "annual_retirement_rate", "annual_survival_factor",
            "cumulative_survival", "cumulative_scrappage", "fit_active_exposure",
            "number_of_vintages", "number_of_transitions",
        ]
        for _column in numeric_columns:
            mto_curves[_column] = pd.to_numeric(mto_curves[_column], errors="coerce")
        annual_rows = mto_curves.loc[mto_curves["annual_retirement_rate"].notna()].copy()
        annual_age_domain = [
            int(annual_rows["age"].min()),
            int(annual_rows["age"].max()),
        ]
        cumulative_age_ceiling = annual_age_domain[1] + 1
        annual_lines = (
            alt.Chart(annual_rows)
            .mark_line(interpolate="linear")
            .encode(
                x=alt.X("age:Q", title="Starting vehicle age", scale=alt.Scale(domain=annual_age_domain)),
                y=alt.Y("annual_retirement_rate:Q", title="Annual apparent retirement rate", axis=alt.Axis(format="%"), scale=alt.Scale(zero=True)),
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right")),
            )
        )
        annual_points = (
            alt.Chart(annual_rows)
            .mark_circle(opacity=0.8)
            .encode(
                x="age:Q",
                y="annual_retirement_rate:Q",
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right")),
                size=alt.Size("fit_active_exposure:Q", title="Starting exposure", scale=alt.Scale(range=[20, 240])),
                tooltip=["vehicle_class:N", "age:Q", alt.Tooltip("annual_retirement_rate:Q", format=".2%"), alt.Tooltip("annual_survival_factor:Q", format=".2%"), alt.Tooltip("fit_active_exposure:Q", format=","), "number_of_vintages:Q", "number_of_transitions:Q"],
            )
        )
        annual_chart = (annual_lines + annual_points).properties(
            width=700, height=300, title="Exposure-pooled annual MTO retirement rates"
        )

        legacy_survival = evidence["legacy_survival"].copy()
        legacy_survival["age"] = pd.to_numeric(legacy_survival["age"], errors="coerce")
        legacy_survival["survival_probability"] = pd.to_numeric(legacy_survival["survival_probability"], errors="coerce")
        legacy_comparison = legacy_survival.loc[
            legacy_survival["source_class"].isin(["Car", "Light Truck"])
            & legacy_survival["age"].le(cumulative_age_ceiling),
            ["source_class", "age", "survival_probability"],
        ].rename(columns={"source_class": "vehicle_class", "survival_probability": "cumulative_survival"})
        legacy_comparison["evidence_source"] = "Legacy NHTSA"
        mto_comparison = mto_curves.loc[
            mto_curves["cumulative_survival"].notna()
            & mto_curves["age"].le(cumulative_age_ceiling),
            ["vehicle_class", "age", "cumulative_survival"],
        ].copy()
        mto_comparison["evidence_source"] = "MTO cohort transitions"
        survival_comparison = pd.concat([legacy_comparison, mto_comparison], ignore_index=True)
        survival_line_types = {
            "MTO cohort transitions": "Solid: MTO transition evidence",
            "Legacy NHTSA": "Dashed: NHTSA legacy schedule",
        }
        survival_comparison["line_type"] = survival_comparison[
            "evidence_source"
        ].map(survival_line_types)
        comparison_age_max = cumulative_age_ceiling
        cumulative_chart = (
            alt.Chart(survival_comparison)
            .mark_line(point=False, interpolate="linear", strokeWidth=3)
            .encode(
                x=alt.X("age:Q", title="Vehicle age", scale=alt.Scale(domain=[0, comparison_age_max])),
                y=alt.Y("cumulative_survival:Q", title="Cumulative survival", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right")),
                strokeDash=alt.StrokeDash(
                    "line_type:N",
                    title="Evidence source",
                    scale=alt.Scale(
                        domain=[
                            "Solid: MTO transition evidence",
                            "Dashed: NHTSA legacy schedule",
                        ],
                        range=[[1, 0], [7, 4]],
                    ),
                    legend=alt.Legend(
                        orient="right",
                        symbolType="stroke",
                        symbolStrokeWidth=3,
                        symbolSize=320,
                    ),
                ),
                tooltip=["vehicle_class:N", "evidence_source:N", "age:Q", alt.Tooltip("cumulative_survival:Q", format=".2%")],
            )
            .properties(width=700, height=300, title="MTO and legacy NHTSA cumulative survival")
        )

        decision = evidence["mto_survival_decision"].copy()
        decision_outcome = str(decision["decision_outcome"].iloc[0])
        survival_comparison_output = mo.vstack([
            mo.md(f"""
            **Decision: {decision_outcome}.** The estimator uses only strictly
            `FIT_ACTIVE` keys observed in consecutive Report A editions, with positive
            starting exposure and nonnegative starting age. Age 0 to 1 is the first
            empirical transition. Class mapping is attached afterwards. Raw and pooled
            negative retirement rates are preserved, never clipped. Production rates use
            only make-model-vintage series that remain present in the latest snapshot;
            latest-snapshot composition weights still do not enter the rate calculation.

            #### 1. Stock-weighted historical mapping coverage

            This is not the share of unique make-model-vintage series. Its denominator is
            the sum of `N_{{p,k,v,t}}`, the FIT_ACTIVE stock at the beginning of every
            eligible historical transition. A series observed over several annual pairs
            contributes its starting exposure to each pair. The mapped share is the part
            of that exposure whose reviewed vintage-specific crosswalk resolves to CEUD
            Car or Light Truck. Mapped non-LDV exposure is shown separately and remains
            excluded from the LDV retirement-rate pools. Vintages older than the dynamic
            1981 mapping floor are reported separately rather than treated as unresolved.
            """),
            chart_ui(coverage_chart),
            mo.md("""
            #### 2. Empirical annual rates, exposure, and vintage support

            At each class and age, the empirical rate is total apparent retirements divided
            by total starting exposure. Starting exposure is therefore the summed stock of
            every mapped make-model-vintage-report-year transition entering that point—not
            the latest fleet stock. Vintage support counts distinct model years, while
            transition support counts the individual make-model-vintage annual pairs.
            Point area represents starting exposure; the color legend distinguishes Car
            and Light Truck.
            """),
            chart_ui(annual_chart),
            mo.md("""
            Hovering reports exposure and both support counts. The configured decision gate
            evaluates starting ages 0 through 35. This keeps all eligible early evidence
            without treating 2000 as a survival-evidence floor, while excluding the sparse,
            noisy older-age tail.

            #### 3. Like-for-like cumulative-survival comparison

            Both series display an age-zero baseline. For MTO, the age-0 annual rate is the
            first empirical factor and determines survival at age 1; the age-1 rate then
            determines survival at age 2. The MTO line is not smoothed, interpolated
            statistically, or fitted: each plotted point is the direct cumulative product
            of the exposure-pooled MTO factors and straight segments merely connect
            consecutive ages. A cumulative product naturally looks smoother than its annual
            inputs because each new rate changes the preceding survival level
            multiplicatively. Declining late-age exposure makes those smooth-looking levels
            less certain, not more reliable. The age-35 annual rate yields the terminal MTO
            survival point at age 36; NHTSA is truncated to the same cumulative horizon for
            comparison. Color identifies vehicle class, while the stroke-symbol legend
            identifies solid MTO transition evidence and dashed NHTSA legacy schedules.

            NHTSA is loaded only after the MTO transition table, annual pooling, and
            cumulative product already exist. It never enters an MTO numerator,
            denominator, weight, rate, interpolation, or calibration step.
            """),
            chart_ui(cumulative_chart),
            mo.ui.tabs({
                "Decision gate": mo.ui.table(decision, selection=None, pagination=False, page_size=10, format_mapping={"mapped_fit_active_exposure_share": "{:.2%}", "in_bounds_rate_share": "{:.2%}"}),
                "Final MTO class-age output": mo.ui.table(mto_curves.sort_values(["vehicle_class", "age"]), selection=None, pagination=True, page_size=10, format_mapping={"annual_retirement_rate": "{:.2%}", "annual_survival_factor": "{:.2%}", "cumulative_survival": "{:.2%}", "cumulative_scrappage": "{:.2%}"}),
                "Transition mapping coverage": mo.ui.table(coverage, selection=None, pagination=False, page_size=10, format_mapping={"mapped_exposure_share": "{:.2%}", "mapped_non_ldv_exposure_share": "{:.2%}", "unmapped_exposure_share": "{:.2%}", "out_of_scope_exposure_share": "{:.2%}"}),
            }),
        ])
        return survival_comparison_output

    _build_view()
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        scope_curves = evidence["ceud_scope_comparison"].copy()
        for column in [
            "age",
            "annual_retirement_rate",
            "annual_survival_factor",
            "cumulative_survival",
            "cumulative_scrappage",
            "fit_active_exposure",
            "number_of_vintages",
            "number_of_transitions",
        ]:
            scope_curves[column] = pd.to_numeric(
                scope_curves[column], errors="coerce"
            )
        scope_labels = {
            "latest_snapshot_survivors_dynamic_floor": "Production: latest survivors, 1981+",
            "latest_snapshot_survivors_1990_plus": "Sensitivity: latest survivors, 1990+",
        }
        scope_curves["aggregation_method"] = scope_curves[
            "aggregation_scope"
        ].map(scope_labels)
        scope_line_types = {
            "latest_snapshot_survivors_dynamic_floor": "Solid: production 1981+ floor",
            "latest_snapshot_survivors_1990_plus": "Dashed: 1990+ sensitivity",
        }
        scope_curves["line_type"] = scope_curves["aggregation_scope"].map(
            scope_line_types
        )
        observed_scope_rates = scope_curves.loc[
            scope_curves["annual_retirement_rate"].notna()
        ].copy()
        scope_rate_age_domain = [
            int(observed_scope_rates["age"].min()),
            int(observed_scope_rates["age"].max()),
        ]
        scope_cumulative_rows = scope_curves.loc[
            scope_curves["cumulative_survival"].notna()
        ].copy()
        scope_cumulative_age_max = int(scope_cumulative_rows["age"].max())
        class_encoding = alt.Color(
            "vehicle_class:N",
            title="NRCan CEUD class",
            scale=alt.Scale(
                domain=["Car", "Light Truck"],
                range=["#457b9d", "#e76f51"],
            ),
            legend=alt.Legend(orient="right"),
        )
        method_encoding = alt.StrokeDash(
            "line_type:N",
            title="Aggregation scope",
            scale=alt.Scale(
                domain=[
                    "Solid: production 1981+ floor",
                    "Dashed: 1990+ sensitivity",
                ],
                range=[[1, 0], [7, 4]],
            ),
            legend=alt.Legend(
                orient="right",
                symbolType="stroke",
                symbolStrokeWidth=3,
                symbolSize=320,
            ),
        )
        scope_annual_chart = (
            alt.Chart(observed_scope_rates)
            .mark_line(point=False, interpolate="linear", strokeWidth=3)
            .encode(
                x=alt.X(
                    "age:Q",
                    title="Starting vehicle age",
                    scale=alt.Scale(domain=scope_rate_age_domain),
                ),
                y=alt.Y(
                    "annual_retirement_rate:Q",
                    title="Annual apparent retirement rate",
                    axis=alt.Axis(format="%"),
                ),
                color=class_encoding,
                strokeDash=method_encoding,
                tooltip=[
                    "vehicle_class:N",
                    "aggregation_method:N",
                    "age:Q",
                    alt.Tooltip("annual_retirement_rate:Q", format=".2%"),
                    alt.Tooltip("fit_active_exposure:Q", format=","),
                    "number_of_vintages:Q",
                    "number_of_transitions:Q",
                ],
            )
            .properties(
                width=700,
                height=300,
                title="Latest-survivor retirement rates under alternative vintage floors",
            )
        )
        scope_cumulative_chart = (
            alt.Chart(scope_cumulative_rows)
            .mark_line(point=False, interpolate="linear", strokeWidth=3)
            .encode(
                x=alt.X("age:Q", title="Vehicle age", scale=alt.Scale(domain=[0, scope_cumulative_age_max])),
                y=alt.Y(
                    "cumulative_survival:Q",
                    title="Cumulative survival",
                    axis=alt.Axis(format="%"),
                    scale=alt.Scale(domain=[0, 1]),
                ),
                color=class_encoding,
                strokeDash=method_encoding,
                tooltip=[
                    "vehicle_class:N",
                    "aggregation_method:N",
                    "age:Q",
                    alt.Tooltip("cumulative_survival:Q", format=".2%"),
                ],
            )
            .properties(
                width=700,
                height=300,
                title="Cumulative effect of the 1990+ sensitivity floor",
            )
        )
        exposure_support_chart = (
            alt.Chart(observed_scope_rates)
            .mark_line(point=False, interpolate="linear", strokeWidth=3)
            .encode(
                x=alt.X("age:Q", title="Starting vehicle age", scale=alt.Scale(domain=scope_rate_age_domain)),
                y=alt.Y(
                    "fit_active_exposure:Q",
                    title="Starting FIT_ACTIVE exposure",
                    scale=alt.Scale(type="log"),
                ),
                color=class_encoding,
                strokeDash=method_encoding,
                tooltip=[
                    "vehicle_class:N",
                    "aggregation_method:N",
                    "age:Q",
                    alt.Tooltip("fit_active_exposure:Q", format=","),
                ],
            )
            .properties(width=700, height=300, title="Exposure by age")
        )
        vintage_support_chart = (
            alt.Chart(observed_scope_rates)
            .mark_line(point=False, interpolate="linear", strokeWidth=3)
            .encode(
                x=alt.X("age:Q", title="Starting vehicle age", scale=alt.Scale(domain=scope_rate_age_domain)),
                y=alt.Y("number_of_vintages:Q", title="Distinct vintages"),
                color=class_encoding,
                strokeDash=method_encoding,
                tooltip=[
                    "vehicle_class:N",
                    "aggregation_method:N",
                    "age:Q",
                    "number_of_vintages:Q",
                    "number_of_transitions:Q",
                ],
            )
            .properties(width=700, height=300, title="Vintage support by age")
        )
        scope_comparison_output = mo.vstack([
            mo.md("""
            #### 4. Production vintage scope and 1990+ sensitivity

            Both methods use only raw FIT_ACTIVE make-model-vintage series present in the
            latest snapshot. The production series retains every vintage capable of
            contributing to an observed starting age from 0 through 35; given the first
            eligible 2016 transition, that dynamic floor is 1981. The sensitivity series
            imposes a 1990 floor after transition construction. It therefore loses the
            age-35 rate and sharply reduces exposure in the oldest ages.
            """),
            chart_ui(scope_annual_chart),
            mo.md("""
            The cumulative curves below are independently compounded from each method's
            own annual MTO rates. NHTSA does not enter this comparison. Differences between
            the lines arise only from excluding 1981-1989 vintages.
            """),
            chart_ui(scope_cumulative_chart),
            mo.md(f"""
            Exposure and distinct-vintage support show what the conditioning removes.
            The production estimator begins with the empirical age-0 transition. In these generated artifacts annual evidence
            extends through starting age {scope_rate_age_domain[1]}, and the terminal
            cumulative point at age {scope_cumulative_age_max} applies that final factor.
            The 2000 floor remains confined to existing-fleet aggregation weights and does
            not truncate this evidence. Color identifies vehicle class; the separate
            line-type legend identifies the production dynamic floor and 1990+ sensitivity.
            """),
            chart_ui(exposure_support_chart),
            chart_ui(vintage_support_chart),
            mo.ui.table(
                scope_curves.sort_values(
                    ["aggregation_scope", "vehicle_class", "age"]
                ),
                selection=None,
                pagination=True,
                page_size=10,
                format_mapping={
                    "annual_retirement_rate": "{:.2%}",
                    "annual_survival_factor": "{:.2%}",
                    "cumulative_survival": "{:.2%}",
                    "cumulative_scrappage": "{:.2%}",
                },
                label="Latest-survivor dynamic-floor and 1990+ class-age output",
            ),
        ])
        return scope_comparison_output

    _build_view()
    return


if __name__ == "__main__":
    app.run()
