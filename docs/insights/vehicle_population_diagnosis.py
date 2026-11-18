"""Ontario vehicle-population mapping and parameter diagnostic."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from validation.notebook_figures import source_markdown
    import altair as alt
    from validation.notebook_figures import identified_chart
    import marimo as mo
    import pandas as pd

    from validation.notebook_evidence import prepare_command
    from validation.vehicle_stock_diagnostic import load_stock_diagnostic

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


    def chart_ui(chart: alt.TopLevelMixin, figure_id: str, title: str) -> mo.Html:
        """Render an Altair chart without marimo selection callbacks."""
        return mo.ui.altair_chart(
            identified_chart(chart, figure_id, title),
            chart_selection=False,
            legend_selection=False,
        )

    return (
        NLR_ORDER,
        NRCAN_CLASS_ORDER,
        STATUS_COLORS,
        STATUS_ORDER,
        alt,
        chart_ui,
        load_stock_diagnostic,
        mo,
        pd,
        prepare_command, source_markdown)


@app.cell
def introduction(mo, prepare_command):
    mo.vstack(
        [
            mo.md("""
                  # What can Ontario vehicle registrations support in the model?

                  Use reviewed vehicle identities to examine **mapping coverage → fleet composition
                  → apparent retention**. These comparisons show where class and age weights have
                  support, where mapping leaves gaps, and why registration changes alone do not
                  establish physical retirement.

                  | Question | Evidence |
                  |---|---|
                  | Which vehicles can be identified? | Reviewed identities, source links, mapped fit-active stock and unresolved reasons. |
                  | What composition does that support? | Class/model-year shares and comparisons with broader Report 5, Report 4, Wards and NRCan CEUD populations. |
                  | What can consecutive editions say about retirement? | Raw cohort changes, pooled apparent retention, support checks and comparison with accepted survival schedules. |

                  `FIT_ACTIVE` is the administrative proxy for vehicles available for road use.
                  Mapped shares describe the accepted subset, not automatically the complete fleet.
                  """),
            mo.md("""
                  ### Evidence boundary

                  This notebook reads the latest successful diagnostic build's captured tables,
                  configuration and file hashes. Reload after an updated build. Opening it does not
                  fetch data, run ETL, change reviewed mappings or adopt a diagnostic result.
                  `PASSENGER` and `COMMERCIAL` remain distinct Ontario source categories despite
                  using the same mapping and cohort calculations.
                  """),
            mo.accordion({"Sources, methods and audit ownership": mo.md("""
                  | Evidence | Owner | Boundary |
                  |---|---|---|
                  | Ontario Reports A, 4 and 5 | `fetching.vehicle_population` | A supplies make/model/vintage counts, 4 weight classes and 5 broad Passenger/Commercial age totals. Administrative status is not physical retirement. |
                  | Canonical identities and source links | Reviewed `config/parameters/` tables; `parameterization.vehicle_reference` | Reviewed year/class/identity scopes are preserved. Catalogue powertrains alone do not establish MTO propulsion. |
                  | Mapping and fleet composition | `parameterization.road_aggregation` | Row coverage counts cohorts; stock coverage sums FIT_ACTIVE vehicles. Unmapped stock stays outside the mapped LDV class weights. |
                  | Existing-fleet age weights | `parameterization.road_stocks_and_demands` | Raw comparisons use 2000+ model years. Downstream lifetime cutoffs govern represented cohorts; the maximum-age cutoff applies only with survival curves. |
                  | MHDV weight-class shares | `parameterization.road_fleet_weights` | The scenario selects Report 4 or registered Wards shares for parameter aggregation; their stock/sales populations differ. |
                  | NRCan CEUD comparison | `fetching.nrcan_ceud` | Independent stock/sales benchmark. MTO is a mapped subset; its same-model-year count is only a registration proxy for sales. |
                  | Lifetime schedules and MTO retention | `parameterization.road_lifetimes_survival` | The scenario selects NHTSA CAFE + EIA NEMS or EPA MOVES schedules. MTO diagnostic pools use reviewed classes and series present in the latest snapshot; they do not choose accepted lifetimes. |
                  """)}),
            mo.md(f"""
                  ### Prepare evidence, then reload

                  Run the offline diagnostic preset explicitly:

                  ```text
                  {prepare_command()}
                  ```

                  It records production parameters, complete registered Ontario history, mapping
                  and retention audits, survival schedules and their configuration together.
                  """),
            mo.md("""
                  ### What counts as an accepted mapping?

                  Production and this diagnosis apply the reviewed source links and their model-year
                  intervals. The separate mapping review command can generate label candidates for
                  human review. NRCan Ratings and FuelEconomy.gov retain their separate catalogue
                  coverage.
                  """),
        ]
    )
    return


@app.cell
def _(mo):
    reload_evidence = mo.ui.run_button(label="Reload latest completed build")
    reload_evidence
    return (reload_evidence,)


@app.cell
def captured_vehicle_evidence(load_stock_diagnostic, reload_evidence):
    reload_evidence.value
    evidence = load_stock_diagnostic()
    for _key in ["mapped_fleet", "mapping_config", "vehicle_class_evidence"]:
        _frame = evidence[_key].copy()
        if _key == "mapping_config" and "entry_type" not in _frame:
            _frame["entry_type"] = "mto_crosswalk"
        for _field in ["make", "model"]:
            _raw = f"raw_canonical_{_field}"
            if _raw not in _frame:
                _frame[_raw] = _frame[f"canonical_{_field}"]
        evidence[_key] = _frame
    return (evidence,)


@app.cell(hide_code=True)
def _(evidence, mo):
    mo.vstack(
        [
            mo.md(f"""
                  **Evidence build:** `{evidence['build_metadata']['build_id']}` ·
                  completed {evidence['build_metadata']['built_at']}. The switches below belong to
                  this capture. `{evidence['build_metadata']['database']['path']}` identifies its
                  successful database publication; charts use the captured ETL tables rather than
                  querying that database.
                  """),
            mo.ui.table(
                evidence["scenario_switches"],
                selection=None,
                pagination=False,
                page_size=10,
                label="Recorded build switches",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def canonical_reference_controls(evidence, mo):
    reference_canonical = evidence["vehicle_reference_canonical"]
    _link_decisions = evidence["vehicle_reference_links"]
    _identity_gaps = int(_link_decisions.vehicle_id.isna().sum())
    _configuration_gaps = int(_link_decisions.link_status.eq("configuration_unresolved").sum())
    reference_years = sorted({year for row in reference_canonical.itertuples(index=False)
                              for year in range(int(row.model_year_from), int(row.model_year_to) + 1)}, reverse=True)
    reference_make = mo.ui.dropdown(
        options=["All makes", *sorted(reference_canonical.canonical_make.unique())],
        value="All makes", label="Reference make",
    )
    reference_year = mo.ui.dropdown(
        options={"All model years": None, **{str(year): year for year in reference_years}},
        value=str(reference_years[0]) if reference_years else "All model years", label="Reference model year",
    )
    reference_powertrain = mo.ui.dropdown(
        options={"All powertrains": None, "Gasoline": "gasoline", "Diesel": "diesel", "HEV": "hev", "PHEV": "phev", "BEV": "bev", "Unresolved": "unknown"},
        value="All powertrains", label="Reference powertrain",
    )
    mo.vstack([
        mo.md(f"""
              ## Which identities and configurations have reviewed support?

              Each canonical row records an identity with supported years, class and powertrain
              scope, including families without MTO codes. The two tables are captured reviewed
              inputs; normal builds do not rewrite them. Eligible target-year configurations
              supply consumption and electric-range calculations in the efficiency diagnostic.

              Across source links, **{_identity_gaps:,} intervals lack an accepted identity** and
              **{_configuration_gaps:,} retain an identity but unresolved configuration**.
              A supported stock size class can therefore remain usable even when consumption
              matching needs more specific evidence.
              """),
        mo.hstack([reference_make, reference_year, reference_powertrain]),
    ])
    return reference_make, reference_powertrain, reference_year


@app.cell(hide_code=True)
def canonical_reference_audit_controls(
    evidence,
    mo,
    reference_make,
    reference_powertrain,
    reference_year,
):
    audit_choices = evidence["vehicle_reference_canonical"].copy()
    if reference_make.value != "All makes":
        audit_choices = audit_choices.loc[audit_choices.canonical_make.eq(reference_make.value)]
    if reference_year.value is not None:
        audit_choices = audit_choices.loc[audit_choices.model_year_from.le(reference_year.value)
                                          & audit_choices.model_year_to.ge(reference_year.value)]
    if reference_powertrain.value is not None:
        audit_choices = audit_choices.loc[audit_choices.powertrain.eq(reference_powertrain.value)]
    audit_choices = audit_choices.drop_duplicates("vehicle_id").sort_values(["canonical_make", "canonical_model"])
    reference_audit_family = mo.ui.dropdown(
        options={"Select an identity for source details": None, **{
            f"{row.canonical_make} / {row.canonical_model}": row.vehicle_id for row in audit_choices.itertuples(index=False)
        }}, value="Select an identity for source details", label="Source linkage and decision audit",
    )
    reference_audit_family
    return (reference_audit_family,)


@app.cell(hide_code=True)
def canonical_reference_inventory(
    evidence,
    mo,
    reference_audit_family,
    reference_make,
    reference_powertrain,
    reference_year,
):
    reference_view = evidence["vehicle_reference_canonical"].copy()
    if reference_make.value != "All makes":
        reference_view = reference_view.loc[reference_view.canonical_make.eq(reference_make.value)]
    if reference_year.value is not None:
        reference_view = reference_view.loc[reference_view.model_year_from.le(reference_year.value)
                                            & reference_view.model_year_to.ge(reference_year.value)]
    if reference_powertrain.value is not None:
        reference_view = reference_view.loc[reference_view.powertrain.eq(reference_powertrain.value)]
    reference_ids = {reference_audit_family.value} if reference_audit_family.value is not None else set()
    columns = {
        "canonical_make": "Make", "canonical_model": "Canonical model",
        "model_year_from": "First model year", "model_year_to": "Last model year",
        "vehicle_scope": "Scope", "nrcan_vehicle_class": "NRCan size class",
        "powertrain": "Supported powertrain", "evidence_status": "Evidence status",
    }
    displayed = reference_view[[column for column in columns if column in reference_view]].copy()
    if "powertrain" in displayed:
        displayed["powertrain"] = displayed.powertrain.map({"gasoline": "Gasoline", "diesel": "Diesel", "hev": "HEV", "phev": "PHEV", "bev": "BEV"}).fillna("Unresolved")
    if "evidence_status" in displayed:
        displayed["evidence_status"] = displayed.evidence_status.map({
            "catalogue_supported": "Catalogue-supported", "propulsion_unresolved": "Propulsion unresolved",
            "reviewed_identity": "Reviewed identity", "source_reported": "Source-reported",
            "model_not_produced": "Model not produced in this interval",
        })
    displayed = displayed.replace(r"^\s*$", None, regex=True).dropna(axis=1, how="all").rename(columns=columns)
    notes_columns = [column for column in ["canonical_make", "canonical_model", "model_year_from", "model_year_to", "evidence_source", "notes"] if column in reference_view]
    reference_notes = reference_view.loc[reference_view.vehicle_id.isin(reference_ids), notes_columns].replace(r"^\s*$", None, regex=True).dropna(axis=1, how="all")
    reference_links = evidence["vehicle_reference_links"]
    reference_links = reference_links.loc[reference_links.vehicle_id.isin(reference_ids)].copy()
    if reference_year.value is not None:
        reference_links = reference_links.loc[
            reference_links.model_year_from.le(reference_year.value) & reference_links.model_year_to.ge(reference_year.value)
        ]
    mo.vstack([
        mo.ui.table(displayed, selection=None, pagination=True, page_size=15, label="Reviewed canonical identity / year / class / powertrain scopes"),
        mo.accordion({
            "Reviewed decisions by source": mo.ui.table(reference_notes, selection=None, pagination=True, page_size=10),
            "Source linkage audit — intervals, not composition weights": mo.ui.table(reference_links.replace(r"^\s*$", None, regex=True).dropna(axis=1, how="all"), selection=None, pagination=True, page_size=10),
        }),
    ])
    return


@app.cell(hide_code=True)
def mapping_quality_section(mo):
    mo.md("""
    ## 1. Reviewed mapping evidence and unresolved stock
    """)
    return


@app.cell(hide_code=True)
def registration_status_heading(mo):
    mo.md("""
    ### Registration-status context
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
                      What does each registration status represent? Percentage bars partition all fetched
                      Report A editions into eight administrative statuses, using the same order and
                      colors for Passenger and Commercial. The dictionary defines the codes.

                      Only `FIT_ACTIVE` supplies the stock proxy. The other statuses provide context;
                      changes between their totals cannot track individual vehicles or isolate retirement
                      from migration, registration changes or recoding.
                      """),
                mo.ui.tabs(
                    {
                        "Passenger": chart_ui(registration_share_chart("PASSENGER"), 'population-1-passenger', 'Passenger registration statuses across report years'),
                        "Commercial": chart_ui(
                            registration_share_chart("COMMERCIAL")
                        , 'population-1-commercial', 'Commercial registration statuses across report years'),
                    }
                ),
                mo.md("""
                      The heat maps show the latest edition's **vehicle counts** by status and model year
                      (2000+). A square-root color scale makes small status groups visible beside
                      FIT_ACTIVE stock; it does not change their counts.
                      """),
                chart_ui(registration_heatmap("PASSENGER"), 'population-2-passenger', 'Passenger stock by status and model year'),
                chart_ui(registration_heatmap("COMMERCIAL"), 'population-2-commercial', 'Commercial stock by status and model year'),
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
    ### Reviewed mapping coverage
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
        mapping_row_source = evidence["mapping_history"].copy()
        mapping_reason_labels = {
            "reviewed_identity_unresolved": "Reviewed model identity unresolved",
            "reviewed_family_class_unresolved": "Family known; size class unresolved",
            "reviewed_family_scope_unresolved": "Family known; vehicle scope unresolved",
            "weak_model_label_agreement": "Weak model-label agreement",
            "ambiguous_top_candidate": "Ambiguous best candidate",
            "no_normalized_make_agreement": "No normalized make agreement",
            "suppressed_or_unknown_code": "Suppressed or unknown source code",
            "no_model_label_candidate": "No model-family candidate",
            "no_reviewed_source_link": "No reviewed source link",
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Label candidate; identity/class unreviewed",
            "no_applicable_reviewed_interval": "No applicable reviewed year interval",
        }
        mapping_row_source["row_reason"] = mapping_row_source["unresolved_reason"].fillna("no_reviewed_source_link")
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
                x=alt.X("share:Q", title="Share of NRCan/EPA class-evidence families", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                y=alt.Y("nlr_atb_class:N", title=None, sort=NLR_ORDER, axis=alt.Axis(labelLimit=150)),
                color=alt.Color("family_status:N", title="Family use", scale=alt.Scale(domain=["Matched to an MTO make-model key", "Unused normalized evidence"], range=["#457b9d", "#d9d9d9"])),
                order=alt.Order("family_status:N", sort="descending"),
                tooltip=["nlr_atb_class:N", "family_status:N", alt.Tooltip("normalized_family_count:Q", format=","), alt.Tooltip("share:Q", format=".2%")],
            )
            .properties(width=700, height=190, title="NRCan/EPA class-evidence families by NLR ATB class")
        )

        passenger_stock = coverage.loc[coverage["vehicle_class"].eq("PASSENGER") & coverage["measure"].eq("fit_active_stock")].iloc[0]
        commercial_stock = coverage.loc[coverage["vehicle_class"].eq("COMMERCIAL") & coverage["measure"].eq("fit_active_stock")].iloc[0]
        crosswalk_count = int(mapping_config["entry_type"].eq("mto_crosswalk").sum())
        reference_mto_links = evidence["vehicle_reference_links"]
        reference_mto_links = reference_mto_links.loc[reference_mto_links.source.eq("ontario_ministry_transport_vehicle_population")]
        used_family_count = reference_mto_links.vehicle_id.dropna().nunique()
        mapping_progress_output = mo.vstack([
            mo.md("""
                  How much fleet evidence does the reviewed map cover? A Report A row is one
                  make/model/model-year cohort; stock coverage weights those rows by FIT_ACTIVE
                  vehicle count. Row coverage and stock coverage answer different questions.

                  The NRCan Ratings/EPA chart supplies auxiliary class evidence. The identity total
                  counts distinct canonical IDs linked to MTO across reviewed scopes, including
                  unrated identities; it is not a count of source-link intervals.
                  """),
            mo.hstack([
                mo.stat(f"{passenger_stock['mapped_ldv_share']:.2%}", "Passenger LDV fit-active coverage", f"{int(passenger_stock['mapped_ldv']):,} LDV; {int(passenger_stock['mapped_non_ldv']):,} non-LDV", bordered=True),
                mo.stat(f"{commercial_stock['mapped_ldv_share']:.2%}", "Commercial LDV fit-active coverage", f"{int(commercial_stock['mapped_ldv']):,} LDV; {int(commercial_stock['mapped_non_ldv']):,} non-LDV", bordered=True),
                mo.stat(f"{used_family_count:,}", "Unique canonical families linked", f"Across {crosswalk_count:,} reviewed MTO intervals", bordered=True),
            ], widths="equal"),
            mo.ui.tabs({
                "Passenger row coverage": chart_ui(edition_row_coverage_chart("PASSENGER"), 'population-3-passenger', 'Reviewed passenger mapping coverage across editions'),
                "Commercial row coverage": chart_ui(edition_row_coverage_chart("COMMERCIAL"), 'population-3-commercial', 'Reviewed commercial mapping coverage across editions'),
            }),

            mo.md("""
                  Each row chart partitions an edition into mapped LDVs, mapped non-LDVs, the four
                  largest unresolved reasons and an Other remainder. These are **row shares**, not
                  vehicle shares. Non-LDVs remain identified but do not enter LDV weights or survival
                  pools.

                  “Outside 1981+ scope” means ineligible for this transition-age window, not unreliable
                  mapping. The first eligible transition starts in 2016 and the maximum starting age
                  is 35, giving a 1981 floor. Older vintages are kept separate from unresolved coverage.
                  """),

            mo.md("""
                  A reviewed link covers one make/model code and year interval; several codes may
                  identify one family. The chart deduplicates the combined NRCan Ratings/FuelEconomy.gov
                  catalogue by canonical make, model and NLR ATB class, then checks whether any accepted
                  MTO code links to that family.
                  """),
            chart_ui(family_use_chart, 'population-4', 'How canonical family mappings support parameterization'),

            mo.md("""
                  ## How does coverage vary by model year and edition?

                  Bars compare mapped FIT_ACTIVE stock with raw FIT_ACTIVE totals for exact model
                  years in each edition. Grouping retains separate snapshots rather than averaging
                  them. Model years run from 2000 to the edition year; row charts apply the reviewed
                  keys directly to each edition.

                  This 2000+ fleet-composition view differs from the dynamic 1981 mapping floor and
                  age-based survival window. Coverage limits must follow the view being interpreted.
                  """),
            mo.vstack([
                chart_ui(mapping_model_year_chart("PASSENGER"), 'population-5-passenger', 'Passenger mapping coverage by model year'),
                chart_ui(mapping_model_year_chart("COMMERCIAL"), 'population-5-commercial', 'Commercial mapping coverage by model year'),
            ]),

            mo.md("""
                  **Unused normalized evidence** means a catalogue family has no reviewed MTO link;
                  its source rows may still serve other purposes. Reviewed model-year, class and
                  powertrain distinctions remain explicit where the reference supports them.
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
    ### Why reviewed coverage remains incomplete
    """)
    return


@app.cell
def unmapped_reasons(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        unmapped_reason_summary = evidence["unresolved_reason_summary"].copy()
        for _column in ["unresolved_rows", "unresolved_fit_active_stock", "share_of_unresolved_rows", "share_of_unresolved_stock", "share_of_total_rows", "share_of_total_stock"]:
            unmapped_reason_summary[_column] = pd.to_numeric(unmapped_reason_summary[_column], errors="coerce")
        reason_labels = {
            "reviewed_identity_unresolved": "Reviewed model identity unresolved",
            "reviewed_family_class_unresolved": "Family known; size class unresolved",
            "reviewed_family_scope_unresolved": "Family known; vehicle scope unresolved",
            "weak_model_label_agreement": "Weak model-label agreement",
            "ambiguous_top_candidate": "Ambiguous best candidate",
            "no_normalized_make_agreement": "No normalized make agreement",
            "suppressed_or_unknown_code": "Suppressed or unknown source code",
            "no_model_label_candidate": "No model-family candidate",
            "no_reviewed_source_link": "No reviewed source link",
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Label candidate; identity/class unreviewed",
            "no_applicable_reviewed_interval": "No applicable reviewed year interval",
        }
        unmapped_reason_summary["reason_label"] = unmapped_reason_summary["unresolved_reason"].map(reason_labels).fillna(unmapped_reason_summary["unresolved_reason"])
        observed_reasons = set(evidence["unresolved_history"]["unresolved_reason"].dropna())
        palette = ["#b2182b", "#ef8a62", "#fddbc7", "#457b9d", "#e76f51", "#f4a261", "#8d99ae", "#90be6d", "#277a70", "#577590", "#9b5de5", "#6c757d"]
        legend_colors = {label: color for (code, label), color in zip(reason_labels.items(), palette, strict=True)
                         if code in observed_reasons}
        reason_order = list(legend_colors)
        unmapped_reason_summary["reason_order"] = unmapped_reason_summary[
            "reason_label"
        ].map({reason: index for index, reason in enumerate(reason_order)})
        unmapped_edition_source = evidence["unresolved_history"].rename(columns={"FIT_ACTIVE": "native_count"}).copy()
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
                        scale=alt.Scale(domain=reason_order, range=list(legend_colors.values())),
                        legend=alt.Legend(orient="bottom", direction="horizontal", columns=2, labelLimit=340),
                    ),
                    order=alt.Order("reason_order:Q"),
                    tooltip=["VEHICLE_CLASS:N", "report_year:O", alt.Tooltip("reason_label:N", title="Reason"), alt.Tooltip("unresolved_fit_active_stock:Q", format=",", title="Unmapped vehicles"), alt.Tooltip("share_of_unresolved_stock:Q", format=".2%", title="Share of edition's unmapped stock")],
                )
                .properties(width=700, height=260, title=f"{vehicle_class.title()} unresolved stock by reason and edition")
            )


        unmapped_reasons_output = mo.vstack([
            mo.md("""
                  Why does stock remain unresolved? Each bar partitions one category/edition's
                  unmapped FIT_ACTIVE vehicles, so its own denominator sums to 100%. Tooltips retain
                  absolute counts. The reviewed links distinguish unresolved identity, size class,
                  vehicle scope and model-year coverage. A missing link means the captured review
                  provides no decision for that source key and model year. Labels can also be masked
                  or unknown. Historical keys absent from the latest registered edition remain in
                  raw transitions. The primary pooled curves separately require latest-edition presence.
                  Older captures can contain additional candidate-based review hints.
                  """),
            mo.vstack([
                chart_ui(unmapped_share_chart("PASSENGER"), 'population-6-passenger', 'Passenger stock without a resolved class mapping'),
                chart_ui(unmapped_share_chart("COMMERCIAL"), 'population-6-commercial', 'Commercial stock without a resolved class mapping'),
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
    ### Highest-stock unresolved make-model keys
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd, reason_labels):
    def _build_view():
        unresolved_detail = evidence["unresolved_reason_detail"].copy()
        unresolved_detail["FIT_ACTIVE"] = pd.to_numeric(unresolved_detail["FIT_ACTIVE"], errors="coerce")
        unresolved_detail["MODEL_YEAR"] = pd.to_numeric(unresolved_detail["MODEL_YEAR"], errors="coerce").astype("Int64")
        unresolved_detail["candidate_model_similarity"] = pd.to_numeric(unresolved_detail["candidate_model_similarity"], errors="coerce")
        current_reason_stock = unresolved_detail.groupby("unresolved_reason", as_index=False)["FIT_ACTIVE"].sum()
        expired_rows = evidence["unresolved_history"].loc[
            evidence["unresolved_history"].unresolved_reason.eq("not_present_in_latest_snapshot")
            & evidence["unresolved_history"].FIT_ACTIVE.gt(0)
        ].copy()
        unresolved_detail = pd.concat(
            [unresolved_detail, expired_rows.reindex(columns=unresolved_detail.columns)],
            ignore_index=True,
        )
        unresolved_detail["mapping_key"] = unresolved_detail["VEHICLE_CLASS"].astype(str) + " / " + unresolved_detail["MAKE"].astype(str) + "/" + unresolved_detail["MODEL"].astype(str)
        top_reason_codes = (
            current_reason_stock.nlargest(3, "FIT_ACTIVE")["unresolved_reason"]
            .tolist()
        )
        no_make_agreement_code = "no_normalized_make_agreement"
        if no_make_agreement_code in current_reason_stock.unresolved_reason.values and no_make_agreement_code not in top_reason_codes:
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
            key_columns = ["VEHICLE_CLASS", "MAKE", "MODEL", "mapping_key", "unresolved_reason"]
            key_totals = (
                reason_rows.groupby(key_columns, as_index=False, dropna=False)
                .agg(
                    fit_active_stock=("FIT_ACTIVE", "sum"),
                    model_year_from=("MODEL_YEAR", "min"),
                    model_year_to=("MODEL_YEAR", "max"),
                    observed_model_years=("MODEL_YEAR", "nunique"),
                    unresolved_reason_detail=("unresolved_reason_detail", first_present),
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
                .merge(key_totals[["mapping_key", "fit_active_stock"]], on="mapping_key", how="inner", validate="many_to_one")
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
                visible_columns = ["VEHICLE_CLASS", "MAKE", "MODEL", "fit_active_stock", "model_year_from", "model_year_to", "observed_model_years", "candidate_canonical_make", "candidate_canonical_model", "candidate_nrcan_vehicle_class", "unresolved_reason_detail"]
            key_totals = key_totals.dropna(axis=1, how="all")
            visible_columns = [column for column in visible_columns if column in key_totals]
            unresolved_tabs[reason_labels.get(reason_code, reason_code)] = mo.vstack([
                chart_ui(reason_chart, f"population-7-{reason_code.replace('_', '-')}",
                         f"Unresolved stock by model year: {reason_labels.get(reason_code, reason_code)}"),
                mo.ui.table(key_totals, selection=None, pagination=True, page_size=10, visible_columns=visible_columns, freeze_columns_left=["VEHICLE_CLASS", "MAKE", "MODEL"], wrapped_columns=[column for column in ["unresolved_reason_detail", "rating_model_labels"] if column in visible_columns], label=f"Top 20 unresolved make-model keys: {reason_labels.get(reason_code, reason_code)}"),
            ])

        highest_unresolved_output = mo.vstack([
            mo.md("""
                  Which unresolved identities affect the most stock? Bars rank make/model keys within
                  each source category and reason, split by model year. Current-stock tables and the
                  historical-only tab's exposure summed across editions use different populations;
                  do not add those totals.

                  Tables retain full labels, row reasons and exact-year decisions from the reviewed
                  links. Where an earlier capture includes candidate scores, they are advisory.
                  Historical keys absent from the latest edition remain
                  in raw transitions but do not enter primary latest-present survival pools.

                  Every observed model year remains in the chart; the compact legend labels only
                  five-year intervals from 1980 onward.
                  """),
            mo.ui.tabs(unresolved_tabs),
        ])
        return highest_unresolved_output

    _build_view()
    return


@app.cell(hide_code=True)
def fleet_representation_section(mo):
    mo.md("""
    ## 2. Fleet composition and source comparisons
    """)
    return


@app.cell(hide_code=True)
def mapped_composition_heading(mo):
    mo.md("""
    ### Mapped class composition across editions
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
                y=alt.Y(
                    "fit_active_stock:Q",
                    title="Mapped FIT_ACTIVE stock",
                    stack="zero",
                    axis=alt.Axis(format="~s"),
                ),
                color=alt.Color("nlr_atb_class:N", title="NLR ATB class", sort=latest_nlr_order),
                tooltip=[
                    "model_year:O",
                    "nlr_atb_class:N",
                    alt.Tooltip("fit_active_stock:Q", format=",", title="Mapped FIT_ACTIVE stock"),
                    alt.Tooltip("share_of_2000_plus_mapped_stock:Q", format=".2%", title="Share of mapped 2000+ stock"),
                ],
            )
            .properties(width=760, height=300, title=f"{latest_mapped_year} mapped fleet: 2000-{latest_eligible_vintage} vintage stock")
        )


        mapped_composition_output = mo.vstack([
            mo.md("""
                  What class mix does the mapped stock support? Each bar partitions one Report A
                  edition's accepted FIT_ACTIVE vehicles into NRCan Ratings classes. Car and Light
                  Truck are separate because their class labels differ. Shares are conditional on
                  the reviewed map: a change can reflect fleet evolution or changing mapping coverage.
                  Unresolved stock is not redistributed across mapped classes.

                  Class order and colors stay fixed from smaller to larger sizes within each body-type
                  family. Two-seaters and vans remain separate source categories.
                  """),
            chart_ui(edition_composition_chart("Car"), 'population-8-cars', 'Car size composition across observed editions'),
            chart_ui(edition_composition_chart("Light Truck"), 'population-8-light-trucks', 'Light-truck size composition across observed editions'),
            mo.md(f"""
                  The area chart shows the mapped fleet in the scenario's selected Report A edition
                  ({latest_mapped_year}), by model year from 2000 through {latest_eligible_vintage}.
                  The NLR ATB class areas add to the mapped **vehicle count** in those model years.
                  """),
            chart_ui(latest_vintage_area, 'population-9', 'Mapped fleet composition by model year'),
            mo.ui.table(edition_ratings_composition.sort_values(["nrcan_ceud_class", "report_year", "within_edition_ceud_share"], ascending=[True, True, False]), selection=None, pagination=True, page_size=10, format_mapping={"within_edition_ceud_share": "{:.2%}"}),
        ])
        return mapped_composition_output

    _build_view()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Latest mapped fleet by model year
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
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Car", class_column, legend_title), f"population-10-cars-{class_column.split('_')[0]}", 'Car composition: registrations versus sales reference'),
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Light Truck", class_column, legend_title), f"population-10-light-trucks-{class_column.split('_')[0]}", 'Light-truck composition: registrations versus sales reference'),
                mo.ui.tabs({
                    "MTO selected-edition stock table": mo.ui.table(empirical.sort_values(["nrcan_ceud_class", class_column, "model_year"]), selection=None, pagination=True, page_size=10, format_mapping={"within_model_year_share": "{:.2%}"}),
                    "Wards 2018/2021 table": mo.ui.table(wards_frame.sort_values(["nrcan_ceud_class", "year", class_column]), selection=None, pagination=False, page_size=10, format_mapping={"market_share": "{:.2%}"}),
                }),
            ])


        age_distribution_output = mo.vstack([
            mo.md("""
                  How is the selected edition's mapped fleet distributed over model years? Areas show accepted
                  FIT_ACTIVE vehicles from the latest Report A, for model years 2000 to the report
                  year. Neither the scenario median-lifetime cutoff nor maximum age filters this
                  view. The 2000 floor is specific to fleet representation; survival evidence uses
                  a different eligibility window. Unmapped LDVs are not allocated to these classes.

                  The stock axis scales to vehicle counts. The separate Wards bars show Canadian
                  **new-sales shares** in 2018 and 2021 on their labelled 0–100% axis. Geography,
                  retirement, migration, mapping coverage and sales history all differ from Ontario
                  surviving stock. These bars are context, not inputs or independent validation of
                  empirical fleet composition.
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
def _(mo):
    mo.md(r"""
    ### Highest-stock mapped families
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
        top_source["original_make_model"] = (
            top_source["raw_canonical_make"].astype(str)
            + " " + top_source["raw_canonical_model"].astype(str)
        )
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
        corrected_families = top_source.loc[
            top_source["original_make_model"].ne(top_source["normalized_make_model"]),
            "normalized_make_model",
        ].unique()
        identity_merge_evidence = (
            top_source.loc[top_source["normalized_make_model"].isin(corrected_families)]
            .groupby(
                ["nrcan_ceud_class", "normalized_make_model", "original_make_model"],
                as_index=False,
            )
            .agg(
                original_label_stock=("FIT_ACTIVE", "sum"),
                original_mto_keys=("mto_key", lambda values: " | ".join(sorted(set(values)))),
                first_model_year=("MODEL_YEAR", "min"),
                last_model_year=("MODEL_YEAR", "max"),
            )
            .merge(
                top_family_totals[["nrcan_ceud_class", "normalized_make_model", "family_fit_active_stock"]],
                on=["nrcan_ceud_class", "normalized_make_model"],
                how="left",
                validate="many_to_one",
            )
        )
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
                "evidence_source",
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
                  Which mapped families dominate the fleet? Each heat-map cell counts FIT_ACTIVE
                  vehicles for one normalized family and exact model year (1990 onward). Families
                  are ordered by their total over displayed years; totals appear in the table rather
                  than being repeated as annual observations. These rankings describe surviving
                  mapped Ontario stock across propulsion types.

                  Using those counts for consumption needs a narrower test. Gasoline weights require
                  documented identities covering the configured share of uniquely gasoline-resolved
                  stock in that year/size class; unsupported cohorts use catalogue medians. Report A
                  does not directly observe propulsion or trim. PEV weights instead use eligible iZEV
                  purchase/lease records with supported identities, then compatible configurations.

                  Tooltips and tables retain abbreviated MTO keys; the axis uses canonical family
                  names for readability. **Source identity details** preserves labels, keys, counts
                  and year ranges. A supported stock class alone does not establish a consumption
                  configuration; inspect the canonical-reference audit for that distinction.
                  """),
            mo.vstack([
                chart_ui(top_family_heatmap("Car"), 'population-11-cars', 'Canonical car families represented in observed stock'),
                chart_ui(top_family_heatmap("Light Truck"), 'population-11-light-trucks', 'Canonical light-truck families represented in observed stock'),
            ]),
            mo.md("""
                  A blank cell means no accepted FIT_ACTIVE observation for that family/year in the
                  latest Report A. Check reviewed source-link intervals before calling it a rejected
                  year or an applicability gap.
                  """),
            mo.ui.tabs({
                "Family totals": mo.ui.table(top_20_families.sort_values(["nrcan_ceud_class", "family_rank"]), selection=None, pagination=True, page_size=10, freeze_columns_left=["nrcan_ceud_class", "normalized_make_model"]),
                "Exact model-year stock": mo.ui.table(top_heatmap_data.sort_values(["nrcan_ceud_class", "family_rank", "model_year"]), selection=None, pagination=True, page_size=10, visible_columns=["nrcan_ceud_class", "normalized_make_model", "matched_mto_keys", "model_year", "fit_active_stock", "nrcan_vehicle_class", "nlr_atb_class"], freeze_columns_left=["nrcan_ceud_class", "normalized_make_model"]),
                "Crosswalk intervals": mo.ui.table(top_crosswalk_interval_table, selection=None, pagination=True, page_size=10, label="Accepted crosswalk intervals for top mapped families"),
                "Source identity details": mo.ui.table(identity_merge_evidence, selection=None, pagination=True, page_size=10, label="Captured source labels and conserved family stock"),
            }),
        ])
        return top_mapped_output

    _build_view()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Mapped-family samples by stock percentile
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
                      What do lower-volume mappings look like? Each heat map samples ten Car and ten
                      Light Truck families from one of the first three within-class stock quartiles.
                      Rows are ordered by family stock. Re-running this cell reshuffles the samples;
                      they illustrate mappings rather than estimate population shares.

                      Matched MTO codes remain in tooltips/tables so the model-year axis stays legible.
                      """),
                mo.vstack(
                    [
                        chart_ui(percentile_family_heatmap(bucket), f"population-12-{bucket}", 'Canonical families across stock-size groups')
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
def fleet_age_source_comparison_heading(mo):
    mo.md("""
    ### Age-distribution sensitivity: Report A versus Report 5
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        report_a = evidence["mapped_fleet"].copy()
        for column in ["report_year", "MODEL_YEAR", "FIT_ACTIVE"]:
            report_a[column] = pd.to_numeric(report_a[column], errors="coerce")
        latest_report_a_year = int(report_a["report_year"].max())
        report_a = report_a.loc[
            report_a["report_year"].eq(latest_report_a_year)
            & report_a["mapping_accepted"].astype(str).str.casefold().eq("true")
            & report_a["nrcan_ceud_class"].isin(["Car", "Light Truck"])
            & report_a["MODEL_YEAR"].between(2000, latest_report_a_year)
            & report_a["FIT_ACTIVE"].gt(0)
        ].copy()
        report_a["age"] = latest_report_a_year - report_a["MODEL_YEAR"]
        report_a_distribution = (
            report_a.groupby(
                ["nrcan_ceud_class", "MODEL_YEAR", "age"], as_index=False
            )["FIT_ACTIVE"]
            .sum()
            .rename(
                columns={
                    "nrcan_ceud_class": "source_class",
                    "MODEL_YEAR": "model_year",
                    "FIT_ACTIVE": "fit_active_stock",
                }
            )
        )
        report_a_distribution["age_share"] = report_a_distribution[
            "fit_active_stock"
        ] / report_a_distribution.groupby("source_class")[
            "fit_active_stock"
        ].transform("sum")
        report_a_distribution["report_year"] = latest_report_a_year
        report_a_distribution["data_source"] = "Report A: mapped FIT_ACTIVE"
        report_a_distribution["series"] = report_a_distribution[
            "source_class"
        ].map(
            {
                "Car": "Report A: Car",
                "Light Truck": "Report A: Light Truck",
            }
        )
        report_a_distribution["comparison"] = report_a_distribution[
            "source_class"
        ].map(
            {
                "Car": "Car versus Passenger",
                "Light Truck": "Light Truck versus Commercial",
            }
        )

        report5 = evidence["report5_distribution"].copy()
        for column in ["year", "AGE", "VALUE", "NATIVE_COUNT"]:
            report5[column] = pd.to_numeric(report5[column], errors="coerce")
        latest_report5_year = int(report5["year"].max())
        report5 = report5.loc[
            report5["year"].eq(latest_report5_year)
            & report5["VEHICLE_CLASS"].isin(["PASSENGER", "COMMERCIAL"])
            & report5["VALUE"].between(2000, latest_report5_year)
            & report5["NATIVE_COUNT"].gt(0)
        ].copy()
        report5_distribution = (
            report5.groupby(
                ["VEHICLE_CLASS", "VALUE", "AGE"], as_index=False
            )["NATIVE_COUNT"]
            .sum()
            .rename(
                columns={
                    "VEHICLE_CLASS": "source_class",
                    "VALUE": "model_year",
                    "AGE": "age",
                    "NATIVE_COUNT": "fit_active_stock",
                }
            )
        )
        report5_distribution["age_share"] = report5_distribution[
            "fit_active_stock"
        ] / report5_distribution.groupby("source_class")[
            "fit_active_stock"
        ].transform("sum")
        report5_distribution["report_year"] = latest_report5_year
        report5_distribution["data_source"] = "Report 5: all FIT_ACTIVE"
        report5_distribution["series"] = report5_distribution[
            "source_class"
        ].map(
            {
                "PASSENGER": "Report 5: Passenger",
                "COMMERCIAL": "Report 5: Commercial",
            }
        )
        report5_distribution["comparison"] = report5_distribution[
            "source_class"
        ].map(
            {
                "PASSENGER": "Car versus Passenger",
                "COMMERCIAL": "Light Truck versus Commercial",
            }
        )

        age_comparison = pd.concat(
            [report_a_distribution, report5_distribution],
            ignore_index=True,
            sort=False,
        )

        def age_distribution_chart(comparison: str) -> alt.Chart:
            rows = age_comparison.loc[
                age_comparison["comparison"].eq(comparison)
            ]
            series_order = (
                ["Report A: Car", "Report 5: Passenger"]
                if comparison == "Car versus Passenger"
                else ["Report A: Light Truck", "Report 5: Commercial"]
            )
            series_encoding = alt.Color(
                "series:N",
                title="Source class",
                sort=series_order,
                scale=alt.Scale(
                    domain=series_order,
                    range=["#457b9d", "#e76f51"],
                ),
                legend=alt.Legend(orient="right"),
            )
            lines = (
                alt.Chart(rows)
                .mark_line(interpolate="linear", strokeWidth=2.5)
                .encode(
                    x=alt.X(
                        "age:Q",
                        title="Vehicle age",
                        axis=alt.Axis(format="d", tickMinStep=1),
                    ),
                    y=alt.Y(
                        "age_share:Q",
                        title="Share of 2000+ class stock",
                        axis=alt.Axis(format="%"),
                    ),
                    color=series_encoding,
                )
            )
            points = (
                alt.Chart(rows)
                .mark_circle(size=50, opacity=0.8)
                .encode(
                    x="age:Q",
                    y="age_share:Q",
                    color=series_encoding,
                    tooltip=[
                        alt.Tooltip("series:N", title="Source class"),
                        alt.Tooltip("report_year:O", title="Snapshot year"),
                        alt.Tooltip("model_year:O", title="Model year"),
                        alt.Tooltip("age:Q", title="Age"),
                        alt.Tooltip(
                            "fit_active_stock:Q",
                            format=",",
                            title="FIT_ACTIVE stock",
                        ),
                        alt.Tooltip(
                            "age_share:Q",
                            format=".2%",
                            title="Within-class share",
                        ),
                    ],
                )
            )
            return (lines + points).properties(
                width=700,
                height=300,
                title=comparison,
            )

        age_comparison_output = mo.vstack(
            [
                mo.md("""
                      Would a broader source imply a different age profile? Both sources use FIT_ACTIVE
                      counts from their latest snapshots, keep 2000+ model years and normalize within
                      the displayed source class. Report A uses reviewed Car/Light Truck mappings;
                      Report 5 uses broader Passenger/Commercial categories. The pairs are approximate,
                      not equivalent populations.

                      This tests the evidence used to distribute NRCan CEUD stock across vintages; it
                      does not replace published weights. Production also applies its cohort cutoff:
                      median-equivalent ages with fixed lifetimes, or `survival_curve_max_age` with curves.
                      These cutoffs limit represented cohorts rather than define individual technology
                      lifetimes. Differences can reflect class boundaries, mapping and source construction
                      as well as fleet age.
                      """),
                chart_ui(age_distribution_chart("Car versus Passenger"), 'population-13-cars', 'Car age evidence and the broader passenger register'),
                chart_ui(
                    age_distribution_chart("Light Truck versus Commercial")
                , 'population-13-light-trucks', 'Light-truck age evidence and the commercial register'),
                mo.ui.table(
                    age_comparison.sort_values(
                        ["comparison", "series", "age"]
                    ),
                    selection=None,
                    pagination=True,
                    page_size=10,
                    format_mapping={"age_share": "{:.2%}"},
                    label="Report A and Report 5 age-distribution audit rows",
                ),
            ]
        )
        return age_comparison_output

    _build_view()
    return


@app.cell(hide_code=True)
def mhdv_weight_source_comparison_heading(mo):
    mo.md("""
    ### MHDV GVWR shares: Report 4 versus [*Wards*](https://wardsintelligence.informa.com/automotive-data)
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd):
    def _build_view():
        weight_class_order = [
            "Class 3",
            "Class 4",
            "Class 5",
            "Class 6",
            "Class 7",
            "Class 8",
        ]
        epa_to_weight_class = {
            "MDV3": "Class 3",
            "MDV4": "Class 4",
            "MDV5": "Class 5",
            "MDV6": "Class 6",
            "MDV7": "Class 7",
            "MDV8": "Class 8",
        }
        target_class = {
            "Class 3": "Medium Trucks",
            "Class 4": "Medium Trucks",
            "Class 5": "Medium Trucks",
            "Class 6": "Medium Trucks",
            "Class 7": "Medium Trucks",
            "Class 8": "Heavy Trucks",
        }

        report4 = evidence["report4_distribution"].copy()
        for column in ["year", "NATIVE_COUNT", "SHARE"]:
            report4[column] = pd.to_numeric(report4[column], errors="coerce")
        report4 = report4.loc[
            report4["EPA_GVWR"].isin(epa_to_weight_class)
        ].copy()
        report4["weight_class"] = report4["EPA_GVWR"].map(
            epa_to_weight_class
        )
        report4["target_vehicle_class"] = report4["weight_class"].map(
            target_class
        )
        report4["source_share"] = report4["NATIVE_COUNT"] / report4.groupby(
            "target_vehicle_class"
        )["NATIVE_COUNT"].transform("sum")
        report4["data_source"] = "MTO Report 4"
        report4 = report4.rename(
            columns={"year": "source_year", "NATIVE_COUNT": "native_count"}
        )

        wards = evidence["wards_comparison"].copy()
        for column in ["year", "market_share"]:
            wards[column] = pd.to_numeric(wards[column], errors="coerce")
        wards = wards.loc[
            wards["vehicle_scope"].eq("mhdv")
            & wards["wards_size_class"].isin(weight_class_order)
        ].copy()
        wards = wards.rename(
            columns={
                "wards_size_class": "weight_class",
                "nrcan_ceud_class": "target_vehicle_class",
                "year": "source_year",
                "market_share": "source_share",
            }
        )
        wards["native_count"] = pd.NA
        wards["EPA_GVWR"] = pd.NA
        wards["data_source"] = "Wards reviewed shares"

        weight_comparison = pd.concat(
            [
                report4[
                    [
                        "target_vehicle_class",
                        "weight_class",
                        "source_year",
                        "source_share",
                        "native_count",
                        "EPA_GVWR",
                        "data_source",
                    ]
                ],
                wards[
                    [
                        "target_vehicle_class",
                        "weight_class",
                        "source_year",
                        "source_share",
                        "native_count",
                        "EPA_GVWR",
                        "data_source",
                    ]
                ],
            ],
            ignore_index=True,
        )
        weight_comparison["weight_class_order"] = weight_comparison[
            "weight_class"
        ].map(
            {weight_class: order for order, weight_class in enumerate(weight_class_order)}
        )

        source_order = ["MTO Report 4", "Wards reviewed shares"]
        source_encoding = alt.Color(
            "data_source:N",
            title="Evidence source",
            sort=source_order,
            scale=alt.Scale(
                domain=source_order,
                range=["#457b9d", "#e76f51"],
            ),
            legend=alt.Legend(orient="right"),
        )

        def weight_share_chart(vehicle_class: str) -> alt.Chart:
            rows = weight_comparison.loc[
                weight_comparison["target_vehicle_class"].eq(vehicle_class)
            ]
            return (
                alt.Chart(rows)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "weight_class:N",
                        title="GVWR class",
                        sort=weight_class_order,
                        axis=alt.Axis(labelAngle=0),
                    ),
                    xOffset=alt.XOffset("data_source:N", sort=source_order),
                    y=alt.Y(
                        "source_share:Q",
                        title="Share within target truck class",
                        axis=alt.Axis(format="%"),
                        scale=alt.Scale(domain=[0, 1]),
                    ),
                    color=source_encoding,
                    tooltip=[
                        alt.Tooltip(
                            "target_vehicle_class:N", title="Target truck class"
                        ),
                        alt.Tooltip("weight_class:N", title="GVWR class"),
                        alt.Tooltip("data_source:N", title="Evidence source"),
                        alt.Tooltip("source_year:O", title="Source year"),
                        alt.Tooltip(
                            "native_count:Q",
                            format=",",
                            title="Report 4 FIT_ACTIVE",
                        ),
                        alt.Tooltip(
                            "source_share:Q",
                            format=".2%",
                            title="Within-class share",
                        ),
                    ],
                )
                .properties(width=700, height=280, title=vehicle_class)
            )

        mhdv_weight_output = mo.vstack(
            [
                mo.md("""
                      How does weight-class evidence affect truck parameter aggregation? Report 4 places
                      latest Ontario Commercial FIT_ACTIVE stock into EPA gross vehicle weight rating
                      (GVWR) bins using each interval's upper kilogram bound converted to pounds. MDV3–8
                      map to Classes 3–8, normalized within Medium Trucks (3–7) and Heavy Trucks (8).
                      LDT1–2, LDT3–4 and MDV2b remain outside this comparison.

                      Wards uses reviewed Canadian sales shares in `vehicle_class_market_shares.csv`.
                      These differ in geography, year and stock/sales basis from Report 4. Their contrast
                      tests aggregation assumptions for NLR ATB MHDV efficiency and costs, rather than
                      validating either source. It does not alter CEUD totals, LDV ages or survival.

                      Only Medium Trucks need a chart: Class 8 is the sole Heavy Truck class in both
                      sources, making its normalized share 100%. The audit retains those Class 8 rows.
                      """),
                chart_ui(weight_share_chart("Medium Trucks"), 'population-14', 'Medium-truck weight classes in registration evidence'),
                # chart_ui(weight_share_chart("Heavy Trucks")),
                mo.ui.table(
                    weight_comparison.sort_values(
                        [
                            "target_vehicle_class",
                            "weight_class_order",
                            "data_source",
                        ]
                    ),
                    selection=None,
                    pagination=True,
                    page_size=10,
                    format_mapping={"source_share": "{:.2%}"},
                    label="Report 4 and Wards MHDV disaggregation audit rows",
                ),
            ]
        )
        return mhdv_weight_output

    _build_view()
    return


@app.cell(hide_code=True)
def mapped_stock_sales_validation_heading(mo):
    mo.md("""
    ### Mapped [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) versus [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) stock and sales
    """)
    return


@app.cell
def _(alt, chart_ui, evidence, mo, pd, source_markdown):
    def _build_view():
        comparison_years = list(range(2016, 2024))
        vehicle_class_order = ["Car", "Light Truck"]
        data_source_order = ["MTO mapped FIT_ACTIVE", "NRCan CEUD"]

        mto_rows = evidence["status_long"].copy()
        for column in ["report_year", "MODEL_YEAR", "native_count"]:
            mto_rows[column] = pd.to_numeric(mto_rows[column], errors="coerce")

        mto_crosswalk = evidence["mapping_config"].loc[
            evidence["mapping_config"]["entry_type"].eq("mto_crosswalk"),
            [
                "mto_make_code",
                "mto_model_code",
                "model_year_from",
                "model_year_to",
                "nrcan_ceud_class",
            ],
        ].copy()
        for column in ["model_year_from", "model_year_to"]:
            mto_crosswalk[column] = pd.to_numeric(
                mto_crosswalk[column], errors="coerce"
            ).astype(int)
        mto_crosswalk = mto_crosswalk.assign(
            MODEL_YEAR=mto_crosswalk.apply(
                lambda row: list(
                    range(int(row["model_year_from"]), int(row["model_year_to"]) + 1)
                ),
                axis=1,
            )
        ).explode("MODEL_YEAR")
        mto_crosswalk["MODEL_YEAR"] = mto_crosswalk["MODEL_YEAR"].astype(int)
        mto_crosswalk = mto_crosswalk.drop_duplicates(
            ["mto_make_code", "mto_model_code", "MODEL_YEAR"]
        )

        mapped_mto_rows = mto_rows.loc[
            mto_rows["stock_status"].eq("FIT_ACTIVE")
            & mto_rows["VEHICLE_CLASS"].isin(["PASSENGER", "COMMERCIAL"])
            & mto_rows["report_year"].isin(comparison_years)
        ].merge(
            mto_crosswalk,
            left_on=["MAKE", "MODEL", "MODEL_YEAR"],
            right_on=["mto_make_code", "mto_model_code", "MODEL_YEAR"],
            how="inner",
            validate="many_to_one",
        )

        mto_stock = (
            mapped_mto_rows.groupby(
                ["report_year", "nrcan_ceud_class"], as_index=False
            )["native_count"]
            .sum()
            .rename(
                columns={
                    "report_year": "year",
                    "nrcan_ceud_class": "vehicle_class",
                    "native_count": "value",
                }
            )
        )
        mto_stock["measure"] = "Stock"
        mto_sales = (
            mapped_mto_rows.loc[
                mapped_mto_rows["MODEL_YEAR"].eq(mapped_mto_rows["report_year"])
            ]
            .groupby(["report_year", "nrcan_ceud_class"], as_index=False)[
                "native_count"
            ]
            .sum()
            .rename(
                columns={
                    "report_year": "year",
                    "nrcan_ceud_class": "vehicle_class",
                    "native_count": "value",
                }
            )
        )
        mto_sales["measure"] = "Sales"
        mto_comparison = pd.concat([mto_stock, mto_sales], ignore_index=True)
        mto_comparison["value_k"] = mto_comparison["value"] / 1_000
        mto_comparison["data_source"] = data_source_order[0]

        ceud_rows = evidence["nrcan_ceud"].copy()
        for column in ["year", "value"]:
            ceud_rows[column] = pd.to_numeric(ceud_rows[column], errors="coerce")
        ceud_comparison = ceud_rows.loc[
            ceud_rows["year"].isin(comparison_years)
            & ceud_rows["series_group"].isin(
                ["Sales (thousands)", "Stock (thousands)"]
            )
            & ceud_rows["series_name"].isin(
                ["Cars", "Passenger Light Trucks", "Freight Light Trucks"]
            )
            & ceud_rows["unit"].eq("thousands")
        ].copy()
        ceud_comparison["vehicle_class"] = ceud_comparison["series_name"].map(
            {
                "Cars": "Car",
                "Passenger Light Trucks": "Light Truck",
                "Freight Light Trucks": "Light Truck",
            }
        )
        ceud_comparison["measure"] = ceud_comparison["series_group"].str.replace(
            " (thousands)", "", regex=False
        )
        ceud_comparison = (
            ceud_comparison.groupby(
                ["year", "vehicle_class", "measure"], as_index=False
            )["value"]
            .sum()
            .rename(columns={"value": "value_k"})
        )
        ceud_comparison["data_source"] = data_source_order[1]

        comparison = pd.concat(
            [
                mto_comparison[
                    ["year", "vehicle_class", "measure", "value_k", "data_source"]
                ],
                ceud_comparison[
                    ["year", "vehicle_class", "measure", "value_k", "data_source"]
                ],
            ],
            ignore_index=True,
        )

        color_encoding = alt.Color(
            "vehicle_class:N",
            title="Vehicle class",
            sort=vehicle_class_order,
            scale=alt.Scale(
                domain=vehicle_class_order,
                range=["#457b9d", "#e76f51"],
            ),
            legend=alt.Legend(orient="right"),
        )
        source_encoding = alt.StrokeDash(
            "data_source:N",
            title="Data source",
            sort=data_source_order,
            scale=alt.Scale(
                domain=data_source_order,
                range=[[1, 0], [7, 4]],
            ),
            legend=alt.Legend(
                orient="right",
                symbolType="stroke",
                symbolStrokeWidth=3,
                symbolSize=320,
            ),
        )

        def comparison_chart(measure: str) -> alt.Chart:
            rows = comparison.loc[comparison["measure"].eq(measure)]
            comparison_lines = (
                alt.Chart(rows)
                .mark_line(interpolate="linear", strokeWidth=2.5)
                .encode(
                    x=alt.X(
                        "year:Q",
                        title="Year",
                        scale=alt.Scale(domain=[2016, 2023], nice=False),
                        axis=alt.Axis(format="d", values=comparison_years),
                    ),
                    y=alt.Y("value_k:Q", title=f"{measure} (k vehicles)"),
                    color=color_encoding,
                    strokeDash=source_encoding,
                )
            )
            comparison_points = (
                alt.Chart(rows)
                .mark_circle(opacity=0.8, size=55)
                .encode(
                    x="year:Q",
                    y="value_k:Q",
                    color=color_encoding,
                    tooltip=[
                        alt.Tooltip("year:O", title="Year"),
                        alt.Tooltip("vehicle_class:N", title="Vehicle class"),
                        alt.Tooltip("data_source:N", title="Data source"),
                        alt.Tooltip(
                            "value_k:Q",
                            format=",.1f",
                            title=f"{measure} (k vehicles)",
                        ),
                    ],
                )
            )
            return (comparison_lines + comparison_points).properties(
                width=700,
                height=300,
                title=f"Ontario {measure.lower()}: mapped MTO versus NRCan CEUD",
            )

        observed_ceud_years = set(
            ceud_comparison.loc[
                ceud_comparison["vehicle_class"].isin(vehicle_class_order), "year"
            ].astype(int)
        )
        missing_ceud_years = sorted(set(comparison_years) - observed_ceud_years)
        coverage_note = (
            mo.md(
                "[*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) covers all requested comparison years."
            )
            if not missing_ceud_years
            else mo.callout(
                mo.md(
                    source_markdown("The normalized NRCan CEUD artifact is missing requested year(s): "
                    + ", ".join(map(str, missing_ceud_years))
                    + ". The MTO lines retain the full 2016–2023 window; refresh the "
                    "configured CEUD source before treating the visual comparison as "
                    "complete.")
                ),
                kind="warn",
            )
        )

        validation_output = mo.vstack(
            [
                mo.md("""
                      Do mapped registrations resemble independent Ontario totals? MTO stock sums all
                      accepted FIT_ACTIVE vehicles in each edition. Its sales proxy keeps only vehicles
                      whose model year equals the edition year. Both are compared with NRCan CEUD Ontario
                      car/light-truck stock and sales, in **thousand vehicles**; light trucks combine
                      passenger and freight services.

                      This is a plausibility check, not a reconciliation. MTO omits unmapped vehicles,
                      and a same-model-year registration can reflect timing, migration or imports rather
                      than a sale in that year. CEUD has separate definitions and revisions. The ratios
                      are not used to expand mapping, rescale weights, calibrate retention or change parameters.
                      """),
                coverage_note,
                chart_ui(comparison_chart("Stock"), 'population-15-stock', 'Registration and CEUD stocks measure different populations'),
                chart_ui(comparison_chart("Sales"), 'population-15-sales', 'Registration cohorts and CEUD sales have different meanings'),
            ]
        )
        return validation_output

    _build_view()
    return


@app.cell(hide_code=True)
def retention_survival_section(mo):
    mo.md("""
    ## 3. Retirement-rate diagnostics and accepted survival context
    """)
    return


@app.cell(hide_code=True)
def heavy_truck_survival_heading(mo):
    mo.md("""
    ### Heavy-truck survival: [*EIA NEMS*](https://github.com/EIAgov/NEMS) versus [*EPA MOVES*](https://www.epa.gov/moves)
    """)
    return


@app.cell
def heavy_truck_survival_comparison_data(evidence):
    heavy_truck_survival = evidence["heavy_truck_survival"]
    heavy_truck_medians = evidence["heavy_truck_medians"]
    heavy_truck_median_interpolation = evidence["heavy_truck_median_interpolation"]
    heavy_truck_selected_source = evidence["heavy_truck_selected_source"]
    return (
        heavy_truck_median_interpolation,
        heavy_truck_medians,
        heavy_truck_selected_source,
        heavy_truck_survival,
    )


@app.cell
def heavy_truck_survival_comparison_chart(
    alt,
    chart_ui,
    heavy_truck_median_interpolation,
    heavy_truck_medians,
    heavy_truck_selected_source,
    heavy_truck_survival,
    mo,
    pd,
):
    def _build_view():
        source_labels = {
            "eia_nems_hd_truck_scrappage": "NEMS Class 7–8",
            "epa_moves_heavy_truck_survival": "EPA MOVES heavy truck",
        }
        curve_rows = heavy_truck_survival.copy()
        curve_rows["evidence_source"] = curve_rows["source_id"].map(source_labels)
        median_rows = heavy_truck_medians.copy()
        median_rows["evidence_source"] = median_rows["source_id"].map(source_labels)
        median_rows["selected_in_scenario"] = median_rows["source_id"].eq(
            heavy_truck_selected_source
        )
        median_points = curve_rows.merge(
            median_rows[["source_id", "target_class", "first_age_at_or_below_0_5"]],
            left_on=["source_id", "source_class", "age"],
            right_on=["source_id", "target_class", "first_age_at_or_below_0_5"],
            how="inner",
            validate="one_to_one",
        )
        source_color = alt.Color(
            "evidence_source:N",
            title="Heavy-truck source",
            scale=alt.Scale(
                domain=list(source_labels.values()),
                range=["#457b9d", "#e76f51"],
            ),
            legend=alt.Legend(orient="right"),
        )
        source_tooltips = [
            alt.Tooltip("evidence_source:N", title="Source"),
            alt.Tooltip("source_class:N", title="Source class"),
            alt.Tooltip("age:Q", title="Vehicle age", format=".0f"),
            alt.Tooltip("survival_probability:Q", title="Cumulative survival", format=".2%"),
        ]
        survival_lines = (
            alt.Chart(curve_rows)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X("age:Q", title="Vehicle age (years)"),
                y=alt.Y(
                    "survival_probability:Q", title="Cumulative survival",
                    axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1]),
                ),
                color=source_color,
                tooltip=source_tooltips,
            )
        )
        median_markers = (
            alt.Chart(median_points)
            .mark_point(filled=True, size=95)
            .encode(
                x="age:Q", y="survival_probability:Q", color=source_color,
                tooltip=source_tooltips,
            )
        )
        median_reference = (
            alt.Chart(pd.DataFrame({"survival_probability": [0.5]}))
            .mark_rule(color="#777777", strokeDash=[5, 4])
            .encode(y="survival_probability:Q")
        )
        heavy_truck_chart = (
            alt.layer(median_reference, survival_lines, median_markers)
            .properties(
                width=800, height=330,
                title=alt.TitleParams(
                    text="NEMS and EPA MOVES heavy-truck cumulative survival",
                    subtitle="50% survival reference; markers show the first whole-age crossing",
                ),
            )
        )
        median_description = (
            "the first whole age with cumulative survival at or below 50%"
            if heavy_truck_median_interpolation == "none"
            else "linear interpolation between the ages around 50% cumulative survival"
        )
        return mo.vstack([
            mo.md(f"""
                  How do annual factors become lifetime schedules? EIA NEMS scrappage becomes annual
                  survival through `1 − scrappage`; EPA MOVES Table C-1 supplies annual conditional
                  survival directly. Production compounds those factors from 100% at age zero. This
                  view shows complete source ages before `survival_curve_max_age`, without extrapolation.

                  For MOVES, `S(0) = 1` and `S(a + 1) = S(a) × annual_rate(a)`, giving a **20-year**
                  heavy-truck median. Applying the current-age factor one year early would give 19.
                  Fixed median-equivalent lifetimes use {median_description}. The selected scenario
                  uses **{source_labels[heavy_truck_selected_source]}** for heavy-truck fixed lifetimes
                  and curves; its source profile also sets the other road-class schedules.
                  """),
            chart_ui(heavy_truck_chart, 'population-16', 'Heavy-truck survival schedules from alternative sources'),
            mo.ui.table(
                median_rows[[
                    "evidence_source", "target_class", "median_equivalent_age",
                    "selected_in_scenario",
                ]],
                selection=None, pagination=False, page_size=5,
                label="Heavy-truck source medians before the scenario age cap",
            ),
        ])

    _build_view()
    return


@app.cell(hide_code=True)
def retention_method_heading(mo):
    mo.md("""
    ### [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) apparent-retention construction
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
                  **What does apparent retention measure?** The scenario-selected NHTSA CAFE + EIA
                  NEMS or EPA MOVES schedules supply accepted lifetimes. MTO retention below is an
                  independent diagnostic, not their replacement.

                  A raw pair needs consecutive editions for the same source category, make/model and
                  vintage, positive starting FIT_ACTIVE stock and a starting age inside the captured
                  window. A missing next-year row is missing evidence, not zero survivors. Raw pairs
                  precede mapping; the primary pool then requires reviewed class evidence and presence
                  of that series in the latest snapshot. This is separate from fleet-composition weights.

                  For category $p$, make/model $k$, vintage $v$, report year $t$ and starting stock $N$:

                  $$
                  a=t-v,\qquad D_{p,k,v,t}=N_{p,k,v,t}-N_{p,k,v,t+1},\qquad
                  r_{p,k,v,t}=\frac{N_{p,k,v,t+1}}{N_{p,k,v,t}},\qquad q_{p,k,v,t}=1-r_{p,k,v,t}.
                  $$

                  Negative apparent retirement $q$ (retention above one) is retained. Without
                  vehicle-level linkage, these changes mix retirement, migration, registration and
                  recoding. For each class and age, pool counts **before** calculating rates:

                  $$
                  q_{c,a}=\frac{\sum D_{p,k,v,t}}{\sum N_{p,k,v,t}},\qquad r_{c,a}=1-q_{c,a}.
                  $$

                  Compound from age zero:

                  $$
                  S_c(0)=1,\qquad S_c(a+1)=S_c(a)r_{c,a},\qquad F_c(a)=1-S_c(a).
                  $$

                  The age-zero factor gives survival at age one; the last factor gives the following
                  age's terminal point. No smoothing, clipping or NHTSA calibration enters this product.
                  The recurrence check verifies it directly.

                  Quality gates test mapped exposure, rate bounds, age continuity, monotonicity and
                  vintage/transition support. The following views show their metrics and recorded
                  decision; they do not publish a lifetime schedule.
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
    ### Raw make-model-vintage retirement-rate distributions
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
        display_minimum = -0.35  # Display window; the estimator retains the full distribution.
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
                  How variable are the raw annual retirement estimates? Each histogram counts
                  make/model/vintage transitions at one starting age, before class pooling. Each
                  eligible annual pair contributes one observation, regardless of vehicle count.
                  Passenger and Commercial remain source categories. These distributions are not
                  stock-weighted class estimates.

                  All panels show −35% to +40%; each six-panel row shares its own count axis to keep
                  sparse older ages visible. Values outside that display window remain in estimation
                  and in the full-range table of extrema, quantiles and counts. Very negative rates
                  can arise when a small cohort grows sharply, so observation count and stock exposure
                  must be read separately.
                  """),
            mo.ui.tabs({
                "Passenger distributions": chart_ui(
                    histogram_matrix("PASSENGER", "#457b9d")
                , 'population-17-passenger', 'Passenger retention distributions and their exposure'),
                "Commercial distributions": chart_ui(
                    histogram_matrix("COMMERCIAL", "#e76f51")
                , 'population-17-commercial', 'Commercial retention distributions and their exposure'),
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
    ### Registration-status comparison: FIT_ACTIVE versus FIT_INACTIVE
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
            return mo.vstack([chart_ui(counts, f"population-18-counts-{vehicle_class}", 'Registration status counts by age'), chart_ui(share, f"population-18-share-{vehicle_class}", 'Fit-active share by age')])


        status_proxy_output = mo.vstack([
            mo.md("""
                  Does administrative fit status change with age? This view compares only Report A
                  `FIT_ACTIVE` and `FIT_INACTIVE` counts, separately for Passenger and Commercial.
                  Other statuses are not folded into FIT_INACTIVE. Neither bucket alone measures
                  physical retirement or scrappage.
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
    ### [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) apparent survival versus accepted [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) curves
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
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right", titleFontStyle="italic")),
            )
        )
        annual_points = (
            alt.Chart(annual_rows)
            .mark_circle(opacity=0.8)
            .encode(
                x="age:Q",
                y="annual_retirement_rate:Q",
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right", titleFontStyle="italic")),
                size=alt.Size("fit_active_exposure:Q", title="Starting exposure", scale=alt.Scale(range=[20, 240])),
                tooltip=["vehicle_class:N", "age:Q", alt.Tooltip("annual_retirement_rate:Q", format=".2%"), alt.Tooltip("annual_survival_factor:Q", format=".2%"), alt.Tooltip("fit_active_exposure:Q", format=","), "number_of_vintages:Q", "number_of_transitions:Q"],
            )
        )
        annual_chart = (annual_lines + annual_points).properties(
            width=700, height=300, title="Exposure-pooled annual Ontario MTO retirement rates"
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
            "MTO cohort transitions": "Solid: Ontario MTO transition evidence",
            "Legacy NHTSA": "Dashed: NHTSA CAFE legacy schedule",
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
                color=alt.Color("vehicle_class:N", title="NRCan CEUD class", scale=alt.Scale(domain=["Car", "Light Truck"], range=["#457b9d", "#e76f51"]), legend=alt.Legend(orient="right", titleFontStyle="italic")),
                strokeDash=alt.StrokeDash(
                    "line_type:N",
                    title="Evidence source",
                    scale=alt.Scale(
                        domain=[
                            "Solid: Ontario MTO transition evidence",
                            "Dashed: NHTSA CAFE legacy schedule",
                        ],
                        range=[[1, 0], [7, 4]],
                    ),
                    legend=alt.Legend(
                        orient="right",
                        symbolType="stroke",
                        symbolStrokeWidth=3,
                        symbolSize=320,
                        labelFontStyle="italic",
                        labelLimit=320,
                    ),
                ),
                tooltip=["vehicle_class:N", "evidence_source:N", "age:Q", alt.Tooltip("cumulative_survival:Q", format=".2%")],
            )
            .properties(width=700, height=300, title="Ontario MTO and legacy NHTSA CAFE cumulative survival")
        )

        decision = evidence["mto_survival_decision"].copy()
        decision_outcome = str(decision["decision_outcome"].iloc[0])
        survival_comparison_output = mo.vstack([
            mo.md(f"""
                  Can apparent MTO survival justify a lifetime schedule?

                  **Recorded diagnostic decision: {decision_outcome}.** This is an evidence-quality
                  assessment, not publication or replacement of an accepted schedule. MTO transitions
                  use consecutive FIT_ACTIVE observations with positive starting stock. Class mapping
                  is attached afterwards; negative retirement estimates remain un-clipped.

                  Primary pools require the same make/model/vintage series in the latest snapshot.
                  This relevance filter also conditions on survival: disappeared families cannot
                  influence the primary curve. Latest-fleet composition weights do not enter it.

                  #### 1. How much historical stock exposure has a reviewed mapping?

                  The denominator sums starting FIT_ACTIVE stock over every eligible annual pair.
                  A series observed repeatedly contributes once per transition; this is not a count
                  of unique series or vehicles. Mapped LDV exposure resolves to CEUD Car/Light Truck;
                  mapped non-LDVs remain separate and excluded from those pools.

                  “Outside 1981+ scope” means ineligible for the selected age window. The oldest
                  contributing vintage is `2016 - 35 = 1981`, not a claim that older identities are
                  less reliable. In equations this denominator is the sum of `N_{{p,k,v,t}}`.
                  """),
            chart_ui(coverage_chart, 'population-19', 'Ontario MTO mapped exposure supporting apparent retention estimates'),
            mo.md("""
                  #### 2. How much support lies behind each annual rate?

                  For each class/starting age, pooled retirement is **total apparent retirements /
                  total starting stock exposure**. Exposure sums contributing historical annual pairs,
                  not the latest fleet. Distinct model years measure vintage support; individual
                  make/model/vintage annual pairs measure transition support. Point area shows exposure
                  and color shows Car or Light Truck.
                  """),
            chart_ui(annual_chart, 'population-20', 'Ontario MTO apparent retirement rates and supporting exposure'),
            mo.md("""
                  Tooltips report exposure and both support counts. The decision gate covers starting
                  ages 0–35, retaining early evidence without a 2000 vintage floor and excluding the
                  sparse older-age tail.

                  #### 3. What changes when annual rates are compounded?

                  Both curves start at age zero. The MTO age-0 factor gives survival at age 1; each
                  later factor multiplies the preceding level. Straight segments join the resulting
                  points—there is no smoothing, interpolation or fit. This product can look smooth
                  even as late-age support declines; smoothness does not establish reliability.

                  The age-35 rate gives survival at age 36; NHTSA CAFE is displayed to the same age.
                  Color distinguishes vehicle class, with solid MTO and dashed NHTSA source lines.
                  The NHTSA reference is loaded after MTO transitions, pooling and compounding; it
                  never enters an MTO count, weight, rate or calibration.
                  """),
            chart_ui(cumulative_chart, 'population-21', 'Cumulative Ontario MTO retention and NHTSA CAFE survival'),
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
        scope_comparison_output = mo.vstack([
            mo.md("""
                  #### 4. What evidence is lost with a 1990 vintage floor?

                  Both calculations use FIT_ACTIVE series present in the latest snapshot. The chart's
                  “production” label means the primary **MTO diagnostic calculation**, not the accepted
                  backend lifetime schedule. Its dynamic floor is 1981, preserving all vintages able
                  to contribute at starting ages 0–35. A 1990 floor applied after transition construction
                  removes the age-35 estimate and substantially reduces older-age exposure.
                  """),
            chart_ui(scope_annual_chart, 'population-22', 'Annual retention sensitivity to population scope'),
            mo.md("""
                  Each curve compounds its own annual MTO factors; NHTSA does not enter this sensitivity.
                  Only exclusion of 1981–1989 vintages changes the comparison.
                  """),
            chart_ui(scope_cumulative_chart, 'population-23', 'Cumulative retention sensitivity to population scope'),
            mo.md(f"""
                  Exposure and distinct-vintage counts show the support removed by the stricter floor.
                  The primary estimator starts with the age-0 transition. These artifacts reach starting
                  age {scope_rate_age_domain[1]}, producing a terminal survival point at
                  age {scope_cumulative_age_max}. The 2000 floor applies only to existing-fleet weights.
                  Color distinguishes classes; line type distinguishes the dynamic floor from 1990+.

                  **Implication for model documentation:** report mapping coverage alongside composition
                  weights, and describe MTO retention as administrative evidence conditioned on its
                  eligible cohorts. A smooth apparent-survival curve is not sufficient grounds to
                  replace an accepted lifetime schedule. Reviewed identity/configuration gaps and
                  support tests identify where additional evidence could improve the backend.
                  """),
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
