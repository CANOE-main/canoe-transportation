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
            # Ontario vehicle-population mapping and parameter diagnostic

            | Evidence question | What is shown |
            |---|---|
            | Mapped fit-active stock | How much of the latest Ontario stock is covered by the reviewed map. |
            | Canonical vehicle reference | Rated and historical identities, exact model-year attributes and source linkages, including families without [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) codes. |
            | Stock that cannot be mapped reliably | Which evidence limits leave make-model keys unresolved. |
            | Class and vintage weights | What the accepted mapped stock implies for class shares and fleet age. |
            | Report A versus Report 5 age profiles | How mapped Car/Light Truck shapes compare with broader Passenger/Commercial stock. |
            | Report 4 versus [*Wards*](https://wardsintelligence.informa.com/automotive-data) truck shares | How the sources distribute medium-duty stock across GVWR classes. |
            | [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) versus [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | Whether mapped stock and same-model-year registration proxies resemble Ontario stock and sales totals. |
            | Mapped-cohort retention | What consecutive Report A editions can—and cannot—say about apparent retirement and survival. |
            | Heavy-truck survival sources | How [*EIA NEMS*](https://github.com/EIAgov/NEMS) and [*EPA MOVES*](https://www.epa.gov/moves) annual factors translate into cumulative survival and median-equivalent lifetimes. |

            """),
            mo.md("""
            ### Reading boundary
            This notebook reads the evidence captured by the latest successful diagnostic
            build, including its recorded configuration and file hashes. Reload after a
            new build to inspect its updated evidence. Opening the notebook never fetches
            sources, repeats ETL, changes mappings, or promotes diagnostic results.
            `PASSENGER` and `COMMERCIAL` use the same mapping and cohort logic while
            remaining distinct [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) source categories.
            """),
            mo.md("""
            ### Working assumptions, owners, and interpretation boundaries

            | Topic | Owner or generator | Interpretation in this notebook |
            |---|---|---|
            | Ontario Report A, 4, and 5 evidence | `fetching.vehicle_population` | `FIT_ACTIVE` is the administrative proxy for vehicles available for road use; Report A supplies make-model-vintage cohorts, Report 4 supplies GVWR evidence, and Report 5 supplies Passenger/Commercial model-year totals. |
            | Canonical identities and source links | Two reviewed tables in `config/parameters/`, read by `parameterization.vehicle_reference` | Canonical year/class/powertrain scopes and source links are fixed configuration inputs. Reviewed links retain historical or unrated identities. Catalogue powertrains do not establish [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) propulsion. |
            | Mapping review evidence | `parameterization.road_aggregation --mapping-diagnostics` | Candidate, coverage, and unresolved-key tables are explicit review diagnostics. Row coverage counts make-model-vintage rows; stock coverage sums `FIT_ACTIVE`. |
            | Mapped-fleet composition | `parameterization.road_aggregation` | Class and vintage composition is conditional on accepted mapped LDV stock. Unmapped stock remains visible in coverage diagnostics and is not imputed across mapped classes. |
            | Existing-fleet age weights | `parameterization.road_stocks_and_demands` | The raw comparison charts use the latest snapshot and a common model-year floor of 2000. The downstream stock-cohort cutoff is separate from technology lifetimes: `survival_curve_max_age` only bounds represented cohorts when curves are enabled. |
            | MHDV weight-class shares | `parameterization.road_fleet_weights`, Report 4 and registered [*Wards*](https://wardsintelligence.informa.com/automotive-data) class shares | The scenario selects the regional GVWR source for efficiencies, costs and utilization. This view compares the sources' different coverage. |
            | [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) comparison | `fetching.nrcan_ceud` | [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) is an external plausibility benchmark. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) stock is the accepted mapped subset; the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) “sales” series is a same-model-year registration proxy. |
            | Accepted lifetime inputs | `parameterization.road_lifetimes_survival` | `road_lifetime_survival_source` selects [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) with [*EIA NEMS*](https://github.com/EIAgov/NEMS) heavy trucks, or [*EPA MOVES*](https://www.epa.gov/moves) cars/light trucks with its heavy-vehicle rates for medium/heavy trucks. The selected class schedules supply fixed medians or survival curves according to `survival_curves`. |
            | [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) apparent-retention diagnostics | `parameterization.road_lifetimes_survival --mto-diagnostics` | Consecutive editions supply raw make-model-vintage transitions within the captured age window. Primary pooled curves retain series present in the latest snapshot. Growth remains visible; these diagnostic curves do not select accepted lifetime parameters. |
            """),
            mo.md(f"""
            ### Prepare once, then reload after updated builds

            Prepare the stock and efficiency evidence with the offline preset:

            ```text
            {prepare_command()}
            ```

            This build prepares production parameters, complete registered Ontario history,
            mapping diagnostics, [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) retention evidence and heavy-truck survival
            schedules, then records their inputs and configuration together.
            """),
            mo.md("""
            ### Mapping proposals and accepted decisions

            The optional bootstrap ranks candidate families for review. Similarity
            scores help find labels; they do not establish identity, model year,
            vehicle class or fuel. Proposals cannot overwrite either authoritative
            table. Production uses reviewed source links and their year intervals.
            [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) and FuelEconomy.gov retain distinct catalogue coverage.
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
            completed {evidence['build_metadata']['built_at']}. These switches were
            recorded with the captured evidence. The associated database is
            `{evidence['build_metadata']['database']['path']}`.
            Charts read the captured ETL tables; the database identifies the successful
            publication and is not queried by this notebook.
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
        ## Canonical vehicle reference

        One reviewed row describes a canonical identity and its supported year,
        class and powertrain scope. Families without [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) codes are included. Both
        tables are captured configuration inputs; ordinary builds do not rewrite them.
        Consumption, representative electric range and range allocation are calculated
        downstream from eligible target-year configurations, as shown in the efficiency diagnostic.

        Across the full source-link table, **{_identity_gaps:,} intervals lack an accepted identity**;
        **{_configuration_gaps:,} retain an identity with unresolved configuration**.
        A supported stock size class can remain usable while efficiency matching needs more specific evidence.
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
                This view defines the Report A registration-status
                context used by the later mapping and transition diagnostics.

                The percentage bars use every fetched Report A edition and their
                eight registration statuses. Passenger and Commercial use the same
                status order and colors. 
                The dictionary supplies administrative codes and short labels. This
                notebook uses `FIT_ACTIVE` as the stock proxy and shows the other
                statuses separately. Status counts do not identify vehicle-level
                transitions, insurance histories or physical retirement. Migration,
                registration changes and recoding can change these totals.
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
                The heat maps then show the latest edition's
                absolute stock by status and model year from 2000 onward; their square-root
                color scale keeps small administrative buckets visible beside fit-active
                stock.
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
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Label candidate; identity/class unreviewed",
            "no_applicable_reviewed_interval": "No applicable reviewed year interval",
        }
        mapping_row_source["row_reason"] = mapping_row_source["unresolved_reason"].fillna("no_model_label_candidate")
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
            This view measures reviewed mapping coverage by source
            category, stock, row, model year, and supporting model-family evidence.

            This measures the usable scale of the reviewed map and the
            evidence breadth behind it. A Report A *row* is one make/model/model-year
            cohort; fit-active stock is the vehicle count carried by those rows.
            The [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64)/EPA chart is auxiliary class evidence for review, separate from
            the canonical inventory above. The identity total counts distinct canonical
            IDs linked to [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) across all reviewed scopes, including unrated identities;
            reviewed interval rows are a different count.
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
            The row charts partition every edition into LDV-mapped, mapped non-LDV,
            the four largest unresolved reasons by row count, and an `Other` remainder. These are row
            shares rather than stock shares; the reasons and their evidence are explained
            in detail immediately below. Non-LDV stock is identified but excluded from
            LDV weights and survival pooling. “Outside 1981+ scope” is an accounting
            category, not a failed or unreliable mapping: 1981 is derived as the first
            eligible transition year, 2016, minus the configured maximum starting age of 35.
            Older vintages cannot enter the selected age window and are separated so
            they do not inflate the unresolved-mapping denominator.
            """),

            mo.md("""
            A source link covers a make/model code and a reviewed year interval.
            Several keys can resolve to one canonical family. This chart uses
            the unified [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) and FuelEconomy.gov family catalogue, deduplicates by
            canonical make, canonical model, and [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) class, and then asks whether any
            accepted [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) key points to each family.
            """),
            chart_ui(family_use_chart, 'population-4', 'How canonical family mappings support parameterization'),

            mo.md("""
            ## Coverage by model year and report edition

            **Summary:** These charts show how accepted `FIT_ACTIVE` mapping coverage
            changes by exact model year in each Report A edition.

            The model-year bars reconcile mapped `FIT_ACTIVE` cohort snapshots against
            raw `FIT_ACTIVE` totals for every fetched edition. Grouped bars retain the
            edition dimension instead of averaging coverage across repeated snapshots;
            only model years from 2000 through each report edition are included. The row
            charts apply the reviewed make/model keys directly to every Report A edition.
            This 2000+ view supports existing-fleet composition; it is separate from the
            1981 dynamic mapping floor and the age-based survival transition window.
            """),
            mo.vstack([
                chart_ui(mapping_model_year_chart("PASSENGER"), 'population-5-passenger', 'Passenger mapping coverage by model year'),
                chart_ui(mapping_model_year_chart("COMMERCIAL"), 'population-5-commercial', 'Commercial mapping coverage by model year'),
            ]),

            mo.md("""
            **Unused normalized evidence** means the canonical family exists in the
            combined evidence catalogue but no reviewed [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make/model code currently
            points to it; it does not mean its source rows are unused elsewhere.
            Source labels follow the reviewed identities; model-year, class and
            powertrain distinctions remain where the reference supports them.
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
            "not_present_in_latest_snapshot": "Not present in latest snapshot",
            "unreviewed_high_confidence_candidate": "Label candidate; identity/class unreviewed",
            "no_applicable_reviewed_interval": "No applicable reviewed year interval",
        }
        unmapped_reason_summary["reason_label"] = unmapped_reason_summary["unresolved_reason"].map(reason_labels).fillna(unmapped_reason_summary["unresolved_reason"])
        reason_order = list(reason_labels.values())
        reason_colors = ["#b2182b", "#ef8a62", "#fddbc7", "#457b9d", "#e76f51", "#f4a261", "#8d99ae", "#90be6d", "#577590", "#9b5de5", "#6c757d"]
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
                        scale=alt.Scale(domain=reason_order, range=reason_colors),
                    ),
                    order=alt.Order("reason_order:Q"),
                    tooltip=["VEHICLE_CLASS:N", "report_year:O", alt.Tooltip("reason_label:N", title="Reason"), alt.Tooltip("unresolved_fit_active_stock:Q", format=",", title="Unmapped vehicles"), alt.Tooltip("share_of_unresolved_stock:Q", format=".2%", title="Share of edition's unmapped stock")],
                )
                .properties(width=700, height=260, title=f"{vehicle_class.title()} unresolved stock by reason and edition")
            )


        unmapped_reasons_output = mo.vstack([
            mo.md("""
            This view partitions unresolved `FIT_ACTIVE` stock by the
            specific evidence limitation that prevents reviewed mapping.

            Each horizontal bar partitions one category and edition's
            unmapped fit-active stock, so every bar sums to 100%. Reason labels are
            calculated for each source row using the captured year-scoped links.
            Label candidates remain hints from the latest review pass. Historical keys
            without current evidence are labelled separately. Tooltips retain absolute
            stock; each edition's bar has its own denominator.

            - **Weak model-label agreement:** make evidence exists, but only substring
              or general similarity supports the model.
            - **Ambiguous best candidate:** equally strong families or class
              interpretations remain.
            - **No normalized make agreement:** the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make code has no configured
              alias or normalized make-prefix match, so model scoring cannot begin.
            - **Suppressed or unknown source code:** make or model is blank, masked, or
              explicitly unknown.
            - **No model-family candidate:** the make normalized successfully, but no
              canonical model family was generated for the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) label.
            - **Not present in latest snapshot:** the historical key does not occur in
              the latest fleet snapshot. This lowers its priority for current fleet
              composition and age weights. Raw transitions remain visible;
              primary pooled survival curves require the same make-model-vintage series in the latest snapshot.
            - **Label candidate; identity/class unreviewed:** an exact/prefix hint has
              no authoritative source link. This is a review lead, not an approval queue.
            - **No applicable reviewed year interval:** the key is reviewed for other
              model years. Check the recorded intervals and supporting year evidence;
              this alone does not establish model nonexistence or a missing rating.
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
            [unresolved_detail, expired_rows[unresolved_detail.columns]],
            ignore_index=True,
        )
        unresolved_detail["mapping_key"] = unresolved_detail["VEHICLE_CLASS"].astype(str) + " / " + unresolved_detail["MAKE"].astype(str) + "/" + unresolved_detail["MODEL"].astype(str)
        top_reason_codes = (
            current_reason_stock.nlargest(3, "FIT_ACTIVE")["unresolved_reason"]
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
                chart_ui(reason_chart, 'population-7', 'Why vehicle identities remain unresolved'),
                mo.ui.table(key_totals, selection=None, pagination=True, page_size=10, visible_columns=visible_columns, freeze_columns_left=["VEHICLE_CLASS", "MAKE", "MODEL"], wrapped_columns=[column for column in ["unresolved_reason_detail", "rating_model_labels"] if column in visible_columns], label=f"Top 20 unresolved make-model keys: {reason_labels.get(reason_code, reason_code)}"),
            ])

        highest_unresolved_output = mo.vstack([
            mo.md("""
            This view ranks the highest-stock unresolved [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make-model
            keys within each evidence-limitation category.

            Each bar shows one [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make/model key within a source category, split
            by model year. Tables rank current unresolved stock; the historical-only
            tab instead sums exposure across editions. Those two totals describe
            different populations and must not be added together.

            Candidate names and classes are review hints. Exact/prefix agreement
            supports discovery; accepted identity and year/class scope come from
            the captured source-link table. The details column gives one source-row
            explanation per key; the reference audit retains exact year decisions.
            Full candidate labels and scores remain available in the table.

            Historical keys absent from the latest edition remain in raw transition
            evidence. Primary pooled [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) survival curves apply their separate
            latest-snapshot presence condition.
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
            This view shows how accepted mapped `FIT_ACTIVE` stock is
            distributed across source-native [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) classes in each Report A edition.

            Each horizontal bar is one Report A edition and partitions the
            accepted fit-active fleet into source-native [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) classes. Car and
            Light Truck are separated because their class vocabularies differ. These are
            relative shares of mapped stock, so a changing segment can reflect both fleet
            evolution and changing mapping coverage. The calculation does not reweight
            mapped classes to represent unresolved stock, so every composition result is
            conditional on the accepted crosswalk.

            [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) class order is fixed from
            smaller to larger size classes within each source body-type family, so both
            stack position and color remain stable across editions. Two-seaters and vans
            follow the size-ordered classes because they are separate source categories.
            """),
            chart_ui(edition_composition_chart("Car"), 'population-8-cars', 'Car size composition across observed editions'),
            chart_ui(edition_composition_chart("Light Truck"), 'population-8-light-trucks', 'Light-truck size composition across observed editions'),
            mo.md("""
            The area chart below isolates the latest snapshot and model-year vintages
            from 2000 onward. Each colored area is an [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) class contribution in
            absolute mapped FIT_ACTIVE stock from the 2025 fleet; the class-vintage
            contributions accumulate to the total mapped 2000+ stock across the
            displayed data.
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
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Car", class_column, legend_title), 'population-10-cars', 'Car composition: registrations versus sales reference'),
                chart_ui(model_year_comparison_chart(empirical, wards_frame, "Light Truck", class_column, legend_title), 'population-10-light-trucks', 'Light-truck composition: registrations versus sales reference'),
                mo.ui.tabs({
                    "MTO latest-stock table": mo.ui.table(empirical.sort_values(["nrcan_ceud_class", class_column, "model_year"]), selection=None, pagination=True, page_size=10, format_mapping={"within_model_year_share": "{:.2%}"}),
                    "Wards 2018/2021 table": mo.ui.table(wards_frame.sort_values(["nrcan_ceud_class", "year", class_column]), selection=None, pagination=False, page_size=10, format_mapping={"market_share": "{:.2%}"}),
                }),
            ])


        age_distribution_output = mo.vstack([
            mo.md("""
            This view shows the model-year distribution of the latest
            accepted mapped Car and Light Truck stock.

            This uses only accepted `FIT_ACTIVE` stock from the latest
            Report A and retains model years 2000 through the report year. It does not
            use the scenario median-lifetime or maximum-age cutoff. The 2000 floor is the
            existing-fleet representation scope; it is not the 1981 mapping-eligibility
            floor and is not applied to survival-transition evidence. Unmapped LDV stock
            is not assigned to the displayed classes or used to normalize them.

            The stacked areas show the absolute mapped `FIT_ACTIVE` stock observed at each
            model year, with an automatically scaled stock axis so the fleet-size trend is
            visible without a fixed percentage ceiling. The compact stacked bars at right
            remain [*Wards*](https://wardsintelligence.informa.com/automotive-data) 2018 and 2021 new-sales percentages and therefore use their own
            labelled 0-100% axis. [*Wards*](https://wardsintelligence.informa.com/automotive-data) describes Canadian new sales, while the areas
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
            This view identifies the highest-stock accepted mapped
            families and shows their exact model-year composition.

            Each heat-map cell is the accepted fit-active stock for one
            normalized make-model and exact model year. Only model years from 1990
            through the latest report year are shown. Families are ranked once by their
            total observation count across those years, and the categorical y-axis is
            explicitly sorted in descending order by that total. Totals appear only in
            the family table and are not repeated as if they were annual observations.
            These pooled rankings describe surviving mapped Ontario stock across
            vintages and propulsion types. Consumption weights require documented raw
            identity support covering the configured share of uniquely gasoline-resolved counts in
            that model-year/size-class cohort. All qualifying families use fit-active
            counts; other cohorts use distinct-model catalogue medians. Report A does
            not observe propulsion or trim. PEV composition uses eligible [*Transport Canada iZEV*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) purchase
            and lease records with supported family identities; compatible configurations
            supply representative consumption and range in the efficiency workflow.
            Matched abbreviated [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make/model keys are retained as list-form tooltips and
            table columns; keeping them out of the categorical axis preserves heat-map
            alignment and readability when many keys resolve to one normalized family.
            Canonical family names and source links follow the two reviewed tables
            captured with this build. Source labels, keys, counts and model-year ranges
            remain in **Source identity details**. Identity and configuration uncertainty
            are inspectable in the canonical reference audit above; a valid stock class
            does not establish an otherwise unsupported consumption configuration.
            """),
            mo.vstack([
                chart_ui(top_family_heatmap("Car"), 'population-11-cars', 'Canonical car families represented in observed stock'),
                chart_ui(top_family_heatmap("Light Truck"), 'population-11-light-trucks', 'Canonical light-truck families represented in observed stock'),
            ]),
            mo.md("""
            A blank model-year cell means no accepted FIT_ACTIVE observation exists for
            that normalized family in the latest Report A. This alone does not establish
            a rejected vintage or a gap in reviewed applicability; inspect the captured
            source-link intervals to distinguish those cases.
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
                This view samples mapped families from the first three
                within-class stock quartiles to expose lower-volume mapping examples.

                Each heat map draws ten random Car families and ten random
                Light Truck families from one within-class fit-active-stock percentile
                bucket. Rows are then ordered by descending family stock so the scale
                remains interpretable even though the sampled families change. Re-run
                this cell to reshuffle all three samples.

                Matched abbreviated [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) keys are included in list-form tooltips and tables.
                A second categorical axis would compress the model-year cells, so the
                validation labels remain available without changing the heat-map geometry.
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
                This view compares the 2000+ fleet-age shapes implied
                by mapped Report A classes and unmapped Report 5 source classes.

                Both series are reconstructed from absolute `FIT_ACTIVE` counts, filtered
                to model years 2000 through each source's latest snapshot, and normalized
                again within the displayed source class. Report A uses only reviewed mapped
                LDV rows and produces Car/Light Truck; Report 5 uses its broader
                Passenger/Commercial classes without make-model mapping. The paired labels
                are therefore deliberately approximate rather than equivalent.

                These shapes diagnose the source choice for distributing [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) stock
                over existing-fleet vintages. They do not replace the published age weights:
                those remain conditional on mapping and on the downstream stock-cohort
                cutoff. When `survival_curves` is false, accepted median-equivalent ages
                bound those cohort distributions; when it is true, `survival_curve_max_age`
                bounds represented cohorts. Neither cutoff creates or replaces an individual
                technology-lifetime parameter. Differences can reflect class scope, mapping
                coverage, and Report-specific construction as well as genuine fleet
                composition.
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
                This view compares Report 4 and [*Wards*](https://wardsintelligence.informa.com/automotive-data) shares for
                disaggregating medium-duty truck [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) parameters.

                Report 4 assigns the latest Ontario Commercial `FIT_ACTIVE` stock to EPA
                GVWR bins using each source interval's upper kilogram bound converted to
                pounds. This comparison keeps MDV3–MDV8, maps them to Classes 3–8, and
                renormalizes Report 4 within Medium Trucks (Classes 3–7) and Heavy Trucks
                (Class 8). LDT1–2, LDT3–4, and MDV2b are outside this MHDV comparison.

                [*Wards*](https://wardsintelligence.informa.com/automotive-data) uses the reviewed Class 3–8 shares from
                `vehicle_class_market_shares.csv`: Classes 3–7 sum to one within Medium
                Trucks and Class 8 is the sole Heavy Truck class. The sources are not
                like-for-like—Report 4 is Ontario registered stock, while [*Wards*](https://wardsintelligence.informa.com/automotive-data) is Canadian
                sales evidence from a different year—so the bars diagnose the choice of
                weight-class disaggregation rather than validate one source against the
                other. These shares are relevant only when combining [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) MHDV
                efficiency and cost parameters; they do not alter [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) stock totals,
                LDV age weights, or survival schedules.

                Only Medium Trucks are charted because Classes 3–7 contain a meaningful
                within-class distribution. Heavy Trucks contain only Class 8 in both
                sources, so their normalized share is mechanically 100%; the table retains
                the Class 8 audit rows, but a separate Heavy Truck chart would add no
                information.
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
                This view compares mapped [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) stock and a
                same-model-year registration proxy with [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) stock and sales.

                This visual check compares only accepted mapped `FIT_ACTIVE` [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) rows
                with Ontario [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) car and light-truck totals. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) stock sums all the
                mapped fit-active stock in each Report A edition; its sales proxy retains only
                rows whose model year equals that edition year. [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) light trucks combine
                the source-native passenger and freight light-truck series. All values are
                shown in thousands of vehicles.

                The comparison is directional rather than a reconciliation. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) is an
                administrative Ontario registration extract restricted to mapped rows;
                [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) is a separate Ontario stock/sales series with its own definitions and
                revisions. The same-model-year [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) proxy can include registration timing,
                migration, imports, or vehicles first observed after sale, and it omits
                unmapped vehicles. No ratio between the lines is used to expand the map,
                rescale fleet weights, calibrate retention, or alter accepted parameters.
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
            [*EIA NEMS*](https://github.com/EIAgov/NEMS) annual scrappage is converted to annual survival as `1 − scrappage`.
            [*EPA MOVES*](https://www.epa.gov/moves) Table C-1 reports annual conditional survival directly. The production
            parameterization multiplies each source's annual factors from a 100% age-zero
            baseline; the notebook reads that same conversion. Full source ages are shown
            before the scenario's `survival_curve_max_age` limit, without extrapolation.

            For [*EPA MOVES*](https://www.epa.gov/moves), elapsed age follows `S(0) = 1` and
            `S(a + 1) = S(a) × annual_rate(a)`. This gives a heavy-truck median of
            **20 years**. The earlier same-index product applied the current-age
            rate one year early and produced a 19-year median.

            Fixed median-equivalent lifetimes use {median_description}. The selected scenario uses
            **{source_labels[heavy_truck_selected_source]}** for both heavy-truck fixed
            lifetimes and survival curves. The scenario source profile also resolves the car,
            light-truck and medium-truck schedules.
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
            **Source boundary.** The scenario selects [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) with [*EIA NEMS*](https://github.com/EIAgov/NEMS) or [*EPA MOVES*](https://www.epa.gov/moves)
            road-class schedules for accepted lifetime parameters. This section
            independently derives [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) apparent retention, and the quality gates
            assess the administrative evidence used in the displayed comparison.

            A raw transition requires the same source category, make/model code and
            vintage in consecutive editions, positive starting `FIT_ACTIVE` stock and
            a starting age inside the captured window. A missing next-year row supplies
            no transition, rather than an assumed zero terminal stock. The raw table
            retains every eligible pair before mapping. The primary class pool then
            requires accepted class evidence and latest-snapshot presence of that series.
            This survivorship condition is separate from current-fleet composition weights.

            For category $\boldsymbol{p}$, make/model key $\boldsymbol{k}$, vintage $\boldsymbol{v}$, report year $\boldsymbol{t}$
            and starting stock $\boldsymbol{N}$:

            $$
            a=t-v,\qquad D_{p,k,v,t}=N_{p,k,v,t}-N_{p,k,v,t+1},\qquad
            r_{p,k,v,t}=\frac{N_{p,k,v,t+1}}{N_{p,k,v,t}},\qquad q_{p,k,v,t}=1-r_{p,k,v,t}.
            $$

            Negative apparent retirement $\boldsymbol{q}$ and retention $r>1$ remain visible.
            They mix physical retirement with migration, registration changes and
            recoding because the source has no vehicle-level linkage.

            Within each accepted class and age, sum apparent retirements and starting
            exposure across eligible series and vintages before dividing:

            $$
            q_{c,a}=\frac{\sum D_{p,k,v,t}}{\sum N_{p,k,v,t}},\qquad r_{c,a}=1-q_{c,a}.
            $$

            Cumulative survival uses an age-zero baseline and the actual annual factors:

            $$
            S_c(0)=1,\qquad S_c(a+1)=S_c(a)r_{c,a},\qquad F_c(a)=1-S_c(a).
            $$

            The age-zero rate determines survival at age one; the final annual rate
            determines the following age's terminal point. No smoothing, clipping or
            [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) calibration enters this calculation. [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) is comparison evidence.
            The recurrence check below verifies this product directly.

            Quality gates assess mapped exposure, annual-rate bounds, age continuity,
            monotonicity and vintage/transition support using the captured rules. The
            decision and supporting metrics appear in the following audit views.

            | Stage | Grain and treatment |
            |---|---|
            | Raw snapshot | Source category, [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) key, vintage and report year; preserve observed stock. |
            | Raw transition | Consecutive eligible pairs; calculate un-clipped apparent retirements and retention. |
            | Mapping | Attach reviewed year-specific class evidence without changing raw rates. |
            | Primary pool | Keep accepted-class series present in the latest snapshot; sum counts before calculating rates. |
            | Class-age curve | Compound the annual factors from an age-zero baseline. |
            | Decision audit | Assess evidence quality without publishing a lifetime schedule. |
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
            This view shows the unweighted distribution of raw annual
            apparent retirement rates at each starting age before class pooling.

            Each small histogram describes the cross-sectional distribution of raw
            one-year rates across eligible [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make-model-vintage series at one starting
            age. Every series receives one observation per eligible report-year pair;
            these histograms are counts, not stock-weighted class estimates. Passenger and
            Commercial remain source categories because this view precedes class mapping.
            Age labels sit inside each panel; all panels and both tabs share the same
            -35% to +40% x-domain. Each six-panel row shares its own automatically scaled
            transition-count y-domain, so later-age distributions remain visible as
            observations diminish. Axis titles and subplot headers are omitted to keep
            the grid compact.

            The histogram window is -35% to +40%.
            This is display-only: no raw value is clipped or discarded from the
            estimator. The table reports full-range minima, maxima, quantiles, negative
            growth-case counts, and observations outside the displayed window. Very large
            negative rates occur when a small starting cohort grows sharply in the next
            edition; their low exposure is visible separately from their observation count.
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
            This view compares the two fit-status buckets by source
            category and vehicle age without treating either as physical retirement.

            Passenger and Commercial remain separate, and this comparison
            uses only the raw `FIT_ACTIVE` and `FIT_INACTIVE` Report A columns. No
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
            This view compares independently derived [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) apparent
            survival with accepted [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) curves and audits the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) evidence scope.

            **Decision: {decision_outcome}.** This is the diagnostic decision recorded by
            the generated validation artifact; it does not itself publish or overwrite an
            accepted lifetime schedule. The estimator uses only strictly
            `FIT_ACTIVE` keys observed in consecutive Report A editions, with positive
            starting exposure and nonnegative starting age. Age 0 to 1 is the first
            empirical transition. Class mapping is attached afterwards. Raw and pooled
            negative retirement rates are preserved, never clipped. The primary [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)
            diagnostic rates use
            only make-model-vintage series that remain present in the latest snapshot;
            latest-snapshot composition weights still do not enter the rate calculation.
            This presence condition is a deliberate relevance filter, but it introduces
            survivorship conditioning: families that disappeared before the latest edition
            cannot influence the primary pooled curve.

            #### 1. Stock-weighted historical mapping coverage

            **Summary:** This audit partitions eligible historical starting exposure by
            whether a reviewed mapping reaches an LDV, a non-LDV, or no in-scope class.

            This is not the share of unique make-model-vintage series. Its denominator is
            the sum of `N_{{p,k,v,t}}`, the FIT_ACTIVE stock at the beginning of every
            eligible historical transition. A series observed over several annual pairs
            contributes its starting exposure to each pair. The mapped share is the part
            of that exposure whose reviewed vintage-specific crosswalk resolves to [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
            Car or Light Truck. Mapped non-LDV exposure is shown separately and remains
            excluded from the LDV retirement-rate pools. Vintages older than the dynamic
            1981 mapping floor are reported separately rather than treated as unresolved.
            The “Outside 1981+ scope” legend entry is therefore an eligibility/accounting
            class, not evidence that pre-1981 vehicles are intrinsically harder or less
            reliable to map. With the first usable consecutive pair beginning in 2016 and
            a maximum starting age of 35, the oldest contributing vintage is
            `2016 - 35 = 1981`.
            """),
            chart_ui(coverage_chart, 'population-19', 'Ontario MTO mapped exposure supporting apparent retention estimates'),
            mo.md("""
            #### 2. Empirical annual rates, exposure, and vintage support

            **Summary:** This audit shows the pooled annual [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) rates together with the
            exposure and vintage support behind every class-age estimate.

            At each class and age, the empirical rate is total apparent retirements divided
            by total starting exposure. Starting exposure is therefore the summed stock of
            every mapped make-model-vintage-report-year transition entering that point—not
            the latest fleet stock. Vintage support counts distinct model years, while
            transition support counts the individual make-model-vintage annual pairs.
            Point area represents starting exposure; the color legend distinguishes Car
            and Light Truck.
            """),
            chart_ui(annual_chart, 'population-20', 'Ontario MTO apparent retirement rates and supporting exposure'),
            mo.md("""
            Hovering reports exposure and both support counts. The configured decision gate
            evaluates starting ages 0 through 35. This keeps all eligible early evidence
            without treating 2000 as a survival-evidence floor, while excluding the sparse,
            noisy older-age tail.

            #### 3. Like-for-like cumulative-survival comparison

            **Summary:** This comparison places the direct cumulative product of [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) rates
            beside accepted [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) curves over the same age horizon.

            Both series display an age-zero baseline. For [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data), the age-0 annual rate is the
            first empirical factor and determines survival at age 1; the age-1 rate then
            determines survival at age 2. The [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) line is not smoothed, interpolated
            statistically, or fitted: each plotted point is the direct cumulative product
            of the exposure-pooled [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) factors and straight segments merely connect
            consecutive ages. A cumulative product naturally looks smoother than its annual
            inputs because each new rate changes the preceding survival level
            multiplicatively. Declining late-age exposure makes those smooth-looking levels
            less certain, not more reliable. The age-35 annual rate yields the terminal [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)
            survival point at age 36; [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) is truncated to the same cumulative horizon for
            comparison. Color identifies vehicle class, while the stroke-symbol legend
            identifies solid [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) transition evidence and dashed [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) legacy schedules.

            [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) is loaded only after the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) transition table, annual pooling, and
            cumulative product already exist. It never enters an [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) numerator,
            denominator, weight, rate, interpolation, or calibration step.
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
            #### 4. Primary [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) diagnostic vintage scope and 1990+ sensitivity

            **Summary:** This sensitivity isolates how excluding 1981–1989 vintages changes
            [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) rates, cumulative survival, exposure, and age coverage.

            Both methods use only raw FIT_ACTIVE make-model-vintage series present in the
            latest snapshot. The chart's “production” label names the primary [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)
            diagnostic aggregation branch; it does not mean the curve is the accepted
            backend lifetime parameter. That branch retains every vintage capable of
            contributing to an observed starting age from 0 through 35; given the first
            eligible 2016 transition, that dynamic floor is 1981. The sensitivity series
            imposes a 1990 floor after transition construction. It therefore loses the
            age-35 rate and sharply reduces exposure in the oldest ages.
            """),
            chart_ui(scope_annual_chart, 'population-22', 'Annual retention sensitivity to population scope'),
            mo.md("""
            The cumulative curves below are independently compounded from each method's
            own annual [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) rates. [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy) does not enter this comparison. Differences between
            the lines arise only from excluding 1981-1989 vintages.
            """),
            chart_ui(scope_cumulative_chart, 'population-23', 'Cumulative retention sensitivity to population scope'),
            mo.md(f"""
            Exposure and distinct-vintage support show what the conditioning removes.
            The production estimator begins with the empirical age-0 transition. In these generated artifacts annual evidence
            extends through starting age {scope_rate_age_domain[1]}, and the terminal
            cumulative point at age {scope_cumulative_age_max} applies that final factor.
            The 2000 floor remains confined to existing-fleet aggregation weights and does
            not truncate this evidence. Color identifies vehicle class; the separate
            line-type legend identifies the production dynamic floor and 1990+ sensitivity.
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
