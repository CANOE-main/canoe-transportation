---
title: Unified MVP documentation with MkDocs
role: Planned content scope, publication approach and acceptance criteria for the MVP report and repository documentation.
status: implementation_record
retrieve_when: Building or reviewing the unified MVP documentation and PDF export.
read_scope: Read the scope and acceptance criteria, then retrieve only the source documents and diagnostic evidence needed for the selected section.
verify: Reconcile all implementation and validation claims against current code, configuration and recorded evidence.
planned_on: 2026-10-29
mvp_target: 2026-11-01
repository_commit_reviewed: 06a1b79b9ae81a1708de5d4d07ad3c49e5cc3b11
---

# Unified MVP documentation with MkDocs

The 2026-11-10 implementation instruction supersedes the original sequencing and
deadline below. The approved reading path follows the modeled transport system;
CEUD calibration and cost diagnostics are in scope, while the CANOE-main adapter,
charging-simulation refactor and adoption constraints remain deferred. The
[publication guide](publication.md) records the current content/build route. The
original proposal below remains planning context, not evidence that a behavior or
validation result has been delivered. Current notebook names are
`vehicle_population_diagnosis.py` and `legacy_comparison_diagnosis.py`.

## Purpose and scope

Deliver a comprehensive, readable account of the transportation backend MVP as standard
repository documentation using **MkDocs and Material for MkDocs**. Maintain the content
in Markdown under `docs/`, publish it at the project's documentation URL, and provide
a simple, verified way to export the report as **one PDF from the same content**.

The primary reader is the supervisor assessing what the MVP delivers, how its inputs
and assumptions work, how it was validated and what remains unresolved. The same
documentation should help another researcher reproduce a build and inspect the evidence.

This is planned work, not a completed report or a new runtime authority. Start after
the efficiency reconciliation, assumptions review and the two exercises in
[transportation_validation.md](transportation_validation.md) have produced reportable
evidence. Follow `.agents/PLANS.md` for implementation. The current target is Sunday,
**November 1, 2026**; CANOE-main adapter work is optional parallel/follow-on scope.

The August 7 backend proposal supplies the original MVP purpose and acceptance criteria:
a reproducible transport SQLite build, provenance/integrity checks and an explanation
of legacy differences. Describe the current delivery scope and subsequent decisions
accurately. Its earlier Snakemake/Sphinx wording and September deadline do not override
the current repository workflow or this MkDocs request.

## Read and reconcile the existing evidence

Begin with [AGENTS.md](../AGENTS.md), current `pyproject.toml`/`uv.lock`, relevant path
configuration and the documentation below. Check claims against current executable
evidence before presenting them as delivered capabilities. The reviewed
[`v2.0` revision][repo-snapshot] records the preparation context only.

| Existing material | Role in the unified documentation |
| --- | --- |
| `docs/executive_summary.md` | Starting point for purpose, delivered scope, outcomes and limitations. |
| `docs/backend_architecture.md` | Current boundaries, ownership, native build lifecycle and standalone/integrated interfaces. |
| `docs/source_inventory.md`; `config/sources.yaml` | Source coverage, provenance and data quality; the registry retains machine-readable authority. |
| `docs/etl_flowcharts.md` | Parameter derivations, transformations, units and target tables. |
| `docs/assumptions.md` | Reviewed modelling assumptions, data limitations and their consequences. |
| `docs/insights/*.py` and their recorded evidence | Selected diagnostic visualizations and findings, with reproducible selections. |
| `docs/transportation_validation.md` and resulting outputs | Scope and results of the two required validation exercises; distinguish the plan from completed findings. |
| `docs/canoe_main_orchestrator.md` | Integration context and remaining adapter contracts, labelled according to actual completion. |
| `docs/codebase_diagnostic_snapshot.md` | Dated engineering context where useful; verify its findings and keep it outside the main research narrative unless directly relevant. |

Reuse existing pages and link to their canonical owners. Reconcile outdated statements
and add only the bridging narrative, results and operating instructions that are missing.
Do not maintain a second copy of the assumptions, configuration or validation logic.
Preserve the repository's review markers and user-controlled Mermaid content; rendering
the existing diagrams does not authorize redesigning them. A small README documentation
link may be added if needed for discovery; a broader README rewrite is outside this scope.

## Required report content

The navigation and PDF should follow a coherent reading order. The following is a
content contract, not a requirement to create one new file for every row.

| Section | Questions it must answer |
| --- | --- |
| MVP overview | What problem does the backend solve? What was delivered against the agreed scope? Which limitations or unfinished items affect use? |
| Backend and reproducibility | How do registered sources, YAML selections, transformations, validation and SQLite publication connect? How can another researcher reproduce the delivered build? |
| Sources and assumptions | Which evidence supports each parameter family? What are the material coverage gaps, mappings, units, proxies and reviewed modelling choices? |
| Baseline validation | What was compared with legacy evidence, on what basis and within which tolerances? Which differences are errors, intentional changes or unresolved? |
| Modified legacy scenario | How do stock-based and sales-based cohorts, utilization calibration and CEUD extension change the parameterization? How closely does the selected scenario reproduce the reference, and why do residual differences remain? |
| Charging infrastructure comparison | How were investment, fixed and variable costs made comparable? Which assumptions or conversion boundaries explain material differences? |
| Diagnostic findings | What do the existing notebooks show about fleet composition/survival, efficiency, parameter differences and relevant constraints? What conclusions and limitations follow from the displayed evidence? |
| Handoff and integration outlook | What commands, inputs and configuration reproduce the results? What remains for CANOE-main integration or later research work? |

Lead with the substantive findings, supported by selected figures and tables. Put
extended technical detail in the linked parameter/reference sections. Comprehensive
coverage means covering the supported backend and its material limitations, not copying
every log, planning note or notebook output into the main report.

State implementation and validation status precisely. A successful SQLite build is not
evidence of a completed energy-system optimization or full CANOE-main integration.
Likewise, matching energy through calibration is not independent validation of fleet
stocks. Include unsolved discrepancies where they affect interpretation.

## Notebook figures and interpretation

Use the existing diagnostic calculations and recorded evidence to produce the report
figures. Current candidates are:

- `vehicle_stock_diagnosis.py`: age composition, stock evidence and survival diagnostics.
- `ldv_efficiency_diagnosis.py`: efficiency comparisons and the completed discrepancy review.
- `transport_comparison_diagnosis.py`: material parameter differences and their drivers.
- `growth_constraint_diagnosis.py`: relevant constraint evidence, where it adds to the MVP findings.

Select views that support the report's conclusions. Preserve notebook functionality and
reuse its calculation/chart owners; the documentation layer should not reimplement ETL
or require an export of every possible interactive state.

**Prefer SVG for suitable figures**, with a verified raster fallback where export or PDF
rendering needs one. Altair supports SVG through `Chart.save()` and `vl-convert-python`
([official export documentation][altair-export]). Include a short caption and explanation
of each result: what is shown, the material finding, its modelling implication and limits.

Record the notebook/generator, evidence/build identity, relevant filters, region, class,
years, units and comparison baseline for each published figure. Use existing provenance
and notebook evidence bundles; a new general-purpose evidence framework is unnecessary.
Ensure all figures supporting the same comparison come from the intended recorded builds,
not whichever shared output was produced most recently.

Do not assume that `marimo export md` carries the charts: its documented output contains
source cells but omits cell outputs ([marimo documentation][marimo-markdown]). Export
selected figures explicitly. An optional static HTML notebook can supplement the report;
live notebook hosting is outside the MVP requirement. Essential information contained in
filters, tooltips or linked selections must also appear in the static figure, table or
narrative so that the PDF is complete.

Version Markdown and reusable export code. Keep notebook bundles and ad hoc captures
under the existing local/ignored artifact policy. Generate the selected publication
assets through a documented command and define their build/publishing location using
the repository's path conventions. The published assets must remain traceable and
regenerable without making raw notebook snapshots part of source control.

## MkDocs implementation and publication

Use a root `mkdocs.yml` with explicit navigation, the Material theme and only the
extensions/plugins needed for the actual content. Support existing tables, code,
equations, cross-references, figures and Mermaid diagrams. Check their rendering in
both the site and PDF before considering the export route settled.

Define the published file set and ordered PDF page set explicitly. Omitting a file from
`nav` does not itself exclude it from the built site; use the appropriate
[MkDocs inclusion/exclusion settings][mkdocs-config] and [print-page options][print-options].
Keep planning pages, notebook sources and evidence bundles distinct from the main report.
Reconcile repository-relative links outside `docs/`, such as links to `AGENTS.md`,
`config/` or `src/`, with suitable repository URLs or intentional documentation handling
so they remain useful on the hosted site.

Manage documentation dependencies with the repository's `uv` environment and lockfile,
preferably in a separate documentation dependency group. Keep documentation/export
dependencies outside the production ETL dependency path where practical. Record the
tested dependency set rather than relying on unconstrained latest packages.

**Compatibility note reviewed October 29, 2026:** Material's official changelog lists
9.8.0, requires Python 3.11 or newer, and records a `mkdocs < 2` constraint. Material
is in maintenance mode, with critical fixes/security updates through May 5, 2027
([official changelog][material-changelog]). Re-check compatibility when implementing
and lock the tested combination. Keep the requested MkDocs/Material choice for this
MVP; a future migration is a separate decision.

GitHub Pages is the natural repository-hosted candidate and is documented by Material
([publishing guide][material-publishing]). Inspect the existing Pages configuration,
root `index.html`, workflows and branch conventions before choosing a deployment path.
Do not assume the documentation should publish from `main` or replace an existing page.
Record the actual documentation URL after deployment and verify project-subpath links.

Document exact commands for installing dependencies, preparing the selected figures,
previewing, building, exporting the PDF and publishing. The MkDocs portion should use
the standard `mkdocs serve` and `mkdocs build --strict` commands through `uv`; record
the appropriate dependency-group invocation once configured.

Separate evidence preparation from rendering documentation. A prose-only documentation
build should consume the chosen prepared assets without silently fetching sources,
changing scenarios or rebuilding the transport database. Explain how a clean environment
obtains the required registered evidence or prepared publication assets. Use available
CI for the documentation build where practical, without creating a new ETL scheduler.

## One report and a verified PDF export

Choose a small, compatible export route and test it early on representative content:
one equation, an existing Mermaid diagram, a wide table and an SVG diagnostic figure.
The acceptance target is the complete report in one PDF, not merely printing individual
pages or offering separate notebook PDFs.

A candidate to evaluate is `mkdocs-print-site-plugin`, which assembles a print page and
documents browser **Print → Save as PDF** ([export instructions][print-pdf]). A short
verified browser workflow is sufficient; automated export is optional. Codex may choose
another compatible route if actual content renders more reliably. Avoid selecting a
PDF plugin before checking its platform/dependency needs and output quality.

The exported report must include a title, revision/date, contents, the required sections,
citations, figures, captions and material limitations. The website may additionally link
developer/planning references, but all substantive report conclusions and their supporting
evidence must be readable in the PDF. Avoid separately edited PDF prose.

Check page breaks, table widths, text size, equation rendering, diagram readability and
links. Expand or render content otherwise hidden behind tabs, collapsed sections or
interactive controls. Use reproducible static diagram/figure rendering where necessary;
do not silently omit content that fails to print. Deliver one representative exported
PDF and the instructions for regenerating it.

## Implementation sequence and acceptance

Keep the work in bounded stages under the repository's planning protocol:

1. **Confirm evidence and contents.** Reconcile the current documentation, completed
   validation outputs and remaining limitations; establish the report's reading order.
2. **Prove rendering.** Set up the minimum MkDocs configuration and test representative
   figures, diagrams, equations and tables through both the site and PDF route.
3. **Complete the narrative.** Integrate existing pages, both validation exercises and
   selected notebook findings; add reproduction instructions and source references.
4. **Verify and deliver.** Build, inspect the complete site/PDF, validate links and
   publish through the repository's chosen documentation route.

Completion requires:

- [ ] The supervisor can read one coherent account of scope, implementation, validation,
  findings and remaining limitations.
- [ ] Both requested validation exercises have evidence-backed results; any incomplete
  portion is explicitly identified and cannot be mistaken for passed validation.
- [ ] Quantitative claims and published figures trace to the recorded inputs/builds and
  reproduce through documented commands.
- [ ] The recorded environment runs `mkdocs build --strict` successfully; relevant links,
  assets, equations and diagrams work under the deployed project URL.
- [ ] One complete PDF has been exported and visually reviewed, including wide tables,
  SVGs, Mermaid diagrams and formerly interactive content.
- [ ] The site and PDF share the same substantive content and revision, with a verified
  preview/build/export/publication procedure.
- [ ] The documentation URL and reproducible PDF are available for handoff; temporary
  outputs and raw notebook snapshots follow the existing artifact/Git policy.

Keep broader research enhancements, theme migration, live notebook services, exhaustive
API documentation and unfinished adapter implementation outside this delivery boundary.
Update this plan's status and record implementation decisions/outcomes when the work is
performed; current code, configuration and validation results remain authoritative.

[repo-snapshot]: https://github.com/CANOE-main/canoe-transportation/tree/06a1b79b9ae81a1708de5d4d07ad3c49e5cc3b11
[material-changelog]: https://squidfunk.github.io/mkdocs-material/changelog/
[material-publishing]: https://squidfunk.github.io/mkdocs-material/publishing-your-site/
[print-pdf]: https://timvink.github.io/mkdocs-print-site-plugin/how-to/export-PDF.html
[marimo-markdown]: https://docs.marimo.io/guides/exporting/markdown/
[altair-export]: https://altair-viz.github.io/user_guide/saving_charts.html
[mkdocs-config]: https://www.mkdocs.org/user-guide/configuration/
[print-options]: https://timvink.github.io/mkdocs-print-site-plugin/options.html
