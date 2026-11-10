# Reproduce the evidence and publication

The website and PDF use the same substantive Markdown, equations, tables and
prepared figures. Interactive exploration stays in the notebooks. Rendering the
documentation does not download transport sources, change a scenario or run ETL.

## Prepare model evidence explicitly

Registered source bytes, reviewed mappings and external-model outputs must already
be available as described in [Getting started](getting_started.md). Run this
sequence serially: the scenario databases have separate names, but production
preparation paths are shared.

```sh
uv sync --frozen --group docs
uv run python src/build_transport.py --scenario config/scenarios/validation_baseline.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/validation_baseline.yaml --write-report
uv run python -m validation.charging_cost_comparison --scenario config/scenarios/validation_baseline.yaml --write-report
uv run python -m validation.vehicle_cost_diagnostic --scenario config/scenarios/validation_baseline.yaml --write-report
uv run python -m validation.documentation_support --write-nonroad-report
uv run python src/build_transport.py --scenario config/scenarios/validation_energy_calibration.yaml --overwrite
uv run python -m validation.ceud_calibration_diagnostic --baseline outputs/sqlite/validation_baseline.sqlite --baseline-report outputs/validation/validation_baseline.json --calibrated outputs/sqlite/validation_energy_calibration.sqlite
uv run python src/build_transport.py --scenario config/scenarios/notebook_diagnostics.yaml --overwrite
uv run python -m validation.documentation_support --write-report --write-nonroad-report
uv run python scripts/export_documentation_figures.py
```

The Ontario baseline and calibrated databases use identical fleet, activity,
representation, currency and source selections. Their only modeling difference
is the explicit energy-calibration switch. The stock/efficiency notebook build
uses observed 2023 and NHTSA/NEMS survival curves for its separate diagnostic
purpose. It is **not** the calibration off/on comparison. Figure captions and the
manifest retain those distinct identities.

The cohort notebook also recalculates both survival alternatives at reference
2025 across the default regions, using already prepared registered inputs. It
records those source/configuration hashes and invokes no transport build. Its
source-based comparisons are distinct from final SQLite off/on checks.

The commands capture source/configuration hashes and build/database identities.
Changed or missing evidence must be regenerated through its owner; editing a
capture hash is not a refresh. Local evidence bundles and notebook session state
remain ignored. Selected SVGs, their manifest and the exact Ontario share table
are the prepared publication inputs that travel with the documentation.

The paired calibration audit uses the baseline build report's database/configuration
hashes and 31 efficiency-input fingerprints, together with the calibrated immutable
capture. It verifies final uncalibrated/calibrated SQLite rows. Separate baseline
preaggregation annual/period exports were not captured, so it does not claim byte
equality of those intermediate files. The calibrated capture retains unadjusted
annual/period values; focused transformation tests establish common-factor
propagation before aggregation.

## Figure identities and selections

Every nonexempt diagnostic plot has an analytical title and a visible stable ID.
IDs describe the subject rather than its current cell position; repeated panels
carry class/powertrain/unit suffixes. Use the ID when requesting a correction.

| Notebook | Identity family and purpose |
| --- | --- |
| `vehicle_population_diagnosis.py` | `population-*`: mapping coverage, class composition, age and apparent retention. |
| `stock_cohort_diagnosis.py` | `cohort-*`: acquisitions, survivor calibration, vintage aggregation and retirement. |
| `ldv_efficiency_diagnosis.py` | `ldv-eff-*`: composition, range shares, consumption bridges and comparisons. |
| `vehicle_cost_diagnosis.py` | `vehicle-cost-1` to `vehicle-cost-5`: source prices, model investment, age-dependent M&R, service costs and range aggregation. |
| `ceud_calibration_diagnosis.py` | `ceud-cal-1` to `ceud-cal-4`: closure, factors, coverage and trajectories. |
| `documentation_support.py` | `fleet-100-1`: Ontario marginal compositions; `nonroad-1`: final non-road efficiency parameters. |

The blanket title/ID requirement exempts exactly
`growth_constraint_diagnosis.py`, `legacy_comparison_diagnosis.py` and
`transportation_validation_diagnosis.py`. Three selected plots in the last
notebook nevertheless share publication IDs: `comparison-charging`,
`comparison-charging-residual` and `comparison-activity-energy`.

Presentation selections live in `config/parameters/documentation.yaml`; they do
not control model parameters. The [figure manifest](assets/figures/manifest.json)
records exported filenames/hashes, generators, actual notebook control values,
chart units and year domains, and captured evidence identity. It hashes plotted
data without publishing raw tables. Selective exports use, for example,
`uv run python scripts/export_documentation_figures.py --groups cost nonroad`;
the other groups are `notebooks`, `calibration`, `fleet` and `comparison`.
Each requested figure prefix must produce a chart, and duplicate identities fail.
Essential labels and units are visible in the static figures. Captions
state what the evidence supports and what it cannot establish.

## Build and preview the prepared publication

Install locked dependencies, then prepare the explicitly pinned local Mermaid and
MathJax packages. This preparation uses network access; later rendering uses their
verified local bytes.

```sh
uv sync --frozen --group docs
uv run --group docs python scripts/build_docs.py prepare-assets
uv run --group docs python scripts/build_docs.py build
uv run --group docs python scripts/build_docs.py serve
```

Preview the project subpath printed by `serve`, including `/canoe-transportation/`.
The strict build checks internal links and assets. `mkdocs.yml` defines the
explicit page order and file set. The scenario page is generated from
`config/scenarios/README.md`; edit that canonical source. Local evidence, raw
notebooks, planning files and unrelated documentation are not copied into the site.
Notebook/source links point to the repository.

## Export the same content as PDF

Install the PDF browser explicitly when using Playwright's bundled Chromium:

```sh
uv run --group docs playwright install chromium
uv run --group docs python scripts/build_docs.py pdf
```

An installed Chrome can instead be selected with
`uv run --group docs python scripts/build_docs.py pdf --browser-channel chrome`.
The exporter loads only the prepared local site, waits for equations and Mermaid,
expands collapsible content, rejects remote requests and broken assets, and checks
that publication inputs have not changed since the site build. Prose uses A4;
selected wide/tall figures use A3, and large preserved Mermaid diagrams use A2
landscape sheets for readability.

| Deliverable | Configured path |
| --- | --- |
| Static site | `outputs/documentation/site/` |
| Complete PDF | `outputs/documentation/canoe_transportation_report.pdf` |
| Build/render identities and page review images | `outputs/validation/documentation/` |
| Selected publication assets | `docs/assets/figures/` |

The manual GitHub Actions workflow builds both deliverables from committed prepared
assets and uploads a review artifact. Its `deploy` input defaults to false. The
optional publishing job targets the repository project site
`https://canoe-main.github.io/canoe-transportation/`; enabling Pages or dispatching
publication is a separate action. No transport inputs are required for routine
site/PDF rendering.

## Interpret the checks separately

Configuration checks establish supported selections; parameter checks establish
keys, units, coverage and conservation; source/DQ checks establish traceability;
legacy comparisons establish agreement on their recorded comparison basis.
Calibration closure establishes agreement with fitted totals. None of those
checks is an optimization run or independent validation of each technology.
Reported residuals and unsupported coverage remain part of the research result.
