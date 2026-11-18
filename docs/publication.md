# Reproduce the evidence and publication

The site and PDF use the same prepared Markdown, equations, tables and selected
figures. Routine rendering uses the checked-in assets. Regenerate evidence when
a published result changes, and keep the evidence identity with the figure.

## Prepare model evidence explicitly

Install the locked documentation dependencies with
`uv sync --frozen --group docs`. For evidence regeneration, also supply the
[registered inputs](getting_started.md#install-the-backend). Run preparation and
exports serially. A separate output database name does not isolate the derived
inputs, and each notebook expects the evidence from its own preparation route.

### Ontario comparisons, costs and non-road parameters

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/validation_baseline.yaml --overwrite
uv run --offline python -m validation.transportation_research --scenario config/scenarios/validation_baseline.yaml --write-report
uv run --offline python -m validation.charging_cost_comparison --scenario config/scenarios/validation_baseline.yaml --write-report
uv run --offline python -m validation.vehicle_cost_diagnostic --scenario config/scenarios/validation_baseline.yaml --write-report
uv run --offline python -m validation.documentation_support --write-nonroad-report
```

The Ontario reference is prospective 2025 with fixed MOVES lifetimes and
representative new LDV ranges. The activity and energy comparison also reads
curated historical reference evidence. Its fitted energy view requires the
matched calibration pair below. The cost and non-road figures can be regenerated
from the Ontario control alone.

### Optional energy calibration comparisons

Build both Ontario cases in the same checkout so their evidence records the same
input locations. Keep the control database and report. The calibrated case changes
only the energy-calibration switch. Run the paired check before building the
regional capture for the anchor heatmaps.

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/validation_energy_calibration.yaml --overwrite
uv run --offline python -m validation.ceud_calibration_diagnostic --baseline outputs/sqlite/validation_baseline.sqlite --baseline-report outputs/validation/validation_baseline.json --calibrated outputs/sqlite/validation_energy_calibration.sqlite
uv run --offline python src/build_transport.py --scenario config/scenarios/validation_energy_calibration_regions.yaml --overwrite
```

The paired audit records input fingerprints and checks final SQLite parameters.
The regional capture supplies plotted anchor changes for ten regions. The two
have distinct scopes. The [energy chapter](model/energy.md#optional-ceud-energy-calibration)
explains the fitted boundary and what the evidence establishes.

### Road population, efficiency and retirement

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/notebook_diagnostics.yaml --overwrite
uv run --offline python -m validation.documentation_support --write-report
```

The diagnostic preset captures an Ontario 2023 build for population and
efficiency. Before opening `stock_cohort_diagnosis.py`, prepare the CEUD source
tables for all regions in the default scenario. The cohort notebook combines
those tables with the captured Report A history, reviewed mapping and survival
inputs, the selected Report 5, and the registered StatCan inputs.

```sh
uv run --offline python -m fetching.nrcan_ceud --scenario config/scenarios/default.yaml --no-download
```

The cohort notebook recalculates both survival selections against the 2025
reference. The Ontario fleet illustration uses separately configured source
years and reports marginal shares. See [the fleet chapter](model/vehicles.md#ontario-as-100-vehicles)
for denominators.

### Travel survey, charging profiles and charger infrastructure {#charging-profiles-and-charger-infrastructure}

Prepare the NHTS survey tables and the six registered RAMP runs before opening
`travel_survey_diagnosis.py` and `ldv_charging_diagnosis.py`, respectively.

```sh
uv run --offline python -m fetching.nhts_ldv_travel --scenario config/scenarios/validation_charging_nhts_uncontrolled.yaml
uv run --offline python -m fetching.ramp_run_profiles --scenario config/scenarios/validation_baseline.yaml
```

The RAMP command validates the frozen source bytes and writes normalized hourly
evidence. Open `charger_infrastructure_diagnosis.py` after the Ontario control
above prepares the Dunsky, currency and stock evidence. The [charging](model/charging.md)
and [infrastructure](model/infrastructure.md) chapters explain the scenarios and
interpretation limits.

For an explicit charger case with `charging_profiles.travel_behavior_source: none`,
the following optional screen uses the matching completed SQLite and build report.

```sh
uv run --offline python -m validation.charger_feasibility --scenario path/to/explicit.yaml --write-report
```

It tests annual throughput and demand envelopes under the exact equipment
mix. It does not model hourly operation, upstream supply or cross-period
investments.

### Growth constraint research

Supply the registered growth evidence, build the default scenario, and prepare
the diagnostic before opening `growth_constraint_diagnosis.py`. The analysis
checks that the fleet inputs belong to the selected scenario, so run it before
building another scenario in the same artifact root.

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/default.yaml --overwrite
uv run --offline python -m parameterization.road_adoption_constraints
```

To acquire missing evidence, run
`uv run python -m fetching.technology_growth_evidence --download-missing`.
Add `--papers-dir <directory>` if you need to register supplied study PDFs. The
[adoption chapter](model/adoption.md) describes how to interpret the results.

### Export selected figures

```sh
uv run --offline python scripts/export_documentation_figures.py --groups cost nonroad
```

Use additional groups `notebooks`, `calibration`, `fleet`, `comparison`,
`charging` or `infrastructure` after preparing their evidence. Omitting
`--groups` requests every group. Display selections live in
[`config/parameters/documentation.yaml`](../config/parameters/documentation.yaml).
The [figure manifest](assets/figures/manifest.json) records stable IDs, hashes,
generators, controls, year domains and evidence identity. Use the ID in the
caption to locate a figure's record. Static exports preserve the documented
scope even if live notebooks or scenarios are subsequently updated.

The selected SVGs, exact fleet share and non-road parameter tables, and figure
manifest travel with the publication. Raw captures and notebook session outputs
remain local. Regenerate changed or missing captures through their producer so
the configuration and input hashes continue to identify the evidence.

## Build and preview the site

Prepare the pinned Mermaid and MathJax packages once. This is the network-enabled
part of the documentation build.

```sh
uv sync --frozen --group docs
uv run --group docs python scripts/build_docs.py prepare-assets
uv run --offline --group docs python scripts/build_docs.py serve
```

Open the project URL printed by `serve` under `/canoe-transportation/`. MkDocs
watches published Markdown and assets and refreshes the browser after each
successful rebuild. The scenario page comes from
[`config/scenarios/README.md`](../config/scenarios/README.md). For port conflicts,
use `serve --port 8001`. Stop the server with **Ctrl+C**. Restart after changing
Python hooks or dependencies.

Preview runs in a temporary site. Create and check the static publication with
the supported build command.

```sh
uv run --offline --group docs python scripts/build_docs.py build
```

The strict build verifies the site and its internal links and records a build
identity. `mkdocs.yml` sets the page order and published file set. Repository
links provide access to source files and notebook code.

## Export the PDF

Install Playwright Chromium or select an installed Chrome with
`pdf --browser-channel chrome`. Rebuild the static site after any source or
asset edits before exporting.

```sh
uv run --group docs playwright install chromium
uv run --offline --group docs python scripts/build_docs.py pdf
```

The exporter checks the static build identity, renders the combined print page
locally, waits for equations and diagrams, and records page review evidence.
Open `print_page/` from the built site's root to read or print the chapters
together.

| Output | Configured path |
| --- | --- |
| Static site | `outputs/documentation/site/` |
| Combined PDF | `outputs/documentation/canoe_transportation_report.pdf` |
| Build/render records and page review images | `outputs/validation/documentation/` |
| Prepared publication assets | `docs/assets/figures/` |

The manual documentation workflow builds the site and PDF from committed
assets and uploads a review artifact. Deployment to GitHub Pages is a separate
option and defaults to off.
