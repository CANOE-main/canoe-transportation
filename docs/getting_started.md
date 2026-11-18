# Build and inspect a scenario

## Install the backend

Install Git and [uv](https://docs.astral.sh/uv/getting-started/installation/). The
project requires Python 3.13 or later and pins the CANOE schema package in its
lockfile.

```sh
git clone --branch main https://github.com/CANOE-main/canoe-transportation.git
cd canoe-transportation
uv sync --frozen
```

Supply the registered source caches, reviewed mappings and manual tables, and the
external-model outputs selected by the scenario. Consult the
[source inventory](source_inventory.md) for access and the
[scenario guide](scenario_configuration.md) for required selections. A model build
can read registered GREET outputs offline. Generating those outputs is a separate
operation that requires Windows and desktop Excel.

## Build the default scenario

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/default.yaml
```

The default models ten regions with prospective periods anchored at 2025,
sales-derived car and truck cohorts with EPA MOVES survival curves, pooled LDV
and MHDV charging, and fixed within-family shares for new LDV plug-in ranges.
LDV charging uses the registered NHTS uncontrolled profile. BCT combines British
Columbia and the territories, subject to the source-specific coverage in the
[assumptions](assumptions.md).

The builder replays production prerequisites on every invocation. Add
`--overwrite` to replace the configured SQLite. If a registered downloadable
cache is missing, add `--download-sources` to permit acquisition. Review new
source editions through the [source update workflow](#review-a-source-update).

!!! warning "Run scenarios serially"
    Intermediate outputs are shared. Separate database filenames alone do not
    isolate two builds. Use separate disposable artifact roots for concurrent work.

For a custom scenario, copy the full default YAML and change its selections and
output names. The [scenario guide](scenario_configuration.md) lists supported
combinations, named validation presets and `--processed-only` for preparing
model-ready CSVs without publishing SQLite.

## Read the build evidence

The default database is `outputs/sqlite/canoe_transport_baseline.sqlite`. Start
with its configured report, `outputs/validation/baseline_database_bootstrap.json`.
Check scenario and source identity, row counts, parameter support, provenance,
foreign keys and integrity. Follow a parameter's intermediate and processed
tables to investigate gaps, warnings or fallback values. A missing row has its
own disposition and can convey a different result from a zero value.

| Path | Contents |
| --- | --- |
| `inputs/1_interim/` | Normalized source evidence, including native identifiers, years and units |
| `inputs/2_processed/` | Parameter-ready outputs and preparation audits |
| `inputs/validation/` | Reviewed or generated source, mapping and parameter checks |
| `outputs/validation/`, `outputs/logs/` | Build reports, optional comparisons and build logs |

To compare two current v4 builds, set `comparison.mode: scenario` and point
`comparison.reference_sqlite` to the reference database. Read the
[comparison settings](scenario_configuration.md#sqlite-comparisons) for tolerances
and optional provenance comparison. SQLite checks and row comparisons describe
the database. A solver run answers a different question about operating and
investment constraints in a supplied model.

Run focused repository tests from the root when changing code or configuration.

```sh
uv run --offline pytest tests/test_config.py tests/test_parameter_rules.py -q
```

## Explore a diagnostic notebook {#prepare-diagnostics-explicitly}

Prepare evidence for the specific notebook, then open it with marimo. For example,
the Ontario population and efficiency notebooks share a captured build.

```sh
uv run --offline python src/build_transport.py --scenario config/scenarios/notebook_diagnostics.yaml --overwrite
uv run --offline marimo run docs/insights/ldv_efficiency_diagnosis.py
```

Read the notebook's scenario, source and build identity before interpreting its
figures. A capture records the actual configuration and input hashes. Regenerate
changed or missing evidence through its producer before reloading the app. Other
notebooks read registered simulations, prepared diagnostics or curated historical
evidence. The [publication guide](publication.md#prepare-model-evidence-explicitly)
routes those workflows.

## Review a source update

```sh
uv run python -m fetching.source_updater --scenario config/scenarios/default.yaml
```

The updater writes a proposal and audit. Review the source-specific diff before
using the same command with `--source <registry-source-key> --apply`, for example
`--source nrcan_fuel_consumption_ratings --apply`. Rebuild
and compare affected parameters after adoption.
