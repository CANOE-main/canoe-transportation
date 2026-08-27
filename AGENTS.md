# AGENTS.md

## Mission and priority

This repository is the v2.0 refactor of the CANOE transportation backend. It replaces
the legacy Excel-centered compiler with a reproducible, auditable Python pipeline that
builds Temoa/CANOE-ready SQLite databases for Canadian multi-sector energy system modelling.

Baseline reproduction is the first priority. Reproduce legacy-equivalent outputs within
documented tolerances and explain or accept parity gaps before adding new assumptions,
scenarios, representation experiments, or sensitivity workflows.

## Repository responsibilities

- `config/paths.yaml` is the machine-readable artifact topology: canonical paths,
  layers, owners, producers, principal consumers, and validation surfaces.
- `config/sources.yaml` owns external-source identity, access, cache registration,
  source-native metadata, provenance, refresh notes, validation expectations, and
  source-specific data-quality evidence, defaults, and reviewed overrides.
- `config/scenarios/*.yaml` owns run selections, periods, regions, outputs, and modelling
  switches. See `config/scenarios/README.md` when authoring a scenario.
- `config/parameters/*.yaml` owns extraction maps, harmonization rules, class mappings,
  filters, bins, conversion factors, units, and parameter metadata.
- `src/fetching/` acquires, validates, caches, and normalizes source artifacts.
- `src/parameterization/` transforms normalized inputs into model parameters.
- `src/validation/` owns configuration, provenance, schema, insertion, integrity,
  parity, and validation-report logic.
- `workflow/` provides lightweight scenario-level dependency and artifact orchestration;
  substantive transformations remain in importable Python modules.
- `docs/` and `docs/insights/` provide targeted explanation, diagnostics, and research
  context. They are not runtime ETL owners or authorities for accepted assumptions.

User-selectable values belong in YAML. Python may contain source-invariant
implementation constants, but not modelling choices, source selections, reproducibility
controls, or output locations.

Use `inputs/0_cache/` for downloads, `inputs/0_external_models/` for registered model
outputs, `inputs/1_interim/` for normalized audit tables, `inputs/validation/` for review
and integrity evidence, and `inputs/2_processed/` for parameter-ready tables. Database
and run-validation reports belong in `outputs/validation/`; logs in `outputs/logs/`.
Backend-owned `inputs/0_canoe_template/` structure is not an external source and must not
receive external citations, DQ scores, or `Txx` identifiers.

Keep production ETL, opt-in development/review diagnostics, and research insight
workflows distinct. Do not silently mutate cached downloads, registered external-model
outputs, `legacy_backend/`, reviewed mappings or evidence, or user-controlled
representations unless the task explicitly places them in scope.

## Context retrieval and presentation

Do not discover or retrieve Markdown trees by default. Normal agent-facing docs are limited
to `docs/backend_architecture.md`, `docs/etl_flowcharts.md`, `docs/assumptions.md`,
`docs/codebase_diagnostic_snapshot.md`, and documents the user explicitly names.
Frontmatter routes them; it cannot enroll new ones.

`README.md` is normally unnecessary implementation context and is the intentional human
presentation/redundancy exception. Never routinely redesign, deduplicate, condense,
synchronize, or edit it; modify it only for user-requested work or a necessary README
correction specifically placed in scope.

Retrieve `docs/backend_architecture.md` only for structure, ownership, orchestration, or
artifact placement. From `docs/etl_flowcharts.md`, retrieve only the affected parameter
family or shared dependency; its Mermaid diagrams are user-controlled and may change only
when the user explicitly requests a Mermaid or flowchart change.

Retrieve `docs/codebase_diagnostic_snapshot.md` only when module boundaries, architecture
fitness, structural complexity, shared infrastructure, development/runtime separation, or
an efficiency/refactor decision is in scope. Read only the relevant diagnostic sections,
never use it as ordinary source or parameter context, and verify its point-in-time findings
against current executable evidence before acting.

Edit docs only for a concrete task reason. Unless exact content is user-approved or the edit
is mechanical, every Codex-authored substantive reviewable change—assumption, interpretation,
evidence/table/bullet, research conclusion, or data-gap/challenge resolution—must carry
`#to-review`. Do not tag unchanged history; especially protect `docs/assumptions.md` and
`docs/etl_flowcharts.md`.

For a repository or artifact-layer seam, use the affected `config/paths.yaml` routes to
bound impact, context, and tests. New modules and artifacts need one owner and configured
layer; keep opt-in diagnostics out of normal ETL readiness. Current code, configuration,
tests, schemas, and generated validation evidence outrank stale prose or plans. Treat
plans and histories as orientation/audit evidence, and investigate uncertain architecture
or modelling choices rather than inventing them.

## Data, provenance, and parity

- Trace external-data parameter rows to a registered source and transformation.
- Trace backend-owned structural rows to a documented internal `data_set` without
  fabricating an external source.
- The full applicable DQ contract is mandatory in the source-to-SQLite provenance chain;
  every external source/component supporting final rows must resolve it before insertion,
  with reviewable scores owned by the typed source registry.
- Make unit conversions explicit in config or code, log them, and test them.
- Log material missing, skipped, fallback, cleanup, count, validation, and artifact events.
- During baseline work, preserve legacy-equivalent assumptions, compare against the
  reference SQLite, document differences, and isolate intentional changes in named
  scenarios or commits.

Keep acquisition, physical validation, parsing, harmonization, and writing separately
testable. Preserve source-native audit values, make reruns safe, and prohibit hidden
network access during offline or `--no-download` execution.

Diagnostic or research evidence does not become an accepted modelling assumption merely
because it exists. Promote it only through the owning configuration or contract, with
review and validation appropriate to the changed interface.

## Validation interfaces

Place validation at the boundary whose trust it protects. Validate physical source
artifacts before harmonization; preserve normalized audit evidence in `inputs/1_interim/`;
use `inputs/validation/` for review/integrity evidence rather than as parameter-ready or
final database authority; and validate parameter-ready outputs before schema insertion.

Use Pydantic where runtime trust is required:

- At source and config interfaces, type stable shared fields and adapter requests.
  Keep source-native extensions with their adapters and validate before I/O.
- At the SQLite seam, construct the relevant `canoe_schema.v4_0` row model immediately
  before insertion, register provenance first, reject invalid or conflicting rows, and
  run focused provenance, foreign-key, and integrity checks before atomic publication.

Do not put arbitrary DataFrames, mutable pipeline state, or ordinary internal classes
into Pydantic models. Do not create speculative universal source schemas or generic
rules engines.

The pinned `canoe-schema` package owns both v4 DDL and final row models. Instantiate
databases with `canoe_schema.get_sql_schema("4.0")`. Local SQL is comparison evidence
only, never a runtime fallback or parallel authority. A small tested compatibility
adapter is acceptable only for a documented upstream gap.

Checks should be proportional to the changed interface and cover relevant schema,
keys, coverage, units, normalization, provenance, skipped/fallback behavior, and parity.

## Snakemake boundary

Use Snakemake as the scenario-level dependency and artifact orchestrator once stage
interfaces are stable. Keep rules short, legible, and limited to declared inputs,
outputs, logs, parameters, and importable Python entrypoints. Direct Python entrypoints
remain valid for isolated development and validation; not every development step belongs
in the DAG. Snakemake must not become a second transformation implementation layer.

## Implementation and completion

- Use `uv` for dependency management and commands.
- Add focused tests for parsing, conversions, config interfaces, schema insertion, and
  parity-sensitive transformations as applicable.
- Avoid speculative abstractions, parallel authorities, generic validation frameworks,
  and workflow structure without a demonstrated repository need.
- Preserve unrelated working-tree changes and keep commits to one logical change.

For multi-file, risky, staged, or explicitly planned work, follow `.agents/PLANS.md` and
maintain a task-specific ExecPlan. Planning procedure belongs there, not in this file.

A change is complete when its entrypoint runs, expected artifacts exist, focused
validation passes or differences are documented, and the result is reproducible from
versioned files plus registered inputs. Record changed assumptions or interfaces in
their durable owner.
