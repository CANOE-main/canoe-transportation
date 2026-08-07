# AGENTS.md

## Mission and priority

This repository is the v2.0 refactor of the CANOE transportation backend. It replaces
the legacy Excel-centred compiler with a reproducible, auditable Python pipeline that
builds Temoa/CANOE-ready SQLite databases for Canadian multi-sector energy system modelling.

Baseline reproduction is the first priority. Reproduce legacy-equivalent outputs within
documented tolerances and explain or accept parity gaps before adding new assumptions,
scenarios, representation experiments, GSA, or MGA workflows.

## Repository responsibilities

- `config/paths.yaml` owns canonical directories and important artifact paths.
- `config/sources.yaml` owns external-source identity, access, cache registration,
  source-native metadata, provenance, refresh notes, validation expectations, and
  reviewed data-quality overrides.
- `config/scenarios/*.yaml` owns run selections, periods, regions, outputs, and modelling
  switches. See `config/scenarios/README.md` when authoring a scenario.
- `config/parameters/*.yaml` owns extraction maps, harmonization rules, class mappings,
  filters, bins, conversion factors, units, and parameter metadata.
- `src/fetching/` acquires, validates, caches, and normalizes source artifacts.
- `src/parameterization/` transforms normalized inputs into model parameters.
- `src/validation/` owns configuration, provenance, schema, insertion, integrity,
  parity, and validation-report logic.
- `workflow/` declares dependencies and artifacts; substantive transformations remain
  in importable Python modules.

User-selectable values belong in YAML. Python may contain source-invariant
implementation constants, but not modelling choices, source selections, reproducibility
controls, or output locations. Keep public interfaces small and place implementation
depth behind the module that owns the behavior.

Use `inputs/0_cache/` for authoritative downloads, `inputs/0_external_models/` for
registered external-model outputs, `inputs/1_interim/` for normalized audit tables, and
`inputs/2_processed/` for parameter-ready tables. Never hand-edit cached source files.
Backend-owned structure under `inputs/0_canoe_template/` is not an external source and
must not receive external citations, DQ scores, or `Txx` identifiers.

## Context retrieval and presentation

`README.md` is a human-facing presentation layer and normally is unnecessary context
for implementation tasks. It is the repository's sole intentional redundancy
exception: it may mirror selected backend architecture and ETL flowcharts from the
structural references to showcase the project. Do not remove or condense that
duplication merely to deduplicate context; change it only when the user explicitly
requests a README redesign.

Retrieve `docs/backend_architecture.md` only when a task affects repository structure,
module ownership, orchestration seams, or artifact placement. Retrieve only the affected
parameter family or direct shared dependency from `docs/etl_flowcharts.md`. Read only
the relevant sections. Before implementing planned documentation, verify it against
current code, config, tests, schemas, and validation evidence.

When you need to search external library/API docs, use Context7.

## Data, provenance, and parity

- Trace external-data parameter rows to a registered source and transformation.
- Trace backend-owned structural rows to a documented internal `data_set` without
  fabricating an external source.
- Make unit conversions explicit in config or code, log them, and test them.
- Log missing inputs, skipped or dropped rows, fallbacks, cleanup, pruning, row counts,
  validation differences, and generated artifacts.
- Omitted external-source DQ fields default to `(5, 5, 5, 5, 5)`. Use family or
  component overrides only when reviewed evidence supports them.
- During baseline work, preserve legacy-equivalent assumptions, compare against the
  reference SQLite, document differences, and isolate intentional changes in named
  scenarios or commits.

Across ETL modules, keep acquisition, physical validation, parsing, harmonization, and
writing separately testable. Validate each physical artifact before harmonization,
preserve source-native values needed for audit, make reruns safe, and prohibit hidden
network access during declared offline or `--no-download` execution.

Treat `legacy_backend/` as read-mostly validation evidence. Inspect only the artifact
needed for the parity question and do not edit legacy files unless cleanup or migration
is explicitly requested. Large legacy binaries and databases are evidence, not active
development targets.

## Validation interfaces

Use Pydantic where runtime trust is required:

- At source and config interfaces, type stable shared fields and implemented adapter
  requests. Keep source-native extensions with their adapters, validate requests before
  I/O, and keep validation close to the interface it protects.
- At the SQLite seam, construct the relevant `canoe_schema.v4_0` row model immediately
  before parameterized insertion, register provenance first, reject invalid or
  conflicting rows, and finish with focused provenance, foreign-key, and integrity
  checks before atomic publication.

Do not put arbitrary DataFrames, mutable pipeline state, or ordinary internal classes
into Pydantic models. Do not create speculative universal source schemas or generic
rules engines.

The pinned `canoe-schema` package owns both v4 DDL and final row models. Instantiate
databases with `canoe_schema.get_sql_schema("4.0")`. Local SQL is comparison evidence
only, never a runtime fallback or parallel authority. A small tested compatibility
adapter is acceptable only for a documented upstream gap.

Validation outputs belong in `outputs/validation/`; logs belong in `outputs/logs/`.
Checks should be proportional to the changed interface and cover relevant schema,
keys, coverage, units, normalization, provenance, skipped/fallback behavior, and parity.

## Snakemake boundary

Use Snakemake when multiple stable stages need dependency and artifact coordination.
Keep rules short, legible, named by stage or parameter family, and limited to declared
inputs, outputs, logs, parameters, and calls to importable Python entrypoints.
Snakemake must not become a second implementation layer. Stabilize direct Python
interfaces and artifact contracts first; workflow failures do not block isolated module
development unless orchestration is in scope.

## Implementation and completion

- Use `uv` for dependency management and commands.
- Prefer small readable functions, useful typing, `pathlib.Path`, explicit columns,
  auditable pandas transformations, and standard logging.
- Add focused tests for parsing, conversions, config interfaces, schema insertion, and
  parity-sensitive transformations as applicable.
- Avoid large frameworks, hypothetical seams, and abstractions that do not reduce
  complexity.
- Preserve unrelated working-tree changes and keep commits to one logical change.

For multi-file, risky, staged, or explicitly planned work, follow `.agents/PLANS.md` and
maintain a task-specific ExecPlan. Planning procedure belongs there, not in this file.

A change is complete when the relevant entrypoint or workflow target runs, expected
artifacts and logs are produced, focused tests and validation pass or differences are
documented, and the result is reproducible from version-controlled files plus registered
inputs. Record changed assumptions, source conventions, interfaces, or architecture in
the owning config, test, validation note, documentation, or ExecPlan outcome.
