# AGENTS.md

## Mission and priority

Develop the CANOE transportation backend as a reproducible, auditable,
configuration-driven Python pipeline converting registered data, external-model
outputs, and reviewed assumptions into validated, provenance-traceable parameters
for the CANOE database used in the Temoa energy system optimization modeling framework.

Validate current scenario behavior and check regressions against comparable builds
within documented tolerances. Investigate and document deviations, distinguishing
intentional changes from regressions. New assumptions, scenarios, and representation
experiments must be explicit, evidence-based, configurable, and independently validated.

Support independently usable standalone Temoa/CANOE-ready SQLite compilation and
CANOE-main integration through the same transport parameterization and
validated-insertion contracts. Integrated assembly must respect upstream ownership
of shared structures and resources without duplicating transport logic.

## Repository responsibilities

- `config/paths.yaml` is the machine-readable artifact topology: canonical paths,
  layers, owners, producers, principal consumers, and validation surfaces.
- `config/sources.yaml` owns external-source identity, access, cache registration,
  source-native metadata, provenance, refresh notes, validation expectations, and
  source-specific data-quality evidence, defaults, and reviewed overrides.
- `config/scenarios/*.yaml` owns run selections, periods, regions, outputs, and modeling
  switches. See `config/scenarios/README.md` when authoring a scenario.
- `config/parameters/*.yaml` owns extraction maps, harmonization rules, class mappings,
  filters, bins, conversion factors, units, and parameter metadata.
- `src/fetching/` acquires, validates, caches, and normalizes source artifacts.
- `src/parameterization/` transforms normalized inputs into parameter-ready model
  contracts organized around coherent modeling behavior. It must not own SQLite transactions
  or CANOE-main orchestration.
- `src/build_transport.py` exposes caller-owned contribution preparation/insertion and owns
  the complete native scenario lifecycle: registered-source replay, prerequisite preparation,
  and standalone atomic database publication.
- `src/validation/` owns configuration, provenance, schema, insertion, integrity,
  parity, and validation-report logic.
- `docs/` and `docs/insights/` provide targeted explanation, diagnostics, and research
  context. `docs/assumptions.md` records enduring source and data challenges and the
  assumptions used to handle them. Docs are not runtime ETL owners or authorities for
  accepted development decisions.

User-selectable values belong in YAML. Python may contain source-invariant
implementation constants, but not modeling choices, source selections, reproducibility
controls, or output locations.

Use `inputs/0_cache/` for downloads, `inputs/0_external_models/` for registered model
outputs, `inputs/1_interim/` for normalized audit tables, `inputs/validation/` for review
and integrity evidence, and `inputs/2_processed/` for parameter-ready tables. Database
and run-validation reports belong in `outputs/validation/`; logs in `outputs/logs/`.
Backend-owned `inputs/0_canoe_template/` structure is not an external source and must not
receive external citations, DQ scores, or `Txx` identifiers.

Keep production ETL, opt-in development/review diagnostics, and research insight
workflows distinct. Do not silently mutate cached downloads, registered external-model
outputs, reviewed mappings or evidence, or user-controlled
representations unless the task explicitly places them in scope.

Keep one-off debug scripts, comparison exports and captures in ignored `tmp/`; remove
them before task completion and do not register them as source inputs or ETL provenance.
Keep generated validation evidence and notebook snapshots local and out of Git. Supported
commands regenerate these outputs from registered/reviewed inputs. The compact historical
references in `inputs/validation/transportation_reference/` are tracked notebook inputs.

## Context retrieval and presentation

Do not discover or retrieve Markdown trees by default. Normal documentation is limited
to `docs/backend_architecture.md`, `docs/etl_flowcharts.md`, `docs/assumptions.md`,
`docs/codebase_diagnostic_snapshot.md`, `docs/canoe_main_orchestrator.md`, and user-named
documents. Frontmatter routes them; it cannot enroll new ones.

For parameter-contract work, including reviewed configuration CSVs and their loader,
provenance or capture consumers, read [config/parameters/AGENTS.md](config/parameters/AGENTS.md).
Use its scope map to retrieve only the affected partitions and executable owners.

Select skills from session metadata; read only applicable `SKILL.md` files and required
references. Skill, ExecPlan, and scenario-authoring procedures are exceptions to this
documentation list; they do not override task scope or repository safeguards.

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

Retrieve `docs/canoe_main_orchestrator.md` only for a CANOE-main integration seam. Treat it
as a routing snapshot: verify the referenced upstream branch and affected interfaces before
acting, and do not turn unfinished upstream details into durable transport policy.

Edit docs only for a concrete task reason. In `docs/etl_flowcharts.md`,
`docs/backend_architecture.md`, and `docs/assumptions.md`, mark every LLM/agent-authored
substantive change `#to-review` unless its exact content is user-approved or the edit is
mechanical. Do not tag unchanged history or remove existing markers without explicit
user authorization. Do not add these tags anywhere else, including the two LLM-generated
snapshots, other docs, ExecPlans, or histories; absence of tags does not imply acceptance.

Use affected `config/paths.yaml` routes to bound seam changes, context and tests. Each new
module or artifact needs one owner and configured layer; opt-in diagnostics stay outside
ETL readiness. Current code, config, tests, schemas and generated validation evidence
outrank prose/plans. Treat plans/history as audit context; investigate uncertain
architecture or modeling choices.

Use `rg` for filenames and literal text; use
`uv run ast-grep --lang python -p '<pattern>' <path>` for structural Python searches
(adjust the language as needed).

## Data, provenance, and parity

- Trace external-data parameter rows to a registered source and transformation.
- Trace backend-owned structural rows to a documented internal `data_set` without
  fabricating an external source.
- The full applicable DQ contract is mandatory in the source-to-SQLite provenance chain;
  every external source/component supporting final rows must resolve it before insertion,
  with reviewable scores owned by the typed source registry.
- Make unit conversions explicit in config or code, log them, and test them.
- Log material missing, skipped, fallback, cleanup, count, validation, and artifact events.
- For regression work, compare matching current-backend scenarios or reproducible
  reference builds, document differences, and isolate intentional modeling changes.

Keep acquisition, physical validation, parsing, harmonization, and writing separately
testable. Preserve source-native audit values, make reruns safe, and prohibit hidden
network access during offline or `--no-download` execution.

Use diagnostic notebooks to visualize and explain the scientific rationale connecting
source evidence to model-ready assumptions; follow `docs/insights/AGENTS.md`. Use findings
to refine backend rules and documentation together. Implement accepted changes in their
owning configuration and code through bounded ExecPlans and validation under
`.agents/PLANS.md`, then refresh the affected explanations and diagnostics. Keep views of
current methods aligned with production; label historical comparisons and proposals.

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

## Scenario orchestration boundary

`uv run python src/build_transport.py --scenario <scenario.yaml>` is the supported
scenario-build command. It replays production prerequisites from registered inputs on
every invocation, offline by default; `--download-sources` explicitly permits acquisition
of missing caches. `--overwrite` permits replacing the configured database. Model and
source selections remain in YAML. Run scenarios serially while generated paths are shared.

Keep the ordered lifecycle in `build_transport.py` and transformations in their existing
source/parameter owners. Add new production prerequisites to this lifecycle and exercise
them in real offline tests; do not add a second scheduler or reuse outputs merely because
they exist. Manual review, mapping bootstrap, optional diagnostics and external-model
generation remain explicit operations. Caller-owned integration reuses prerequisite
preparation, contribution preparation and validated insertion without standalone publication.

Production replay must preserve broader opt-in evidence. A selected source edition may
refresh its annual model inputs and edition-scoped audits, but must not narrow aggregate
historical manifests, status tables or diagnostic handoffs. Explicit historical consumers
must reject incomplete registered coverage rather than silently display partial history.

## Implementation and completion

Use subagents for independent investigation, review or implementation when parallelism
or context isolation justifies coordination; handle small or sequential tasks directly.
Assign bounded scopes and exclusive write ownership; the lead reviews evidence, resolves conflicts and
validates integration. Contexts/worktrees and different SQLite destinations do not prove
artifact isolation. Resolve caches, registered/reviewed inputs, interim/processed tables,
manifests, reports, logs and database paths via `config/paths.yaml`; use disjoint disposable
write roots or serialize operations sharing mutable artifacts, including tests.

- Use `uv` for dependencies and commands; run focused `uv run pytest <test-path>` and
  `uv run ruff check <python-path>` checks.
- Add focused tests for parsing, conversions, config interfaces, schema insertion, and
  parity-sensitive transformations as applicable.
- Avoid speculative abstractions, parallel authorities, generic validation frameworks,
  and workflow structure without a demonstrated repository need.
- Preserve unrelated working-tree changes and keep commits to one logical change.

For multi-file, risky, staged, or explicitly planned work, follow `.agents/PLANS.md` and
maintain a task-specific ExecPlan. Planning procedure belongs there, not in this file.

A change is complete when applicable entrypoints run, expected artifacts exist, focused
validation passes or differences are documented, and the result is reproducible from
versioned files plus registered inputs. Record changed assumptions or interfaces in
their durable owner.
