---
title: Codebase diagnostic snapshot
role: Point-in-time evidence for codebase complexity, modularity, duplication, ownership drift, and refactor planning.
retrieve_when: A task affects module boundaries, architecture fitness, codebase complexity, shared infrastructure, development/runtime separation, or an efficiency refactor.
read_scope: Read only the relevant diagnostic sections unless the task is explicitly repository-wide.
verify: Reconcile snapshot findings against current code, config, tests, schemas, workflow, and generated evidence before acting.
last_diagnostic_run: 2026-10-01
---

# Codebase diagnostic snapshot

This refresh replaces the 2026-08-20 assessment. The main concerns are now long parameter
assembly functions, incomplete workflow dependency coverage, preparation/publication side
effects, and a few dispersed helper responsibilities. Large source-specific modules and
highly reused trust boundaries need different treatment. The findings below support later
bounded decisions; they do not authorize a refactor or a modelling change.

Current code, configuration, tests, schemas, workflow, and generated evidence outrank this
point-in-time snapshot. Use [artifact routes](../config/paths.yaml) to bound a subsequent
change, [backend architecture](backend_architecture.md) for ownership, and
[CANOE-main context](canoe_main_orchestrator.md) only when working on the integration seam.
This document does not replace those owners.

## 1. Inspection scope and method

### Repository state

- Inspected on 2026-10-01, branch `v2.0`, HEAD
  `23329ea1f368ebef30bb854f9ae4312da72658de`
  (`feat: Add EV charger parameterization and lifetime parameter builder`). Findings
  describe the working tree, including uncommitted workflow changes.
- Pre-existing changes included `docs/backend_architecture.md`,
  `docs/canoe_main_orchestrator.md`, `workflow/Snakefile`, `tests/test_workflow.py`,
  `workflow/profiles/`, and efficiency smoke directories.
  `docs/etl_flowcharts.md` also became dirty during inspection. These unrelated changes
  were preserved; this task changes the diagnostic and its planning record.
- All discovered Python under `src/`, `scripts/`, and `tests/` was inventoried and
  parsed. Every production module received a responsibility/dependency assessment in
  section 3. The current research notebook, workflow/profile, artifact routes, typed
  configuration, relevant parameter/source selections, and boundary tests were inspected
  separately. Legacy code, cached data, generated directories, and `.venv/` were excluded
  from code metrics.

### What the measurements mean

- Physical lines include blanks, comments, and docstrings. Approximate LOC excludes blank
  and full-line comment lines but includes docstrings. Function span runs from
  `lineno` to `end_lineno`, including nested helpers. Function counts include methods
  and nested functions. All 88 Python files parsed successfully.
- The branch signal counts AST `If`, `For`, `AsyncFor`, `While`, `Try`,
  `IfExp`, `BoolOp`, `Match`, and comprehension nodes. Nesting counts control-flow
  and context-manager blocks. Nested helper bodies contribute to their containing
  function's signal. These are inspection aids, not cyclomatic complexity, runtime cost,
  defect probability, or reasons to split a module.
- The production import graph covers 47 source/script files and 117 distinct directed
  local import edges, including imports inside functions. Fan-in/out excludes tests,
  external libraries, command-line invocations, and artifact-mediated dependencies.
  No static import cycles were found. Low import counts do not imply low dependency
  burden: configuration keys and files supply many runtime dependencies.
- A narrow duplicate screen normalized function names, removed leading docstrings, and
  compared location-independent ASTs for functions spanning at least eight lines with
  at least 20 AST nodes. It found no exact production duplicates. It does not normalize
  local identifiers or prove the absence of similar mechanics or overlapping ownership.
  The consolidation findings rely on inspected callers and implementations.
- Lexical discovery used `rg`; AST inspection located size, calls, and imports.
  An ast-grep conditional-call rule was checked on positive and negative stdin examples,
  then confirmed that capacity preparation in the cost/efficiency builders is guarded
  by missing supplied rows. Matching source and the contribution caller were inspected
  before drawing the execution conclusion.

Representative read-only commands are below; the inventory/AST analysis was run through
`uv run --offline python -B -` without adding a permanent diagnostic framework.

```powershell
git status --short
git rev-parse HEAD
rg --files -g '*.py' -g '!legacy_backend/**' -g '!inputs/**' -g '!outputs/**'
rg -n '^def |^class |^from |^import ' src scripts
rg -n 'prepare_|resolve_artifact_path|read_csv|write_dataframe_atomic|to_csv' src
uv run --offline pytest --collect-only -q
uv run --offline ruff check src scripts tests --statistics
uv run --offline snakemake --snakefile workflow/Snakefile --cores 1 --dry-run --config scenario=config/scenarios/legacy_reproduction.yaml
```

No source refresh, production ETL/build, notebook execution, or parity run was performed.
Behavior checks used temporary fixtures/databases or read existing accepted artifacts.
The real workflow command above was a dry-run; section 7 states the limits.

## 2. Current scale and execution shape

| Scope | Python files | Physical lines | Approximate LOC | Change since 2026-08-20 |
|---|---:|---:|---:|---|
| `src/` | 44 | 26,213 | 24,523 | +14 files, +8,976 physical lines. |
| `scripts/` | 3 | 400 | 330 | Unchanged counts. |
| `tests/` | 40 | 10,805 | 9,540 | +16 files, +3,292 physical lines. |
| Research notebook under `docs/insights/` | 1 | 3,968 | 3,691 | Measured separately from production. |

There are 39 test modules plus `conftest.py`, 288 statically defined `test_*` functions,
and 323 collected pytest cases. The older snapshot recorded 204 test functions.
Collection establishes import/discovery readiness, not that all cases pass.

[build_transport.py](../src/build_transport.py) now prepares templates and seven parameter
families: existing capacity, demand, road utilization, lifetimes, efficiencies/input splits,
costs, and EV chargers. Its `TransportContribution` holds package row models, datasets,
provenance contexts, and audits; `insert_transport_contribution` uses the caller's
connection without committing it. The standalone path adds packaged-schema initialization,
validation, and atomic SQLite publication around that same assembly path.

The [current Snakefile](../workflow/Snakefile) has five rules: `all`, `doctor_smoke`,
`statcan_transport_tables`, `cer_enerfuture`, and `transport_database`. Its default
source jobs require caches and use `--no-download`; `download_sources=true` opts into
acquisition. It tracks source table outputs, templates, registered manual inputs through
doctor, shared validation/utilities, conversion inputs, and parameterization code. The
database rule runs the full contribution build. It is no longer template-only.

The [workflow profile](../workflow/profiles/default/profile.yaml) places Snakemake source
cache metadata under `.snakemake/source-cache`. Workflow fixture tests now exercise
ordering, reuse, missing-table recovery, invalidation, failed-publication restoration, and
offline missing-cache rejection. Two of those fixtures were rerun for this assessment;
the real dry-run found five jobs and completed successfully in 2.49 seconds.

### Static hotspots and shared interfaces

The table combines size with actual function concentration. Argument counts include
keyword-only arguments; branch/nesting signals follow section 1.

| Module / function | Physical module lines | Function start / span | Arguments | Branch signal / nesting | Interpretation |
|---|---:|---:|---:|---:|---|
| `build_costs.prepare_cost_rows` | 958 | 226 / 719 | 4 | 133 / 10 | Several source, pathway, row, provenance, and publication stages share one closure. |
| `build_efficiencies.prepare_efficiency_rows` | 956 | 307 / 555 | 2 | 75 / 7 | Evidence assembly, relationships, vintages, PHEV splits, and publication are concentrated. |
| `build_existing_capacity.build_existing_capacity_artifacts` | 764 | 210 / 534 | 1 | 51 / 5 | Multi-mode assembly plus prerequisite regeneration and audit/publication. |
| `ev_chargers.prepare_ev_charger_rows` | 480 | 123 / 337 | 3 | 90 / 5 | Smaller module, but one function assembles several parameter families. |
| `road_stocks_and_demands.distribute_existing_road_capacity` | 1,308 | 553 / 327 | 14 | 80 / 4 | Many evidence/eligibility inputs; cohort stages merit local decomposition. |
| `road_stocks_and_demands.distribute_existing_bus_capacity` | 1,308 | 882 / 292 | 9 | 60 / 5 | A separate bus allocation contract, not an interchangeable road/off-road algorithm. |
| `vehicle_mapping_bootstrap.build_bootstrap_mapping` | 1,948 | 1438 / 399 | 1 | 35 / 3 | Long ordered evidence pipeline, with established stage helpers. |
| `nlr_atb_autonomie.derive_phev_efficiency` | 1,616 | 776 / 300 | 8 | 18 / 1 | Long joined source calculation; low nesting changes the diagnosis. |
| `vehicle_population.normalize_report_a` | 1,772 | 886 / 309 | 6 | 9 / 1 | Large source-native normalization with modest control-flow depth. |
| `manual_parameters.resolve_manual_parameters` | 798 | 429 / 312 | 3 | 41 / 5 | Cohesive selector resolver; local stages are more plausible than a generic rules engine. |
| `road_aggregation.unresolved_mapping_reasons` | 1,769 | 949 / 276 | 4 | 7 / 2 | Diagnostic responsibility lives beside runtime mapping. |
| `road_lifetimes_survival._derive_mto_diagnostic_outputs` | 2,173 | 1869 / 180 | 3 | 16 / 0 | Largest production module, with explicit accepted/diagnostic entrypoints. |

The highest direct production fan-ins are `utils` (28), `validation.provenance` (11),
`validation.config_models` (10), `validation.insertion` (9),
`road_efficiencies` (6), and `manual_parameters` (5). The first four centralize small,
stable contracts. Their reuse is useful; it is not evidence of excessive ownership.
By comparison, `build_transport` imports 14 local modules, `build_costs` 12,
`build_efficiencies` 10, and `build_existing_capacity` 7: integration responsibility
is concentrated in a few assemblers.

## 3. Responsibility and disposition map

Every nonempty production module is covered below. Sizes identify inspection surfaces;
the suggested disposition follows its contracts, callers, side effects, and artifacts.
“Extract” means a candidate for later review, not an approved module/package design.

### Source adapters

| Module under `src/fetching/` | Lines | Responsibility and disposition |
|---|---:|---|
| [vehicle_population.py](../src/fetching/vehicle_population.py) | 1,772 | Ontario CKAN discovery, archive validation, Reports A/4/5 normalization, reconciliation, and source inventories form one source family. Keep that ownership; simplify stage orchestration if needed. A streaming long-status output also makes indiscriminate writer replacement unsuitable. |
| [nlr_atb_autonomie.py](../src/fetching/nlr_atb_autonomie.py) | 1,616 | ATB archive components, Autonomie/PHEV energy reconciliation, VMT, and maintenance evidence share source-member lineage. Extract internal PHEV/archive stages before considering separate source owners. Its nested dispatcher is an inspection target, not evidence that all ANL/ATB work should be separated. |
| [assorted_sources.py](../src/fetching/assorted_sources.py) | 1,836 | NHTSA, NEMS, GCAM, ReGen, FAA, and TC dashboard contracts have independent identities and formats. Strong source-family extraction candidate behind the current facade. The default function couples the first five families; TC already has an independent entrypoint. |
| [nrcan_ceud.py](../src/fetching/nrcan_ceud.py) | 923 | CEUD Excel tables and fuel-rating CSVs have distinct request models, normalizers, and publishers. A defensible future separation follows those contracts, while retaining common path/source mechanics. Do not split CEUD provincial/national series merely by geography. |
| [statcan_tables.py](../src/fetching/statcan_tables.py) | 913 | StatCan ZIP/metadata contracts and transport-specific historical/candidate tables remain source-owned. Keep the adapter; isolate derivation stages if they grow. Its JSON publisher is a small shared-mechanics candidate. |
| [cer_enerfuture.py](../src/fetching/cer_enerfuture.py) | 548 | Edition/scenario requests, physical CSV validation, macro/demand/price normalization, and manifest output are cohesive. Keep separate from demand and currency parameterization. |
| [fueleconomy_vehicles.py](../src/fetching/fueleconomy_vehicles.py) | 370 | Validates one ZIP/vehicle-table family and publishes classification evidence. Keep source ownership; share only proven publication primitives. |
| [vpic_vehicle_types.py](../src/fetching/vpic_vehicle_types.py) | 526 | Make/year/type endpoint scope evidence, response validation, cache replay, and classification are coherent. Keep endpoint-specific request/eligibility behavior. |
| [vpic_model_years.py](../src/fetching/vpic_model_years.py) | 453 | Temporal confirmation includes Canadian specifications for older vintages and a distinct normalization contract. Reuses `VPicResponse` from the type adapter; shared response/cache mechanics merit a small common owner, not a wholesale endpoint merge. |

### Parameter families

| Module under `src/parameterization/` | Lines | Responsibility and disposition |
|---|---:|---|
| [build_existing_capacity.py](../src/parameterization/build_existing_capacity.py) | 764 | Road, bus, and off-road evidence assembly, provenance, cleanup, and publication. Preserve its public preparation seam; extract mode/stage helpers and make prerequisite/publication behavior explicit. |
| [build_demand.py](../src/parameterization/build_demand.py) | 265 | CEUD baseline service demand, CER GDP indexing, provenance, and output validation. Relatively bounded family assembler; do not merge with stock or currency builders just because inputs overlap. |
| [road_utilization.py](../src/parameterization/road_utilization.py) | 651 | C2A, annual/age utilization, truck weights, row validation, and audits. Owns the utilization result but imports five utilization algorithms from the stock/demand module. Clarify that code ownership without merging whole families. |
| [build_lifetime_parameters.py](../src/parameterization/build_lifetime_parameters.py) | 130 | Selects fixed versus curve representation, combines road/manual/bus owners, checks coverage, and exposes a result plus an explicit artifact wrapper. Preserve as the small lifetime assembler. |
| [build_efficiencies.py](../src/parameterization/build_efficiencies.py) | 956 | Source selection, relationship ownership, historical/future efficiencies, PHEV splits, provenance, and publication. High-priority internal stage extraction; numerical road/off-road helpers already exist. |
| [build_costs.py](../src/parameterization/build_costs.py) | 958 | Source pricing, currency/service conversion, lifetime-gated variable costs, provenance, and audit/publication. Highest concentration of orchestration and nested state. Extract concrete pathway/stage helpers while preserving `CostPreparation`. |
| [ev_chargers.py](../src/parameterization/ev_chargers.py) | 480 | Vehicle-stock-to-port capacity, charger costs, efficiency, utilization evidence, and composite provenance. These outputs share one charger pathway. Extract stages within that owner; do not distribute charger modelling across all generic parameter builders. |
| [road_stocks_and_demands.py](../src/parameterization/road_stocks_and_demands.py) | 1,308 | Now includes utilization algorithms, age artifacts, road/bus cohort allocation, eligibility/redistribution, and demand helpers. The old “small cohesive module” verdict is stale. Clarify utilization ownership and simplify cohort stages before selecting any file split. |
| [offroad_stocks_and_demands.py](../src/parameterization/offroad_stocks_and_demands.py) | 492 | Service demand, linear additions, survival, and eligible-cohort redistribution use different evidence and dimensions from vehicle stocks. Keep separate; share an arithmetic primitive only after equivalent contracts are demonstrated. |
| [road_efficiencies.py](../src/parameterization/road_efficiencies.py) | 944 | Ratings/ATB aggregation, load factors, a numerical evidence object, and bus annual evidence are coherent road calculations. Generic interpolation/positivity/CEUD selectors are borrowed by cost/off-road helpers and merit narrower ownership. Preserve the evidence object and its per-instance caches. |
| [offroad_efficiencies.py](../src/parameterization/offroad_efficiencies.py) | 95 | Bounded CEUD/manual trajectory calculations. Keep separate from road pathways; move genuinely shared numerical selectors rather than merge these modules. |
| [road_capex_opex.py](../src/parameterization/road_capex_opex.py) | 174 | NLR purchase-price/RPE handling and LDV/BEAN maintenance arithmetic are concrete road-cost helpers. Keep; generic interpolation currently comes from the road-efficiency owner. |
| [offroad_capex_opex.py](../src/parameterization/offroad_capex_opex.py) | 188 | FAA service denominators and manual off-road cost ratios have their own units/source semantics. Keep separate; shares interpolation, not the full road-cost transformation. |
| [currency.py](../src/parameterization/currency.py) | 99 | CER-backed FX/deflator harmonization is already reused by vehicle and charger costs. Keep one numerical owner; the source adapter owns acquisition/normalization. |
| [offroad_lifetimes.py](../src/parameterization/offroad_lifetimes.py) | 207 | Reviewed manual lifetimes plus provincial StatCan road-bus lifetimes. The bus function is consumed by capacity and lifetime builders; its off-road location obscures ownership. Clarify/move that function behind stable callers, rather than merge the road survival machinery here. |
| [road_lifetimes_survival.py](../src/parameterization/road_lifetimes_survival.py) | 2,173 | Accepted source curves and MTO retention/decision diagnostics coexist, but default execution is now accepted-only. Optional code separation is a lower-priority maintainability question; mandatory diagnostic execution is resolved. |
| [road_aggregation.py](../src/parameterization/road_aggregation.py) | 1,769 | Reviewed-map validation/application and runtime weights coexist with candidate/reason diagnostics and shared rating catalog logic. Runtime writes are separated already. Extract diagnostic responsibilities only with shared matching contracts preserved. |
| [vehicle_mapping_bootstrap.py](../src/parameterization/vehicle_mapping_bootstrap.py) | 1,948 | Builds review evidence through ordered automatic/manual/temporal gates, overrides, and coverage. Keep opt-in and distinct from runtime map application; simplify its orchestration before adding more generalized machinery. |
| [manual_parameters.py](../src/parameterization/manual_parameters.py) | 798 | Registry validation, typed-source reconciliation, technology selectors, and resolution audits serve five production importers. Coherent shared trust boundary; extract local stages if useful, without a universal parameter engine. |

### Assembly, validation, utilities, scripts, and research

| Module or group | Lines | Responsibility and disposition |
|---|---:|---|
| [build_transport.py](../src/build_transport.py) | 877 | Template/schema preparation, contribution orchestration, caller-owned insertion, standalone publication, and parity reporting. Broad but explicit facade. Keep one transport assembly path; local helpers are preferable to a parallel integrated implementation. |
| [validation/config_models.py](../src/validation/config_models.py) | 364 | Stable paths/scenario/source/DQ models and validation. High reuse with little orchestration. Keep source-native extensions in adapters, not an enlarged universal schema. |
| [validation/provenance.py](../src/validation/provenance.py) | 308 | Stable IDs, registry rows, DQ inheritance, composite contributor validation, and conflicts. Keep as one provenance owner; family wrappers still decide meaningful contributors/variants. |
| [validation/insertion.py](../src/validation/insertion.py) | 217 | Package-row validation, duplicate/conflict checks, cross-table transport cleanup, and parameterized insertion. Cleanup is domain admission logic, not a second parameter derivation. Keep explicit and shared; avoid copying it into an adapter. |
| [validation/schema_contract.py](../src/validation/schema_contract.py) | 192 | Pinned schema evidence, packaged DDL, preflight checks, and the documented notes compatibility extension. Keep this SQLite trust boundary separate from upstream orchestration. |
| [validation/database_bootstrap.py](../src/validation/database_bootstrap.py) | 136 | Expected-key coverage, touched-table provenance, foreign keys, and integrity. Its template-era name/docstring understates current callers; function responsibility is focused. |
| [validation/legacy_compare.py](../src/validation/legacy_compare.py) | 329 | Table/family parity diagnostics with parameter-specific keys/tolerances. Keep diagnostic comparison distinct from accepted transformation and insertion; similar comparison loops do not justify a generic validator. |
| [validation/config_smoke.py](../src/validation/config_smoke.py) | 56 | Config/schema readiness shared by setup and doctor. Preserve the common check and the different entrypoint effects. |
| [validation/sqlite_utils.py](../src/validation/sqlite_utils.py) | 6 | Identifier quoting used by build and integrity checks. Small coherent primitive; no need to expand a helper into a framework. |
| [utils/__init__.py](../src/utils/__init__.py) | 183 | Config bundle loading and configured path/layer resolution. Keep thin despite high fan-in; domain calculations do not belong here. |
| [utils/files.py](../src/utils/files.py) | 36 | Streaming SHA-256 and same-directory atomic CSV publication. Existing shared mechanism; a bounded JSON/text publication primitive is plausible. |
| [utils/vehicle_labels.py](../src/utils/vehicle_labels.py) | 106 | Candidate annotation/null handling and agreement comparison. Its semantics differ from road aggregation's ASCII make/model normalization. Preserve distinctions until a contract table proves equivalence. |
| [setup.py](../src/setup.py) | 45 | Creates configured directories and writes smoke status. Keep as the mutating setup entrypoint, distinct from default read-only doctor. |
| [scripts/doctor.py](../scripts/doctor.py) | 185 | Import/config/manual/schema/directory checks; creation is opt-in. Keep operational readiness distinct from parameter ETL. |
| [scripts/clean_runtime.py](../scripts/clean_runtime.py) | 214 | Scoped cleanup planning, root containment, tracked-file protection, and explicit deletion. Keep separate from readiness and production compilation. No cleanup was executed for this task. |
| Package `__init__.py` entrypoints | 0–1 each | `fetching`, `parameterization`, `validation`, and `scripts` are empty/minimal; their presence does not imply duplicate runtime owners. |
| [Vehicle mapping research notebook](insights/vehicle_population_aggregation_mapping.py) | 3,968 | Artifact-backed marimo evidence and visualizations; largest cell spans 398 lines, including a 394-line view helper. It imports shared config/path helpers rather than ETL builders. Simplify presentation helpers if necessary; keep research out of runtime readiness and modelling authority. |

## 4. Verified concerns across module boundaries

### 4.1 Long builders combine several independently testable stages

Cost preparation loads many source families, validates manual/source metadata, selects
regional/pathway prices, performs currency and service normalization, gates period/vintage
activity against lifetimes, attaches composite provenance, validates coverage, and publishes
several CSV/JSON artifacts. Nested `active_period`, `add`, and `road_price` helpers
capture a large preparation context. Efficiency and capacity builders show a similar
assembly-to-publication concentration; the charger function is smaller but still spans
stock/port reconciliation, costs, efficiency, and utilization.

The evidence supports named stage/pathway helpers with explicit inputs and the existing
result types retained. It does not yet select a new package layout. Start with one family,
keep one public preparation implementation, and compare rows, keys, units, provenance/DQ,
exclusions, and audit outputs before/after. Preserve consequential choices in YAML and
baseline behavior while decomposing functions. A function-only extraction within its
current module can be the first useful step.

Affected routes: `costs_interim/processed/validation`,
`efficiencies_interim/processed/validation`,
`existing_capacity_interim/processed/validation`, and
`ev_chargers_processed/validation`. These are separate artifact owners; shared source
inputs do not make their parameter outputs interchangeable.

### 4.2 Preparation reuse is partly implemented, with remaining side effects

[Contribution preparation](../src/build_transport.py), starting at line 396, prepares
capacity once in the default full build and supplies those rows to efficiency, cost, and
charger preparation. It also supplies fixed/curve lifetime rows to costs. The cost and
efficiency fallback capacity calls occur only when supplied rows are `None`.
Separate parameter CLIs legitimately prepare missing prerequisites; disabling a family's
insertion can still leave another family needing its evidence. It would be inaccurate to
claim that default full assembly rebuilds capacity once per dependent family.

Nevertheless, [capacity preparation](../src/parameterization/build_existing_capacity.py)
always invokes `build_existing_stock_age_artifacts` before reading its result, and its
public `prepare_existing_capacity_rows` delegates to a publishing builder. Efficiency,
cost, demand, road-utilization, and charger preparation also write configured artifacts.
[Lifetime preparation](../src/parameterization/build_lifetime_parameters.py) instead
returns rows/context/audit, with publication in a separate wrapper. “No SQLite writes”
therefore does not mean “no filesystem writes”; these interfaces have different effects.
The charger path also fingerprints the processed vehicle-capacity CSV while consuming
supplied capacity rows.

There are narrower repeated computations in normal assembly:
`prepare_statcan_bus_lifetimes` is called by both capacity and lifetime preparation;
`derive_load_factors` is called by capacity's bus evidence helper and by efficiency/cost
preparation. Several families reread CEUD, technology templates, and CER macro evidence.
These are verified call/read paths, not measured performance bottlenecks; the road
efficiency object already uses caches local to one evidence instance.

A later change should state each preparation entrypoint's inputs, returned provenance,
published artifacts, and prerequisite policy. Reuse immutable evidence within a run only
where its selections/dimensions agree. Any persisted handoff must reconstruct row models,
datasets, contributor/DQ context, value variants, and audits; a parameter CSV alone is not
the complete `TransportContribution`. Avoid global mutable caches or a second assembly
implementation to solve this.

### 4.3 Workflow coverage and artifact freshness remain incomplete

The current DAG correctly declares StatCan/CER source products and the full database
builder, and `all` requests source outputs even when SQLite already exists. Its
`update(...)` database/report outputs and standalone atomic writer protect publication
on failure. The selected fixture tests verified source ordering/reuse/missing-table
recovery and restoration of the previous database/report after a failed writer.

Other required normalized evidence and processed prerequisites remain outside the DAG:
CEUD/ratings, Ontario population, ATB/Autonomie, FuelEconomy, assorted/dashboard evidence,
reviewed-map-derived weights, and accepted lifetime/age products are read by production
functions without equivalent producer/input declarations in the database rule.
Some source-adapter helper code imported by parameter builders is likewise absent from
that rule's direct code list. Broad config/code dependencies can trigger incidental
rebuilds, but do not guarantee that changing one of these undeclared inputs invalidates
SQLite or regenerates its upstream evidence.

The gap is artifact dependency coverage, not competing transformation implementations.
Before adding fine-grained parameter rules, stabilize the preparation/publication handoff
in 4.2. Then declare one existing producer and all material inputs for each family,
including reviewed mappings and accepted prerequisite freshness. Intermediate/processed
routes are currently shared across scenarios; concurrent scenarios with different source
selections could overwrite them even if final SQLite names differ. A scenario-specific
artifact contract is required before promising parallel scenario execution.

[Architecture fitness tests](../tests/test_architecture_fitness.py) verify that declared
owners/producers/validation symbols exist and that accepted/diagnostic routes differ.
They do not compare actual reads/writes with route metadata or prove full DAG freshness.
For example, `ev_chargers` consumes/fingerprints existing capacity, while that route's
listed consumers currently mention `build_transport` and reviewers. Routes intentionally
mix logical row consumers with file consumers; a later ownership update should distinguish
those meanings rather than infer file reads from a “consumer” label.

### 4.4 Shared calculations are reused, but some owners are misleading

Five utilization functions at the beginning of
[road_stocks_and_demands.py](../src/parameterization/road_stocks_and_demands.py) are imported
only by `road_utilization` in production: C2A reconciliation, annual utilization,
normalized mileage profiles, flat factor rows, and period age utilization. Their
responsibility aligns with the utilization family, while stock/demand retains distinct
age/cohort/demand work. This is a concrete ownership-consolidation candidate; it is not
evidence for merging the 651-line utilization module with the 1,308-line stock module.
Corresponding tests currently reside in the stock/demand test module.

Generic `interpolate` is imported from
[road_efficiencies.py](../src/parameterization/road_efficiencies.py) by road/off-road cost
helpers; off-road efficiency also imports `positive` and `ceud_series`. These helpers
encode real validation semantics, including no extrapolation and positive/zero handling.
A small numerical/evidence helper owner could reduce unrelated road dependencies if
existing contract tests follow it. Keep load-factor/ATB/rating pathway calculations with
their domain owner unless another concrete use requires a shared owner.

`prepare_statcan_bus_lifetimes` is a road-bus contract in
[offroad_lifetimes.py](../src/parameterization/offroad_lifetimes.py), used by capacity and
lifetime assembly. Give that function a clear shared lifetime owner during a bounded
change. The already shared `CerCurrencyConverter` and `ResolvedProvenance` are examples
of successful consolidation; duplicate family-specific currency or DQ implementations
would move the codebase in the opposite direction.

### 4.5 Publication mechanics can be consolidated without merging adapters

Both vPIC adapters write validated JSON responses using same-directory temporary files,
`json.dump`, replacement, and cleanup; StatCan has a separately implemented fixed
`.part` JSON writer. VehiclePopulation and FuelEconomy have text writers with different
temporary-file choices and input shapes. Several source output publishers still use direct
`to_csv` or `write_text`, while parameter CSVs use `write_dataframe_atomic` and their
JSON audits often use direct writes. The implementations overlap but are not identical;
publication and cleanup contracts need comparison before replacement.

The existing [files utility](../src/utils/files.py) is the natural place to evaluate a
small atomic text/JSON primitive with deterministic serialization and failure cleanup.
Keep archive validation, response parsing, API retry/cache policy, streaming append
behavior, and manifests with source adapters. Shared file mechanics do not justify a
generic fetch engine or merging endpoints/sources. No publication failure was observed
in this assessment.

### 4.6 Independent source families and diagnostics have defensible extraction seams

[Assorted sources](../src/fetching/assorted_sources.py) now covers six independent families
across workbook, CSV, JavaScript chart, PDF, and HTML contracts. Its main entrypoint
requires the five older families together; `fetch_and_normalize_tc_dashboard` and
`--tc-dashboard-only` already expose a separate sixth path. Source-family helpers behind
a compatibility facade would reduce change impact without changing source keys,
normalizers, output paths, or warnings. NRCan's CEUD/rating separation is another concrete
seam, with distinct source request models and publishers already present.

Mapping runtime applies the reviewed crosswalk and publishes weights; candidate generation,
reason diagnostics, and bootstrap arbitration have opt-in entrypoints. Lifetime default
execution derives accepted curves without loading MTO history; MTO diagnostics have their
own publisher and routes. These execution boundaries now work. Diagnostic code still
shares large modules with runtime logic, so optional extraction can improve navigation
later, but should retain shared matching/curve primitives and keep review evidence out
of accepted input readiness. Moving diagnostic files alone would not resolve freshness
or improve model parity.

### 4.7 Integration should reuse the contribution seam, with an explicit object boundary

The current builders still read configured technology/commodity/region/period templates,
often independently. `prepare_transport_contribution` accepts a template directory for
structural loading, while dependent builders usually resolve templates through the bundle;
`prepare_road_utilization` already allows an optional technology frame. Consequently,
passing upstream objects only to a new outer adapter would not automatically change every
family's object authority or align fuel names.

A future `canoe_adapter.py` should translate agreed upstream structure/configuration into
the same transport preparation and insertion path, with inherited commodities/adjacent
objects and transport-owned pathways made explicit. Fuel/commodity alignment, shared-row
ownership, geography/schema/provenance compatibility, and renewable blending/hydrogen
representation still require the integration evidence recorded in
[canoe_main_orchestrator.md](canoe_main_orchestrator.md). This diagnostic does not turn
unfinished upstream contracts into backend policy or propose a wholesale object refactor.
The useful preparation work now is to clarify existing builder inputs and effects.

## 5. Boundaries that should remain distinct

- Source acquisition/native normalization and parameterization consume different contracts.
  Parameter modules import a few source-owned configuration/cache-validation helpers, but
  inspection found no direct HTTP acquisition or SQLite transactions in parameterization.
  Preserve offline preparation and caller-owned SQLite insertion.
- Road/off-road transformations and individual parameter builders share evidence, not
  interchangeable units, keys, source choices, or validation. Consolidate proven primitives
  and evidence owners; keep parameter result/audit ownership visible.
- Charger capacity/cost/efficiency products belong to one charger pathway. Their shared
  ownership supports staged preparation under that module rather than scattering the
  modelling across generic cost/capacity/efficiency implementations.
- Runtime mapping and accepted lifetimes must remain distinct from bootstrap, MTO
  diagnostics, research visualizations, and parity reports. Shared functions should not
  make review evidence mandatory for an ordinary build.
- `normalize_vehicle_text` transliterates ASCII make/model strings; shared
  `normalize_vehicle_label` handles nulls and candidate annotations differently.
  A blind merger could change reviewed matches. Record/test these differences first.
- Setup, doctor, and cleanup have different mutation/authorization contracts. Their small
  root/path/import mechanics can share helpers if useful; their commands should remain
  separate. Schema, provenance, insertion, and integrity similarly protect different
  trust boundaries despite sharing SQLite concepts.

## 6. Reconciliation with the previous snapshot

| Previous finding | Current evidence and disposition |
|---|---|
| Accepted lifetime generation always runs MTO diagnostics. | Resolved execution coupling: accepted-only default and separate diagnostic publishers/routes, verified by tests. Optional code separation remains a lower-priority question. |
| Database build is mostly template loading; parameter artifact consumers are aspirational. | Superseded: seven parameter families feed a real contribution and validated insertion. Row-versus-file consumption and preparation side effects still need clearer contracts. |
| `stocks_and_demands` is a small, cohesive module. | Superseded: road/off-road family names now exist and road stock/demand grew to 1,308 lines with utilization plus two substantial cohort allocators. |
| Large MTO adapter might warrant a split. | Keep source-family ownership; its 309-line Report A normalization is not deeply nested. Size alone still does not establish excessive ownership. |
| Mapping runtime/review boundaries need examination. | Execution/publication separation is already explicit; shared module responsibilities and artifact freshness remain review targets. |
| Shared hashing/atomic writes/provenance may be duplicated. | Hashing/CSV publication and source/DQ provenance already have shared owners. Remaining JSON/text mechanics and numerical helper placement are narrower opportunities. |
| Workflow coverage needs to match real compilation. | Improved for tracked StatCan/CER outputs, templates/manual/code inputs, and the full build; several required source/processed prerequisites still lack declared DAG edges. |

## 7. Validation performed and limits

| Check | Result on 2026-10-01 |
|---|---|
| Python AST parse/inventory | All 88 discovered files parsed; inventory reconciled with section 2. |
| Static local imports / exact normalized functions | 117 production edges, no cycles, no exact nontrivial duplicates under the stated narrow screen. |
| Conditional preparation rule | Positive/negative stdin behavior verified; exactly two parameterization conditional matches, in cost and efficiency preparation. |
| Pytest collection | 323 cases collected in 2.83 seconds. |
| Focused behavioral checks | 50 passed in 41.95 seconds; one dependency deprecation warning. |
| Ruff | `uv run --offline ruff check src scripts tests --statistics` passed. |
| Real Snakemake dry-run | Five jobs, return code 0, 2.49 seconds, using the current local workflow profile; no ETL jobs executed. |
| Document verification | Frontmatter and all 60 local links checked; 44 module-size rows and 12 function hotspots reconciled with code; every nonempty production file covered. |

The 50 executed cases cover artifact topology, runtime hygiene, schema contracts, source/DQ
provenance, parameter validation/insertion, accepted/MTO lifetime separation, reviewed-map
protection, caller-owned transaction/schema rejection, and two temporary workflow fixtures.
The workflow fixtures replace substantive ETL entrypoints with fixture writers; they
verify orchestration/publication behavior, not full production data readiness.

Exact focused command:

```powershell
uv run --offline pytest -q tests/test_architecture_fitness.py tests/test_runtime_hygiene.py tests/test_schema_contract.py tests/test_provenance.py tests/test_parameter_validation.py tests/test_road_lifetimes_survival.py tests/test_vehicle_mapping_bootstrap.py::test_runtime_reads_but_does_not_overwrite_reviewed_mapping tests/test_vehicle_mapping_bootstrap.py::test_mapping_diagnostics_remain_explicitly_callable tests/test_database_bootstrap.py::test_transport_contribution_uses_a_caller_owned_transaction tests/test_database_bootstrap.py::test_transport_contribution_rejects_an_incompatible_caller_schema tests/test_workflow.py::test_workflow_sequences_sources_reuses_outputs_and_rebuilds_missing_table tests/test_workflow.py::test_workflow_restores_previous_publication_when_writer_fails
```

No full-suite pass, build-runtime/memory profile, fresh source-to-SQLite reconciliation,
legacy parity acceptance, or CANOE-main integration success is claimed. Existing tests
include source fixtures and parameter/parity checks, but not all were executed here.
New architectural checks should target a demonstrated seam: actual artifact dependency
invalidation, preparation/publication effects, shared-helper semantics, or row/context
equivalence after an extraction. A LOC budget or import count alone would not protect
the backend's contracts.

## 8. Prioritized decision surface for later work

These are investigation priorities with bounded acceptance evidence, not a chosen refactor
sequence or accepted modelling changes. Baseline equivalence remains the constraint;
do not use architectural cleanup to silently resolve unfinished parameter assumptions.

| Priority | Candidate and affected owners | Smallest useful next step | Evidence required before completion |
|---|---|---|---|
| 1 | Cost/efficiency/capacity/charger assembly concentration | Select one family; extract source/context, pathway, row-validation, and publication stages as needed while retaining its public result/interface. | Compare keys, units, values, provenance/DQ, audits, exclusions, and applicable legacy parity; avoid an additional transformation path. |
| 2 | Preparation effects and prerequisite freshness across builders/workflow | Document and stabilize one family's prepare/publish contract; represent its material prerequisite inputs explicitly. | Supplied prerequisites suppress fallback work; invalidation/missing-input tests observe real dependencies; persisted handoffs retain registry/context evidence. |
| 3 | Utilization, interpolation/CEUD selectors, and bus-lifetime ownership | Move only functions with demonstrated callers and clear shared/domain responsibility. | Existing numerical/selector tests follow the owner; row results and reviewed mappings remain equivalent; routes/callers reflect the final owner. |
| 4 | Atomic JSON/text mechanics and vPIC response ownership | Compare current serialization/temp/cleanup behavior; extract one bounded primitive/common response contract if equivalent. | Deterministic cache replay, invalid payload/failure cleanup tests, unchanged source-native contracts and streaming behavior. |
| 5 | Assorted-source and NRCan independent contracts | Introduce source-family helpers behind existing entrypoints when a concrete change needs isolation. | Offline fixtures, unchanged source/component IDs, cache/output routes, manifests, warnings, and selection counts. |
| 6 | Mapping/bootstrap and lifetime diagnostic code organization | Isolate review responsibilities if change impact or navigation warrants it; retain shared numerical/matching primitives. | Default entrypoints never run diagnostic writers or overwrite reviewed inputs; opt-in review outputs remain reproducible. |
| 7 | Adapter/object boundary and scenario artifact isolation | After upstream contracts are agreed, supply one explicit structural context to the same transport builders; define scenario artifact ownership before parallel runs. | Upstream-authoritative commodity IDs, transport-owned pathways, schema/region/provenance checks, shared insertion tests, and concurrent-artifact isolation where supported. |
