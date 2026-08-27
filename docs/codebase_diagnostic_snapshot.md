---
title: Codebase diagnostic snapshot
role: Point-in-time evidence for codebase complexity, modularity, duplication, ownership drift, and refactor planning.
retrieve_when: A task affects module boundaries, architecture fitness, codebase complexity, shared infrastructure, development/runtime separation, or an efficiency refactor.
read_scope: Read only the relevant diagnostic sections unless the task is explicitly repository-wide.
verify: Reconcile snapshot findings against current code, config, tests, schemas, workflow, and generated evidence before acting.
last_diagnostic_run: 2026-08-20
review_status: "#to-review"
---

# Codebase diagnostic snapshot

This document replaces the 2026-08-13 diagnostic. It is a point-in-time assessment, not
an architecture authority, artifact registry, cumulative decision log, or refactor plan.
Current code, configuration, tests, schemas, workflow, and generated evidence outrank it.
The `#to-review` status applies to the complete diagnostic.

## 1. Diagnostic context and method

### Repository state

- Diagnostic completed: 2026-08-20.
- Branch and commit inspected: `v2.0` at
  `a8032f0890ca2189c86d749ff221995cd322a278` (2026-08-17,
  `Refactor parameterization and validation paths; introduce artifact routing`).
- The working tree was dirty before this task. Tracked `src/`, `tests/`, and `workflow/`
  matched the inspected commit; unrelated plan-archive moves, config/document edits, the
  deletion of the former snapshot filename, and this snapshot under its new filename were
  already present. Conclusions describe the working tree, not a clean checkout.
- The prior snapshot was committed on 2026-08-13 as
  `docs/architecture_context_optimization_diagnostic_snapshot.md`. Plan 030 subsequently
  implemented its artifact-routing, runtime/development, and shared-mechanics findings.

### Scope inspected

The inspection covered all current Python files under `src/`, `scripts/`, and `tests/`;
`workflow/Snakefile`; `config/paths.yaml`; relevant route, source, scenario, and parameter
configuration; the structural and execution-boundary sections of
`docs/backend_architecture.md`; and current files under the configured Ontario vehicle,
road, lifetime, stock, and validation artifact families. Detailed review concentrated on:

- `fetching.vehicle_population`, `fetching.fueleconomy_vehicles`, both vPIC adapters,
  and `fetching.nrcan_ceud`;
- `parameterization.road_aggregation`, `vehicle_mapping_bootstrap`,
  `lifetimes_survival`, `stocks_and_demands`, and `manual_parameters`;
- `fetching.nlr_atb_autonomie`, `fetching.assorted_sources`, `build_transport`, shared
  utilities, validation modules, their callers, and focused tests.

### Reproducible inspection method

Read-only commands used included:

```powershell
git status --short --branch
git rev-parse HEAD
git log -1 --format="%H%n%cI%n%s"
git diff --name-status 9f065eb..HEAD -- src tests workflow config/paths.yaml
rg --files src tests workflow config
rg -n "^(from|import) (fetching|parameterization|validation|utils)" src scripts tests
rg -n "resolve_artifact_path|resolve_input_path|write_dataframe_atomic|to_csv|read_csv" src
rg -n "def (file_sha256|write_.*atomic|find_repo_root|quote_identifier|normalize_vehicle_)" src scripts
```

A temporary in-memory AST script, run with `uv run python -B -`, counted physical lines,
nonblank/non-full-line-comment LOC, functions, argument counts, function spans, branch-like
nodes, and maximum control-flow nesting. The branch signal is a screening aid, not a formal
cyclomatic-complexity grade. Callers, artifacts, and responsibility boundaries were then
verified directly in code and configuration rather than inferred from metrics.

No network calls, source refreshes, ETL entrypoints, database builds, notebooks, or artifact
writers were run. Focused non-network tests are recorded in section 8.

## 2. Repository and execution shape

### Inventory and size

| Scope | Python files | Physical lines | Approximate Python LOC |
|---|---:|---:|---:|
| `src/` | 30 | 17,237 | 15,932 |
| `tests/` | 24, including `conftest.py` | 7,513 | 6,568 |
| `scripts/` | 3 | 400 | 330 |

There are 23 `test_*.py` modules with 204 statically defined `test_*` functions. Pytest
parameterization can produce a larger executed-case count. Size is useful for locating
inspection effort; it is not used as a disposition criterion.

The default Snakemake DAG remains deliberately narrow: doctor, StatCan, CER, and a
template-only SQLite build. It does not invoke Ontario vehicle acquisition, road
aggregation, lifetime derivation, stock derivation, FuelEconomy, vPIC, or mapping
bootstrap. Direct Python entrypoints are therefore the only current execution surface for
the vehicle slice.

This is not inherently wrong: repository policy allows direct entrypoints until stage
interfaces stabilize. It does mean that “active source,” “configured artifact route,” and
“participates in normal database compilation” are three different facts.

### Current vehicle artifact placement

| Configured family | Current files | Current role and observation |
|---|---:|---|
| `ontario_vehicle_population` | 27, about 292 MB | Source-normalized MTO reports, audit tables, manifest, and warnings. One old lifetime-comparison CSV remains here despite current code routing that filename to lifetime validation. |
| `vehicle_survival_interim` | 7, about 71 MB | Large raw and mapped transition audit tables owned by lifetime derivation. |
| `road_aggregation` | 5, about 6.4 MB | Mapped stock and runtime aggregation weights. |
| `lifetimes_survival` | 10, about 0.38 MB | Processed survival curves, mappings, and medians. |
| `stocks_and_demands` | 1 | Processed Ontario LDV age distribution. |
| `vehicle_mapping_review` | 15, about 14.5 MB | Candidate, bootstrap, request, coverage, review, and exported-notebook evidence. |
| `lifetime_validation` | 6 | Decision and review evidence; it also contains an old unconfigured commercial/EIA comparison filename while the currently configured comparison filename is absent. |
| `outputs/validation` | 0 | Reserved for database validation reports; no database report is currently present. |

The typed routes added by Plan 030 now distinguish source interim, transformation interim,
processed, input-validation, database, and output-validation layers. The two stray lifetime
comparison files show that generated directories are not themselves proof of a current,
complete run. Neither stray file is a current code consumer, so this is evidence-lifecycle
drift rather than a normal ETL dependency.

## 3. Static complexity signals

| Module | Lines | Functions | Largest function span | Notable signal |
|---|---:|---:|---|---|
| `parameterization.vehicle_mapping_bootstrap` | 1,949 | 29 | `build_bootstrap_mapping`, 392 | `automatically_supported_years` has 11 arguments and the module contains several 120-243 line arbitration stages. |
| `fetching.vehicle_population` | 1,806 | 48 | `normalize_report_a`, 331 | Long vectorized normalization with low control-flow nesting; `fetch_and_normalize` is 237 lines. |
| `parameterization.road_aggregation` | 1,753 | 27 | `unresolved_mapping_reasons`, 276 | Runtime and diagnostic entrypoints are separate, but most candidate/review mechanics remain in the same module. |
| `parameterization.lifetimes_survival` | 1,727 | 25 | `build_lifetime_artifacts`, 220 | One command builds MTO diagnostics, source transforms, parameter-ready curves, and validation evidence across three routes. |
| `fetching.nlr_atb_autonomie` | 1,616 | 29 | `derive_phev_efficiency`, 300 | `fetch_and_normalize` is 272 lines with the deepest observed control-flow nesting (7). |
| `fetching.assorted_sources` | 1,550 | 35 | `normalize_regen`, 205 | Five source families with distinct physical parsers share one 179-line dispatcher. |
| `fetching.nrcan_ceud` | 923 | 33 | `normalize_ceud_dataframe`, 97 | Separate CEUD workbook and fuel-rating CSV contracts share a CLI/module but already have separate request models and fetch functions. |
| `parameterization.manual_parameters` | 726 | 12 | `resolve_manual_parameters`, 270 | Coherent selector expansion, but its main resolver has the highest branch-like score observed (29). |
| `build_transport` | 553 | 14 | `bootstrap_database`, 112 | Coherent database orchestration; current supported inputs are only technology and commodity templates. |
| `fetching.vpic_vehicle_types` | 526 | 14 | `fetch_and_normalize`, 112 | Two-pass endpoint probing and manifest/error handling. |
| `fetching.vpic_model_years` | 453 | 17 | `fetch_and_normalize`, 85 | Two endpoint schemas and temporal corroboration, with request/cache mechanics similar to the other vPIC adapter. |
| `fetching.fueleconomy_vehicles` | 370 | 11 | `fetch_and_normalize`, 49 | Small, focused evidence adapter. |
| `parameterization.stocks_and_demands` | 235 | 6 | `derive_ldv_age_distributions`, 133 | One coherent transformation and one publisher. |

The largest functions are not uniformly the most branch-heavy. In particular,
`normalize_report_a` is long because it preserves and reconciles a wide source contract,
whereas shorter manual/validation functions can have denser conditional behavior.

## 4. Responsibility, cohesion, and dispositions

### MTO and road-vehicle slice

#### `fetching.vehicle_population` — **healthy large module**

The module owns one external source family end to end: CKAN discovery, immutable ZIP
requests, physical member resolution, source validation, Reports A/4/5 normalization,
source-native key inventory, manifest/warnings, and publication to one interim route.
`road_aggregation` and `lifetimes_survival` are the principal runtime consumers; bootstrap
also consumes its historical reports as an explicit maintainer workflow.

Its size is mainly intrinsic to three MTO report shapes and Report A's audit-preserving
normalization. The 331-line `normalize_report_a` and 237-line publisher are candidates for
internal staging if they become difficult to change, but the evidence does not support
splitting acquisition from each report merely to reduce file size. Any later internal
simplification must preserve report-grain columns, suppression handling, reconciliation,
deterministic cached reruns, and `tests/test_vehicle_population.py`.

#### `parameterization.road_aggregation` — **responsibility-boundary candidate**

The runtime entrypoint at lines 1562-1622 now only validates/applies the reviewed mapping,
derives weights, and publishes five processed artifacts plus coverage. The explicit
diagnostic entrypoint at lines 1625-1724 loads FuelEconomy/NRCan evidence, generates
candidates, attributes unresolved reasons, and publishes review artifacts. This resolves
execution coupling, but not code ownership coupling: about half the module is mapping
development/review logic, and bootstrap imports eight road helpers.

A later boundary investigation is justified by two independently callable workflows and
two artifact roles, not by 1,753 lines. Reasonable options include retaining one module
with clearer internal sections, moving only candidate/review mechanics behind a development
module, or keeping compatibility re-exports. Mapping validation/application and aggregation
weight derivation belong together unless caller evidence demonstrates a better contract.

#### `parameterization.vehicle_mapping_bootstrap` — **internal simplification candidate**

This is explicitly a maintainer tool, requires an output path, and refuses to replace the
reviewed mapping without `--replace-reviewed-config`. Its responsibilities—manual-pass
reconciliation, vPIC gates, historical support, range collapse, proposed-map validation,
and review evidence—form one coherent arbitration workflow. It is not called by normal ETL.

The accidental complexity is inside the pipeline: `build_bootstrap_mapping` spans 392
lines, several intermediate frames are successively enriched, and three arbitration
functions span 202-243 lines. Future work should first expose named stage inputs/outputs
or immutable result objects inside the module; a package split is not yet justified.
Safety constraints are the reviewed-map byte-preservation test, the explicit replacement
gate, current mapping coverage, and the artifact-backed arbitration assertions.

#### `parameterization.lifetimes_survival` — **development/runtime separation candidate**

The module contains two related but independently useful chains:

1. historical MTO snapshot, transition, mapping-coverage, scope, and decision evidence;
2. NHTSA/NEMS transformation, Wards aggregation, source/target class mapping, survival
   curves, and median lifetimes.

`build_lifetime_artifacts` always executes both chains and writes seven interim, ten
processed, and five configured validation outputs. The final medians are derived from
the transformed external-source/legacy curves, while the MTO decision remains diagnostic
evidence. Therefore a consumer needing accepted source curves currently also pays the
MTO-history/mapping diagnostic dependency.

This is the clearest remaining execution-role coupling in the vehicle slice. A future
change could use two explicit entrypoints in the same module or separate modules with a
small shared survival-math layer. The safe boundary must preserve age-zero semantics,
transition ordering, raw ratios above one, source-labelled curves, Wards weighting,
configured decision gates, and all focused lifetime tests and artifacts.

#### `parameterization.stocks_and_demands` — **healthy cohesive module**

Despite a 133-line transformation, this small module has one responsibility: combine
mapped current stock with configured lifetime evidence to publish an Ontario LDV age
distribution and exclusion findings. Its processed and validation outputs are separately
routed, and tests cover both median and survival-curve modes. Internal helper extraction
may improve readability, but no responsibility split is supported.

#### FuelEconomy and vPIC adapters — **healthy adapters with a shared-mechanics candidate**

FuelEconomy has a distinct ZIP/CSV/class-harmonization contract and should remain a
separate source adapter. The two vPIC modules also have different eligibility, endpoint,
response, and normalization contracts: vehicle-type evidence performs base and typed
probes, while temporal evidence selects between model-year and Canadian-specification
endpoints. Merging the adapters would obscure these differences.

Concrete duplication remains in validated JSON cache replay, atomic JSON publication,
HTTP/error loops, manifest rows, delay handling, and warnings publication. The temporal
adapter already imports `VPicResponse` from the vehicle-type adapter, which makes the
latter a partial shared-infrastructure owner. A narrow JSON writer or tested request-result
executor is a demonstrated candidate; a universal fetch framework is not.

#### Vehicle-label normalization — **shared-interface clarification candidate**

`utils.vehicle_labels.normalize_vehicle_label` removes review annotations, handles nulls,
and supports family-level comparison. `road_aggregation.normalize_vehicle_text` performs
ASCII transliteration and stricter source-key normalization. Bootstrap uses both concepts.
They are not drop-in duplicates: accents, nulls, and annotations have different behavior.
The current evidence supports documenting and testing their domains before considering
consolidation, not replacing one with the other.

### Other substantial modules

#### `fetching.nlr_atb_autonomie` — **internal simplification candidate**

ATB archive acquisition, manually registered Autonomie inputs, utility-factor matching,
PHEV reconciliation, VMT normalization, and BEAN coefficient extraction serve one
composite source-native evidence contract. The derivations are separately callable and
well covered. Complexity is concentrated in the 300-line PHEV derivation and the deeply
nested publisher, so staged orchestration is a better first investigation than separating
the two sources and duplicating their reconciliation context.

#### `fetching.assorted_sources` — **responsibility-boundary candidate**

NHTSA, NEMS, GCAM, REGEN, and FAA have different physical formats, validation failures,
and parsers but share one request type, one CLI, one manifest/warnings publication, and one
large test module. A failure or change in one source crosses the combined dispatcher and
test surface. There is evidence for source-specific adapter boundaries behind a retained
orchestrator; there is not evidence for a generic parsing framework. Preserve each
source-native output contract and the offline all-source smoke if this is investigated.

#### `fetching.nrcan_ceud` — **responsibility-boundary candidate**

The module owns three source IDs and two materially different contracts: CEUD `.xls`
tables and pinned Fuel Consumption Ratings CSVs. Separate request models, rule groups,
normalizers, interim directories, and public fetch functions already provide a natural
seam; only the module and CLI remain combined. A future split could reduce change impact
without inventing an abstraction, provided current public imports and the dual-mode CLI
remain compatible.

#### `parameterization.manual_parameters` — **internal simplification candidate**

Registry validation and compact selector expansion are distinct stages of the same manual
parameter contract. The 270-line resolver and 158-line registry validator contain dense
branching, but both protect one trust boundary and share adapter metadata. Named internal
stages and smaller reconciliation-result builders are better supported than separate
packages.

#### `build_transport` and `src/validation` — **ownership/documentation drift**

Schema creation, typed insertion, post-insertion integrity, SQLite identifier quoting,
legacy comparison, atomic publication, and report writing now have explicit owners. The
previous duplication/ownership concern is mostly resolved. `build_transport` remains a
cohesive database orchestrator and currently loads only technology and commodity templates.

The drift is in declared consumption: `config/paths.yaml` names `build_transport` as a
consumer of lifetime and stock products, but current build code neither imports nor reads
those families. `tests/test_architecture_fitness.py` verifies that consumer references
resolve to live objects, not that consumers actually read routed artifacts. This overstates
end-to-end integration and should be treated as topology truth debt, not as a reason to
merge parameterization into the database builder.

#### `src/utils` — **healthy shared infrastructure**

Typed config/path loading, SHA-256, atomic DataFrame publication, and vehicle-label
comparison are small and directly reused. Script-local `find_repo_root` functions are now
compatibility wrappers delegating to the shared utility rather than duplicate algorithms.
`validation.sqlite_utils` similarly owns the shared identifier-quoting mechanic. The
utilities have not grown into hidden ETL orchestration.

## 5. Dependency and duplication assessment

### Demonstrated reuse opportunities

- Atomic DataFrame publication and ordinary file SHA-256 are now shared through
  `utils.files`; vehicle and parameter modules no longer import generic writing behavior
  from `fetching.vehicle_population`.
- SQLite identifier quoting is shared through `validation.sqlite_utils`.
- Atomic JSON/text writers remain repeated in StatCan, FuelEconomy, Ontario vehicle, and
  vPIC adapters. The JSON implementations are close enough for a narrow shared primitive;
  archive download replacement and source validation remain source-specific.
- vPIC request execution repeats enough cache/error/manifest mechanics to merit a tested
  seam if either adapter next changes. Their endpoint eligibility and response
  interpretation should stay adapter-owned.
- No broad normalization, manifest, retry, or parsing framework is supported. Similar
  method names conceal different physical contracts and failure policies.

### Layer and dependency observations

- Fetchers depend on `utils` and typed config models; parameter modules depend on `utils`
  and, where justified, other parameter contracts. No fetching module imports
  parameterization.
- `lifetimes_survival` imports reviewed mapping validation/application from
  `road_aggregation`. This is a real shared parameter contract, but it also means a future
  road development split must keep the runtime mapping surface stable.
- `vehicle_mapping_bootstrap` imports both runtime mapping and development candidate
  mechanics from `road_aggregation`. That coupling is the strongest evidence for examining
  a road development boundary.
- `utils` imports typed models from `validation.config_models`, while `config_smoke`
  imports config-loading utilities. There is no runtime import cycle, and the documented
  split—models protect structure, utilities load/resolve—is small enough to remain healthy.
- Workflow rules remain thin; the architectural gap is coverage, not duplicated
  transformation logic in Snakemake.

## 6. Test and evidence boundaries

Most focused tests are fixture-based and exercise parsing, normalization, offline cache
behavior, mapping, lifetimes, routes, and database trust boundaries independently.

One notable exception is
`tests/test_vehicle_mapping_bootstrap.py::test_repository_mapping_has_material_scale_and_all_ldv_classes`.
It reads the reviewed mapping plus ignored current-stock, rating, and bootstrap artifacts,
then asserts current mapping scale, exact re-audit counts, arbitration outcomes, and
coverage. This is valuable artifact-backed acceptance evidence, but it is collected as an
ordinary unmarked pytest test. A fresh checkout without ignored generated artifacts cannot
reproduce it from versioned files alone.

That coupling does not make mapping bootstrap part of runtime ETL. It does make the
default test surface dependent on retained development evidence and vulnerable to stale
artifact combinations. A later test-boundary change should retain the assertions in an
explicit artifact/integration target with prerequisite checks or a registered fixture,
while keeping ordinary unit tests hermetic.

## 7. Reconciliation with the 2026-08-13 diagnostic

| Prior finding | Status now | Current evidence |
|---|---|---|
| Hashing, atomic DataFrame writing, root discovery, and SQL quoting were duplicated. | **Resolved for the proven generic mechanics.** | `utils.files` and `validation.sqlite_utils` are used broadly; script root functions delegate. Source-specific JSON/text/cache publication remains only partially shared. |
| Vehicle/parameter modules imported a writer from the Ontario source adapter. | **Resolved.** | Callers import `write_dataframe_atomic` from `utils`. |
| Runtime road aggregation always invoked candidate/rating/manual inference. | **Resolved.** | Runtime and `--mapping-diagnostics` entrypoints are disjoint; focused tests fail if runtime calls development functions. |
| Mapping-development evidence leaked into default doctor readiness. | **Resolved.** | Doctor calls `validate_manual_registry(..., include_development=False)`. |
| Vehicle source, runtime, and review products shared one Ontario interim directory; processed/validation layers were empty or ambiguous. | **Resolved architecturally, partially stale on disk.** | Typed routes and current writers separate the layers; two obsolete lifetime-comparison files remain misplaced/unconfigured. |
| Three competing mapping-candidate snapshots had unclear identity. | **Resolved.** | Reviewed config, runtime outputs, and mapping-review evidence now have distinct owners/routes; old output-validation candidate copies are gone. |
| Backend architecture listed a nonexistent vehicle-class facade and omitted actual evidence adapters. | **Resolved.** | The current structural tree lists FuelEconomy and both vPIC adapters and no synthetic facade. |
| Vehicle-label normalization overlapped. | **Still present but not proven duplicate.** | The two normalizers have distinct null, annotation, transliteration, and comparison semantics. |
| vPIC request/cache/manifest loops were duplicated. | **Still present, bounded.** | Adapters remain separate; response model and general file mechanics are shared, while endpoint-specific loops remain. |
| Mapping development and runtime were coupled in one execution path. | **Resolved at execution; partially present in module ownership.** | Separate entrypoints and routes exist, but road development helpers and runtime mapping functions still share one module. |
| Validation responsibilities and database publication ownership overlapped. | **Largely resolved.** | Config, schema, insertion, integrity, publication, and comparison owners are explicit. Route consumers still overstate current parameter-to-database integration. |
| Default orchestration omitted the vehicle family. | **Still present.** | The 95-line Snakefile remains doctor/StatCan/CER/template database only; this is now an explicit coverage gap rather than hidden transformation duplication. |

Meaningful growth since the previous run is structural rather than volumetric: `src/`
grew by roughly 55 physical lines and two utility modules, tests gained the 62-line
architecture-fitness module, road aggregation gained explicit runtime/diagnostic
entrypoints, and affected vehicle artifacts moved to typed families. No production source
module was mechanically split.

## 8. Validation performed

The following focused checks were selected because they exercise the claims changed since
the prior snapshot without refreshing sources or publishing artifacts:

```powershell
uv run pytest -p no:cacheprovider tests/test_architecture_fitness.py tests/test_runtime_hygiene.py tests/test_vehicle_mapping_bootstrap.py::test_runtime_reads_but_does_not_overwrite_reviewed_mapping tests/test_vehicle_mapping_bootstrap.py::test_mapping_diagnostics_remain_explicitly_callable
```

Result: 10 tests passed in 4.65 seconds with one upstream `canoe_schema` deprecation
warning. The full test suite, ETL commands, notebook execution, and Snakemake dry-run were
intentionally not required:
this task changes only diagnostic/retrieval documentation, and those commands would not
improve the static responsibility or ownership evidence enough to justify broader runtime
activity. Frontmatter parsing, inventory checks, scoped diff inspection, and whitespace
validation are also part of completion.

## 9. Prioritized decision surface for later work

### 1. Separate accepted lifetime outputs from MTO survival diagnostics

- **Why it matters:** one direct command currently requires the full MTO history/mapping
  analysis even when a consumer needs only accepted source curves and medians.
- **Evidence:** `build_lifetime_artifacts` runs both chains and publishes to interim,
  processed, and validation routes; final medians use transformed source/legacy curves.
- **Risk if unchanged:** expensive diagnostic prerequisites remain coupled to future
  reproducible lifetime generation, and stale review evidence can be mistaken for a
  required parameter input.
- **Likely payoff:** clearer runtime prerequisites, cheaper focused reruns, and independent
  review refreshes.
- **Safe constraints:** preserve age-zero/transition semantics, all current files and
  schemas, configured decision gates, source labels, Wards weighting, lifetime tests, and
  parity evidence. Compare two entrypoints in one module against a module boundary before
  choosing.

### 2. Reconcile declared consumers and DAG coverage with actual database compilation

- **Why it matters:** the topology names `build_transport` as a consumer of lifetime and
  stock artifacts although the builder loads only two templates, and the DAG does not
  produce vehicle artifacts.
- **Evidence:** `config/paths.yaml`, `workflow/Snakefile`, `build_transport.TEMPLATE_TABLES`,
  and the shallow consumer-resolution check in `test_architecture_fitness.py`.
- **Risk if unchanged:** impact analysis and readiness can imply end-to-end integration
  that does not exist.
- **Likely payoff:** truthful architecture fitness and safer sequencing of later compiler
  integration.
- **Safe constraints:** do not add vehicle stages merely for completeness. Decide whether
  routes describe current or intended consumers, then test actual artifact reads/targets
  at the chosen contract.

### 3. Examine a road mapping runtime/development code boundary

- **Why it matters:** behavior is separated, but candidate/review changes and runtime
  mapping still share a 1,753-line module and bootstrap imports both kinds of helpers.
- **Evidence:** disjoint entrypoints at lines 1562 and 1625 and bootstrap imports at lines
  16-26.
- **Risk if unchanged:** development iterations retain a large change and review surface
  around runtime mapping application.
- **Likely payoff:** clearer ownership and smaller regression surface without changing
  artifact contracts.
- **Safe constraints:** preserve the runtime mapping API, mapping bytes, output schemas,
  coverage, candidate diagnostics, CLI compatibility, and focused tests. A compatibility
  facade or internal namespace may be preferable to an immediate file split.

### 4. Make artifact-backed mapping acceptance and generated-evidence freshness explicit

- **Why it matters:** an ordinary pytest test requires ignored artifacts, while current
  directories contain two stale lifetime-comparison files inconsistent with current routes.
- **Evidence:** the artifact-backed test at lines 760-907, `.gitignore`, current artifact
  inventory, and the current lifetime writer map.
- **Risk if unchanged:** fresh-clone tests can fail or, worse, pass against mismatched
  evidence generations.
- **Likely payoff:** deterministic unit validation and a reviewable integration evidence
  lifecycle.
- **Safe constraints:** retain all material mapping/arbitration/coverage assertions;
  require registered prerequisites, a manifest/run identity, or an explicit integration
  marker rather than weakening checks.

### 5. Reduce internal pipeline complexity before splitting cohesive large modules

- **Why it matters:** the highest local complexity is concentrated in long orchestration
  and transformation functions in bootstrap, Ontario Report A, NLR PHEV, and manual
  selector resolution.
- **Evidence:** 270-392 line functions, large argument surfaces, and deep orchestration
  nesting, while their module-level responsibilities remain coherent.
- **Risk if unchanged:** changes require reasoning about many intermediate frames and
  failure paths at once.
- **Likely payoff:** smaller independently testable stages without new package boundaries
  or changed ownership.
- **Safe constraints:** preserve source-native audit columns, deterministic writes,
  logged counts/warnings, no-download behavior, current public functions, and focused
  fixture tests. Extract only stages with stable input/output invariants.

### 6. Extract only proven small-source/vPIC shared mechanics

- **Why it matters:** assorted sources bundle unrelated parsers, NRCan combines two
  physical contracts, and vPIC repeats JSON request/cache publication.
- **Evidence:** existing separate request models/rule groups/fetch functions for NRCan;
  source-specific normalizers plus one dispatcher in assorted sources; nearly parallel
  vPIC cache/error/manifest loops.
- **Risk if unchanged:** source-specific changes cross broad modules and repeated cache
  mechanics can drift.
- **Likely payoff:** narrower change impact and one tested implementation of exact shared
  publication behavior.
- **Safe constraints:** retain source-specific validation, URLs, cache identities, offline
  failures, manifests, and normalized schemas. Prefer source adapters behind a retained
  orchestrator and a narrow JSON/file primitive over a universal fetch framework.

The strongest present case is for execution and evidence boundaries, not wholesale module
splitting. `vehicle_population`, `stocks_and_demands`, `build_transport`, and the shared
utilities are cohesive or central for coherent reasons. Road, lifetime, assorted-source, and
NRCan boundaries deserve targeted experiments only when a future change can preserve the
listed interfaces and artifacts with focused parity evidence.
