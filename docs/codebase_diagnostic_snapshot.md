---
title: Codebase diagnostic snapshot
role: Point-in-time evidence for complexity, modularity, shared ownership, and bounded structural improvement.
retrieve_when: A task affects module boundaries, architecture fitness, shared infrastructure, development/runtime separation, or efficiency/refactor decisions.
read_scope: Read only relevant diagnostic sections unless the task is explicitly repository-wide.
verify: Current code, configuration, tests, schemas and generated evidence outrank this snapshot.
last_diagnostic_run: 2026-10-02
local_commit_reviewed: 1bd946bc19a6d8df2cc00331a7f284626d125167
---

# Codebase diagnostic snapshot

The largest remaining responsibility concentrations are parameter assembly plus artifact
publication and shared mechanics held by domain
modules with broader names. Recent changes have improved fleet-weight ownership, typed
configuration, parameter-support checks and read-only scenario comparison. This refresh
also completes four bounded corrections; it identifies subsequent work without selecting
a wholesale redesign or changing modelling assumptions.

Use [artifact routes](../config/paths.yaml) to bound implementation impact,
[backend architecture](backend_architecture.md) for durable ownership, and
[CANOE-main context](canoe_main_orchestrator.md) for the integration seam. This snapshot
records executable evidence and recommendations; it does not accept parity gaps,
assumptions or unfinished integration contracts.

## 1. Scope, revision and measurement

- Reviewed on 2026-10-02 at local HEAD `1bd946bc19a6d8df2cc00331a7f284626d125167`
  (`feat: Add road fleet weights and validation for regional aggregation`), followed
  by the task's bounded fixes in section 4. Metrics describe that final working tree,
  not HEAD alone. Comparison counts use the 2026-10-01 diagnostic.
- Initial unrelated work consisted of two efficiency smoke directories; both were
  preserved. Cached/manual inputs, legacy code, published databases, user-authored
  context docs, README and the research notebook were outside the edit scope.
- All Python under `src/`, `scripts/`, `tests/` and the research notebook under
  `docs/insights/` was inventoried and parsed: **94 files**. Every source/script
  module is covered in section 3. Workflow/profile, typed source/scenario controls,
  affected artifact routes, callers and boundary tests were inspected separately.
  Legacy/generated code, caches and `.venv/` were excluded from metrics.
- Physical lines include blanks/comments/docstrings. Approximate LOC excludes blank
  and full-line comment lines but includes docstrings. Function counts include methods
  and nested helpers; span is `end_lineno - lineno + 1`.
- Branch signals count AST `If`, loops, `Try/TryStar`, `IfExp`, `BoolOp`,
  `Match` and comprehension nodes. Nesting counts control-flow/context-manager
  blocks, including nested helper bodies. These are inspection signals, not
  cyclomatic complexity, runtime cost, defect probability or line-count limits.
- The production graph contains **49 source/script modules and 126 distinct local
  import edges**, including delayed imports. Relative imports are resolved against
  their package and imported names against the longest matching local module.
  Tests, external dependencies, subprocess entrypoints and file-mediated edges are
  excluded. No static cycles remain. Initially there were 127 edges and one delayed
  lifetime/stock cycle; section 4 records its removal.
- A narrow duplicate screen normalized function names, removed leading docstrings and
  compared location-independent ASTs for spans of at least eight lines and 20 nodes.
  No exact production duplicates remain under that screen. Similar mechanics still
  require caller/contract inspection; text writers with different signatures and
  serialization were one such case.

Read-only discovery used `rg`; inventory, imports, nesting and relocation equivalence
used Python's AST through `uv run --offline python -B -`. Representative checks:

```powershell
git status --short
git rev-parse HEAD
rg --files src scripts tests docs/insights -g '*.py'
rg -n '^def |^class |^from |^import ' src scripts
rg -n 'prepare_|resolve_artifact_path|read_csv|write_text|write_dataframe_atomic' src
uv run --offline pytest --collect-only -q
uv run --offline ruff check src scripts tests --statistics
```

No production ETL/build, source refresh, notebook execution or legacy parity run was
performed. Tests wrote temporary fixtures/databases; accepted local inputs were read
where existing tests require them. No measured runtime speedup or complete integrated
compiler readiness is asserted.

## 2. Scale and executable boundaries

| Scope | Python files | Physical lines | Approximate LOC | Change from 2026-10-01 |
| --- | ---: | ---: | ---: | --- |
| `src/` | 46 | 26,776 | 25,015 | +2 files, +563 lines, including this refresh's fixes. |
| `scripts/` | 3 | 400 | 330 | Unchanged. |
| `tests/` | 44 | 11,728 | 10,339 | +4 files, +923 lines. |
| Research notebook | 1 | 3,969 | 3,692 | Measured separately from production. |

There are 43 test modules plus `conftest.py`, **331 statically defined test functions
and 417 collected cases**. Collection verifies discovery/import readiness; section 7
distinguishes the tests actually executed.

[build_transport](../src/build_transport.py) composes templates and seven families:
capacity, demand, utilization, lifetimes, efficiencies/input splits, costs and chargers.
`TransportContribution` retains row-model blocks, provenance, family audits and support
evidence. Preparation writes no SQLite rows; insertion validates/rechecks rows on the
caller's connection without committing. Standalone initialization and atomic publication
wrap this same seam.

Recent structural improvements verified against current code:

- [road_fleet_weights](../src/parameterization/road_fleet_weights.py) centralizes regional
  selection, LDV/medium/heavy weights, evidence paths and component lineage.
  Four parameter owners import it. Required `aggregation_sources` selectors now live
  in scenario YAML and are checked against active implemented source adapters before I/O.
  BC/BCT lookup aliases do not resolve model-region semantics.
- [config_models](../src/validation/config_models.py) provides explicit typed source,
  scenario, economics, representation and comparison controls. Source identity/DQ remain
  registry-owned; harmonization rules have fetching/parameterization owner groups.
  This replaces several stale configuration findings in the previous snapshot.
- [insertion](../src/validation/insertion.py) now checks assembled historical capacity,
  efficiency/cost support, positive stock and new factor vintages before publication;
  `insert_transport_contribution` repeats support checks before writes.
  These guards protect partial-layer and mutable-row boundaries.
- [sqlite_compare](../src/validation/sqlite_compare.py) adds read-only same-v4 scenario
  comparison, optional provenance, tolerances and key/column diagnostics. It runs before
  standalone publication. It complements legacy comparison and is not a feasibility or
  parity-acceptance authority.
- Lifetime preparation derives accepted frames in memory rather than treating previously
  published lifetime CSVs as parameter authority. Accepted versus MTO/Wards review
  publishers remain separate and tested. Source policy changes are owned by config;
  this diagnostic does not repeat or approve their scientific rationale.

### Concentrated functions

| Function | Module lines | Start / span | Arguments | Branch / nesting | Inspection implication |
| --- | ---: | ---: | ---: | ---: | --- |
| `build_costs.prepare_cost_rows` | 920 | 190 / 717 | 4 | 132 / 10 | Evidence, pathway row assembly, provenance and publication share nested state. |
| `build_efficiencies.prepare_efficiency_rows` | 912 | 306 / 512 | 2 | 70 / 7 | Still long after fleet-weight extraction; several independently testable stages. |
| `build_existing_capacity.build_existing_capacity_artifacts` | 788 | 215 / 553 | 1 | 54 / 5 | Mode assembly plus prerequisite generation and publication. |
| `ev_chargers.prepare_ev_charger_rows` | 480 | 123 / 337 | 3 | 90 / 5 | Several parameter blocks share one charger pathway. |
| `road_stocks_and_demands.distribute_existing_road_capacity` | 1,272 | 513 / 329 | 16 | 80 / 4 | Sixteen evidence/config inputs and cohort eligibility stages. |
| `road_stocks_and_demands.distribute_existing_bus_capacity` | 1,272 | 844 / 294 | 11 | 60 / 5 | A distinct allocation contract; do not generalize into off-road allocation. |
| `vehicle_mapping_bootstrap.build_bootstrap_mapping` | 1,948 | 1438 / 399 | 1 | 35 / 3 | Long ordered development pipeline with established stage helpers. |
| `nlr_atb_autonomie.derive_phev_efficiency` | 1,615 | 775 / 300 | 8 | 18 / 1 | Joined source calculation; low nesting differs from builder complexity. |
| `vehicle_population.normalize_report_a` | 1,759 | 887 / 309 | 6 | 9 / 1 | Large source-native normalization with low control-flow depth. |
| `manual_parameters.resolve_manual_parameters` | 798 | 429 / 312 | 3 | 41 / 5 | Cohesive selector resolver; extract stages rather than a rules engine. |
| `road_aggregation.unresolved_mapping_reasons` | 1,767 | 947 / 276 | 4 | 7 / 2 | Review/reporting work beside runtime mapping. |
| `road_lifetimes_survival._derive_mto_diagnostic_outputs` | 2,247 | 1937 / 186 | 3 | 16 / 0 | Opt-in review estimator; not a normal ETL dependency. |

The highest direct fan-ins are `utils` (29), `validation.config_models` and
`validation.provenance` (11 each), `validation.insertion` (9),
`road_efficiencies` and `manual_parameters` (6 each). High reuse of a small trust
boundary is useful. A large module with few importers may still own many source files,
configuration keys, publishing decisions or command-line responsibilities.

## 3. Complete responsibility and disposition map

Counts include local internal imports only. “Extract” identifies a possible bounded
implementation slice; it does not prescribe a new package or authorize model changes.

### Source adapters

| Module | Lines | Functions / classes | Fan-in / out | Responsibility and disposition |
| --- | ---: | ---: | ---: | --- |
| [fetching.assorted_sources](../src/fetching/assorted_sources.py) | 1,836 | 44 / 5 | 0 / 2 | Independent NHTSA/NEMS/GCAM/ReGen/FAA/TC adapters behind one dispatcher. Extract a source family when its request/publisher contract needs independent evolution; TC already has a separate entrypoint. |
| [fetching.vehicle_population](../src/fetching/vehicle_population.py) | 1,759 | 47 / 4 | 0 / 2 | CKAN discovery, ZIP validation, Reports A/4/5 normalization and reconciliation share one source lineage. Retain the source owner; reduce dispatcher load before splitting reports. Keep its streaming large-table publisher. |
| [fetching.nlr_atb_autonomie](../src/fetching/nlr_atb_autonomie.py) | 1,615 | 29 / 4 | 3 / 2 | Archive/member contracts, vehicle prices, VMT/maintenance and PHEV energy reconciliation share ATB/Autonomie evidence. Extract internal archive/PHEV stages if needed; avoid another ATB transformation path. |
| [fetching.statcan_tables](../src/fetching/statcan_tables.py) | 915 | 24 / 2 | 0 / 2 | Shared StatCan metadata/ZIP validation plus historical fuel shares and freight candidates. Keep physical parsing source-owned; JSON publication now uses the shared text primitive. |
| [fetching.nrcan_ceud](../src/fetching/nrcan_ceud.py) | 907 | 32 / 2 | 0 / 2 | CEUD workbook and fuel-rating CSV request/normalization contracts differ. A future split can follow those source contracts; provincial/national CEUD geography alone is a weak split criterion. |
| [fetching.cer_enerfuture](../src/fetching/cer_enerfuture.py) | 548 | 17 / 2 | 1 / 2 | Edition/scenario requests, physical validation, macro/demand/price normalization and manifests. Retain this source owner, separate from parameter demand/currency calculations. |
| [fetching.vpic_vehicle_types](../src/fetching/vpic_vehicle_types.py) | 526 | 14 / 3 | 1 / 2 | Type/scope endpoint requests, response validation, cache replay and classification. Preserve this endpoint-specific eligibility contract. |
| [fetching.vpic_model_years](../src/fetching/vpic_model_years.py) | 436 | 16 / 4 | 0 / 3 | Temporal corroboration and older Canadian specifications; reuses the typed vPIC response. Share small mechanics when equivalent; do not merge distinct evidence meanings. JSON publication is consolidated. |
| [fetching.fueleconomy_vehicles](../src/fetching/fueleconomy_vehicles.py) | 353 | 10 / 1 | 1 / 2 | One registered ZIP/table and vehicle-class normalization contract. Retain source ownership; warning publication now shares file mechanics. |

### Parameter preparation and numerical owners

| Module | Lines | Functions / classes | Fan-in / out | Responsibility and disposition |
| --- | ---: | ---: | ---: | --- |
| [parameterization.road_lifetimes_survival](../src/parameterization/road_lifetimes_survival.py) | 2,247 | 38 / 0 | 3 / 5 | Accepted source curves/medians, row expansion and separate MTO/Wards review estimators/publishers. Size mixes runtime and opt-in diagnostics. Fixed-lifetime selection now has this single owner; preserve the tested accepted/diagnostic boundary. |
| [parameterization.vehicle_mapping_bootstrap](../src/parameterization/vehicle_mapping_bootstrap.py) | 1,948 | 29 / 0 | 0 / 3 | Ordered source evidence, automatic support and candidate-map construction. Opt-in development owner; no production importers. Keep it out of normal readiness and never automatically replace reviewed mappings. |
| [parameterization.road_aggregation](../src/parameterization/road_aggregation.py) | 1,767 | 27 / 0 | 2 / 2 | Reviewed mapping consumption and runtime weights alongside unresolved-mapping review reports. Preserve read-only reviewed mappings; a future split should isolate review reporting from accepted mapping application. |
| [parameterization.road_stocks_and_demands](../src/parameterization/road_stocks_and_demands.py) | 1,272 | 16 / 0 | 3 / 2 | Stock age artifacts, road/bus cohort allocation, eligibility and utilization arithmetic. Fixed lifetimes now belong to the lifetime owner; remaining utilization ownership still needs clarification. |
| [parameterization.road_efficiencies](../src/parameterization/road_efficiencies.py) | 921 | 17 / 1 | 6 / 2 | Ratings/ATB/load-factor calculations, bus evidence and a cached per-instance evidence object. Retain the evidence object; interpolation/positivity/CEUD selectors used outside efficiency deserve a narrower owner. |
| [parameterization.build_costs](../src/parameterization/build_costs.py) | 920 | 13 / 1 | 1 / 12 | Price evidence, currency/service conversions, vintage eligibility, O&M, provenance and publication. Highest control-flow concentration; extract concrete pathway stages within this owner. |
| [parameterization.build_efficiencies](../src/parameterization/build_efficiencies.py) | 912 | 10 / 1 | 1 / 10 | Source/relationship selection, historic/future efficiencies, PHEV splits, provenance and publication. First candidate for a bounded stage decomposition; retain its result contract. |
| [parameterization.manual_parameters](../src/parameterization/manual_parameters.py) | 798 | 12 / 5 | 6 / 2 | Typed manual registration, physical/table validation, applicability resolution and reconciliation. Shared contract with six importers; extract local resolver stages if needed, rather than a universal rules engine. |
| [parameterization.build_existing_capacity](../src/parameterization/build_existing_capacity.py) | 788 | 8 / 0 | 4 / 8 | Road/bus/off-road assembly, prerequisites, provenance, cleanup and publication. Retain the public preparation seam; extract mode/stage calculations locally. |
| [parameterization.road_utilization](../src/parameterization/road_utilization.py) | 560 | 8 / 1 | 1 / 5 | C2A, flat annual factors and audit-only age utilization. Shared fleet weights are now imported; five utilization algorithms still live in the stock module. |
| [parameterization.offroad_stocks_and_demands](../src/parameterization/offroad_stocks_and_demands.py) | 493 | 5 / 0 | 2 / 0 | Service demand, additions, survival and redistribution use different evidence/dimensions from vehicle stock. Retain domain separation; shared arithmetic needs equivalent contracts. |
| [parameterization.ev_chargers](../src/parameterization/ev_chargers.py) | 480 | 6 / 1 | 1 / 6 | Vehicle-stock-to-port capacity, efficiency/cost blocks, utilization evidence and composite provenance form one charger pathway. Extract stages here; fix prepared-row versus disk-fingerprint coupling next. |
| [parameterization.build_demand](../src/parameterization/build_demand.py) | 265 | 5 / 0 | 1 / 6 | CEUD service baseline, CER GDP indexing, configured period expansion and provenance. Bounded family owner; shared macro data does not justify merging with currency/costs. |
| [parameterization.offroad_lifetimes](../src/parameterization/offroad_lifetimes.py) | 207 | 2 / 0 | 2 / 4 | Manual lifetime resolution and StatCan bus lifetime logic. The bus function serves road assembly despite the filename; clarify its semantic owner when that interface changes. |
| [parameterization.road_fleet_weights](../src/parameterization/road_fleet_weights.py) | 207 | 9 / 1 | 4 / 3 | New canonical regional LDV/medium/heavy weight selector and evidence bundle used by costs, efficiencies, utilization and lifetimes. Keep this demonstrated shared owner; selectors are scenario-owned. |
| [parameterization.offroad_capex_opex](../src/parameterization/offroad_capex_opex.py) | 188 | 5 / 1 | 1 / 1 | Aircraft service/currency conversion and manual off-road cost relationships. Preserve explicit units and source-specific relationships. |
| [parameterization.road_capex_opex](../src/parameterization/road_capex_opex.py) | 174 | 5 / 0 | 1 / 1 | ATB price selection, aggregation and road maintenance/repair curves. Keep beside road numerical evidence, below schema-row assembly. |
| [parameterization.build_lifetime_parameters](../src/parameterization/build_lifetime_parameters.py) | 135 | 3 / 1 | 2 / 4 | Small representation assembler for road/manual/bus fixed or survival rows, plus explicit artifact publication. Keep this interface and its coverage checks. |
| [parameterization.currency](../src/parameterization/currency.py) | 99 | 2 / 2 | 2 / 0 | Small auditable CER FX/deflator converter with an explicit conversion result. Healthy shared numerical owner; retain native and converted values. |
| [parameterization.offroad_efficiencies](../src/parameterization/offroad_efficiencies.py) | 95 | 2 / 0 | 1 / 1 | Bounded CEUD/manual off-road trajectories. Keep separate from road pathways; avoid genericizing different service units. |

### Assembly, trust boundaries and shared mechanics

| Module | Lines | Functions / classes | Fan-in / out | Responsibility and disposition |
| --- | ---: | ---: | ---: | --- |
| [build_transport](../src/build_transport.py) | 962 | 18 / 4 | 0 / 15 | Composition, schema preflight, templates, family reuse, provenance, support checks and caller insertion; standalone atomic publication wraps the same path. High fan-out is expected here. Separate concrete stages only while retaining one implementation. |
| [validation.config_models](../src/validation/config_models.py) | 452 | 17 / 30 | 11 / 0 | Thirty typed config/source models, including required scenario selectors and comparisons. Keep runtime trust here; adapter-specific extensions remain with their adapters. |
| [validation.legacy_compare](../src/validation/legacy_compare.py) | 343 | 8 / 0 | 1 / 1 | Legacy-specific numeric/key comparisons and parity diagnostics. Retain separate from same-schema scenario comparison. |
| [validation.provenance](../src/validation/provenance.py) | 316 | 9 / 3 | 11 / 1 | Resolved source/component/composite provenance, full DQ and dataset/source registration. Eleven importers are justified by one insertion trust chain. |
| [validation.insertion](../src/validation/insertion.py) | 274 | 6 / 0 | 9 / 1 | Typed-row validation, conflict handling, historical cleanup and assembled support checks. Nine importers; retain this shared boundary and recheck mutable rows before writes. |
| [utils](../src/utils/__init__.py) | 262 | 16 / 2 | 29 / 2 | ConfigBundle, YAML loading/path routing and cross-file checks. High reuse centralizes stable interfaces; avoid moving domain parameter calculations into this package. |
| [validation.schema_contract](../src/validation/schema_contract.py) | 192 | 8 / 1 | 3 / 0 | Installed package revision/DDL evidence and the explicit technology-notes extension. Preserve the pinned schema authority and tested compatibility scope. |
| [validation.sqlite_compare](../src/validation/sqlite_compare.py) | 176 | 6 / 0 | 1 / 1 | New read-only v4 scenario comparator with optional provenance and configured tolerances; reports incompatible/duplicate keys. Useful diagnostic owner, independent of build logic. |
| [validation.database_bootstrap](../src/validation/database_bootstrap.py) | 136 | 1 / 0 | 1 / 1 | Read-only schema, expected-key, provenance, FK/integrity and model-default validation. Cohesive publication guard; do not merge its checks into builders. |
| [utils.vehicle_labels](../src/utils/vehicle_labels.py) | 106 | 6 / 1 | 4 / 0 | Small canonical vehicle-label equivalence contract shared by mapping/source evidence. Retain its explicit semantics. |
| [validation.config_smoke](../src/validation/config_smoke.py) | 66 | 1 / 0 | 1 / 2 | Typed config/schema readiness and configured-directory smoke checks. Keep distinct from parameter/source completeness and production compilation. |
| [utils.files](../src/utils/files.py) | 57 | 3 / 0 | 1 / 0 | Streaming SHA-256 and atomic CSV/text publication. Four repeated text/JSON mechanics now share one implementation; serialization remains caller-owned. |
| [setup](../src/setup.py) | 45 | 3 / 0 | 0 / 2 | Setup smoke CLI/status wrapper over shared configuration validation. Keep distinct from doctor source/manual readiness. |
| [validation.sqlite_utils](../src/validation/sqlite_utils.py) | 16 | 2 / 0 | 4 / 0 | Quoted SQL identifiers and non-creating read-only SQLite opener shared across comparisons. Retain these small mechanics. |

### Operational scripts and other surfaces

| Module | Lines | Functions / classes | Fan-in / out | Responsibility and disposition |
| --- | ---: | ---: | ---: | --- |
| [scripts.clean_runtime](../scripts/clean_runtime.py) | 214 | 11 / 2 | 0 / 1 | Bounded runtime cleanup/dry-run inventory. Keep explicit path/permission safeguards; no ETL or source authority. |
| [scripts.doctor](../scripts/doctor.py) | 185 | 8 / 1 | 0 / 3 | CLI for config, manual and cached-source physical readiness. It neither parameterizes transport nor proves complete baseline coverage. |

The four package initializers are `fetching/__init__.py` (0 lines) and
`parameterization/__init__.py`, `validation/__init__.py`,
`scripts/__init__.py` (1 line each); they contain no runtime owner logic.

The research notebook `docs/insights/vehicle_stock_diagnosis.py`
(3,969 lines, 75 functions) has substantial inspection, plotting and review responsibility
but is not a production parameter owner. Tests are evidence for the boundaries above,
not alternate transformations. No notebook or test-module decomposition was performed.

Orchestration update (2026-10-08): the supported scenario interface is
`build_transport.build_from_scenario`, including ordered production prerequisite replay
through `prepare_scenario_inputs`. It rebuilds derived evidence from registered raw inputs
on each invocation and retains the existing live preparation/insertion seam. The older
workflow and source-cache profile have been removed. Earlier module metrics above remain
a dated snapshot, not a remeasurement of this change.

## 4. Bounded fixes completed in this refresh

| Finding | Final owner/change | Evidence of preservation |
| --- | --- | --- |
| `road_lifetimes_survival` imported a fixed-lifetime selector from stocks, while stock-age preparation lazily imported accepted lifetime derivation. | Moved `fixed_existing_lifetimes` into `road_lifetimes_survival`; updated capacity/test imports and removed the old definition. No forwarding alias or new module. | Function AST is identical before/after relocation; reviewed fixed values, both capacity eligibility modes, lifetime rows and caller insertion tests pass. Static cycle removed. |
| FuelEconomy, Ontario population, vPIC temporal and StatCan metadata had separate text/JSON replacement mechanics. Some paths used deterministic temporary names or could leave a temporary file after write failure. | One `utils.files.write_text_atomic` implementation creates a unique sibling, replaces the destination and cleans up on write/replace failure. Callers retain JSON sorting/formatting and LF versus platform-newline contracts. Streaming/binary source publication is unchanged. | UTF-8/newline/empty-text cases and write/replace failures pass; source request/parser/cache-replay tests pass. No cached artifact was rewritten. |
| Dashboard input routing omitted the capacity builder, and prepared capacity routing omitted the charger's file fingerprint dependency. | Added the demonstrated consumers in `config/paths.yaml`. | Route owners/producers/consumers resolve; configuration and architecture checks pass. This is impact metadata, with no parameter or path-value change. |

## 5. Remaining shared ownership and execution gaps

### Preparation, publication and scenario isolation

Several `prepare_*` functions publish shared interim/processed/audit files; standalone
calls can also generate prerequisites. The SQLite boundary is separated, but preparation
is not uniformly free of artifact side effects. A future adapter must account for these
effects as well as its database transaction.

The contribution already passes prepared capacity into efficiencies, costs and chargers.
However, `ev_chargers.prepare_ev_charger_rows` also fingerprints
`existing_capacity_processed/existing_capacity.csv` although its stock values and
provenance come from supplied rows/contexts. That on-disk file can belong to an earlier or
another scenario. Passing a complete prepared-input identity would remove this mixed
authority; the current fix only makes the dependency visible.

Scenario database/report names are explicit, while many preparation outputs share
rule-defined filenames/directories. There is no demonstrated parallel-scenario isolation
contract for those products. This is a potential collision/stale-lineage risk, not an
observed concurrent-build failure. Resolve output identity and input digests alongside
the preparation/publication boundary rather than adding a global mutable cache.

### Helpers that should converge; owners that should remain separate

| Current ownership | Evidence / next boundary |
| --- | --- |
| Fleet aggregation formerly repeated across costs/efficiencies/utilization/lifetimes | The new `road_fleet_weights` is the shared owner. Preserve it; consider sharing its evidence per build only if repeated I/O is measured. |
| Five utilization algorithms still in `road_stocks_and_demands` | `road_utilization` imports mileage aggregation, annual utilization, flat factor records, age-period means and reconciliation. Move unchanged behavior toward the utilization owner when this seam is next edited; retain stock allocation separately. |
| Generic `positive`, `interpolate` and CEUD selectors in `road_efficiencies` | Costs and off-road numerical helpers import efficiency-owned mechanics. Inspect equivalent unit/key/extrapolation contracts before selecting a smaller numerical owner; do not introduce a universal transformation engine. |
| StatCan bus lifetimes in `offroad_lifetimes` | The capacity and lifetime assemblers use them for road buses. Naming/ownership needs attention; the bus algorithm should not merge with stock allocation or generic manual lifetime resolution. |
| Accepted and review estimators in `road_lifetimes_survival`; mapping review in `road_aggregation` | Runtime/publisher separation is already explicit. An extraction can follow opt-in review contracts, preserving accepted outputs and import readiness. Size alone does not justify moving all scientific calculations. |
| Legacy versus scenario SQLite comparison | Distinct schemas, keys and diagnostic meanings justify separate comparators. Shared quoting/read-only opening already has one owner. |
| `assorted_sources` versus individual adapters | Separate source identities/requests support eventual source-family extraction. Shared archive/path mechanics do not justify merging parsing and harmonization policy. |

### Native prerequisite coverage (2026-10-08)

The scenario command replays CEUD/ratings, StatCan, CER, ATB/Autonomie, assorted inputs,
TC dashboard and the selected Ontario edition, validates FuelEconomy cache evidence and
rebuilds reviewed road aggregation. Parameter preparation consumes those fresh products;
GREET, charging and OMEGA validation stay with their existing owners. Ontario cache
identities now live in the source registry, removing dependence on an interim manifest.
No transformation implementation or second dependency graph is introduced.

Full replay deliberately replaces incremental scheduling. Shared paths still require
serial scenarios; standalone atomic publication preserves the database on build failures
and restores the prior report if database replacement fails. The pair of files cannot be
crash-atomic together. Optional historical/mapping diagnostics and external-model generation
remain separate. Validation commands and measured results are recorded in ExecPlan 075.

## 6. Priorities and concrete next slices

| Priority | Current evidence / consequence | Bounded next work and verification |
| --- | --- | --- |
| 1 — Prepared-input and artifact identity | Charger disk fingerprint plus supplied rows; preparation side effects and shared scenario paths. These affect reproducibility and adapter safety. | Define a complete capacity preparation result/digest and an explicit publication step for one family. Test supplied-input replay and isolated scenario artifacts; preserve numerical row batches and provenance. |
| 2 — Long family assembly functions | Cost 717 lines/132 branch signals, efficiency 512/70, capacity 553/54; source/pathway selection and publication share state. | Start with efficiency stage helpers within its current owner/result interface. Separate evidence loading, pathway calculation, row/provenance assembly and publication one layer at a time. Verify keys, units, source chains, missing behavior and row equivalence. |
| 4 — Remaining helper ownership | Utilization in stocks; general selectors in efficiency; road bus lifetimes in an off-road owner. | Relocate one proven contract and update its callers without forwarding copies. Preserve domain-specific unit/eligibility semantics and proportional boundary tests. |
| 5 — Review/source-family separation | Large lifetime/mapping modules combine runtime and opt-in review; assorted adapters have independent source identities. | Extract one review or source family when independent maintenance warrants it. Verify production preparation never invokes review, and deterministic offline source replay is retained. |
| 6 — CANOE-main adapter | Industry now demonstrates pure entity assembly; scope, shared rows, schema notes and fuel/emissions remain unresolved. | Resolve the contracts in the upstream context and construct an upstream-initialized fixture. Wrap the existing contribution seam; require conflict, provenance, connected endpoints and rollback checks before registration. |

Upstream industry is a useful standardization example: typed configuration, source-specific
loaders, named frame calculations, pure entity assembly, ordered insertion and declarations
from actual pathways. The local backend already has most corresponding owners.
Its full DQ/conflict validation and survival representation must remain intact; adopting
upstream writers directly would change those contracts. Static organization is not
performance evidence. Profile a representative offline build before trading traceability
for vectorization, caching or larger architectural changes.

## 7. Validation performed and limits

| Check | Current result | Limit |
| --- | --- | --- |
| Inventory/AST/import/duplicate screen | 94 files parsed; 49 production modules, 126 import edges, no static cycles or screened exact duplicates. | Excludes file/config/CLI dependencies and approximate similarities. |
| Lifetime/stock/file/architecture boundaries | 49 tests passed. Relocated function AST unchanged; UTF-8/newline and publication-failure cleanup verified. | Existing accepted inputs were read; no full row-batch/parity comparison performed here. |
| Source/config/provenance-related support, schema, fleet weights and scenario comparison | 156 tests passed across nine test modules. | Fixture/source-boundary evidence; no new acquisition or full integrated database build. |
| Caller-owned contribution | Two template-only transaction/schema tests passed; with the schema module above, six focused seam tests passed. | No CANOE-main-initialized multi-sector fixture. |
| Test discovery | 417 cases collected. | The entire suite was not executed. |
| Ruff and whitespace/doc contracts | Ruff, task diff whitespace, frontmatter, metrics and local/commit-pinned source links checked with the completed task. | Does not establish legacy parity, solver feasibility or benchmark performance. |

The task record is [ExecPlan 060](../.agents/plans/archive/060_industry_orchestrator_and_structural_refresh.md).
Reviewable modelling choices remain with their source/scenario/parameter owners and the
user-authored assumptions document; this generated snapshot carries no review tags.
