---
title: Codebase diagnostic snapshot
role: Point-in-time engineering evidence, finding reconciliation and bounded improvement priorities.
retrieve_when: A task affects module boundaries, architecture fitness, shared infrastructure, development/runtime separation, or efficiency/refactor decisions.
read_scope: Read the affected findings and evidence; full coverage is explicitly bounded below.
verify: Current code, configuration, tests, schemas and generated validation evidence outrank this snapshot.
last_diagnostic_run: 2026-11-04
local_commit_reviewed: aafdd708e54840a13188b82c3f80e62fa83ff0fc
previous_diagnostic_run: 2026-10-02
previous_local_commit_reviewed: 1bd946bc19a6d8df2cc00331a7f284626d125167
---

# Codebase diagnostic snapshot

The native backend retains one prerequisite/preparation/insertion lifecycle with explicit
source, full DQ and publication checks. The most consequential remaining engineering
work is prepared-input identity and shared artifact isolation, followed by a real
CANOE-main integration fixture. Historical CPU improvements are substantial; this review
does not establish another measured optimization or justify a new caching framework.

## Scope, methods and freshness

Reviewed on 2026-11-04 at the revision above plus this task's bounded documentation,
evidence-route, diagnostic-loader and test repairs. Pre-existing edits in
`docs/backend_architecture.md`, `docs/etl_flowcharts.md`, `.gitignore` and the new
codebase-improver package were preserved. [ExecPlan 137][plan] records commands, initial
failures/corrections, scenario fingerprints and omissions. The previous October snapshot
is retained in Git history and its original [ExecPlan 060][old-plan].

- Inventoried/parsed all Python in `src/`, `scripts/` and `tests/`: 162 files (71, 4,
  87 respectively). Inspected executable ownership/trust/publication seams, current
  optimization implementations, relevant fixtures and file/configuration edges via
  `config/paths.yaml`. Three affected diagnostic notebook consumers were examined;
  notebook calculations/plots were not comprehensively rerun or rendered.
- This is a codebase-wide screen with deeper investigation at the identified seams,
  not a line-by-line scientific audit of every adapter/parameter. Excludes legacy code,
  generated/cache/external-model contents, dependency implementations and solver runs.
  Existing comparison evidence and performance logs were read only.
- Current evidence: source/config/test inspection, isolated focused execution, typed
  loading of all four scenarios, actual captured-report loading and protected-file hashes.
  Reused historical evidence: Plans 075, 131, 132 and 136, dated native logs and their
  comparison records. No source refresh, native build, fresh benchmark, registered-input
  equivalence campaign, full upstream compile or solver was run.

Reproducible entrypoints include `git status --short`, `git rev-parse HEAD`,
`rg --files src scripts tests -g '*.py'`, focused `uv run --offline pytest` commands in
[the verification record][plan], and `uv run --offline pytest --collect-only -q`.
The current local environment is Python 3.13.5, pandas 3.0.2 on Windows 11, using `uv.lock`;
these identities do not retroactively establish the old benchmarks' environment.

The static method parses UTF-8 Python with `ast`, names modules relative to `src/`
(or the repository for scripts), resolves relative imports and imported symbols to the
longest existing module prefix, excludes self-edges and deduplicates directed edges.
The production graph excludes `scripts/experiments`: 74 modules, 276 syntactic edges
including guarded/deferred imports; 274 without `TYPE_CHECKING`; 215 without type-only
or function-local imports. Four multi-module strongly connected components remain when
deferred imports count; none remain in the eager-only graph. They concern GREET wrappers,
rating/efficiency, mapping/reference and diagnostic/capture ownership. This is coupling
evidence, not an observed import failure. Dynamic imports, execution order and file/config
edges are outside this graph and were traced separately at affected interfaces.

The previous exact-duplicate screen was repeated: functions spanning at least eight
physical lines and 20 AST nodes, with function name normalized, leading docstring removed
and source locations ignored. No exact duplicate groups were found. Approximate duplication,
nesting and large assemblers still require contract inspection; none of these metrics is
a quality score or performance measurement. October's 49-module/126-edge and 94-file
figures describe older code and different inventory coverage, not a directly comparable
runtime result. Use the declared current scope/method for the next iteration.

## Historical performance evidence

Plans [131][p131] and [132][p132] contain the detailed profiles, experimental decisions and
comparisons. The two completed offline native runs on 2026-11-04 used the normal
`uv run python src/build_transport.py --scenario config/scenarios/default.yaml --overwrite`
entrypoint: ten provinces, prospective base 2025, five existing vintages, five model
periods through 2045/end 2050, constant CEUD extension, stock-age allocation, fixed MOVES
lifetimes, charging and explicit future range shares. Both logs record scenario SHA256
`5dc03a1b3dfd0e4bd6bd21b2d6b749040a39eb503b76f402f28a8c584b7ad051`, which still matches
the surviving default YAML. This does not establish equality of every current input.

| Native stage, elapsed seconds | Plan 131 completed control | Plan 132 optimized run |
| --- | ---: | ---: |
| Complete native build | 866.05 | 385.10 |
| Prerequisites | 93.27 | 73.01 |
| Existing capacity | 58.31 | 21.75 |
| Road utilization | 25.66 | 4.74 |
| Efficiencies | 546.11 | 222.08 |
| Composition, included in efficiencies | 343.71 | 132.51 |
| Vehicle costs | 101.43 | 23.28 |
| Insertion plus final checks | 13.18 | 13.63 |

The 55.5% elapsed reduction is historical evidence. Persistent logs
[control][control-log] and [optimized][optimized-log] currently corroborate timings,
222,135 contribution rows across 25 tables, all 14 support checks and zero FK violations.
Plan 132 records exact equality of every SQLite table/schema/key/model/provenance value
against the completed control at **absolute = relative tolerance = 0**. Plan 131 separately
records exact pre/post lookup-optimization equality. These are previous-implementation
regression references, not independent scientific justification or legacy parity. The
separately approved MOVES6 source correction was evaluated apart from optimization.

Conditions/limits: the control overlapped focused tests; the final optimized build had
no competing heavy test/profile work except a 20 Hz sampler (6,720 stacks, no local-value
capture). CPU totals were 842.38 and 373.48 seconds respectively. The records do not
supply a complete hardware/environment/input manifest or repeated whole-build variance.
The paired database controls and benchmark scratch were removed at task completion;
zero-tolerance equality is retained as the original recorded result, not rerun here.
Current logs were reread; old measurements are not current runtime guarantees.

| Component probe / representative work | Recorded improvement | Preservation evidence and limit |
| --- | --- | --- |
| Plan 131 ten-region 2023 composition; 600 cohorts/63,113 observations | 370.49 to 93.96 s | All five frames exact, including dtypes/order; smaller scope than complete history. |
| Plan 131 indexed provenance resolution; 8,384 lookups | 313.12 to 23.38 s | Exact results or exception type/message; not a whole-build speedup. |
| Plan 132 full-history composition; 11,080 cohorts/69,401 observations/83,551 variants | 358.07 to 132.62 s | Five frames exact, including indexes/columns/order; current fixture checks supplement the historical real-input comparison. |
| Plan 132 classification; 30,886 Ratings and 50,407 EPA rows | 71.79 to 5.34 s | All 63 columns, dtypes/values/order and invalid-input behavior compared. |
| Plan 132 ten-region cost preparation | 115.41 to 17.71 s | 4,050 investment/13,124 variable rows, 40 contexts, audits and 13 exports exact. |
| Plan 132 five StatCan reads/normalizations, including 7,910,600-row registrations | 48.15 to 26.02 s | Selected/normalized frames, input counts, member/dtypes/order exact in the recorded comparison; fixture-oracle limit below. |

The implemented changes remove invariant matching/schema/median work, repeated wide
frame filtering, technology-set construction and physical cost calculations within
existing owners. Source-sensitive reuse is preparation/instance-local; bounded global
text memoization is source-invariant. The full bus workload exposed a hotspot missed by
an ON-only probe; narrow validated CEUD/load views reduced its 2,340-row evidence probe
from 37.11 s to about 5.2 s. The unchanged diagnostic scenario historically built in
404.05 then 232.05 s; its 29-output app ran in 69.66 s. These are separate workloads.

Retain rejected lessons: ATB reuse saved about 1.08 s and was not adopted; another CEUD
experiment saved 0.66 s and was reverted. Currency/provenance were cheap in that profile.
A new persistent cache needs explicit identity/freshness/invalidation and complete audit
coverage. Validation must not be bypassed: its measured insertion/check cost was small.
Remaining historical costs were composition 132.51 s, source replay 73.01 s and about
30 s sampled CSV serialization across stages; sampled time is not an additive stage.
No current RSS/peak-memory or disk-throughput measurements exist. Present resource limits
and further reuse benefits remain hypotheses requiring a representative profile.

## Current findings and bounded priorities

| Stable ID / status | Owner/interface and consequence | Evidence / uncertainty | Priority and smallest useful next action |
| --- | --- | --- | --- |
| **CBI-ARTIFACT-IDENTITY — still open** | `ev_chargers.prepare_ev_charger_rows` combines supplied capacity rows/contexts with a hash of `existing_capacity_processed/existing_capacity.csv`. Identity depends on an incidental disk artifact. | High-confidence source evidence: [path](../src/parameterization/ev_chargers.py#L385), [digest](../src/parameterization/ev_chargers.py#L476). Different/missing disk artifacts can change identity or fail preparation; no concurrent corruption reproduced. | High: define a complete prepared-capacity handoff for this family. Prove exact numerical/coverage/DQ/audit preservation and supplied-input independence. Any corrected provenance identity is an intentional interface correction to evaluate separately. |
| **CBI-ARTIFACT-ISOLATION — still open** | Native prerequisites and family preparation publish shared interim/processed/audit files before final candidate publication. Failure can leave preparation evidence for the failed run while SQLite/report remain previously published. | Verified writes in capacity, efficiency, costs and utilization owners; [native ordering](../src/build_transport.py#L1346). Database/report/notebook-index replacement is individually atomic, not crash-atomic as a set. | High: retain serial scenarios; bound preparation/publication for one family before reuse or concurrency. Test rollback/interruption, output identity and stale/missing artifacts across consumers. |
| **CBI-INTEGRATION — still open, updated** | Caller-owned insertion exists; no upstream-initialized multi-sector fixture/adapter establishes shared rows, geography, notes, supply/emission or reduced-time compatibility. | Current local fixture checks pass; [upstream assessment](canoe_main_orchestrator.md) pins unstable `2c05f0af`. Electricity supply is implemented there; several former unknowns are now concrete contracts. | High before adapter work: agree ownership and compatibility, then one fixture exercising the existing prerequisite/contribution/insertion seam. Do not introduce parallel parameterization. |
| **CBI-PERFORMANCE-EVIDENCE — partly superseded** | Old repeated-computation advice is partly implemented. Composition/replay/CSV remain historical cost centers; repeated fleet evidence reads alone do not demonstrate current material cost. | Current local caches/indexes inspected and fixture regressions pass; no fresh native profile or memory measurement. | Medium: profile an equivalent full-history workload in one serial native build when performance work is authorized. Keep source replay/validation; assess kernels and full-build benefit separately before adding state. |
| **CBI-STATCAN-ORACLE — still open from Plan 136** | Stream preservation fixtures build expected frames with the same live `_select_chunk`. A selector defect can be shared by both sides. | [Exact regression test](../tests/test_statcan_tables.py#L532) preserves trim timing, dtypes/order/member/counts; independent province/freight tests help but do not exhaust multi-region overlap. No production defect established. | Low, before modifying selection: add explicit expected selected rows/region annotations and a two-region freight overlap fixture; retain exact frame/source-audit assertions. |
| **CBI-ASSEMBLY-COHESION / CBI-HELPER-OWNERSHIP — still open, conditional** | Large family assemblers combine calculation/row-provenance assembly and publication; five utilization algorithms live in stocks, general numerical helpers in efficiencies, road bus lifetimes in off-road lifetime code. | Verified source/import seams, no demonstrated numerical or runtime failure. Domain-specific extrapolation/eligibility/unit meanings differ. | Low: when an independently useful seam changes, extract/relocate one contract without forwarding duplicates. Compare exact rows, contexts, frames and fallback/errors; size alone does not justify refactoring. |
| **CBI-TEST-ISOLATION / CBI-ROUTE-CONSUMERS — resolved for the exercised cases** | Lifecycle mocks omitted CEUD/iZEV producers. Even the template-only caller test published range products through a real-root bundle. Deleted researcher YAML blocked a captured-report consumer; renamed evidence paths were stale. | Private lifecycle/caller roots, current producer mocks/order assertions and before/after artifact hashes; captured-report loader retains recorded metadata/export checks. Focused consumers pass. | Retain these checks. Other full bootstrap tests still use real-root preparation despite temporary SQLite paths; audit/isolate their complete footprint before running them. |

## Reconciliation with the previous snapshot

Older findings lacked stable IDs; the crosswalk below prevents unresolved items from
silently disappearing. Status changes use current source/tests, not module counts.

| Previous section/claim | Stable finding / disposition | Current evidence or remaining limit |
| --- | --- | --- |
| §4 fixed-lifetime selector cycle | CBI-FIXED-LIFETIME-OWNER — remains resolved | One selector remains in `road_lifetimes_survival`; new deferred graph edges do not undo that relocation. |
| §4 repeated text replacement | CBI-ATOMIC-TEXT — remains resolved | Unique sibling-temp cleanup remains in `utils.files`; newline/write/replace failure tests pass. |
| §4 missing route consumers | CBI-ROUTE-CONSUMERS — remains resolved for those edges | Capacity/charger consumers remain declared; current full executable-route check passes after recognizing the human legacy producer explicitly. |
| §5 mixed capacity/disk fingerprint | CBI-ARTIFACT-IDENTITY — still open | Same charger file-mediated dependency verified. |
| §5 shared preparation/parallel scenarios | CBI-ARTIFACT-ISOLATION — still open | Shared writes remain; serial convention retained. No concurrent failure experiment run. |
| §5 repeated fleet/evidence I/O suggestion | CBI-PERFORMANCE-EVIDENCE — partly superseded | Plans 131/132 remove measured repetition; remaining repeated reads need current cost evidence. |
| §6 long family functions | CBI-ASSEMBLY-COHESION — still open, conditional | Calculation/publication seams still mix; no decomposition justified by LOC alone. |
| §5 utilization/general numerical/bus lifetime owners | CBI-HELPER-OWNERSHIP — still open | Cross-domain imports remain; move one proven contract only when useful. |
| §5 accepted/review estimators and mapping reporting | CBI-REVIEW-BOUNDARY — boundary retained; optional extraction not selected | Separate accepted/diagnostic publishers and explicit modes remain. No independent maintenance need established for a split. |
| §5 assorted source-family extraction | CBI-REVIEW-BOUNDARY — not rechecked in depth | Source families retain distinct registry identities; no extraction proposed merely for size. |
| §5 legacy versus same-schema comparators | CBI-COMPARATOR-BOUNDARY — remains resolved | Separate keys/semantics; shared read-only opening/quoting. Preserve both owners. |
| §5 native prerequisite / older workflow suggestions | CBI-NATIVE-LIFECYCLE — superseded | Current ordered replay and opt-in capture source confirmed; publication/cancellation fixture checks pass. Full replay not rerun. |
| §6 CANOE-main adapter | CBI-INTEGRATION — still open, reassessed | Residential/electricity coverage added upstream; integration blockers are now commit-pinned. |
| §1 no static cycles / 417 collected tests | Metrics superseded | Current eager graph has no cycle; guarded/deferred graph has four SCCs. Current collection is 1,873 cases; counts do not establish correctness. |

## Verification, preservation and next decision

Current checks are recorded in [ExecPlan 137][plan]. The 111-case scenario/evidence/config
suite passes; the 57-case schema/provenance/support/publication/caller suite passes with
two opt-in native cases skipped. The diagnostic regression group passed 364 cases and
initially failed the artifact-route check's attempt to import human producer `researcher`;
after the bounded test correction all three architecture-route checks pass. This is
**533 distinct passed cases, two skips**. Full collection succeeds for 1,873 cases in
86 test files; the entire suite was not executed.

Two initial verification failures were stale test expectations, not parameter defects:
the source-ID exact-set assertion omitted registered T40, and the route check treated a
human legacy producer as executable code. Explicit current assertions now retain both
contracts. The broader real-input bootstrap path was not run merely to exercise its
matching source-ID literal. The current captured transportation-validation report loads
read-only under its original `researcher_sales_2025` identity without a retired YAML.
All four surviving scenario YAMLs and all 11 renamed evidence files remain byte-identical
to task start. No accepted reference, modeling selection, tolerance, source cache,
registered external model or final database/report was regenerated.

The initial template-only caller checks unexpectedly published five shared range
products in `none` mode. Four initially clean tracked files were restored exactly from
the index; the ignored inactive conservation marker was restored from the owning
publisher's complete deterministic format, native newline handling and the restored
`new_capacity_shares` mode. No pre-run checksums exist for these generated products;
their original byte equality, including newline style, is unverified. Git-normalized
tracked contents and the marker's semantic state are restored.
The caller fixture now has a private config/artifact root; both caller checks pass with
before/after checksums covering the restored products and protected task-start files.

Ruff, documentation metadata/links, commit-pinned upstream references and active
scenario/evidence resolution are checked in the final task record. Existing upstream
`Traversable` deprecation warnings are retained. No fresh legacy parity, solver
feasibility, scientific justification, full replay or combined-sector success is claimed.

The next useful implementation decision is one prepared-capacity identity/publication
slice or a narrowly agreed upstream fixture. A new speed target, persistent output cache,
parallel scenario runner, broad module split or generic entity conversion is deferred:
current evidence does not justify its scope or regression risk.

[plan]: ../.agents/plans/archive/137_diagnostic_integration_and_scenario_refresh.md
[old-plan]: ../.agents/plans/archive/060_industry_orchestrator_and_structural_refresh.md
[p131]: ../.agents/plans/archive/131_moves6_build_performance_and_bridge_diagnostics.md
[p132]: ../.agents/plans/archive/132_deeper_native_build_performance.md
[control-log]: ../outputs/logs/baseline_default_2026-11-04_18-41-13-934903Z.log
[optimized-log]: ../outputs/logs/baseline_default_2026-11-04_19-57-00-318974Z.log
