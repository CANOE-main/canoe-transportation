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
last_bounded_implementation: 2026-11-16
implementation_base_commit: 1a1001a4c54ee8e1b40e5bb657c9d8d309a02835
---

# Codebase diagnostic snapshot

The native backend retains one prerequisite/preparation/insertion lifecycle with explicit
source, full DQ and publication checks. The bounded November 5 implementation corrects
charger prepared-input identity and guards the capacity-to-efficiency evidence handoff.
Broader shared artifact isolation and a real CANOE-main integration fixture remain open.
Historical CPU improvements are substantial; this work introduces no new caching framework.

The pre-177 Plan 176 production pass on November 16 added fresh-acquisition and clean-build evidence,
equivalent Dunsky input extraction, stronger cache publication and update review,
an installed-distribution/caller-root check, and a current upstream assessment.
It is a bounded verification of those interfaces before the Plan 177 parameter changes.
The broad inventory and graph measurements below still describe November 4; no new
repository-wide inventory, graph, performance benchmark or clean broad-suite result is claimed.

Plan 177 adds a shared production charger core and changes infrastructure parameters.
Its bounded clean aggregate/explicit native checks and serial canonical evidence
refresh passed; the dated Plan 176 comparisons remain pre-change evidence.

Plan 178 separates parameter contracts into six declared domains and a small routing
index. All 36 owner payloads remain exact. Its new loader, full-file-set identities,
captured-root validation and independent native comparison are scoped below; this
does not update the historical repository-wide inventory or performance claims.

## Scope, methods and freshness

The inventory and broad diagnostic scope below describe November 4. November 5 is a
bounded implementation at the three selected findings, documented in [Plan 142][p142];
it does not claim a new repository-wide audit or scientific validation.

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
The November 4 production graph excludes `scripts/experiments`: 74 modules, 276 syntactic edges
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
`5dc03a1b3dfd0e4bd6bd21b2d6b749040a39eb503b76f402f28a8c584b7ad051`, identifying
those historical runs. It does not identify the current default scenario or establish
equality of current inputs.

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

## Findings with dated bounded updates

| Stable ID / status | Owner/interface and consequence | Evidence / uncertainty | Priority and smallest useful next action |
| --- | --- | --- | --- |
| **CBI-ARTIFACT-IDENTITY — resolved in the selected interface** | `ev_chargers._prepared_stock_identity` fingerprints the ordered consumed BEV row models and full matching contexts. It excludes unused rows/contexts and never reads the incidental capacity CSV. | The versioned `prepared_stock` audit records the digest, row count and contributing IDs. Missing/conflicting lineage, duplicate consumed keys and nonfinite stock fail explicitly. Corrected identity deliberately propagates through the existing common charger/H2 digest; it is not an identifier-preserving refactor. | Retain exact preparation/native regression and missing/different-disk, changed-input, repeated-input and regional-scope tests. Numerical and provenance preservation evidence is recorded in [Plan 142][p142]. |
| **CBI-ARTIFACT-ISOLATION — partially resolved** | Capacity invalidates `integrity.json` before preparation, then atomically publishes it last with exact row/context/scenario identity and all direct family artifact hashes. Efficiencies validates the completion handoff before stock consumption and after calculation. | Missing, replaced, corrupt or differently scoped completion evidence fails closed. This prevents the selected production consumer from accepting partial family publication; individual CSVs can still remain after failure. | Retain serial native execution and final SQLite publication safeguards. Other families, raw-file diagnostic consumers and cross-file/cross-process crash atomicity remain outside this slice; extend only at a demonstrated next consumer boundary. |
| **CBI-INTEGRATION — still open, updated November 16** | Caller-owned insertion exists; no upstream-initialized multi-sector fixture/adapter establishes shared rows, geography, notes, supply/emission or reduced-time compatibility. | The [upstream assessment](canoe_main_orchestrator.md) now pins unstable `be2235d6`. Its transportation ghost reserves supply endpoints, but is not an end-use adapter or post-supply insertion hook. Duplicate sector identities are rejected. | Agree ownership and compatibility, then exercise one upstream fixture before representative reduction or solving. Preserve the existing parameterization/insertion owners. |
| **CBI-PERFORMANCE-EVIDENCE — partly superseded** | Old repeated-computation advice is partly implemented. Composition/replay/CSV remain historical cost centers; repeated fleet evidence reads alone do not demonstrate current material cost. | The bounded implementation adds comparable native stage/total timings below, not a new hotspot, peak-memory or I/O profile. Existing local reuse remains unchanged. | Profile an equivalent full-history workload before the next optimization. Keep source replay/validation; assess kernels and full-build benefit separately before adding state. |
| **CBI-STATCAN-ORACLE — resolved for the selected stream boundary November 16** | Stream expectations now enumerate retained source rows without invoking `_select_chunk`. A separate ON/QC fixture covers bilateral duplication, origin/destination/both matches, null geography, exclusions and chunk order. | [Stream tests](../tests/test_statcan_tables.py) assert exact native fields, dtypes, nullability, ZIP member and input count. Production selection is unchanged. | Retain these independent expectations when changing selectors or streaming; this does not independently validate every source normalization or scientific aggregation. |
| **CBI-ASSEMBLY-COHESION / CBI-HELPER-OWNERSHIP — still open, conditional** | Large family assemblers combine calculation, provenance and publication; cross-domain helpers remain. | `aggregate_consumption_period` and `combine_range_observations` serve both efficiencies and LDV diagnostics and have independent tests. Bus lifetimes still have an off-road owner. No numerical/import/runtime defect is established. | When weighting next changes, consider moving those two pure kernels together to the existing road-efficiency owner without forwarding duplicates. Preserve consumption-before-reciprocal and observed-support rules. Do not decompose based on size or relocate bus logic absent a source-contract change. |
| **CBI-TEST-ISOLATION / CBI-ROUTE-CONSUMERS — resolved for exercised boundaries; wider audit open** | The shared `isolated_legacy_bundle` fixture serves bootstrap, charger preparation and real efficiency tests. It copies registered raw/control inputs and replays prerequisites in a private repository; all configured artifact/input/output routes are checked. | Private template-only caller checks and prior route repairs remain. Actual capacity late-write interruption/recovery tests use this fixture; synthetic efficiency handoffs also redirect the completion-audit route. Plan 142 recorded 15 passing bootstrap cases. | Unexercised full bootstrap cases in `test_existing_capacity_bus.py` and the real-offline range-mode test still need complete root isolation before execution. Do not infer whole-suite safety from this bounded repair. |

## Reconciliation with the previous snapshot

Older findings lacked stable IDs; the crosswalk below prevents unresolved items from
silently disappearing. Status changes use current source/tests, not module counts.

| Previous section/claim | Stable finding / disposition | Current evidence or remaining limit |
| --- | --- | --- |
| §4 fixed-lifetime selector cycle | CBI-FIXED-LIFETIME-OWNER — remains resolved | One selector remains in `road_lifetimes_survival`; new deferred graph edges do not undo that relocation. |
| §4 repeated text replacement | CBI-ATOMIC-TEXT — remains resolved | Unique sibling-temp cleanup remains in `utils.files`; newline/write/replace failure tests pass. |
| §4 missing route consumers | CBI-ROUTE-CONSUMERS — remains resolved for those edges | Capacity/charger consumers remain declared; current full executable-route check passes after recognizing the human legacy producer explicitly. |
| §5 mixed capacity/disk fingerprint | CBI-ARTIFACT-IDENTITY — resolved for chargers | Supplied consumed rows/full contexts now own identity; explicit downstream identifier correction evaluated separately. |
| §5 shared preparation/parallel scenarios | CBI-ARTIFACT-ISOLATION — partially resolved | Capacity completion guards the efficiency handoff. Broader shared writes and serial convention remain; no concurrency guarantee introduced. |
| §5 repeated fleet/evidence I/O suggestion | CBI-PERFORMANCE-EVIDENCE — partly superseded | Plans 131/132 remove measured repetition; remaining repeated reads need current cost evidence. |
| §6 long family functions | CBI-ASSEMBLY-COHESION — still open, conditional | Calculation/publication seams still mix; no decomposition justified by LOC alone. |
| §5 utilization/general numerical/bus lifetime owners | CBI-HELPER-OWNERSHIP — still open | Cross-domain imports remain; move one proven contract only when useful. |
| §5 accepted/review estimators and mapping reporting | CBI-REVIEW-BOUNDARY — boundary retained; optional extraction not selected | Separate accepted/diagnostic publishers and explicit modes remain. No independent maintenance need established for a split. |
| §5 assorted source-family extraction | CBI-REVIEW-BOUNDARY — not rechecked in depth | Source families retain distinct registry identities; no extraction proposed merely for size. |
| §5 legacy versus same-schema comparators | CBI-COMPARATOR-BOUNDARY — remains resolved | Separate keys/semantics; shared read-only opening/quoting. Preserve both owners. |
| §5 native prerequisite / older workflow suggestions | CBI-NATIVE-LIFECYCLE — superseded | Current ordered replay and opt-in capture source confirmed; publication/cancellation fixture checks pass. Full replay not rerun. |
| §6 CANOE-main adapter | CBI-INTEGRATION — still open, reassessed | Residential/electricity coverage added upstream; integration blockers are now commit-pinned. |
| §1 no static cycles / 417 collected tests | Metrics superseded | The November 4 eager graph had no cycle; its guarded/deferred graph had four SCCs and collection had 1,873 cases. These are historical metrics, not current correctness evidence. |

## Verification, preservation and next decision

### Plan 178 — Parameter-contract routing and identity

The [parameter instructions](../config/parameters/AGENTS.md) describe six cohesive domains
under a 53-line version-3 `rules.yaml` index: source normalization, fleet stock/activity,
efficiency/emissions, costs, charging/ranges and opt-in diagnostics. Eight specialized
YAML contracts retain their distinct extraction, conversion or review schemas. The
directory has fifteen YAML files including the index; there is no file-per-function
policy, recursive discovery or cross-file override mechanism.

Independent comparison against frozen HEAD `1a1001a` preserves every one of the 36
owner subtrees, including scalar/key types, nested mapping order and list order.
Callers retain `load_harmonization_rules(bundle, owner)`; selected reads parse only
their member, while full configuration validation checks the complete declared set.
Native guards/reports, cost/adoption provenance, source-review adoption and validation
readers now fingerprint every member. Captures verify closed membership and bind
their recorded parameter directory before scenario validation. Explicit historical
v2 captures still load under current v3 live routing; no live substitution is used.

Fresh isolated Ontario pre/post native builds begin without derived inputs and deny
network sockets. Both pass integrity/FK checks and export 31 parameter tables. Raw
91-table comparison records the expected physical-identity differences. A proved
bijection of all 230 dataset roles preserves complete relational content, all source
citations, all five DQ fields, model keys, units, row notes and numerics exactly.
Only 206 final content-identity digests change; hash-based allocation permutes 38
dataset surrogate IDs. No tolerance or scientific/DQ/numeric normalization hides
those changes. This is regression evidence, not independent scientific validation.

Focused consumer, source-update, capture, configuration, wheel and replay checks pass;
their individual counts and prior fixture failures are recorded in [Plan 178][p178].
GREET generation identity remains exact and validate-only reads 966 bank gas rows
without writes. The strict documentation/link build passes 20 HTML pages. There is
no final full-suite, arbitrary external diagnostic-control, macOS or full-solver claim.

A fresh private native notebook build also passes integrity/FK checks and 15 support
checks, publishing 24,215 rows in 23 tables. Both captured profiles authenticate all
seven rule files; actual stock and efficiency readers complete with sockets and
writes denied. The explicitly matched Ontario-2025 cohort reader passes with 260
annual rows and ten calibration groups. Its all-region default was not prepared by
these Ontario-only builds; no broader regional diagnostic coverage is inferred.

### Current Plan 177 changes — scoped native checks passed

[Plan 177][p177] implements the user-selected charger method. The new shared
[`parameterization.charger_representation`](../src/parameterization/charger_representation.py)
owns source-to-archetype calculation; production assembly remains in `ev_chargers`
and the native builder, while diagnostics consume the same calculations. Evolving
Dunsky Table 6/38 composition and cost-matched Table 12 DCFC ratings serve the
aggregate path and five explicit archetypes. Explicit available-GW constraints,
selected template/lifetime ownership and full provenance use the existing insertion seam.

The [infrastructure chapter](model/infrastructure.md) and
[assumptions](assumptions.md#charging-and-hydrogen-refueling-infrastructure) own the
physical choices. Home utilization 0.08 is a scenario proxy linked to the new `T41`
source, whose DQ annotations await review; iZEV annotations are now user-reviewed.
Explicit RAMP profiles remain pending, so explicit scenarios require
`travel_behavior_source: none`. The 26 historical validation outputs were untracked
with local bytes preserved, and two unused replay routes were removed.

Independent aggregate and explicit Ontario builds started with no derived inputs
and denied network sockets. Both passed support, foreign-key, integrity, provenance
and processed-export checks. A fresh-process final explicit build passed after the
parent-group identity correction, including eight groups, twelve members, six
shares per period, no hourly capacity factors and home-source lineage across all
five model periods. The serial canonical baseline, calibration, regional and notebook
builds and five report/support commands then completed. Source/default/path controls
and all 2,256 cache files retained their hashes; four pre-existing user-modified
range outputs were restored after immutable captures.

A sorted-SQL comparison across 88 tables against the matched Plan 176 Ontario
reference found no noncharger numerical/structural differences after excluding the
three provenance registry tables and identifier, source, notes and DQ fields.
User-reviewed DQ changes are intentional; charger numerical differences are confined
to investment and fixed costs. This is bounded current regression evidence; the
Plan 176 matrix and counts below do not certify other changed scenario selections.

The opt-in annual screen passes all five explicit Ontario periods. It tests a
necessary annual relaxation and exposes uptake/throughput coupling; hourly dispatch,
upstream supply and cross-period investment persistence remain outside its scope.
Focused module checks, actual notebook execution and dense/sparse/empty rendering
are recorded in [Plan 177][p177]. There is no full current test-suite result,
upstream-initialized multi-sector fixture, macOS execution or full Temoa solve claim.

The publication rehearsal exposed and repaired fleet/cohort dependence on the last
scenario's shared mapped year, an unused LDV dependency in truck-battery evidence,
and stale comparison-notebook cost columns. Captured complete history and reviewed
mapping reproduce all five selected-2025 pooled NLR weights exactly; truck-only
reads omit LDV while retaining consumed-input checks. Final production/configuration/
insertion/DQ checks pass 172 cases. The installed-wheel check verifies all 90 Python
payload files and isolated imports without eagerly requiring diagnostic SciPy.
All 39 publication figures export successfully; the strict site/link check covers
20 HTML pages. Local charging/infrastructure/derivation review passes, including
12 diagrams and the requested equal displayed hourly-figure widths, with no browser
errors or external requests. Cleanup removes 4.84 GB of disposable native-test
copies after preserving and rehashing three compact native audit captures. Their
relocated explicit capture still passes all five annual screens. No manual cleanup
block remains from the previously protected replay/reference directories.

### Pre-177 production verification — Plan 176, November 16

[ExecPlan 176][p176] records the following pre-177 commands, configurations, counts
and comparisons. They do not establish current Plan 177 acceptance. The reference
was frozen local HEAD `1689c87e`, not an accepted legacy/scientific reference. Both
reference and then-post-change default builds began with
registered raw inputs and no interim, processed or validation products, with socket
access denied. Each build owns its entire mutable namespace.

- The first post-change ten-region default build passes integrity, foreign keys,
  provenance and all 17 cross-table support checks, and exports 30 model-ready tables
  with 385,535 rows. At zero absolute/relative tolerance all 88 non-registry tables
  match the frozen reference. An independent sorted-SQL comparison also includes DQ
  fields and matches all 88 tables, excluding only `data_id`, `data_source`, `notes`
  and the three provenance registry tables. Changed source lineage is intentional.
- In Plan 176, Dunsky extraction replaced 28 manual rows while preserving every one of the original
  48 combined keys, values, units, periods, notes and citations. Twenty manual rows
  remained, including nine Dunsky overrides. Those counts and fixed-share rules are
  historical input-preservation evidence; Plan 177 intentionally changes this method.
  MHDV fixed shares use the mean of annual component-count shares rounded to two
  decimals; this preserves `.82/.16/.02`. The old notes' phrase “average total counts”
  did not specify an executable algorithm; a ratio of summed counts would differ.
- Fresh downloads exercise all 19 upstream-download families, separately from the ten
  reviewed manual sources and four registered/external-model families. The full
  empty-cache native run completes acquisition through atomic SQLite publication:
  210 fresh files / 537,384,857 bytes, 385,512 inserted transport rows across 27
  tables, all 17 support checks, integrity OK and zero foreign-key violations.
  Its 88 model tables also match the frozen reference at zero tolerance, with DQ
  included in the independent comparison and the same declared provenance exclusions.
  All seven presets and six additional method configurations also complete native
  rebuilds, including all six NHTS/TTS profiles, Advanced/Conservative trajectories,
  fixed NHTSA/MOVES and survival, calibration, range and lightweight selections.
- CEUD downloads stage privately, validate workbook readability and publish atomically.
  Source-owned bounded retries cover connection, header-read and selected transient
  HTTP failures. Permanent HTTP errors, invalid workbooks and dropped post-header
  bodies fail without publishing corrupt cache bytes. vPIC likewise validates temporal
  response shape before publication. These repair demonstrated failures, not inferred
  performance bottlenecks.
- FuelEconomy's rolling publisher no longer served the old pinned bytes. A narrow,
  exact report-bound update preserves all consumed evidence through 2025 and all 289
  selected range rows; newly available 2026 evidence resolves one previously excluded
  Pacifica AWD observation. Update review now fingerprints the actual nine propulsion
  fields consumed by production in addition to class and range evidence. A shared
  physical reader preserves prior native ordering, nulls, duplicates and types.
- The extraction YAML contributes to charger provenance identity, the existing input
  audit, native parameter fingerprints/mutation checks, and immutable notebook captures.
  An equal-valued selector change must change the dataset identity. FAA dimensional
  tests derive evidence from raw PDFs in memory instead of pre-existing interim CSVs.
- Real Windows Excel/xlwings recalculation passes all 23 GREET cases and exactly matches
  all 966 gas rows and 322 exclusions in the bank; source workbooks remain unchanged.
  Portable bank replay is separate from Windows COM generation. Current macOS/Linux
  runtime verification remains outstanding: their prior CI jobs installed dependencies
  but collected no tests because of a now-corrected stale workflow test path.
- Installed packaging omitted `build_transport`; explicit package/module discovery
  repairs the wheel without dropping existing modules. Configuration bundles now
  preserve caller roots and alternative controls through prerequisite replay and cost
  fingerprints. A clean runtime-only installed wheel runs from a foreign directory
  without repository imports, precomputed tables or sockets: an Ontario/one-model-period
  caller prepares/inserts 2,335 rows across 17 tables, passes all 17 support checks and
  full integrity/provenance/FK validation, then rolls back in its own transaction.
- Real Git checkout tests found LF stored RAMP blobs versus CRLF registered hashes.
  Narrow attributes reproduce all 17 selected registered pins from actual index/HEAD
  blobs under all three `core.autocrlf` modes. Original source bytes remain unchanged.
- VKT age mode could suppress ordinary annual factors without inserting its age rows.
  Native build, caller preparation, insertion and export now reject that unsupported
  representation. No padding, lifetime truncation or incomplete-model success remains.
  ATB Conservative adopts only the three source-documented native Constant vocational
  exceptions, preserving native/effective labels and all rows; unsupported global
  Constant is rejected before acquisition. Strict inverse authentication lets unchanged
  GREET banks replay the new ATB lineage columns only when the original SHA is recovered.
- The frozen broad run finishes at 2,460 passed, 21 skipped, 53 failed and 39 errors
  (2,573 cases). Targeted repairs make tests independent of old generated products and
  explicit about synthetic fixed/physical stock, selected years and optional diagnostics.
  All 92 failed/error nodes are reconciled: 91 pass targeted pytest reruns, including
  a fresh real-input bootstrap; optional OMEGA original-parent identity is skipped
  when absent, with mandatory compact replay passing. Final focused configuration/
  lifecycle/support checks passed 159 cases with 15 intentional opt-in skips; the
  Plan 176 collection was 2,604. No final-tree clean broad-run claim is made.
  Its then-pending iZEV annotations were subsequently user-reviewed in Plan 177.

The pre-177 Plan 176 matched default timings were 1,528.64 s reference and 1,542.75 s post-change.
Other isolated workers were active; these runs establish completion and regression
preservation, not a controlled performance benchmark or optimization benefit. Full
scenario/method matrix, final cold publication and full-suite outcomes are recorded
in the plan. No solver or combined-sector compile is inferred from these checks.

### Bounded implementation evidence, November 5

[Plan 142][p142] compares frozen pre-change code at `5b81a1f` with the completed
implementation using the unchanged default scenario, ten regions, prospective base 2025,
fixed lifetimes, charging and future range shares. Both normal native entrypoints replayed
registered prerequisites offline with sockets denied in the same disposable repository.
All 2,276 raw input files match; no accepted reference or real publication was replaced.
This establishes regression equivalence for this workload, not independent scientific
validity, legacy parity, solver feasibility or CANOE-main readiness.

- All **91 SQLite table schemas** match. **90 complete tables are strictly exact**,
  including all parameter values, keys, coverage, units, readable IDs, source links and
  DQ. The sole table difference is **27 `data_set.notes.content_identity` values**.
  An explicit label-derived bijection makes those notes exact without changing any
  other notes byte. The prepared identities cover 18 regional charger-capacity contexts,
  six LDV/MHDV cost/efficiency contexts and three H2 contexts. Existing readable-ID
  publication preserves the final database keys.
- Captured efficiency preparation is strictly exact. Capacity rows/contexts are exact;
  its audit gains `prepared_handoff`. Charger/full contribution objects match after
  only that identity mapping and the declared audit changes. All **21 actual pre-CSV
  frames / 19,416 rows** match at zero tolerance, including dtypes, nulls, indexes,
  ordering and attrs. Among captured files, 171 are byte-identical; four charger CSVs
  differ only in prepared IDs. Two family audits add the handoff/stock identity and
  remove the unused capacity-CSV fingerprint. The build report additionally records
  the changed database/config hashes, timestamp and log path; a new run log is expected.
- Both builds insert **222,135 transport rows**, pass all **14 support checks** and
  retain integrity, foreign-key and provenance validation. Baseline/final native elapsed
  time is **385.95 / 335.44 s**, CPU **375.25 / 328.22 s**; capacity **22.65 / 18.74 s**,
  chargers **3.00 / 2.66 s**, insertion **10.42 / 10.94 s**. No material runtime regression
  is observed. This is one instrumented pair with fresh processes, identical work and
  no competing agent builds; OS-cache/load variability remains, so no speedup or peak
  memory claim is made.
- The actual full-default contribution also inserts into a fresh local caller-owned
  v4 database with existing region/time axes and an unrelated dataset sentinel. Both
  insertions pass validation and retain the caller's active transaction; rollback
  restores every original table, and subsequent commit matches all 91 standalone
  tables except the sentinel. The contribution remains unchanged. This is a local
  seam check, not the deferred upstream-initialized multi-sector fixture.
- **301 distinct focused tests pass, two opt-in native cases skip**: 279 across charger,
  efficiency, handoff, lifecycle, configuration, schema/provenance/support and artifact
  route checks; seven readable-ID checks; 15 private bootstrap cases. The latter exercise
  real second-CSV-write failures under `RuntimeError`, `KeyboardInterrupt` and `SystemExit`,
  rejected incomplete handoffs and exact recovery. Ruff and whitespace/reference checks
  pass. The existing dependency `Traversable` deprecation remains; no full-suite claim.
- Final protected inventory covers 3,095 files: 3,052 readable byte hashes and 43
  pre-existing unreadable files checked only by size/mtime. The only change is authorized
  `config/paths.yaml` ownership/consumer metadata. Byte preservation of those 43 files
  cannot be independently asserted. All task-local native/caller/comparison captures and
  the three bulk private test roots were removed after recording the evidence.

The tests below also exercise legacy-period selections and caller-owned transactions.
The shared private-input fixture is limited to affected real preparations; pure unit
tests retain their lightweight fixtures. Unexercised bootstrap/range-mode paths remain
listed under CBI-TEST-ISOLATION. The guarded family is not a multi-file transaction:
failed runs can leave partial CSVs, which the efficiency consumer now refuses until
successful re-preparation. Other raw-file consumers and broader shared publication are
not claimed safe for concurrency.

### Previous diagnostic verification

The November 4 diagnostic checks are recorded in [ExecPlan 137][plan]. The 111-case scenario/evidence/config
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

The independent StatCan streaming oracle was completed in the November 16 slice above.
A later implementation can guard another demonstrated shared-artifact consumer;
upstream integration requires its own agreed fixture. A new speed target, persistent
cache, parallel scenario runner or broad module split remains unjustified by this work.

[plan]: ../.agents/plans/archive/137_diagnostic_integration_and_scenario_refresh.md
[p142]: ../.agents/plans/archive/142_prepared_capacity_identity_and_publication.md
[p176]: ../.agents/plans/176_production_integration_readiness.md
[p177]: ../.agents/plans/177_charger_production_and_reproducible_evidence.md
[p178]: ../.agents/plans/178_parameter_contract_partitions.md
[old-plan]: ../.agents/plans/archive/060_industry_orchestrator_and_structural_refresh.md
[p131]: ../.agents/plans/archive/131_moves6_build_performance_and_bridge_diagnostics.md
[p132]: ../.agents/plans/archive/132_deeper_native_build_performance.md
[control-log]: ../outputs/logs/baseline_default_2026-11-04_18-41-13-934903Z.log
[optimized-log]: ../outputs/logs/baseline_default_2026-11-04_19-57-00-318974Z.log
