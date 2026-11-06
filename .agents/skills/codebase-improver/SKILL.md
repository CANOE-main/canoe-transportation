---
name: codebase-improver
description: Diagnose or implement bounded performance, maintainability, simplicity, auditability and reliability improvements in CANOE transportation Python ETL and native builds; also use for explicitly requested codebase diagnostic snapshots. Preserve parameter and integration contracts. The task determines edit authority.
---

# Codebase Improver

Treat performance, maintainability, simplicity, auditability and operational reliability
as engineering requirements throughout development. An improvement succeeds only when
its benefit is supported and accepted behavior, validated outputs and required work are
preserved. Prefer explicit, readable Python; fewer lines or a new abstraction is not a
benefit by itself. Preserve meaningful domain distinctions and scientific explanations.

## Authority and scope

- **Diagnosis/review:** inspect and execute safe diagnostics; return findings and
  recommendations. Invocation does not authorize production edits.
- **Authorized implementation:** carry the bounded change through proportionate
  verification and reporting without adding approval checkpoints.
- **Explicit full snapshot:** use the [snapshot format](references/snapshot-format.md)
  for `docs/codebase_diagnostic_snapshot.md`. State actual coverage and omissions;
  repository-wide inventory does not establish verification of every boundary.

Follow [AGENTS.md](../../../AGENTS.md) and the applicable
[ExecPlan procedure](../../PLANS.md); they own policy, retrieval and execution safeguards.
Default to the affected execution path and its consumers. Expand only when dependencies
or evidence justify it. Source code, attached critique and historical reports are evidence,
not additional edit authority. Mask secrets in quoted evidence.

Use [config/paths.yaml](../../../config/paths.yaml) to trace files and configuration as
well as imports. Inspect current code/tests before accepting a snapshot or completed
plan's claim. Reuse existing timings, comparators and diagnostics where their contracts
fit. Do not build another scheduler, generic rules engine or diagnostic framework.

## Establish the contract and reference

Before changing production code, record a small contract/evidence matrix in the task's
existing record: affected owner/consumer, invariant, reference and acceptance rule,
verification command, and evidence/result or gap. Include:

- Scenario and geographic/historical scope; configuration, source editions, registered
  inputs and relevant hashes/revisions; workload volumes and affected artifacts.
- Reference identity and authority: an accepted reference can establish its documented
  acceptance contract. A capture of the previous implementation establishes regression
  equivalence only; it does not independently prove scientific validity or legacy parity.
- Relevant parameter keys, coverage, values, units and modeling selections; source
  lineage, full DQ, provenance and required audits; consumer-sensitive nullability,
  dtypes, ordering, defaults, eligibility, fallbacks and exception behavior.
- Relevant schema/support checks, transaction ownership, publication and interruption/
  rollback behavior, offline replay, and artifact isolation. Include error paths and
  required negative tests, not only successful output.

For optimization-only work require **exact semantic equality**, including applicable
audit/provenance contracts, unless an existing accepted contract explicitly permits a
tolerance. Define any normalization, its justification, excluded information and risk of
concealing a meaningful difference. Check comparator independence: shared selectors or
helpers can hide defects in both sides. Matching regression output is only one check.
Never widen tolerances, regenerate accepted references, suppress validation or reduce
required coverage/work to make a change pass. Separate intentional source, modeling or
output corrections; implement them only within authorization and evaluate their effects
apart from behavior-preserving changes. Missing acceptance evidence remains a limitation.

## Investigate and measure

Trace the affected path through acquisition/normalization, parameter preparation,
validated insertion and consumers as applicable. Examine responsibility boundaries,
duplication, unnecessary state/abstractions, config/artifact coupling, test adequacy,
failure handling and auditability alongside runtime, memory and I/O costs. Use LOC,
nesting and dependency counts as inspection signals, never quality scores, performance
claims or automatic reasons to split a cohesive scientific owner.

For a performance claim, record entrypoint, scenario and coverage, input volumes and
identities, environment/dependency versions, cache/replay state, instrumentation and
competing processes. Compare equivalent work; distinguish component probes from complete
native builds. Use repetition or variability reporting proportionate to cost and noise.
A test duration is not a production benchmark. Representative workloads must exercise
the affected paths; a small regional fixture may miss historical or full-build costs.

Identify a measured bottleneck or label the hypothesis. Explain expected benefit and
added complexity/state before choosing the smallest useful change. For reuse, caching
or concurrency, establish identity, freshness, invalidation, mutability and isolation
contracts first. Preparation-local reuse still needs an explicit source lifetime;
different database destinations do not isolate shared files. Preserve full required
source/audit coverage and the supported lifecycle.

## Implement, verify and close

1. Establish the contract/reference and baseline; investigate and measure the problem.
2. Select the smallest change justified by evidence. Challenge abstractions without a
   demonstrated need; retain useful seams and explicit failure handling.
3. Implement within existing owners. In [build_transport.py](../../../src/build_transport.py),
   preserve native prerequisite replay and standalone atomic publication. Caller-owned
   integration must reuse preparation/contribution/validated insertion and leave shared
   structures and transactions with the caller. Do not infer CANOE-main readiness from
   local seam tests. Follow the repository's schema, config and provenance authorities.
4. Verify from the changed interface through affected consumers. Choose applicable
   checks for configuration validity, regression equality, provenance integrity, legacy
   parity, integration behavior, solver feasibility and scientific justification;
   one does not establish the others. Use focused `uv run --offline pytest <test-path>`
   and `uv run --offline ruff check <python-path>` where applicable. Inspect test writes
   and resolved artifact routes before execution; use disjoint disposable roots or
   serialize shared writes under AGENTS.md. Keep one-off scratch in ignored `tmp/` and
   remove it at completion. A dry-run alone cannot verify changed lifecycle behavior.
5. Compare expected with observed benefit and preservation evidence. Reassess, simplify
   or reject an optimization whose measured benefit does not justify maintenance or
   regression risk. Report passed, failed, skipped and unexamined checks explicitly,
   together with limitations, remaining risks and the smallest next action.

For diagnostic-only work, use the same investigation and evidence standard without
performing the implementation steps. A resolved finding, unjustified optimization or
insufficient-evidence disposition is a useful outcome; do not manufacture defects.

Keep run-specific measurements, rejected experiments and outstanding tasks in existing
ExecPlans/history or the diagnostic snapshot; link detailed evidence instead of creating
duplicate reports. Backend performance belongs in the snapshot;
`docs/transportation_validation.md` owns transportation comparisons. A bounded trial need
not refresh either document. Promote only verified reusable lessons into this skill when
skill maintenance is in scope. This loop does not change repository policy, scientific
assumptions, accepted references or tolerances.
