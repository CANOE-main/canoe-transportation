# Diagnostic snapshot format

Use only when writing an explicitly requested codebase diagnostic snapshot. Maintain
`docs/codebase_diagnostic_snapshot.md` as point-in-time engineering evidence, with its
existing retrieval frontmatter. It is not a new policy or acceptance authority. The
format below supports comparison without requiring an exhaustive audit on every run.

## 1. Identity and coverage

Record date, code revision, relevant dirty-worktree changes, scope and exclusions.
Identify what was inspected/executed now, historical evidence reused (with dates and
revisions), evidence freshness, and unexamined boundaries. Identify previous snapshot/
task records used for reconciliation. Do not present old metrics as current.

## 2. Methods and workloads

Give reproducible commands, environment/dependency identities and artifact/log/report
links, with scenario, geography/history, input/configuration identities and volumes.
For measurements include wall/CPU/resource observations as relevant, replay/cache state,
instrumentation, repetitions/variability and competing processes. Label component versus
native-build measurements and compare only equivalent work. Explain comparator/reference
authority, normalization, exclusions and limitations. State structural-metric definitions
and exclusions; they are inspection signals, not quality scores or runtime evidence.

## 3. Current findings and reconciliation

Use stable finding IDs across iterations. For older findings without IDs, assign one
with an explicit old-heading/claim crosswalk. Do not silently drop unresolved entries;
retain a compact reconciliation row even when they are outside the current scope.

| ID / status | Owner / interface | Consequence and evidence | Priority / uncertainty | Smallest next action / validation |
| --- | --- | --- | --- | --- |
| Persistent ID; new, still open, resolved, regressed, superseded or not rechecked | Affected code, config/file edges and consumers | Concrete impact; current evidence or dated historical claim, linked precisely | Impact, evidence strength, implementation risk and dependencies; label hypotheses | Bounded action; acceptance reference/invariants and checks needed to prove benefit and preservation |

Require evidence for each status change. Resolved needs current verification against
the finding's contract; superseded needs a reason and replacement link. Not rechecked
is not resolved. Separate preservation evidence from scientific/legacy validation and
integration readiness. For each relevant check report passed, failed, skipped or
unexamined; state scope limits even when tests pass.

Review runtime/resource costs, responsibility boundaries, duplication, unnecessary
state/abstractions, configuration/artifact coupling, test adequacy, failure handling and
auditability. Trace file/configuration dependencies beyond imports. Preserve useful
domain distinctions; module size alone does not justify decomposition.

## 4. Changes and bounded next improvements

Summarize meaningful changes since the previous evaluation and prioritize a small set
of next actions by demonstrated impact, evidence, risk and dependencies. Compare expected
and observed benefits of completed experiments alongside preservation results. Retain
concise lessons from rejected/ineffective experiments where they prevent repeated work;
link existing ExecPlans/history for details. Keep outstanding findings visible without
accumulating duplicate reports or copying a whole module inventory on every iteration.

Backend build-performance evidence belongs here; transportation validation comparisons
remain with their own document. A snapshot records evidence and recommendations, not
permission for production changes or acceptance of assumptions and parity gaps.
