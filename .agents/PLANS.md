# PLANS.md

ExecPlans are short-lived implementation records for bounded changes. They let a coding
agent or contributor research, execute, and verify a task without relying on conversation
history. They constrain scope and acceptance criteria; they do not replace durable policy
in `AGENTS.md` or freeze an initial design after repository evidence contradicts it.

## When an ExecPlan is required

Create or update a plan when a task:

- changes multiple files or responsibilities;
- affects configuration, validation, orchestration, database artifacts, or parity;
- introduces a source, scenario, parameter slice, or interface;
- has staged, risky, ambiguous, or explicitly requested work;
- needs progress, decisions, deviations, or validation evidence preserved.

Skip a plan for a small isolated edit unless the user requests one.

Store plans under `.agents/plans/` using the next numbered descriptive name, for example
`NNN_descriptive_task.md`. Completed plans are audit trails, not permanent
architecture references.

Before creating a numbered plan, search existing plans for the same source,
parameter family, or diagnostic subject. An adjacent follow-up should reopen and
update the latest plan that owns that subject, marking older findings or decisions
as superseded where necessary. Create another plan only when the work establishes
a genuinely distinct responsibility or interface, or when the user explicitly asks
for a separate plan.

## Research and task boundary

Before editing:

1. Read `AGENTS.md`, the user-named files, and the current working-tree state.
2. Inspect the implementation and focused tests at the interface being changed.
3. Retrieve parameter, source, scenario, workflow, or legacy context only when the task
   crosses that seam; do not load whole directories for possible relevance.
4. Record concrete current findings, conflicts, and uncertainties.
5. Define the smallest complete slice and explicit non-goals.

Identify the applicable contracts:

- **Source contract:** identity, version, access or registration, physical structure,
  native units, availability checks, provenance, and offline behavior.
- **Interface contract:** caller inputs, invariants, ordering, errors, and compatibility.
- **Artifact contract:** path, format, schema, grain, keys, units, duplicate/null policy,
  and deterministic write behavior.
- **Output contract:** consumer, target table or report, acceptance tolerance, logs, and
  reconciliation or parity evidence.

Use only the contracts relevant to the task. Do not invent missing assumptions; mark an
unresolved choice and keep it out of implementation until its owner supplies evidence.

### ETL refinement

For ETL work, classify the changed slice as acquisition, physical validation,
normalization, parameter transformation, insertion, orchestration, or parity. Retrieve
only context that crosses that seam.

When applicable, the plan should pin down source-native structure and units; cache
identity and offline behavior; interim grain, keys, provenance, nulls, and duplicates;
and the final row model or target table. Validate from the first changed interface
inward: config/request, physical artifact, parser, transformation/conversion, output
contract, provenance/reconciliation, then SQLite integrity or parity.

ETL outcomes must record generated artifacts, logs or reports, warned or dropped
records, offline behavior, and unresolved source or parity issues.

## Required plan content

Keep plans concise and task-specific, normally with:

1. **Goal**
2. **Context** — inspected evidence and current findings
3. **Scope**
4. **Non-goals**
5. **Implementation steps**
6. **Validation**
7. **Acceptance criteria**
8. **Progress**
9. **Outcomes**

Add risks where failure could damage artifacts, alter behavior silently, require network
or external inputs, or make rollback difficult. Add a short Decision log only for
choices that affect source metadata, interfaces, module seams, assumptions, or later
work. Reference `AGENTS.md` instead of copying repository policy.

## Plan the verification

Choose the lightest tier that can exercise the changed contract:

- **Interface/static:** parse, import, schema, link, lint, or structural checks.
- **Focused behavior:** unit or fixture tests at the changed interface.
- **Artifact/integration:** deterministic offline smoke, affected DAG target, temporary
  database, or source-to-output reconciliation.
- **End-to-end/parity:** full build and comparison against accepted reference outputs.

Escalate only when the lower tier cannot observe the risk. A successful command is not
proof of correctness: define what output, invariant, row count, warning, reconciliation,
or tolerance makes the result acceptable. Plans must list exact commands, required
inputs, expected artifacts, and where logs or reports will be written.

## Execute and update

- Keep progress checkboxes current and record commands as they run.
- Preserve unrelated working-tree changes.
- Validate from the changed interface inward before running broader checks.
- Prefer deterministic offline evidence when the task does not require a source refresh.
- Record skipped, dropped, fallback, cleanup, and warning behavior relevant to the task.
- Stop scope growth at the stated non-goals; record larger inconsistencies as follow-up.

When code, tests, config, artifacts, or validation evidence contradict the plan, update
the plan rather than forcing implementation to satisfy an obsolete assumption. Record:

- what changed;
- why it changed;
- the evidence;
- the effect on scope or acceptance;
- any unresolved follow-up.

Durable truth belongs in the owning code, config, tests, validation reports, or focused
documentation. Do not turn an ExecPlan into a second architecture manual.

## Completion

Before marking a plan complete, update Progress, Outcomes, the commands and results,
generated artifacts, known placeholders, deviations, unresolved work, and any recommended
follow-up. Completion requires the acceptance criteria to be met or each exception to be
explicitly documented.
