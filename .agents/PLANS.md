# PLANS.md

This repository uses ExecPlans for bounded, multi-step changes. An ExecPlan is a short living implementation note that helps a coding agent or contributor execute a task without relying on conversation history.

## When to use an ExecPlan

Create or update an ExecPlan when a task:

- changes multiple files or directories;
- affects Snakemake orchestration, configuration, validation, or SQLite outputs;
- introduces a source, parameter module, extraction map, or scenario;
- changes baseline reproduction behavior;
- is risky enough that progress, decisions, or validation need to be recorded.

Do not use an ExecPlan for small one-file edits, typo fixes, formatting-only changes, or isolated tests unless requested.

## Location and naming

Store plans in:

    .agents/plans/

Use numbered descriptive names:

    001_config_control_layer.md
    002_sqlite_schema_instantiation.md
    003_legacy_sqlite_comparison.md
    004_stocks_and_demands_baseline.md

## Required sections

Keep each ExecPlan concise. Use these sections:

1. Goal
2. Context
3. Scope
4. Non-goals
5. Implementation steps
6. Validation
7. Acceptance criteria
8. Progress
9. Outcomes

Add a short “Decision log” only when architecture or modeling choices change.

## ExecPlan rules

- Inspect the repository before editing.
- Update the plan with concrete findings only when they affect the task.
- Keep the plan task-specific; do not repeat `AGENTS.md`.
- Prefer the smallest useful implementation slice.
- Preserve baseline reproduction as the first priority.
- Do not invent source data, formulas, table mappings, or modeling assumptions.
- Mark uncertain details as placeholders.
- Keep progress checkboxes current.
- Record commands run and whether they passed or failed.

## Validation rules

Every ExecPlan must define command-line checks.

Use relevant commands such as:

    uv run pytest
    uv run ruff check .
    uv run python src/setup.py --scenario config/scenarios/baseline.yaml
    uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/baseline.yaml --cores 1

Early scaffold plans may validate only YAML loading, path creation, imports, and smoke tests.

Parity-sensitive plans must compare new outputs against legacy SQLite outputs or documented reference tables.

## Completion

Before marking an ExecPlan complete, update:

- Progress;
- Outcomes;
- commands run;
- passing/failing checks;
- generated outputs;
- known placeholders;
- recommended next plan, if any.