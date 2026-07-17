# PLANS.md

This repository uses ExecPlans for bounded, multi-step changes. An ExecPlan is a short-lived implementation note that helps a coding agent or contributor execute a task without relying on conversation history.

ExecPlans are scaffolds, not permanent design law. They should constrain scope, risks, and acceptance criteria while allowing the implementation to evolve when repository evidence, tests, configs, or validation results justify a better path.

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
    002_stocks_and_demands_data_fetching.md
    003_ontario_vehicle_population_fetching.md

## Required sections

Keep each ExecPlan concise, usually 50–100 lines. Use these sections:
1. Goal
2. Context
3. Scope
4. Non-goals
5. Implementation steps
6. Validation
7. Acceptance criteria
8. Progress
9. Outcomes

Add a short “Decision log” only when architecture, source metadata, module boundaries, or modeling choices change.

## ExecPlan rules

- Inspect the repository before editing.
- Update the plan with concrete findings only when they affect the task.
- Keep the plan task-specific; do not repeat `AGENTS.md`.
- Prefer the smallest useful implementation slice.
- Preserve baseline reproduction as the first priority.
- Do not invent source data, formulas, table mappings, or modeling assumptions.
- Mark uncertain details as provisional, not permanent.
- Let proven code, tests, configs, and validation results refine earlier assumptions.
- Keep progress checkboxes current.
- Record commands run and whether they passed or failed.

## Evolvability rules

- Plans should constrain scope, not freeze architecture.
- Tests should constrain behavior.
- Configs should constrain assumptions.
- Validation should constrain trust.
- Legacy artifacts should constrain baseline parity.

When implementation evidence contradicts the original plan, do not force the code to satisfy an outdated assumption. Update the plan’s Progress, Outcomes, or Decision log with:
- what changed;
- why it changed;
- what evidence justified the change;
- whether follow-up work is needed.

**Important**: Durable project truth should move into code, tests, configs, validation reports, and concise documentation. Completed ExecPlans are an audit trail, not the source of truth.

## Validation rules

Every ExecPlan must define command-line checks.

Use relevant commands such as:
    uv run pytest
    uv run ruff check .
    uv run python src/setup.py --scenario config/scenarios/legacy_reproduction.yaml
    uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1

Early scaffold plans may validate only YAML loading, path creation, imports, and smoke tests.

Parity-sensitive plans must compare new outputs against legacy SQLite outputs or documented reference tables.

*Note: Use the lightest verification tier relevant to the task; do not require Snakemake or full SQLite parity for isolated module-level work.*

## Completion

Before marking an ExecPlan complete, update:
- Progress;
- Outcomes;
- commands run;
- passing/failing checks;
- generated outputs;
- known placeholders;
- deviations from the original plan, if any;
- recommended next plan, if any.