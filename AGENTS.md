# AGENTS.md

## Mission

This repository is the v2.0 refactor of the CANOE transportation backend. Replace the legacy Excel-centered compiler with a reproducible, auditable, Snakemake-orchestrated Python pipeline that builds Temoa/CANOE-ready SQLite databases for transport-sector modeling.

First priority: baseline reproduction. Using legacy-equivalent assumptions and source data, v2.0 should reproduce the old compiled SQLite outputs within documented tolerances. New scenarios, fuel-price futures, representation-choice experiments, GSA, and MGA workflows come only after parity gaps are explained or accepted.

## Modeling context

CANOE is a Canadian Open Energy Model built on Temoa. The transport backend represents road/off-road technologies, demands, stocks, efficiencies, costs, emissions, lifetimes, market constraints, fuel links, and EV charging profiles.

The backend must support controlled experiments on transport representation choices: technology progress, demand projections, fuel prices, utilization, BEV charging, adoption constraints, retirement formulations, emissions scopes, and coupling with electricity, hydrogen, liquid fuels, and broader CANOE supply chains.

## Core principles

1. Reproducibility: run from version-controlled code, YAML configs, cached sources, and registered external inputs.
2. Traceability: map every parameter to its source, transformation logic, units, assumptions, and target SQLite table.
3. Validation first: reproduce legacy SQLite outputs before changing assumptions or expanding scenarios.
4. Modularity: group code by parameter family and source relationship, not one giant compiler.
5. Scenario awareness: use Snakemake and YAML for regions, periods, sources, fuel prices, outputs, and modeling switches.
6. Robustness: log missing inputs, skipped outputs, fallbacks, warnings, unit conversions, and validation differences.
7. Practical Python: prefer readable, typed, testable functions over unnecessary abstractions.
8. Evolvability: treat planned architecture and ExecPlans as scaffolds; let proven code, tests, configs, and validation refine earlier assumptions.

## Planned architecture

This layout is the preferred direction, not a rigid template. Preserve it where it supports functionality, traceability, and validation. If implementation evidence suggests a better structure, adapt deliberately, document the reason, and keep references aligned.

```text
.
├── AGENTS.md                       # Stable repo instructions
├── .agents/                        # Optional agent planning guidance
│   ├── PLANS.md                    # ExecPlan protocol
│   ├── plans/                      # Task-specific plans
│   └── skills/                     # Small repo-local skills
├── config/                         # YAML configs and scenarios
│   ├── paths.yaml                  # Canonical paths
│   ├── sources.yaml                # Source registry
│   ├── scenarios/                  # Baseline and alternative scenario configs
│   └── parameters/                 # Harmonization rules, extraction maps, and parameter metadata
├── workflow/                       # Snakemake orchestration
│   ├── Snakefile                   # Main workflow entry point
│   └── rules/                      # Modular workflow rules
├── src/
│   ├── setup.py                    # Load config, create paths, fetch/cache data, validate sources
│   ├── build_transport.py          # Build SQLite, run modules, post-process, log
│   ├── utils.py                    # Shared I/O, logging, YAML, units, CSV/Excel helpers
│   ├── validation/                 # SQLite parity, schema, tolerance, and smoke checks
│   ├── fetching/                   # Upstream download, cache, and interim normalization
│   │   ├── nrcan_ceud.py           # NRCan CEUD transport tables
│   │   ├── vehicle_population.py   # Provincial vehicle population reports
│   │   ├── statcan_tables.py       # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py       # CER energy future projections
│   │   ├── nlr_atb_autonomie.py    # NLR ATB and Autonomie technology data
│   │   └── assorted_sources.py     # Smaller registered source adapters
│   └── parameterization/           # Transform normalized inputs into model parameters
│       ├── stocks_and_demands.py   # Capacity, demand, utilization, and anchors
│       ├── lifetimes_survival.py   # Lifetimes and survival curves
│       ├── road_aggregation.py     # Road class mappings and aggregation weights
│       ├── efficiencies.py         # Technology efficiencies
│       ├── capex_opex.py           # Investment and operating costs
│       ├── ldv_charging.py         # BEV charging profiles and time slices
│       ├── emissions.py            # Vehicle-cycle and operating emissions
│       ├── market_constraints.py   # Market shares, policy limits, and SCC rules
│       ├── adoption_constraints.py # Adoption and growth constraints
│       └── sector_coupling.py      # Fuel, electricity, hydrogen, and blends
├── inputs/
│   ├── temoa_schema_v4.sql         # CANOE/Temoa SQLite schema
│   ├── 0_canoe_template/           # Hard-coded metadata and tech/commodity archetypes
│   ├── 0_manual_params/            # Hard-coded heterogeneous parameter inputs
│   ├── 0_cache/                    # Fetched upstream downloads; never hand-edit
│   ├── 0_external_models/          # Registered external model outputs
│   ├── 1_interim/                  # Extracted and harmonized debug tables
│   └── 2_processed/                # Parameter-ready tables
├── outputs/
│   ├── sqlite/                     # Final CANOE/Temoa-ready databases
│   ├── validation/                 # Parity/schema/tolerance reports
│   └── logs/                       # Build logs and warnings
├── legacy_backend/                 # Old backend kept as validation evidence; read-mostly
├── tests/                          # Unit, parsing, schema, smoke, and parity tests
└── pyproject.toml                  # uv-managed package metadata and tooling
```

## Configuration boundaries

User-selectable choices must live in YAML config, not inside Python modules. This includes regions, years, source versions, source switches, cache/interim/processed/output paths, model periods, fuel prices, demand projections, technology assumptions, conversion factors, validation baselines, bins, mappings, filters, thresholds, and class groupings.

Use:

* `config/paths.yaml` for canonical directories and important artifact paths;
* `config/sources.yaml` for source identity, access, cache templates, validation rules, citations, and refresh notes;
* `config/scenarios/*.yaml` for run-specific selections and modeling switches;
* `config/parameters/*.yaml` for extraction maps, bins, class mappings, units, filters, conversion factors, and parameter metadata.
* `config/parameters/rules.yaml` stores harmonization protocols and assumptions, e.g., source filters, class mappings, bin definitions, and thresholds. 
* `config/parameters/conversion.yaml` stores any conversion factor used across the model, e.g., mass, volume, currency, and energy unit conversions.

Python modules may contain source-invariant implementation constants only. If a value affects modeling behavior, source selection, output location, or reproducibility, move it to config. Scripts should resolve paths through the config bundle instead of embedding repo-relative strings.

## Data and source rules

* Keep authoritative downloads in `inputs/0_cache/`; never hand-edit cached files.
* Keep registered external model outputs in `inputs/0_external_models/`.
* Keep normalized intermediates in `inputs/1_interim/`.
* Keep parameter-ready tables in `inputs/2_processed/`.
* Excel inputs require explicit maps in `config/parameters/`: workbook, sheet, range/table, units, expected shape, transformation, and target table.
* `sources.yaml` should record source path/URL, version/date, file type, units, citation, checksum or validation rule, and refresh notes where available.
* Prefer consistent, reusable source metadata fields. Do not over-normalize the source schema before patterns are proven across multiple source families.

## SQLite and Temoa rules

* Instantiate target databases from `inputs/canoe_dataset_schema.sql` unless a task explicitly changes the schema path.
* Preserve Temoa/CANOE schema integrity for regions, periods, vintages, technologies, commodities, demands, and time slices.
* No silent unit conversions; unit changes must be explicit, logged, and tested.
* Inserted rows must trace to a source record or documented assumption.
* Log deletions, skipped rows, fallback assumptions, cleanup actions, and pruned technologies.

## Baseline reproduction rules

Before introducing new assumptions or scenarios:

1. Rebuild the baseline with legacy-equivalent source assumptions.
2. Compare the new SQLite against the legacy compiled SQLite.
3. Document differences from refactoring, schema cleanup, rounding, quinquennial aggregation, extraction changes, or bug fixes.
4. Isolate intentional assumption changes in named scenarios or separate commits.

Do not improve assumptions during baseline reproduction unless the change is explicit, documented, and validated.

## Validation and logging

Validation code lives under `src/validation/`. Validation outputs go to `outputs/validation/`; logs go to `outputs/logs/`.

Validation should cover schema creation, row counts, parameter tolerances, key integrity, region/period/vintage/technology/commodity coverage, unit consistency, CFT normalization, skipped/fallback reports, pruning reports, and at least one small baseline smoke build.

Logs should capture selected configs, source status, modules executed, row counts, unit conversions, fallbacks, missing inputs, failures, validation results, pruned technologies, and post-processing corrections.

## Legacy policy

The v2.0 branch keeps the old backend as validation evidence. Treat `legacy/` as read-mostly reference material. Do not edit legacy artifacts unless explicitly asked for cleanup or migration.

When legacy behavior is needed, inspect only the relevant files first, usually the legacy compiler, NRCan fetcher, database-processing scripts, charging profiles, constraint workbooks, or baseline SQLite. Avoid broad rewrites, bulk formatting, or edits inside `legacy/`.

Large binary/data artifacts in `legacy/` are parity baselines or source evidence, not active development targets.

## Coding standards

* Use `uv` for dependency management.
* Keep functions small, readable, typed where useful, and testable.
* Prefer `pathlib.Path` and explicit column names.
* Use standard logging instead of `print()` in pipeline code.
* Keep pandas transformations readable and auditable.
* Add tests for parsing, unit conversion, schema insertion, and parity-sensitive transformations.
* Do not introduce large frameworks unless they clearly reduce complexity.

## Snakemake expectations

Snakemake should orchestrate config loading, cache setup, source download or registration, source validation, extraction, transformation, SQLite instantiation, parameter insertion, post-processing, cleanup, and validation reports.

Rules should be modular and named by workflow stage or parameter family.

*Note: During early development, stabilize Python entrypoints and artifact contracts before wiring them into Snakemake. Snakemake failures block workflow changes, not isolated source or parameter-module work, unless orchestration is explicitly part of the task.*

## Agent workflow

For multi-step, risky, or staged work, use `.agents/PLANS.md` and a task-specific ExecPlan. Treat ExecPlans as bounded implementation scaffolds: they constrain scope, risks, and acceptance criteria, but should not override better evidence found during implementation.

Otherwise, keep diffs small, inspect relevant files first, preserve compatibility, avoid invented assumptions, update tests or validation when behavior changes, and document meaningful changes.

When implementation diverges from a plan, update the plan’s progress/outcomes or decision notes instead of forcing code to satisfy an outdated assumption. Durable truth belongs in code, configs, tests, validation reports, and concise documentation.

## Git and done criteria

Each commit should be one logical change, such as one source integration, parameter module, validation addition, workflow rule group, SQLite insertion change, or focused refactor.

A task is done when the relevant command or Snakemake target runs, expected outputs are generated, validation passes or differences are documented, logs are written, tests are updated where appropriate, and the change is reproducible from version-controlled files plus registered inputs.

If the task changed assumptions, source metadata conventions, module boundaries, or architecture expectations, record the rationale in the relevant plan, config comments, validation notes, or documentation before considering it complete.
