# AGENTS.md

## Mission

This repository is the v2.0 refactor of the CANOE transportation backend. Replace the legacy Excel-centered compiler with a reproducible, auditable, Snakemake-orchestrated Python pipeline that builds a Temoa/CANOE-ready SQLite database for transport-sector modeling.

First priority: baseline reproduction. Using legacy-equivalent assumptions and source data, v2.0 should reproduce the old compiled SQLite outputs within documented tolerances. New scenarios, fuel-price futures, representation-choice experiments, GSA, and MGA workflows come only after parity gaps are explained or accepted.

## Modeling context

CANOE is a Canadian Open Energy Model built on Temoa. The transport backend represents road/off-road technologies, demands, stocks, efficiencies, costs, emissions, lifetimes, market constraints, fuel links, and EV charging profiles.

The backend must support controlled experiments on transport representation choices: technology progress, demand projections, fuel prices, utilization, BEV charging, adoption constraints, retirement formulations, emissions scopes, and coupling with electricity, hydrogen, liquid fuels, and broader CANOE supply chains.

## Engineering principles

1. Reproducibility: run from version-controlled code, YAML configs, cached sources, and registered external inputs.
2. Traceability: map every parameter to source, transformation logic, units, assumptions, and target SQLite table.
3. Validation first: reproduce legacy SQLite outputs before changing assumptions or expanding scenarios.
4. Robustness: log missing non-critical inputs, skipped outputs, fallbacks, and warnings.
5. Modularity: group code by parameter family and source relationship, not one giant compiler.
6. Scenario awareness: use Snakemake and YAML for regions, periods, sources, fuel prices, and modeling switches.
7. Practical Python: prefer readable, typed, testable functions over unnecessary abstractions.

## Planned architecture

    .
    ├── AGENTS.md                       # Stable repo instructions
    ├── .agent/                         # Optional agent planning guidance
    │   ├── PLANS.md                    # ExecPlan protocol
    │   ├── plans/                      # Task-specific plans
    │   └── skills/                     # Small repo-local skills
    ├── config/                         # YAML configs and scenarios
    │   ├── paths.yaml                  # Canonical paths
    │   ├── sources.yaml                # Source registry
    │   ├── scenarios/                  # Baseline and experiment configs
    │   └── parameters/                 # Extraction maps and parameter metadata
    ├── workflow/                       # Snakemake orchestration
    │   ├── Snakefile                   # Main workflow entry point
    │   └── rules/                      # Modular workflow rules
    ├── src/
    │   ├── setup.py                    # Load config, create paths, fetch/cache data, validate sources
    │   ├── utils.py                    # Shared I/O, logging, YAML, units, CSV/Excel helpers
    │   ├── build_transport.py          # Build SQLite, run modules, post-process, log
    │   └── parameterization/
    │       ├── validation/                     # SQLite parity, schema, tolerance, and smoke checks
    │       ├── stocks_and_demands.py           # Existing capacity, demand, utilization, historical anchors
    │       ├── on_road_effs_and_costs.py       # Road efficiency, capex, fixed O&M
    │       ├── on_road_variable_costs.py       # Road maintenance and variable O&M
    │       ├── off_road_effs_and_costs.py      # Aviation, rail, marine, other off-road costs/efficiencies
    │       ├── on_road_lifetimes.py            # Road survival, retirement, lifetimes
    │       ├── off_road_lifetimes.py           # Off-road lifetimes and vintage availability
    │       ├── ldv_charging_dist.py            # RAMP-mobility BEV profiles to CFT-ready tables
    │       ├── emission_embodied.py            # Vehicle-cycle/manufacturing emissions
    │       ├── emission_activity.py            # Fuel-cycle/upstream/activity emissions
    │       ├── market_constraints.py           # SCC, ZEV shares, AER classes, fuel/policy limits
    │       ├── adoption_constraints.py         # Adoption/growth constraints
    │       └── sector_coupling.py              # Fuel, electricity, hydrogen, blending links
    ├── inputs/
    │   ├── canoe_dataset_schema.sql    # CANOE/Temoa SQLite schema
    │   ├── existing_techs.csv          # Existing technology dictionary
    │   ├── new_techs.csv               # Future technology dictionary
    │   ├── fuel_commodities.csv        # Fuel/commodity definitions
    │   ├── demands.csv                 # Service-demand definitions
    │   ├── regions.csv                 # Region and proxy mappings
    │   ├── time.csv                    # Periods, seasons, slices, fractions, hour maps
    │   ├── cache/                      # Cached upstream downloads; never hand-edit
    │   ├── external/                   # Registered external model outputs
    │   │   └── RAMP-mobility/          # LDV BEV charging profiles
    │   ├── interim/                    # Extracted/normalized debug tables
    │   └── processed/                  # Parameter-ready tables
    ├── outputs/
    │   ├── sqlite/                     # Final CANOE/Temoa-ready databases
    │   ├── validation/                 # Parity/schema/tolerance reports
    │   └── logs/                       # Build logs and warnings
    ├── legacy/                         # Old backend kept as validation evidence; read-mostly
    │   ├── transportation/             # Legacy compilers and Excel aggregation workflow
    │   ├── charging_profiles/          # Legacy RAMP-mobility outputs
    │   ├── db_processing/              # Legacy SQLite transformation/merge scripts
    │   ├── model_constraints/          # Legacy constraint workbooks
    │   ├── results_analysis/           # Legacy analysis notebooks
    │   ├── canoe_on_12d_baseline.sqlite    # Legacy CANOE-transportation baseline database
    │   └── canoe_schema.sql                # Legacy CANOE SQLite schema
    ├── tests/                          # Unit, parsing, schema, smoke, and parity tests
    └── pyproject.toml                  # uv-managed package metadata and tooling

## Data rules

- Keep authoritative downloads in `inputs/cache/`; never hand-edit cached files.
- Keep external model outputs in `inputs/external/`; register, validate, and consume them as inputs.
- Keep normalized intermediates in `inputs/interim/` and parameter-ready outputs in `inputs/processed/`.
- Excel inputs require explicit maps in `config/parameters/`: workbook, sheet, range/table, units, expected shape, transformation, and target table.
- `config/sources.yaml` should record source path/URL, version/date, file type, units, citation, checksum or validation rule, and refresh notes where available.

## SQLite and Temoa rules

- Instantiate target databases from `inputs/canoe_dataset_schema.sql` unless a task explicitly changes the schema path.
- Preserve Temoa/CANOE schema integrity for regions, periods, vintages, technologies, commodities, demands, and time slices.
- No silent unit conversions; unit changes must be explicit, logged, and tested.
- Inserted rows must trace to a source record or documented assumption.
- Log deletions, skipped rows, fallback assumptions, cleanup actions, and pruned technologies.

## Baseline reproduction rules

Before introducing new assumptions or scenarios:

1. Rebuild the baseline with legacy-equivalent source assumptions.
2. Compare the new SQLite against the legacy compiled SQLite.
3. Document differences from refactoring, schema cleanup, rounding, quinquennial aggregation, extraction changes, or bug fixes.
4. Isolate intentional assumption changes in named scenarios or separate commits.

Do not “improve” assumptions during baseline reproduction unless the change is explicit, documented, and validated.

## Scenario management rules

Scenario YAML files should control regions, periods, source versions, source-selection switches, fuel prices, demand projections, technology assumptions, charging profiles, utilization factors, emissions scopes, constraints, retirement formulations, output names, and validation baselines.

Avoid hardcoding scenario logic in Python when the choice belongs in YAML.

## Validation and logging

Validation outputs go to `outputs/validation/`; logs go to `outputs/logs/`.

Validation should cover schema creation, row counts, parameter tolerances, key integrity, region/period/vintage/technology/commodity coverage, unit consistency, CFT normalization, skipped/fallback reports, pruning reports, and at least one small baseline smoke build.

Logs should capture selected configs, source status, modules executed, row counts, unit conversions, fallbacks, missing inputs, failures, validation results, pruned technologies, and post-processing corrections.

## Legacy policy

The v2.0 branch keeps the old backend as validation evidence. Treat `legacy/` as read-mostly reference material. Do not edit legacy artifacts unless explicitly asked for cleanup or migration.

When legacy behavior is needed, inspect only the relevant files first, usually the legacy compiler, NRCan fetcher, database-processing scripts, charging profiles, constraint workbooks, or baseline SQLite. Avoid broad rewrites, bulk formatting, or edits inside `legacy/`.

Large binary/data artifacts in `legacy/` are parity baselines or source evidence, not active development targets. Document their role in `legacy/README.md` or reference them from config/validation files.

## Coding standards

- Use `uv` for dependency management.
- Keep functions small, readable, typed where useful, and testable.
- Prefer `pathlib.Path` and explicit column names.
- Use standard logging instead of `print()` in pipeline code.
- Keep pandas transformations readable.
- Add tests for parsing, unit conversion, schema insertion, and parity-sensitive transformations.
- Do not introduce large frameworks unless they clearly reduce complexity.

## Snakemake expectations

Snakemake should orchestrate config loading, cache setup, source download/registration, source validation, extraction, transformation, SQLite instantiation, parameter insertion, post-processing, cleanup, and validation reports.

Rules should be modular and named by workflow stage or parameter family.

## Agent workflow

For multi-step, risky, or staged work, use `.agent/PLANS.md` and a task-specific ExecPlan. Otherwise, keep diffs small, inspect relevant files first, preserve compatibility, avoid invented assumptions, update tests/validation, and document behavior changes.

## Git and done criteria

Each commit should be one logical change, such as one parameter module, source integration, validation addition, workflow rule group, schema/SQLite insertion change, or focused refactor.

A task is done when the relevant command or Snakemake target runs, expected outputs are generated, validation passes or differences are documented, logs are written, tests are updated where appropriate, and the change is reproducible from version-controlled files plus registered inputs.