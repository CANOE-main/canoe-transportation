# AGENTS.md

## Mission

This repository is the v2.0 refactor of the CANOE transportation backend. Replace the legacy Excel-centered compiler with a reproducible, auditable, Snakemake-orchestrated Python pipeline that builds a Temoa/CANOE-ready SQLite database for transport-sector modeling.

The first milestone is baseline reproduction: using the same legacy assumptions and source data, the v2.0 backend should reproduce the old compiled SQLite outputs within documented tolerances. New scenarios, fuel-price futures, representation-choice experiments, GSA, or MGA workflows come after parity gaps are explained or accepted.

## Modeling context

CANOE is a Canadian Open Energy Model built on Temoa. The transport backend represents road and off-road technologies, demands, vehicle stocks, efficiencies, costs, emissions, lifetimes, market constraints, fuel links, and EV charging profiles.

The backend must remain flexible enough to test alternative transport-sector representation choices, including technology progress, demand projections, fuel-price trajectories, utilization factors, BEV charging profiles, adoption constraints, retirement formulations, emissions scopes, and sector coupling with electricity, hydrogen, liquid fuels, and broader CANOE supply chains.

## Engineering principles

1. Reproducibility: run from version-controlled code, YAML configs, cached sources, and registered external inputs.
2. Traceability: every parameter should map to source, transformation logic, units, assumptions, and target SQLite table.
3. Validation first: reproduce legacy SQLite outputs before changing assumptions or expanding scenarios.
4. Robustness: missing non-critical inputs should produce warnings, skipped-output reports, and logs, not silent failure.
5. Modularity: group code by parameter family and source relationship, not one giant compiler.
6. Scenario awareness: use Snakemake and YAML for source choices, regions, periods, fuel prices, and modeling switches.
7. Practical Python: prefer readable, typed, testable functions over unnecessary abstractions.
8. Agent-ready work: define files, commands, validation checks, and acceptance criteria for each task.

## Planned architecture (rough draft, non-exhaustive)

    .
    ├── AGENTS.md                       # Repo-level instructions for Codex, Copilot, Antigravity, and ChatGPT
    ├── .agent/                         # Optional agent execution guidance
    │   ├── PLANS.md                    # ExecPlan template; use only for multi-step or risky refactors
    │   ├── plans/                      # Task-specific implementation plans
    │   └── skills/                     # Small repo-local skills with narrow, repeated guidance
    ├── config/                         # YAML configuration and scenario definitions
    │   ├── paths.yaml                  # Canonical input, cache, interim, output, validation, and log paths
    │   ├── sources.yaml                # Source registry: URLs, versions, checksums, units, citations, refresh rules
    │   ├── scenarios/                  # Baseline, fuel-price, modeling-choice, GSA, and MGA scenario configs
    │   └── parameters/                 # Extraction maps and parameter metadata
    ├── workflow/                       # Snakemake orchestration
    │   ├── Snakefile                   # Main workflow entry point
    │   └── rules/                      # Modular rules for fetch, process, build, validate, and report stages
    ├── src/
    │   ├── setup.py                    # Load config, create paths, fetch/cache data, validate source availability
    │   ├── utils.py                    # Shared I/O, logging, YAML loading, unit checks, CSV/Excel helpers
    │   ├── build_transport.py          # Instantiate SQLite, run parameter modules, post-process, and write logs
    │   └── parameterization/
    │       ├── validation/             # SQLite parity, schema checks, row diffs, tolerance reports, smoke tests
    │       ├── stocks_and_demands.py   # Existing capacity, activity demand, utilization, historical anchors
    │       ├── on_road_effs_and_costs.py       # Road vehicle efficiency, capex, fixed O&M, tech projections
    │       ├── on_road_variable_costs.py       # Road vehicle maintenance, variable O&M, and operating costs
    │       ├── off_road_effs_and_costs.py      # Aviation, rail, marine, and other off-road efficiency/costs
    │       ├── on_road_lifetimes.py            # Road vehicle survival curves, retirement, and lifetime assumptions
    │       ├── off_road_lifetimes.py           # Off-road lifetimes, retirement assumptions, vintage availability
    │       ├── ldv_charging_dist.py            # RAMP-mobility BEV charging profiles to Capacity Factor Tech (CFT)-ready tables
    │       ├── emission_embodied.py            # Vehicle-cycle/manufacturing emissions by tech and vintage
    │       ├── emission_activity.py            # Fuel-cycle/upstream and activity-linked emissions factors
    │       ├── market_constraints.py           # SCC, ZEV shares, AER classes, fuel limits, policy constraints
    │       ├── adoption_constraints.py         # Empirical adoption/growth constraints for emerging powertrains
    │       └── sector_coupling.py              # Transport links to fuel supply, electricity, hydrogen, blending
    ├── inputs/
    │   ├── canoe_dataset_schema.sql    # Standard CANOE SQLite schema for Temoa-compatible target databases
    │   ├── existing_techs.csv          # Existing technology dictionary and source-category mappings
    │   ├── new_techs.csv               # Future investment technology dictionary and proxy mappings
    │   ├── fuel_commodities.csv        # Fuel/commodity definitions, units, labels, and emissions links
    │   ├── demands.csv                 # Service-demand definitions: VKT, passenger-km, tonne-km, modal demand
    │   ├── regions.csv                 # Provincial/territorial regions and proxy-region mappings
    │   ├── time.csv                    # Periods, seasons, day types, time slices, segment fractions, hour maps
    │   ├── cache/                      # Cached upstream downloads; never hand-edit
    │   ├── external/                   # Registered external model outputs not generated by this backend
    │   │   └── RAMP-mobility/          # LDV BEV charging profiles used for DSD/CFT construction
    │   ├── interim/                    # Extracted/normalized debugging and validation tables
    │   └── processed/                  # Parameter-ready tables before SQLite insertion
    ├── outputs/
    │   ├── sqlite/                     # Final CANOE/Temoa-ready SQLite databases
    │   ├── validation/                 # Legacy parity, schema checks, row diffs, tolerance reports
    │   └── logs/                       # Build logs, warnings, skipped parameters, source status, scenarios
    ├── tests/                          # Unit, parsing, parameter, schema, smoke, and parity tests
    ├── legacy/                                 # Old backend kept only as validation evidence, has [deprecated] tags in subdirectories
    │   ├── transportation/                     # Contains data aggregation Excel files and transport sector compilers
    │   │   │── compile_transport.py            # Core script of the old backend - sanitizes and inserts Excel params into Temoa sqlites
    │   │   └── get_nrcan_data.py               # Fetches NRCan transport data into an Excel master template to create .xlsx province-variants
    │   ├── charging_profiles/                  # Output data from RAMP-mobility used to derive LDV charging profiles 
    │   ├── db_processing/                      # Scripts used to update and transform sqlites, and merge with other sectors 
    │   ├── model_constraints/                  # Excel files with explicit constraints applied to vanilla database
    │   ├── results_analysis/                   # Jupyter notebooks used to analyze and visualize scenario results
    │   ├── canoe_on_12d_baseline.sqlite/       # Legacy CANOE-transportation baseline scenario database - backend output
    │   └── canoe_schema.sql/                   # Legacy CANOE SQLite schema for Temoa-compatible target databases
    └── pyproject.toml                  # uv-managed package metadata, dependencies, tests, linting

## Data rules

- Keep authoritative downloads in `inputs/cache/`; never hand-edit cached files.
- Keep external model outputs in `inputs/external/`; register them, validate them, and consume them as inputs.
- Keep normalized intermediates in `inputs/interim/` and parameter-ready outputs in `inputs/processed/`.
- Excel inputs are allowed only when workbook, sheet, range/table, units, expected shape, and target parameter are mapped in `config/parameters/`.
- Every source in `config/sources.yaml` should include, where available: URL/path, version/date, file type, units, citation, checksum or validation rule, and refresh instructions.
- Every extraction map should identify: source file, sheet/table/range, expected dimensions, units, transformation function, and target SQLite table.

## SQLite and Temoa rules

- Instantiate target databases from `src/canoe_schema.sql`.
- Preserve Temoa/CANOE schema integrity: validate regions, periods, vintages, technologies, commodities, demands, and time slices before insertion.
- No silent unit conversions. Unit changes must be explicit, logged, and tested.
- All inserted rows should be traceable to a source record or documented assumption.
- All deletions, skipped rows, fallback assumptions, and cleanup actions must be logged.
- Do not silently prune technologies. If a technology is removed because it lacks `ExistingCapacity`, falls below epsilon, or lacks valid support parameters, log the reason.

## Baseline reproduction rules

Before introducing new assumptions or scenarios:

1. Rebuild the baseline with legacy-equivalent source assumptions.
2. Compare the new SQLite against the legacy compiled SQLite.
3. Document differences from refactoring, schema cleanup, rounding, quinquennial aggregation, extraction changes, or bug fixes.
4. Isolate any intentional assumption change in a named scenario or separate commit.

Do not “improve” assumptions during baseline reproduction unless the change is explicit, documented, and validated.

## Scenario management rules

Scenario YAML files should control regions, periods, source versions, source-selection switches, fuel-price trajectories, demand projections, technology cost/efficiency assumptions, LDV charging profiles, utilization factors, emissions scopes, adoption constraints, retirement formulations, output database names, and validation baselines.

Avoid hardcoding scenario logic in Python when the choice belongs in YAML.

## Validation expectations

Validation should write outputs to `outputs/validation/` and include:

- schema creation checks;
- row-count comparisons against legacy SQLite;
- parameter-level tolerance checks;
- primary-key and foreign-key integrity checks;
- region, period, vintage, technology, commodity, and demand coverage checks;
- unit consistency checks;
- CFT normalization checks;
- skipped-parameter, fallback, and pruning reports;
- at least one smoke test that builds a small regional baseline database.

## Legacy framework policy

The v2.0 branch intentionally keeps the old transportation backend as validation evidence. Legacy spreadsheets, compiler scripts, helper scripts, charging-profile inputs, constraints, notebooks, schemas, and compiled SQLite outputs live under `legacy/`.

Treat `legacy/` as read-mostly reference material. Do not modify legacy artifacts unless the task explicitly asks for legacy cleanup or migration. The active v2.0 backend should be implemented under `src/`, orchestrated through `workflow/`, configured through `config/`, and validated against selected files in `legacy/`.

When a task requires legacy behavior, inspect only the relevant legacy files first, usually `legacy/transportation[deprecated]/compile_transport.py`, `legacy/transportation[deprecated]/get_nrcan_data.py`, `legacy_backend/db_processing[deprecated]/update_database/subset_replacement.py` or the baseline SQLite. Avoid broad rewrites, bulk formatting, or edits inside `legacy/`.

Large binary/data artifacts in `legacy/` are parity baselines or source evidence, not active development targets. Prefer documenting their role in `legacy/README.md` and referencing them from `config/paths.yaml` or validation configs.

## Logging expectations

Logs should be useful for both local debugging and agent-generated changes. Capture selected scenario/config files, source files found/downloaded/cached/skipped/missing, modules executed, row counts before and after transformations, unit conversions, fallback assumptions, non-critical missing inputs, critical failures, validation results, pruned technologies, and post-processing corrections.

## Coding standards

- Use `uv` for dependency management.
- Keep functions small, readable, typed where useful, and testable.
- Prefer `pathlib.Path` over raw path strings.
- Prefer explicit column names over positional indexing, except for documented legacy Excel ranges.
- Use standard logging instead of `print()` in pipeline code.
- Keep pandas transformations readable; avoid dense one-liners when clarity matters.
- Add tests for parsing, unit conversion, schema insertion, and parity-sensitive transformations.
- Do not introduce large frameworks unless they clearly reduce complexity.

## Snakemake expectations

Snakemake should orchestrate configuration loading, cache setup, source download or registration, source validation, extraction to interim tables, transformation to processed parameter tables, SQLite schema instantiation, parameter insertion, post-processing, cleanup, and validation reports.

Rules should be modular and named by workflow stage or parameter family.

## Agent workflow

Before editing:

1. Inspect relevant files and legacy behavior.
2. Identify expected outputs and validation targets.
3. Propose or implement the smallest safe change.
4. Add or update tests/validation.
5. Update an ExecPlan only for multi-step, risky, or staged work.

When editing:

- Keep diffs small.
- Avoid unnecessary renames and broad rewrites.
- Preserve compatibility unless the user asks for a breaking refactor.
- Do not invent source data or assumptions.
- Comment only to explain non-obvious modeling or transformation logic.
- Update docs when behavior, commands, files, or validation expectations change.

## Git expectations

Each commit should represent one logical change: one parameter module, source integration, validation addition, workflow rule group, schema/SQLite insertion change, or focused refactor.

Commit messages should state what changed and why it matters for reproducibility, validation, or scenario readiness.

## Definition of done

A task is done when code runs from the repo environment, the relevant Snakemake target or Python entry point executes, expected outputs are generated, validation passes or differences are documented, logs are written, tests are added or updated where appropriate, and the change is reproducible from version-controlled files plus registered inputs.