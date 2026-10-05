---
title: Backend architecture
role: Structural design reference for repository layout and module ownership.
retrieve_when: A task affects repository structure, module ownership, orchestration seams, or artifact placement.
read_scope: Read only the relevant tree branches and descriptions.
verify: Check planned content against current code, config, tests, schemas, and validation evidence before implementation.
---

## Application and operating model

This repository is an **agent-native ETL backend** for compiling configured Canadian
transport-sector scenarios into auditable, CANOE/Temoa-ready SQLite databases. It turns
registered external, external-model, and reviewed manual inputs into normalized evidence,
model parameters, provenance records, and a schema-validated database published atomically.

The standalone transport SQLite remains a first-class output for legacy parity, focused
validation, and independent transport research. The backend also exposes contribution
preparation and insertion for a caller-owned compatible database. Both paths use the same
transport parameterization; an upstream-specific adapter remains planned. #to-review

The tree below shows implemented files and the selected homes for planned parameterization
families. Planned entries are labeled explicitly; they are ownership targets, not empty module
requirements.

```text
.
├── AGENTS.md                                # Stable repository policy
├── README.md                                # Human-facing project orientation
├── .agents/
│   ├── PLANS.md                             # ExecPlan protocol
│   ├── plans/                               # Task-local implementation records
│   └── skills/                              # Optional task-retrieved procedures
├── config/
│   ├── paths.yaml                           # Canonical paths and artifact-family impact routes
│   ├── sources.yaml                         # External-source registry and provenance
│   ├── scenarios/                           # Scenario authoring contract
│   └── parameters/
│       ├── rules.yaml                       # Extraction and harmonization contracts
│       └── conversion.yaml                  # Reusable conversion factors
├── workflow/
│   └── Snakefile                            # Coarse dependency and artifact orchestration #to-review
├── src/
│   ├── setup.py                             # Configuration/schema smoke entrypoint
│   ├── build_transport.py                   # Transport contribution and atomic database assembly
│   ├── canoe_adapter.py                     # CANOE-main orchestrator adapter for running this backend - #to-do
│   ├── fetching/                            # Upstream download, cache, and interim normalization
│   │   ├── nrcan_ceud.py                    # NRCan CEUD transport tables
│   │   ├── vehicle_population.py            # Ontario MTO report acquisition and normalization
│   │   ├── statcan_tables.py                # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py                # CER energy future tables
│   │   ├── nlr_atb_autonomie.py             # NLR ATB and ANL Autonomie inputs
│   │   ├── greet_automation.py              # Explicit disposable-copy Excel generation of vehicle-cycle evidence #to-review
│   │   ├── greet_vehicle_cycle.py           # Registered source contracts, extraction and offline bank validation #to-review
│   │   ├── fueleconomy_vehicles.py          # Opt-in FuelEconomy.gov class evidence
│   │   ├── vpic_vehicle_types.py            # Opt-in vPIC vehicle-type evidence
│   │   ├── vpic_model_years.py              # Opt-in vPIC make/model-year evidence
│   │   └── assorted_sources.py              # Smaller registered source adapters
│   ├── parameterization/                    # Transform normalized inputs into model parameters
│   │   ├── build_existing_capacity.py       # Combine and validate road/off-road capacity rows
│   │   ├── build_demand.py                  # Project and validate combined transport demand
│   │   ├── build_lifetime_parameters.py     # Select and validate fixed/curve lifetime rows
│   │   ├── build_efficiencies.py            # Combine efficiency rows and PHEV input splits
│   │   ├── build_costs.py                   # Combine and validate investment/variable costs
│   │   ├── manual_parameters.py             # Validate registry and resolve generic selectors
│   │   ├── road_stocks_and_demands.py       # Road existing stock and demand products
│   │   ├── road_utilization.py              # Road C2A and annual utilization, including vintage-period artifacts
│   │   ├── offroad_stocks_and_demands.py    # Off-road existing stock and demand products
│   │   ├── road_lifetimes_survival.py       # Accepted road lifetime, survival, and MTO diagnostics
│   │   ├── offroad_lifetimes.py             # Lifetimes of remaining technologies
│   │   ├── road_aggregation.py              # Reviewed mapping application and aggregation weights
│   │   ├── vehicle_mapping_bootstrap.py     # Explicit mapping-development entrypoint
│   │   ├── road_efficiencies.py             # Road technology efficiencies
│   │   ├── offroad_efficiencies.py          # Off-road technology efficiencies
│   │   ├── road_capex_opex.py               # Road investment and operating costs
│   │   ├── offroad_capex_opex.py            # Off-road investment and operating costs
│   │   ├── currency.py                      # CER-backed currency and price-year conversion
│   │   ├── ev_chargers.py                   # EV charging infrastructure preparation and validated artifacts
│   │   ├── ldv_charging_profiles.py         # Hourly LDEV charging demand profiles - #to-do
│   │   ├── road_embodied_emissions.py       # Road vehicle-cycle lifetime gases and regional aggregation #to-review
│   │   ├── market_constraints.py            # Market shares, policy limits, and SCC rules - #to-do
│   │   └── adoption_constraints.py          # Adoption and growth constraints - #to-do
│   ├── utils/
│   │   ├── __init__.py                      # Typed config loading and artifact path resolution
│   │   ├── files.py                         # Shared hashing and atomic CSV publication
│   │   └── vehicle_labels.py                # Shared vehicle-label mechanics
│   └── validation/
│       ├── config_models.py                 # Pydantic configuration contracts
│       ├── config_smoke.py                  # Setup-time config/schema status and directory creation
│       ├── provenance.py                    # Source and dataset provenance
│       ├── schema_contract.py               # canoe-schema v4 compatibility
│       ├── insertion.py                     # Scoped pre-insertion cleanup and validated insertion
│       ├── database_bootstrap.py            # Post-insertion database integrity checks
│       ├── sqlite_utils.py                  # Shared SQLite identifier mechanics
│       └── legacy_compare.py                # Narrow configured legacy comparison
├── inputs/
│   ├── 0_canoe_template/                    # Backend-owned structural templates
│   ├── 0_cache/                             # Authoritative cached downloads
│   ├── 0_external_models/                   # Registered external-model artifacts
│   ├── 0_manual_params/                     # Review-owned compact manual parameter tables
│   ├── 1_interim/                           # Normalized source and auditable intermediate tables
│   ├── 2_processed/                         # Parameter-ready ETL products
│   └── validation/                          # Development, review, and transformation diagnostics
├── outputs/
│   ├── sqlite/                              # Built CANOE/Temoa-ready databases
│   ├── validation/                          # Validation and schema integrity reports
│   └── logs/                                # Run logs and warnings
├── docs/
│   ├── backend_architecture.md              # Repository structure and ownership reference
│   ├── canoe_main_orchestrator.md           # Verified upstream CANOE-main integration context
│   └── etl_flowcharts.md                    # Parameter-specific lineage reference
├── legacy_backend/                          # Read-mostly parity evidence
├── scripts/
│   ├── doctor.py                            # Non-mutating repository readiness check
│   └── clean_runtime.py                     # Explicit runtime cleanup
├── tests/                                   # Focused and integration tests
└── pyproject.toml                           # uv dependencies and tool configuration
```

## Module boundaries

The implemented pipeline follows acquisition → normalized evidence → parameter preparation
→ database assembly. `fetching/` owns source-specific I/O and normalization. Within
`parameterization/`, road/off-road modules own the differing calculations; `road_aggregation`,
`road_utilization`, and `ev_chargers` own their shared behavioral contracts. Builders compose
these functions directly, with no forwarding modules or second transformation path.

The shared transport builders have a `build_` prefix to distinguish preparation entrypoints
from schema tables and sector-wide assembly. They resolve provenance, validate combined
coverage and keys, and publish configured artifacts without opening SQLite connections:

| Builder | Prepared contract |
| --- | --- |
| `build_existing_capacity` | Road/off-road technology-vintage capacity and reconciliation evidence. |
| `build_demand` | Road/off-road base activity, CER scenario projections, and regional-period demand. |
| `build_lifetime_parameters` | Exactly one fixed or survival-curve representation per modeled region/technology. |
| `build_efficiencies` | Fuel/service efficiency edges and PHEV input splits, gated by existing capacity. |
| `build_costs` | Investment and variable costs, harmonized with `currency` and constrained by capacity/lifetime coverage. |

`build_transport.py` calls the same preparation functions for standalone and caller-owned
assembly. `prepare_transport_contribution` gathers structural templates, capacity, demand,
road utilization, lifetimes, efficiencies, costs, charger rows and the full embodied
preparation (gas rows, provenance, source and aggregation audits) as selected; #to-review
`insert_transport_contribution` registers provenance and inserts validated rows into a
compatible caller-owned connection. The standalone path also owns schema initialization,
transactions, integrity checks, and atomic publication. Schema contracts and insertion
mechanics remain in `validation/`, backed by the pinned `canoe-schema` package.

## Running and extending the backend

From the repository root, use `uv run python src/setup.py --scenario <scenario.yaml>` for
configuration/schema setup and `uv run python scripts/doctor.py --scenario <scenario.yaml>`
for readiness checks. Run a prepared scenario with
`uv run python src/build_transport.py --scenario <scenario.yaml>`; use
`uv run python -m parameterization.build_<family> --scenario <scenario.yaml>` for an
individual builder. Preparation also publishes audit CSVs. Fetching entrypoints support
explicit cache replay with `--no-download` where implemented.

Use `uv run snakemake --snakefile workflow/Snakefile --cores 1 --config
scenario=<scenario.yaml>` for coarse orchestration; add `--dry-run` to inspect pending
jobs. The workflow calls existing Python entrypoints, requires readiness and the declared
StatCan/CER tables before assembly, and tracks control files, production manual tables,
templates, implementation files, and offline caches at those boundaries. Downloads require
`download_sources=true`; this permits fetching missing caches rather than refreshing them.
The default profile keeps runtime metadata under ignored `.snakemake/`. Failed database
jobs restore the previous SQLite and validation report.

This is still a partial DAG: other normalized evidence, road aggregation, and accepted
road lifetime products must already be prepared. Changes to those undeclared prerequisites
require `--forcerun transport_database`. Parameter CSVs are audit exports rather than
complete serialized preparation results: assembly also needs provenance contexts, internal
datasets, and audits returned by the builders. Assembly therefore prepares parameters once
through the shared Python path. Run one scenario at a time while interim/processed products
share configured directories.

Mapping bootstrap and `--mapping-diagnostics` are opt-in development paths; road lifetime
generation defaults to accepted evidence, with MTO review enabled by `--mto-diagnostics`
or `--all`.

For a change, follow the affected `config/paths.yaml` artifact family to its owner,
producers, consumers, and validation surfaces, then verify the current code and focused
tests. Module renames change those references; artifact keys and schema table names retain
their product meaning. `config/sources.yaml` component `parameter_modules` records source
lineage, including planned owners, rather than executable build readiness.

Use [AGENTS.md](../AGENTS.md) for repository policy and config responsibilities,
[etl_flowcharts.md](etl_flowcharts.md) for parameter lineage, and
[assumptions.md](assumptions.md) for enduring data challenges. Retrieve
[codebase_diagnostic_snapshot.md](codebase_diagnostic_snapshot.md) for a structural review
and [canoe_main_orchestrator.md](canoe_main_orchestrator.md) for an upstream integration
change; verify their snapshots against current interfaces.
