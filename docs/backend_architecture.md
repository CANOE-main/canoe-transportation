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
transport parameterization; an upstream-specific adapter remains planned.

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
│   │   ├── greet_automation.py              # Explicit disposable-copy Excel generation of vehicle-cycle evidence
│   │   ├── greet_vehicle_cycle.py           # Registered source contracts, extraction and offline bank validation
│   │   ├── fueleconomy_vehicles.py          # Opt-in FuelEconomy.gov class evidence
│   │   ├── legacy_charging_profiles.py     # Immutable NHTS/TTS charging evidence and legacy hourly transformation
│   │   ├── epa_omega_baseline.py           # Compact legacy OMEGA baseline sales/CD ranges and source identity validation
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
│   │   ├── road_embodied_emissions.py       # Road vehicle-cycle lifetime gases and regional aggregation
│   │   ├── ev_chargers.py                   # EV charging infrastructure preparation and validated artifacts
│   │   ├── ldv_charging_profiles.py         # Inherited legacy LDEV charging profiles, to be refactored
│   │   ├── ldv_ev_ranges.py                 # OMEGA range shares, minimum constraints and parameter-specific representative LDV aggregation
│   │   └── adoption_constraints.py          # Vehicle technology adoption constraints - #to-do
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
road utilization, lifetimes, efficiencies, costs, charger rows, charging profiles,
range-share groups/constraints and embodied emissions as selected.
`insert_transport_contribution` registers provenance and inserts validated rows into a
compatible caller-owned connection. The standalone path also owns schema initialization,
transactions, integrity checks, and atomic publication. Schema contracts and insertion
mechanics remain in `validation/`, backed by the pinned `canoe-schema` package.

## Running and extending the backend

From the repository root, use `uv run python src/setup.py --scenario <scenario.yaml>` for
configuration/schema setup and `uv run python scripts/doctor.py --scenario <scenario.yaml>`
for non-mutating readiness checks. The supported scenario command is
`uv run python src/build_transport.py --scenario <scenario.yaml> --overwrite`. It owns the
complete production lifecycle through `prepare_scenario_inputs`, followed by the shared
contribution preparation, validated insertion and atomic SQLite publication.

Every invocation regenerates CEUD/ratings, StatCan, CER, ATB/Autonomie, assorted sources,
the TC dashboard, the selected Ontario Report A/4/5 edition and reviewed road aggregation
from registered inputs. FuelEconomy class evidence is validated directly from its cache.
Lifetime and other parameter families use the existing live preparation contracts; CSVs
remain audit products rather than serialized contribution handoffs. Registered GREET,
OMEGA and charging evidence is validated by its owning preparation functions.

Execution is offline by default. `--download-sources` explicitly permits acquisition of
missing caches; existing caches are reused. Ontario replay uses the resource identities
and hashes in `sources.yaml`, independently of generated manifests. Source refresh,
reviewed mapping changes and external-model generation remain separate operations.

Derived prerequisites are rebuilt even when files already exist, so changes to source
content, YAML or implementation cannot leave the database silently reused. This is a
serial full rebuild, with no incremental job cache. Run one scenario at a time while
interim/processed paths are shared. Individual fetching and parameter entrypoints remain
available for development. Caller-owned assembly can call `prepare_scenario_inputs`
before the existing contribution/insertion seam without invoking publication.

The scenario command stages the validated database and report before publication. Build
and ordinary I/O failures retain the previous pair; a failed SQLite replacement restores
the prior report. SQLite replacement is atomic, but the two files are not crash-atomic
as a pair across power loss or process termination. Intermediate audit files may reflect
a failed attempt and are regenerated on the next invocation.

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
