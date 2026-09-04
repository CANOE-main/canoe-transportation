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
validation, and independent transport research. The planned CANOE-main integration is a
second assembly path: the broader compiler currently initializes one shared CANOE SQLite
database and invokes sector modules against it. Transportation should therefore reuse the
same parameterization contracts either to build its standalone database or to contribute
rows to an already initialized master database; multi-sector integration should not require
a second transport transformation implementation.

```text
.
├── AGENTS.md                               # Stable repository policy
├── README.md                               # Human-facing project orientation
├── .agents/
│   ├── PLANS.md                            # ExecPlan protocol
│   ├── plans/                              # Task-local implementation records
│   └── skills/                             # Optional task-retrieved procedures
├── config/
│   ├── paths.yaml                          # Canonical paths and artifact-family impact routes
│   ├── sources.yaml                        # External-source registry and provenance
│   ├── scenarios/                          # Scenario authoring contract
│   └── parameters/
│       ├── rules.yaml                      # Extraction and harmonization contracts
│       └── conversion.yaml                 # Reusable conversion factors
├── workflow/
│   └── Snakefile                           # Dependency and artifact orchestration
├── src/
│   ├── setup.py                            # Configuration/schema smoke entrypoint
│   ├── build_transport.py                  # Atomic database build, publication, and report owner
│   ├── canoe_adapter.py                    # Canoe-main orchestrator adapter for running this backend; exact upstream contract is WIP - #to-do
│   ├── fetching/                           # Upstream download, cache, and interim normalization
│   │   ├── nrcan_ceud.py                   # NRCan CEUD transport tables
│   │   ├── vehicle_population.py           # Ontario MTO report acquisition and normalization
│   │   ├── statcan_tables.py               # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py               # CER energy future tables
│   │   ├── nlr_atb_autonomie.py            # NLR ATB and ANL Autonomie inputs
│   │   ├── fueleconomy_vehicles.py         # Opt-in FuelEconomy.gov class evidence
│   │   ├── vpic_vehicle_types.py           # Opt-in vPIC vehicle-type evidence
│   │   ├── vpic_model_years.py             # Opt-in vPIC make/model-year evidence
│   │   └── assorted_sources.py             # Smaller registered source adapters
│   ├── parameterization/                   # Transform normalized inputs into model parameters
│   │   ├── manual_parameters.py            # Resolve compact category/powertrain selectors
│   │   ├── road_stocks_and_demands.py      # Existing stock and demand products from road modes
│   │   ├── offroad_stocks_and_demands.py   # Existing stock and demand products from off-road modes
│   │   ├── ev_chargers.py                  # EV charging infrastructure capacity
│   │   ├── road_lifetimes_survival.py      # Accepted road lifetime, survival, and MTO diagnostics
│   │   ├── offroad_lifetimes.py            # Lifetimes of remaining technologies
│   │   ├── road_aggregation.py             # Reviewed mapping application and aggregation weights
│   │   ├── vehicle_mapping_bootstrap.py    # Explicit mapping-development entrypoint
│   │   ├── road_efficiencies.py            # Road technology efficiencies - #to-do
│   │   ├── offroad_efficiencies.py         # Off-road technology efficiencies - #to-do
│   │   ├── road_capex_opex.py              # Investment and operating costs - #to-do
│   │   ├── offroad_capex_opex.py           # Investment and operating costs - #to-do
│   │   ├── ldv_charging_profiles.py        # Hourly LDEV charging demand profiles - #to-do
│   │   ├── road_embodied_emissions.py      # Vehicle-cycle and operating emissions - #to-do
│   │   ├── market_constraints.py           # Market shares, policy limits, and SCC rules - #to-do
│   │   └── adoption_constraints.py         # Adoption and growth constraints - #to-do
│   ├── utils/
│   │   ├── __init__.py                     # Typed config loading and artifact path resolution
│   │   ├── files.py                        # Shared hashing and atomic CSV publication
│   │   └── vehicle_labels.py               # Shared vehicle-label mechanics
│   └── validation/
│       ├── config_models.py                # Pydantic configuration contracts
│       ├── config_smoke.py                 # Setup-time config/schema status and directory creation
│       ├── provenance.py                   # Source and dataset provenance
│       ├── schema_contract.py              # canoe-schema v4 compatibility
│       ├── insertion.py                    # Validated parameterized insertion
│       ├── database_bootstrap.py           # Post-insertion database integrity checks
│       ├── sqlite_utils.py                 # Shared SQLite identifier mechanics
│       └── legacy_compare.py               # Narrow configured legacy comparison
├── inputs/
│   ├── 0_canoe_template/                   # Backend-owned structural templates
│   ├── 0_cache/                            # Authoritative cached downloads
│   ├── 0_external_models/                  # Registered external-model artifacts
│   ├── 0_manual_params/                    # Review-owned compact manual parameter tables
│   ├── 1_interim/                          # Normalized source and auditable intermediate tables
│   ├── 2_processed/                        # Parameter-ready ETL products
│   └── validation/                         # Development, review, and transformation diagnostics
├── outputs/
│   ├── sqlite/                             # Built CANOE/Temoa-ready databases
│   ├── validation/                         # Validation and schema integrity reports
│   └── logs/                               # Run logs and warnings
├── docs/
│   ├── backend_architecture.md             # Repository structure and ownership reference
│   ├── canoe_main_orchestrator.md          # Verified upstream CANOE-main integration context
│   └── etl_flowcharts.md                   # Parameter-specific lineage reference
├── legacy_backend/                         # Read-mostly parity evidence
├── scripts/
│   ├── doctor.py                           # Non-mutating repository readiness check
│   └── clean_runtime.py                    # Explicit runtime cleanup
├── tests/                                  # Focused and integration tests
└── pyproject.toml                          # uv dependencies and tool configuration
```

## Parameterization and assembly boundary

Parameterization should be organized around coherent behavior and materially different
data/transformation contracts rather than one backend-wide module per SQLite parameter.
Road versus off-road is a useful middle-level seam where source evidence, capacity
representation, units, and harmonization differ; it should not be applied mechanically
when a shared behavior is genuinely common. Behavioral owners such as EV infrastructure,
charging profiles, road aggregation, market constraints, and adoption constraints remain
appropriate where they form the clearer seam.

`src/parameterization/` should produce deterministic parameter-ready artifacts or row-builder
outputs and remain independent of SQLite transactions. `build_transport.py` consumes those
contracts to create the standalone transport database. The future CANOE-main boundary should
consume the same contracts against the shared initialized CANOE database and follow the
then-current upstream `CANOEModule`/sector-config lifecycle documented in
`docs/canoe_main_orchestrator.md`. Generic schema validation, provenance registration, and
insertion remain shared infrastructure rather than being reimplemented by each module.

## Artifact ownership and impact routing

`config/paths.yaml` is the machine-readable topology anchor. Its `artifacts` entries are
coarse stable families, not an import graph: each declares one canonical directory,
layer, owner, producer entrypoint(s), principal downstream consumers, and focused validation
surfaces. Runtime code resolves these families through `utils.resolve_artifact_path`.
The typed path contract rejects routes outside their declared interim, processed,
input-validation, database, or output-validation root.

Ontario MTO normalized reports remain owned by `fetching.vehicle_population` in
`inputs/1_interim/fetched_ontario_vehicle_population`. Reviewed mapping application and
road aggregation products are owned by `parameterization.road_aggregation` in
`inputs/2_processed/road_aggregation`. Accepted NHTSA/NEMS/Wards lifetime products are
published under `inputs/2_processed/road_lifetimes_survival`; MTO apparent-retention
intermediates and review evidence are routed separately through `inputs/1_interim` and
`inputs/validation`.

## Runtime and development boundaries

Ordinary road aggregation reads `config/parameters/vehicle_size_class_map.csv` and does
not load rating catalogues, generate candidates, or consume manual/vPIC review evidence.
Candidate diagnostics require the explicit `--mapping-diagnostics` flag on
`parameterization.road_aggregation`; mapping bootstrap and replacement require the
explicit
`parameterization.vehicle_mapping_bootstrap` entrypoint. vPIC adapters retain their
distinct API/cache/offline contracts and consume only bootstrap-produced request files.
The default doctor excludes development-only mapping evidence, while explicit manual
registry validation still checks it.

`parameterization.road_lifetimes_survival` defaults to accepted source-derived lifetime
generation and does not load Ontario Report A history or the reviewed vehicle mapping.
Historical MTO apparent-retention, mapping-coverage, scope, and decision evidence requires
the explicit `--mto-diagnostics` mode; `--all` and the retained
`build_lifetime_artifacts` Python function provide the combined compatibility path.

Configuration structure is validated by `validation.config_models` during
`utils.load_config_bundle`; `scripts/doctor.py` adds non-mutating readiness checks.
`validation.schema_contract`, `validation.insertion`, and
`validation.database_bootstrap` own schema creation/compatibility, row validation and
insertion, and post-insertion integrity respectively. `build_transport.py` owns atomic
database publication and the configured report in `outputs/validation`; legacy
comparison remains isolated in `validation.legacy_compare`.
